# frozen_string_literal: true

require "test_helper"

# Tests for hub lifecycle endpoints.
#
# The CLI uses these endpoints to:
# 1. Register hub on startup (POST /hubs)
# 2. Update hub / send heartbeat (PUT /hubs/:id)
# 3. Heartbeat to keep hub alive (PATCH /hubs/:id/heartbeat)
# 4. Unregister hub on shutdown (DELETE /hubs/:id)
#
# These tests verify the API contract the CLI expects.
class HubsControllerTest < ActionDispatch::IntegrationTest
  include ApiTestHelper

  # ==========================================================================
  # POST /hubs - Register Hub
  # ==========================================================================

  test "POST /hubs returns 401 without authentication" do
    post hubs_url,
      params: { identifier: "new-hub-123" }.to_json,
      headers: json_headers

    assert_response :unauthorized
  end

  test "POST /hubs creates new hub and returns id" do
    identifier = "new-hub-#{SecureRandom.hex(8)}"
    # Pre-create auth headers so auth hub isn't counted in assert_difference
    headers = auth_headers_for(:primary_user)

    assert_difference -> { Hub.count }, 1 do
      post hubs_url,
        params: { identifier: identifier }.to_json,
        headers: headers
    end

    assert_response :created
    json = assert_json_keys(:id, :identifier)

    assert_kind_of Integer, json["id"]
    assert_equal identifier, json["identifier"]

    hub = Hub.find(json["id"])
    assert_equal identifier, hub.identifier
    assert_equal users(:primary_user), hub.user
    refute hub.alive?, "Hub should not be advertised online until HubCommandChannel subscribes"
  end

  test "POST /hubs ignores name param from CLI registration" do
    identifier = "named-hub-#{SecureRandom.hex(8)}"

    post hubs_url,
      params: { identifier: identifier, name: "My Cool Hub", repo: "owner/repo" }.to_json,
      headers: auth_headers_for(:primary_user)

    assert_response :created

    hub = Hub.find_by(identifier: identifier)
    assert_equal "owner/repo", hub.name, "name param should be ignored; repo used as default"
  end

  test "POST /hubs sets repo as name for new hubs" do
    identifier = "repo-hub-#{SecureRandom.hex(8)}"

    post hubs_url,
      params: { identifier: identifier, repo: "owner/my-repo" }.to_json,
      headers: auth_headers_for(:primary_user)

    assert_response :created

    hub = Hub.find_by(identifier: identifier)
    assert_equal "owner/my-repo", hub.name
  end

  test "POST /hubs finds existing hub by identifier and returns 200" do
    hub = hubs(:active_hub)
    original_id = hub.id
    headers = auth_headers_for(:primary_user)

    assert_no_difference -> { Hub.count } do
      post hubs_url,
        params: { identifier: hub.identifier }.to_json,
        headers: headers
    end

    assert_response :ok  # 200 for existing, 201 for new
    json = assert_json_keys(:id, :identifier)

    assert_equal original_id, json["id"]
  end

  test "POST /hubs finds existing hub by fingerprint and updates identifier" do
    hub = hubs(:active_hub)
    new_identifier = "device-#{SecureRandom.hex(8)}"
    original_id = hub.id
    headers = auth_headers_for(:primary_user)

    assert_no_difference -> { Hub.count } do
      post hubs_url,
        params: { identifier: new_identifier, fingerprint: hub.fingerprint }.to_json,
        headers: headers
    end

    assert_response :ok
    json = assert_json_keys(:id, :identifier)

    assert_equal original_id, json["id"]
    assert_equal new_identifier, json["identifier"]
    assert_equal new_identifier, hub.reload.identifier
  end

  # ==========================================================================
  # PUT /hubs/:id - Update Hub
  # ==========================================================================

  test "PUT /hubs/:id returns 401 without authentication" do
    hub = hubs(:active_hub)

    put hub_url(hub),
      params: { repo: "owner/repo" }.to_json,
      headers: json_headers

    assert_response :unauthorized
  end

  test "PUT /hubs/:id updates existing hub" do
    hub = hubs(:active_hub)

    put hub_url(hub),
      params: {}.to_json,
      headers: auth_headers_for(:primary_user)

    assert_response :ok
    json = assert_json_keys(:success, :hub_id)

    assert_equal hub.id, json["hub_id"]
  end

  test "PUT /hubs/:id updates last_seen_at" do
    hub = hubs(:stale_hub)
    old_last_seen = hub.last_seen_at

    put hub_url(hub),
      params: {}.to_json,
      headers: auth_headers_for(:primary_user)

    assert_response :ok

    hub.reload
    assert_operator hub.last_seen_at, :>, old_last_seen
  end

  test "PUT /hubs/:id returns 404 for other user's hub" do
    other_hub = users(:one).hubs.create!(
      identifier: "other-user-hub-#{SecureRandom.hex(4)}",
      last_seen_at: Time.current
    )

    put hub_url(other_hub),
      params: {}.to_json,
      headers: auth_headers_for(:primary_user)

    assert_response :not_found
  end

  # ==========================================================================
  # PATCH /hubs/:hub_id/heartbeat - Heartbeat
  # ==========================================================================

  test "PATCH /hubs/:hub_id/heartbeat returns 401 without authentication" do
    hub = hubs(:active_hub)

    patch hub_heartbeat_url(hub),
      headers: json_headers

    assert_response :unauthorized
  end

  test "PATCH /hubs/:hub_id/heartbeat updates last_seen_at" do
    hub = hubs(:stale_hub)
    old_last_seen = hub.last_seen_at

    patch hub_heartbeat_url(hub),
      headers: auth_headers_for(:primary_user)

    assert_response :ok
    json = assert_json_keys(:success, :last_seen_at)

    assert_equal true, json["success"]

    hub.reload
    assert_operator hub.last_seen_at, :>, old_last_seen
  end

  test "PATCH /hubs/:hub_id/heartbeat returns 404 for unknown hub" do
    patch hub_heartbeat_url(hub_id: 999999),
      headers: auth_headers_for(:primary_user)

    assert_response :not_found
    assert_json_error("Hub not found")
  end

  test "PATCH /hubs/:hub_id/heartbeat returns 404 for other user's hub" do
    other_hub = users(:one).hubs.create!(
      identifier: "other-user-hub-#{SecureRandom.hex(4)}",
      last_seen_at: Time.current
    )

    patch hub_heartbeat_url(other_hub),
      headers: auth_headers_for(:primary_user)

    assert_response :not_found
  end

  # ==========================================================================
  # PUT /hubs/:id with alive: false - Graceful Shutdown
  # ==========================================================================

  test "PUT /hubs/:id with alive false marks hub offline" do
    hub = hubs(:active_hub)
    assert hub.alive?, "Precondition: hub should be alive"

    put hub_url(hub),
      params: { alive: false }.to_json,
      headers: auth_headers_for(:primary_user)

    assert_response :ok

    hub.reload
    refute hub.alive?, "Hub should be marked offline"
    assert Hub.exists?(hub.id), "Hub record should still exist"
  end

  test "PUT /hubs/:id without alive param defaults to alive true" do
    hub = hubs(:stale_hub)
    refute hub.alive?, "Precondition: hub should be offline"

    put hub_url(hub),
      params: {}.to_json,
      headers: auth_headers_for(:primary_user)

    assert_response :ok

    hub.reload
    assert hub.alive?, "Hub should be marked alive by default"
  end

  # ==========================================================================
  # DELETE /hubs/:id - Destroy Hub (CLI Reset)
  # ==========================================================================

  test "DELETE /hubs/:id returns 401 without authentication" do
    hub = hubs(:active_hub)

    delete hub_url(hub),
      headers: json_headers

    assert_response :unauthorized
  end

  test "DELETE /hubs/:id destroys hub record" do
    hub = hubs(:active_hub)
    headers = auth_headers_for(:primary_user)

    assert_difference -> { Hub.count }, -1 do
      delete hub_url(hub),
        headers: headers
    end

    assert_response :ok
    json = assert_json_keys(:success)

    assert_equal true, json["success"]
    assert_nil Hub.find_by(id: hub.id), "Hub should be destroyed"
  end

  test "DELETE /hubs/:id is idempotent for nonexistent hub" do
    delete hub_url(id: 999999),
      headers: auth_headers_for(:primary_user)

    assert_response :ok
    json = assert_json_keys(:success)

    assert_equal true, json["success"]
  end

  test "DELETE /hubs/:id cannot delete other user's hub" do
    headers = auth_headers_for(:primary_user)
    other_hub = users(:one).hubs.create!(
      identifier: "other-hub-#{SecureRandom.hex(4)}",
      last_seen_at: Time.current
    )

    assert_no_difference -> { Hub.count } do
      delete hub_url(other_hub),
        headers: headers
    end

    # Returns success because it's idempotent (hub "not found" for this user)
    assert_response :ok
  end
end
