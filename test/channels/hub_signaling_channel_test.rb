# frozen_string_literal: true

require "test_helper"

# Tests HubSignalingChannel — browser-facing WebRTC signal relay.
#
# This is the browser's channel for WebRTC signaling. It:
# - Streams scoped signals from CLI (via hub:id:signal:identity)
# - Streams shared health updates (via hub:id:health)
# - Relays opaque encrypted envelopes from browser → CLI (via hub_command stream)
#
# Auth: Browser session (Warden) — NOT for CLI use.
# CLI uses HubCommandChannel for the other direction.
class HubSignalingChannelTest < ActionCable::Channel::TestCase
  tests HubSignalingChannel

  setup do
    @user = users(:primary_user)
    @hub = hubs(:active_hub)
    @browser_identity = "browser-#{SecureRandom.hex(16)}"
    stub_connection current_user: @user
  end

  # === Subscription Tests ===

  test "subscribes and streams from signal and health channels" do
    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    assert subscription.confirmed?
    assert_has_stream "hub:#{@hub.id}:signal:#{@browser_identity}"
    assert_has_stream "hub:#{@hub.id}:health"
  end

  test "transmits initial health status on subscribe" do
    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    assert subscription.confirmed?
    assert_equal 1, transmissions.size

    health = transmissions.first
    assert_equal "health", health["type"]
    assert_includes %w[online offline], health["cli"]
  end

  test "transmits online when hub is active" do
    @hub.update!(alive: true, last_seen_at: Time.current)

    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    health = transmissions.first
    assert_equal "online", health["cli"]
  end

  test "transmits offline when hub is not active" do
    @hub.update!(alive: false)

    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    health = transmissions.first
    assert_equal "offline", health["cli"]
  end

  test "transmits offline when hub heartbeat is stale" do
    @hub.update!(alive: true, last_seen_at: 3.minutes.ago)

    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    health = transmissions.first
    assert_equal "offline", health["cli"]
  end

  test "subscribe requests a fresh bundle for paired browsers" do
    assert_broadcast_on("hub_command:#{@hub.id}", {
      type: "bundle_request",
      browser_identity: @browser_identity
    }) do
      subscribe hub_id: @hub.id, browser_identity: @browser_identity
    end
  end

  test "subscribe requests transport config from the live hub" do
    assert_broadcast_on("hub_command:#{@hub.id}", {
      type: "transport_config_request",
      browser_identity: @browser_identity
    }) do
      subscribe hub_id: @hub.id, browser_identity: @browser_identity
    end
  end


  test "anonymous subscribe requests config but not a bundle" do
    assert_broadcasts("hub_command:#{@hub.id}", 1) do
      subscribe hub_id: @hub.id, browser_identity: "anon:test-tab"
    end

    assert subscription.confirmed?
  end

  test "subscribe does not request a bundle while hub is offline" do
    @hub.update!(alive: false)

    assert_broadcasts("hub_command:#{@hub.id}", 0) do
      subscribe hub_id: @hub.id, browser_identity: @browser_identity
    end

    assert subscription.confirmed?
  end

  test "rejects subscription without hub_id" do
    subscribe browser_identity: @browser_identity

    assert subscription.rejected?
  end

  test "rejects subscription without browser_identity" do
    subscribe hub_id: @hub.id

    assert subscription.rejected?
  end

  test "rejects subscription with nonexistent hub" do
    subscribe hub_id: 999_999, browser_identity: @browser_identity

    assert subscription.rejected?
  end

  test "rejects subscription for hub owned by different user" do
    other_user = users(:one)
    other_hub = Hub.create!(user: other_user, identifier: "other-hub", last_seen_at: Time.current)

    subscribe hub_id: other_hub.id, browser_identity: @browser_identity

    assert subscription.rejected?
  ensure
    other_hub&.destroy
  end

  # === Signal Action Tests ===

  test "signal relays envelope to hub_command stream for CLI" do
    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    hub_command_stream = "hub_command:#{@hub.id}"

    assert_broadcast_on(hub_command_stream, {
      type: "signal",
      browser_identity: @browser_identity,
      envelope: { "ciphertext" => "encrypted_offer" }
    }) do
      perform :signal, envelope: { ciphertext: "encrypted_offer" }
    end
  end

  test "request_bundle relays a fresh bundle request to hub_command stream for CLI" do
    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    assert_broadcast_on("hub_command:#{@hub.id}", {
      type: "bundle_request",
      browser_identity: @browser_identity
    }) do
      perform :request_bundle
    end
  end

  test "request_transport_config relays a config request to the live hub" do
    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    assert_broadcast_on("hub_command:#{@hub.id}", {
      type: "transport_config_request",
      browser_identity: @browser_identity
    }) do
      perform :request_transport_config
    end
  end

  test "signal does nothing when hub is nil (rejected subscription)" do
    # Force a subscription that was rejected (no hub found)
    subscribe hub_id: 999_999, browser_identity: @browser_identity
    assert subscription.rejected?
  end

  # === Stream Isolation Tests ===

  test "different browser identities get separate signal streams" do
    identity_a = "browser-aaa"
    identity_b = "browser-bbb"

    subscribe hub_id: @hub.id, browser_identity: identity_a

    assert_has_stream "hub:#{@hub.id}:signal:#{identity_a}"
    assert_has_no_stream "hub:#{@hub.id}:signal:#{identity_b}"
  end

  test "signal stream does not leak to other hubs" do
    subscribe hub_id: @hub.id, browser_identity: @browser_identity

    assert_has_stream "hub:#{@hub.id}:signal:#{@browser_identity}"
    assert_has_no_stream "hub:999:signal:#{@browser_identity}"
  end
end
