# frozen_string_literal: true

require_relative "concerns/health_status"

# Hub Signaling Channel - Browser-facing WebRTC signal relay
#
# Thin relay for encrypted WebRTC signaling between browser and CLI.
# Rails is a dumb pipe — it never inspects envelope contents.
# Only the endpoints (browser/CLI) can decrypt after Olm session handshake.
#
# Streams:
# - hub:{hub_id}:signal:{browser_identity} — scoped signals from CLI (opaque envelopes)
# - hub:{hub_id}:health — shared CLI online/offline status
#
# Actions:
# - signal(envelope) — relays opaque envelope to CLI via hub_command broadcast
# - request_bundle() — asks CLI for a fresh signed public bundle
#
# Auth: Browser session (Warden) — NOT for CLI use
class HubSignalingChannel < ApplicationCable::Channel
  def subscribed
    @hub = current_user.hubs.find_by(id: params[:hub_id])
    @browser_identity = params[:browser_identity]

    unless @hub && @browser_identity.present?
      reject
      return
    end

    # Scoped signal stream — only this browser tab receives signals
    stream_from "hub:#{@hub.id}:signal:#{@browser_identity}"

    # Shared health stream — all browsers see CLI status
    stream_from "hub:#{@hub.id}:health"

    # Immediately tell browser current CLI status
    transmit(HealthStatus.message(@hub.active? ? HealthStatus::ONLINE : HealthStatus::OFFLINE))

    # Ask the live Hub for its transport mode. The server only relays the reply.
    if @hub.active?
      ActionCable.server.broadcast(
        "hub_command:#{@hub.id}",
        { type: "transport_config_request", browser_identity: @browser_identity }
      )
    end

    # Paired browsers connect signaling with their long-term identity key.
    # Ask the CLI for a fresh signed bundle right away so the first offer can
    # use a new session without an extra browser-request round trip.
    if @hub.active? && !@browser_identity.start_with?("anon:")
      ActionCable.server.broadcast(
        "hub_command:#{@hub.id}",
        { type: "bundle_request", browser_identity: @browser_identity }
      )
    end

    Rails.logger.info "[HubSignalingChannel] Browser subscribed: hub=#{@hub.id}, identity=#{@browser_identity}"
  end

  def unsubscribed
    Rails.logger.info "[HubSignalingChannel] Browser unsubscribed: hub=#{@hub&.id}"
  end

  # Relay opaque encrypted envelope from browser → CLI
  def signal(data)
    return unless @hub

    ActionCable.server.broadcast(
      "hub_command:#{@hub.id}",
      { type: "signal", browser_identity: @browser_identity, envelope: data["envelope"] }
    )
  end

  def request_bundle(_data = nil)
    return unless @hub

    ActionCable.server.broadcast(
      "hub_command:#{@hub.id}",
      { type: "bundle_request", browser_identity: @browser_identity }
    )
  end

  def request_transport_config(_data = nil)
    return unless @hub

    ActionCable.server.broadcast(
      "hub_command:#{@hub.id}",
      { type: "transport_config_request", browser_identity: @browser_identity }
    )
  end
end
