# frozen_string_literal: true

class Hub < ApplicationRecord
  belongs_to :user
  has_many :hub_commands, dependent: :destroy
  has_one :hub_token, dependent: :destroy
  has_one :mcp_token, class_name: "Integrations::Github::MCPToken", dependent: :destroy
  has_one :cloudflare_tunnel, class_name: "Hubs::CloudflareTunnel", dependent: :destroy
  has_many :stable_webhook_hostnames, class_name: "Hubs::StableWebhookHostname", dependent: :destroy

  validates :identifier, presence: true, uniqueness: true
  validates :last_seen_at, presence: true
  validates :fingerprint, uniqueness: { scope: :user_id }, allow_nil: true

  scope :active, -> { where(alive: true).where("last_seen_at > ?", 2.minutes.ago) }
  scope :stale, -> { where(alive: false).or(where("last_seen_at <= ?", 2.minutes.ago)) }
  scope :with_notifications, -> { where(notifications_enabled: true) }

  def self.registration_candidate_for(user, identifier:, fingerprint:)
    hubs = user.hubs
    return hubs.find_by(identifier: identifier) if fingerprint.blank?

    hubs.where(identifier: identifier).or(hubs.where(fingerprint: fingerprint)).first
  end

  after_commit :broadcast_hubs_list, if: :hub_list_commit?
  after_update_commit :broadcast_health_status, if: :health_status_changed?
  after_destroy_commit :broadcast_health_offline

  # Check if this hub is active (alive flag set and seen within 2 minutes)
  def active?
    alive? && last_seen_at > 2.minutes.ago
  end

  # Display name for the hub
  def name
    read_attribute(:name).presence || identifier.truncate(20)
  end

  # Atomically increment and return the next message sequence number.
  # Uses row-level locking for safe concurrent access.
  def next_message_sequence!
    with_lock do
      increment!(:message_sequence)
      message_sequence
    end
  end

  private

  def broadcast_health_offline
    ActionCable.server.broadcast("hub:#{id}:health", { type: "health", cli: "offline" })
  end

  def broadcast_hubs_list
    HubListChannel.broadcast_to(user, { type: "refresh" })
  end

  # Only broadcast when active? status actually transitions
  def health_status_changed?
    return true if saved_change_to_alive?
    return false unless saved_change_to_last_seen_at?

    # Check if last_seen_at change caused an active? transition
    old_last_seen, new_last_seen = saved_change_to_last_seen_at
    threshold = 2.minutes.ago

    was_active = alive? && old_last_seen.present? && old_last_seen > threshold
    is_active = alive? && new_last_seen.present? && new_last_seen > threshold

    was_active != is_active
  end

  def hub_list_changed?
    health_status_changed? || saved_change_to_name? || saved_change_to_identifier?
  end

  def hub_list_commit?
    transaction_include_any_action?([ :create, :destroy ]) || hub_list_changed?
  end

  # Broadcast hub health status to ActionCable health stream
  # JavaScript connections listen for these via hub:{id}:health stream
  def broadcast_health_status
    status = active? ? "online" : "offline"
    ActionCable.server.broadcast("hub:#{id}:health", { type: "health", cli: status })
    Rails.logger.debug "[Hub] Broadcast health transition: #{status}"
  end
end
