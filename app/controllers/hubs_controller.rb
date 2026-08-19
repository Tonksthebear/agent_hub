# frozen_string_literal: true

class HubsController < ApplicationController
  include ApiKeyAuthenticatable

  before_action :authenticate_index_request!, only: [ :index ]
  before_action :authenticate_user!, only: [ :show ]
  before_action :authenticate_hub_request!, only: [ :create, :update, :destroy ]
  before_action :set_hub, only: [ :show, :update, :destroy ]

  # GET /hubs
  # Dashboard showing list of hubs with health status
  def index
    @hubs = current_hub_user.hubs.order(last_seen_at: :desc)

    respond_to do |format|
      format.html { render html: "", layout: "spa" }
      format.json do
        render json: @hubs.map { |hub|
          {
            id: hub.id,
            name: hub.name,
            identifier: hub.identifier,
            fingerprint: hub.fingerprint,
            active: hub.active?,
            last_seen_at: hub.last_seen_at&.iso8601
          }
        }
      end
    end
  end

  # GET /hubs/:id
  # Terminal view for a specific hub
  #
  # JSON format returns status info for browser-side error handling
  def show
    unless Current.hub
      respond_to do |format|
        format.html { redirect_to hubs_path, alert: "Hub not found" }
        format.json { render json: { error: "Hub not found" }, status: :not_found }
      end
      return
    end

    respond_to do |format|
      format.html { render html: "", layout: "spa" }
      format.json do
        render json: {
          id: Current.hub.id,
          identifier: Current.hub.identifier,
          active: Current.hub.active?,
          alive: Current.hub.alive?,
          last_seen_at: Current.hub.last_seen_at&.iso8601,
          seconds_since_heartbeat: Current.hub.last_seen_at ? (Time.current - Current.hub.last_seen_at).to_i : nil
        }
      end
    end
  end

  # POST /hubs
  # CLI registration: creates or finds hub and returns Rails ID
  # Called once at CLI startup before QR code generation
  def create
    hub = Hub.registration_candidate_for(
      current_hub_user,
      identifier: params[:identifier],
      fingerprint: params[:fingerprint]
    ) || current_hub_user.hubs.build
    is_new = hub.new_record?
    hub.identifier = params[:identifier]
    hub.last_seen_at = Time.current
    hub.alive = false
    if is_new && params[:repo].present?
      hub.name = params[:repo]
    end

    hub.fingerprint = params[:fingerprint] if params[:fingerprint].present?

    if hub.save
      status = is_new ? :created : :ok
      render json: { id: hub.id, identifier: hub.identifier }, status: status
    else
      render json: { error: hub.errors.full_messages.join(", ") }, status: :unprocessable_entity
    end
  end

  # PUT /hubs/:id
  # CLI heartbeat: updates existing hub by Rails ID
  def update
    unless Current.hub
      render json: { error: "Hub not found" }, status: :not_found
      return
    end

    Current.hub.last_seen_at = Time.current
    Current.hub.alive = params.key?(:alive) ? ActiveModel::Type::Boolean.new.cast(params[:alive]) : true
    Current.hub.fingerprint = params[:fingerprint] if params[:fingerprint].present?

    if Current.hub.save
      render json: { success: true, hub_id: Current.hub.id }
    else
      render json: { error: Current.hub.errors.full_messages.join(", ") }, status: :unprocessable_entity
    end
  end

  # DELETE /hubs/:id
  # Destroys the hub record. Only called on CLI reset.
  # Normal shutdown uses PUT with alive: false instead.
  def destroy
    if Current.hub
      Current.hub.destroy!
      render json: { success: true }
    else
      render json: { success: true } # Idempotent - already gone is fine
    end
  end

  private

  def authenticate_index_request!
    if api_key_present?
      authenticate_with_api_key!
    else
      authenticate_user!
    end
  end

  def authenticate_hub_request!
    if api_key_present?
      authenticate_with_api_key!
    elsif request.format.json? || request.content_type&.include?("json")
      render json: { error: "API key required" }, status: :unauthorized
    else
      authenticate_user!
    end
  end

  def current_hub_user
    current_api_user || current_user
  end

  def set_hub
    Current.hub = current_hub_user.hubs.find_by(id: params[:id])
  end
end
