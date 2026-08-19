-- Hub Command Channel (Lua plugin)
--
-- Manages the HubCommandChannel subscription:
--   - Forwards decrypted signaling/control messages to the Rust Hub
--   - Routes command messages to Lua event system
--   - Acks commands by sequence number
--   - Sends application-level heartbeat every 30s (agent status sync)
--   - Relays outgoing WebRTC signals through ActionCable
--
-- NOTE: ActionCable protocol pings are handled automatically by the
-- action_cable primitive (Rust). The 30s heartbeat here is application-
-- level HubCommandChannel business logic, NOT protocol-level.
--
-- Hot-reload safe: connection, subscription, timer, and event listener
-- are stored in hub.state so they survive reloads without orphaning.

local state = require("hub.state")
local Agent = require("lib.agent")
local InternalClient = require("lib.internal_client")
local TargetContext = require("lib.target_context")

local function resolve_webhook_target(payload)
    payload = payload or {}

    if payload.target_id then
        return TargetContext.resolve({
            explicit = {
                target_id = payload.target_id,
                target_path = payload.target_path,
                target_repo = payload.target_repo,
            },
            require_target_id = true,
            require_target_path = true,
        })
    end

    return TargetContext.find_by_repo(payload.target_repo)
end

-- Persistent handles across hot-reloads
local handles = state.get("hub_commands.handles", {})

-- Skip network connections in unit test mode (BOTSTER_ENV=test)
if config.env("BOTSTER_ENV") == "test" then
    log.info("Test mode: skipping ActionCable connection")
    return {}
end

-- Skip network connections in offline mode (--offline flag)
if hub.is_offline() then
    log.info("Offline mode: skipping ActionCable connection")
    return {}
end

-- Reuse existing connection or create a new one
if not handles.conn then
    handles.conn = action_cable.connect({ crypto = true })
end

-- Subscribe to HubCommandChannel (reuse existing or create new)
-- The callback is always replaced on reload so routing logic stays current.
if handles.channel then
    action_cable.unsubscribe(handles.channel)
end

handles.channel = action_cable.subscribe(handles.conn, "HubCommandChannel",
    { hub_id = hub.server_id(), start_from = 0 },
    function(message, channel_id)
        local msg_type = message.type

        if msg_type == "signal" or msg_type == "bundle_request" or msg_type == "transport_config_request" then
            hub.handle_signaling_message(message)
        elseif msg_type == "message" then
            local event_type = message.event_type or ""

            if event_type == "create_agent" then
                local payload = message.payload or {}
                local resolved_target, target_err = resolve_webhook_target(payload)
                if not resolved_target then
                    log.warn(string.format("Ignoring webhook create_agent without admitted target: %s", tostring(target_err)))
                    goto ack_message
                end
                local cmd_repo = resolved_target.target_repo or resolved_target.repo
                local issue_num = payload.issue_number
                -- Build workspace name inline
                local ws_name = nil
                if cmd_repo and issue_num then
                    ws_name = cmd_repo .. "#" .. tostring(issue_num)
                end
                InternalClient.dispatch("hub_commands", {
                    type = "create_agent",
                    issue_or_branch = issue_num and tostring(issue_num),
                    prompt = payload.prompt or payload.context or payload.comment_body,
                    repo = cmd_repo,
                    target_id = resolved_target.target_id,
                    target_path = resolved_target.target_path,
                    target_repo = resolved_target.target_repo,
                    metadata = {
                        issue_number = issue_num,
                        invocation_url = payload.issue_url,
                        workspace = ws_name,
                        workspace_metadata = cmd_repo and { repo = cmd_repo, issue_number = issue_num } or nil,
                        target_id = resolved_target.target_id,
                        target_path = resolved_target.target_path,
                        target_repo = resolved_target.target_repo,
                    },
                })
            elseif event_type == "agent_cleanup" then
                local payload = message.payload or {}
                local resolved_target, target_err = resolve_webhook_target(payload)
                if not resolved_target then
                    log.warn(string.format("Ignoring webhook agent_cleanup without admitted target: %s", tostring(target_err)))
                    goto ack_message
                end
                local cmd_repo = resolved_target.target_repo or resolved_target.repo or ""
                if payload.issue_number then
                    local ws_name = cmd_repo .. "#" .. tostring(payload.issue_number)
                    local matches = Agent.find_by_workspace(ws_name, resolved_target)
                    if #matches == 0 then
                        for _, agent in ipairs(Agent.find_by_meta("issue_number", payload.issue_number)) do
                            if TargetContext.matches(agent, resolved_target) then
                                matches[#matches + 1] = agent
                            end
                        end
                    end
                    for _, agent in ipairs(matches) do
                        InternalClient.dispatch("hub_commands", {
                            type = "delete_agent",
                            session_uuid = agent.session_uuid,
                            delete_worktree = false,
                        })
                    end
                end
            else
                log.warn("Unhandled command event_type: " .. event_type)
            end

            -- Ack by sequence
            ::ack_message::
            if message.sequence then
                action_cable.perform(channel_id, "ack", { sequence = message.sequence })
            end
        end
    end
)

-- Send heartbeat helper (used by timer)
local function send_heartbeat()
    if handles.channel then
        action_cable.perform(handles.channel, "heartbeat", {})
    end
end

-- Cancel old heartbeat timer before creating a new one
if handles.heartbeat_timer then
    timer.cancel(handles.heartbeat_timer)
end

-- Application-level heartbeat (30s interval, 3 chances before 90s timeout)
-- Keeps the hub marked alive in Rails — NOT an ActionCable protocol heartbeat
handles.heartbeat_timer = timer.every(30, send_heartbeat)

-- Unsubscribe old event listener before re-registering
if handles.signal_event_sub then
    events.off(handles.signal_event_sub)
end

-- Relay outgoing WebRTC signals through ActionCable. Rust encrypts them when enabled.
handles.signal_event_sub = events.on("outgoing_signal", function(data)
    if handles.channel then
        action_cable.perform(handles.channel, "signal", data)
    end
end)

-- ============================================================================
-- Module Interface
-- ============================================================================

local M = {}

function M._before_reload()
    -- Connection and subscription are stored in hub.state — no cleanup needed
    -- here. The module top-level code unsubscribes/resubscribes the channel
    -- and cancels/recreates the timer on reload.
    log.info("hub_commands.lua reloading")
end

function M._after_reload()
    log.info("hub_commands.lua reloaded")
end

log.info("Hub commands plugin loaded")
return M
