//! Server communication for Hub.
//!
//! This module handles all communication with the Rails server, including:
//!
//! - WebRTC peer connections and signaling (E2E encrypted with vodozemac Olm)
//! - Agent notification delivery via background worker
//! - Device and hub registration
//! - Lua plugin event processing (ActionCable, WebSocket, timers, HTTP, etc.)
//!
//! # Architecture
//!
//! ActionCable channels and heartbeat are now managed by Lua plugins.
//! The Rust side handles WebRTC peer connections, agent notifications,
//! and Lua event processing in the tick loop.

// Rust guideline compliant 2026-02

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::hub::actions::{self, HubAction};
use crate::hub::{registration, Hub, PendingTerminalAttach, PendingTerminalAttachRequest};
use crate::notifications::push::send_push_direct;
use base64::Engine;

mod event_lua;
mod event_maintenance;
mod event_session;
mod event_socket_terminal;
mod event_webrtc;
mod fixtures;
mod lua_bridge;
mod metrics_guardrails;
mod push_notifications;
mod server_lifecycle;
mod session_io_bridge;
mod session_reconnect;
mod terminal_attach;
mod terminal_cleanup;
mod terminal_client_adapters;
mod terminal_clients;
mod terminal_profile;
mod terminal_snapshot;
mod terminal_stream;
mod webrtc_transport;

#[derive(Debug, Clone, PartialEq, Eq)]
enum CargoBuildProfile {
    Debug,
    Release,
    Named(String),
}

fn detect_running_cargo_profile(current_exe: &Path) -> Option<CargoBuildProfile> {
    let components: Vec<_> = current_exe.components().collect();

    for window in components.windows(2) {
        let target = window[0].as_os_str();
        let profile = window[1].as_os_str();
        if target == "target" {
            let profile = profile.to_string_lossy();
            return match profile.as_ref() {
                "debug" => Some(CargoBuildProfile::Debug),
                "release" => Some(CargoBuildProfile::Release),
                "" => None,
                other => Some(CargoBuildProfile::Named(other.to_string())),
            };
        }
    }

    None
}

/// Infer Cargo target dir from the running executable path.
///
/// For paths like `<...>/target/<profile>/<bin>`, returns `<...>/target`.
fn detect_running_target_dir(current_exe: &Path) -> Option<std::path::PathBuf> {
    let profile_dir = current_exe.parent()?;
    let target_dir = profile_dir.parent()?;
    (target_dir.file_name()? == "target").then(|| target_dir.to_path_buf())
}

impl Hub {
    /// How long a terminal attach intent can stay pending before `not_found`.
    const TERMINAL_ATTACH_NOT_FOUND_TIMEOUT: Duration = Duration::from_secs(10);
    const HOT_SUBHANDLER_SLOW: Duration = Duration::from_millis(50);
    const SNAPSHOT_SLOW: Duration = Duration::from_millis(100);
    const CLEANUP_SCAN_SLOW: Duration = Duration::from_millis(50);
    const CLOSED_AFTER_CONNECT_WINDOW: Duration = Duration::from_secs(10);

    /// Spawn a background task to reconnect to a session process.
    ///
    /// The blocking `connect_and_seed` handshake runs in `spawn_blocking`.
    /// On success, a `SessionReconnectReady` event is sent back to the hub
    /// loop for reader installation and state seeding.
    /// Build a single-line preview for ICE candidate logging.
    /// Test polling entrypoint: calls all poll functions and flushes state.
    ///
    /// Only available in tests. Production uses `run_event_loop()` which drives
    /// individual handlers via `tokio::select!` with zero polling.
    #[cfg(test)]
    pub fn tick(&mut self) {
        self.poll_tui_requests();
        self.poll_outgoing_webrtc_signals();
        self.poll_stream_frames_incoming();
        self.poll_worktree_results();
        self.tick_periodic();
        // Drain shared vecs/flags that are used by tests without the event channel.
        // In production, these are delivered via HubEvent instead.
        self.poll_lua_http_responses();
        self.poll_lua_websocket_events();
        self.poll_pty_notifications();
        self.poll_webrtc_dc_opens();
        self.poll_lua_timers();
        self.poll_lua_action_cable_channels();
        self.poll_webrtc_peer_payloads_for_tests();
        self.poll_user_file_watches();
        self.poll_hub_events();
        self.process_pending_terminal_attaches();
    }

    #[cfg(test)]
    fn poll_hub_events(&mut self) {
        let mut high_priority_rx = self.hub_event_high_priority_rx.take();
        if let Some(ref mut rx) = high_priority_rx {
            while let Ok(event) = rx.try_recv() {
                self.handle_hub_event(event);
            }
        }
        self.hub_event_high_priority_rx = high_priority_rx;

        let Some(ref mut rx) = self.hub_event_rx else {
            return;
        };
        let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        for event in events {
            self.handle_hub_event(event);
        }
    }

    /// Test periodic maintenance fallback.
    ///
    /// Production uses `HubEvent::CleanupTick` from a spawned interval task.
    #[cfg(test)]
    fn tick_periodic(&mut self) {
        self.cleanup_webrtc_peer_registry();
        self.poll_stream_frames_outgoing();
        self.process_pending_terminal_attaches();
        self.dispatch_webrtc_recovery_snapshot_requests();
    }

    // === Per-Event Handlers for select! Loop ===

    /// Dispatch a unified event from the `HubEvent` channel.
    ///
    /// Called by the `select!` loop for each event delivered by background
    /// producers. Each match arm delegates to the appropriate Lua callback
    /// firing logic or message handling.
    pub(crate) fn handle_hub_event(&mut self, event: super::events::HubEvent) {
        use super::events::HubEvent;

        match event {
            HubEvent::HttpResponse(response) => {
                self.lua.fire_http_callback(response);
            }
            HubEvent::WebSocketEvent(ws_event) => {
                self.lua.fire_websocket_event(ws_event);
            }
            HubEvent::PtyNotification(notif) => {
                self.lua.notify_pty_notification(
                    &notif.session_uuid,
                    &notif.session_name,
                    &notif.notification,
                );
            }
            HubEvent::PtyOscEvent {
                session_uuid,
                session_name,
                event,
            } => {
                self.handle_pty_osc_event(session_uuid, session_name, event);
            }
            HubEvent::PtyOutput {
                session_uuid,
                session_name,
                data,
            } => {
                self.handle_pty_output_event(session_uuid, session_name, data);
            }
            HubEvent::PtyProcessExited {
                session_uuid,
                session_name,
                exit_code,
            } => {
                self.handle_pty_process_exited(session_uuid, session_name, exit_code);
            }
            HubEvent::SessionIo(event) => {
                self.handle_session_io_event(event);
            }
            HubEvent::DropPendingSessionIoSnapshot { request_id } => {
                self.pending_session_io_snapshots.remove(&request_id);
            }
            HubEvent::ClientWorkerControl(message) => {
                self.handle_client_worker_control(message);
            }
            HubEvent::TimerFired { timer_id } => {
                self.record_volume_guardrail("timer_fired.count", "timer_fired.volume_burst");
                self.lua.fire_timer_callback(&timer_id);
            }
            HubEvent::AcChannelMessage {
                channel_id,
                message,
            } => {
                self.handle_action_cable_message_event(channel_id, message);
            }
            HubEvent::LuaActionCableRequest(request) => {
                self.process_single_action_cable_request(request);
            }
            HubEvent::LuaHubClientRequest(request) => {
                self.process_hub_client_request(request);
            }
            HubEvent::HubClientMessage {
                connection_id,
                message,
            } => {
                self.handle_hub_client_message_event(connection_id, message);
            }
            HubEvent::HubClientDisconnected { connection_id } => {
                self.handle_hub_client_disconnected_event(connection_id);
            }
            HubEvent::LuaPushRequest { payload } => {
                self.handle_lua_push_request(payload);
            }
            HubEvent::BrowserPushControl {
                browser_identity,
                payload,
            } => {
                self.handle_browser_push_control(&browser_identity, &payload);
            }
            HubEvent::PushSubscriptionsExpired { identities } => {
                self.handle_push_subscriptions_expired(identities);
            }
            HubEvent::WebRtcMessage {
                browser_identity,
                payload,
            } => {
                self.handle_webrtc_message_event(browser_identity, payload);
            }
            HubEvent::WebRtcOutgoingSignal(signal) => {
                self.handle_webrtc_signal(signal);
            }
            HubEvent::WebRtcStreamFrame(frame) => {
                self.handle_stream_frame(frame);
                self.poll_stream_frames_outgoing();
            }
            HubEvent::UserFileWatch { watch_id, events } => {
                let fired = self.lua.fire_user_file_watch(&watch_id, events);
                if fired > 0 {
                    log::debug!("Fired {} user file watch event(s)", fired);
                }
            }
            // LuaFileChange removed — hot-reload now handled by Lua's module_watcher
            HubEvent::CleanupTick => {
                self.handle_cleanup_tick();
            }
            HubEvent::DcOpened {
                browser_identity,
                generation,
            } => {
                self.handle_dc_opened_event(browser_identity, generation);
            }
            HubEvent::WebRtcIngressBackpressure {
                browser_identity,
                source,
            } => {
                self.handle_webrtc_ingress_backpressure_event(browser_identity, source);
            }
            HubEvent::WebRtcSend(send_req) => {
                self.handle_webrtc_send_event(send_req);
            }
            HubEvent::TuiSend(send_req) => {
                self.handle_tui_send_event(send_req);
            }
            HubEvent::SocketClientConnected { client_id, conn } => {
                self.handle_socket_client_connected_event(client_id, conn);
            }
            HubEvent::SocketClientDisconnected { client_id } => {
                self.handle_socket_client_disconnected_event(client_id);
            }
            HubEvent::SocketMessage { client_id, msg } => {
                self.handle_socket_message_event(client_id, msg);
            }
            HubEvent::SocketSend(send_req) => {
                self.handle_socket_send_event(send_req);
            }
            HubEvent::McpHttpRequest {
                caller,
                method,
                params,
                reply,
            } => {
                crate::mcp_http::dispatch_lua_mcp(self.lua.lua(), &caller, &method, params, reply);
            }
            HubEvent::LuaPtyRequest(request) => {
                self.handle_lua_pty_request_event(request);
            }
            HubEvent::LuaHubRequest(request) => {
                self.handle_lua_hub_request_event(request);
            }
            HubEvent::PluginWorkerParentRequest(request) => {
                crate::lua::primitives::plugin_worker::service_parent_request(
                    self.lua.lua(),
                    request,
                );
            }
            HubEvent::LuaConnectionRequest(request) => {
                self.handle_lua_connection_request_event(request);
            }
            HubEvent::LuaWorktreeRequest(request) => {
                self.handle_lua_worktree_request_event(request);
            }
            HubEvent::WorktreeDeleteCompleted {
                path,
                branch,
                result,
            } => {
                self.handle_worktree_delete_completed_event(path, branch, result);
            }
            HubEvent::UrlProbeReady {
                connector_session_uuid,
                parent_session_uuid,
                url,
                ready,
                error,
            } => {
                self.handle_url_probe_ready_event(
                    connector_session_uuid,
                    parent_session_uuid,
                    url,
                    ready,
                    error,
                );
            }
            HubEvent::PluginCommandPrepared {
                request_id,
                command,
                config_path,
                context,
                error_kind,
                error,
            } => {
                self.handle_plugin_command_prepared_event(
                    request_id,
                    command,
                    config_path,
                    context,
                    error_kind,
                    error,
                );
            }
            HubEvent::CommandGateCompleted {
                request_id,
                metadata,
                context,
                success,
                exit_status,
                stdout_tail,
                stderr_tail,
                output_truncated,
                error_kind,
                error,
                duration_ms,
            } => {
                self.handle_command_gate_completed_event(
                    request_id,
                    metadata,
                    context,
                    success,
                    exit_status,
                    stdout_tail,
                    stderr_tail,
                    output_truncated,
                    error_kind,
                    error,
                    duration_ms,
                );
            }
            HubEvent::MessageDelivered { message_len } => {
                log::info!("[MessageDelivery] Delivered message ({message_len} bytes)");
            }
            // Per-session process exited or disconnected.
            // The reader thread already broadcasts PtyEvent directly, so we
            // just need to notify Lua for cleanup — unless this is a reader
            // death (exit_code=None) on a session-backed handle, in which case
            // we attempt to reconnect before declaring the session dead.
            HubEvent::SessionProcessExited {
                session_uuid,
                exit_code,
            } => {
                self.handle_session_process_exited_event(session_uuid, exit_code);
            }

            // Background reconnect completed — validate generation, install
            // reader, publish connection, seed state.
            HubEvent::SessionReconnectReady {
                session_uuid,
                generation,
                conn,
                metadata,
            } => {
                self.handle_session_reconnect_ready_event(session_uuid, generation, conn, metadata);
            }

            HubEvent::SessionUnregistered { session_uuid } => {
                self.handle_session_unregistered_event(session_uuid);
            }
            HubEvent::WebRtcOfferNegotiated(completion) => {
                self.handle_webrtc_offer_negotiated_event(completion);
            }
            HubEvent::WebRtcRecoverySnapshotReady { request, result } => {
                self.handle_webrtc_recovery_snapshot_ready(request, result);
            }
        }

        // Resolve attach intents after every event so session registration and
        // subscribe handling converge immediately without client-side retry loops.
        self.process_pending_terminal_attaches();
    }
}

#[cfg(test)]
mod architecture_guardrail_tests;
#[cfg(test)]
mod async_runtime_tests;
#[cfg(test)]
mod cargo_profile_tests;
#[cfg(test)]
mod event_session_tests;
#[cfg(test)]
mod event_socket_terminal_tests;
#[cfg(test)]
mod lua_bridge_tests;
#[cfg(test)]
mod metrics_guardrails_tests;
#[cfg(test)]
mod push_notifications_tests;
#[cfg(test)]
mod session_io_bridge_tests;
#[cfg(test)]
mod session_reconnect_tests;
#[cfg(test)]
mod terminal_client_adapters_tests;
#[cfg(test)]
mod terminal_profile_tests;
#[cfg(test)]
mod terminal_stream_tests;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod webrtc_transport_tests;
