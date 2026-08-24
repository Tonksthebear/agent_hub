//! Unified event channel for the Hub event loop.
//!
//! All background producers (HTTP threads, WebSocket threads, tokio tasks,
//! PTY watchers, timers, forwarding tasks) send events through bounded
//! priority lanes. The `select!` loop receives on the corresponding receivers
//! and dispatches via `handle_hub_event()`.

// Rust guideline compliant 2026-02

use crate::file_watcher::FileEvent;
use crate::lua::primitives::action_cable::ActionCableRequest;
use crate::lua::primitives::connection::ConnectionRequest;
use crate::lua::primitives::http::CompletedHttpResponse;
use crate::lua::primitives::hub::HubRequest;
use crate::lua::primitives::hub_client::HubClientRequest;
use crate::lua::primitives::plugin_worker::WorkerParentRequest;
use crate::lua::primitives::pty::PtyRequest;
use crate::lua::primitives::tui::TuiSendRequest;
use crate::lua::primitives::webrtc::WebRtcSendRequest;
use crate::lua::primitives::websocket::WsEvent;
use crate::lua::primitives::worktree::WorktreeRequest;
use crate::socket::client_conn::SocketClientConn;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub(crate) const HUB_EVENT_QUEUE_CAPACITY: usize = 65_536;
pub(crate) const HUB_EVENT_HIGH_PRIORITY_QUEUE_CAPACITY: usize = 8_192;

/// Event from a background producer delivered to the Hub event loop.
///
/// Background threads and spawned tasks send events through bounded
/// high-priority and bulk lanes. The `select!` loop dispatches each variant via
/// `handle_hub_event()`.
#[derive(Debug)]
pub(crate) enum HubEvent {
    /// Completed HTTP response from a background thread.
    HttpResponse(CompletedHttpResponse),

    /// WebSocket event from a background connection thread.
    WebSocketEvent(WsEvent),

    /// PTY notification from a watcher task.
    PtyNotification(super::PtyNotificationEvent),

    /// PTY OSC metadata event forwarded from a watcher task.
    ///
    /// A single variant carries agent context + the raw PtyEvent.
    /// The Lua bridge method discriminates the event type and fires
    /// the appropriate hook (e.g., `pty_title_changed`, `pty_cwd_changed`).
    PtyOscEvent {
        /// Session UUID for routing and Lua hook context.
        session_uuid: String,
        /// Session name (e.g., "agent", "server").
        session_name: String,
        /// The PtyEvent variant (TitleChanged, CwdChanged, PromptMark).
        event: crate::agent::pty::PtyEvent,
    },

    /// Opt-in PTY output observer event for sessions that explicitly request
    /// output observation. Normal terminal delivery stays on the data plane.
    PtyOutput {
        /// Session UUID for routing and Lua hook context.
        session_uuid: String,
        /// Session name (e.g., "agent", "server").
        session_name: String,
        /// Raw terminal bytes observed from the PTY stream.
        data: Vec<u8>,
    },

    /// PTY process exited (reader thread detected EOF).
    ///
    /// Sent from the notification watcher task when it receives
    /// `PtyEvent::ProcessExited`. Triggers Lua `process_exited` event
    /// which updates agent status and broadcasts to all clients.
    PtyProcessExited {
        /// Session UUID identifying which session's PTY exited.
        session_uuid: String,
        /// Session name (e.g., "agent", "server").
        session_name: String,
        /// Exit code if available (None if killed by signal or unknown).
        exit_code: Option<i32>,
    },

    /// Session-I/O worker result for non-output mailbox requests.
    SessionIo(crate::worker::session_io::SessionIoEvent),

    /// Drop a Hub-owned pending session-I/O snapshot request.
    ///
    /// Spawned Hub tasks emit this when a snapshot request was reserved in the
    /// pending map but no worker mailbox request was accepted.
    DropPendingSessionIoSnapshot {
        /// Pending snapshot request ID.
        request_id: String,
    },

    /// Client-worker request that needs hub-owned orchestration state.
    ClientWorkerControl(crate::worker::hub_control::HubControlMessage),

    /// WebRTC DataChannel has opened for a browser peer.
    ///
    /// Sent from the `on_data_channel` callback. Triggers `peer_connected`
    /// Lua callback and spawns the WebRTC message forwarding task.
    DcOpened {
        /// Browser identity key for the peer whose DC just opened.
        browser_identity: String,
        /// Offer generation owned by the channel that emitted this event.
        generation: u64,
    },

    /// A bounded WebRTC ingress queue filled up for a browser peer.
    ///
    /// Indicates the Hub is no longer keeping up with inbound frames from that
    /// peer. The channel should be cleaned up so the browser reconnects and
    /// re-synchronizes state from a clean baseline.
    WebRtcIngressBackpressure {
        /// Browser identity for the overloaded WebRTC connection.
        browser_identity: String,
        /// Queue/source label for diagnostics.
        source: &'static str,
    },

    /// Lua timer has fired (one-shot or repeating iteration).
    ///
    /// Spawned tokio tasks send this after `tokio::time::sleep()` completes.
    /// The handler looks up the callback key in the timer registry.
    TimerFired {
        /// Unique timer ID (e.g. `"timer_0"`).
        timer_id: String,
    },

    /// ActionCable channel message from a forwarding task.
    ///
    /// One forwarding task per channel reads from `ChannelHandle.message_rx`
    /// and sends this event for each received message.
    AcChannelMessage {
        /// Channel ID for callback lookup.
        channel_id: String,
        /// Raw JSON message from the ActionCable channel.
        message: serde_json::Value,
    },

    /// WebRTC DataChannel message from a forwarding task.
    ///
    /// One forwarding task per peer reads from `recv_rx` and sends this
    /// event for each received message.
    WebRtcMessage {
        /// Browser identity key for the peer that sent this message.
        browser_identity: String,
        /// Decrypted message payload bytes.
        payload: Vec<u8>,
    },

    /// Outgoing signaling envelope from a WebRTC registry-owned queue forwarder.
    WebRtcOutgoingSignal(crate::channel::webrtc::OutgoingSignal),

    /// Stream multiplexer frame from a WebRTC registry-owned queue forwarder.
    WebRtcStreamFrame(crate::channel::webrtc::StreamIncoming),

    /// User file watch event from a blocking forwarder task.
    ///
    /// One forwarder per `watch.directory()` call reads from the `notify`
    /// crate's `std::sync::mpsc::Receiver` and sends classified events.
    UserFileWatch {
        /// Watch ID for callback lookup (e.g. `"watch_0"`).
        watch_id: String,
        /// Classified file events from the OS watcher.
        events: Vec<FileEvent>,
    },

    /// Periodic cleanup tick from a spawned interval task.
    ///
    /// Fires every 5 seconds. Handles WebRTC connection cleanup
    /// (timeout/disconnect checks) and safety-net queue drains for
    /// stream frames and PTY observers.
    CleanupTick,

    // =========================================================================
    // Lua primitive events — sent directly from Lua closures via HubEventSender
    // =========================================================================
    /// WebRTC send request from a Lua callback.
    WebRtcSend(WebRtcSendRequest),

    /// TUI send request from a Lua callback.
    TuiSend(TuiSendRequest),

    /// PTY operation request from a Lua callback.
    LuaPtyRequest(PtyRequest),

    /// Hub operation request from a Lua callback.
    LuaHubRequest(HubRequest),

    /// Parent-hub operation requested by a plugin worker callback.
    PluginWorkerParentRequest(WorkerParentRequest),

    /// Connection operation request from a Lua callback.
    LuaConnectionRequest(ConnectionRequest),

    /// Worktree operation request from a Lua callback.
    LuaWorktreeRequest(WorktreeRequest),

    /// ActionCable operation request from a Lua callback.
    LuaActionCableRequest(ActionCableRequest),

    /// Hub client operation request from a Lua callback.
    LuaHubClientRequest(HubClientRequest),

    /// Incoming JSON message from a remote hub via outgoing socket client.
    HubClientMessage {
        /// Connection ID for callback lookup.
        connection_id: String,
        /// JSON message from the remote hub.
        message: serde_json::Value,
    },

    /// Remote hub connection disconnected (EOF or error).
    HubClientDisconnected {
        /// Connection ID that disconnected.
        connection_id: String,
    },

    /// Web push notification request from a Lua callback.
    ///
    /// Sent from Lua's `push.send()` with a JSON payload containing
    /// notification fields (kind, title, body, url, icon, tag, data).
    /// The Hub merges defaults (id, hubId, createdAt) and broadcasts
    /// to all subscribed browsers.
    LuaPushRequest {
        /// Notification payload from Lua (must include at least `kind`).
        payload: serde_json::Value,
    },

    /// Browser push-notification control routed through Lua command dispatch.
    ///
    /// Sent by Lua's `push.control(peer_id, command)` from hub command
    /// handlers. Rust still owns push subscription persistence and VAPID
    /// mechanics, but browser-originated commands pass through `client.lua`
    /// first like other hub UI commands.
    BrowserPushControl {
        browser_identity: String,
        payload: serde_json::Value,
    },

    /// Stale push subscriptions to remove (410 Gone from push service).
    ///
    /// Sent from the async web push broadcast task when subscriptions expire.
    PushSubscriptionsExpired {
        /// Browser identity keys whose subscriptions returned 410 Gone.
        identities: Vec<String>,
    },

    /// URL readiness probe requested by a plugin completed.
    UrlProbeReady {
        connector_session_uuid: String,
        parent_session_uuid: String,
        url: String,
        ready: bool,
        error: Option<String>,
    },

    /// Plugin command preparation requested from Lua completed.
    PluginCommandPrepared {
        /// Plugin-scoped request token from the original request.
        request_id: String,
        /// Resolved executable path when preparation succeeded.
        command: Option<String>,
        /// Optional config file path written for the command.
        config_path: Option<String>,
        /// Opaque plugin-owned context from the original request.
        context: serde_json::Value,
        /// Stable machine-readable error kind when preparation failed.
        error_kind: Option<String>,
        /// Error message when preparation failed.
        error: Option<String>,
    },

    /// One-shot plugin command gate completed.
    CommandGateCompleted {
        /// Plugin-scoped request token from the original request.
        request_id: String,
        /// Opaque plugin-owned metadata from the original request.
        metadata: serde_json::Value,
        /// Opaque plugin-owned context from the original request.
        context: serde_json::Value,
        /// Whether the command exited successfully before timeout.
        success: bool,
        /// Process exit status when available.
        exit_status: Option<i32>,
        /// Bounded stdout tail.
        stdout_tail: String,
        /// Bounded stderr tail.
        stderr_tail: String,
        /// True when captured output exceeded the tail bound.
        output_truncated: bool,
        /// Stable machine-readable error kind when the gate failed.
        error_kind: Option<String>,
        /// Human-readable error message when the gate failed.
        error: Option<String>,
        /// Elapsed runtime in milliseconds.
        duration_ms: u128,
    },

    // =========================================================================
    // Socket IPC events — Unix domain socket client connections
    // =========================================================================
    /// A new socket client has connected.
    ///
    /// Sent from the socket server accept loop. The Hub stores the connection
    /// and notifies Lua via the socket client_connected callback.
    SocketClientConnected {
        /// Unique identifier for this socket client (e.g., "socket:a1b2c3").
        client_id: String,
        /// Connection handle for sending frames back to this client.
        conn: SocketClientConn,
    },

    /// A socket client has disconnected (EOF or error).
    SocketClientDisconnected {
        /// Client identifier.
        client_id: String,
    },

    /// JSON message from a socket client.
    ///
    /// Routed through Lua's socket message callback, which delegates
    /// to the shared `client.lua` protocol (same as TUI and WebRTC).
    SocketMessage {
        /// Client identifier.
        client_id: String,
        /// JSON message payload.
        msg: serde_json::Value,
    },

    /// Socket send request from a Lua callback.
    ///
    /// Lua's `socket.send(client_id, msg)` pushes this event.
    SocketSend(crate::lua::primitives::socket::SocketSendRequest),

    /// Authorized Streamable HTTP MCP request from the Hub-owned listener.
    ///
    /// The HTTP layer already bound a caller credential. The event loop must
    /// dispatch Lua MCP with that caller only.
    McpHttpRequest {
        /// Authorized caller identity.
        caller: crate::mcp_http::McpCaller,
        /// MCP method name such as `tools/list`.
        method: String,
        /// JSON-RPC params object.
        params: serde_json::Value,
        /// Reply channel for the waiting HTTP request.
        reply: crate::mcp_http::McpHttpReplyTx,
    },

    /// A queued message was successfully delivered to an agent PTY.
    ///
    /// Sent from the message delivery task after probe succeeded and
    /// message was injected. Can be used by Lua for delivery confirmation.
    MessageDelivered {
        /// Length of the delivered message in bytes.
        message_len: usize,
    },

    // =========================================================================
    // Session Process Events
    // =========================================================================
    /// A per-session process has exited or disconnected.
    ///
    /// Sent by the session reader thread when the session socket closes
    /// (child process exited) or the reader detects a `ProcessExited` frame.
    /// The hub routes this to the appropriate session's `PtyEvent` broadcast
    /// channel and notifies Lua via the `process_exited` hook.
    SessionProcessExited {
        /// Session UUID identifying the session.
        session_uuid: String,
        /// Exit code, or `None` if killed by signal or socket EOF.
        exit_code: Option<i32>,
    },

    /// A background reconnect task completed successfully.
    ///
    /// Sent by `spawn_blocking` after `SessionConnection::connect_and_seed()`
    /// succeeds. The hub loop validates the generation, installs the reader
    /// thread, and publishes the new connection into the shared mutex.
    SessionReconnectReady {
        /// Session UUID that reconnected.
        session_uuid: String,
        /// Generation counter to detect stale completions.
        generation: u64,
        /// Fresh connection (reader not yet installed).
        conn: crate::session::connection::SessionConnection,
        /// Metadata fetched during reconnect handshake.
        metadata: crate::session::protocol::SessionMetadata,
    },

    /// A session was removed from `HandleCache` by `hub.unregister_session()`.
    ///
    /// The Hub removes any per-session routing state whose `session_uuid`
    /// matches so in-memory indexes do not grow without bound when sessions cycle.
    SessionUnregistered {
        /// The session UUID that was removed.
        session_uuid: String,
    },

    /// Async worktree deletion completed.
    ///
    /// Sent by the `spawn_blocking` task in the `WorktreeRequest::Delete`
    /// handler after `delete_worktree_by_path` finishes (success or failure).
    /// The main loop removes the worktree from `HandleCache` on success so
    /// `worktree.list()` / `worktree.find()` reflect the deletion immediately.
    WorktreeDeleteCompleted {
        /// Filesystem path of the deleted worktree (retained for logging context).
        path: String,
        /// Branch name that was deleted (for logging).
        branch: String,
        /// `Ok(())` on success, `Err(message)` on failure.
        result: Result<(), String>,
    },

    /// Async WebRTC offer negotiation completed.
    ///
    /// Sent by the spawned runner after SDP negotiation and answer encryption.
    /// The WebRTC registry owns stale generation checks, channel retention, and
    /// cleanup when this completion returns to the hub thread.
    WebRtcOfferNegotiated(crate::worker::webrtc::WebRtcOfferCompletion),

    /// Hub-owned snapshot provider finished a WebRTC recovery request.
    WebRtcRecoverySnapshotReady {
        request: crate::worker::webrtc::WebRtcRecoverySnapshotRequest,
        result: crate::worker::webrtc::WebRtcRecoverySnapshotResult,
    },
}

impl HubEvent {
    #[must_use]
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::HttpResponse(_) => "http_response",
            Self::WebSocketEvent(_) => "websocket_event",
            Self::PtyNotification(_) => "pty_notification",
            Self::PtyOscEvent { event, .. } => match event {
                crate::agent::pty::PtyEvent::TitleChanged(_) => "pty_osc_title",
                crate::agent::pty::PtyEvent::CwdChanged(_) => "pty_osc_cwd",
                crate::agent::pty::PtyEvent::PromptMark(_) => "pty_osc_prompt",
                crate::agent::pty::PtyEvent::CursorVisibilityChanged(_) => "pty_osc_cursor",
                _ => "pty_osc_event",
            },
            Self::PtyOutput { .. } => "pty_output",
            Self::PtyProcessExited { .. } => "pty_process_exited",
            Self::SessionIo(_) => "session_io",
            Self::DropPendingSessionIoSnapshot { .. } => "drop_pending_session_io_snapshot",
            Self::ClientWorkerControl(_) => "client_worker_control",
            Self::DcOpened { .. } => "dc_opened",
            Self::WebRtcIngressBackpressure { .. } => "webrtc_ingress_backpressure",
            Self::TimerFired { .. } => "timer_fired",
            Self::AcChannelMessage { .. } => "ac_channel_message",
            Self::WebRtcMessage { .. } => "webrtc_message",
            Self::WebRtcOutgoingSignal(_) => "webrtc_outgoing_signal",
            Self::WebRtcStreamFrame(_) => "webrtc_stream_frame",
            Self::UserFileWatch { .. } => "user_file_watch",
            Self::CleanupTick => "cleanup_tick",
            Self::WebRtcSend(_) => "webrtc_send",
            Self::TuiSend(_) => "tui_send",
            Self::LuaPtyRequest(_) => "lua_pty_request",
            Self::LuaHubRequest(_) => "lua_hub_request",
            Self::PluginWorkerParentRequest(_) => "plugin_worker_parent_request",
            Self::LuaConnectionRequest(_) => "lua_connection_request",
            Self::LuaWorktreeRequest(_) => "lua_worktree_request",
            Self::LuaActionCableRequest(_) => "lua_action_cable_request",
            Self::LuaHubClientRequest(_) => "lua_hub_client_request",
            Self::HubClientMessage { .. } => "hub_client_message",
            Self::HubClientDisconnected { .. } => "hub_client_disconnected",
            Self::LuaPushRequest { .. } => "lua_push_request",
            Self::BrowserPushControl { .. } => "browser_push_control",
            Self::PushSubscriptionsExpired { .. } => "push_subscriptions_expired",
            Self::UrlProbeReady { .. } => "url_probe_ready",
            Self::PluginCommandPrepared { .. } => "plugin_command_prepared",
            Self::CommandGateCompleted { .. } => "command_gate_completed",
            Self::SocketClientConnected { .. } => "socket_client_connected",
            Self::SocketClientDisconnected { .. } => "socket_client_disconnected",
            Self::SocketMessage { .. } => "socket_message",
            Self::SocketSend(_) => "socket_send",
            Self::McpHttpRequest { .. } => "mcp_http_request",
            Self::MessageDelivered { .. } => "message_delivered",
            Self::SessionProcessExited { .. } => "session_process_exited",
            Self::SessionReconnectReady { .. } => "session_reconnect_ready",
            Self::SessionUnregistered { .. } => "session_unregistered",
            Self::WorktreeDeleteCompleted { .. } => "worktree_delete_completed",
            Self::WebRtcOfferNegotiated(_) => "webrtc_offer_negotiated",
            Self::WebRtcRecoverySnapshotReady { .. } => "webrtc_recovery_snapshot_ready",
        }
    }

    #[must_use]
    pub(crate) fn is_high_priority(&self) -> bool {
        match self {
            Self::LuaPtyRequest(request) => Self::is_terminal_control_request(request),
            // AcChannelMessage is not high-priority (cold-turkey boundary change).
            // Synchronous plugin Lua handlers must not starve terminal control
            // and mode frames (e.g. ModeChanged carrying mouse_mode) required for
            // reattach fidelity of nested TUIs. See worker-actor-contracts.md and
            // the AC cross-VM conversion.
            Self::LuaActionCableRequest(_)
            | Self::WebRtcOfferNegotiated(_)
            | Self::WebRtcOutgoingSignal(_)
            | Self::DcOpened { .. }
            | Self::WebRtcMessage { .. }
            | Self::WebRtcIngressBackpressure { .. }
            | Self::BrowserPushControl { .. }
            | Self::WebRtcSend(_)
            | Self::CleanupTick
            | Self::DropPendingSessionIoSnapshot { .. }
            | Self::PluginWorkerParentRequest(_)
            | Self::SessionProcessExited { .. }
            | Self::SessionUnregistered { .. }
            | Self::SessionReconnectReady { .. }
            | Self::WorktreeDeleteCompleted { .. }
            | Self::SocketClientConnected { .. }
            | Self::SocketClientDisconnected { .. }
            | Self::SocketMessage { .. }
            | Self::SocketSend(_)
            | Self::McpHttpRequest { .. } => true,
            _ => false,
        }
    }

    #[must_use]
    fn is_terminal_control_request(request: &PtyRequest) -> bool {
        match request {
            PtyRequest::SubscribeBrowserTerminal(_)
            | PtyRequest::SubscribeTuiTerminal(_)
            | PtyRequest::SubscribeSocketTerminal(_)
            | PtyRequest::StopTerminalSubscription { .. }
            | PtyRequest::RefreshSnapshot(_)
            | PtyRequest::WritePty { .. }
            | PtyRequest::ResizePty { .. } => true,
            PtyRequest::SpawnNotificationWatcher { .. } => false,
        }
    }

    #[must_use]
    fn is_repeatable_under_pressure(&self) -> bool {
        match self {
            Self::PtyOscEvent { .. }
            | Self::TimerFired { .. }
            | Self::WebRtcRecoverySnapshotReady { .. } => true,
            Self::ClientWorkerControl(message) => matches!(
                message,
                crate::worker::hub_control::HubControlMessage::RequestSnapshot { .. }
                    | crate::worker::hub_control::HubControlMessage::Backpressure(_)
                    | crate::worker::hub_control::HubControlMessage::TransportBackpressure { .. }
            ),
            _ => false,
        }
    }

    #[must_use]
    fn coalescing_key(&self) -> Option<HubEventCoalescingKey> {
        match self {
            Self::PtyOscEvent {
                session_uuid,
                event,
                ..
            } => match event {
                crate::agent::pty::PtyEvent::TitleChanged(_) => {
                    Some(HubEventCoalescingKey::PtyOsc {
                        session_uuid: session_uuid.clone(),
                        kind: "title",
                    })
                }
                crate::agent::pty::PtyEvent::CwdChanged(_) => Some(HubEventCoalescingKey::PtyOsc {
                    session_uuid: session_uuid.clone(),
                    kind: "cwd",
                }),
                crate::agent::pty::PtyEvent::CursorVisibilityChanged(_) => {
                    Some(HubEventCoalescingKey::PtyOsc {
                        session_uuid: session_uuid.clone(),
                        kind: "cursor",
                    })
                }
                _ => None,
            },
            Self::TimerFired { timer_id } => Some(HubEventCoalescingKey::Timer {
                timer_id: timer_id.clone(),
            }),
            Self::ClientWorkerControl(message) => match message {
                crate::worker::hub_control::HubControlMessage::RequestSnapshot {
                    client_id,
                    session_uuid,
                    subscription_id,
                    rows,
                    cols,
                } => Some(HubEventCoalescingKey::SnapshotRequest {
                    client_id: client_id.clone(),
                    session_uuid: session_uuid.clone(),
                    subscription_id: subscription_id.clone(),
                    rows: *rows,
                    cols: *cols,
                }),
                crate::worker::hub_control::HubControlMessage::Backpressure(pressure) => {
                    Some(HubEventCoalescingKey::Backpressure {
                        source: pressure.source,
                        session_uuid: pressure.session_uuid.clone(),
                        client_id: pressure.client_id.clone(),
                    })
                }
                crate::worker::hub_control::HubControlMessage::TransportBackpressure {
                    pressure,
                    ..
                } => Some(HubEventCoalescingKey::Backpressure {
                    source: pressure.source,
                    session_uuid: pressure.session_uuid.clone(),
                    client_id: pressure.client_id.clone(),
                }),
                _ => None,
            },
            _ => None,
        }
    }

    #[must_use]
    pub(crate) fn approx_size_bytes(&self) -> usize {
        const BASE: usize = 32;
        match self {
            Self::WebRtcMessage {
                browser_identity,
                payload,
            } => BASE + browser_identity.len() + payload.len(),
            Self::WebRtcStreamFrame(frame) => {
                BASE + frame.browser_identity.len() + frame.payload.len()
            }
            Self::SocketMessage { client_id, msg } => {
                BASE + client_id.len() + msg.to_string().len()
            }
            Self::HubClientMessage {
                connection_id,
                message,
            } => BASE + connection_id.len() + message.to_string().len(),
            Self::AcChannelMessage {
                channel_id,
                message,
            } => BASE + channel_id.len() + message.to_string().len(),
            Self::UserFileWatch { watch_id, events } => BASE + watch_id.len() + (events.len() * 48),
            Self::PtyOutput {
                session_uuid,
                session_name,
                data,
            } => BASE + session_uuid.len() + session_name.len() + data.len(),
            Self::PushSubscriptionsExpired { identities } => {
                BASE + identities
                    .iter()
                    .map(std::string::String::len)
                    .sum::<usize>()
            }
            Self::LuaPushRequest { payload } => BASE + payload.to_string().len(),
            _ => BASE,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HubEventCoalescingKey {
    PtyOsc {
        session_uuid: String,
        kind: &'static str,
    },
    Timer {
        timer_id: String,
    },
    SnapshotRequest {
        client_id: crate::client::ClientId,
        session_uuid: String,
        subscription_id: String,
        rows: u16,
        cols: u16,
    },
    Backpressure {
        source: &'static str,
        session_uuid: Option<String>,
        client_id: Option<crate::client::ClientId>,
    },
}

#[derive(Clone, Debug, Default)]
pub(crate) struct HubEventTypeSnapshot {
    pub enqueue_ok: u64,
    pub enqueue_failed: u64,
    pub dequeue: u64,
    pub pending: usize,
    pub pending_high_water: usize,
    pub bytes_pending: usize,
    pub bytes_high_water: usize,
    pub handler_time_total_ns: u64,
    pub handler_time_max_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct HubEventMetricsSnapshot {
    pub enqueue_ok_total: u64,
    pub enqueue_failed_total: u64,
    pub dequeue_total: u64,
    pub pending_total: usize,
    pub pending_high_water_total: usize,
    pub bytes_pending_total: usize,
    pub bytes_high_water_total: usize,
    pub handler_time_total_ns: u64,
    pub handler_time_max_ns: u64,
    pub by_type: BTreeMap<&'static str, HubEventTypeSnapshot>,
    pub counters: BTreeMap<&'static str, u64>,
    pub spans: BTreeMap<&'static str, HubEventSpanSnapshot>,
    pub slow_samples: Vec<HubEventSlowSample>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct HubEventSpanSnapshot {
    pub count: u64,
    pub total_ns: u64,
    pub max_ns: u64,
    pub slow_count: u64,
    pub bytes_total: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct HubEventSlowSample {
    pub span: &'static str,
    pub elapsed_us: u64,
    pub bytes: usize,
    pub label: String,
}

#[derive(Debug, Default)]
struct HubEventTypeMetrics {
    enqueue_ok: u64,
    enqueue_failed: u64,
    dequeue: u64,
    pending: usize,
    pending_high_water: usize,
    bytes_pending: usize,
    bytes_high_water: usize,
    handler_time_total_ns: u64,
    handler_time_max_ns: u64,
}

#[derive(Debug, Default)]
struct HubEventSpanMetrics {
    count: u64,
    total_ns: u64,
    max_ns: u64,
    slow_count: u64,
    bytes_total: u64,
}

#[derive(Debug, Default)]
pub(crate) struct HubEventMetrics {
    enqueue_ok_total: AtomicU64,
    enqueue_failed_total: AtomicU64,
    dequeue_total: AtomicU64,
    pending_total: AtomicUsize,
    pending_high_water_total: AtomicUsize,
    bytes_pending_total: AtomicUsize,
    bytes_high_water_total: AtomicUsize,
    handler_time_total_ns: AtomicU64,
    handler_time_max_ns: AtomicU64,
    by_type: Mutex<BTreeMap<&'static str, HubEventTypeMetrics>>,
    counters: Mutex<BTreeMap<&'static str, u64>>,
    spans: Mutex<BTreeMap<&'static str, HubEventSpanMetrics>>,
    slow_samples: Mutex<VecDeque<HubEventSlowSample>>,
}

impl HubEventMetrics {
    pub(crate) const SLOW_SAMPLE_LIMIT: usize = 32;
    pub(crate) const SLOW_SAMPLE_LOG_LIMIT: usize = 8;
    const SLOW_LABEL_LIMIT: usize = 24;

    fn bump_high_water(atom: &AtomicUsize, value: usize) {
        let mut current = atom.load(Ordering::Relaxed);
        while value > current {
            match atom.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(updated) => current = updated,
            }
        }
    }

    fn bump_high_water_u64(atom: &AtomicU64, value: u64) {
        let mut current = atom.load(Ordering::Relaxed);
        while value > current {
            match atom.compare_exchange(current, value, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(updated) => current = updated,
            }
        }
    }

    pub(crate) fn record_enqueue(&self, kind: &'static str, bytes: usize) {
        self.enqueue_ok_total.fetch_add(1, Ordering::Relaxed);
        let pending = self.pending_total.fetch_add(1, Ordering::Relaxed) + 1;
        Self::bump_high_water(&self.pending_high_water_total, pending);

        let bytes_pending = self.bytes_pending_total.fetch_add(bytes, Ordering::Relaxed) + bytes;
        Self::bump_high_water(&self.bytes_high_water_total, bytes_pending);

        if let Ok(mut map) = self.by_type.lock() {
            let entry = map.entry(kind).or_default();
            entry.enqueue_ok += 1;
            entry.pending += 1;
            entry.pending_high_water = entry.pending_high_water.max(entry.pending);
            entry.bytes_pending += bytes;
            entry.bytes_high_water = entry.bytes_high_water.max(entry.bytes_pending);
        }
    }

    pub(crate) fn record_enqueue_failed(&self, kind: &'static str, bytes: usize) {
        self.enqueue_failed_total.fetch_add(1, Ordering::Relaxed);
        self.pending_total.fetch_sub(1, Ordering::Relaxed);
        self.bytes_pending_total.fetch_sub(bytes, Ordering::Relaxed);

        if let Ok(mut map) = self.by_type.lock() {
            let entry = map.entry(kind).or_default();
            entry.enqueue_failed += 1;
            entry.pending = entry.pending.saturating_sub(1);
            entry.bytes_pending = entry.bytes_pending.saturating_sub(bytes);
        }
    }

    pub(crate) fn record_dequeue(&self, kind: &'static str, bytes: usize) {
        self.dequeue_total.fetch_add(1, Ordering::Relaxed);
        self.pending_total.fetch_sub(1, Ordering::Relaxed);
        self.bytes_pending_total.fetch_sub(bytes, Ordering::Relaxed);

        if let Ok(mut map) = self.by_type.lock() {
            let entry = map.entry(kind).or_default();
            entry.dequeue += 1;
            entry.pending = entry.pending.saturating_sub(1);
            entry.bytes_pending = entry.bytes_pending.saturating_sub(bytes);
        }
    }

    pub(crate) fn record_handler_time(&self, kind: &'static str, elapsed: std::time::Duration) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        self.handler_time_total_ns
            .fetch_add(nanos, Ordering::Relaxed);
        Self::bump_high_water_u64(&self.handler_time_max_ns, nanos);

        if let Ok(mut map) = self.by_type.lock() {
            let entry = map.entry(kind).or_default();
            entry.handler_time_total_ns = entry.handler_time_total_ns.saturating_add(nanos);
            entry.handler_time_max_ns = entry.handler_time_max_ns.max(nanos);
        }
    }

    pub(crate) fn record_counter(&self, name: &'static str, amount: u64) {
        if let Ok(mut map) = self.counters.lock() {
            let entry = map.entry(name).or_default();
            *entry = entry.saturating_add(amount);
        }
    }

    pub(crate) fn record_high_water(&self, name: &'static str, value: u64) {
        if let Ok(mut map) = self.counters.lock() {
            let entry = map.entry(name).or_default();
            *entry = (*entry).max(value);
        }
    }

    pub(crate) fn record_span(
        &self,
        span: &'static str,
        elapsed: std::time::Duration,
        bytes: usize,
    ) {
        self.record_span_labeled(span, elapsed, bytes, "");
    }

    pub(crate) fn record_span_labeled(
        &self,
        span: &'static str,
        elapsed: std::time::Duration,
        bytes: usize,
        label: &str,
    ) {
        self.record_span_with_threshold(span, elapsed, bytes, std::time::Duration::MAX, label);
    }

    pub(crate) fn record_span_with_threshold(
        &self,
        span: &'static str,
        elapsed: std::time::Duration,
        bytes: usize,
        slow_threshold: std::time::Duration,
        label: &str,
    ) {
        let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
        let is_slow = elapsed >= slow_threshold;
        if let Ok(mut map) = self.spans.lock() {
            let entry = map.entry(span).or_default();
            entry.count = entry.count.saturating_add(1);
            entry.total_ns = entry.total_ns.saturating_add(nanos);
            entry.max_ns = entry.max_ns.max(nanos);
            entry.bytes_total = entry.bytes_total.saturating_add(bytes as u64);
            if is_slow {
                entry.slow_count = entry.slow_count.saturating_add(1);
            }
        }
        if is_slow {
            self.record_slow_sample(span, elapsed, bytes, label);
        }
    }

    fn record_slow_sample(
        &self,
        span: &'static str,
        elapsed: std::time::Duration,
        bytes: usize,
        label: &str,
    ) {
        let mut capped = label
            .chars()
            .take(Self::SLOW_LABEL_LIMIT)
            .collect::<String>();
        if capped.is_empty() {
            capped = "-".to_string();
        }
        let sample = HubEventSlowSample {
            span,
            elapsed_us: elapsed.as_micros().min(u64::MAX as u128) as u64,
            bytes,
            label: capped,
        };
        if let Ok(mut samples) = self.slow_samples.lock() {
            if samples.len() >= Self::SLOW_SAMPLE_LIMIT {
                samples.pop_front();
            }
            samples.push_back(sample);
        }
    }

    #[must_use]
    pub(crate) fn snapshot(&self) -> HubEventMetricsSnapshot {
        let by_type = if let Ok(map) = self.by_type.lock() {
            map.iter()
                .map(|(k, v)| {
                    (
                        *k,
                        HubEventTypeSnapshot {
                            enqueue_ok: v.enqueue_ok,
                            enqueue_failed: v.enqueue_failed,
                            dequeue: v.dequeue,
                            pending: v.pending,
                            pending_high_water: v.pending_high_water,
                            bytes_pending: v.bytes_pending,
                            bytes_high_water: v.bytes_high_water,
                            handler_time_total_ns: v.handler_time_total_ns,
                            handler_time_max_ns: v.handler_time_max_ns,
                        },
                    )
                })
                .collect()
        } else {
            BTreeMap::new()
        };
        let counters = self
            .counters
            .lock()
            .map(|map| map.clone())
            .unwrap_or_default();
        let spans = self
            .spans
            .lock()
            .map(|map| {
                map.iter()
                    .map(|(k, v)| {
                        (
                            *k,
                            HubEventSpanSnapshot {
                                count: v.count,
                                total_ns: v.total_ns,
                                max_ns: v.max_ns,
                                slow_count: v.slow_count,
                                bytes_total: v.bytes_total,
                            },
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut slow_samples = self
            .slow_samples
            .lock()
            .map(|samples| samples.iter().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        slow_samples.sort_by(|a, b| b.elapsed_us.cmp(&a.elapsed_us));
        slow_samples.truncate(Self::SLOW_SAMPLE_LOG_LIMIT);

        HubEventMetricsSnapshot {
            enqueue_ok_total: self.enqueue_ok_total.load(Ordering::Relaxed),
            enqueue_failed_total: self.enqueue_failed_total.load(Ordering::Relaxed),
            dequeue_total: self.dequeue_total.load(Ordering::Relaxed),
            pending_total: self.pending_total.load(Ordering::Relaxed),
            pending_high_water_total: self.pending_high_water_total.load(Ordering::Relaxed),
            bytes_pending_total: self.bytes_pending_total.load(Ordering::Relaxed),
            bytes_high_water_total: self.bytes_high_water_total.load(Ordering::Relaxed),
            handler_time_total_ns: self.handler_time_total_ns.load(Ordering::Relaxed),
            handler_time_max_ns: self.handler_time_max_ns.load(Ordering::Relaxed),
            by_type,
            counters,
            spans,
            slow_samples,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct HubEventTx {
    inner: mpsc::Sender<HubEvent>,
    high_priority: mpsc::Sender<HubEvent>,
    metrics: Arc<HubEventMetrics>,
    pending_coalesced: Arc<Mutex<HashSet<HubEventCoalescingKey>>>,
}

impl HubEventTx {
    #[must_use]
    pub(crate) fn new(inner: mpsc::Sender<HubEvent>, metrics: Arc<HubEventMetrics>) -> Self {
        Self {
            high_priority: inner.clone(),
            inner,
            metrics,
            pending_coalesced: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    #[must_use]
    pub(crate) fn new_with_priority(
        inner: mpsc::Sender<HubEvent>,
        high_priority: mpsc::Sender<HubEvent>,
        metrics: Arc<HubEventMetrics>,
    ) -> Self {
        Self {
            inner,
            high_priority,
            metrics,
            pending_coalesced: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub(crate) fn send(&self, event: HubEvent) -> Result<(), mpsc::error::SendError<HubEvent>> {
        let kind = event.kind();
        let bytes = event.approx_size_bytes();
        let coalescing_key = event.coalescing_key();
        if let Some(key) = &coalescing_key {
            if let Ok(mut pending) = self.pending_coalesced.lock() {
                if !pending.insert(key.clone()) {
                    // This is intentionally drop-newest while the first event
                    // remains queued. Producers should pre-coalesce
                    // latest-value payloads before they reach the hub event
                    // lane when exact last-write-wins delivery matters.
                    self.metrics.record_counter("hub_event.coalesced", 1);
                    match kind {
                        "client_worker_control" => self
                            .metrics
                            .record_counter("hub_event.coalesced.client_worker_control", 1),
                        "pty_osc_title" | "pty_osc_cwd" | "pty_osc_cursor" => self
                            .metrics
                            .record_counter("hub_event.coalesced.pty_osc", 1),
                        "timer_fired" => self
                            .metrics
                            .record_counter("hub_event.coalesced.timer_fired", 1),
                        _ => {}
                    }
                    return Ok(());
                }
            }
        }
        self.metrics.record_enqueue(kind, bytes);
        let tx = if event.is_high_priority() {
            &self.high_priority
        } else {
            &self.inner
        };
        match tx.try_send(event) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(event)) => {
                self.clear_coalescing_key(coalescing_key.as_ref());
                self.metrics.record_enqueue_failed(kind, bytes);
                self.metrics.record_counter("hub_event.queue_full", 1);
                if event.is_repeatable_under_pressure() {
                    self.metrics
                        .record_counter("hub_event.repeatable_rejected", 1);
                    match kind {
                        "client_worker_control" => self.metrics.record_counter(
                            "hub_event.repeatable_rejected.client_worker_control",
                            1,
                        ),
                        "pty_osc_title" | "pty_osc_cwd" | "pty_osc_prompt" | "pty_osc_cursor"
                        | "pty_osc_event" => self
                            .metrics
                            .record_counter("hub_event.repeatable_rejected.pty_osc", 1),
                        "timer_fired" => self
                            .metrics
                            .record_counter("hub_event.repeatable_rejected.timer_fired", 1),
                        "webrtc_recovery_snapshot_ready" => self
                            .metrics
                            .record_counter("hub_event.repeatable_rejected.recovery_snapshot", 1),
                        _ => {}
                    }
                }
                Err(mpsc::error::SendError(event))
            }
            Err(mpsc::error::TrySendError::Closed(event)) => {
                self.clear_coalescing_key(coalescing_key.as_ref());
                self.metrics.record_enqueue_failed(kind, bytes);
                Err(mpsc::error::SendError(event))
            }
        }
    }

    pub(crate) fn mark_dequeued(&self, event: &HubEvent) {
        let key = event.coalescing_key();
        self.clear_coalescing_key(key.as_ref());
    }

    fn clear_coalescing_key(&self, key: Option<&HubEventCoalescingKey>) {
        let Some(key) = key else {
            return;
        };
        if let Ok(mut pending) = self.pending_coalesced.lock() {
            pending.remove(key);
        }
    }
}

impl From<mpsc::Sender<HubEvent>> for HubEventTx {
    fn from(inner: mpsc::Sender<HubEvent>) -> Self {
        Self::new(inner, Arc::new(HubEventMetrics::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn kind_splits_pty_osc_subtypes() {
        let title = HubEvent::PtyOscEvent {
            session_uuid: "a".to_string(),
            session_name: "s".to_string(),
            event: crate::agent::pty::PtyEvent::title_changed("hello"),
        };
        assert_eq!(title.kind(), "pty_osc_title");

        let cwd = HubEvent::PtyOscEvent {
            session_uuid: "a".to_string(),
            session_name: "s".to_string(),
            event: crate::agent::pty::PtyEvent::cwd_changed("/tmp"),
        };
        assert_eq!(cwd.kind(), "pty_osc_cwd");
    }

    #[test]
    fn metrics_snapshot_includes_handler_timing() {
        let metrics = HubEventMetrics::default();
        metrics.record_enqueue("pty_osc_title", 32);
        metrics.record_dequeue("pty_osc_title", 32);
        metrics.record_handler_time("pty_osc_title", Duration::from_micros(250));

        let snapshot = metrics.snapshot();
        let kind = snapshot.by_type.get("pty_osc_title").unwrap();
        assert_eq!(kind.dequeue, 1);
        assert_eq!(kind.handler_time_total_ns, 250_000);
        assert_eq!(kind.handler_time_max_ns, 250_000);
        assert_eq!(snapshot.handler_time_total_ns, 250_000);
        assert_eq!(snapshot.handler_time_max_ns, 250_000);
    }

    #[test]
    fn metrics_snapshot_includes_counters_spans_and_bounded_slow_samples() {
        let metrics = HubEventMetrics::default();
        metrics.record_counter("webrtc_send.queued", 2);
        metrics.record_counter("webrtc_send.queued", 3);
        metrics.record_high_water("queue.batch_hwm", 5);
        metrics.record_high_water("queue.batch_hwm", 3);

        metrics.record_span("webrtc_message.dc_ping", Duration::from_micros(200), 64);
        for i in 0..40 {
            metrics.record_span_with_threshold(
                "socket_message.lua",
                Duration::from_millis(60 + i),
                i as usize,
                Duration::from_millis(50),
                "peer-abcdefghijklmnopqrstuvwxyz",
            );
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.counters["webrtc_send.queued"], 5);
        assert_eq!(snapshot.counters["queue.batch_hwm"], 5);
        let span = snapshot.spans.get("socket_message.lua").unwrap();
        assert_eq!(span.count, 40);
        assert_eq!(span.slow_count, 40);
        assert_eq!(
            snapshot.slow_samples.len(),
            HubEventMetrics::SLOW_SAMPLE_LOG_LIMIT
        );
        assert!(snapshot
            .slow_samples
            .iter()
            .all(|sample| sample.label.chars().count() <= 24));
        assert_eq!(snapshot.slow_samples[0].elapsed_us, 99_000);
    }

    #[test]
    fn sender_routes_client_control_events_to_priority_queue() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(8);
        let (priority_tx, mut priority_rx) = mpsc::channel(8);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, metrics);

        sender
            .send(HubEvent::PtyOscEvent {
                session_uuid: "sess-1".to_string(),
                session_name: "agent".to_string(),
                event: crate::agent::pty::PtyEvent::title_changed("bulk"),
            })
            .expect("send bulk event");
        sender
            .send(HubEvent::SocketMessage {
                client_id: "socket:test".to_string(),
                msg: serde_json::json!({"type": "ping"}),
            })
            .expect("send priority event");

        assert!(matches!(
            bulk_rx.try_recv(),
            Ok(HubEvent::PtyOscEvent { .. })
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::SocketMessage { .. })
        ));
        assert!(bulk_rx.try_recv().is_err());
        assert!(priority_rx.try_recv().is_err());
    }

    #[test]
    fn sender_coalesces_duplicate_title_updates_until_dequeued() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(8);
        let (priority_tx, mut priority_rx) = mpsc::channel(8);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, Arc::clone(&metrics));

        sender
            .send(HubEvent::PtyOscEvent {
                session_uuid: "sess-1".to_string(),
                session_name: "agent".to_string(),
                event: crate::agent::pty::PtyEvent::title_changed("step-1"),
            })
            .expect("send first title");
        sender
            .send(HubEvent::PtyOscEvent {
                session_uuid: "sess-1".to_string(),
                session_name: "agent".to_string(),
                event: crate::agent::pty::PtyEvent::title_changed("step-2"),
            })
            .expect("coalesced second title is not an error");

        assert!(matches!(
            bulk_rx.try_recv(),
            Ok(HubEvent::PtyOscEvent {
                event: crate::agent::pty::PtyEvent::TitleChanged(title),
                ..
            }) if title == "step-1"
        ));
        assert!(bulk_rx.try_recv().is_err());
        assert!(priority_rx.try_recv().is_err());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.counters["hub_event.coalesced"], 1);
        assert_eq!(snapshot.counters["hub_event.coalesced.pty_osc"], 1);
    }

    #[test]
    fn sender_rejects_repeatable_events_when_bulk_lane_is_full() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(1);
        let (priority_tx, mut priority_rx) = mpsc::channel(1);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, Arc::clone(&metrics));

        sender
            .send(HubEvent::PtyOscEvent {
                session_uuid: "sess-1".to_string(),
                session_name: "agent".to_string(),
                event: crate::agent::pty::PtyEvent::title_changed("busy"),
            })
            .expect("prefill bulk lane");

        let result = sender.send(HubEvent::ClientWorkerControl(
            crate::worker::hub_control::HubControlMessage::RequestSnapshot {
                client_id: crate::client::ClientId::Tui,
                session_uuid: "sess-1".to_string(),
                subscription_id: "sub-1".to_string(),
                rows: 24,
                cols: 80,
            },
        ));

        assert!(result.is_err());
        assert!(matches!(
            bulk_rx.try_recv(),
            Ok(HubEvent::PtyOscEvent { .. })
        ));
        assert!(priority_rx.try_recv().is_err());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.enqueue_failed_total, 1);
        assert_eq!(snapshot.counters["hub_event.queue_full"], 1);
        assert_eq!(
            snapshot.counters["hub_event.repeatable_rejected.client_worker_control"],
            1
        );
    }

    #[test]
    fn sender_routes_cleanup_events_to_priority_lane() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(1);
        let (priority_tx, mut priority_rx) = mpsc::channel(1);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, metrics);

        sender
            .send(HubEvent::DropPendingSessionIoSnapshot {
                request_id: "snapshot-1".to_string(),
            })
            .expect("send cleanup event");

        assert!(bulk_rx.try_recv().is_err());
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::DropPendingSessionIoSnapshot { .. })
        ));
    }

    #[test]
    fn sender_routes_terminal_control_lua_pty_requests_to_priority_lane() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(8);
        let (priority_tx, mut priority_rx) = mpsc::channel(8);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, metrics);

        sender
            .send(HubEvent::LuaPtyRequest(
                PtyRequest::SubscribeBrowserTerminal(
                    crate::lua::primitives::pty::BrowserTerminalSubscriptionRequest {
                        peer_id: "browser-priority".to_string(),
                        session_uuid: "sess-priority".to_string(),
                        prefix: None,
                        subscription_id: "sub-priority".to_string(),
                        rows: 24,
                        cols: 80,
                        active_flag: Arc::new(Mutex::new(true)),
                    },
                ),
            ))
            .expect("send browser terminal subscribe");
        sender
            .send(HubEvent::LuaPtyRequest(
                PtyRequest::StopTerminalSubscription {
                    subscription_key: "browser-priority:sess-priority".to_string(),
                },
            ))
            .expect("send terminal stop");
        sender
            .send(HubEvent::LuaPtyRequest(PtyRequest::SubscribeTuiTerminal(
                crate::lua::primitives::pty::TuiTerminalSubscriptionRequest {
                    session_uuid: "sess-priority".to_string(),
                    subscription_id: "tui-sub-priority".to_string(),
                    rows: 24,
                    cols: 80,
                    active_flag: Arc::new(Mutex::new(true)),
                },
            )))
            .expect("send tui terminal subscribe");
        sender
            .send(HubEvent::LuaPtyRequest(
                PtyRequest::SubscribeSocketTerminal(
                    crate::lua::primitives::pty::SocketTerminalSubscriptionRequest {
                        client_id: "socket:priority".to_string(),
                        session_uuid: "sess-priority".to_string(),
                        subscription_id: "socket-sub-priority".to_string(),
                        rows: 24,
                        cols: 80,
                        active_flag: Arc::new(Mutex::new(true)),
                    },
                ),
            ))
            .expect("send socket terminal subscribe");
        sender
            .send(HubEvent::LuaPtyRequest(PtyRequest::ResizePty {
                session_uuid: "sess-priority".to_string(),
                rows: 30,
                cols: 120,
            }))
            .expect("send terminal resize");
        sender
            .send(HubEvent::LuaPtyRequest(
                PtyRequest::SpawnNotificationWatcher {
                    watcher_key: "sess-priority:agent".to_string(),
                    session_uuid: "sess-priority".to_string(),
                    session_name: "agent".to_string(),
                    observe_output: false,
                    event_tx: tokio::sync::broadcast::channel(1).0,
                },
            ))
            .expect("send non-control pty request");

        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(
                PtyRequest::SubscribeBrowserTerminal(_)
            ))
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(
                PtyRequest::StopTerminalSubscription { .. }
            ))
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(PtyRequest::SubscribeTuiTerminal(_)))
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(
                PtyRequest::SubscribeSocketTerminal(_)
            ))
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(PtyRequest::ResizePty { .. }))
        ));
        assert!(priority_rx.try_recv().is_err());
        assert!(matches!(
            bulk_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(
                PtyRequest::SpawnNotificationWatcher { .. }
            ))
        ));
        assert!(bulk_rx.try_recv().is_err());
    }

    #[test]
    fn terminal_control_bypasses_saturated_bulk_lane() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(1);
        let (priority_tx, mut priority_rx) = mpsc::channel(4);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, Arc::clone(&metrics));

        sender
            .send(HubEvent::UserFileWatch {
                watch_id: "bulk-filler".to_string(),
                events: Vec::new(),
            })
            .expect("fill bulk lane");
        sender
            .send(HubEvent::LuaPtyRequest(PtyRequest::WritePty {
                session_uuid: "sess-priority".to_string(),
                data: b"input".to_vec(),
            }))
            .expect("terminal input bypasses saturated bulk lane");
        sender
            .send(HubEvent::LuaPtyRequest(PtyRequest::RefreshSnapshot(
                crate::lua::primitives::pty::RefreshSnapshotRequest {
                    peer_id: "browser-priority".to_string(),
                    session_uuid: "sess-priority".to_string(),
                    subscription_id: "sub-priority".to_string(),
                    rows: 24,
                    cols: 80,
                },
            )))
            .expect("snapshot refresh bypasses saturated bulk lane");

        assert!(matches!(
            bulk_rx.try_recv(),
            Ok(HubEvent::UserFileWatch { .. })
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(PtyRequest::WritePty { .. }))
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(PtyRequest::RefreshSnapshot(_)))
        ));
        assert_eq!(metrics.snapshot().enqueue_failed_total, 0);
    }

    #[test]
    fn terminal_subscribe_survives_metadata_and_snapshot_backpressure_storm() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(4);
        let (priority_tx, mut priority_rx) = mpsc::channel(4);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, Arc::clone(&metrics));

        for i in 0..4 {
            sender
                .send(HubEvent::UserFileWatch {
                    watch_id: format!("bulk-filler-{i}"),
                    events: Vec::new(),
                })
                .expect("fill bulk lane");
        }

        for i in 0..128 {
            assert!(sender
                .send(HubEvent::PtyOscEvent {
                    session_uuid: format!("storm-title-{i}"),
                    session_name: "agent".to_string(),
                    event: crate::agent::pty::PtyEvent::title_changed(format!("title-{i}")),
                })
                .is_err());
            assert!(sender
                .send(HubEvent::ClientWorkerControl(
                    crate::worker::hub_control::HubControlMessage::RequestSnapshot {
                        client_id: crate::client::ClientId::Socket(format!("socket:{i}")),
                        session_uuid: format!("storm-snapshot-{i}"),
                        subscription_id: format!("sub-{i}"),
                        rows: 24,
                        cols: 80,
                    },
                ))
                .is_err());
        }

        sender
            .send(HubEvent::LuaPtyRequest(
                PtyRequest::SubscribeBrowserTerminal(
                    crate::lua::primitives::pty::BrowserTerminalSubscriptionRequest {
                        peer_id: "browser-fast-lane".to_string(),
                        session_uuid: "sess-fast-lane".to_string(),
                        prefix: None,
                        subscription_id: "sub-fast-lane".to_string(),
                        rows: 24,
                        cols: 80,
                        active_flag: Arc::new(Mutex::new(true)),
                    },
                ),
            ))
            .expect("terminal subscribe must bypass bulk backpressure");
        sender
            .send(HubEvent::LuaPtyRequest(PtyRequest::RefreshSnapshot(
                crate::lua::primitives::pty::RefreshSnapshotRequest {
                    peer_id: "browser-fast-lane".to_string(),
                    session_uuid: "sess-fast-lane".to_string(),
                    subscription_id: "sub-fast-lane".to_string(),
                    rows: 24,
                    cols: 80,
                },
            )))
            .expect("terminal snapshot refresh must bypass bulk backpressure");

        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(
                PtyRequest::SubscribeBrowserTerminal(_)
            ))
        ));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::LuaPtyRequest(PtyRequest::RefreshSnapshot(_)))
        ));
        assert!(priority_rx.try_recv().is_err());

        let mut bulk_count = 0;
        while let Ok(event) = bulk_rx.try_recv() {
            match event {
                HubEvent::UserFileWatch { .. } => bulk_count += 1,
                other => panic!("unexpected bulk event: {other:?}"),
            }
        }
        assert_eq!(bulk_count, 4);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.by_type["user_file_watch"].pending_high_water, 4);
        assert_eq!(snapshot.by_type["lua_pty_request"].enqueue_failed, 0);
        assert_eq!(snapshot.by_type["lua_pty_request"].pending_high_water, 2);
        assert_eq!(snapshot.counters["hub_event.queue_full"], 256);
        assert_eq!(
            snapshot.counters["hub_event.repeatable_rejected.pty_osc"],
            128
        );
        assert_eq!(
            snapshot.counters["hub_event.repeatable_rejected.client_worker_control"],
            128
        );
    }

    #[test]
    fn sender_coalesces_duplicate_repeatable_events_until_dequeued() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(8);
        let (priority_tx, mut priority_rx) = mpsc::channel(8);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, Arc::clone(&metrics));

        let first = HubEvent::ClientWorkerControl(
            crate::worker::hub_control::HubControlMessage::Backpressure(
                crate::worker::hub_control::WorkerBackpressure {
                    source: "worker.client.outbound",
                    capacity: 8,
                    session_uuid: Some("sess-1".to_string()),
                    client_id: Some(crate::client::ClientId::Tui),
                },
            ),
        );
        let duplicate = HubEvent::ClientWorkerControl(
            crate::worker::hub_control::HubControlMessage::Backpressure(
                crate::worker::hub_control::WorkerBackpressure {
                    source: "worker.client.outbound",
                    capacity: 8,
                    session_uuid: Some("sess-1".to_string()),
                    client_id: Some(crate::client::ClientId::Tui),
                },
            ),
        );

        sender.send(first).expect("send first backpressure event");
        sender
            .send(duplicate)
            .expect("coalesced duplicate is not an error");

        let event = bulk_rx.try_recv().expect("one queued event");
        assert!(bulk_rx.try_recv().is_err());
        assert!(priority_rx.try_recv().is_err());
        assert_eq!(metrics.snapshot().counters["hub_event.coalesced"], 1);

        sender.mark_dequeued(&event);
        sender
            .send(HubEvent::ClientWorkerControl(
                crate::worker::hub_control::HubControlMessage::Backpressure(
                    crate::worker::hub_control::WorkerBackpressure {
                        source: "worker.client.outbound",
                        capacity: 8,
                        session_uuid: Some("sess-1".to_string()),
                        client_id: Some(crate::client::ClientId::Tui),
                    },
                ),
            ))
            .expect("event can be queued again after dequeue");

        assert!(matches!(
            bulk_rx.try_recv(),
            Ok(HubEvent::ClientWorkerControl(_))
        ));
    }

    #[test]
    fn sender_bounds_repeatable_floods_and_preserves_cleanup_lane() {
        let (bulk_tx, mut bulk_rx) = mpsc::channel(16);
        let (priority_tx, mut priority_rx) = mpsc::channel(4);
        let metrics = Arc::new(HubEventMetrics::default());
        let sender = HubEventTx::new_with_priority(bulk_tx, priority_tx, Arc::clone(&metrics));

        for _ in 0..1_000 {
            sender
                .send(HubEvent::TimerFired {
                    timer_id: "fast-tick".to_string(),
                })
                .expect("coalesced timer flood should not fail");
        }

        for i in 0..15 {
            sender
                .send(HubEvent::UserFileWatch {
                    watch_id: format!("watch-{i}"),
                    events: Vec::new(),
                })
                .expect("fill remaining bulk capacity");
        }

        let rejected = sender.send(HubEvent::ClientWorkerControl(
            crate::worker::hub_control::HubControlMessage::RequestSnapshot {
                client_id: crate::client::ClientId::Tui,
                session_uuid: "sess-1".to_string(),
                subscription_id: "sub-1".to_string(),
                rows: 24,
                cols: 80,
            },
        ));
        assert!(rejected.is_err());

        sender
            .send(HubEvent::CleanupTick)
            .expect("cleanup tick must bypass saturated bulk lane");
        sender
            .send(HubEvent::SocketClientDisconnected {
                client_id: "socket:stress".to_string(),
            })
            .expect("disconnect cleanup must bypass saturated bulk lane");

        assert!(matches!(priority_rx.try_recv(), Ok(HubEvent::CleanupTick)));
        assert!(matches!(
            priority_rx.try_recv(),
            Ok(HubEvent::SocketClientDisconnected { .. })
        ));
        assert!(priority_rx.try_recv().is_err());

        let mut timer_count = 0;
        let mut file_watch_count = 0;
        while let Ok(event) = bulk_rx.try_recv() {
            match event {
                HubEvent::TimerFired { .. } => timer_count += 1,
                HubEvent::UserFileWatch { .. } => file_watch_count += 1,
                other => panic!("unexpected bulk event: {other:?}"),
            }
        }
        assert_eq!(timer_count, 1);
        assert_eq!(file_watch_count, 15);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.by_type["timer_fired"].pending_high_water, 1);
        assert_eq!(snapshot.by_type["user_file_watch"].pending_high_water, 15);
        assert_eq!(
            snapshot.counters.get("hub_event.coalesced.timer_fired"),
            Some(&999)
        );
        assert_eq!(snapshot.counters["hub_event.queue_full"], 1);
        assert_eq!(
            snapshot.counters["hub_event.repeatable_rejected.client_worker_control"],
            1
        );
    }
}
