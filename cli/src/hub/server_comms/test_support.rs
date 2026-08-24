use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) use std::sync::{Arc, Mutex};
pub(super) use std::time::{Duration, Instant};

pub(super) use crate::agent::pty::PtySession;
pub(super) use crate::client::{ClientId, TuiOutput, TuiRequest};
pub(super) use crate::config::Config;
pub(super) use crate::hub::agent_handle::{PtyHandle, SessionHandle, SessionType};
pub(super) use crate::hub::{Hub, PendingTerminalAttachRequest};
pub(super) use crate::lua::BrowserTerminalSubscriptionRequest;
pub(super) use crate::relay::create_crypto_service;
pub(super) use crate::socket::framing::{Frame, FrameDecoder};

/// Single shared tokio runtime for all server_comms tests.
pub(super) fn shared_test_runtime() -> Arc<tokio::runtime::Runtime> {
    static RT: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();
    Arc::clone(RT.get_or_init(|| Arc::new(tokio::runtime::Runtime::new().unwrap())))
}

pub(super) fn e2e_config() -> Config {
    let mut config = Config::default();
    config.server_url = "http://localhost:3000".to_string();
    config.token = "btstr_test-key".to_string();
    config.poll_interval = 10;
    config.agent_timeout = 300;
    config.max_sessions = 10;
    config.worktree_base = PathBuf::from("/tmp/test-worktrees");
    config
}

/// Create a Hub with TUI registered, crypto initialized, and all real
/// Lua handlers loaded. Returns the Hub plus the TUI channels for
/// sending requests and receiving output.
///
/// Manually calls `register_hub_primitives()` + `load_lua_init()`
/// instead of the full `setup()` for test isolation.
pub(super) fn e2e_hub() -> (
    Hub,
    tokio::sync::mpsc::UnboundedSender<TuiRequest>,
    tokio::sync::mpsc::UnboundedReceiver<TuiOutput>,
) {
    let config = e2e_config();
    let mut hub = Hub::with_runtime(config, shared_test_runtime()).unwrap();

    let crypto_service = create_crypto_service("test-hub");
    hub.browser.crypto_service = Some(crypto_service);

    // Register Hub primitives (must happen before loading init script)
    hub.lua
        .register_hub_primitives(
            std::sync::Arc::clone(&hub.handle_cache),
            hub.config.worktree_base.clone(),
            hub.hub_identifier.clone(),
            std::sync::Arc::clone(&hub.shared_server_id),
            std::sync::Arc::clone(&hub.state),
            std::sync::Arc::clone(&hub.shared_color_cache),
        )
        .expect("Should register hub primitives");
    crate::mcp_http::register_lua(
        hub.lua.lua(),
        hub.mcp_callers.clone(),
        hub.mcp_http_fanout.clone(),
    )
    .expect("Should register shared MCP HTTP primitives");

    // Load real Lua handlers (init.lua and all handlers)
    hub.load_lua_init();

    // Register TUI AFTER Lua handlers are loaded (triggers
    // tui_connected which may broadcast initial state)
    let (request_tx, request_rx) = tokio::sync::mpsc::unbounded_channel::<TuiRequest>();
    let output_rx = hub.register_tui_via_lua(request_rx);

    (hub, request_tx, output_rx)
}

pub(super) fn test_session_backed_handle(
    session_uuid: &str,
    rows: u16,
    cols: u16,
) -> SessionHandle {
    let (event_tx, _rx) = tokio::sync::broadcast::channel(64);
    let pty = PtyHandle::new_with_session(
        event_tx,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicBool::new(false)),
        None,
        Arc::new(Mutex::new(None)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        rows,
        cols,
    );
    SessionHandle::new(session_uuid, "test-agent", SessionType::Agent, None, pty)
}

pub(super) fn test_session_backed_handle_with_mailbox(
    session_uuid: &str,
    session_io_tx: tokio::sync::mpsc::Sender<crate::worker::session_io::SessionIoRequest>,
) -> SessionHandle {
    let conn =
        crate::session::connection::SessionConnection::test_with_session_io_sender(session_io_tx);
    test_session_backed_handle_with_connection(session_uuid, conn)
}

pub(super) fn test_session_backed_handle_with_connection(
    session_uuid: &str,
    conn: crate::session::connection::SessionConnection,
) -> SessionHandle {
    let (event_tx, _rx) = tokio::sync::broadcast::channel(64);
    let pty = PtyHandle::new_with_session(
        event_tx,
        Arc::new(AtomicBool::new(false)),
        Arc::new(AtomicBool::new(true)),
        Arc::new(AtomicBool::new(false)),
        None,
        Arc::new(Mutex::new(Some(conn))),
        Arc::new(AtomicU64::new(0)),
        Arc::new(std::sync::atomic::AtomicI64::new(0)),
        24,
        80,
    );
    SessionHandle::new(session_uuid, "test-agent", SessionType::Agent, None, pty)
}

pub(super) fn unique_session_uuid(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time drift")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

pub(super) fn register_live_session_identity(session_uuid: &str) {
    let socket_path = crate::session::session_socket_path(session_uuid).expect("socket path");
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent).expect("create sessions dir");
    }
    std::fs::write(&socket_path, b"live").expect("write socket sentinel");
    crate::session::write_session_pid_file(session_uuid, std::process::id())
        .expect("write session pid file");
}

pub(super) fn cleanup_live_session_identity(session_uuid: &str) {
    if let Ok(path) = crate::session::session_socket_path(session_uuid) {
        let _ = std::fs::remove_file(path);
    }
    if let Ok(path) = crate::session::session_pid_path(session_uuid) {
        let _ = std::fs::remove_file(path);
    }
}

pub(super) fn register_test_socket_client(
    hub: &mut Hub,
    client_id: &str,
) -> tokio::net::UnixStream {
    let (client_std, server_std) =
        std::os::unix::net::UnixStream::pair().expect("std UnixStream::pair");
    client_std
        .set_nonblocking(true)
        .expect("set_nonblocking client socket");
    server_std
        .set_nonblocking(true)
        .expect("set_nonblocking server socket");
    let _guard = hub.tokio_runtime.enter();
    let client_stream =
        tokio::net::UnixStream::from_std(client_std).expect("tokio::UnixStream client");
    let server_stream =
        tokio::net::UnixStream::from_std(server_std).expect("tokio::UnixStream server");
    let conn = crate::socket::client_conn::SocketClientConn::new(
        client_id.to_string(),
        server_stream,
        hub.hub_event_tx.clone(),
    );
    hub.socket_clients.insert(client_id.to_string(), conn);
    client_stream
}

pub(super) fn read_test_socket_frame_matching<F>(
    stream: &mut tokio::net::UnixStream,
    timeout: Duration,
    mut frame_matches: F,
) -> Option<Frame>
where
    F: FnMut(&Frame) -> bool,
{
    let handle = shared_test_runtime();
    handle.block_on(async {
        use tokio::io::AsyncReadExt;

        let mut decoder = FrameDecoder::new();
        let mut buf = [0u8; 4096];
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            let n = tokio::time::timeout(remaining, stream.read(&mut buf))
                .await
                .ok()?
                .expect("socket read");
            assert!(n > 0, "socket closed before frame arrived");
            let frames = decoder.feed(&buf[..n]).expect("decode frame");
            for frame in frames {
                if frame_matches(&frame) {
                    return Some(frame);
                }
            }
        }
    })
}

pub(super) fn read_test_socket_frames(
    stream: &mut tokio::net::UnixStream,
    max_frames: usize,
    timeout: Duration,
) -> Vec<Frame> {
    shared_test_runtime().block_on(async {
        use tokio::io::AsyncReadExt;

        let mut decoder = FrameDecoder::new();
        let mut buf = [0u8; 4096];
        let mut frames = Vec::new();
        let deadline = tokio::time::Instant::now() + timeout;
        while frames.len() < max_frames && tokio::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(
                remaining.min(Duration::from_millis(100)),
                stream.read(&mut buf),
            )
            .await
            {
                Ok(Ok(n)) if n > 0 => {
                    frames.extend(decoder.feed(&buf[..n]).expect("decode frames"));
                }
                Ok(Ok(_)) => break,
                Ok(Err(e)) => panic!("socket read: {e}"),
                Err(_) if frames.is_empty() => continue,
                Err(_) => break,
            }
        }
        frames
    })
}

pub(super) fn settle_worker_subscription() {
    shared_test_runtime().block_on(async {
        tokio::time::sleep(Duration::from_millis(50)).await;
    });
}

pub(super) fn recv_session_io_request_matching<F>(
    rx: &mut tokio::sync::mpsc::Receiver<crate::worker::session_io::SessionIoRequest>,
    mut matches_request: F,
) -> crate::worker::session_io::SessionIoRequest
where
    F: FnMut(&crate::worker::session_io::SessionIoRequest) -> bool,
{
    shared_test_runtime().block_on(async {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for matching SessionIoRequest"
            );
            if let Ok(Some(request)) =
                tokio::time::timeout(remaining.min(Duration::from_millis(50)), rx.recv()).await
            {
                if matches_request(&request) {
                    return request;
                }
            }
        }
    })
}

pub(super) fn recv_terminal_initial_snapshot_delivery(
    rx: &mut tokio::sync::mpsc::Receiver<crate::worker::session_io::SessionIoRequest>,
) -> crate::worker::session_io::TerminalInitialSnapshotDelivery {
    match recv_session_io_request_matching(rx, |request| {
        matches!(
            request,
            crate::worker::session_io::SessionIoRequest::GetInitialSnapshot { .. }
        )
    }) {
        crate::worker::session_io::SessionIoRequest::GetInitialSnapshot { delivery } => delivery,
        other => panic!("expected GetInitialSnapshot request, got {other:?}"),
    }
}

pub(super) fn deliver_terminal_initial_snapshot(
    delivery: crate::worker::session_io::TerminalInitialSnapshotDelivery,
    snapshot: Vec<u8>,
) {
    delivery
        .worker
        .try_send(crate::worker::client::ClientWorkerMessage::ControlFrame(
            crate::worker::client::ClientControlFrame::Scrollback {
                session_uuid: delivery.session_uuid.clone(),
                rows: delivery.rows,
                cols: delivery.cols,
                kitty_enabled: delivery.kitty_enabled,
                data: snapshot,
            },
        ))
        .expect("deliver initial snapshot to client worker");

    if delivery.confirm_subscription {
        delivery
            .worker
            .try_send(crate::worker::client::ClientWorkerMessage::ControlFrame(
                crate::worker::client::ClientControlFrame::BoundaryJson(serde_json::json!({
                    "type": "subscribed",
                    "subscriptionId": delivery.subscription_id.clone(),
                })),
            ))
            .expect("deliver subscription confirmation to client worker");
    }

    // Mirror production ordering for tests: mode (if present) before final attach.
    if let Some(mode) = &delivery.mode {
        delivery
            .worker
            .try_send(crate::worker::client::ClientWorkerMessage::ControlFrame(
                crate::worker::client::ClientControlFrame::ModeChanged {
                    session_uuid: delivery.session_uuid.clone(),
                    mode: mode.clone(),
                },
            ))
            .expect("deliver mode replay in test initial snapshot");
    }

    delivery
        .worker
        .try_send(crate::worker::client::ClientWorkerMessage::ControlFrame(
            crate::worker::client::ClientControlFrame::TerminalAttach {
                subscription_id: delivery.subscription_id,
                session_uuid: delivery.session_uuid,
                state: crate::worker::client::TerminalAttachState::Attached,
            },
        ))
        .expect("deliver attached state to client worker");
}

pub(super) fn deliver_terminal_attach_state(
    delivery: crate::worker::session_io::TerminalInitialSnapshotDelivery,
    state: crate::worker::client::TerminalAttachState,
) {
    delivery
        .worker
        .try_send(crate::worker::client::ClientWorkerMessage::ControlFrame(
            crate::worker::client::ClientControlFrame::TerminalAttach {
                subscription_id: delivery.subscription_id,
                session_uuid: delivery.session_uuid,
                state,
            },
        ))
        .expect("deliver terminal attach state to client worker");
}

pub(super) fn function_body<'a>(source: &'a str, name: &str) -> &'a str {
    let start = source
        .find(&format!("fn {name}"))
        .unwrap_or_else(|| panic!("missing function {name}"));
    let body_start = source[start..]
        .find('{')
        .map(|offset| start + offset)
        .expect("function body start");
    let mut depth = 0usize;
    for (idx, ch) in source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[body_start..=body_start + idx];
                }
            }
            _ => {}
        }
    }
    panic!("unterminated function {name}");
}

/// Create a test session handle. No local shadow screen — all PTYs are
/// session-backed. Seed output is broadcast but not parsed locally.
pub(super) fn test_local_session_handle_with_seed(
    session_uuid: &str,
    seed_output: &[u8],
) -> SessionHandle {
    let pty_session = PtySession::new(24, 80);
    let (shared_state, event_tx, kitty_enabled, cursor_visible, resize_pending) =
        pty_session.get_direct_access();
    std::mem::forget(pty_session);

    let pty = PtyHandle::new(
        event_tx,
        shared_state,
        kitty_enabled,
        cursor_visible,
        resize_pending,
        None,
    );
    let _ = pty
        .event_tx_clone()
        .send(crate::agent::pty::events::PtyEvent::output(
            seed_output.to_vec(),
        ));

    SessionHandle::new(session_uuid, "test-agent", SessionType::Agent, None, pty)
}

pub(super) fn test_local_session_handle(session_uuid: &str) -> SessionHandle {
    test_local_session_handle_with_seed(session_uuid, b"cached-local-output\n")
}

// Terminal probe caching is exercised via session-process paths.

pub(super) fn test_browser_subscription_request(
    peer_id: &str,
    session_uuid: &str,
    subscription_id: &str,
) -> BrowserTerminalSubscriptionRequest {
    BrowserTerminalSubscriptionRequest {
        peer_id: peer_id.to_string(),
        session_uuid: session_uuid.to_string(),
        prefix: Some(vec![0x01]),
        subscription_id: subscription_id.to_string(),
        rows: 24,
        cols: 80,
        active_flag: Arc::new(Mutex::new(true)),
    }
}

pub(super) fn install_test_browser_worker(
    hub: &mut Hub,
    peer_id: &str,
    session_uuid: &str,
    subscription_id: &str,
) -> tokio::sync::mpsc::Receiver<crate::worker::webrtc::WebRtcAdapterCommand> {
    let command_rx = hub
        .webrtc
        .install_test_recovery_sender(peer_id, &hub.tokio_runtime);
    let _guard = hub.tokio_runtime.enter();
    let peer_command_tx = hub
        .webrtc
        .peer_command_sender(peer_id)
        .expect("test recovery sender should install a peer command queue");
    let worker = hub.spawn_webrtc_client_worker_adapter(peer_id.to_string(), peer_command_tx);
    hub.webrtc
        .register_client_worker_route(peer_id.to_string(), worker.clone());
    let _ = worker.try_send(
        crate::worker::client::ClientWorkerMessage::SubscribeSession {
            session_uuid: session_uuid.to_string(),
            subscription_id: subscription_id.to_string(),
        },
    );
    hub.browser_client_workers
        .insert(peer_id.to_string(), worker);
    command_rx
}

pub(super) fn install_test_browser_worker_unsubscribed(
    hub: &mut Hub,
    peer_id: &str,
) -> tokio::sync::mpsc::Receiver<crate::worker::webrtc::WebRtcAdapterCommand> {
    let command_rx = hub
        .webrtc
        .install_test_recovery_sender(peer_id, &hub.tokio_runtime);
    let _guard = hub.tokio_runtime.enter();
    let peer_command_tx = hub
        .webrtc
        .peer_command_sender(peer_id)
        .expect("test recovery sender should install a peer command queue");
    let worker = hub.spawn_webrtc_client_worker_adapter(peer_id.to_string(), peer_command_tx);
    hub.webrtc
        .register_client_worker_route(peer_id.to_string(), worker.clone());
    hub.browser_client_workers
        .insert(peer_id.to_string(), worker);
    command_rx
}

pub(super) fn recv_next_webrtc_pty_command(
    hub: &mut Hub,
    rx: &mut tokio::sync::mpsc::Receiver<crate::worker::webrtc::WebRtcAdapterCommand>,
    prefix: u8,
) -> (String, Vec<u8>) {
    for _ in 0..20 {
        hub.tokio_runtime.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });
        hub.poll_hub_events();
        while let Ok(command) = rx.try_recv() {
            if let crate::worker::webrtc::WebRtcAdapterCommand::Pty {
                subscription_id,
                data,
            } = command
            {
                if data.first() == Some(&prefix) {
                    return (subscription_id, data);
                }
            }
        }
    }

    panic!("expected WebRTC PTY command with prefix {prefix:#x}");
}

/// Drains all pending `TuiOutput::Message` JSON values from the output
/// channel, ignoring non-Message variants.
pub(super) fn drain_messages(
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<TuiOutput>,
) -> Vec<serde_json::Value> {
    let mut messages = Vec::new();
    while let Ok(output) = rx.try_recv() {
        if let TuiOutput::Message(json) = output {
            messages.push(json);
        }
    }
    messages
}
