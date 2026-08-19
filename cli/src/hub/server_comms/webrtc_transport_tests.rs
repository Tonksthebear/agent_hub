use super::test_support::*;

fn drain_webrtc_json_commands(
    hub: &mut Hub,
    rx: &mut tokio::sync::mpsc::Receiver<crate::worker::webrtc::WebRtcAdapterCommand>,
    max_frames: usize,
) -> Vec<serde_json::Value> {
    let mut frames = Vec::new();
    for _ in 0..40 {
        hub.tokio_runtime.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });
        hub.poll_hub_events();
        while let Ok(command) = rx.try_recv() {
            if let crate::worker::webrtc::WebRtcAdapterCommand::Json { data } = command {
                frames.push(serde_json::from_slice(&data).expect("json command"));
                if frames.len() >= max_frames {
                    return frames;
                }
            }
        }
    }
    frames
}

fn subscribe_browser_terminal(
    hub: &mut Hub,
    browser_identity: &str,
    session_uuid: &str,
    subscription_id: &str,
    rows: u16,
    cols: u16,
) {
    let payload = serde_json::json!({
        "type": "subscribe",
        "channel": "terminal",
        "subscriptionId": subscription_id,
        "params": {
            "session_uuid": session_uuid,
            "rows": rows,
            "cols": cols,
        }
    });
    hub.process_webrtc_plaintext_payload(browser_identity, payload.to_string().as_bytes());
}

fn settle_webrtc_terminal_attach(hub: &mut Hub) {
    for _ in 0..20 {
        hub.tokio_runtime.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });
        hub.poll_hub_events();
    }
}

fn drain_initial_webrtc_terminal_attach_requests(
    rx: &mut tokio::sync::mpsc::Receiver<crate::worker::session_io::SessionIoRequest>,
) -> (
    crate::worker::session_io::TerminalOutputSubscription,
    crate::worker::session_io::TerminalInitialSnapshotDelivery,
) {
    let _ = recv_session_io_request_matching(rx, |request| {
        matches!(
            request,
            crate::worker::session_io::SessionIoRequest::Resize { .. }
        )
    });
    let delivery = recv_terminal_initial_snapshot_delivery(rx);
    let subscription = delivery
        .live_subscription
        .clone()
        .expect("initial snapshot should activate live subscription after delivery");
    (subscription, delivery)
}

#[test]
pub(super) fn test_inactive_webrtc_subscription_strips_probe_queries() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = "sess-filter-inactive-webrtc";
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let mut command_rx =
        install_test_browser_worker(&mut hub, "browser-a", session_uuid, "terminal_sub");

    assert!(
        hub.try_attach_browser_terminal_subscription(&test_browser_subscription_request(
            "browser-a",
            session_uuid,
            "terminal_sub"
        ))
    );
    hub.set_active_terminal_peer(session_uuid, "tui", true);
    let (subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    let mut filter_buffer = Vec::new();
    let filtered = subscription.filter.filter_chunk(
        session_uuid,
        &mut filter_buffer,
        b"before\x1b]11;?\x07after",
    );
    subscription
        .worker
        .try_send(crate::worker::client::ClientWorkerMessage::TerminalBytes {
            session_uuid: session_uuid.to_string(),
            data: {
                let mut data = subscription.output_prefix.clone();
                data.extend(filtered);
                data
            },
        })
        .expect("send filtered terminal bytes");

    let (_subscription_id, data) = recv_next_webrtc_pty_command(&mut hub, &mut command_rx, 0x01);
    assert_eq!(data, b"\x01beforeafter");
}

#[test]
pub(super) fn test_active_webrtc_subscription_keeps_probe_queries() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = "sess-filter-active-webrtc";
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let mut command_rx =
        install_test_browser_worker(&mut hub, "browser-a", session_uuid, "terminal_sub");

    assert!(
        hub.try_attach_browser_terminal_subscription(&test_browser_subscription_request(
            "browser-a",
            session_uuid,
            "terminal_sub"
        ))
    );
    hub.set_active_terminal_peer(session_uuid, "browser-a", true);
    let (subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    let mut filter_buffer = Vec::new();
    let filtered =
        subscription
            .filter
            .filter_chunk(session_uuid, &mut filter_buffer, b"\x1b]11;?\x07after");
    subscription
        .worker
        .try_send(crate::worker::client::ClientWorkerMessage::TerminalBytes {
            session_uuid: session_uuid.to_string(),
            data: {
                let mut data = subscription.output_prefix.clone();
                data.extend(filtered);
                data
            },
        })
        .expect("send terminal bytes");

    let (_subscription_id, data) = recv_next_webrtc_pty_command(&mut hub, &mut command_rx, 0x01);
    assert_eq!(data, b"\x01\x1b]11;?\x07after");
}

#[test]
pub(super) fn test_webrtc_worker_live_output_bypasses_lua_callbacks() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = "sess-webrtc-live-callbacks";
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let mut command_rx =
        install_test_browser_worker(&mut hub, "browser-callbacks", session_uuid, "terminal_live");

    assert!(
        hub.try_attach_browser_terminal_subscription(&test_browser_subscription_request(
            "browser-callbacks",
            session_uuid,
            "terminal_live"
        ))
    );
    let (subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    subscription
        .worker
        .try_send(crate::worker::client::ClientWorkerMessage::TerminalBytes {
            session_uuid: session_uuid.to_string(),
            data: {
                let mut data = subscription.output_prefix.clone();
                data.extend(b"live");
                data
            },
        })
        .expect("send terminal bytes");

    let (_subscription_id, data) = recv_next_webrtc_pty_command(&mut hub, &mut command_rx, 0x01);
    assert_eq!(data, b"\x01live");
}

#[test]
pub(super) fn test_webrtc_focus_message_updates_active_terminal_peer() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = "sess-webrtc-focus";
    let payload = serde_json::to_vec(&serde_json::json!({
        "type": "focus_changed",
        "session_uuid": session_uuid,
        "focused": true,
    }))
    .expect("focus payload");

    hub.process_webrtc_plaintext_payload("browser-a", &payload);

    assert_eq!(
        hub.active_terminal_peers
            .lock()
            .expect("active peers mutex")
            .get(session_uuid)
            .cloned(),
        Some("browser-a".to_string())
    );
}

#[test]
pub(super) fn test_backpressure_recovery_routes_prepared_snapshot_to_peer() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let mut peer_rx = hub
        .webrtc
        .install_test_recovery_sender("browser-recovery", &hub.tokio_runtime);
    let request_id = "snapshot-recovery-test".to_string();
    hub.insert_pending_session_io_snapshot(
        request_id.clone(),
        crate::hub::PendingSessionIoSnapshot {
            session_uuid: "sess-recovery".to_string(),
            started_at: Instant::now(),
            target: crate::hub::PendingSessionIoSnapshotTarget::WebRtcPeerRecovery {
                request: crate::worker::webrtc::WebRtcRecoverySnapshotRequest {
                    request_id: "browser-recovery:sess-recovery".to_string(),
                    browser_identity: "browser-recovery".to_string(),
                    session_uuid: "sess-recovery".to_string(),
                    subscription_id: "sub-recovery".to_string(),
                },
            },
        },
    );

    hub.handle_session_io_event(
        crate::worker::session_io::SessionIoEvent::PreparedSnapshot {
            request_id,
            session_uuid: "sess-recovery".to_string(),
            uncompressed_len: 128,
            payload: vec![0x1f, 0x8b, 0x08],
            recovery: true,
        },
    );

    match peer_rx.try_recv().expect("recovery snapshot command") {
        crate::worker::webrtc::WebRtcAdapterCommand::Pty {
            subscription_id,
            data,
        } => {
            assert_eq!(subscription_id, "sub-recovery");
            assert!(data.starts_with(&[0x1f, 0x8b]));
        }
        other => panic!("expected PTY recovery command, got {other:?}"),
    }

    let snapshot = hub.hub_event_metrics.snapshot();
    assert_eq!(snapshot.counters["snapshot.backpressure_recovery.sent"], 1);
    assert!(snapshot.spans.contains_key("snapshot.gzip_queue"));
}

#[test]
pub(super) fn test_backpressure_recovery_missing_session_counts_failed_without_dispatch() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = "sess-recovery-missing";
    let browser_identity = "browser-recovery-missing";
    let mut peer_rx = hub
        .webrtc
        .install_test_recovery_sender(browser_identity, &hub.tokio_runtime);
    let key = format!("{browser_identity}:{session_uuid}");
    hub.webrtc.record_backpressure_recovery(
        key,
        crate::worker::webrtc::BackpressureRecoveryEntry {
            browser_identity: browser_identity.to_string(),
            session_uuid: session_uuid.to_string(),
            subscription_id: "sub-recovery-missing".to_string(),
            last_drop: Instant::now() - crate::worker::webrtc::BACKPRESSURE_SNAPSHOT_COOLDOWN,
        },
    );

    hub.dispatch_webrtc_recovery_snapshot_requests();

    assert!(peer_rx.try_recv().is_err());
    let metrics = hub.hub_event_metrics.snapshot();
    assert!(!metrics
        .counters
        .contains_key("snapshot.backpressure_recovery.sent"));
    assert!(!metrics
        .counters
        .contains_key("snapshot.backpressure_recovery.empty"));
    assert_eq!(metrics.counters["snapshot.backpressure_recovery.failed"], 1);
}

#[test]
pub(super) fn test_session_backed_webrtc_recovery_snapshot_uses_session_io_get_snapshot() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-recovery-mailbox";
    let session_uuid = "sess-recovery-mailbox";
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    let _peer_rx = hub
        .webrtc
        .install_test_recovery_sender(browser_identity, &hub.tokio_runtime);

    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    hub.webrtc.record_backpressure_recovery(
        format!("{browser_identity}:{session_uuid}"),
        crate::worker::webrtc::BackpressureRecoveryEntry {
            browser_identity: browser_identity.to_string(),
            session_uuid: session_uuid.to_string(),
            subscription_id: "sub-recovery-mailbox".to_string(),
            last_drop: Instant::now() - crate::worker::webrtc::BACKPRESSURE_SNAPSHOT_COOLDOWN,
        },
    );

    hub.dispatch_webrtc_recovery_snapshot_requests();

    let request_id = match recv_session_io_request_matching(&mut session_io_rx, |request| {
        matches!(
            request,
            crate::worker::session_io::SessionIoRequest::GetSnapshot { .. }
        )
    }) {
        crate::worker::session_io::SessionIoRequest::GetSnapshot { request_id } => request_id,
        other => panic!("expected recovery GetSnapshot request, got {other:?}"),
    };

    hub.handle_session_io_event(crate::worker::session_io::SessionIoEvent::Snapshot {
        request_id: request_id.clone(),
        session_uuid: session_uuid.to_string(),
        payload: b"recovery-snapshot".to_vec(),
    });

    assert!(matches!(
        recv_session_io_request_matching(&mut session_io_rx, |request| matches!(
            request,
            crate::worker::session_io::SessionIoRequest::PrepareSnapshot {
                request_id: observed,
                recovery: true,
                ..
            } if observed == &request_id
        )),
        crate::worker::session_io::SessionIoRequest::PrepareSnapshot { .. }
    ));
}

#[test]
pub(super) fn test_webrtc_terminal_control_without_session_uuid_fails_closed_before_lua() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-missing-session-uuid";

    let resize = serde_json::json!({
        "type": "resize",
        "rows": 30,
        "cols": 100,
    });
    hub.process_webrtc_plaintext_payload(browser_identity, resize.to_string().as_bytes());

    let snapshot = hub.hub_event_metrics.snapshot();
    assert_eq!(
        snapshot.counters["webrtc_message.unsupported_terminal_control"],
        1,
        "resize without session_uuid must fail closed instead of falling through Lua subscription state"
    );

    let request_snapshot = serde_json::json!({
        "type": "request_snapshot",
        "rows": 30,
        "cols": 100,
    });
    hub.process_webrtc_plaintext_payload(browser_identity, request_snapshot.to_string().as_bytes());

    let snapshot = hub.hub_event_metrics.snapshot();
    assert_eq!(
        snapshot.counters["webrtc_message.unsupported_terminal_control"],
        2,
        "request_snapshot without session_uuid must fail closed instead of falling through Lua subscription state"
    );
}

#[test]
pub(super) fn test_unknown_peer_burst_guardrail_is_bounded_and_rate_limited() {
    let (hub, _request_tx, _output_rx) = e2e_hub();

    for _ in 0..crate::worker::webrtc::PeerBurstState::THRESHOLD {
        hub.queue_webrtc_peer_command(
            "peer-alpha-abcdefghijklmnopqrstuvwxyz",
            crate::worker::webrtc::WebRtcAdapterCommand::Json { data: vec![1] },
        );
    }
    for i in 0..32 {
        hub.queue_webrtc_peer_command(
            &format!("peer-distinct-{i}"),
            crate::worker::webrtc::WebRtcAdapterCommand::Json { data: vec![1] },
        );
    }

    let snapshot = hub.hub_event_metrics.snapshot();
    assert_eq!(
        snapshot.counters["webrtc_send.unknown_peer_burst"], 1,
        "same peer should warn once per window"
    );
    assert_eq!(
        snapshot.counters["webrtc_send.unknown_peer"],
        (crate::worker::webrtc::PeerBurstState::THRESHOLD + 32) as u64
    );
    let peer_count = hub.webrtc.unknown_peer_distinct_count();
    assert!(peer_count <= crate::worker::webrtc::PeerBurstState::PEER_CAP);
}

#[test]
pub(super) fn test_terminal_attach_intent_resolves_when_session_appears() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let key = "peer-attach:sess-attach".to_string();

    let req =
        test_browser_subscription_request("peer-attach", "sess-attach", "terminal_sess-attach");
    hub.create_browser_terminal_subscription(req);

    assert!(
        hub.pending_terminal_attaches.contains_key(&key),
        "missing session should create pending attach intent"
    );

    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            "sess-attach",
            session_io_tx,
        ));
    let _command_rx = install_test_browser_worker(
        &mut hub,
        "peer-attach",
        "sess-attach",
        "terminal_sess-attach",
    );
    hub.tick();

    assert!(
        !hub.pending_terminal_attaches.contains_key(&key),
        "pending attach intent should clear once session exists"
    );
    assert!(
        hub.terminal_subscription_peers.contains_key(&key),
        "subscription should register after session registration"
    );
    let (subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(subscription.subscription_key, key);
    assert!(
        hub.browser_client_workers.contains_key("peer-attach"),
        "WebRTC peer should register a browser ClientWorker handle"
    );
}

#[test]
pub(super) fn test_webrtc_worker_handle_registered_and_removed_with_subscription() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = "sess-webrtc-worker-lifecycle";
    let key = format!("browser-worker:{session_uuid}");

    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx =
        install_test_browser_worker(&mut hub, "browser-worker", session_uuid, "terminal_sub");

    assert!(
        hub.try_attach_browser_terminal_subscription(&test_browser_subscription_request(
            "browser-worker",
            session_uuid,
            "terminal_sub"
        ))
    );
    assert!(
        hub.browser_client_workers.contains_key("browser-worker"),
        "WebRTC peer should register a browser ClientWorker handle"
    );
    let (subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(subscription.subscription_key, key);
    assert_eq!(
        hub.terminal_subscription_id(&key),
        Some("terminal_sub"),
        "active WebRTC subscription id should be tracked with the terminal key"
    );

    hub.stop_terminal_subscription(&key);

    assert!(
        hub.browser_client_workers.contains_key("browser-worker"),
        "stopping one WebRTC subscription should keep the peer-level ClientWorker handle"
    );
    assert!(
        !hub.terminal_subscription_peers.contains_key(&key),
        "stopping WebRTC subscription should remove the SessionIo subscription peer"
    );
    assert_eq!(
        hub.terminal_subscription_id(&key),
        None,
        "stopping WebRTC subscription should clear the tracked subscription id"
    );
}

#[test]
pub(super) fn test_duplicate_webrtc_terminal_attach_reuses_active_subscription_without_snapshot() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = "sess-webrtc-duplicate-attach";
    let subscription_id = "terminal_duplicate_attach";
    let key = format!("browser-duplicate:{session_uuid}");

    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx =
        install_test_browser_worker(&mut hub, "browser-duplicate", session_uuid, subscription_id);

    let req = test_browser_subscription_request("browser-duplicate", session_uuid, subscription_id);
    assert!(hub.try_attach_browser_terminal_subscription(&req));
    let (subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(subscription.subscription_key, key);

    assert!(hub.try_attach_browser_terminal_subscription(&req));

    let mut saw_resize = false;
    while let Ok(request) = session_io_rx.try_recv() {
        match request {
            crate::worker::session_io::SessionIoRequest::Resize { .. } => {
                saw_resize = true;
            }
            crate::worker::session_io::SessionIoRequest::SubscribeTerminal { .. } => {
                panic!("duplicate attach must not create a second terminal subscription")
            }
            crate::worker::session_io::SessionIoRequest::GetInitialSnapshot { .. } => {
                panic!("duplicate attach must not request a second initial snapshot")
            }
            _ => {}
        }
    }

    assert!(
        saw_resize,
        "duplicate attach should still apply the latest size"
    );
    assert!(
        hub.terminal_subscription_peers.contains_key(&key),
        "active terminal subscription should remain registered"
    );
}

#[test]
pub(super) fn test_webrtc_terminal_subscribe_routes_attach_through_browser_worker() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-subscribe-worker";
    let session_uuid = "sess-webrtc-subscribe-worker";
    let subscription_key = format!("{browser_identity}:{session_uuid}");

    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let mut command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    let payload = serde_json::json!({
        "type": "subscribe",
        "channel": "terminal",
        "subscriptionId": "terminal_sub_worker",
        "params": {
            "session_uuid": session_uuid,
            "rows": 33,
            "cols": 120,
        }
    });
    hub.process_webrtc_plaintext_payload(browser_identity, payload.to_string().as_bytes());

    hub.tokio_runtime.block_on(async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    });
    hub.poll_hub_events();

    assert!(
        hub.terminal_subscription_peers
            .contains_key(&subscription_key),
        "browser subscribe should attach through the peer-level ClientWorker"
    );
    let (subscription, delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(subscription.subscription_key, subscription_key);

    let frames_before_snapshot = drain_webrtc_json_commands(&mut hub, &mut command_rx, 16);
    assert!(
        !frames_before_snapshot
            .iter()
            .any(|frame| frame.get("type").and_then(|v| v.as_str()) == Some("subscribed")),
        "terminal subscribe ack must wait for SessionIo initial snapshot"
    );

    deliver_terminal_initial_snapshot(delivery, b"browser-initial-snapshot".to_vec());

    let mut saw_subscribed_ack = false;
    for _ in 0..20 {
        hub.tokio_runtime.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });
        hub.poll_hub_events();
        while let Ok(command) = command_rx.try_recv() {
            if matches!(
                command,
                crate::worker::webrtc::WebRtcAdapterCommand::Json { data }
                    if serde_json::from_slice::<serde_json::Value>(&data)
                        .ok()
                        .and_then(|value| value.get("type").and_then(|v| v.as_str()).map(str::to_owned))
                        .as_deref()
                        == Some("subscribed")
            ) {
                saw_subscribed_ack = true;
                break;
            }
        }
        if saw_subscribed_ack {
            break;
        }
    }
    assert!(saw_subscribed_ack, "expected subscribed ack");
}

#[test]
pub(super) fn test_webrtc_hub_subscribe_does_not_push_entity_baseline_before_pull() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-hub-baseline";
    let mut command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    hub.lua
        .call_peer_connected(browser_identity)
        .expect("peer connected");
    let payload = serde_json::json!({
        "type": "subscribe",
        "channel": "hub",
        "subscriptionId": "hub_sub_baseline",
        "params": {}
    });
    hub.process_webrtc_plaintext_payload(browser_identity, payload.to_string().as_bytes());

    let frames = drain_webrtc_json_commands(&mut hub, &mut command_rx, 128);
    let subscribed_idx = frames
        .iter()
        .position(|frame| frame.get("type").and_then(|v| v.as_str()) == Some("subscribed"))
        .expect("hub subscribed ack");
    let entity_types_before_ack: std::collections::BTreeSet<String> = frames[..subscribed_idx]
        .iter()
        .filter_map(|frame| {
            (frame.get("type").and_then(|v| v.as_str()) == Some("entity_snapshot"))
                .then(|| frame.get("entity_type").and_then(|v| v.as_str()))
                .flatten()
                .map(str::to_owned)
        })
        .collect();

    assert!(
        entity_types_before_ack.is_empty(),
        "hub subscribe must not eagerly push entity_snapshot frames; saw {frames:?}"
    );
}

#[test]
pub(super) fn test_webrtc_first_attach_queues_measured_resize_before_snapshot() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-first-attach-mailbox";
    let session_uuid = "sess-first-attach-mailbox";
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);

    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    let payload = serde_json::json!({
        "type": "subscribe",
        "channel": "terminal",
        "subscriptionId": "terminal_first_attach",
        "params": {
            "session_uuid": session_uuid,
            "rows": 37,
            "cols": 132,
        }
    });
    hub.process_webrtc_plaintext_payload(browser_identity, payload.to_string().as_bytes());
    settle_webrtc_terminal_attach(&mut hub);

    assert!(matches!(
        recv_session_io_request_matching(&mut session_io_rx, |request| matches!(
            request,
            crate::worker::session_io::SessionIoRequest::Resize {
                rows: 37,
                cols: 132
            }
        )),
        crate::worker::session_io::SessionIoRequest::Resize { .. }
    ));
    let delivery = recv_terminal_initial_snapshot_delivery(&mut session_io_rx);
    assert_eq!(delivery.subscription_id, "terminal_first_attach");
    assert!(matches!(
        delivery.payload_mode,
        crate::worker::session_io::TerminalSnapshotPayloadMode::PrefixedGzip
    ));
    assert!(matches!(hub.pending_session_io_snapshots.is_empty(), true));
}

#[test]
pub(super) fn test_webrtc_duplicate_subscribe_replaces_subscription_and_preserves_latest_geometry()
{
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-duplicate-subscribe";
    let session_uuid = "sess-duplicate-subscribe";
    let subscription_key = format!("{browser_identity}:{session_uuid}");
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(16);

    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    subscribe_browser_terminal(
        &mut hub,
        browser_identity,
        session_uuid,
        "terminal_old_geometry",
        24,
        80,
    );
    settle_webrtc_terminal_attach(&mut hub);
    let (_old_subscription, _old_delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert!(hub.pending_session_io_snapshots.is_empty());
    assert_eq!(
        hub.terminal_subscription_id(&subscription_key),
        Some("terminal_old_geometry")
    );

    subscribe_browser_terminal(
        &mut hub,
        browser_identity,
        session_uuid,
        "terminal_new_geometry",
        44,
        160,
    );
    settle_webrtc_terminal_attach(&mut hub);

    let _ = recv_session_io_request_matching(&mut session_io_rx, |request| {
        matches!(
            request,
            crate::worker::session_io::SessionIoRequest::UnsubscribeTerminal {
                subscription_key: key
            } if key == &subscription_key
        )
    });
    let (new_subscription, new_delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(new_subscription.subscription_key, subscription_key);
    assert_eq!(new_subscription.subscription_id, "terminal_new_geometry");
    assert_eq!(new_delivery.rows, 44);
    assert_eq!(new_delivery.cols, 160);
    assert_eq!(new_delivery.subscription_id, "terminal_new_geometry");
    assert_eq!(
        hub.terminal_subscription_id(&subscription_key),
        Some("terminal_new_geometry")
    );
    let worker = hub
        .browser_client_workers
        .get(browser_identity)
        .expect("browser worker survives subscription replacement")
        .clone();
    let _ = worker.try_send(crate::worker::client::ClientWorkerMessage::SessionInput {
        session_uuid: session_uuid.to_string(),
        data: b"after-replace".to_vec(),
    });
    settle_webrtc_terminal_attach(&mut hub);
    assert!(matches!(
        recv_session_io_request_matching(&mut session_io_rx, |request| matches!(
            request,
            crate::worker::session_io::SessionIoRequest::PtyInput { data }
                if data == b"after-replace"
        )),
        crate::worker::session_io::SessionIoRequest::PtyInput { .. }
    ));
    assert!(matches!(
        new_delivery.payload_mode,
        crate::worker::session_io::TerminalSnapshotPayloadMode::PrefixedGzip
    ));
}

#[test]
pub(super) fn test_stale_browser_detach_does_not_stop_current_terminal_subscription() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-stale-detach";
    let session_uuid = "sess-stale-detach";
    let subscription_key = format!("{browser_identity}:{session_uuid}");
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(16);

    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    subscribe_browser_terminal(
        &mut hub,
        browser_identity,
        session_uuid,
        "terminal_current",
        24,
        80,
    );
    settle_webrtc_terminal_attach(&mut hub);
    let _ = drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(
        hub.terminal_subscription_id(&subscription_key),
        Some("terminal_current")
    );

    hub.handle_client_worker_control(
        crate::worker::hub_control::HubControlMessage::DetachClient {
            client_id: crate::client::ClientId::browser(browser_identity.to_string()),
            session_uuid: session_uuid.to_string(),
            subscription_id: "terminal_stale".to_string(),
        },
    );

    assert!(hub
        .terminal_subscription_peers
        .contains_key(&subscription_key));
    assert_eq!(
        hub.terminal_subscription_id(&subscription_key),
        Some("terminal_current")
    );
    assert!(session_io_rx.try_recv().is_err());
}

#[test]
pub(super) fn test_webrtc_attach_with_new_subscription_id_replaces_active_subscription() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-direct-replace";
    let session_uuid = "sess-direct-replace";
    let subscription_key = format!("{browser_identity}:{session_uuid}");
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(16);

    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    assert!(
        hub.try_attach_browser_terminal_subscription(&test_browser_subscription_request(
            browser_identity,
            session_uuid,
            "terminal_old"
        ))
    );
    let _ = drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);

    assert!(
        hub.try_attach_browser_terminal_subscription(&test_browser_subscription_request(
            browser_identity,
            session_uuid,
            "terminal_new"
        ))
    );
    assert!(matches!(
        recv_session_io_request_matching(&mut session_io_rx, |request| matches!(
            request,
            crate::worker::session_io::SessionIoRequest::UnsubscribeTerminal {
                subscription_key: key
            } if key == &subscription_key
        )),
        crate::worker::session_io::SessionIoRequest::UnsubscribeTerminal { .. }
    ));
    let (subscription, delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(subscription.subscription_id, "terminal_new");
    assert_eq!(delivery.subscription_id, "terminal_new");
    assert_eq!(
        hub.terminal_subscription_id(&subscription_key),
        Some("terminal_new")
    );
}

#[test]
pub(super) fn test_webrtc_request_snapshot_after_subscribe_uses_session_io_without_lua_subscription(
) {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-refresh-mailbox";
    let session_uuid = "sess-refresh-mailbox";
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);

    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    let subscribe = serde_json::json!({
        "type": "subscribe",
        "channel": "terminal",
        "subscriptionId": "terminal_refresh",
        "params": {
            "session_uuid": session_uuid,
            "rows": 24,
            "cols": 80,
        }
    });
    hub.process_webrtc_plaintext_payload(browser_identity, subscribe.to_string().as_bytes());
    settle_webrtc_terminal_attach(&mut hub);
    let (_subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert!(hub.pending_session_io_snapshots.is_empty());

    let refresh = serde_json::json!({
        "type": "request_snapshot",
        "session_uuid": session_uuid,
        "rows": 41,
        "cols": 144,
    });
    hub.process_webrtc_plaintext_payload(browser_identity, refresh.to_string().as_bytes());
    for _ in 0..20 {
        hub.tokio_runtime.block_on(async {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        });
        hub.poll_hub_events();
        if hub.pending_session_io_snapshots.len() == 1 {
            break;
        }
    }

    assert!(matches!(
        recv_session_io_request_matching(&mut session_io_rx, |request| matches!(
            request,
            crate::worker::session_io::SessionIoRequest::Resize {
                rows: 41,
                cols: 144
            }
        )),
        crate::worker::session_io::SessionIoRequest::Resize { .. }
    ));
    assert!(matches!(
        recv_session_io_request_matching(&mut session_io_rx, |request| matches!(
            request,
            crate::worker::session_io::SessionIoRequest::GetSnapshot { .. }
        )),
        crate::worker::session_io::SessionIoRequest::GetSnapshot { .. }
    ));
}

#[test]
pub(super) fn test_webrtc_resize_after_subscribe_routes_through_session_io_mailbox() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-resize-mailbox";
    let session_uuid = "sess-resize-mailbox";
    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);

    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);

    let subscribe = serde_json::json!({
        "type": "subscribe",
        "channel": "terminal",
        "subscriptionId": "terminal_resize",
        "params": {
            "session_uuid": session_uuid,
            "rows": 24,
            "cols": 80,
        }
    });
    hub.process_webrtc_plaintext_payload(browser_identity, subscribe.to_string().as_bytes());
    settle_webrtc_terminal_attach(&mut hub);
    let (_subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert!(hub.pending_session_io_snapshots.is_empty());

    let resize = serde_json::json!({
        "type": "resize",
        "session_uuid": session_uuid,
        "rows": 42,
        "cols": 150,
    });
    hub.process_webrtc_plaintext_payload(browser_identity, resize.to_string().as_bytes());

    assert!(matches!(
        recv_session_io_request_matching(&mut session_io_rx, |request| matches!(
            request,
            crate::worker::session_io::SessionIoRequest::Resize {
                rows: 42,
                cols: 150
            }
        )),
        crate::worker::session_io::SessionIoRequest::Resize { .. }
    ));
}

#[test]
pub(super) fn test_webrtc_peer_cleanup_removes_terminal_client_worker() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-cleanup";
    let session_uuid = "sess-webrtc-cleanup";
    let key = format!("{browser_identity}:{session_uuid}");

    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            session_uuid,
            session_io_tx,
        ));
    let _command_rx =
        install_test_browser_worker(&mut hub, browser_identity, session_uuid, "terminal_sub");
    assert!(
        hub.try_attach_browser_terminal_subscription(&test_browser_subscription_request(
            browser_identity,
            session_uuid,
            "terminal_sub"
        ))
    );
    assert!(
        hub.browser_client_workers.contains_key(browser_identity),
        "WebRTC peer should register a browser ClientWorker handle"
    );
    let (subscription, _delivery) =
        drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert_eq!(subscription.subscription_key, key);

    let channel = crate::channel::WebRtcChannel::builder()
        .server_url(hub.config.server_url.clone())
        .api_key(hub.config.get_api_key().to_string())
        .signal_tx(hub.webrtc.outgoing_signal_tx())
        .stream_frame_tx(hub.webrtc.stream_frame_tx())
        .pty_input_tx(hub.webrtc.pty_input_tx())
        .file_input_tx(hub.webrtc.file_input_tx())
        .crypto_service(
            hub.browser
                .crypto_service
                .clone()
                .expect("crypto service required"),
        )
        .build();
    let generation = hub.webrtc.next_offer_generation(browser_identity);
    let _ = hub.webrtc.complete_offer(
        crate::worker::webrtc::WebRtcOfferCompletion {
            browser_identity: browser_identity.to_string(),
            generation,
            channel,
            answer_signal: Some(serde_json::json!({"type": "answer"})),
        },
        &hub.tokio_runtime,
    );

    hub.cleanup_webrtc_peer(browser_identity, "disconnected");

    assert!(
        !hub.browser_client_workers.contains_key(browser_identity),
        "WebRTC peer cleanup should remove the browser ClientWorker handle"
    );
    assert!(
        !hub.terminal_subscription_peers.contains_key(&key),
        "WebRTC peer cleanup should remove the terminal subscription registration"
    );
}

#[test]
pub(super) fn test_webrtc_dead_sender_cleanup_removes_peer_owned_state() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-dead-sender";
    let session_uuid = "sess-dead-sender";
    let subscription_key = format!("{browser_identity}:{session_uuid}");

    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);
    hub.webrtc
        .install_test_dead_connected_peer(browser_identity, &hub.tokio_runtime);
    hub.browser_terminal_attach_sizes
        .insert(subscription_key.clone(), (24, 80));
    hub.register_terminal_subscription_peer(&subscription_key, session_uuid, browser_identity);
    hub.pending_terminal_attaches.insert(
        subscription_key.clone(),
        crate::hub::PendingTerminalAttach {
            request: crate::hub::PendingTerminalAttachRequest::WebRtc(
                crate::lua::primitives::BrowserTerminalSubscriptionRequest {
                    peer_id: browser_identity.to_string(),
                    session_uuid: session_uuid.to_string(),
                    prefix: None,
                    subscription_id: "terminal_dead_sender".to_string(),
                    rows: 24,
                    cols: 80,
                    active_flag: std::sync::Arc::new(std::sync::Mutex::new(true)),
                },
            ),
            requested_at: std::time::Instant::now(),
        },
    );
    hub.pending_session_io_snapshots.insert(
        "snapshot-dead-sender".to_string(),
        crate::hub::PendingSessionIoSnapshot {
            session_uuid: session_uuid.to_string(),
            started_at: std::time::Instant::now(),
            target: crate::hub::PendingSessionIoSnapshotTarget::WebRtcOutput {
                peer_id: browser_identity.to_string(),
                rows: 24,
                cols: 80,
                kitty_enabled: false,
                subscription_key: Some(subscription_key.clone()),
                active_flag: None,
            },
        },
    );

    hub.cleanup_webrtc_peer_registry();

    assert!(!hub.webrtc.has_channel(browser_identity));
    assert!(!hub.browser_client_workers.contains_key(browser_identity));
    assert!(!hub
        .browser_terminal_attach_sizes
        .contains_key(&subscription_key));
    assert!(!hub
        .terminal_subscription_peers
        .contains_key(&subscription_key));
    assert!(!hub
        .pending_terminal_attaches
        .contains_key(&subscription_key));
    assert!(hub.pending_session_io_snapshots.is_empty());
}

#[test]
pub(super) fn test_failed_webrtc_offer_completion_records_start_failed_metric() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-offer-failed";
    let channel = crate::channel::WebRtcChannel::builder()
        .server_url(hub.config.server_url.clone())
        .api_key(hub.config.get_api_key().to_string())
        .signal_tx(hub.webrtc.outgoing_signal_tx())
        .stream_frame_tx(hub.webrtc.stream_frame_tx())
        .pty_input_tx(hub.webrtc.pty_input_tx())
        .file_input_tx(hub.webrtc.file_input_tx())
        .crypto_service(
            hub.browser
                .crypto_service
                .clone()
                .expect("crypto service required"),
        )
        .build();
    let generation = hub.webrtc.next_offer_generation(browser_identity);

    hub.handle_webrtc_offer_negotiated_event(crate::worker::webrtc::WebRtcOfferCompletion {
        browser_identity: browser_identity.to_string(),
        generation,
        channel,
        answer_signal: None,
    });

    let snapshot = hub.hub_event_metrics.snapshot();
    assert_eq!(snapshot.counters["webrtc_offer.start_failed"], 1);
}

#[test]
pub(super) fn test_webrtc_ice_apply_backpressure_metric_recorded() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-ice-saturated";
    hub.webrtc
        .install_test_dead_connected_peer(browser_identity, &hub.tokio_runtime);
    let _permits = hub.webrtc.saturate_test_ice_apply_limit(browser_identity);

    hub.handle_browser_ice_candidate(
        browser_identity,
        serde_json::json!({
            "candidate": "candidate:1 1 UDP 1 127.0.0.1 9 typ host",
        }),
    );

    let snapshot = hub.hub_event_metrics.snapshot();
    assert_eq!(snapshot.counters["webrtc_ice.apply_backpressure"], 1);
}

#[test]
pub(super) fn test_debug_memory_diagnostics_reports_counts_without_heap_claims() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let browser_identity = "browser-diagnostics";

    let _command_rx = install_test_browser_worker_unsubscribed(&mut hub, browser_identity);
    hub.webrtc
        .install_test_dead_connected_peer(browser_identity, &hub.tokio_runtime);
    hub.pending_session_io_snapshots.insert(
        "snapshot-diagnostics".to_string(),
        crate::hub::PendingSessionIoSnapshot {
            session_uuid: "sess-diagnostics".to_string(),
            started_at: std::time::Instant::now(),
            target: crate::hub::PendingSessionIoSnapshotTarget::WebRtcPeerRecovery {
                request: crate::worker::webrtc::WebRtcRecoverySnapshotRequest {
                    request_id: "snapshot-diagnostics".to_string(),
                    browser_identity: browser_identity.to_string(),
                    session_uuid: "sess-diagnostics".to_string(),
                    subscription_id: "terminal_diagnostics".to_string(),
                },
            },
        },
    );

    let diagnostics = hub.debug_memory_diagnostics();

    assert_eq!(diagnostics["type"], "debug_memory");
    assert_eq!(diagnostics["process"]["allocator"], "mimalloc");
    assert!(diagnostics["process"]["rust_heap_note"]
        .as_str()
        .is_some_and(|note| note.contains("not exposed")));
    assert_eq!(diagnostics["webrtc"]["channels"], 1);
    assert_eq!(diagnostics["webrtc"]["dead_send_tasks"], 1);
    assert_eq!(diagnostics["workers"]["browser_client_workers"], 1);
    assert_eq!(diagnostics["snapshots"]["pending_session_io_snapshots"], 1);
    assert_eq!(
        diagnostics["snapshots"]["pending_webrtc_recovery_snapshots"],
        1
    );
}

#[test]
pub(super) fn test_browser_attach_during_reader_reconnect_queues_reconnecting() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = unique_session_uuid("sess-reconnect-attach");
    let peer_id = "peer-reconnect-attach";
    let subscription_id = "terminal_reconnect_attach";
    let key = format!("{peer_id}:{session_uuid}");

    // Session is registered but SessionIo mailbox is gone while hub is
    // reconnecting the reader — attach must pending as reconnecting, not
    // permanent-close the client.
    cleanup_live_session_identity(&session_uuid);
    hub.handle_cache
        .add_session(test_session_backed_handle(&session_uuid, 24, 80));
    hub.pending_reconnects.insert(
        session_uuid.clone(),
        crate::hub::ReconnectState {
            started_at: Instant::now(),
            attempt_started_at: None,
            generation: 1,
            in_flight: true,
        },
    );
    let mut command_rx =
        install_test_browser_worker(&mut hub, peer_id, &session_uuid, subscription_id);

    hub.create_browser_terminal_subscription(test_browser_subscription_request(
        peer_id,
        &session_uuid,
        subscription_id,
    ));

    assert!(
        hub.pending_terminal_attaches.contains_key(&key),
        "reader reconnect must keep a pending terminal attach"
    );
    assert!(
        hub.should_queue_pending_terminal_attach(&session_uuid),
        "reconnect-pending sessions are allowed to stay pending"
    );

    // No thrash: repeated process_pending keeps a single pending intent.
    for _ in 0..3 {
        hub.process_pending_terminal_attaches();
    }
    assert_eq!(
        hub.pending_terminal_attaches.len(),
        1,
        "reconnect pending must not multiply"
    );

    let frames = drain_webrtc_json_commands(&mut hub, &mut command_rx, 8);
    assert!(
        frames.iter().any(|frame| {
            frame.get("type").and_then(|v| v.as_str()) == Some("terminal_attach")
                && frame.get("state").and_then(|v| v.as_str()) == Some("reconnecting")
        }),
        "reconnect attach must emit terminal_attach reconnecting, frames={frames:?}"
    );
    assert!(
        !frames.iter().any(|frame| {
            frame.get("type").and_then(|v| v.as_str()) == Some("process_exited")
        }),
        "reconnect attach must not permanent-close with process_exited, frames={frames:?}"
    );

    hub.pending_reconnects.clear();
    cleanup_live_session_identity(&session_uuid);
}

#[test]
pub(super) fn test_browser_reuse_path_defers_while_reader_reconnect_pending() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = unique_session_uuid("sess-reuse-reconnect");
    let peer_id = "peer-reuse-reconnect";
    let subscription_id = "terminal_reuse_reconnect";
    let key = format!("{peer_id}:{session_uuid}");

    let (session_io_tx, mut session_io_rx) = tokio::sync::mpsc::channel(8);
    hub.handle_cache
        .add_session(test_session_backed_handle_with_mailbox(
            &session_uuid,
            session_io_tx,
        ));
    let mut command_rx =
        install_test_browser_worker(&mut hub, peer_id, &session_uuid, subscription_id);

    assert!(hub.try_attach_browser_terminal_subscription(
        &test_browser_subscription_request(peer_id, &session_uuid, subscription_id)
    ));
    let _ = drain_initial_webrtc_terminal_attach_requests(&mut session_io_rx);
    assert!(hub.terminal_subscription_peers.contains_key(&key));

    // Reader death: clear mailbox and mark reconnect pending while hub peer
    // registration still exists (the production race).
    hub.pending_reconnects.insert(
        session_uuid.clone(),
        crate::hub::ReconnectState {
            started_at: Instant::now(),
            attempt_started_at: None,
            generation: 1,
            in_flight: true,
        },
    );

    hub.create_browser_terminal_subscription(test_browser_subscription_request(
        peer_id,
        &session_uuid,
        subscription_id,
    ));

    assert!(
        hub.pending_terminal_attaches.contains_key(&key),
        "reuse path during reconnect must pending, not claim attached"
    );
    assert!(
        !hub.terminal_subscription_peers.contains_key(&key),
        "reconnect defer must invalidate stale peer registration for full reattach later"
    );
    let frames = drain_webrtc_json_commands(&mut hub, &mut command_rx, 12);
    assert!(
        !frames.iter().any(|frame| {
            frame.get("type").and_then(|v| v.as_str()) == Some("terminal_attach")
                && frame.get("state").and_then(|v| v.as_str()) == Some("attached")
        }),
        "must not claim attached during reconnect, frames={frames:?}"
    );

    hub.pending_reconnects.clear();
    cleanup_live_session_identity(&session_uuid);
}

#[test]
pub(super) fn test_browser_attach_dead_registered_session_does_not_pending_thrash() {
    // Session still in HandleCache but SessionIo mailbox is gone and the
    // process is not live. Pending must not absorb this — that was the
    // Queue ≫ Started thrash path on every hub tick.
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = unique_session_uuid("sess-dead-attach");
    let peer_id = "peer-dead-attach";
    let subscription_id = "terminal_dead_attach";
    let key = format!("{peer_id}:{session_uuid}");

    cleanup_live_session_identity(&session_uuid);
    hub.handle_cache
        .add_session(test_session_backed_handle(&session_uuid, 24, 80));
    let mut command_rx =
        install_test_browser_worker(&mut hub, peer_id, &session_uuid, subscription_id);

    let req = test_browser_subscription_request(peer_id, &session_uuid, subscription_id);
    hub.create_browser_terminal_subscription(req);

    assert!(
        !hub.pending_terminal_attaches.contains_key(&key),
        "dead registered session must not enter pending attach"
    );
    assert!(
        !hub.terminal_subscription_peers.contains_key(&key),
        "failed attach must not register a live terminal subscription"
    );

    // Repeated create / process_pending must stay quiet (no thrash requeue).
    for _ in 0..5 {
        hub.create_browser_terminal_subscription(test_browser_subscription_request(
            peer_id,
            &session_uuid,
            subscription_id,
        ));
        hub.process_pending_terminal_attaches();
    }
    assert!(
        hub.pending_terminal_attaches.is_empty(),
        "pending map must stay empty after dead-session attach failures"
    );

    let frames = drain_webrtc_json_commands(&mut hub, &mut command_rx, 8);
    assert!(
        frames.iter().any(|frame| frame.get("type").and_then(|v| v.as_str()) == Some("process_exited")),
        "dead attach must emit process_exited once, frames={frames:?}"
    );
    cleanup_live_session_identity(&session_uuid);
}

#[test]
pub(super) fn test_pending_attach_does_not_requeue_when_registered_session_attach_fails() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let session_uuid = unique_session_uuid("sess-pending-drop");
    let peer_id = "peer-pending-drop";
    let subscription_id = "terminal_pending_drop";
    let key = format!("{peer_id}:{session_uuid}");

    // Start as true pending (session missing).
    hub.create_browser_terminal_subscription(test_browser_subscription_request(
        peer_id,
        &session_uuid,
        subscription_id,
    ));
    assert!(hub.pending_terminal_attaches.contains_key(&key));

    // Session appears without a live SessionIo mailbox → attach fails permanently.
    cleanup_live_session_identity(&session_uuid);
    hub.handle_cache
        .add_session(test_session_backed_handle(&session_uuid, 24, 80));
    let _command_rx =
        install_test_browser_worker(&mut hub, peer_id, &session_uuid, subscription_id);

    hub.process_pending_terminal_attaches();
    assert!(
        !hub.pending_terminal_attaches.contains_key(&key),
        "failed attach on a now-registered session must drop pending, not requeue"
    );

    hub.process_pending_terminal_attaches();
    hub.process_pending_terminal_attaches();
    assert!(
        hub.pending_terminal_attaches.is_empty(),
        "subsequent ticks must not resurrect thrash pending"
    );
    cleanup_live_session_identity(&session_uuid);
}

#[test]
pub(super) fn test_terminal_attach_intent_times_out_to_not_found() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let key = "peer-timeout:sess-timeout".to_string();

    let req =
        test_browser_subscription_request("peer-timeout", "sess-timeout", "terminal_sess-timeout");
    let active_flag = Arc::clone(&req.active_flag);
    hub.create_browser_terminal_subscription(req);

    {
        let intent = hub
            .pending_terminal_attaches
            .get_mut(&key)
            .expect("pending attach intent should exist");
        intent.requested_at =
            Instant::now() - (Hub::TERMINAL_ATTACH_NOT_FOUND_TIMEOUT + Duration::from_millis(1));
    }

    hub.tick();

    assert!(
        !hub.pending_terminal_attaches.contains_key(&key),
        "stale pending attach should be removed"
    );
    assert!(
        !*active_flag
            .lock()
            .expect("Subscription active_flag mutex poisoned"),
        "not_found transition should deactivate subscription handle"
    );
}

#[test]
pub(super) fn test_terminal_attach_intent_replaces_previous_pending_request() {
    let (mut hub, _request_tx, _output_rx) = e2e_hub();
    let key = "peer-replace:sess-replace".to_string();

    let req1 = test_browser_subscription_request("peer-replace", "sess-replace", "terminal_old");
    let req1_active = Arc::clone(&req1.active_flag);
    hub.create_browser_terminal_subscription(req1);

    let req2 = test_browser_subscription_request("peer-replace", "sess-replace", "terminal_new");
    let req2_active = Arc::clone(&req2.active_flag);
    hub.create_browser_terminal_subscription(req2);

    let pending = hub
        .pending_terminal_attaches
        .get(&key)
        .expect("pending attach should still exist for missing session");
    let subscription_id = match &pending.request {
        PendingTerminalAttachRequest::WebRtc(req) => req.subscription_id.as_str(),
        other => panic!("expected WebRTC pending attach, got {other:?}"),
    };
    assert_eq!(
        subscription_id, "terminal_new",
        "latest subscribe should replace previous pending attach"
    );
    assert!(
        !*req1_active
            .lock()
            .expect("Subscription active_flag mutex poisoned"),
        "previous pending attach should be deactivated"
    );
    assert!(
        *req2_active
            .lock()
            .expect("Subscription active_flag mutex poisoned"),
        "replacement attach should remain active"
    );
}
