use super::*;

impl Hub {
    /// Handle one outgoing WebRTC signal from the transport registry.
    pub fn handle_webrtc_signal(&mut self, signal: crate::channel::webrtc::OutgoingSignal) {
        use crate::channel::webrtc::OutgoingSignal;
        match signal {
            OutgoingSignal::Ice {
                browser_identity,
                envelope,
            } => {
                self.handle_transport_control_message(
                    crate::worker::hub_control::HubControlMessage::TransportSignalReady {
                        client_id: crate::client::ClientId::browser(browser_identity.clone()),
                        signal: crate::worker::hub_control::TransportSignal::Ice {
                            browser_identity,
                            envelope,
                        },
                    },
                );
                log::debug!("[Crypto] Relayed ICE candidate through transport control surface",);
            }
        }
    }

    pub(super) fn emit_outgoing_signal(
        &self,
        browser_identity: &str,
        envelope: serde_json::Value,
        signal_kind: &str,
    ) -> bool {
        let data = serde_json::json!({
            "browser_identity": browser_identity,
            "envelope": envelope,
        });
        if let Err(error) = self.lua.fire_json_event("outgoing_signal", &data) {
            log::error!("[WebRTC] Failed to fire outgoing_signal for {signal_kind}: {error}");
            return false;
        }
        true
    }

    pub(super) fn handle_signaling_message(&mut self, message: serde_json::Value) {
        let msg_type = message.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let browser_identity = message
            .get("browser_identity")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        match msg_type {
            "transport_config_request" => {
                if browser_identity.is_empty() {
                    log::warn!("[Lua] transport_config_request missing browser_identity");
                    return;
                }
                let config = serde_json::json!({
                    "type": "transport_config",
                    "e2e_enabled": !crate::env::is_encryption_disabled(),
                });
                self.emit_outgoing_signal(browser_identity, config, "transport config");
            }
            "signal" => {
                if browser_identity.is_empty() {
                    log::warn!("[Lua] Signal message missing browser_identity");
                    return;
                }

                if message
                    .get("decrypt_failed")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    log::warn!(
                        "Signal decryption failed for browser {}, requesting ratchet restart",
                        browser_identity
                    );
                    self.request_transport_ratchet_restart(browser_identity);
                    return;
                }

                let Some(signal_data) = message.get("envelope") else {
                    log::warn!(
                        "[Lua] Signal message missing envelope for {}",
                        &browser_identity[..browser_identity.len().min(8)]
                    );
                    return;
                };
                let signal_type = signal_data
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                match signal_type {
                    "offer" => {
                        let Some(sdp) = signal_data.get("sdp").and_then(|v| v.as_str()) else {
                            log::warn!(
                                "[Lua] Offer missing sdp for {}",
                                &browser_identity[..browser_identity.len().min(8)]
                            );
                            return;
                        };
                        log::info!(
                            "[Lua] Processing WebRTC offer from {}",
                            &browser_identity[..browser_identity.len().min(8)]
                        );
                        self.start_webrtc_offer(sdp, browser_identity);
                    }
                    "ice" => {
                        let candidate = signal_data
                            .get("candidate")
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        self.handle_browser_ice_candidate(browser_identity, candidate);
                    }
                    other => {
                        log::warn!(
                            "[Lua] Unknown signal type for {}: {}",
                            &browser_identity[..browser_identity.len().min(8)],
                            other
                        );
                    }
                }
            }
            "bundle_request" => {
                if browser_identity.is_empty() {
                    log::warn!("[Lua] bundle_request missing browser_identity");
                    return;
                }
                self.send_ratchet_bundle_refresh(browser_identity);
            }
            other => {
                log::warn!("[Lua] Unsupported signaling message type: {}", other);
            }
        }
    }

    pub(super) fn handle_browser_ice_candidate(
        &mut self,
        browser_identity: &str,
        candidate: serde_json::Value,
    ) {
        const MAX_QUEUED_ICE_PER_BROWSER: usize = 128;

        let browser_id_short = browser_identity[..browser_identity.len().min(8)].to_string();
        match self.webrtc.queue_or_apply_ice(
            browser_identity,
            candidate,
            MAX_QUEUED_ICE_PER_BROWSER,
            &self.tokio_runtime,
            move |candidate_str, sdp_mid, sdp_mline_index, error| {
                log::warn!(
                    "[Lua] Failed to add ICE candidate for {}: {} (mid={:?}, mline={:?}, candidate='{}')",
                    browser_id_short,
                    error,
                    sdp_mid,
                    sdp_mline_index,
                    Self::ice_candidate_preview(candidate_str),
                );
            },
        ) {
            crate::worker::webrtc::QueueOrApplyIceOutcome::ApplySpawned => {}
            crate::worker::webrtc::QueueOrApplyIceOutcome::ApplyBackpressure => {
                self.hub_event_metrics
                    .record_counter("webrtc_ice.apply_backpressure", 1);
                log::warn!(
                    "[Lua] Dropping ICE candidate for {} because peer ICE apply tasks are saturated",
                    &browser_identity[..browser_identity.len().min(8)]
                );
            }
            crate::worker::webrtc::QueueOrApplyIceOutcome::Queued(queued) => {
                log::debug!(
                    "[Lua] Queued ICE candidate while offer in flight for {} (queued={})",
                    &browser_identity[..browser_identity.len().min(8)],
                    queued
                );
            }
            crate::worker::webrtc::QueueOrApplyIceOutcome::IgnoredEmpty => {
                log::debug!(
                    "[Lua] Ignoring empty ICE candidate for {}",
                    &browser_identity[..browser_identity.len().min(8)]
                );
            }
            crate::worker::webrtc::QueueOrApplyIceOutcome::UnknownBrowser => {
                log::warn!(
                    "[Lua] ICE candidate for unknown browser {}",
                    &browser_identity[..browser_identity.len().min(8)]
                );
            }
        }
    }

    /// Handle one incoming WebRTC stream frame.
    pub fn handle_stream_frame(&mut self, frame: crate::channel::webrtc::StreamIncoming) {
        use crate::relay::stream_mux::StreamMultiplexer;

        let _guard = self.tokio_runtime.enter();
        let mux = self
            .stream_muxes
            .entry(frame.browser_identity.clone())
            .or_insert_with(StreamMultiplexer::new);
        mux.handle_frame(frame.frame_type, frame.stream_id, frame.payload);
    }

    #[cfg(test)]
    pub(super) fn poll_webrtc_peer_payloads_for_tests(&mut self) {
        let messages = self.webrtc.poll_received_messages(&self.tokio_runtime);
        if !messages.is_empty() {
            log::trace!("[WebRTC-POLL] Drained {} messages", messages.len());
        }
        for (browser_identity, payload) in messages {
            self.process_webrtc_plaintext_payload(&browser_identity, &payload);
        }
        for browser_identity in self.webrtc.drain_decrypt_failure_triggers() {
            self.request_transport_ratchet_restart(&browser_identity);
        }
    }

    #[cfg(test)]
    pub(super) fn poll_webrtc_dc_opens(&mut self) {
        for browser_identity in self.webrtc.take_opened_peers() {
            log::info!(
                "[WebRTC] DataChannel opened for {}, firing peer_connected",
                &browser_identity[..browser_identity.len().min(8)]
            );
            // Spawn per-peer send task (same as production DcOpened handler)
            self.spawn_webrtc_peer_sender(&browser_identity);
            let Some(peer_command_tx) = self.webrtc.peer_command_sender(&browser_identity) else {
                log::warn!(
                    "[WebRTC] Test DataChannel opened for {} but peer sender was unavailable",
                    &browser_identity[..browser_identity.len().min(8)]
                );
                continue;
            };
            let worker =
                self.spawn_webrtc_client_worker_adapter(browser_identity.clone(), peer_command_tx);
            self.webrtc
                .register_client_worker_route(browser_identity.clone(), worker.clone());
            self.browser_client_workers
                .insert(browser_identity.clone(), worker);
            if let Err(e) = self.lua.call_peer_connected(&browser_identity) {
                log::warn!("[WebRTC] Lua peer_connected callback error: {e}");
            }
        }
    }

    pub(super) fn request_transport_ratchet_restart(&mut self, browser_identity: &str) {
        let Some(message) = self.webrtc.record_decrypt_failure(browser_identity) else {
            return;
        };
        log::warn!(
            "[RatchetRestart] Initiating restart for {}",
            &browser_identity[..browser_identity.len().min(8)]
        );
        self.handle_transport_control_message(message);
    }

    pub(super) fn send_ratchet_bundle_refresh(&mut self, browser_identity: &str) {
        let peer_olm_key = crate::relay::extract_olm_key(browser_identity).to_string();
        let Some(ref cs) = self.browser.crypto_service else {
            log::warn!("[RatchetRestart] No crypto service available");
            return;
        };

        let bundle_bytes = match cs.lock() {
            Ok(mut guard) => match guard.refresh_bundle_for_peer(&peer_olm_key) {
                Ok(bytes) => bytes,
                Err(e) => {
                    log::error!("[RatchetRestart] Failed to generate refresh bundle: {e}");
                    return;
                }
            },
            Err(e) => {
                log::error!("[RatchetRestart] Crypto mutex poisoned: {e}");
                return;
            }
        };

        // Send type 2 via DataChannel — non-blocking via per-peer send task
        self.queue_webrtc_peer_command(
            browser_identity,
            crate::worker::webrtc::WebRtcAdapterCommand::BundleRefresh {
                bundle_bytes: bundle_bytes.clone(),
            },
        );

        // Also send via ActionCable
        let envelope = serde_json::json!({
            "t": 2,
            "b": base64::engine::general_purpose::STANDARD_NO_PAD
                .encode(&bundle_bytes),
        });
        self.emit_outgoing_signal(&browser_identity, envelope, "bundle refresh");

        log::info!(
            "[RatchetRestart] Sent fresh bundle to {}",
            &browser_identity[..browser_identity.len().min(8)]
        );
    }

    pub(super) fn cleanup_webrtc_peer_registry(&mut self) {
        let scan_started = Instant::now();

        // Enter tokio runtime for channel state() calls
        let _guard = self.tokio_runtime.enter();

        // Timeout for connections stuck in "Connecting" state.
        // Keep this comfortably above the offer/answer happy path, but short
        // enough that failed negotiations do not force manual refreshes.
        const CONNECTION_TIMEOUT: Duration = Duration::from_secs(15);
        let to_cleanup = self.webrtc.cleanup_scan(CONNECTION_TIMEOUT);

        // Clean up stale channels
        for (browser_identity, reason) in to_cleanup {
            self.cleanup_webrtc_peer(&browser_identity, reason);
        }

        self.hub_event_metrics.record_span_with_threshold(
            "cleanup.webrtc_scan",
            scan_started.elapsed(),
            self.webrtc.channel_count(),
            Self::CLEANUP_SCAN_SLOW,
            "channels",
        );
    }

    pub(super) fn cleanup_webrtc_peer(&mut self, browser_identity: &str, reason: &str) {
        let cleanup_started = Instant::now();
        let reason_counter = match reason {
            "disconnected" => "cleanup.webrtc.reason.disconnected",
            "timeout" => "cleanup.webrtc.reason.timeout",
            "send_failed" => "cleanup.webrtc.reason.send_failed",
            "replaced" => "cleanup.webrtc.reason.replaced",
            _ => "cleanup.webrtc.reason.other",
        };
        self.hub_event_metrics.record_counter(reason_counter, 1);
        self.cleanup_pending_session_io_snapshots_for_peer(browser_identity);
        // Guard against duplicate cleanup calls. If the channel is already gone
        // this is a no-op — we must not fire peer_disconnected a second time or
        // the browser JS state machine will enter an unrecoverable state and
        // stop reconnecting.
        let disconnect_reason = match reason {
            "timeout" => crate::worker::hub_control::TransportDisconnectReason::ConnectionTimeout,
            "send_failed" => crate::worker::hub_control::TransportDisconnectReason::SendTimeout,
            "replaced" => crate::worker::hub_control::TransportDisconnectReason::ReplacedByNewPeer,
            "disconnected" => {
                crate::worker::hub_control::TransportDisconnectReason::DataChannelClose
            }
            _ => crate::worker::hub_control::TransportDisconnectReason::ExplicitDisconnect,
        };
        let generation = self.webrtc.current_offer_generation(browser_identity);
        let (cleanup, disconnected) = if let Some((cleanup, disconnected)) =
            self.webrtc.mark_data_channel_closed(
                browser_identity,
                generation,
                disconnect_reason,
                &self.tokio_runtime,
            ) {
            (cleanup, Some(disconnected))
        } else if self
            .webrtc
            .cleanup_peer_transport_state_only(browser_identity)
        {
            self.hub_event_metrics
                .record_counter("cleanup.webrtc.orphan_state", 1);
            (
                crate::worker::webrtc::TransportCleanup {
                    connected_age: None,
                },
                None,
            )
        } else {
            self.hub_event_metrics
                .record_counter("cleanup.webrtc.duplicate_skipped", 1);
            log::debug!(
                "[WebRTC] cleanup_webrtc_peer({}) called but channel already removed (duplicate skipped)",
                &browser_identity[..browser_identity.len().min(8)]
            );
            return;
        };

        log::info!(
            "[WebRTC] Cleaning up {} channel: {}",
            reason,
            &browser_identity[..browser_identity.len().min(8)]
        );
        if let Some(connected_age) = cleanup.connected_age {
            if connected_age <= Self::CLOSED_AFTER_CONNECT_WINDOW
                && matches!(reason, "disconnected" | "send_failed" | "timeout")
            {
                self.hub_event_metrics
                    .record_counter("webrtc_channel.closed_after_connect", 1);
                log::warn!(
                    "[WebRTC-Guardrail] event=closed_after_connect peer={} reason={} connected_age_ms={}",
                    &browser_identity[..browser_identity.len().min(24)],
                    reason,
                    connected_age.as_millis()
                );
            }
        }

        // Close and remove stream multiplexer for this browser
        if let Some(mut mux) = self.stream_muxes.remove(browser_identity) {
            mux.close_all();
            log::debug!(
                "[WebRTC] Closed stream multiplexer for {}",
                &browser_identity[..browser_identity.len().min(8)]
            );
        }

        // Abort any terminal subscriptions for this browser.
        // Subscription keys are "{peer_id}:{session_uuid}" where peer_id = browser_identity
        let peer_prefix = format!("{browser_identity}:");
        if let Some(worker) = self.browser_client_workers.remove(browser_identity) {
            let _ = worker.try_send(crate::worker::client::ClientWorkerMessage::Shutdown {
                reason: reason.to_string(),
            });
        }
        self.webrtc.unregister_client_worker_route(browser_identity);
        self.browser_terminal_attach_sizes
            .retain(|key, _| !key.starts_with(&peer_prefix));
        let subscription_keys: Vec<String> = self
            .terminal_subscription_peers
            .keys()
            .filter(|key| key.starts_with(&peer_prefix))
            .cloned()
            .collect();
        for key in subscription_keys {
            self.stop_terminal_subscription(&key);
        }
        let worker_keys: Vec<String> = self
            .terminal_client_workers
            .keys()
            .filter(|key| key.starts_with(&peer_prefix))
            .cloned()
            .collect();
        for key in worker_keys {
            if let Some(session_uuid) = key.strip_prefix(&peer_prefix).map(str::to_owned) {
                self.remove_terminal_client_worker(&key, &session_uuid, "WebRTC");
            }
        }
        self.pending_terminal_attaches.retain(|key, intent| {
            if key.starts_with(&peer_prefix) {
                intent.request.deactivate();
                log::debug!("[WebRTC] Dropped pending terminal attach intent: {}", key);
                false
            } else {
                true
            }
        });
        self.unregister_terminal_client_peer(browser_identity, true);

        if let Some(disconnected) = disconnected {
            self.handle_transport_control_message(disconnected);
        }

        if reason != "send_failed" || cleanup.connected_age.is_some() {
            // Notify Lua of peer disconnection (Lua handles subscription cleanup).
            // Orphan state cleanup may run after an earlier disconnect already
            // notified Lua, so only emit for real transport cleanup.
            if let Err(e) = self.lua.call_peer_disconnected(browser_identity) {
                log::warn!("[WebRTC] Lua peer_disconnected callback error: {e}");
            }
        }
        self.hub_event_metrics.record_span_with_threshold(
            "cleanup.webrtc_channel",
            cleanup_started.elapsed(),
            0,
            Self::HOT_SUBHANDLER_SLOW,
            browser_identity,
        );
    }

    pub(super) fn process_webrtc_plaintext_payload(
        &mut self,
        browser_identity: &str,
        payload: &[u8],
    ) {
        let parse_started = Instant::now();
        match self
            .webrtc
            .handle_plaintext_payload(browser_identity, payload)
        {
            crate::worker::webrtc::WebRtcIngressOutcome::ParseFailed => {
                self.hub_event_metrics
                    .record_counter("webrtc_message.parse_error", 1);
                self.record_hot_span(
                    "webrtc_message.parse_json",
                    parse_started,
                    payload.len(),
                    browser_identity,
                );
                return;
            }
            crate::worker::webrtc::WebRtcIngressOutcome::PongObserved => {
                self.record_hot_span(
                    "webrtc_message.parse_json",
                    parse_started,
                    payload.len(),
                    browser_identity,
                );
                log::trace!(
                    "[WebRTC] dc_pong from {}",
                    &browser_identity[..browser_identity.len().min(8)]
                );
                self.record_hot_span(
                    "webrtc_message.dc_pong",
                    Instant::now(),
                    payload.len(),
                    browser_identity,
                );
            }
            crate::worker::webrtc::WebRtcIngressOutcome::TerminalColorProfile(msg) => {
                self.record_hot_span(
                    "webrtc_message.parse_json",
                    parse_started,
                    payload.len(),
                    browser_identity,
                );
                self.handle_terminal_color_profile_message(browser_identity, &msg);
                self.record_hot_span(
                    "webrtc_message.terminal_color_profile",
                    Instant::now(),
                    payload.len(),
                    browser_identity,
                );
            }
            crate::worker::webrtc::WebRtcIngressOutcome::UnsupportedTerminalControl {
                message_type,
            } => {
                self.record_hot_span(
                    "webrtc_message.parse_json",
                    parse_started,
                    payload.len(),
                    browser_identity,
                );
                self.hub_event_metrics
                    .record_counter("webrtc_message.unsupported_terminal_control", 1);
                let _ = self.hub_event_tx.send(
                    crate::hub::events::HubEvent::WebRtcIngressBackpressure {
                        browser_identity: browser_identity.to_string(),
                        source: "webrtc_terminal_missing_session_uuid",
                    },
                );
                log::warn!(
                    "[WebRTC-MSG] Dropped terminal {} without session_uuid from {}",
                    message_type,
                    &browser_identity[..browser_identity.len().min(8)]
                );
            }
            crate::worker::webrtc::WebRtcIngressOutcome::LuaMessage(msg) => {
                self.record_hot_span(
                    "webrtc_message.parse_json",
                    parse_started,
                    payload.len(),
                    browser_identity,
                );
                let metric_label = Self::webrtc_lua_message_metric_label(browser_identity, &msg);
                let started = Instant::now();
                self.call_lua_webrtc_message(browser_identity, msg);
                self.record_hot_span("webrtc_message.lua", started, payload.len(), &metric_label);
            }
            crate::worker::webrtc::WebRtcIngressOutcome::ClientWorker(other) => {
                self.record_hot_span(
                    "webrtc_message.parse_json",
                    parse_started,
                    payload.len(),
                    browser_identity,
                );
                match &other {
                    crate::worker::client::ClientWorkerMessage::ControlFrame(
                        crate::worker::client::ClientControlFrame::FocusChanged {
                            session_uuid,
                            focused,
                        },
                    ) => {
                        let started = Instant::now();
                        if !session_uuid.is_empty() {
                            self.set_active_terminal_peer(session_uuid, browser_identity, *focused);
                            self.lua
                                .set_pty_focused(session_uuid, browser_identity, *focused);
                        }
                        self.record_hot_span(
                            "webrtc_message.focus_changed",
                            started,
                            payload.len(),
                            browser_identity,
                        );
                    }
                    crate::worker::client::ClientWorkerMessage::SubscribeSession {
                        session_uuid,
                        ..
                    } => {
                        let value = serde_json::from_slice::<serde_json::Value>(payload).ok();
                        let rows = value
                            .as_ref()
                            .and_then(|v| v.pointer("/params/rows"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(24)
                            .clamp(1, u16::MAX as u64) as u16;
                        let cols = value
                            .as_ref()
                            .and_then(|v| v.pointer("/params/cols"))
                            .and_then(|v| v.as_u64())
                            .unwrap_or(80)
                            .clamp(1, u16::MAX as u64) as u16;
                        self.browser_terminal_attach_sizes
                            .insert(format!("{browser_identity}:{session_uuid}"), (rows, cols));
                    }
                    _ => {}
                }

                if let Some(worker) = self.browser_client_workers.get(browser_identity) {
                    if let Err(e) = worker.try_send(other) {
                        log::warn!(
                            "[WebRTC-MSG] Browser worker queue rejected inbound message for {}: {}",
                            &browser_identity[..browser_identity.len().min(8)],
                            e
                        );
                    }
                } else {
                    log::warn!(
                        "[WebRTC-MSG] No browser worker for {}; dropping client-worker message",
                        &browser_identity[..browser_identity.len().min(8)]
                    );
                }
            }
        }
    }

    pub(super) fn call_lua_webrtc_message(
        &mut self,
        browser_identity: &str,
        msg: serde_json::Value,
    ) {
        // Call Lua callback
        if let Err(e) = self.lua.call_webrtc_message(browser_identity, msg) {
            self.hub_event_metrics
                .record_counter("webrtc_message.lua_error", 1);
            log::error!("[WebRTC-LUA] Lua callback error: {e}");
        }
    }

    fn webrtc_lua_message_metric_label(browser_identity: &str, msg: &serde_json::Value) -> String {
        let msg_type = msg
            .get("type")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown");
        let action_id = msg
            .get("envelope")
            .and_then(|value| value.get("id"))
            .and_then(|value| value.as_str());
        let detail = action_id.unwrap_or(msg_type);
        // Slow samples cap labels at 24 chars; keep the command/action detail first.
        format!(
            "{}:{}:{}",
            detail,
            msg_type,
            Self::short_ascii_prefix(browser_identity, 8)
        )
    }

    fn short_ascii_prefix(value: &str, max_chars: usize) -> &str {
        value
            .char_indices()
            .nth(max_chars)
            .map(|(idx, _)| &value[..idx])
            .unwrap_or(value)
    }

    pub(super) fn queue_webrtc_peer_command(
        &self,
        peer_id: &str,
        item: crate::worker::webrtc::WebRtcAdapterCommand,
    ) {
        self.webrtc
            .queue_command(peer_id, item, &self.hub_event_metrics);
    }

    pub(super) fn spawn_webrtc_peer_sender(&mut self, browser_identity: &str) {
        self.webrtc.spawn_peer_sender(
            browser_identity,
            &self.tokio_runtime,
            Arc::clone(&self.hub_event_metrics),
        );
    }

    pub(super) fn spawn_dc_ping_task(&mut self, browser_identity: &str) {
        self.webrtc
            .spawn_liveness_probe(browser_identity, &self.tokio_runtime);
    }

    #[cfg(test)]
    pub(super) fn poll_outgoing_webrtc_signals(&mut self) {
        use crate::channel::webrtc::OutgoingSignal;

        let mut rx = self.webrtc.lease_outgoing_signal_receiver_for_test();
        let Some(ref mut rx_ref) = rx else {
            return;
        };
        let signals: Vec<_> = std::iter::from_fn(|| rx_ref.try_recv().ok()).collect();
        self.webrtc.return_outgoing_signal_receiver_for_test(rx);
        for signal in signals {
            match signal {
                OutgoingSignal::Ice {
                    browser_identity,
                    envelope,
                } => {
                    self.emit_outgoing_signal(&browser_identity, envelope, "ICE candidate");
                    log::debug!(
                        "[Crypto] Relayed ICE candidate to browser {}",
                        &browser_identity[..browser_identity.len().min(8)]
                    );
                }
            }
        }
    }

    pub(super) fn start_webrtc_offer(&mut self, sdp: &str, browser_identity: &str) {
        let offer_started = Instant::now();
        if crate::env::is_offline() {
            log::warn!("[WebRTC] Rejecting offer — hub is in offline mode");
            return;
        }

        let encryption_disabled = crate::env::is_encryption_disabled();
        let crypto_service = if encryption_disabled {
            None
        } else {
            self.browser.crypto_service.clone()
        };
        if !encryption_disabled && crypto_service.is_none() {
            log::error!("[WebRTC] Rejecting encrypted offer because crypto is not ready");
            return;
        }

        let hub_id = self.server_hub_id().to_string();
        let server_url = self.config.server_url.clone();
        let api_key = self.config.get_api_key().to_string();

        log::info!(
            "[WebRTC] Received offer from {}",
            &browser_identity[..browser_identity.len().min(12)]
        );

        if !self.webrtc.has_channel(browser_identity) {
            // Clean up stale channels from the same device (same Olm key, different tab UUID).
            let replacement_started = Instant::now();
            let mut did_replacement_work = false;
            let olm_key = crate::relay::extract_olm_key(browser_identity);
            let stale = self.webrtc.same_device_channels(browser_identity);
            if !stale.is_empty() {
                did_replacement_work = true;
            }
            for stale_id in stale {
                log::info!(
                    "[WebRTC] Replacing stale channel for same device: {}",
                    &stale_id[..stale_id.len().min(8)]
                );
                self.cleanup_webrtc_peer(&stale_id, "replaced");
            }

            // Wait briefly for the previous connection's sockets to be released.
            match self.webrtc.wait_for_replaced_peer_close(
                olm_key,
                std::time::Duration::from_millis(100),
                &self.tokio_runtime,
            ) {
                crate::worker::webrtc::ReplacedPeerCloseWait::NoPendingClose => {}
                crate::worker::webrtc::ReplacedPeerCloseWait::AlreadyClosed => {
                    did_replacement_work = true;
                    log::debug!("[WebRTC] Previous connection already closed");
                }
                crate::worker::webrtc::ReplacedPeerCloseWait::Closed => {
                    did_replacement_work = true;
                    log::debug!("[WebRTC] Previous connection sockets released");
                }
                crate::worker::webrtc::ReplacedPeerCloseWait::ClosedChannelDropped => {
                    did_replacement_work = true;
                    log::debug!("[WebRTC] Close channel dropped, proceeding");
                }
                crate::worker::webrtc::ReplacedPeerCloseWait::TimedOut => {
                    did_replacement_work = true;
                    log::debug!("[WebRTC] Previous connection still closing, proceeding anyway");
                }
            }
            if did_replacement_work {
                self.hub_event_metrics.record_span_with_threshold(
                    "webrtc_offer.replacement",
                    replacement_started.elapsed(),
                    0,
                    Self::HOT_SUBHANDLER_SLOW,
                    browser_identity,
                );
            }
        }

        let request = crate::worker::webrtc::WebRtcOfferRequest {
            browser_identity: browser_identity.to_string(),
            sdp: sdp.to_string(),
            hub_id,
            server_url,
            api_key,
            crypto_service,
            outgoing_signal_tx: self.webrtc.outgoing_signal_tx(),
            stream_frame_tx: self.webrtc.stream_frame_tx(),
            hub_event_tx: self.hub_event_tx.clone(),
            pty_input_tx: self.webrtc.pty_input_tx(),
            file_input_tx: self.webrtc.file_input_tx(),
        };
        let channel_started = Instant::now();
        let start = self.webrtc.start_offer(request);
        self.hub_event_metrics.record_span_with_threshold(
            "webrtc_offer.start_channel",
            channel_started.elapsed(),
            sdp.len(),
            Self::HOT_SUBHANDLER_SLOW,
            browser_identity,
        );
        let event_tx = self.hub_event_tx.clone();

        // Spawn an async task for SDP negotiation and optional answer encryption.
        self.tokio_runtime.spawn(async move {
            let completion =
                crate::worker::webrtc::WebRtcTransportRunner::negotiate_offer(start).await;
            let _ = event_tx.send(crate::hub::events::HubEvent::WebRtcOfferNegotiated(
                completion,
            ));
        });
        self.hub_event_metrics.record_span_with_threshold(
            "webrtc_offer.dispatch",
            offer_started.elapsed(),
            sdp.len(),
            Self::HOT_SUBHANDLER_SLOW,
            browser_identity,
        );
    }

    #[cfg(test)]
    pub(super) fn poll_stream_frames_incoming(&mut self) {
        use crate::relay::stream_mux::StreamMultiplexer;

        let mut rx = self.webrtc.lease_stream_frame_receiver_for_test();
        let Some(ref mut rx_ref) = rx else {
            return;
        };
        let frames: Vec<crate::channel::webrtc::StreamIncoming> =
            std::iter::from_fn(|| rx_ref.try_recv().ok()).collect();
        self.webrtc.return_stream_frame_receiver_for_test(rx);

        if frames.is_empty() {
            return;
        }

        // handle_frame may call tokio::spawn, so we need a runtime context
        let _guard = self.tokio_runtime.enter();

        for frame in frames {
            let mux = self
                .stream_muxes
                .entry(frame.browser_identity.clone())
                .or_insert_with(StreamMultiplexer::new);

            mux.handle_frame(frame.frame_type, frame.stream_id, frame.payload);
        }
    }

    pub(crate) fn poll_stream_frames_outgoing(&mut self) {
        let browser_ids: Vec<String> = self.stream_muxes.keys().cloned().collect();

        for browser_identity in browser_ids {
            let frames = {
                let Some(mux) = self.stream_muxes.get_mut(&browser_identity) else {
                    continue;
                };
                mux.drain_output()
            };

            if frames.is_empty() {
                continue;
            }

            for frame in frames {
                self.queue_webrtc_peer_command(
                    &browser_identity,
                    crate::worker::webrtc::WebRtcAdapterCommand::Stream {
                        frame_type: frame.frame_type,
                        stream_id: frame.stream_id,
                        payload: frame.payload,
                    },
                );
            }
        }
    }
}
