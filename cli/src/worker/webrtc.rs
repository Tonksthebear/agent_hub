//! WebRTC transport adapter ownership.
//!
//! This module owns browser WebRTC peer state for the client-worker boundary.
//! The hub keeps orchestration policy, but per-peer channels, send queues,
//! reconnect generation, pending ICE, liveness tasks, and cleanup bookkeeping
//! live behind this registry.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::channel::Channel;
use crate::client::ClientId;

use super::client::{ClientControlFrame, ClientWorkerMessage};
use super::hub_control::HubControlMessage;
use super::transport::{TransportAdapter, TransportEgress, TransportIngress};

/// Capacity for outgoing ActionCable signaling envelopes from WebRTC tasks.
pub(crate) const WEBRTC_OUTGOING_SIGNAL_QUEUE_CAPACITY: usize = 512;
/// Capacity for incoming stream multiplexer frames.
pub(crate) const WEBRTC_STREAM_FRAME_QUEUE_CAPACITY: usize = 1024;
/// Capacity for binary PTY input frames from browsers.
pub(crate) const WEBRTC_PTY_INPUT_QUEUE_CAPACITY: usize = 2048;
/// Capacity for browser file-transfer frames.
pub(crate) const WEBRTC_FILE_INPUT_QUEUE_CAPACITY: usize = 128;
/// Capacity of the per-peer DataChannel send queue.
pub(crate) const PEER_SEND_CHANNEL_CAPACITY: usize = 256;
/// Timeout for individual DataChannel sends in per-peer tasks.
pub(crate) const PEER_SEND_TIMEOUT: Duration = Duration::from_secs(2);
/// Cooldown before sending a backpressure-recovery snapshot.
pub(crate) const BACKPRESSURE_SNAPSHOT_COOLDOWN: Duration = Duration::from_millis(500);
/// Maximum in-flight ICE candidate apply tasks per browser peer.
pub(crate) const WEBRTC_ICE_APPLY_IN_FLIGHT_PER_BROWSER: usize = 8;
/// Maximum queued ICE candidates replayed concurrently after an answer is ready.
///
/// Replay is a one-shot post-answer drain, so it gets a smaller budget than
/// steady-state ICE application while still avoiding serial mDNS timeout tails.
pub(crate) const WEBRTC_QUEUED_ICE_REPLAY_IN_FLIGHT_PER_BROWSER: usize = 4;

/// Item queued for a per-peer async WebRTC send task.
#[derive(Debug)]
pub(crate) enum WebRtcAdapterCommand {
    /// PTY output (hot path): subscription_id + raw data.
    Pty {
        /// Subscription ID for browser-side routing.
        subscription_id: String,
        /// Raw PTY data.
        data: Vec<u8>,
    },
    /// JSON control message.
    Json {
        /// Serialized JSON bytes.
        data: Vec<u8>,
    },
    /// Binary message.
    Binary {
        /// Raw binary data.
        data: Vec<u8>,
    },
    /// Stream multiplexer frame.
    Stream {
        /// Frame type byte.
        frame_type: u8,
        /// Stream identifier.
        stream_id: u16,
        /// Frame payload.
        payload: Vec<u8>,
    },
    /// Bundle refresh (ratchet restart, unencrypted).
    BundleRefresh {
        /// DeviceKeyBundle bytes.
        bundle_bytes: Vec<u8>,
    },
}

/// Per-peer send task state owned by the WebRTC registry.
pub(crate) struct PeerSendState {
    /// Bounded channel sender for queuing send items.
    pub tx: tokio::sync::mpsc::Sender<WebRtcAdapterCommand>,
    /// Set when the send task detects a dead peer.
    pub dead: Arc<AtomicBool>,
    /// Handle for the spawned send task.
    pub task: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for PeerSendState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerSendState")
            .field(
                "dead",
                &self.dead.load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

/// Bounded unknown-peer send burst guardrail state.
#[derive(Debug, Default)]
pub(crate) struct PeerBurstState {
    entries: VecDeque<(String, Instant)>,
    warned: HashSet<String>,
}

impl PeerBurstState {
    pub(crate) const WINDOW: Duration = Duration::from_secs(30);
    pub(crate) const THRESHOLD: usize = 10;
    pub(crate) const PEER_CAP: usize = 16;

    pub(crate) fn record(&mut self, peer_id: &str, now: Instant) -> Option<(String, usize)> {
        let prefix = peer_id.chars().take(24).collect::<String>();
        while self
            .entries
            .front()
            .is_some_and(|(_, at)| now.duration_since(*at) > Self::WINDOW)
        {
            if let Some((old_prefix, _)) = self.entries.pop_front() {
                if !self.entries.iter().any(|(p, _)| p == &old_prefix) {
                    self.warned.remove(&old_prefix);
                }
            }
        }

        let distinct = self.entries.iter().map(|(p, _)| p).collect::<HashSet<_>>();
        if distinct.len() >= Self::PEER_CAP && !distinct.contains(&prefix) {
            return None;
        }

        self.entries.push_back((prefix.clone(), now));
        let count = self.entries.iter().filter(|(p, _)| p == &prefix).count();
        if count >= Self::THRESHOLD && self.warned.insert(prefix.clone()) {
            Some((prefix, count))
        } else {
            None
        }
    }

    #[cfg(test)]
    pub(crate) fn distinct_peer_count(&self) -> usize {
        self.entries
            .iter()
            .map(|(peer, _)| peer)
            .collect::<HashSet<_>>()
            .len()
    }
}

/// Entry tracking a peer+session that needs a recovery snapshot.
#[derive(Debug, Clone)]
pub(crate) struct BackpressureRecoveryEntry {
    pub(crate) browser_identity: String,
    pub(crate) session_uuid: String,
    pub(crate) subscription_id: String,
    pub(crate) last_drop: Instant,
}

#[derive(Debug)]
pub(crate) struct WebRtcOfferRequest {
    pub(crate) browser_identity: String,
    pub(crate) sdp: String,
    pub(crate) hub_id: String,
    pub(crate) server_url: String,
    pub(crate) api_key: String,
    pub(crate) crypto_service: Option<crate::relay::CryptoService>,
    pub(crate) outgoing_signal_tx:
        tokio::sync::mpsc::Sender<crate::channel::webrtc::OutgoingSignal>,
    pub(crate) stream_frame_tx: tokio::sync::mpsc::Sender<crate::channel::webrtc::StreamIncoming>,
    pub(crate) hub_event_tx: crate::hub::events::HubEventTx,
    pub(crate) pty_input_tx: tokio::sync::mpsc::Sender<crate::channel::webrtc::PtyInputIncoming>,
    pub(crate) file_input_tx: tokio::sync::mpsc::Sender<crate::channel::webrtc::FileInputIncoming>,
}

#[derive(Debug)]
pub(crate) struct WebRtcOfferStart {
    pub(crate) browser_identity: String,
    pub(crate) sdp: String,
    pub(crate) generation: u64,
    pub(crate) channel: crate::channel::WebRtcChannel,
    pub(crate) config: crate::channel::ChannelConfig,
    pub(crate) crypto_service: Option<crate::relay::CryptoService>,
}

#[derive(Debug)]
pub(crate) struct WebRtcOfferCompletion {
    pub(crate) browser_identity: String,
    pub(crate) generation: u64,
    pub(crate) channel: crate::channel::WebRtcChannel,
    pub(crate) answer_signal: Option<serde_json::Value>,
}

#[derive(Debug)]
pub(crate) enum WebRtcOfferCompletionOutcome {
    AnswerReady {
        browser_identity: String,
        generation: u64,
        envelope: serde_json::Value,
        queued_ice: Vec<(u64, serde_json::Value)>,
    },
    StaleDropped {
        browser_identity: String,
        completed_generation: u64,
        current_generation: u64,
    },
    FailedCleaned {
        browser_identity: String,
        generation: u64,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum WebRtcIngressOutcome {
    PongObserved,
    TerminalColorProfile(serde_json::Value),
    UnsupportedTerminalControl { message_type: &'static str },
    LuaMessage(serde_json::Value),
    ClientWorker(ClientWorkerMessage),
    ParseFailed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WebRtcRecoverySnapshotRequest {
    pub(crate) request_id: String,
    pub(crate) browser_identity: String,
    pub(crate) session_uuid: String,
    pub(crate) subscription_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WebRtcRecoverySnapshotResult {
    Snapshot(Vec<u8>),
    PreparedSnapshot {
        uncompressed_len: usize,
        payload: Vec<u8>,
    },
    Empty,
    Failed,
}

/// Adapter-owned per-peer WebRTC registry.
#[derive(Debug)]
pub(crate) struct WebRtcPeerRegistry {
    channels: HashMap<String, crate::channel::WebRtcChannel>,
    connection_started: HashMap<String, Instant>,
    connected_at: HashMap<String, Instant>,
    send_tasks: HashMap<String, PeerSendState>,
    unknown_peer_bursts: std::sync::Mutex<PeerBurstState>,
    ping_tasks: HashMap<String, tokio::task::JoinHandle<()>>,
    pending_closes: HashMap<String, tokio::sync::watch::Receiver<bool>>,
    offer_generation: HashMap<String, u64>,
    pending_ice_candidates: HashMap<String, Vec<(u64, serde_json::Value)>>,
    ice_apply_limits: HashMap<String, Arc<tokio::sync::Semaphore>>,
    ratchet_restarted_peers: HashSet<String>,
    backpressure_recovery: HashMap<String, BackpressureRecoveryEntry>,
    outgoing_signal_tx: tokio::sync::mpsc::Sender<crate::channel::webrtc::OutgoingSignal>,
    outgoing_signal_rx: Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::OutgoingSignal>>,
    stream_frame_rx: Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::StreamIncoming>>,
    stream_frame_tx: tokio::sync::mpsc::Sender<crate::channel::webrtc::StreamIncoming>,
    pty_input_rx: Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::PtyInputIncoming>>,
    pty_input_tx: tokio::sync::mpsc::Sender<crate::channel::webrtc::PtyInputIncoming>,
    file_input_rx: Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::FileInputIncoming>>,
    file_input_tx: tokio::sync::mpsc::Sender<crate::channel::webrtc::FileInputIncoming>,
    client_routes:
        std::sync::Arc<std::sync::Mutex<HashMap<String, super::client::ClientWorkerHandle>>>,
    queue_forwarders: Vec<tokio::task::JoinHandle<()>>,
    runners: HashMap<String, WebRtcTransportRunner>,
}

#[derive(Debug, Clone, serde::Serialize, PartialEq, Eq)]
pub(crate) struct WebRtcPeerDiagnostics {
    pub(crate) channels: usize,
    pub(crate) connecting: usize,
    pub(crate) connected: usize,
    pub(crate) reconnecting: usize,
    pub(crate) disconnected: usize,
    pub(crate) errored: usize,
    pub(crate) send_tasks: usize,
    pub(crate) dead_send_tasks: usize,
    pub(crate) ping_tasks: usize,
    pub(crate) pending_closes: usize,
    pub(crate) pending_ice_peers: usize,
    pub(crate) pending_ice_candidates: usize,
    pub(crate) client_worker_routes: usize,
    pub(crate) runners: usize,
    pub(crate) backpressure_recovery_pending: usize,
}

/// Async peer runner ownership marker for WebRTC transport mechanics.
///
/// The registry creates runners for connected peers. Hub policy receives typed
/// summaries and keeps orchestration decisions outside the data-plane owner.
#[derive(Debug, Clone)]
pub(crate) struct WebRtcTransportRunner {
    pub(crate) client_id: ClientId,
    pub(crate) browser_identity: String,
    pub(crate) generation: u64,
    pub(crate) command_tx: tokio::sync::mpsc::Sender<WebRtcAdapterCommand>,
    adapter: WebRtcTransportAdapter,
}

impl WebRtcTransportRunner {
    #[must_use]
    pub(crate) fn new(
        client_id: ClientId,
        browser_identity: String,
        generation: u64,
        command_tx: tokio::sync::mpsc::Sender<WebRtcAdapterCommand>,
    ) -> Self {
        let adapter = WebRtcTransportAdapter::new(client_id.clone());
        Self {
            client_id,
            browser_identity,
            generation,
            command_tx,
            adapter,
        }
    }

    pub(crate) fn ingress_to_client(&self, ingress: TransportIngress) -> ClientWorkerMessage {
        let _runner_scope = (
            &self.client_id,
            &self.browser_identity,
            self.generation,
            self.command_tx.capacity(),
        );
        self.adapter.ingress_to_client(ingress)
    }

    pub(crate) fn handle_plaintext_payload(&self, payload: &[u8]) -> WebRtcIngressOutcome {
        let msg = match serde_json::from_slice::<serde_json::Value>(payload) {
            Ok(value) => value,
            Err(error) => {
                log::error!("[WebRTC-MSG] JSON parse failed: {error}");
                return WebRtcIngressOutcome::ParseFailed;
            }
        };

        let msg_type = msg.get("type").and_then(|t| t.as_str());
        let ingress = match msg_type {
            Some("dc_ping") => TransportIngress::DcPing,
            Some("dc_pong") => TransportIngress::DcPong,
            Some("subscribe")
                if msg.get("channel").and_then(|v| v.as_str()) == Some("terminal") =>
            {
                let Some(session_uuid) = msg
                    .pointer("/params/session_uuid")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                else {
                    return WebRtcIngressOutcome::LuaMessage(msg);
                };
                let subscription_id = msg
                    .get("subscriptionId")
                    .or_else(|| msg.get("subscription_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(&session_uuid)
                    .to_string();
                TransportIngress::Subscribe {
                    session_uuid,
                    subscription_id,
                }
            }
            Some("unsubscribe") => {
                let subscription_id = msg
                    .get("subscriptionId")
                    .or_else(|| msg.get("subscription_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let session_uuid = msg
                    .get("session_uuid")
                    .or_else(|| msg.pointer("/params/session_uuid"))
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                    .or_else(|| {
                        subscription_id
                            .strip_prefix("terminal_")
                            .map(ToString::to_string)
                    });
                let Some(session_uuid) = session_uuid else {
                    return WebRtcIngressOutcome::LuaMessage(msg);
                };
                TransportIngress::Unsubscribe {
                    session_uuid,
                    subscription_id,
                }
            }
            Some("focus_changed") => {
                let session_uuid = msg
                    .get("session_uuid")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();
                let focused = msg
                    .get("focused")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                TransportIngress::FocusChanged {
                    session_uuid,
                    focused,
                }
            }
            Some("resize") => {
                let Some(session_uuid) = msg
                    .get("session_uuid")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                else {
                    return WebRtcIngressOutcome::UnsupportedTerminalControl {
                        message_type: "resize",
                    };
                };
                let rows = msg
                    .get("rows")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(24)
                    .clamp(1, u16::MAX as u64) as u16;
                let cols = msg
                    .get("cols")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(80)
                    .clamp(1, u16::MAX as u64) as u16;
                TransportIngress::TerminalResize {
                    session_uuid,
                    rows,
                    cols,
                }
            }
            Some("request_snapshot") => {
                let Some(session_uuid) = msg
                    .get("session_uuid")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                else {
                    return WebRtcIngressOutcome::UnsupportedTerminalControl {
                        message_type: "request_snapshot",
                    };
                };
                let rows = msg
                    .get("rows")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(24)
                    .clamp(1, u16::MAX as u64) as u16;
                let cols = msg
                    .get("cols")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(80)
                    .clamp(1, u16::MAX as u64) as u16;
                TransportIngress::RequestSnapshot {
                    session_uuid,
                    rows,
                    cols,
                }
            }
            _ => TransportIngress::BoundaryJson(msg),
        };

        let client_message = self.ingress_to_client(ingress);
        let msg = match client_message {
            ClientWorkerMessage::ControlFrame(ClientControlFrame::BoundaryJson(value)) => value,
            ClientWorkerMessage::ControlFrame(ClientControlFrame::DcPong) => {
                return WebRtcIngressOutcome::ClientWorker(ClientWorkerMessage::ControlFrame(
                    ClientControlFrame::DcPong,
                ));
            }
            ClientWorkerMessage::ControlFrame(ClientControlFrame::DcPongReceived) => {
                return WebRtcIngressOutcome::PongObserved;
            }
            other => return WebRtcIngressOutcome::ClientWorker(other),
        };

        match msg.get("type").and_then(|t| t.as_str()) {
            Some("terminal_color_profile") => WebRtcIngressOutcome::TerminalColorProfile(msg),
            _ => WebRtcIngressOutcome::LuaMessage(msg),
        }
    }

    /// Connect the WebRTC channel and process the browser offer.
    pub(crate) async fn negotiate_offer(start: WebRtcOfferStart) -> WebRtcOfferCompletion {
        let WebRtcOfferStart {
            browser_identity,
            sdp,
            generation,
            mut channel,
            config,
            crypto_service,
        } = start;
        let started_at = Instant::now();
        let answer_signal = if let Err(error) = channel.connect(config).await {
            log::error!(
                "[WebRTC] Failed to configure channel before offer handling after {}ms: {error}",
                started_at.elapsed().as_millis()
            );
            None
        } else {
            Self::handle_offer_and_build_answer(
                &channel,
                &browser_identity,
                &sdp,
                crypto_service.as_ref(),
                started_at,
            )
            .await
        };

        WebRtcOfferCompletion {
            browser_identity,
            generation,
            channel,
            answer_signal,
        }
    }

    async fn handle_offer_and_build_answer(
        channel: &crate::channel::WebRtcChannel,
        browser_identity: &str,
        sdp: &str,
        crypto_service: Option<&crate::relay::CryptoService>,
        started_at: Instant,
    ) -> Option<serde_json::Value> {
        let olm_key = crate::relay::extract_olm_key(&browser_identity).to_string();
        match channel.handle_sdp_offer(sdp, browser_identity).await {
            Ok(answer_sdp) => {
                log::info!(
                    "[WebRTC] Created answer for {} in {}ms",
                    &browser_identity[..browser_identity.len().min(12)],
                    started_at.elapsed().as_millis()
                );
                let answer_payload = serde_json::json!({
                    "type": "answer",
                    "sdp": answer_sdp,
                });
                let Some(crypto_service) = crypto_service else {
                    return Some(answer_payload);
                };
                let plaintext = serde_json::to_vec(&answer_payload).unwrap_or_default();
                match crypto_service.lock() {
                    Ok(mut guard) => match guard.encrypt(&plaintext, &olm_key) {
                        Ok(envelope) => match serde_json::to_value(&envelope) {
                            Ok(value) => Some(value),
                            Err(error) => {
                                log::error!(
                                    "[WebRTC] Failed to serialize answer envelope: {error}"
                                );
                                None
                            }
                        },
                        Err(error) => {
                            log::error!(
                                "[WebRTC] Failed to encrypt answer after {}ms: {error}",
                                started_at.elapsed().as_millis()
                            );
                            None
                        }
                    },
                    Err(error) => {
                        log::error!(
                            "[WebRTC] Crypto mutex poisoned after {}ms: {error}",
                            started_at.elapsed().as_millis()
                        );
                        None
                    }
                }
            }
            Err(error) => {
                log::error!(
                    "[WebRTC] Failed to handle offer after {}ms: {error}",
                    started_at.elapsed().as_millis()
                );
                None
            }
        }
    }
}

impl WebRtcPeerRegistry {
    /// Build a registry with all hot-path queues bounded.
    #[must_use]
    pub(crate) fn new() -> Self {
        let (outgoing_signal_tx, outgoing_signal_rx) =
            tokio::sync::mpsc::channel(WEBRTC_OUTGOING_SIGNAL_QUEUE_CAPACITY);
        let (stream_frame_tx, stream_frame_rx) =
            tokio::sync::mpsc::channel(WEBRTC_STREAM_FRAME_QUEUE_CAPACITY);
        let (pty_input_tx, pty_input_rx) =
            tokio::sync::mpsc::channel(WEBRTC_PTY_INPUT_QUEUE_CAPACITY);
        let (file_input_tx, file_input_rx) =
            tokio::sync::mpsc::channel(WEBRTC_FILE_INPUT_QUEUE_CAPACITY);

        Self {
            channels: HashMap::new(),
            connection_started: HashMap::new(),
            connected_at: HashMap::new(),
            send_tasks: HashMap::new(),
            unknown_peer_bursts: std::sync::Mutex::new(PeerBurstState::default()),
            ping_tasks: HashMap::new(),
            pending_closes: HashMap::new(),
            offer_generation: HashMap::new(),
            pending_ice_candidates: HashMap::new(),
            ice_apply_limits: HashMap::new(),
            ratchet_restarted_peers: HashSet::new(),
            backpressure_recovery: HashMap::new(),
            outgoing_signal_tx,
            outgoing_signal_rx: Some(outgoing_signal_rx),
            stream_frame_rx: Some(stream_frame_rx),
            stream_frame_tx,
            pty_input_rx: Some(pty_input_rx),
            pty_input_tx,
            file_input_rx: Some(file_input_rx),
            file_input_tx,
            client_routes: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            queue_forwarders: Vec::new(),
            runners: HashMap::new(),
        }
    }

    pub(crate) fn outgoing_signal_tx(
        &self,
    ) -> tokio::sync::mpsc::Sender<crate::channel::webrtc::OutgoingSignal> {
        self.outgoing_signal_tx.clone()
    }

    pub(crate) fn stream_frame_tx(
        &self,
    ) -> tokio::sync::mpsc::Sender<crate::channel::webrtc::StreamIncoming> {
        self.stream_frame_tx.clone()
    }

    pub(crate) fn pty_input_tx(
        &self,
    ) -> tokio::sync::mpsc::Sender<crate::channel::webrtc::PtyInputIncoming> {
        self.pty_input_tx.clone()
    }

    pub(crate) fn file_input_tx(
        &self,
    ) -> tokio::sync::mpsc::Sender<crate::channel::webrtc::FileInputIncoming> {
        self.file_input_tx.clone()
    }

    pub(crate) fn register_client_worker_route(
        &self,
        browser_identity: String,
        worker: super::client::ClientWorkerHandle,
    ) {
        if let Ok(mut routes) = self.client_routes.lock() {
            routes.insert(browser_identity, worker);
        }
    }

    pub(crate) fn unregister_client_worker_route(&self, browser_identity: &str) {
        if let Ok(mut routes) = self.client_routes.lock() {
            routes.remove(browser_identity);
        }
    }

    pub(crate) fn start_queue_forwarders(
        &mut self,
        runtime: &tokio::runtime::Runtime,
        hub_event_tx: crate::hub::events::HubEventTx,
    ) {
        if !self.queue_forwarders.is_empty() {
            return;
        }

        if let Some(mut rx) = self.pty_input_rx.take() {
            let routes = self.client_routes.clone();
            let backpressure_tx = hub_event_tx.clone();
            self.queue_forwarders.push(runtime.spawn(async move {
                while let Some(input) = rx.recv().await {
                    let Some(worker) = routes
                        .lock()
                        .ok()
                        .and_then(|routes| routes.get(&input.browser_identity).cloned())
                    else {
                        continue;
                    };
                    match worker.try_send(super::client::ClientWorkerMessage::SessionInput {
                        session_uuid: input.session_uuid,
                        data: input.data,
                    }) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            let _ = backpressure_tx.send(
                                crate::hub::events::HubEvent::WebRtcIngressBackpressure {
                                    browser_identity: input.browser_identity,
                                    source: "worker.client.webrtc.input",
                                },
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
            }));
        }
        if let Some(mut rx) = self.file_input_rx.take() {
            let routes = self.client_routes.clone();
            let backpressure_tx = hub_event_tx.clone();
            self.queue_forwarders.push(runtime.spawn(async move {
                while let Some(file) = rx.recv().await {
                    let browser_identity = file.browser_identity.clone();
                    let Some(worker) = routes
                        .lock()
                        .ok()
                        .and_then(|routes| routes.get(&browser_identity).cloned())
                    else {
                        continue;
                    };
                    match worker.try_send(super::client::ClientWorkerMessage::PasteFile {
                        session_uuid: file.session_uuid,
                        filename: file.filename,
                        data: file.data,
                    }) {
                        Ok(()) => {}
                        Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                            let _ = backpressure_tx.send(
                                crate::hub::events::HubEvent::WebRtcIngressBackpressure {
                                    browser_identity,
                                    source: "worker.client.webrtc.file_input",
                                },
                            );
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {}
                    }
                }
            }));
        }
        if let Some(mut rx) = self.outgoing_signal_rx.take() {
            let tx = hub_event_tx.clone();
            self.queue_forwarders.push(runtime.spawn(async move {
                while let Some(signal) = rx.recv().await {
                    if tx
                        .send(crate::hub::events::HubEvent::WebRtcOutgoingSignal(signal))
                        .is_err()
                    {
                        break;
                    }
                }
            }));
        }
        if let Some(mut rx) = self.stream_frame_rx.take() {
            self.queue_forwarders.push(runtime.spawn(async move {
                while let Some(frame) = rx.recv().await {
                    if hub_event_tx
                        .send(crate::hub::events::HubEvent::WebRtcStreamFrame(frame))
                        .is_err()
                    {
                        break;
                    }
                }
            }));
        }
    }

    #[cfg(test)]
    pub(crate) fn lease_outgoing_signal_receiver_for_test(
        &mut self,
    ) -> Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::OutgoingSignal>> {
        self.outgoing_signal_rx.take()
    }

    #[cfg(test)]
    pub(crate) fn return_outgoing_signal_receiver_for_test(
        &mut self,
        rx: Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::OutgoingSignal>>,
    ) {
        self.outgoing_signal_rx = rx;
    }

    #[cfg(test)]
    pub(crate) fn lease_stream_frame_receiver_for_test(
        &mut self,
    ) -> Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::StreamIncoming>> {
        self.stream_frame_rx.take()
    }

    #[cfg(test)]
    pub(crate) fn return_stream_frame_receiver_for_test(
        &mut self,
        rx: Option<tokio::sync::mpsc::Receiver<crate::channel::webrtc::StreamIncoming>>,
    ) {
        self.stream_frame_rx = rx;
    }

    pub(crate) fn send_item_len(item: &WebRtcAdapterCommand) -> usize {
        match item {
            WebRtcAdapterCommand::Pty { data, .. }
            | WebRtcAdapterCommand::Json { data }
            | WebRtcAdapterCommand::Binary { data }
            | WebRtcAdapterCommand::Stream { payload: data, .. } => data.len(),
            WebRtcAdapterCommand::BundleRefresh { bundle_bytes } => bundle_bytes.len(),
        }
    }

    pub(crate) fn channel_count(&self) -> usize {
        self.channels.len()
    }

    pub(crate) fn diagnostics(&self) -> WebRtcPeerDiagnostics {
        use crate::channel::ConnectionState;

        let mut connecting = 0;
        let mut connected = 0;
        let mut reconnecting = 0;
        let mut disconnected = 0;
        let mut errored = 0;
        for channel in self.channels.values() {
            match channel.state() {
                ConnectionState::Connecting => connecting += 1,
                ConnectionState::Connected => connected += 1,
                ConnectionState::Reconnecting { .. } => reconnecting += 1,
                ConnectionState::Disconnected => disconnected += 1,
                ConnectionState::Error(_) => errored += 1,
            }
        }

        WebRtcPeerDiagnostics {
            channels: self.channels.len(),
            connecting,
            connected,
            reconnecting,
            disconnected,
            errored,
            send_tasks: self.send_tasks.len(),
            dead_send_tasks: self
                .send_tasks
                .values()
                .filter(|state| state.dead.load(std::sync::atomic::Ordering::Relaxed))
                .count(),
            ping_tasks: self.ping_tasks.len(),
            pending_closes: self.pending_closes.len(),
            pending_ice_peers: self.pending_ice_candidates.len(),
            pending_ice_candidates: self.pending_ice_candidates.values().map(Vec::len).sum(),
            client_worker_routes: self
                .client_routes
                .lock()
                .map(|routes| routes.len())
                .unwrap_or(0),
            runners: self.runners.len(),
            backpressure_recovery_pending: self.backpressure_recovery.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn unknown_peer_distinct_count(&self) -> usize {
        self.unknown_peer_bursts
            .lock()
            .expect("unknown peer burst mutex poisoned")
            .distinct_peer_count()
    }

    pub(crate) fn has_channel(&self, browser_identity: &str) -> bool {
        self.channels.contains_key(browser_identity)
    }

    pub(crate) fn mark_data_channel_open(
        &mut self,
        browser_identity: &str,
        generation: u64,
    ) -> Option<HubControlMessage> {
        if !self.channels.contains_key(browser_identity) {
            return None;
        }
        if self.current_offer_generation(browser_identity) != generation {
            return None;
        }
        self.connected_at
            .insert(browser_identity.to_string(), Instant::now());
        Some(HubControlMessage::TransportPeerStateChanged {
            client_id: ClientId::browser(browser_identity.to_string()),
            browser_identity: browser_identity.to_string(),
            state: super::hub_control::TransportPeerState::Connected {
                generation,
                mode: super::hub_control::TransportConnectionMode::Unknown,
            },
        })
    }

    pub(crate) fn mark_data_channel_closed(
        &mut self,
        browser_identity: &str,
        generation: u64,
        reason: super::hub_control::TransportDisconnectReason,
        runtime: &tokio::runtime::Runtime,
    ) -> Option<(TransportCleanup, HubControlMessage)> {
        let cleanup = self.cleanup_peer_transport(browser_identity, runtime)?;
        Some((
            cleanup,
            HubControlMessage::TransportPeerStateChanged {
                client_id: ClientId::browser(browser_identity.to_string()),
                browser_identity: browser_identity.to_string(),
                state: super::hub_control::TransportPeerState::Disconnected { generation, reason },
            },
        ))
    }

    pub(crate) fn same_device_channels(&self, browser_identity: &str) -> Vec<String> {
        let olm_key = crate::relay::extract_olm_key(browser_identity);
        self.channels
            .keys()
            .filter(|id| {
                id.as_str() != browser_identity && crate::relay::extract_olm_key(id) == olm_key
            })
            .cloned()
            .collect()
    }

    pub(crate) fn wait_for_replaced_peer_close(
        &mut self,
        olm_key: &str,
        timeout: Duration,
        runtime: &tokio::runtime::Runtime,
    ) -> ReplacedPeerCloseWait {
        let Some(mut close_rx) = self.pending_closes.remove(olm_key) else {
            return ReplacedPeerCloseWait::NoPendingClose;
        };
        if *close_rx.borrow() {
            return ReplacedPeerCloseWait::AlreadyClosed;
        }
        let wait_result = tokio::task::block_in_place(|| {
            runtime
                .block_on(async { tokio::time::timeout(timeout, close_rx.wait_for(|v| *v)).await })
        });
        match wait_result {
            Ok(Ok(_)) => ReplacedPeerCloseWait::Closed,
            Ok(Err(_)) => ReplacedPeerCloseWait::ClosedChannelDropped,
            Err(_) => ReplacedPeerCloseWait::TimedOut,
        }
    }

    pub(crate) fn next_offer_generation(&mut self, browser_identity: &str) -> u64 {
        let entry = self
            .offer_generation
            .entry(browser_identity.to_string())
            .or_insert(0);
        *entry += 1;
        *entry
    }

    pub(crate) fn current_offer_generation(&self, browser_identity: &str) -> u64 {
        self.offer_generation
            .get(browser_identity)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn start_offer(&mut self, request: WebRtcOfferRequest) -> WebRtcOfferStart {
        use crate::channel::{ChannelConfig, WebRtcChannel};

        let generation = self.next_offer_generation(&request.browser_identity);
        let mut builder = WebRtcChannel::builder()
            .server_url(&request.server_url)
            .api_key(&request.api_key)
            .signal_tx(request.outgoing_signal_tx)
            .stream_frame_tx(request.stream_frame_tx)
            .hub_event_tx(request.hub_event_tx)
            .pty_input_tx(request.pty_input_tx)
            .file_input_tx(request.file_input_tx);

        if let Some(ref crypto_service) = request.crypto_service {
            builder = builder.crypto_service(Arc::clone(crypto_service));
        }

        let channel = builder.build();
        channel.set_offer_generation(generation);
        let config = ChannelConfig {
            channel_name: "WebRtcChannel".to_string(),
            hub_id: request.hub_id,
            browser_identity: Some(request.browser_identity.clone()),
            encrypt: request.crypto_service.is_some(),
            compression_threshold: Some(4096),
            cli_subscription: false,
        };

        self.connection_started
            .insert(request.browser_identity.clone(), Instant::now());

        WebRtcOfferStart {
            browser_identity: request.browser_identity,
            sdp: request.sdp,
            generation,
            channel,
            config,
            crypto_service: request.crypto_service,
        }
    }

    pub(crate) fn complete_offer(
        &mut self,
        completion: WebRtcOfferCompletion,
        runtime: &tokio::runtime::Runtime,
    ) -> WebRtcOfferCompletionOutcome {
        let WebRtcOfferCompletion {
            browser_identity,
            generation,
            mut channel,
            answer_signal,
        } = completion;
        let current_generation = self.current_offer_generation(&browser_identity);
        if generation != current_generation {
            let stale_browser_identity = browser_identity.clone();
            runtime.spawn(async move {
                channel.disconnect().await;
            });
            return WebRtcOfferCompletionOutcome::StaleDropped {
                browser_identity: stale_browser_identity,
                completed_generation: generation,
                current_generation,
            };
        }

        let Some(envelope) = answer_signal else {
            self.clear_offer_state(&browser_identity);
            let failed_browser_identity = browser_identity.clone();
            runtime.spawn(async move {
                channel.disconnect().await;
            });
            return WebRtcOfferCompletionOutcome::FailedCleaned {
                browser_identity: failed_browser_identity,
                generation,
            };
        };

        if let Some(mut replaced) = self.channels.insert(browser_identity.clone(), channel) {
            runtime.spawn(async move {
                replaced.disconnect().await;
            });
        }
        let queued_ice = self
            .pending_ice_candidates
            .remove(&browser_identity)
            .unwrap_or_default();
        WebRtcOfferCompletionOutcome::AnswerReady {
            browser_identity,
            generation,
            envelope,
            queued_ice,
        }
    }

    pub(crate) fn clear_offer_state(&mut self, browser_identity: &str) {
        self.connection_started.remove(browser_identity);
        self.offer_generation.remove(browser_identity);
        self.pending_ice_candidates.remove(browser_identity);
        self.ice_apply_limits.remove(browser_identity);
    }

    pub(crate) fn queue_ice_for_current_generation(
        &mut self,
        browser_identity: &str,
        candidate: serde_json::Value,
        max_queued: usize,
    ) -> Option<usize> {
        let current_generation = self.current_offer_generation(browser_identity);
        if current_generation == 0 {
            return None;
        }
        let queue = self
            .pending_ice_candidates
            .entry(browser_identity.to_string())
            .or_default();
        queue.push((current_generation, candidate));
        if queue.len() > max_queued {
            let dropped = queue.len() - max_queued;
            queue.drain(..dropped);
        }
        Some(queue.len())
    }

    pub(crate) fn queue_or_apply_ice<F>(
        &mut self,
        browser_identity: &str,
        candidate: serde_json::Value,
        max_queued: usize,
        runtime: &tokio::runtime::Runtime,
        on_error: F,
    ) -> QueueOrApplyIceOutcome
    where
        F: FnOnce(&str, Option<&str>, Option<u16>, crate::channel::ChannelError) + Send + 'static,
    {
        let candidate_str = candidate
            .get("candidate")
            .and_then(|c| c.as_str())
            .unwrap_or("");
        if candidate_str.is_empty() {
            return QueueOrApplyIceOutcome::IgnoredEmpty;
        }

        let sdp_mid = candidate.get("sdpMid").and_then(|m| m.as_str());
        let sdp_mline_index = candidate
            .get("sdpMLineIndex")
            .and_then(|i| i.as_u64())
            .map(|i| i as u16);

        if self.spawn_browser_ice_candidate_apply(
            browser_identity,
            candidate_str,
            sdp_mid,
            sdp_mline_index,
            runtime,
            on_error,
        ) {
            return QueueOrApplyIceOutcome::ApplySpawned;
        } else if self.channels.contains_key(browser_identity) {
            return QueueOrApplyIceOutcome::ApplyBackpressure;
        }

        self.queue_ice_for_current_generation(browser_identity, candidate, max_queued)
            .map_or(
                QueueOrApplyIceOutcome::UnknownBrowser,
                QueueOrApplyIceOutcome::Queued,
            )
    }

    #[cfg(test)]
    pub(crate) fn drain_pending_ice(
        &mut self,
        browser_identity: &str,
    ) -> Option<Vec<(u64, serde_json::Value)>> {
        self.pending_ice_candidates.remove(browser_identity)
    }

    pub(crate) fn start_recv_forwarder(
        &self,
        browser_identity: &str,
        runtime: &tokio::runtime::Runtime,
        hub_event_tx: crate::hub::events::HubEventTx,
    ) -> bool {
        let Some(channel) = self.channels.get(browser_identity) else {
            return false;
        };

        let recv_rx_arc = channel.recv_rx_arc();
        let bi = browser_identity.to_string();
        let handle = runtime.handle().clone();
        handle.spawn(async move {
            let mut rx = {
                let mut guard = recv_rx_arc.lock().await;
                match guard.take() {
                    Some(rx) => rx,
                    None => return,
                }
            };
            while let Some(raw) = rx.recv().await {
                if hub_event_tx
                    .send(crate::hub::events::HubEvent::WebRtcMessage {
                        browser_identity: bi.clone(),
                        payload: raw.payload,
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        true
    }

    /// Spawn bounded ICE application work for an active peer.
    ///
    /// Candidate application is intentionally unordered; callers must not rely
    /// on FIFO behavior for candidate side effects.
    pub(crate) fn spawn_browser_ice_candidate_apply<F>(
        &mut self,
        browser_identity: &str,
        candidate_str: &str,
        sdp_mid: Option<&str>,
        sdp_mline_index: Option<u16>,
        runtime: &tokio::runtime::Runtime,
        on_error: F,
    ) -> bool
    where
        F: FnOnce(&str, Option<&str>, Option<u16>, crate::channel::ChannelError) + Send + 'static,
    {
        let Some(channel) = self.channels.get(browser_identity).cloned() else {
            return false;
        };
        let limit = Arc::clone(
            self.ice_apply_limits
                .entry(browser_identity.to_string())
                .or_insert_with(|| {
                    Arc::new(tokio::sync::Semaphore::new(
                        WEBRTC_ICE_APPLY_IN_FLIGHT_PER_BROWSER,
                    ))
                }),
        );
        let Ok(permit) = limit.try_acquire_owned() else {
            return false;
        };
        let candidate_str = candidate_str.to_string();
        let sdp_mid = sdp_mid.map(String::from);
        runtime.spawn(async move {
            let _permit = permit;
            if let Err(error) = channel
                .handle_ice_candidate(&candidate_str, sdp_mid.as_deref(), sdp_mline_index)
                .await
            {
                on_error(&candidate_str, sdp_mid.as_deref(), sdp_mline_index, error);
            }
        });
        true
    }

    pub(crate) fn apply_queued_ice_for_offer<F>(
        &self,
        browser_identity: &str,
        offer_generation: u64,
        candidates: Vec<(u64, serde_json::Value)>,
        runtime: &tokio::runtime::Runtime,
        mut on_error: F,
    ) where
        F: FnMut(u64, &str, Option<&str>, Option<u16>, crate::channel::ChannelError)
            + Send
            + 'static,
    {
        // Queued ICE drains through one serial task after AnswerReady, so it is
        // already bounded separately from the per-candidate fresh ICE gate.
        let Some(channel) = self.channels.get(browser_identity).cloned() else {
            return;
        };

        let mut valid: Vec<_> = candidates
            .into_iter()
            .filter_map(|(candidate_generation, candidate)| {
                if candidate_generation != offer_generation {
                    log::debug!(
                        "[WebRTC] Dropping stale queued ICE candidate for {} (candidate gen {}, current gen {})",
                        &browser_identity[..browser_identity.len().min(8)],
                        candidate_generation,
                        offer_generation
                    );
                    return None;
                }
                let candidate_str = candidate
                    .get("candidate")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string();
                if candidate_str.is_empty() {
                    return None;
                }
                let sdp_mid = candidate
                    .get("sdpMid")
                    .and_then(|m| m.as_str())
                    .map(String::from);
                let sdp_mline_index = candidate
                    .get("sdpMLineIndex")
                    .and_then(|i| i.as_u64())
                    .map(|i| i as u16);
                Some((
                    candidate_generation,
                    candidate_str,
                    sdp_mid,
                    sdp_mline_index,
                ))
            })
            .collect();

        if valid.is_empty() {
            return;
        }

        // Stable sort keeps FIFO order within each priority class while moving
        // slow mDNS hostname candidates behind immediately parseable ones.
        valid.sort_by_key(|(_, candidate_str, _, _)| {
            crate::channel::WebRtcChannel::ice_candidate_application_priority(candidate_str)
        });

        runtime.spawn(async move {
            let limit = Arc::new(tokio::sync::Semaphore::new(
                WEBRTC_QUEUED_ICE_REPLAY_IN_FLIGHT_PER_BROWSER,
            ));
            let mut handles = Vec::with_capacity(valid.len());

            for (generation, candidate_str, sdp_mid, sdp_mline_index) in valid {
                let channel = channel.clone();
                let limit = Arc::clone(&limit);
                handles.push(tokio::spawn(async move {
                    let _permit = limit
                        .acquire_owned()
                        .await
                        .expect("queued ICE replay semaphore is never closed");
                    let result = channel
                        .handle_ice_candidate(&candidate_str, sdp_mid.as_deref(), sdp_mline_index)
                        .await;
                    (generation, candidate_str, sdp_mid, sdp_mline_index, result)
                }));
            }

            for handle in handles {
                match handle.await {
                    Ok((generation, candidate_str, sdp_mid, sdp_mline_index, Err(error))) => {
                        on_error(
                            generation,
                            &candidate_str,
                            sdp_mid.as_deref(),
                            sdp_mline_index,
                            error,
                        );
                    }
                    Ok((_, _, _, _, Ok(()))) => {}
                    Err(err) => {
                        log::warn!("[WebRTC] Queued ICE application task failed: {err}");
                    }
                }
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn poll_received_messages(
        &self,
        runtime: &tokio::runtime::Runtime,
    ) -> Vec<(String, Vec<u8>)> {
        let mut messages = Vec::new();
        for (browser_identity, channel) in &self.channels {
            while let Some(message) = channel.try_recv(runtime) {
                messages.push((browser_identity.clone(), message.payload));
            }
        }
        messages
    }

    pub(crate) fn drain_decrypt_failure_triggers(&self) -> Vec<String> {
        self.channels
            .iter()
            .filter_map(|(browser_identity, channel)| {
                let failures = channel.decrypt_failure_count();
                if failures >= 3 {
                    channel.reset_decrypt_failures();
                    Some(browser_identity.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    pub(crate) fn record_decrypt_failure(
        &mut self,
        browser_identity: &str,
    ) -> Option<HubControlMessage> {
        let olm_key = crate::relay::extract_olm_key(browser_identity).to_string();
        let tab_id = browser_identity
            .split_once(':')
            .map(|(_, id)| id.to_string());
        let already_restarted = self.ratchet_restarted_peers.contains(&olm_key)
            || tab_id
                .as_ref()
                .is_some_and(|id| self.ratchet_restarted_peers.contains(id));
        if already_restarted {
            return None;
        }
        self.ratchet_restarted_peers.insert(olm_key);
        if let Some(id) = tab_id {
            self.ratchet_restarted_peers.insert(id);
        }
        Some(HubControlMessage::TransportRatchetRestartRequested {
            client_id: ClientId::browser(browser_identity.to_string()),
            browser_identity: browser_identity.to_string(),
        })
    }

    pub(crate) fn clear_ratchet_restart_dedupe(&mut self) {
        self.ratchet_restarted_peers.clear();
    }

    #[cfg(test)]
    pub(crate) fn take_opened_peers(&self) -> Vec<String> {
        self.channels
            .iter()
            .filter_map(|(browser_identity, channel)| {
                channel.take_dc_opened().then(|| browser_identity.clone())
            })
            .collect()
    }

    pub(crate) fn handle_plaintext_payload(
        &self,
        browser_identity: &str,
        payload: &[u8],
    ) -> WebRtcIngressOutcome {
        self.runners.get(browser_identity).map_or_else(
            || {
                WebRtcTransportRunner::new(
                    ClientId::browser(browser_identity.to_string()),
                    browser_identity.to_string(),
                    self.current_offer_generation(browser_identity),
                    self.send_tasks
                        .get(browser_identity)
                        .map(|state| state.tx.clone())
                        .unwrap_or_else(|| {
                            let (tx, _rx) = tokio::sync::mpsc::channel(1);
                            tx
                        }),
                )
                .handle_plaintext_payload(payload)
            },
            |runner| runner.handle_plaintext_payload(payload),
        )
    }

    pub(crate) fn queue_command(
        &self,
        peer_id: &str,
        item: WebRtcAdapterCommand,
        metrics: &crate::hub::events::HubEventMetrics,
    ) {
        let bytes = Self::send_item_len(&item);
        let Some(state) = self.send_tasks.get(peer_id) else {
            metrics.record_counter("webrtc_send.unknown_peer", 1);
            let burst = self
                .unknown_peer_bursts
                .lock()
                .ok()
                .and_then(|mut guard| guard.record(peer_id, Instant::now()));
            if let Some((prefix, count)) = burst {
                metrics.record_counter("webrtc_send.unknown_peer_burst", 1);
                log::warn!(
                    "[WebRTC-Guardrail] event=unknown_peer_burst peer={} count={} window_ms=30000",
                    prefix,
                    count
                );
            } else {
                log::debug!(
                    "[WebRTC] Send to unknown/disconnected peer: {}",
                    &peer_id[..peer_id.len().min(8)]
                );
            }
            return;
        };

        if state.dead.load(std::sync::atomic::Ordering::Relaxed) {
            metrics.record_counter("webrtc_send.dead_peer", 1);
            log::debug!(
                "[WebRTC] Send to dead peer: {}",
                &peer_id[..peer_id.len().min(8)]
            );
            return;
        }

        match state.tx.try_send(item) {
            Ok(()) => {
                metrics.record_counter("webrtc_send.queued", 1);
                metrics.record_span("webrtc_send.queue", Duration::ZERO, bytes);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics.record_counter("webrtc_send.full", 1);
                log::debug!(
                    "[WebRTC] Send channel full for {}, dropping frame",
                    &peer_id[..peer_id.len().min(8)]
                );
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                metrics.record_counter("webrtc_send.closed", 1);
                state.dead.store(true, std::sync::atomic::Ordering::Relaxed);
                log::debug!(
                    "[WebRTC] Send channel closed for {}, marking peer dead",
                    &peer_id[..peer_id.len().min(8)]
                );
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn record_backpressure_recovery(
        &mut self,
        key: String,
        entry: BackpressureRecoveryEntry,
    ) {
        self.backpressure_recovery.insert(key, entry);
    }

    pub(crate) fn drain_recovery_requests(
        &self,
        now: Instant,
    ) -> Vec<WebRtcRecoverySnapshotRequest> {
        self.backpressure_recovery
            .iter()
            .filter(|(_, entry)| {
                now.duration_since(entry.last_drop) >= BACKPRESSURE_SNAPSHOT_COOLDOWN
            })
            .map(|(key, entry)| WebRtcRecoverySnapshotRequest {
                request_id: key.clone(),
                browser_identity: entry.browser_identity.clone(),
                session_uuid: entry.session_uuid.clone(),
                subscription_id: entry.subscription_id.clone(),
            })
            .collect()
    }

    pub(crate) fn complete_recovery_snapshot(
        &mut self,
        request: WebRtcRecoverySnapshotRequest,
        result: WebRtcRecoverySnapshotResult,
        metrics: &crate::hub::events::HubEventMetrics,
    ) -> WebRtcRecoveryDispatchOutcome {
        let Some(peer_state) = self.send_tasks.get(&request.browser_identity) else {
            return WebRtcRecoveryDispatchOutcome::PeerUnavailable;
        };
        if peer_state.dead.load(std::sync::atomic::Ordering::Relaxed) {
            return WebRtcRecoveryDispatchOutcome::PeerUnavailable;
        }

        let payload = match result {
            WebRtcRecoverySnapshotResult::PreparedSnapshot { payload, .. } => {
                if payload.is_empty() {
                    metrics.record_counter("snapshot.backpressure_recovery.empty", 1);
                    self.backpressure_recovery.remove(&request.request_id);
                    return WebRtcRecoveryDispatchOutcome::NoPayload;
                }
                payload
            }
            WebRtcRecoverySnapshotResult::Snapshot(snapshot) => {
                let gzip_started = Instant::now();
                let Some(prepared) = crate::worker::session_io::prepare_snapshot_payload(&snapshot)
                else {
                    metrics.record_counter("snapshot.backpressure_recovery.empty", 1);
                    self.backpressure_recovery.remove(&request.request_id);
                    return WebRtcRecoveryDispatchOutcome::NoPayload;
                };
                metrics.record_span_with_threshold(
                    "snapshot.gzip_queue",
                    gzip_started.elapsed(),
                    prepared.uncompressed_len + prepared.payload.len(),
                    Duration::from_millis(100),
                    "backpressure_recovery",
                );
                prepared.payload
            }
            WebRtcRecoverySnapshotResult::Empty => {
                metrics.record_counter("snapshot.backpressure_recovery.empty", 1);
                self.backpressure_recovery.remove(&request.request_id);
                return WebRtcRecoveryDispatchOutcome::NoPayload;
            }
            WebRtcRecoverySnapshotResult::Failed => {
                metrics.record_counter("snapshot.backpressure_recovery.failed", 1);
                self.backpressure_recovery.remove(&request.request_id);
                return WebRtcRecoveryDispatchOutcome::NoPayload;
            }
        };

        match peer_state.tx.try_send(WebRtcAdapterCommand::Pty {
            subscription_id: request.subscription_id,
            data: payload,
        }) {
            Ok(()) => {
                metrics.record_counter("snapshot.backpressure_recovery.sent", 1);
                self.backpressure_recovery.remove(&request.request_id);
                WebRtcRecoveryDispatchOutcome::Queued
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                metrics.record_counter("snapshot.queue_full", 1);
                metrics.record_counter("snapshot.backpressure_recovery.failed", 1);
                WebRtcRecoveryDispatchOutcome::StillCongested
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                metrics.record_counter("snapshot.queue_closed", 1);
                metrics.record_counter("snapshot.backpressure_recovery.failed", 1);
                self.backpressure_recovery.remove(&request.request_id);
                WebRtcRecoveryDispatchOutcome::PeerUnavailable
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn install_test_recovery_sender(
        &mut self,
        browser_identity: &str,
        runtime: &tokio::runtime::Runtime,
    ) -> tokio::sync::mpsc::Receiver<WebRtcAdapterCommand> {
        if let Some(old) = self.send_tasks.remove(browser_identity) {
            old.task.abort();
        }

        let (tx, rx) = tokio::sync::mpsc::channel(PEER_SEND_CHANNEL_CAPACITY);
        let task = runtime.spawn(async {});
        self.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState {
                tx,
                dead: Arc::new(AtomicBool::new(false)),
                task,
            },
        );
        rx
    }

    #[cfg(test)]
    pub(crate) fn install_test_dead_connected_peer(
        &mut self,
        browser_identity: &str,
        runtime: &tokio::runtime::Runtime,
    ) {
        let generation = self.next_offer_generation(browser_identity);
        self.channels.insert(
            browser_identity.to_string(),
            crate::channel::WebRtcChannel::builder()
                .server_url("https://example.test")
                .api_key("test-key")
                .build(),
        );
        self.connected_at
            .insert(browser_identity.to_string(), Instant::now());
        let (tx, _rx) = tokio::sync::mpsc::channel(PEER_SEND_CHANNEL_CAPACITY);
        self.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState {
                tx: tx.clone(),
                dead: Arc::new(AtomicBool::new(true)),
                task: runtime.spawn(async {}),
            },
        );
        self.runners.insert(
            browser_identity.to_string(),
            WebRtcTransportRunner::new(
                ClientId::browser(browser_identity.to_string()),
                browser_identity.to_string(),
                generation,
                tx,
            ),
        );
    }

    #[cfg(test)]
    pub(crate) fn saturate_test_ice_apply_limit(
        &mut self,
        browser_identity: &str,
    ) -> Vec<tokio::sync::OwnedSemaphorePermit> {
        let limit = Arc::clone(
            self.ice_apply_limits
                .entry(browser_identity.to_string())
                .or_insert_with(|| {
                    Arc::new(tokio::sync::Semaphore::new(
                        WEBRTC_ICE_APPLY_IN_FLIGHT_PER_BROWSER,
                    ))
                }),
        );

        (0..WEBRTC_ICE_APPLY_IN_FLIGHT_PER_BROWSER)
            .map(|_| Arc::clone(&limit).try_acquire_owned().expect("permit"))
            .collect()
    }

    pub(crate) fn spawn_peer_sender(
        &mut self,
        browser_identity: &str,
        runtime: &tokio::runtime::Runtime,
        metrics: Arc<crate::hub::events::HubEventMetrics>,
    ) {
        if let Some(old) = self.send_tasks.remove(browser_identity) {
            old.task.abort();
        }

        let Some(channel) = self.channels.get(browser_identity) else {
            return;
        };

        let sender = channel.sender();
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<WebRtcAdapterCommand>(PEER_SEND_CHANNEL_CAPACITY);
        let dead = Arc::new(AtomicBool::new(false));
        let dead_clone = Arc::clone(&dead);
        let bi = browser_identity.to_string();

        let task = runtime.spawn(async move {
            while let Some(item) = rx.recv().await {
                let result =
                    tokio::time::timeout(PEER_SEND_TIMEOUT, execute_adapter_command(&sender, item))
                        .await;

                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        let msg = e.to_string();
                        if msg.contains("not opened")
                            || msg.contains("No data channel")
                            || msg.contains("No peer connection")
                        {
                            log::warn!(
                                "[WebRTC-Send] Peer {} dead ({}), exiting send task",
                                &bi[..bi.len().min(8)],
                                msg
                            );
                            metrics.record_counter("webrtc_send.dead_peer", 1);
                            dead_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                            break;
                        }
                        log::warn!(
                            "[WebRTC-Send] Send error for {}: {e}",
                            &bi[..bi.len().min(8)]
                        );
                    }
                    Err(_elapsed) => {
                        log::warn!(
                            "[WebRTC-Send] Send timed out for {} (SCTP congestion), marking dead",
                            &bi[..bi.len().min(8)]
                        );
                        metrics.record_counter("webrtc_send.dead_peer", 1);
                        dead_clone.store(true, std::sync::atomic::Ordering::Relaxed);
                        break;
                    }
                }
            }
            log::debug!(
                "[WebRTC-Send] Send task exiting for {}",
                &bi[..bi.len().min(8)]
            );
        });

        self.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState { tx, dead, task },
        );
        let generation = self.current_offer_generation(browser_identity);
        let runner_tx = self
            .send_tasks
            .get(browser_identity)
            .map(|state| state.tx.clone())
            .expect("send task inserted");
        self.runners.insert(
            browser_identity.to_string(),
            WebRtcTransportRunner::new(
                ClientId::browser(browser_identity.to_string()),
                browser_identity.to_string(),
                generation,
                runner_tx,
            ),
        );
    }

    pub(crate) fn peer_command_sender(
        &self,
        browser_identity: &str,
    ) -> Option<tokio::sync::mpsc::Sender<WebRtcAdapterCommand>> {
        self.send_tasks
            .get(browser_identity)
            .map(|state| state.tx.clone())
    }

    pub(crate) fn spawn_liveness_probe(
        &mut self,
        browser_identity: &str,
        runtime: &tokio::runtime::Runtime,
    ) {
        const DC_PING_INTERVAL: Duration = Duration::from_secs(10);

        if let Some(old) = self.ping_tasks.remove(browser_identity) {
            old.abort();
        }

        let Some(state) = self.send_tasks.get(browser_identity) else {
            return;
        };
        let tx = state.tx.clone();
        let bi = browser_identity.to_string();
        let ping_payload = serde_json::to_vec(&serde_json::json!({ "type": "dc_ping" }))
            .expect("static JSON serialization cannot fail");

        let task = runtime.spawn(async move {
            let mut interval = tokio::time::interval(DC_PING_INTERVAL);
            interval.tick().await;

            loop {
                interval.tick().await;
                let item = WebRtcAdapterCommand::Json {
                    data: ping_payload.clone(),
                };
                if tx.send(item).await.is_err() {
                    log::debug!(
                        "[WebRTC] DC ping task exiting for {} (channel closed)",
                        &bi[..bi.len().min(8)]
                    );
                    break;
                }
            }
        });

        self.ping_tasks.insert(browser_identity.to_string(), task);
    }

    pub(crate) fn cleanup_scan(&mut self, timeout: Duration) -> Vec<(String, &'static str)> {
        use crate::channel::ConnectionState;

        let now = Instant::now();
        let mut to_cleanup: Vec<(String, &'static str)> = self
            .channels
            .iter()
            .filter_map(|(id, ch)| {
                let state = ch.state();
                if state == ConnectionState::Disconnected {
                    Some((id.clone(), "disconnected"))
                } else if state == ConnectionState::Connecting {
                    if let Some(started) = self.connection_started.get(id) {
                        if now.duration_since(*started) > timeout {
                            return Some((id.clone(), "timeout"));
                        }
                    }
                    None
                } else {
                    None
                }
            })
            .collect();
        let known = to_cleanup
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<HashSet<_>>();
        to_cleanup.extend(
            self.send_tasks
                .iter()
                .filter(|(id, state)| {
                    !known.contains(*id) && state.dead.load(std::sync::atomic::Ordering::Relaxed)
                })
                .map(|(id, _)| (id.clone(), "send_failed")),
        );

        let connected: Vec<String> = self
            .channels
            .iter()
            .filter(|(_, ch)| ch.state() == ConnectionState::Connected)
            .map(|(id, _)| id.clone())
            .collect();
        for id in connected {
            self.connection_started.remove(&id);
            self.connected_at.entry(id).or_insert_with(Instant::now);
        }

        self.pending_closes
            .retain(|_, close_rx| !*close_rx.borrow());
        to_cleanup
    }

    pub(crate) fn cleanup_peer_transport(
        &mut self,
        browser_identity: &str,
        runtime: &tokio::runtime::Runtime,
    ) -> Option<TransportCleanup> {
        let mut channel = self.channels.remove(browser_identity)?;
        let connected_age = self
            .connected_at
            .remove(browser_identity)
            .map(|connected_at| connected_at.elapsed());

        let close_rx = channel.close_receiver();
        let olm_key = crate::relay::extract_olm_key(browser_identity).to_string();
        self.pending_closes.insert(olm_key, close_rx);

        runtime.spawn(async move {
            channel.disconnect().await;
            log::debug!("[WebRTC] Channel disconnect completed");
        });

        self.connection_started.remove(browser_identity);
        self.offer_generation.remove(browser_identity);
        self.pending_ice_candidates.remove(browser_identity);

        if let Some(state) = self.send_tasks.remove(browser_identity) {
            drop(state.tx);
            state.task.abort();
        }
        self.runners.remove(browser_identity);

        self.backpressure_recovery
            .retain(|_, entry| entry.browser_identity != browser_identity);

        if let Some(task) = self.ping_tasks.remove(browser_identity) {
            task.abort();
        }

        Some(TransportCleanup { connected_age })
    }

    pub(crate) fn cleanup_peer_transport_state_only(&mut self, browser_identity: &str) -> bool {
        let mut removed = false;

        self.connection_started.remove(browser_identity);
        self.connected_at.remove(browser_identity);
        self.offer_generation.remove(browser_identity);
        self.pending_ice_candidates.remove(browser_identity);
        self.ice_apply_limits.remove(browser_identity);
        if let Some(state) = self.send_tasks.remove(browser_identity) {
            drop(state.tx);
            state.task.abort();
            removed = true;
        }
        if self.runners.remove(browser_identity).is_some() {
            removed = true;
        }
        if let Some(task) = self.ping_tasks.remove(browser_identity) {
            task.abort();
            removed = true;
        }
        let before = self.backpressure_recovery.len();
        self.backpressure_recovery
            .retain(|_, entry| entry.browser_identity != browser_identity);
        removed || self.backpressure_recovery.len() != before
    }

    pub(crate) fn shutdown(&mut self, runtime: &tokio::runtime::Runtime) {
        for (_id, state) in self.send_tasks.drain() {
            drop(state.tx);
            state.task.abort();
        }

        for (_id, mut channel) in self.channels.drain() {
            runtime.spawn(async move {
                channel.disconnect().await;
            });
        }

        for (_id, task) in self.ping_tasks.drain() {
            task.abort();
        }
        for task in self.queue_forwarders.drain(..) {
            task.abort();
        }

        self.connection_started.clear();
        self.connected_at.clear();
        self.pending_ice_candidates.clear();
        self.ice_apply_limits.clear();
        self.ratchet_restarted_peers.clear();
        self.pending_closes.clear();
        self.offer_generation.clear();
        self.backpressure_recovery.clear();
        self.runners.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TransportCleanup {
    pub(crate) connected_age: Option<Duration>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplacedPeerCloseWait {
    NoPendingClose,
    AlreadyClosed,
    Closed,
    ClosedChannelDropped,
    TimedOut,
}

#[derive(Debug)]
pub(crate) enum QueueOrApplyIceOutcome {
    /// ICE application was spawned; failures are reported through the callback.
    ApplySpawned,
    /// The peer already has too many in-flight ICE application tasks.
    ApplyBackpressure,
    Queued(usize),
    IgnoredEmpty,
    UnknownBrowser,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WebRtcRecoveryDispatchOutcome {
    Queued,
    StillCongested,
    PeerUnavailable,
    NoPayload,
}

/// Execute a single queued WebRTC send command.
pub(crate) async fn execute_adapter_command(
    sender: &crate::channel::webrtc::WebRtcSender,
    command: WebRtcAdapterCommand,
) -> Result<(), crate::channel::ChannelError> {
    match command {
        WebRtcAdapterCommand::Pty {
            subscription_id,
            data,
        } => sender.send_pty_raw(&subscription_id, &data).await,
        WebRtcAdapterCommand::Json { data } => sender.send_json(&data).await,
        WebRtcAdapterCommand::Binary { data } => sender.send_message_raw(&data).await,
        WebRtcAdapterCommand::Stream {
            frame_type,
            stream_id,
            payload,
        } => {
            sender
                .send_stream_raw(frame_type, stream_id, &payload)
                .await
        }
        WebRtcAdapterCommand::BundleRefresh { bundle_bytes } => {
            sender.send_bundle_refresh(&bundle_bytes).await
        }
    }
}

/// Synchronous WebRTC adapter conversion boundary.
#[derive(Debug, Clone)]
pub(crate) struct WebRtcTransportAdapter {
    client_id: ClientId,
}

impl WebRtcTransportAdapter {
    #[must_use]
    pub(crate) fn new(client_id: ClientId) -> Self {
        Self { client_id }
    }

    #[must_use]
    pub(crate) fn egress_to_command(egress: TransportEgress) -> Option<WebRtcAdapterCommand> {
        match egress {
            TransportEgress::TerminalBytes {
                subscription_id,
                data,
                ..
            }
            | TransportEgress::Scrollback {
                subscription_id,
                data,
                ..
            } => Some(WebRtcAdapterCommand::Pty {
                subscription_id,
                data,
            }),
            TransportEgress::ProcessExited {
                subscription_id,
                session_uuid,
                exit_code,
            } => serde_json::to_vec(&serde_json::json!({
                "type": "process_exited",
                "subscriptionId": subscription_id,
                "session_uuid": session_uuid,
                // Browser soft-closes on null (reader reconnect) and hard-closes
                // on a numeric code — omitting this made every exit look soft.
                "exit_code": exit_code,
            }))
            .ok()
            .map(|data| WebRtcAdapterCommand::Json { data }),
            TransportEgress::Pong { request_id } => {
                serde_json::to_vec(&crate::worker::transport::egress_pong(request_id))
                    .ok()
                    .map(|data| WebRtcAdapterCommand::Json { data })
            }
            TransportEgress::TerminalAttach {
                subscription_id,
                session_uuid,
                state,
            } => serde_json::to_vec(&crate::worker::transport::egress_terminal_attach(
                subscription_id,
                session_uuid,
                state,
            ))
            .ok()
            .map(|data| WebRtcAdapterCommand::Json { data }),
            TransportEgress::DcPong => {
                serde_json::to_vec(&crate::worker::transport::egress_dc_pong())
                    .ok()
                    .map(|data| WebRtcAdapterCommand::Json { data })
            }
            TransportEgress::BoundaryJson(value) => serde_json::to_vec(&value)
                .ok()
                .map(|data| WebRtcAdapterCommand::Json { data }),
            TransportEgress::Binary(data) => Some(WebRtcAdapterCommand::Binary { data }),
            TransportEgress::Snapshot { .. }
            | TransportEgress::ModeChanged { .. }
            | TransportEgress::KittyChanged { .. }
            | TransportEgress::FocusReportingChanged { .. }
            | TransportEgress::FocusChanged { .. }
            | TransportEgress::Close { .. } => None,
        }
    }
}

impl TransportAdapter for WebRtcTransportAdapter {
    fn client_id(&self) -> &ClientId {
        &self.client_id
    }

    fn ingress_to_client(&self, ingress: TransportIngress) -> ClientWorkerMessage {
        crate::worker::transport::ingress_to_client_message(ingress)
    }

    fn client_to_egress(&self, message: ClientWorkerMessage) -> Option<TransportEgress> {
        match message {
            ClientWorkerMessage::TerminalBytes { session_uuid, data } => {
                Some(TransportEgress::TerminalBytes {
                    subscription_id: self.client_id.to_string(),
                    session_uuid,
                    data,
                })
            }
            ClientWorkerMessage::ControlFrame(ClientControlFrame::DcPong) => {
                Some(TransportEgress::DcPong)
            }
            ClientWorkerMessage::ControlFrame(ClientControlFrame::BoundaryJson(value)) => {
                Some(TransportEgress::BoundaryJson(value))
            }
            ClientWorkerMessage::Shutdown { reason } => Some(TransportEgress::Close { reason }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_channel() -> crate::channel::WebRtcChannel {
        crate::channel::WebRtcChannel::builder()
            .server_url("https://example.test")
            .api_key("test-key")
            .build()
    }

    fn test_offer_request(browser_identity: &str) -> WebRtcOfferRequest {
        let (outgoing_signal_tx, _outgoing_signal_rx) = tokio::sync::mpsc::channel(1);
        let (stream_frame_tx, _stream_frame_rx) = tokio::sync::mpsc::channel(1);
        let (hub_event_tx, _hub_event_rx) = tokio::sync::mpsc::channel(1);
        let (pty_input_tx, _pty_input_rx) = tokio::sync::mpsc::channel(1);
        let (file_input_tx, _file_input_rx) = tokio::sync::mpsc::channel(1);

        WebRtcOfferRequest {
            browser_identity: browser_identity.to_string(),
            sdp: "v=0\r\n".to_string(),
            hub_id: "hub-test".to_string(),
            server_url: "https://example.test".to_string(),
            api_key: "test-key".to_string(),
            crypto_service: Some(crate::relay::create_crypto_service("hub-test")),
            outgoing_signal_tx,
            stream_frame_tx,
            hub_event_tx: hub_event_tx.into(),
            pty_input_tx,
            file_input_tx,
        }
    }

    #[test]
    fn registry_uses_bounded_queues() {
        let registry = WebRtcPeerRegistry::new();

        assert_eq!(
            registry.outgoing_signal_tx.max_capacity(),
            WEBRTC_OUTGOING_SIGNAL_QUEUE_CAPACITY
        );
        assert_eq!(
            registry.stream_frame_tx.max_capacity(),
            WEBRTC_STREAM_FRAME_QUEUE_CAPACITY
        );
        assert_eq!(
            registry.pty_input_tx.max_capacity(),
            WEBRTC_PTY_INPUT_QUEUE_CAPACITY
        );
        assert_eq!(
            registry.file_input_tx.max_capacity(),
            WEBRTC_FILE_INPUT_QUEUE_CAPACITY
        );
    }

    #[test]
    fn adapter_converts_webrtc_ingress_without_async_trait_changes() {
        let adapter = WebRtcTransportAdapter::new(ClientId::Browser("browser-1".to_string()));

        let message = adapter.ingress_to_client(TransportIngress::TerminalInput {
            session_uuid: "sess-1".to_string(),
            data: b"abc".to_vec(),
        });

        assert!(matches!(
            message,
            ClientWorkerMessage::SessionInput { ref session_uuid, ref data }
                if session_uuid == "sess-1" && data == b"abc"
        ));
    }

    #[test]
    fn egress_process_exited_includes_exit_code_for_browser_soft_hard_split() {
        let with_code = WebRtcTransportAdapter::egress_to_command(TransportEgress::ProcessExited {
            subscription_id: "terminal_sess-1".to_string(),
            session_uuid: "sess-1".to_string(),
            exit_code: Some(0),
        })
        .expect("process_exited command");
        let WebRtcAdapterCommand::Json { data } = with_code else {
            panic!("expected JSON command");
        };
        let value: serde_json::Value = serde_json::from_slice(&data).expect("json");
        assert_eq!(value["type"], "process_exited");
        assert_eq!(value["session_uuid"], "sess-1");
        assert_eq!(value["exit_code"], 0);

        let soft = WebRtcTransportAdapter::egress_to_command(TransportEgress::ProcessExited {
            subscription_id: "terminal_sess-1".to_string(),
            session_uuid: "sess-1".to_string(),
            exit_code: None,
        })
        .expect("soft process_exited command");
        let WebRtcAdapterCommand::Json { data } = soft else {
            panic!("expected JSON command");
        };
        let value: serde_json::Value = serde_json::from_slice(&data).expect("json");
        assert!(value["exit_code"].is_null(), "null exit_code must be present for soft close");
    }

    #[test]
    fn adapter_classifies_webrtc_heartbeat_frames_as_typed_controls() {
        let adapter = WebRtcTransportAdapter::new(ClientId::Browser("browser-1".to_string()));

        let ping = adapter.ingress_to_client(TransportIngress::DcPing);
        assert!(matches!(
            ping,
            ClientWorkerMessage::ControlFrame(ClientControlFrame::DcPong)
        ));

        let pong = adapter.ingress_to_client(TransportIngress::DcPong);
        assert!(matches!(
            pong,
            ClientWorkerMessage::ControlFrame(ClientControlFrame::DcPongReceived)
        ));

        let egress = adapter.client_to_egress(ClientWorkerMessage::ControlFrame(
            ClientControlFrame::DcPong,
        ));
        assert!(matches!(egress, Some(TransportEgress::DcPong)));
    }

    #[test]
    fn adapter_classifies_webrtc_focus_changed_as_typed_control() {
        let adapter = WebRtcTransportAdapter::new(ClientId::Browser("browser-1".to_string()));

        let message = adapter.ingress_to_client(TransportIngress::FocusChanged {
            session_uuid: "sess-1".to_string(),
            focused: true,
        });

        assert!(matches!(
            message,
            ClientWorkerMessage::ControlFrame(ClientControlFrame::FocusChanged {
                session_uuid,
                focused: true,
            }) if session_uuid == "sess-1"
        ));
    }

    #[test]
    fn terminal_resize_and_snapshot_without_session_uuid_do_not_fall_back_to_lua() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let runner = WebRtcTransportRunner::new(
            ClientId::browser("browser-1"),
            "browser-1".to_string(),
            0,
            tx,
        );

        let resize = runner.handle_plaintext_payload(br#"{"type":"resize","rows":30,"cols":100}"#);
        assert!(matches!(
            resize,
            WebRtcIngressOutcome::UnsupportedTerminalControl {
                message_type: "resize"
            }
        ));

        let request_snapshot =
            runner.handle_plaintext_payload(br#"{"type":"request_snapshot","rows":30,"cols":100}"#);
        assert!(matches!(
            request_snapshot,
            WebRtcIngressOutcome::UnsupportedTerminalControl {
                message_type: "request_snapshot"
            }
        ));
    }

    #[test]
    fn unknown_peer_burst_is_bounded_and_coalesced() {
        let mut burst = PeerBurstState::default();
        let now = Instant::now();
        let mut emitted = None;

        for _ in 0..PeerBurstState::THRESHOLD {
            emitted = burst.record("peer:abc", now);
        }

        assert_eq!(
            emitted,
            Some(("peer:abc".to_string(), PeerBurstState::THRESHOLD))
        );
        assert_eq!(burst.record("peer:abc", now), None);
    }

    #[test]
    fn registry_unknown_peer_burst_is_coalesced_on_send_path() {
        let registry = WebRtcPeerRegistry::new();
        let metrics = crate::hub::events::HubEventMetrics::default();

        for _ in 0..PeerBurstState::THRESHOLD {
            registry.queue_command(
                "peer:abc",
                WebRtcAdapterCommand::Json { data: vec![b'{'] },
                &metrics,
            );
        }

        assert_eq!(registry.unknown_peer_distinct_count(), 1);
    }

    #[test]
    fn registry_tags_pending_ice_by_current_offer_generation() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let first = registry.next_offer_generation(browser_identity);
        assert_eq!(first, 1);
        let first_candidate = serde_json::json!({
            "candidate": "candidate:first",
            "sdpMid": "0",
            "sdpMLineIndex": 0,
        });
        assert_eq!(
            registry.queue_ice_for_current_generation(browser_identity, first_candidate, 128),
            Some(1)
        );

        let second = registry.next_offer_generation(browser_identity);
        let second_candidate = serde_json::json!({
            "candidate": "candidate:second",
            "sdpMid": "0",
            "sdpMLineIndex": 0,
        });
        assert_eq!(
            registry.queue_ice_for_current_generation(browser_identity, second_candidate, 128),
            Some(2)
        );

        let queued = registry
            .drain_pending_ice(browser_identity)
            .expect("queued candidates");
        assert_eq!(queued[0].0, first);
        assert_eq!(queued[1].0, second);
        assert_eq!(registry.current_offer_generation(browser_identity), second);
    }

    #[test]
    fn start_offer_prepares_async_negotiation_without_runtime_connect() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";

        let start = registry.start_offer(test_offer_request(browser_identity));

        assert_eq!(start.browser_identity, browser_identity);
        assert_eq!(start.generation, 1);
        assert_eq!(start.config.channel_name, "WebRtcChannel");
        assert_eq!(start.config.hub_id, "hub-test");
        assert_eq!(
            start.config.browser_identity.as_deref(),
            Some(browser_identity)
        );
        assert!(start.config.encrypt);
        assert_eq!(start.config.compression_threshold, Some(4096));
        assert!(!start.config.cli_subscription);
        assert_eq!(registry.current_offer_generation(browser_identity), 1);
        assert!(registry.connection_started.contains_key(browser_identity));
        assert!(!registry.channels.contains_key(browser_identity));
    }

    #[test]
    fn start_offer_supports_plaintext_transport() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "anon:tab-1";
        let mut request = test_offer_request(browser_identity);
        request.crypto_service = None;

        let start = registry.start_offer(request);

        assert!(!start.config.encrypt);
        assert!(start.crypto_service.is_none());
        assert_eq!(
            start.config.browser_identity.as_deref(),
            Some(browser_identity)
        );
    }

    #[test]
    fn failed_offer_completion_clears_inflight_state_for_retry() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let generation = registry.next_offer_generation(browser_identity);
        registry
            .connection_started
            .insert(browser_identity.to_string(), Instant::now());
        assert_eq!(
            registry.queue_ice_for_current_generation(
                browser_identity,
                serde_json::json!({"candidate": "candidate:1 1 UDP 1 127.0.0.1 9 typ host"}),
                128,
            ),
            Some(1)
        );

        let outcome = registry.complete_offer(
            WebRtcOfferCompletion {
                browser_identity: browser_identity.to_string(),
                generation,
                channel: test_channel(),
                answer_signal: None,
            },
            &runtime,
        );

        assert!(matches!(
            outcome,
            WebRtcOfferCompletionOutcome::FailedCleaned { generation: 1, .. }
        ));
        assert_eq!(registry.current_offer_generation(browser_identity), 0);
        assert!(!registry.connection_started.contains_key(browser_identity));
        assert!(registry.drain_pending_ice(browser_identity).is_none());
        assert!(!registry.channels.contains_key(browser_identity));
    }

    #[test]
    fn active_ice_candidate_application_is_spawned_off_hot_path() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        registry
            .channels
            .insert(browser_identity.to_string(), test_channel());
        let (error_tx, error_rx) = std::sync::mpsc::channel();

        let outcome = registry.queue_or_apply_ice(
            browser_identity,
            serde_json::json!({"candidate": "candidate:1 1 UDP 1 127.0.0.1 9 typ host"}),
            128,
            &runtime,
            move |candidate, sdp_mid, sdp_mline_index, error| {
                error_tx
                    .send((
                        candidate.to_string(),
                        sdp_mid.map(String::from),
                        sdp_mline_index,
                        error.to_string(),
                    ))
                    .expect("send error");
            },
        );

        assert!(matches!(outcome, QueueOrApplyIceOutcome::ApplySpawned));
        let (candidate, sdp_mid, sdp_mline_index, error) = error_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("spawned ICE application reports async failure");
        assert_eq!(candidate, "candidate:1 1 UDP 1 127.0.0.1 9 typ host");
        assert_eq!(sdp_mid, None);
        assert_eq!(sdp_mline_index, None);
        assert!(error.contains("No peer connection"));
    }

    #[test]
    fn active_ice_candidate_application_is_bounded_per_peer() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        registry
            .channels
            .insert(browser_identity.to_string(), test_channel());
        let limit = Arc::new(tokio::sync::Semaphore::new(
            WEBRTC_ICE_APPLY_IN_FLIGHT_PER_BROWSER,
        ));
        let _permits: Vec<_> = (0..WEBRTC_ICE_APPLY_IN_FLIGHT_PER_BROWSER)
            .map(|_| Arc::clone(&limit).try_acquire_owned().expect("permit"))
            .collect();
        registry
            .ice_apply_limits
            .insert(browser_identity.to_string(), limit);

        let outcome = registry.queue_or_apply_ice(
            browser_identity,
            serde_json::json!({"candidate": "candidate:1 1 UDP 1 127.0.0.1 9 typ host"}),
            128,
            &runtime,
            |_, _, _, _| panic!("saturated apply should not spawn"),
        );

        assert!(matches!(outcome, QueueOrApplyIceOutcome::ApplyBackpressure));
    }

    #[test]
    fn queue_or_apply_ice_queues_or_rejects_when_no_active_peer_exists() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        assert!(matches!(
            registry.queue_or_apply_ice(
                browser_identity,
                serde_json::json!({"candidate": "candidate:1"}),
                128,
                &runtime,
                |_, _, _, _| panic!("no active peer should not spawn"),
            ),
            QueueOrApplyIceOutcome::UnknownBrowser
        ));

        registry.next_offer_generation(browser_identity);
        assert!(matches!(
            registry.queue_or_apply_ice(
                browser_identity,
                serde_json::json!({"candidate": "candidate:1"}),
                128,
                &runtime,
                |_, _, _, _| panic!("queued candidate should not spawn"),
            ),
            QueueOrApplyIceOutcome::Queued(1)
        ));
    }

    #[test]
    fn queued_ice_apply_drops_stale_generations_and_reports_failures() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        registry
            .channels
            .insert(browser_identity.to_string(), test_channel());
        let (error_tx, error_rx) = std::sync::mpsc::channel();

        let current_candidates = (0..=WEBRTC_QUEUED_ICE_REPLAY_IN_FLIGHT_PER_BROWSER)
            .map(|index| {
                (
                    2,
                    serde_json::json!({
                        "candidate": format!("candidate:current-{index} 1 UDP 1 127.0.0.1 9 typ host")
                    }),
                )
            })
            .collect::<Vec<_>>();
        let mut candidates = vec![(
            1,
            serde_json::json!({"candidate": "candidate:stale 1 UDP 1 127.0.0.1 9 typ host"}),
        )];
        candidates.extend(current_candidates);

        registry.apply_queued_ice_for_offer(
            browser_identity,
            2,
            candidates,
            &runtime,
            move |generation, candidate, _, _, error| {
                error_tx
                    .send((generation, candidate.to_string(), error.to_string()))
                    .expect("send queued error");
            },
        );

        let mut reported = (0..=WEBRTC_QUEUED_ICE_REPLAY_IN_FLIGHT_PER_BROWSER)
            .map(|_| {
                error_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("current generation candidate reports async failure")
            })
            .collect::<Vec<_>>();
        reported.sort_by(|(_, left, _), (_, right, _)| left.cmp(right));
        for (index, (generation, candidate, error)) in reported.iter().enumerate() {
            assert_eq!(*generation, 2);
            assert!(candidate.starts_with(&format!("candidate:current-{index}")));
            assert!(error.contains("No peer connection"));
        }
        assert!(error_rx.try_recv().is_err());
    }

    #[test]
    fn queued_ice_application_prioritizes_non_mdns_candidates() {
        let mdns_1 = "candidate:1 1 UDP 2122260223 a4b9c343-f925-4558-b2ce-76521f4bc787.local 54872 typ host";
        let direct_1 = "candidate:2 1 UDP 2122260223 127.0.0.1 54873 typ host";
        let mdns_2 = "3 1 UDP 2122260223 e84d042a-c142-44f9-9f96-335c04f505e2.local 54874 typ host";
        let direct_2 = "candidate:4 1 UDP 2122260223 192.0.2.10 54875 typ host";

        assert_eq!(
            crate::channel::WebRtcChannel::ice_candidate_application_priority(direct_1),
            0
        );
        assert_eq!(
            crate::channel::WebRtcChannel::ice_candidate_application_priority(mdns_2),
            1
        );

        let mut candidates = [mdns_1, direct_1, mdns_2, direct_2];
        candidates.sort_by_key(|candidate| {
            crate::channel::WebRtcChannel::ice_candidate_application_priority(candidate)
        });

        assert_eq!(candidates, [direct_1, direct_2, mdns_1, mdns_2]);
    }

    #[test]
    fn registry_recovery_and_generation_state_stay_bounded_under_reconnect_churn() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let first = registry.next_offer_generation(browser_identity);
        assert_eq!(first, 1);
        assert_eq!(
            registry.queue_ice_for_current_generation(
                browser_identity,
                serde_json::json!({ "candidate": "candidate:first" }),
                1,
            ),
            Some(1)
        );
        let second = registry.next_offer_generation(browser_identity);
        assert_eq!(
            registry.queue_ice_for_current_generation(
                browser_identity,
                serde_json::json!({ "candidate": "candidate:second" }),
                1,
            ),
            Some(1)
        );

        let queued = registry
            .drain_pending_ice(browser_identity)
            .expect("queued candidates");
        assert_eq!(
            queued,
            vec![(
                second,
                serde_json::json!({ "candidate": "candidate:second" })
            )]
        );

        let key = format!("{browser_identity}:sess-1");
        registry.record_backpressure_recovery(
            key.clone(),
            BackpressureRecoveryEntry {
                browser_identity: browser_identity.to_string(),
                session_uuid: "sess-1".to_string(),
                subscription_id: "sub-1".to_string(),
                last_drop: Instant::now() - BACKPRESSURE_SNAPSHOT_COOLDOWN,
            },
        );

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let task = runtime.spawn(async {});
        registry.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState {
                tx,
                dead: Arc::new(AtomicBool::new(false)),
                task,
            },
        );

        assert_eq!(registry.drain_recovery_requests(Instant::now()).len(), 1);
        let request = registry
            .drain_recovery_requests(Instant::now())
            .into_iter()
            .find(|request| request.request_id == key)
            .expect("request");
        let metrics = crate::hub::events::HubEventMetrics::default();
        assert_eq!(
            registry.complete_recovery_snapshot(
                request,
                WebRtcRecoverySnapshotResult::Snapshot(b"snapshot".to_vec()),
                &metrics,
            ),
            WebRtcRecoveryDispatchOutcome::Queued
        );
        assert!(registry.drain_recovery_requests(Instant::now()).is_empty());

        registry.clear_offer_state(browser_identity);
        assert_eq!(registry.current_offer_generation(browser_identity), 0);
        assert!(registry.drain_pending_ice(browser_identity).is_none());
    }

    #[test]
    fn decrypt_failure_ratchet_trigger_deduplicates_by_olm_key_and_tab_id() {
        let mut registry = WebRtcPeerRegistry::new();
        let first = registry.record_decrypt_failure("olm-key:tab-1");
        assert!(matches!(
            first,
            Some(HubControlMessage::TransportRatchetRestartRequested {
                ref browser_identity,
                ..
            }) if browser_identity == "olm-key:tab-1"
        ));

        assert!(registry.record_decrypt_failure("olm-key:tab-2").is_none());
        assert!(registry
            .record_decrypt_failure("different-olm:tab-1")
            .is_none());

        registry.clear_ratchet_restart_dedupe();
        assert!(registry
            .record_decrypt_failure("different-olm:tab-1")
            .is_some());
    }

    #[test]
    fn bounded_replacement_close_wait_times_out_without_blocking_indefinitely() {
        let mut registry = WebRtcPeerRegistry::new();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        registry.pending_closes.insert("olm-key".to_string(), rx);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let started = Instant::now();

        let result =
            registry.wait_for_replaced_peer_close("olm-key", Duration::from_millis(100), &runtime);

        assert_eq!(result, ReplacedPeerCloseWait::TimedOut);
        assert!(started.elapsed() < Duration::from_millis(500));

        let stale_identity = "olm-key:old-tab";
        let generation = registry.next_offer_generation(stale_identity);
        registry
            .channels
            .insert(stale_identity.to_string(), test_channel());
        let (_cleanup, disconnected) = registry
            .mark_data_channel_closed(
                stale_identity,
                generation,
                super::super::hub_control::TransportDisconnectReason::ReplacedByNewPeer,
                &runtime,
            )
            .expect("replacement cleanup summary");
        assert!(matches!(
            disconnected,
            HubControlMessage::TransportPeerStateChanged {
                state: super::super::hub_control::TransportPeerState::Disconnected {
                    reason: super::super::hub_control::TransportDisconnectReason::ReplacedByNewPeer,
                    ..
                },
                ..
            }
        ));
        let replacement_generation = registry.next_offer_generation("olm-key:new-tab");
        assert_eq!(replacement_generation, 1);
    }

    #[test]
    fn data_channel_close_after_connected_returns_one_cleanup_summary() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let generation = registry.next_offer_generation(browser_identity);
        let stale_generation = generation;
        let generation = registry.next_offer_generation(browser_identity);
        registry
            .channels
            .insert(browser_identity.to_string(), test_channel());
        let (send_tx, _send_rx) = tokio::sync::mpsc::channel(1);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let send_task = runtime.spawn(async {});
        registry.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState {
                tx: send_tx.clone(),
                dead: Arc::new(AtomicBool::new(false)),
                task: send_task,
            },
        );
        registry.runners.insert(
            browser_identity.to_string(),
            WebRtcTransportRunner::new(
                crate::client::ClientId::browser(browser_identity.to_string()),
                browser_identity.to_string(),
                generation,
                send_tx,
            ),
        );
        let ping_task = runtime.spawn(async {});
        registry
            .ping_tasks
            .insert(browser_identity.to_string(), ping_task);
        registry.record_backpressure_recovery(
            format!("{browser_identity}:sess-1"),
            BackpressureRecoveryEntry {
                browser_identity: browser_identity.to_string(),
                session_uuid: "sess-1".to_string(),
                subscription_id: "sub-1".to_string(),
                last_drop: Instant::now() - BACKPRESSURE_SNAPSHOT_COOLDOWN,
            },
        );

        assert!(
            registry
                .mark_data_channel_open(browser_identity, stale_generation)
                .is_none(),
            "stale DataChannel-open events must not mark the current peer connected"
        );
        let opened = registry
            .mark_data_channel_open(browser_identity, generation)
            .expect("open summary");
        assert!(matches!(
            opened,
            HubControlMessage::TransportPeerStateChanged {
                state: super::super::hub_control::TransportPeerState::Connected { generation: g, .. },
                ..
            } if g == generation
        ));

        let (_cleanup, closed) = registry
            .mark_data_channel_closed(
                browser_identity,
                generation,
                super::super::hub_control::TransportDisconnectReason::DataChannelClose,
                &runtime,
            )
            .expect("close summary");
        assert!(matches!(
            closed,
            HubControlMessage::TransportPeerStateChanged {
                state: super::super::hub_control::TransportPeerState::Disconnected {
                    generation: g,
                    reason: super::super::hub_control::TransportDisconnectReason::DataChannelClose,
                },
                ..
            } if g == generation
        ));
        assert!(!registry.send_tasks.contains_key(browser_identity));
        assert!(!registry.runners.contains_key(browser_identity));
        assert!(!registry.ping_tasks.contains_key(browser_identity));
        assert!(registry
            .backpressure_recovery
            .values()
            .all(|entry| entry.browser_identity != browser_identity));
        assert!(registry
            .mark_data_channel_closed(
                browser_identity,
                generation,
                super::super::hub_control::TransportDisconnectReason::DataChannelClose,
                &runtime,
            )
            .is_none());
    }

    #[test]
    fn cleanup_scan_flags_dead_send_task_even_when_channel_still_connected() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:dead-send";
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let _guard = runtime.enter();
        let (send_tx, _send_rx) = tokio::sync::mpsc::channel(1);
        registry.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState {
                tx: send_tx,
                dead: Arc::new(AtomicBool::new(true)),
                task: runtime.spawn(async {}),
            },
        );

        let cleanup = registry.cleanup_scan(Duration::from_secs(15));
        assert!(cleanup
            .iter()
            .any(|(id, reason)| { id == browser_identity && *reason == "send_failed" }));
        assert_eq!(registry.diagnostics().dead_send_tasks, 1);
    }

    #[test]
    fn quick_successive_offers_drop_stale_completion_and_accept_current() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let first_generation = registry.next_offer_generation(browser_identity);
        let second_generation = registry.next_offer_generation(browser_identity);
        assert_eq!(first_generation, 1);
        assert_eq!(second_generation, 2);

        let first_channel = test_channel();
        let mut first_close = first_channel.close_receiver();
        let stale = registry.complete_offer(
            WebRtcOfferCompletion {
                browser_identity: browser_identity.to_string(),
                generation: first_generation,
                channel: first_channel,
                answer_signal: Some(serde_json::json!({"t": 1, "c": "stale"})),
            },
            &runtime,
        );
        assert!(matches!(
            stale,
            WebRtcOfferCompletionOutcome::StaleDropped {
                completed_generation: 1,
                current_generation: 2,
                ..
            }
        ));

        let current = registry.complete_offer(
            WebRtcOfferCompletion {
                browser_identity: browser_identity.to_string(),
                generation: second_generation,
                channel: test_channel(),
                answer_signal: Some(serde_json::json!({"t": 1, "c": "current"})),
            },
            &runtime,
        );
        assert!(matches!(
            current,
            WebRtcOfferCompletionOutcome::AnswerReady {
                generation: 2,
                envelope,
                queued_ice,
                ..
            } if envelope == serde_json::json!({"t": 1, "c": "current"}) && queued_ice.is_empty()
        ));
        assert!(registry.channels.contains_key(browser_identity));

        let _ = runtime.block_on(async {
            tokio::time::timeout(Duration::from_millis(250), first_close.wait_for(|v| *v)).await
        });
    }

    #[test]
    fn recovery_snapshot_dispatch_keeps_congested_request_queued() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let key = format!("{browser_identity}:sess-1");
        registry.record_backpressure_recovery(
            key.clone(),
            BackpressureRecoveryEntry {
                browser_identity: browser_identity.to_string(),
                session_uuid: "sess-1".to_string(),
                subscription_id: "sub-1".to_string(),
                last_drop: Instant::now() - BACKPRESSURE_SNAPSHOT_COOLDOWN,
            },
        );

        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(WebRtcAdapterCommand::Json { data: vec![b'{'] })
            .expect("fill queue");
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let task = runtime.spawn(async {});
        registry.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState {
                tx,
                dead: Arc::new(AtomicBool::new(false)),
                task,
            },
        );

        let request = registry
            .drain_recovery_requests(Instant::now())
            .into_iter()
            .find(|request| request.request_id == key)
            .expect("request");
        let metrics = crate::hub::events::HubEventMetrics::default();
        assert_eq!(
            registry.complete_recovery_snapshot(
                request,
                WebRtcRecoverySnapshotResult::Snapshot(b"snapshot".to_vec()),
                &metrics,
            ),
            WebRtcRecoveryDispatchOutcome::StillCongested
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.counters["snapshot.queue_full"], 1);
        assert_eq!(
            snapshot.counters["snapshot.backpressure_recovery.failed"],
            1
        );
        assert_eq!(registry.drain_recovery_requests(Instant::now()).len(), 1);
    }

    #[test]
    fn recovery_snapshot_dispatch_closed_peer_counts_failed_and_removes_request() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let key = format!("{browser_identity}:sess-1");
        registry.record_backpressure_recovery(
            key.clone(),
            BackpressureRecoveryEntry {
                browser_identity: browser_identity.to_string(),
                session_uuid: "sess-1".to_string(),
                subscription_id: "sub-1".to_string(),
                last_drop: Instant::now() - BACKPRESSURE_SNAPSHOT_COOLDOWN,
            },
        );

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let task = runtime.spawn(async {});
        registry.send_tasks.insert(
            browser_identity.to_string(),
            PeerSendState {
                tx,
                dead: Arc::new(AtomicBool::new(false)),
                task,
            },
        );

        let request = registry
            .drain_recovery_requests(Instant::now())
            .into_iter()
            .find(|request| request.request_id == key)
            .expect("request");
        let metrics = crate::hub::events::HubEventMetrics::default();
        assert_eq!(
            registry.complete_recovery_snapshot(
                request,
                WebRtcRecoverySnapshotResult::Snapshot(b"snapshot".to_vec()),
                &metrics,
            ),
            WebRtcRecoveryDispatchOutcome::PeerUnavailable
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.counters["snapshot.queue_closed"], 1);
        assert_eq!(
            snapshot.counters["snapshot.backpressure_recovery.failed"],
            1
        );
        assert!(registry.drain_recovery_requests(Instant::now()).is_empty());
    }

    #[test]
    fn recovery_snapshot_dispatch_empty_result_counts_empty_and_removes_request() {
        let mut registry = WebRtcPeerRegistry::new();
        let browser_identity = "olm-key:tab-1";
        let key = format!("{browser_identity}:sess-1");
        registry.record_backpressure_recovery(
            key.clone(),
            BackpressureRecoveryEntry {
                browser_identity: browser_identity.to_string(),
                session_uuid: "sess-1".to_string(),
                subscription_id: "sub-1".to_string(),
                last_drop: Instant::now() - BACKPRESSURE_SNAPSHOT_COOLDOWN,
            },
        );

        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let _rx = registry.install_test_recovery_sender(browser_identity, &runtime);
        let request = registry
            .drain_recovery_requests(Instant::now())
            .into_iter()
            .find(|request| request.request_id == key)
            .expect("request");
        let metrics = crate::hub::events::HubEventMetrics::default();
        assert_eq!(
            registry.complete_recovery_snapshot(
                request,
                WebRtcRecoverySnapshotResult::Empty,
                &metrics,
            ),
            WebRtcRecoveryDispatchOutcome::NoPayload
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.counters["snapshot.backpressure_recovery.empty"], 1);
        assert!(!snapshot
            .counters
            .contains_key("snapshot.backpressure_recovery.sent"));
        assert!(registry.drain_recovery_requests(Instant::now()).is_empty());
    }

    #[test]
    fn runner_keeps_peer_generation_and_bounded_command_queue() {
        let (command_tx, _rx) = tokio::sync::mpsc::channel(PEER_SEND_CHANNEL_CAPACITY);
        let runner = WebRtcTransportRunner::new(
            ClientId::browser("browser-1"),
            "browser-1".to_string(),
            7,
            command_tx,
        );

        assert_eq!(runner.client_id, ClientId::browser("browser-1"));
        assert_eq!(runner.browser_identity, "browser-1");
        assert_eq!(runner.generation, 7);
        assert_eq!(runner.command_tx.max_capacity(), PEER_SEND_CHANNEL_CAPACITY);
    }
}
