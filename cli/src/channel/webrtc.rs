//! WebRTC DataChannel implementation.
//!
//! This module provides `WebRtcChannel`, an implementation of the `Channel`
//! trait that communicates through a WebRTC DataChannel with optional E2E encryption.
//!
//! # Architecture
//!
//! ```text
//! WebRtcChannel
//!     |-- PeerConnection (rustrtc)
//!     |-- DataChannel (SCTP - reliable ordered, no message size limit)
//!     |-- Optional E2E encryption (via CryptoService = Arc<Mutex<VodozemacCrypto>>)
//!     |-- Gzip compression (via compression module)
//!     `-- Signaling via ActionCable
//! ```
//!
//! # Key Differences from ActionCable
//!
//! - No custom reliable delivery needed (SCTP provides it natively)
//! - Peer-to-peer when possible, TURN relay as fallback
//! - Signaling (offer/answer/ICE) via ActionCable, with optional E2E encryption

use async_trait::async_trait;
use mdns_sd::{HostnameResolutionEvent, ScopedIp, ServiceDaemon};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock};

use rustrtc::transports::ice::IceCandidate;
use rustrtc::transports::sctp::DataChannel;
use rustrtc::DataChannelEvent;
use rustrtc::{
    IceServer, PeerConnection, PeerConnectionEvent, RtcConfiguration, SdpType, SessionDescription,
};

use crate::relay::crypto_service::CryptoService;
use crate::relay::olm_crypto::{
    CONTENT_FILE, CONTENT_FILE_CHUNK, CONTENT_MSG, CONTENT_PTY, CONTENT_STREAM,
};

/// Shared ICE config cache for all WebRTC channels in this process.
///
/// Keyed by `{server_url}|{hub_id}` so repeated offers for the same hub can
/// reuse TURN/STUN config without waiting on another Rails request.
fn ice_config_cache() -> &'static std::sync::Mutex<HashMap<String, CachedIceConfig>> {
    static CACHE: OnceLock<std::sync::Mutex<HashMap<String, CachedIceConfig>>> = OnceLock::new();
    CACHE.get_or_init(|| std::sync::Mutex::new(HashMap::new()))
}

/// Shared mDNS daemon for resolving browser `.local` ICE candidates.
///
/// Browser host candidates can use mDNS names for privacy. `rustrtc` expects
/// numeric socket addresses, so we resolve `.local.` hostnames to IPs here and
/// rewrite the candidate SDP before handing it to the parser.
fn mdns_daemon() -> Option<ServiceDaemon> {
    static DAEMON: OnceLock<Option<ServiceDaemon>> = OnceLock::new();
    DAEMON
        .get_or_init(|| match ServiceDaemon::new() {
            Ok(daemon) => Some(daemon),
            Err(err) => {
                log::warn!("[WebRTC] Failed to initialize mDNS resolver: {err}");
                None
            }
        })
        .clone()
}

#[derive(Clone, Debug)]
struct CachedIceServerConfig {
    urls: String,
    username: Option<String>,
    credential: Option<String>,
}

#[derive(Clone, Debug)]
struct CachedIceConfig {
    fetched_at: Instant,
    ice_servers: Vec<CachedIceServerConfig>,
}

/// Incoming PTY input from browser via binary DataChannel frame.
///
/// Parsed from `CONTENT_PTY` with input flag set (flags & 0x02).
/// Bypasses JSON/Lua for zero-overhead keystroke delivery.
#[derive(Debug)]
pub struct PtyInputIncoming {
    /// Session UUID identifying the target PTY.
    pub session_uuid: String,
    /// Raw input bytes from browser.
    pub data: Vec<u8>,
    /// Browser identity key (for per-client focus tracking).
    pub browser_identity: String,
}

/// Incoming file from browser via binary DataChannel frame.
///
/// Parsed from `CONTENT_FILE`. The browser sends image/file data
/// which the CLI writes to a temp file and injects the path into the PTY.
#[derive(Debug)]
pub struct FileInputIncoming {
    /// Session UUID identifying the target PTY.
    pub session_uuid: String,
    /// Browser identity key for routing to the owning client worker.
    pub browser_identity: String,
    /// Original filename from the browser (e.g., "screenshot.png").
    pub filename: String,
    /// Raw file bytes.
    pub data: Vec<u8>,
}

/// Incoming stream frame from browser via DataChannel.
#[derive(Debug)]
pub struct StreamIncoming {
    /// Browser identity that sent this frame.
    pub browser_identity: String,
    /// Stream frame type (OPEN, DATA, CLOSE).
    pub frame_type: u8,
    /// Stream identifier.
    pub stream_id: u16,
    /// Frame payload.
    pub payload: Vec<u8>,
}

/// In-progress chunked file transfer reassembly state.
struct FileChunkAssembly {
    /// Session UUID identifying the target PTY.
    session_uuid: String,
    /// Browser identity key for routing to the owning client worker.
    browser_identity: String,
    filename: String,
    /// Accumulated file data across chunks.
    data: Vec<u8>,
}

use super::compression::maybe_compress;
use super::{
    Channel, ChannelConfig, ChannelError, ConnectionState, IncomingMessage, PeerId,
    SharedConnectionState,
};

/// Prepare a PTY payload for wire transmission, honoring an optional compression
/// threshold and passing pre-gzipped payloads through unchanged.
///
/// Callers (e.g. `queue_webrtc_terminal_snapshot`) may pre-compress very large
/// blobs — the raw snapshot can exceed SCTP's negotiated max message size, and
/// compressing inside `send_pty_raw` happens too late to save it. Pre-gzipped
/// data starts with the gzip magic bytes `1f 8b`; routing prefixes for live
/// output (`0x01`) and snapshots (`0x02`) are disjoint from that magic, so a
/// byte-sniff is unambiguous.
fn pty_payload_with_compression<'a>(
    data: &'a [u8],
    threshold: Option<usize>,
) -> Result<(std::borrow::Cow<'a, [u8]>, bool), ChannelError> {
    // Pre-gzipped: pass through as compressed, skip re-gzip.
    if data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b {
        return Ok((std::borrow::Cow::Borrowed(data), true));
    }

    let Some(threshold) = threshold else {
        return Ok((std::borrow::Cow::Borrowed(data), false));
    };

    let compressed = maybe_compress(data, Some(threshold))
        .map_err(|e| ChannelError::CompressionError(e.to_string()))?;
    if compressed[0] == 0x1f {
        Ok((std::borrow::Cow::Owned(compressed[1..].to_vec()), true))
    } else {
        Ok((std::borrow::Cow::Borrowed(data), false))
    }
}

/// Internal message for the receive queue.
#[derive(Debug)]
pub(crate) struct RawIncoming {
    pub(crate) payload: Vec<u8>,
    pub(crate) sender: PeerId,
}

/// Outgoing signal destined for a browser, sent via ActionCable relay.
///
/// Produced by the ICE candidate forwarder task, drained
/// by `server_comms` tick loop, and forwarded through `CommandChannelHandle::perform`.
#[derive(Debug)]
pub enum OutgoingSignal {
    /// Encrypted ICE candidate for a specific browser.
    Ice {
        /// Target browser identity (`identityKey:tabId`).
        browser_identity: String,
        /// E2E encrypted envelope (opaque to Rails).
        envelope: serde_json::Value,
    },
}

/// Configuration for WebRTC signaling.
#[derive(Clone, Debug)]
pub struct WebRtcConfig {
    /// Base URL for signaling (e.g., "https://trybotster.com").
    pub server_url: String,
    /// API key for authentication.
    pub api_key: String,
    /// Hub ID for routing.
    pub hub_id: String,
    /// Browser identity we're connecting to.
    pub browser_identity: String,
}

/// Builder for `WebRtcChannel`.
pub struct WebRtcChannelBuilder {
    server_url: Option<String>,
    api_key: Option<String>,
    crypto_service: Option<CryptoService>,
    signal_tx: Option<mpsc::Sender<OutgoingSignal>>,
    stream_frame_tx: Option<mpsc::Sender<StreamIncoming>>,
    pty_input_tx: Option<mpsc::Sender<PtyInputIncoming>>,
    file_input_tx: Option<mpsc::Sender<FileInputIncoming>>,
    hub_event_tx: Option<crate::hub::events::HubEventTx>,
}

impl std::fmt::Debug for WebRtcChannelBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebRtcChannelBuilder")
            .field("server_url", &self.server_url)
            .field("crypto_service", &self.crypto_service.is_some())
            .field("signal_tx", &self.signal_tx.is_some())
            .field("stream_frame_tx", &self.stream_frame_tx.is_some())
            .field("pty_input_tx", &self.pty_input_tx.is_some())
            .field("file_input_tx", &self.file_input_tx.is_some())
            .field("hub_event_tx", &self.hub_event_tx.is_some())
            .finish()
    }
}

impl Default for WebRtcChannelBuilder {
    fn default() -> Self {
        Self {
            server_url: None,
            api_key: None,
            crypto_service: None,
            signal_tx: None,
            stream_frame_tx: None,
            pty_input_tx: None,
            file_input_tx: None,
            hub_event_tx: None,
        }
    }
}

impl WebRtcChannelBuilder {
    /// Create a new builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the server URL for signaling.
    #[must_use]
    pub fn server_url(mut self, url: impl Into<String>) -> Self {
        self.server_url = Some(url.into());
        self
    }

    /// Set the API key for authentication.
    #[must_use]
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    /// Set the crypto service for E2E encryption.
    #[must_use]
    pub fn crypto_service(mut self, cs: CryptoService) -> Self {
        self.crypto_service = Some(cs);
        self
    }

    /// Set the outgoing signal sender for ICE candidate relay via ActionCable.
    #[must_use]
    pub fn signal_tx(mut self, tx: mpsc::Sender<OutgoingSignal>) -> Self {
        self.signal_tx = Some(tx);
        self
    }

    /// Set the stream frame sender for TCP stream multiplexer frames.
    #[must_use]
    pub fn stream_frame_tx(mut self, tx: mpsc::Sender<StreamIncoming>) -> Self {
        self.stream_frame_tx = Some(tx);
        self
    }

    /// Set the PTY input sender for binary PTY input from browser.
    #[must_use]
    pub fn pty_input_tx(mut self, tx: mpsc::Sender<PtyInputIncoming>) -> Self {
        self.pty_input_tx = Some(tx);
        self
    }

    /// Set the file input sender for file transfers from browser.
    #[must_use]
    pub fn file_input_tx(mut self, tx: mpsc::Sender<FileInputIncoming>) -> Self {
        self.file_input_tx = Some(tx);
        self
    }

    /// Set the Hub event channel sender for DC opened notifications.
    #[must_use]
    pub(crate) fn hub_event_tx(mut self, tx: crate::hub::events::HubEventTx) -> Self {
        self.hub_event_tx = Some(tx);
        self
    }

    /// Build the channel.
    ///
    /// # Panics
    ///
    /// Panics if required fields are not set.
    #[must_use]
    pub fn build(self) -> WebRtcChannel {
        let (close_tx, close_rx) = tokio::sync::watch::channel(false);
        WebRtcChannel {
            server_url: self.server_url.expect("server_url required"),
            api_key: self.api_key.expect("api_key required"),
            crypto_service: self.crypto_service,
            signal_tx: self.signal_tx,
            stream_frame_tx: self.stream_frame_tx,
            pty_input_tx: self.pty_input_tx,
            file_input_tx: self.file_input_tx,
            peer_connection: Arc::new(Mutex::new(None)),
            data_channel: Arc::new(Mutex::new(None)),
            data_channel_id: Arc::new(Mutex::new(None)),
            state: SharedConnectionState::new(),
            peers: Arc::new(RwLock::new(HashSet::new())),
            config: Arc::new(Mutex::new(None)),
            recv_rx: Arc::new(Mutex::new(None)),
            recv_tx: Arc::new(Mutex::new(None)),
            peer_olm_key: Arc::new(Mutex::new(None)),
            decrypt_failures: Arc::new(AtomicU32::new(0)),
            offer_generation: Arc::new(AtomicU64::new(0)),
            dc_opened: Arc::new(AtomicBool::new(false)),
            hub_event_tx: self.hub_event_tx,
            close_complete_tx: close_tx,
            close_complete_rx: close_rx,
            event_loop_handle: Arc::new(Mutex::new(None)),
        }
    }
}

/// WebRTC DataChannel-based channel implementation.
///
/// Provides WebRTC communication with SCTP reliable delivery and optional E2E encryption.
#[derive(Clone)]
pub struct WebRtcChannel {
    /// Server URL for signaling.
    server_url: String,
    /// API key for auth.
    api_key: String,
    /// Optional crypto service for E2E encryption.
    crypto_service: Option<CryptoService>,
    /// Sender for outgoing signals (ICE candidates) to relay via ActionCable.
    signal_tx: Option<mpsc::Sender<OutgoingSignal>>,
    /// Sender for incoming stream multiplexer frames.
    stream_frame_tx: Option<mpsc::Sender<StreamIncoming>>,
    /// Sender for incoming PTY input from browser (binary, bypasses JSON/Lua).
    pty_input_tx: Option<mpsc::Sender<PtyInputIncoming>>,
    /// Sender for incoming file transfers from browser.
    file_input_tx: Option<mpsc::Sender<FileInputIncoming>>,
    /// WebRTC peer connection (rustrtc — Clone wraps Arc internally).
    peer_connection: Arc<Mutex<Option<PeerConnection>>>,
    /// WebRTC data channel (set by event loop when browser creates it).
    data_channel: Arc<Mutex<Option<Arc<DataChannel>>>>,
    /// DataChannel SCTP stream ID (for `pc.send_data(channel_id, ...)`).
    data_channel_id: Arc<Mutex<Option<u16>>>,
    /// Shared connection state.
    state: Arc<SharedConnectionState>,
    /// Connected peers (browser identities with active sessions).
    peers: Arc<RwLock<HashSet<PeerId>>>,
    /// Channel configuration.
    config: Arc<Mutex<Option<ChannelConfig>>>,
    /// Receive queue.
    recv_rx: Arc<Mutex<Option<mpsc::Receiver<RawIncoming>>>>,
    /// Send side of receive queue.
    recv_tx: Arc<Mutex<Option<mpsc::Sender<RawIncoming>>>>,
    /// Peer's Olm identity key (set when SDP offer is handled).
    peer_olm_key: Arc<Mutex<Option<String>>>,
    /// Consecutive decryption failure count for session health monitoring.
    decrypt_failures: Arc<AtomicU32>,
    /// Hub-side offer generation that owns this channel instance.
    offer_generation: Arc<AtomicU64>,
    /// Set to `true` when the DataChannel opens; consumed by `take_dc_opened()`.
    /// Kept as test-only fallback when `hub_event_tx` is None.
    dc_opened: Arc<AtomicBool>,
    /// Event channel sender for DC opened notifications.
    /// When set, the event loop sends `HubEvent::DcOpened` instead of
    /// setting the atomic bool.
    hub_event_tx: Option<crate::hub::events::HubEventTx>,
    /// Set to `true` when the connection closes (pc/dc sockets released).
    /// Uses `watch` so late subscribers see the value even if the close already happened.
    close_complete_tx: tokio::sync::watch::Sender<bool>,
    close_complete_rx: tokio::sync::watch::Receiver<bool>,
    /// Handle for the spawned event loop task (for cleanup on disconnect).
    event_loop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl std::fmt::Debug for WebRtcChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebRtcChannel")
            .field("server_url", &self.server_url)
            .field("crypto_service", &self.crypto_service.is_some())
            .field("signal_tx", &self.signal_tx.is_some())
            .field("stream_frame_tx", &self.stream_frame_tx.is_some())
            .field("pty_input_tx", &self.pty_input_tx.is_some())
            .finish()
    }
}

impl WebRtcChannel {
    /// Create a new builder.
    #[must_use]
    pub fn builder() -> WebRtcChannelBuilder {
        WebRtcChannelBuilder::new()
    }

    /// Timeout for the ICE config HTTP request. Keeps the tick loop responsive
    /// even when the endpoint is slow or the runtime is under load from
    /// concurrent WebRTC teardown tasks.
    const ICE_CONFIG_TIMEOUT: Duration = Duration::from_secs(10);

    /// Fresh-cache TTL for ICE config reuse across new offers.
    ///
    /// Short enough to avoid leaning on stale TURN credentials, but long enough
    /// to skip repeated `/webrtc` requests during reconnect churn.
    const ICE_CONFIG_CACHE_TTL: Duration = Duration::from_secs(60);

    /// Maximum age for stale ICE config fallback when a refresh fails.
    ///
    /// Lets reconnects keep moving through transient Rails/TURN endpoint issues
    /// instead of hanging until a manual refresh.
    const ICE_CONFIG_STALE_FALLBACK_TTL: Duration = Duration::from_secs(5 * 60);

    fn ice_cache_key(&self, hub_id: &str) -> String {
        format!("{}|{hub_id}", self.server_url)
    }

    fn ice_servers_from_cached(ice_servers: &[CachedIceServerConfig]) -> Vec<IceServer> {
        ice_servers
            .iter()
            .map(|server| IceServer {
                urls: vec![server.urls.clone()],
                username: server.username.clone(),
                credential: server.credential.clone(),
                credential_type: rustrtc::IceCredentialType::Password,
            })
            .collect()
    }

    fn cached_ice_servers(&self, cache_key: &str, max_age: Duration) -> Option<Vec<IceServer>> {
        let cache = ice_config_cache()
            .lock()
            .expect("ICE config cache lock poisoned");
        let cached = cache.get(cache_key)?;
        if cached.fetched_at.elapsed() > max_age {
            return None;
        }
        Some(Self::ice_servers_from_cached(&cached.ice_servers))
    }

    #[allow(dead_code)]
    fn store_cached_ice_servers(&self, cache_key: String, ice_servers: Vec<CachedIceServerConfig>) {
        let mut cache = ice_config_cache()
            .lock()
            .expect("ICE config cache lock poisoned");
        cache.insert(
            cache_key,
            CachedIceConfig {
                fetched_at: Instant::now(),
                ice_servers,
            },
        );
    }

    /// Prefetch ICE config into the global cache so the first offer doesn't
    /// pay the HTTP round-trip cost. Call after hub registration when
    /// `server_url`, `api_key`, and `hub_id` are known.
    pub async fn prefetch_ice_config(server_url: &str, api_key: &str, hub_id: &str) {
        let cache_key = format!("{server_url}|{hub_id}");
        {
            let cache = ice_config_cache()
                .lock()
                .expect("ICE config cache lock poisoned");
            if let Some(cached) = cache.get(&cache_key) {
                if cached.fetched_at.elapsed() <= Self::ICE_CONFIG_CACHE_TTL {
                    log::debug!(
                        "[WebRTC] ICE config already cached for hub {}",
                        &hub_id[..hub_id.len().min(8)]
                    );
                    return;
                }
            }
        }

        match Self::fetch_and_cache_ice_config(server_url, api_key, hub_id, &cache_key).await {
            Ok(_) => {}
            Err(e) => log::warn!("[WebRTC] ICE prefetch failed: {e:#}"),
        }
    }

    /// Core HTTP fetch + parse + cache store for ICE config.
    ///
    /// Shared by [`prefetch_ice_config`] and [`fetch_ice_config`]. Returns the
    /// parsed server list on success; callers handle stale-cache fallback.
    async fn fetch_and_cache_ice_config(
        server_url: &str,
        api_key: &str,
        hub_id: &str,
        cache_key: &str,
    ) -> Result<Vec<CachedIceServerConfig>, ChannelError> {
        let url = format!("{server_url}/hubs/{hub_id}/webrtc");
        let started_at = Instant::now();

        let client = reqwest::Client::builder()
            .timeout(Self::ICE_CONFIG_TIMEOUT)
            .build()
            .map_err(|e| {
                ChannelError::ConnectionFailed(format!("Failed to build HTTP client: {e:#}"))
            })?;

        let response = client
            .get(&url)
            .bearer_auth(api_key)
            .send()
            .await
            .map_err(|e| {
                ChannelError::ConnectionFailed(format!("Failed to fetch ICE config: {e:#}"))
            })?;

        if !response.status().is_success() {
            return Err(ChannelError::ConnectionFailed(format!(
                "ICE config request failed: {}",
                response.status()
            )));
        }

        #[derive(serde::Deserialize)]
        struct IceConfig {
            ice_servers: Vec<IceServerJson>,
        }

        #[derive(serde::Deserialize)]
        struct IceServerJson {
            urls: String,
            username: Option<String>,
            credential: Option<String>,
        }

        let config = response.json::<IceConfig>().await.map_err(|e| {
            ChannelError::ConnectionFailed(format!("Failed to parse ICE config: {e:#}"))
        })?;

        let cached_servers: Vec<CachedIceServerConfig> = config
            .ice_servers
            .into_iter()
            .map(|s| CachedIceServerConfig {
                urls: s.urls,
                username: s.username,
                credential: s.credential,
            })
            .collect();

        let mut cache = ice_config_cache()
            .lock()
            .expect("ICE config cache lock poisoned");
        cache.insert(
            cache_key.to_string(),
            CachedIceConfig {
                fetched_at: Instant::now(),
                ice_servers: cached_servers.clone(),
            },
        );
        drop(cache);

        log::info!(
            "[WebRTC] Fetched ICE config for hub {} in {}ms",
            &hub_id[..hub_id.len().min(8)],
            started_at.elapsed().as_millis()
        );

        Ok(cached_servers)
    }

    /// Fetch ICE server configuration from Rails.
    ///
    /// Uses [`fetch_and_cache_ice_config`] for the HTTP call, with stale-cache
    /// fallback on failure (keeps reconnects working through transient issues).
    async fn fetch_ice_config(&self, hub_id: &str) -> Result<Vec<IceServer>, ChannelError> {
        let cache_key = self.ice_cache_key(hub_id);
        if let Some(cached) = self.cached_ice_servers(&cache_key, Self::ICE_CONFIG_CACHE_TTL) {
            log::debug!(
                "[WebRTC] Using cached ICE config for hub {}",
                &hub_id[..hub_id.len().min(8)]
            );
            return Ok(cached);
        }

        match Self::fetch_and_cache_ice_config(&self.server_url, &self.api_key, hub_id, &cache_key)
            .await
        {
            Ok(cached_servers) => Ok(Self::ice_servers_from_cached(&cached_servers)),
            Err(e) => {
                // Fall back to stale cache on any failure.
                if let Some(cached) =
                    self.cached_ice_servers(&cache_key, Self::ICE_CONFIG_STALE_FALLBACK_TTL)
                {
                    log::warn!("[WebRTC] ICE config fetch failed, using stale cache: {e:#}");
                    Ok(cached)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Create the WebRTC peer connection.
    fn create_peer_connection(
        &self,
        ice_servers: Vec<IceServer>,
    ) -> Result<PeerConnection, ChannelError> {
        let config = RtcConfiguration {
            ice_servers,
            // Reduce STUN/TURN probe timeout from default 5s to 2s.
            // Failed probes (unreachable STUN/TURN servers) block ICE gathering
            // completion, causing bimodal 6s/11s delays at the default.
            stun_timeout: Duration::from_secs(2),
            // Match the CLI's own CONNECTION_TIMEOUT (15s) instead of the
            // default 30s, so failed ICE connections are detected promptly.
            ice_connection_timeout: Duration::from_secs(15),
            ..Default::default()
        };

        Ok(PeerConnection::new(config))
    }

    /// Handle incoming SDP offer from browser and create answer.
    ///
    /// Called when CLI receives an encrypted offer via ActionCable signal channel.
    pub async fn handle_sdp_offer(
        &self,
        sdp: &str,
        browser_identity: &str,
    ) -> Result<String, ChannelError> {
        // Check for existing connection
        let mut pc_guard = self.peer_connection.lock().await;

        if pc_guard.is_some() {
            // ICE restart: apply new offer on existing PC for renegotiation.
            // The browser sends a new offer with fresh ICE credentials when the
            // network path changes (wifi→cellular, NAT rebinding, etc.). We apply
            // it to the existing PC so ICE can gather new candidates without
            // tearing down the DataChannel or SCTP association.
            let pc = pc_guard.as_ref().unwrap().clone();
            drop(pc_guard);

            log::info!("[WebRTC] Applying ICE restart offer on existing connection");

            let offer = SessionDescription::parse(SdpType::Offer, sdp)
                .map_err(|e| ChannelError::ConnectionFailed(format!("Invalid SDP offer: {e}")))?;

            pc.set_remote_description(offer).await.map_err(|e| {
                ChannelError::ConnectionFailed(format!("Failed to set remote description: {e}"))
            })?;

            let answer = pc.create_answer().await.map_err(|e| {
                ChannelError::ConnectionFailed(format!("Failed to create answer: {e}"))
            })?;

            pc.set_local_description(answer.clone()).map_err(|e| {
                ChannelError::ConnectionFailed(format!("Failed to set local description: {e}"))
            })?;

            let mut sdp = answer.to_sdp_string();
            if !sdp.contains("max-message-size") {
                sdp = inject_max_message_size(&sdp, 16 * 1024 * 1024);
            }

            return Ok(sdp);
        }

        // Reset DC-opened flag so a stale signal from the previous PC doesn't
        // cause a premature peer_connected on the new connection.
        self.dc_opened.store(false, Ordering::Relaxed);

        // Fetch ICE config
        let config_guard = self.config.lock().await;
        let hub_id = config_guard
            .as_ref()
            .map(|c| c.hub_id.clone())
            .unwrap_or_default();
        drop(config_guard);

        let ice_servers = self.fetch_ice_config(&hub_id).await?;

        // Create peer connection (sync — no MediaEngine/Registry/APIBuilder boilerplate)
        let pc = self.create_peer_connection(ice_servers)?;

        // Parse and set remote description (offer from browser)
        let offer = SessionDescription::parse(SdpType::Offer, sdp)
            .map_err(|e| ChannelError::ConnectionFailed(format!("Invalid SDP offer: {e}")))?;

        pc.set_remote_description(offer).await.map_err(|e| {
            ChannelError::ConnectionFailed(format!("Failed to set remote description: {e}"))
        })?;

        // Create and set local description (answer)
        let answer = pc
            .create_answer()
            .await
            .map_err(|e| ChannelError::ConnectionFailed(format!("Failed to create answer: {e}")))?;

        pc.set_local_description(answer.clone()).map_err(|e| {
            ChannelError::ConnectionFailed(format!("Failed to set local description: {e}"))
        })?;

        // Store the peer's Olm key for encrypt routing.
        {
            let mut olm_key = self.peer_olm_key.lock().await;
            *olm_key = Some(crate::relay::extract_olm_key(browser_identity).to_string());
        }

        // Store PC clone in struct (for handle_ice_candidate and send methods)
        *pc_guard = Some(pc.clone());
        drop(pc_guard);

        // Spawn event loop task (replaces all on_* callbacks from webrtc-rs)
        let handle = self.spawn_event_loop(pc, browser_identity);
        *self.event_loop_handle.lock().await = Some(handle);

        // Add peer
        {
            let mut peers = self.peers.write().await;
            peers.insert(PeerId(browser_identity.to_string()));
        }

        // rustrtc omits a=max-message-size from SDP. Per RFC 8841, browsers
        // default to 65536 when absent, causing RTCErrorEvent on large sends
        // (e.g. encrypted screenshots). Chrome caps at 256KB when value is 0,
        // so use an explicit large value (16MB) instead.
        let mut sdp = answer.to_sdp_string();
        if !sdp.contains("max-message-size") {
            sdp = inject_max_message_size(&sdp, 16 * 1024 * 1024);
        }

        // Log the application section of SDP answer for debugging
        if let Some(app_idx) = sdp.find("m=application") {
            let section: String = sdp[app_idx..]
                .lines()
                .take(5)
                .collect::<Vec<_>>()
                .join(" | ");
            log::debug!("[WebRTC] SDP answer application section: {section}");
        } else {
            log::warn!("[WebRTC] SDP answer has no m=application section!");
        }

        Ok(sdp)
    }

    /// Spawn the event loop task that replaces webrtc-rs callbacks.
    ///
    /// This single task handles:
    /// - ICE candidate forwarding (via sub-task)
    /// - Peer connection state changes
    /// - DataChannel open/message/close events
    fn spawn_event_loop(
        &self,
        pc: PeerConnection,
        browser_identity: &str,
    ) -> tokio::task::JoinHandle<()> {
        // Clone all Arc/mpsc handles needed (same set as the old callbacks)
        let state = Arc::clone(&self.state);
        let data_channel = Arc::clone(&self.data_channel);
        let data_channel_id = Arc::clone(&self.data_channel_id);
        let peer_connection = Arc::clone(&self.peer_connection);
        let close_complete = self.close_complete_tx.clone();
        let recv_tx = Arc::clone(&self.recv_tx);
        let peers = Arc::clone(&self.peers);
        let crypto_service = self.crypto_service.clone();
        let browser_id = browser_identity.to_string();
        let decrypt_failures = Arc::clone(&self.decrypt_failures);
        let stream_frame_tx = self.stream_frame_tx.clone();
        let pty_input_tx = self.pty_input_tx.clone();
        let file_input_tx = self.file_input_tx.clone();
        let dc_opened = Arc::clone(&self.dc_opened);
        let offer_generation = Arc::clone(&self.offer_generation);
        let hub_event_tx = self.hub_event_tx.clone();

        // Subscribe to ICE candidates for forwarding
        let mut ice_rx = pc.subscribe_ice_candidates();
        let ice_crypto = self.crypto_service.clone();
        let ice_signal_tx = self.signal_tx.clone();
        let ice_browser_id = browser_id.clone();

        // Subscribe to peer connection state changes
        let mut peer_state_rx = pc.subscribe_peer_state();

        tokio::spawn(async move {
            // Sub-task: forward local ICE candidates to browser via ActionCable
            let ice_task = tokio::spawn(async move {
                loop {
                    match ice_rx.recv().await {
                        Ok(candidate) => {
                            // Build JSON matching browser's RTCIceCandidateInit format
                            let candidate_json = serde_json::json!({
                                "candidate": format!("candidate:{}", candidate.to_sdp()),
                                "sdpMid": "0",
                                "sdpMLineIndex": 0,
                            });

                            let payload = serde_json::json!({
                                "type": "ice",
                                "candidate": candidate_json,
                            });

                            // Encrypt with E2E encryption
                            let envelope = if let Some(ref cs) = ice_crypto {
                                let plaintext = serde_json::to_vec(&payload).unwrap_or_default();
                                match cs.lock() {
                                    Ok(mut guard) => match guard.encrypt(
                                        &plaintext,
                                        crate::relay::extract_olm_key(&ice_browser_id),
                                    ) {
                                        Ok(env) => match serde_json::to_value(&env) {
                                            Ok(v) => v,
                                            Err(e) => {
                                                log::error!("[WebRTC] Failed to serialize ICE envelope: {e}");
                                                continue;
                                            }
                                        },
                                        Err(e) => {
                                            log::error!(
                                                "[WebRTC] Failed to encrypt ICE candidate: {e}"
                                            );
                                            continue;
                                        }
                                    },
                                    Err(e) => {
                                        log::error!("[WebRTC] Crypto mutex poisoned: {e}");
                                        continue;
                                    }
                                }
                            } else {
                                payload
                            };

                            if let Some(ref tx) = ice_signal_tx {
                                match tx.try_send(OutgoingSignal::Ice {
                                    browser_identity: ice_browser_id.clone(),
                                    envelope,
                                }) {
                                    Ok(()) => {}
                                    Err(mpsc::error::TrySendError::Full(_)) => {
                                        log::warn!(
                                            "[WebRTC] Outgoing signal queue full; dropping ICE candidate for {}",
                                            &ice_browser_id[..ice_browser_id.len().min(8)]
                                        );
                                    }
                                    Err(mpsc::error::TrySendError::Closed(_)) => {
                                        log::warn!(
                                            "[WebRTC] Signal queue closed; dropping ICE candidate"
                                        );
                                    }
                                }
                            } else {
                                log::warn!("[WebRTC] No signal_tx -- cannot relay ICE candidate");
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                            log::warn!("[WebRTC] ICE candidate subscription lagged by {n}");
                        }
                    }
                }
            });

            // Track the DC reader so we can abort it on exit
            let mut dc_reader_handle: Option<tokio::task::JoinHandle<()>> = None;

            // Notify from DC reader → event loop when DataChannel closes.
            // Without this, the event loop stays alive after DC close (peer
            // state may remain Connected), blocking new offers with
            // "Connection in progress".
            let dc_closed = Arc::new(tokio::sync::Notify::new());

            // Main event loop: select between PeerConnection events, state changes,
            // and DC close notifications
            loop {
                tokio::select! {
                    event = pc.recv() => {
                        match event {
                            Some(PeerConnectionEvent::DataChannel(dc)) => {
                                log::info!("[WebRTC] Data channel opened: {}", dc.label);

                                let channel_id = dc.id;

                                // Store data channel and ID for send methods
                                // dc is already Arc<DataChannel> from PeerConnectionEvent
                                *data_channel.lock().await = Some(Arc::clone(&dc));
                                *data_channel_id.lock().await = Some(channel_id);

                                // Notify DC opened
                                if let Some(ref tx) = hub_event_tx {
                                    let _ = tx.send(crate::hub::events::HubEvent::DcOpened {
                                        browser_identity: browser_id.clone(),
                                        generation: offer_generation.load(Ordering::Relaxed),
                                    });
                                } else {
                                    dc_opened.store(true, Ordering::Relaxed);
                                }

                                // Abort previous DC reader if replacing (defensive)
                                if let Some(h) = dc_reader_handle.take() {
                                    h.abort();
                                }

                                // Spawn DataChannel message reader
                                let recv_tx = Arc::clone(&recv_tx);
                                let peers = Arc::clone(&peers);
                                let crypto = crypto_service.clone();
                                let browser = browser_id.clone();
                                let failures = Arc::clone(&decrypt_failures);
                                let stream_tx = stream_frame_tx.clone();
                                let pty_tx = pty_input_tx.clone();
                                let file_tx = file_input_tx.clone();
                                let hub_tx = hub_event_tx.clone();
                                let dc_reader = Arc::clone(&dc);
                                let dc_closed_signal = Arc::clone(&dc_closed);

                                dc_reader_handle = Some(tokio::spawn(async move {
                                    let mut chunk_assemblies: std::collections::HashMap<u8, FileChunkAssembly> = std::collections::HashMap::new();
                                    loop {
                                        match dc_reader.recv().await {
                                            Some(DataChannelEvent::Message(data)) => {
                                                handle_dc_message(
                                                    &data,
                                                    &browser,
                                                    &crypto,
                                                    &failures,
                                                    &recv_tx,
                                                    &peers,
                                                    &stream_tx,
                                                    &pty_tx,
                                                    &file_tx,
                                                    &hub_tx,
                                                    &mut chunk_assemblies,
                                                )
                                                .await;
                                            }
                                            Some(DataChannelEvent::Open) => {
                                                log::debug!("[WebRTC-DC] DataChannel Open event");
                                            }
                                            Some(DataChannelEvent::Close) | None => {
                                                log::info!("[WebRTC-DC] DataChannel closed");
                                                dc_closed_signal.notify_one();
                                                break;
                                            }
                                        }
                                    }
                                }));
                            }
                            Some(PeerConnectionEvent::Track(_)) => {
                                // We don't use media tracks, ignore
                            }
                            None => {
                                // PeerConnection event channel closed
                                log::info!("[WebRTC] PeerConnection event loop ended");
                                state.set(ConnectionState::Disconnected).await;
                                data_channel.lock().await.take();
                                data_channel_id.lock().await.take();
                                peer_connection.lock().await.take();
                                let _ = close_complete.send(true);
                                break;
                            }
                        }
                    }
                    _ = peer_state_rx.changed() => {
                        let s = *peer_state_rx.borrow();
                        log::info!("[WebRTC] Connection state changed: {s:?}");
                        match s {
                            rustrtc::PeerConnectionState::Connected => {
                                state.set(ConnectionState::Connected).await;
                            }
                            rustrtc::PeerConnectionState::Disconnected
                            | rustrtc::PeerConnectionState::Failed => {
                                state.set(ConnectionState::Disconnected).await;
                                data_channel.lock().await.take();
                                data_channel_id.lock().await.take();
                                // Close is sync — no 60-second wait
                                if let Some(pc) = peer_connection.lock().await.take() {
                                    pc.close();
                                }
                                let _ = close_complete.send(true);
                                break;
                            }
                            rustrtc::PeerConnectionState::Closed => {
                                state.set(ConnectionState::Disconnected).await;
                                data_channel.lock().await.take();
                                data_channel_id.lock().await.take();
                                peer_connection.lock().await.take();
                                let _ = close_complete.send(true);
                                break;
                            }
                            _ => {}
                        }
                    }
                    _ = dc_closed.notified() => {
                        // DataChannel closed but peer state didn't transition —
                        // tear down so new offers aren't blocked.
                        log::info!("[WebRTC] DataChannel closed, tearing down connection");
                        state.set(ConnectionState::Disconnected).await;
                        data_channel.lock().await.take();
                        data_channel_id.lock().await.take();
                        if let Some(pc) = peer_connection.lock().await.take() {
                            pc.close();
                        }
                        let _ = close_complete.send(true);
                        break;
                    }
                }
            }

            // Cleanup: abort sub-tasks so nothing leaks
            ice_task.abort();
            if let Some(h) = dc_reader_handle {
                h.abort();
            }
        })
    }

    /// Handle incoming ICE candidate from browser.
    pub async fn handle_ice_candidate(
        &self,
        candidate: &str,
        _sdp_mid: Option<&str>,
        _sdp_mline_index: Option<u16>,
    ) -> Result<(), ChannelError> {
        {
            let pc_guard = self.peer_connection.lock().await;
            if pc_guard.is_none() {
                return Err(ChannelError::ConnectionFailed(
                    "No peer connection".to_string(),
                ));
            }
        }

        // Parse the candidate SDP string (browser sends "candidate:..." format)
        let sdp_str = candidate.trim_start_matches("candidate:");
        let normalized_sdp = Self::normalize_candidate_sdp(sdp_str).await;
        let candidate_for_parse = normalized_sdp.as_deref().unwrap_or(sdp_str);
        let is_mdns_hostname = Self::is_mdns_hostname_candidate(sdp_str);

        let ice_candidate = match IceCandidate::from_sdp(candidate_for_parse) {
            Ok(parsed) => parsed,
            Err(parse_err) => {
                // Browser mDNS host candidates are only useful if the Rust side
                // can resolve them. If not, drop them and keep moving rather
                // than stalling the answer path on an unparseable candidate.
                if is_mdns_hostname {
                    log::debug!(
                        "[WebRTC] Ignoring unresolved mDNS ICE candidate from browser: {}",
                        Self::candidate_preview(sdp_str)
                    );
                    return Ok(());
                }

                // Retry the original SDP if a proactive rewrite made parsing worse.
                if normalized_sdp.is_some() {
                    match IceCandidate::from_sdp(sdp_str) {
                        Ok(parsed) => {
                            log::debug!(
                                "[WebRTC] Falling back to original hostname ICE candidate after rewritten parse failure"
                            );
                            parsed
                        }
                        Err(original_err) => {
                            return Err(ChannelError::ConnectionFailed(format!(
                                "Failed to parse ICE candidate: rewritten={parse_err}; original={original_err}"
                            )));
                        }
                    }
                } else {
                    return Err(ChannelError::ConnectionFailed(format!(
                        "Failed to parse ICE candidate: {parse_err}"
                    )));
                }
            }
        };

        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::ConnectionFailed("No peer connection".to_string()))?;

        pc.add_ice_candidate(ice_candidate).map_err(|e| {
            ChannelError::ConnectionFailed(format!("Failed to add ICE candidate: {e}"))
        })?;

        Ok(())
    }

    /// Normalize an ICE candidate SDP string before passing it into rustrtc.
    ///
    /// Chrome can emit mDNS host candidates (`*.local`) for privacy. Those are
    /// fine in browser-to-browser flows, but the Rust side needs a numeric IP
    /// candidate to guarantee parser and transport compatibility.
    async fn normalize_candidate_sdp(candidate_sdp: &str) -> Option<String> {
        Self::rewrite_hostname_candidate_to_ip(candidate_sdp).await
    }

    /// Rewrite a hostname ICE candidate to an IP candidate when possible.
    ///
    /// Browser ICE can include mDNS host candidates (`*.local`) for privacy.
    /// This keeps compatibility with parsers that only accept numeric IPs.
    async fn rewrite_hostname_candidate_to_ip(candidate_sdp: &str) -> Option<String> {
        let mut parts: Vec<String> = candidate_sdp
            .split_whitespace()
            .map(std::string::ToString::to_string)
            .collect();

        // candidate:<foundation> <component> <transport> <priority> <address> <port> ...
        if parts.len() < 6 {
            return None;
        }

        let host = parts[4].clone();
        let port: u16 = parts[5].parse().ok()?;

        if host.parse::<IpAddr>().is_ok() {
            return None;
        }

        if host.ends_with(".local") {
            let ip = Self::resolve_mdns_hostname(&host).await?;
            parts[4] = ip.to_string();
            log::debug!("[WebRTC] Rewrote mDNS ICE candidate {host} -> {}", parts[4]);
            return Some(parts.join(" "));
        }

        let mut resolved = tokio::net::lookup_host((host.as_str(), port)).await.ok()?;
        let ip = resolved.next()?.ip().to_string();
        parts[4] = ip;
        log::debug!(
            "[WebRTC] Rewrote hostname ICE candidate {host} -> {}",
            parts[4]
        );
        Some(parts.join(" "))
    }

    async fn resolve_mdns_hostname(host: &str) -> Option<IpAddr> {
        const MDNS_HOSTNAME_TIMEOUT_MS: u64 = 750;

        let daemon = mdns_daemon()?;
        let hostname = Self::canonical_mdns_hostname(host);
        let receiver = daemon
            .resolve_hostname(&hostname, Some(MDNS_HOSTNAME_TIMEOUT_MS))
            .ok()?;
        let _cleanup = MdnsResolveCleanup::new(daemon.clone(), hostname.clone());

        let result = loop {
            match receiver.recv_async().await.ok()? {
                HostnameResolutionEvent::AddressesFound(found_hostname, addresses) => {
                    if !found_hostname.eq_ignore_ascii_case(&hostname) {
                        continue;
                    }
                    if let Some(ip) = Self::select_mdns_ip(&addresses) {
                        break Some(ip);
                    }
                }
                HostnameResolutionEvent::SearchTimeout(found_hostname)
                | HostnameResolutionEvent::SearchStopped(found_hostname) => {
                    if found_hostname.eq_ignore_ascii_case(&hostname) {
                        break None;
                    }
                }
                HostnameResolutionEvent::SearchStarted(_) => {}
                _ => {}
            }
        };
        result
    }

    fn canonical_mdns_hostname(host: &str) -> String {
        if host.ends_with(".local.") {
            host.to_string()
        } else if host.ends_with(".local") {
            format!("{host}.")
        } else {
            host.to_string()
        }
    }

    fn select_mdns_ip(addresses: &HashSet<ScopedIp>) -> Option<IpAddr> {
        addresses
            .iter()
            .find_map(|addr| match addr {
                ScopedIp::V4(v4) => Some(IpAddr::V4(*v4.addr())),
                ScopedIp::V6(_) => None,
                _ => None,
            })
            .or_else(|| {
                addresses.iter().find_map(|addr| match addr {
                    ScopedIp::V6(v6) => Some(IpAddr::V6(*v6.addr())),
                    ScopedIp::V4(_) => None,
                    _ => None,
                })
            })
    }

    /// Sort key for applying ICE candidates: lower values are applied first.
    /// mDNS hostnames are deprioritized because resolution can wait up to 750ms.
    pub(crate) fn ice_candidate_application_priority(candidate: &str) -> u8 {
        if Self::is_mdns_hostname_candidate(candidate.trim_start_matches("candidate:")) {
            1
        } else {
            0
        }
    }

    fn is_mdns_hostname_candidate(candidate_sdp: &str) -> bool {
        let parts: Vec<&str> = candidate_sdp.split_whitespace().collect();
        if parts.len() < 6 {
            return false;
        }

        let host = parts[4];
        host.ends_with(".local")
    }

    fn candidate_preview(candidate_sdp: &str) -> String {
        const MAX: usize = 180;
        let count = candidate_sdp.chars().count();
        if count <= MAX {
            return candidate_sdp.to_string();
        }

        let truncated: String = candidate_sdp.chars().take(MAX).collect();
        format!("{truncated}...<truncated,len={count}>")
    }

    /// Get the peer's Olm identity key for encrypting messages.
    async fn get_peer_olm_key(&self) -> Result<String, ChannelError> {
        self.peer_olm_key.lock().await.clone().ok_or_else(|| {
            ChannelError::EncryptionError("No peer Olm key (SDP offer not yet handled)".into())
        })
    }
}

struct MdnsResolveCleanup {
    daemon: ServiceDaemon,
    hostname: String,
}

impl MdnsResolveCleanup {
    fn new(daemon: ServiceDaemon, hostname: String) -> Self {
        Self { daemon, hostname }
    }
}

impl Drop for MdnsResolveCleanup {
    fn drop(&mut self) {
        let _ = self.daemon.stop_resolve_hostname(&self.hostname);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[tokio::test]
    async fn rewrite_hostname_candidate_to_ip_skips_numeric_addresses() {
        let candidate = "1 1 udp 2122260223 127.0.0.1 54872 typ host";
        assert_eq!(
            WebRtcChannel::rewrite_hostname_candidate_to_ip(candidate).await,
            None
        );
    }

    #[tokio::test]
    async fn rewrite_hostname_candidate_to_ip_resolves_localhost() {
        let candidate = "1 1 udp 2122260223 localhost 54872 typ host";
        let rewritten = WebRtcChannel::rewrite_hostname_candidate_to_ip(candidate)
            .await
            .expect("expected localhost candidate to resolve");

        assert!(!rewritten.contains("localhost"));
        assert!(
            rewritten.contains("127.0.0.1") || rewritten.contains("::1"),
            "expected localhost to resolve to loopback, got {rewritten}"
        );
    }

    #[test]
    fn mdns_hostname_candidates_are_detected() {
        let candidate =
            "1 1 udp 2122260223 a4b9c343-f925-4558-b2ce-76521f4bc787.local 54872 typ host";
        assert!(WebRtcChannel::is_mdns_hostname_candidate(candidate));
    }

    #[test]
    fn canonical_mdns_hostname_appends_trailing_dot() {
        assert_eq!(
            WebRtcChannel::canonical_mdns_hostname("abcd.local"),
            "abcd.local."
        );
        assert_eq!(
            WebRtcChannel::canonical_mdns_hostname("abcd.local."),
            "abcd.local."
        );
    }

    #[test]
    fn select_mdns_ip_prefers_ipv4() {
        let mut addresses = HashSet::new();
        addresses.insert(ScopedIp::from(IpAddr::V6(Ipv6Addr::LOCALHOST)));
        addresses.insert(ScopedIp::from(IpAddr::V4(Ipv4Addr::LOCALHOST)));

        assert_eq!(
            WebRtcChannel::select_mdns_ip(&addresses),
            Some(IpAddr::V4(Ipv4Addr::LOCALHOST))
        );
    }

    #[tokio::test]
    async fn plaintext_control_frame_routes_without_crypto() {
        let payload = br#"{"type":"subscribe"}"#;
        let mut frame = vec![CONTENT_MSG];
        frame.extend_from_slice(payload);
        let (recv_tx, mut recv_rx) = mpsc::channel(1);
        let recv_tx = Arc::new(Mutex::new(Some(recv_tx)));
        let peers = Arc::new(RwLock::new(HashSet::new()));
        let decrypt_failures = Arc::new(AtomicU32::new(0));
        let mut chunk_assemblies = HashMap::new();

        handle_dc_message(
            &frame,
            "anon:tab-1",
            &None,
            &decrypt_failures,
            &recv_tx,
            &peers,
            &None,
            &None,
            &None,
            &None,
            &mut chunk_assemblies,
        )
        .await;

        let incoming = recv_rx.recv().await.expect("plaintext control frame");
        assert_eq!(incoming.payload, payload);
        assert_eq!(incoming.sender.as_ref(), "anon:tab-1");
        assert!(peers.read().await.contains(&PeerId("anon:tab-1".into())));
    }
}

#[async_trait]
impl Channel for WebRtcChannel {
    async fn connect(&mut self, config: ChannelConfig) -> Result<(), ChannelError> {
        self.state.set(ConnectionState::Connecting).await;

        // Store config
        *self.config.lock().await = Some(config.clone());

        // Create receive channel
        let (recv_tx, recv_rx) = mpsc::channel::<RawIncoming>(256);
        *self.recv_tx.lock().await = Some(recv_tx);
        *self.recv_rx.lock().await = Some(recv_rx);

        // For CLI, we wait for the browser to initiate the offer
        log::info!("[WebRTC] Channel configured, waiting for browser offer");

        Ok(())
    }

    async fn disconnect(&mut self) {
        // Abort event loop task (stops recv() loop)
        if let Some(handle) = self.event_loop_handle.lock().await.take() {
            handle.abort();
        }

        // Clear data channel
        self.data_channel.lock().await.take();
        self.data_channel_id.lock().await.take();

        // Close and drop peer connection (sync, immediate — no 60-second wait)
        if let Some(pc) = self.peer_connection.lock().await.take() {
            pc.close();
        }

        self.state.set(ConnectionState::Disconnected).await;
        self.peers.write().await.clear();
        let _ = self.close_complete_tx.send(true);
    }

    fn state(&self) -> ConnectionState {
        // Use blocking read for sync interface
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async { self.state.get().await })
        })
    }

    async fn send(&self, msg: &[u8]) -> Result<(), ChannelError> {
        let peers: Vec<PeerId> = self.peers.read().await.iter().cloned().collect();

        for peer in peers {
            self.send_to(msg, &peer).await?;
        }

        Ok(())
    }

    async fn send_to(&self, msg: &[u8], _peer: &PeerId) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        // Binary inner: [0x00][JSON bytes] (control message)
        let mut plaintext = Vec::with_capacity(1 + msg.len());
        plaintext.push(CONTENT_MSG);
        plaintext.extend_from_slice(msg);
        let wire = self.encode_data_channel_frame(plaintext).await?;

        pc.send_data(dc_id, &wire)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn recv(&mut self) -> Result<IncomingMessage, ChannelError> {
        let mut recv_guard = self.recv_rx.lock().await;
        let recv_rx = recv_guard.as_mut().ok_or(ChannelError::Closed)?;

        match recv_rx.recv().await {
            Some(raw) => Ok(IncomingMessage {
                payload: raw.payload,
                sender: raw.sender,
            }),
            None => Err(ChannelError::Closed),
        }
    }

    fn peers(&self) -> Vec<PeerId> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.peers.read().await.iter().cloned().collect() })
        })
    }

    fn has_peer(&self, peer: &PeerId) -> bool {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.peers.read().await.contains(peer) })
        })
    }
}

impl WebRtcChannel {
    /// Try to receive a message without blocking.
    ///
    /// Returns `Some(message)` if a message is available, `None` otherwise.
    /// Must be called from within a tokio runtime context (use `runtime.enter()`).
    pub fn try_recv(&self, runtime: &tokio::runtime::Runtime) -> Option<IncomingMessage> {
        runtime.block_on(async {
            let mut recv_guard = self.recv_rx.lock().await;
            let recv_rx = recv_guard.as_mut()?;

            match recv_rx.try_recv() {
                Ok(raw) => Some(IncomingMessage {
                    payload: raw.payload,
                    sender: raw.sender,
                }),
                Err(_) => None,
            }
        })
    }

    /// Get a clone of the `recv_rx` Arc for spawning a forwarding task.
    ///
    /// The forwarding task takes the `Option<Receiver>` from inside the Arc
    /// and reads from it, sending each message as a `HubEvent::WebRtcMessage`.
    /// After the receiver is taken, [`try_recv`] will return `None`.
    pub(crate) fn recv_rx_arc(
        &self,
    ) -> Arc<tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<RawIncoming>>>> {
        Arc::clone(&self.recv_rx)
    }

    /// Get consecutive decryption failure count.
    pub fn decrypt_failure_count(&self) -> u32 {
        self.decrypt_failures.load(Ordering::Relaxed)
    }

    /// Reset decryption failure counter.
    pub fn reset_decrypt_failures(&self) {
        self.decrypt_failures.store(0, Ordering::Relaxed);
    }

    /// Stamp this channel with the hub offer generation that owns it.
    ///
    /// DataChannel-open events carry this value back to the hub so stale opens
    /// from replaced channels cannot be attributed to the current offer.
    pub(crate) fn set_offer_generation(&self, generation: u64) {
        self.offer_generation.store(generation, Ordering::Relaxed);
    }

    /// Returns `true` exactly once after the DataChannel opens.
    ///
    /// Polled by the tick loop to fire `on_peer_connected` at the right time
    /// (when the DC is actually usable, not just when ICE connects).
    pub fn take_dc_opened(&self) -> bool {
        self.dc_opened.swap(false, Ordering::Relaxed)
    }

    /// Returns a watch receiver for close-complete signaling.
    ///
    /// The value transitions to `true` when the connection closes and
    /// sockets are released. Callers can `wait_for(|v| *v)`
    /// (with a timeout) before creating a replacement connection to prevent
    /// fd exhaustion. Unlike `Notify`, `watch` retains the last value so
    /// late subscribers see it immediately.
    pub fn close_receiver(&self) -> tokio::sync::watch::Receiver<bool> {
        self.close_complete_rx.clone()
    }

    /// Check if the channel is ready for application messages.
    ///
    /// With vodozemac, the session is established on first PreKey decrypt --
    /// no separate handshake needed.
    pub fn is_ready(&self) -> bool {
        if self.crypto_service.is_none() {
            return true;
        }
        self.crypto_service
            .as_ref()
            .and_then(|cs| cs.lock().ok())
            .is_some_and(|guard| guard.has_session())
    }

    /// Send PTY output through the hot path with optional Olm encryption.
    ///
    /// Zero base64, zero JSON. Binary inner format:
    /// `[0x01][flags:1][sub_id_len:1][sub_id][raw payload]`
    pub async fn send_pty_raw(
        &self,
        subscription_id: &str,
        data: &[u8],
        _peer: &PeerId,
    ) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        // Compress raw bytes (gzip is very effective on terminal output)
        let config_guard = self.config.lock().await;
        let threshold = config_guard.as_ref().and_then(|c| c.compression_threshold);
        drop(config_guard);

        let (payload, was_compressed) = pty_payload_with_compression(data, threshold)?;

        // Build binary inner content: [CONTENT_PTY][flags][sub_id_len][sub_id][payload]
        let sub_bytes = subscription_id.as_bytes();
        let flags: u8 = if was_compressed { 0x01 } else { 0x00 };
        let mut plaintext = Vec::with_capacity(3 + sub_bytes.len() + payload.len());
        plaintext.push(CONTENT_PTY);
        plaintext.push(flags);
        plaintext.push(sub_bytes.len() as u8);
        plaintext.extend_from_slice(sub_bytes);
        plaintext.extend_from_slice(&payload);

        let wire = self.encode_data_channel_frame(plaintext).await?;

        pc.send_data(dc_id, &wire)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Send a stream multiplexer frame through the DataChannel.
    ///
    /// Binary format: `[CONTENT_STREAM][frame_type][stream_id_hi][stream_id_lo][payload]`
    pub async fn send_stream_raw(
        &self,
        frame_type: u8,
        stream_id: u16,
        payload: &[u8],
        _peer: &PeerId,
    ) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        let stream_id_bytes = stream_id.to_be_bytes();
        let mut plaintext = Vec::with_capacity(4 + payload.len());
        plaintext.push(CONTENT_STREAM);
        plaintext.push(frame_type);
        plaintext.extend_from_slice(&stream_id_bytes);
        plaintext.extend_from_slice(payload);

        let wire = self.encode_data_channel_frame(plaintext).await?;

        pc.send_data(dc_id, &wire)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Send a bundle refresh (type 2) via DataChannel.
    ///
    /// Wire format: `[0x02][161-byte DeviceKeyBundle]` (unencrypted).
    /// The browser verifies the identity key matches the original QR trust anchor,
    /// then creates a new outbound Olm session from the fresh one-time key.
    pub async fn send_bundle_refresh(&self, bundle_bytes: &[u8]) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        let mut frame = Vec::with_capacity(1 + bundle_bytes.len());
        frame.push(crate::relay::MSG_TYPE_BUNDLE_REFRESH);
        frame.extend_from_slice(bundle_bytes);

        pc.send_data(dc_id, &frame)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    async fn encode_data_channel_frame(&self, plaintext: Vec<u8>) -> Result<Vec<u8>, ChannelError> {
        let Some(ref crypto_service) = self.crypto_service else {
            return Ok(plaintext);
        };
        let peer_key = self.get_peer_olm_key().await?;
        crypto_service
            .lock()
            .map_err(|e| ChannelError::EncryptionError(format!("Crypto mutex poisoned: {e}")))?
            .encrypt_binary(&plaintext, &peer_key)
            .map_err(|e| ChannelError::EncryptionError(e.to_string()))
    }
}

/// Lightweight, cloneable send handle for a [`WebRtcChannel`].
///
/// Holds only the `Arc`-wrapped fields needed by the async send methods
/// (`send_pty_raw`, `send_to`, `send_stream_raw`, `send_bundle_refresh`).
/// Designed to be moved into a per-peer `tokio::spawn` task so that
/// DataChannel sends run off the Hub event loop.
#[derive(Clone)]
pub struct WebRtcSender {
    /// Peer connection (owns the SCTP transport).
    peer_connection: Arc<Mutex<Option<PeerConnection>>>,
    /// SCTP stream ID for `pc.send_data(channel_id, ...)`.
    data_channel_id: Arc<Mutex<Option<u16>>>,
    /// Optional Olm crypto for E2E encryption.
    crypto_service: Option<CryptoService>,
    /// Channel configuration (compression threshold, etc.).
    config: Arc<Mutex<Option<ChannelConfig>>>,
    /// Peer's Olm identity key for encryption.
    peer_olm_key: Arc<Mutex<Option<String>>>,
}

impl std::fmt::Debug for WebRtcSender {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebRtcSender")
            .field("crypto_service", &self.crypto_service.is_some())
            .finish_non_exhaustive()
    }
}

impl WebRtcSender {
    /// Compress and send PTY output with optional encryption.
    ///
    /// Same logic as [`WebRtcChannel::send_pty_raw`] but operates on the
    /// extracted Arc fields so it can run in a spawned task.
    pub async fn send_pty_raw(
        &self,
        subscription_id: &str,
        data: &[u8],
    ) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        let config_guard = self.config.lock().await;
        let threshold = config_guard.as_ref().and_then(|c| c.compression_threshold);
        drop(config_guard);

        let (payload, was_compressed) = pty_payload_with_compression(data, threshold)?;

        let sub_bytes = subscription_id.as_bytes();
        let flags: u8 = if was_compressed { 0x01 } else { 0x00 };
        let mut plaintext = Vec::with_capacity(3 + sub_bytes.len() + payload.len());
        plaintext.push(CONTENT_PTY);
        plaintext.push(flags);
        plaintext.push(sub_bytes.len() as u8);
        plaintext.extend_from_slice(sub_bytes);
        plaintext.extend_from_slice(&payload);

        let wire = self.encode_data_channel_frame(plaintext).await?;

        pc.send_data(dc_id, &wire)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Serialize and send a JSON message with optional encryption.
    ///
    /// Same logic as [`WebRtcChannel::send_to`] but for the extracted handle.
    pub async fn send_json(&self, payload: &[u8]) -> Result<(), ChannelError> {
        self.send_message_raw(payload).await
    }

    /// Send a raw `CONTENT_MSG` payload through the DataChannel.
    pub async fn send_message_raw(&self, payload: &[u8]) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        // Wrap in CONTENT_MSG frame: [0x00][json bytes]
        let mut plaintext = Vec::with_capacity(1 + payload.len());
        plaintext.push(CONTENT_MSG);
        plaintext.extend_from_slice(payload);

        let wire = self.encode_data_channel_frame(plaintext).await?;

        pc.send_data(dc_id, &wire)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Send a stream multiplexer frame through the DataChannel.
    pub async fn send_stream_raw(
        &self,
        frame_type: u8,
        stream_id: u16,
        payload: &[u8],
    ) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        let stream_id_bytes = stream_id.to_be_bytes();
        let mut plaintext = Vec::with_capacity(4 + payload.len());
        plaintext.push(CONTENT_STREAM);
        plaintext.push(frame_type);
        plaintext.extend_from_slice(&stream_id_bytes);
        plaintext.extend_from_slice(payload);

        let wire = self.encode_data_channel_frame(plaintext).await?;

        pc.send_data(dc_id, &wire)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Send a bundle refresh (type 2) via DataChannel (unencrypted).
    pub async fn send_bundle_refresh(&self, bundle_bytes: &[u8]) -> Result<(), ChannelError> {
        let pc_guard = self.peer_connection.lock().await;
        let pc = pc_guard
            .as_ref()
            .ok_or_else(|| ChannelError::SendFailed("No peer connection".to_string()))?;

        let dc_id = self
            .data_channel_id
            .lock()
            .await
            .ok_or_else(|| ChannelError::SendFailed("No data channel".to_string()))?;

        let mut frame = Vec::with_capacity(1 + bundle_bytes.len());
        frame.push(crate::relay::MSG_TYPE_BUNDLE_REFRESH);
        frame.extend_from_slice(bundle_bytes);

        pc.send_data(dc_id, &frame)
            .await
            .map_err(|e| ChannelError::SendFailed(e.to_string()))?;

        Ok(())
    }

    /// Get the peer's Olm identity key.
    async fn get_peer_olm_key(&self) -> Result<String, ChannelError> {
        self.peer_olm_key
            .lock()
            .await
            .clone()
            .ok_or_else(|| ChannelError::EncryptionError("No peer Olm key set".into()))
    }

    async fn encode_data_channel_frame(&self, plaintext: Vec<u8>) -> Result<Vec<u8>, ChannelError> {
        let Some(ref crypto_service) = self.crypto_service else {
            return Ok(plaintext);
        };
        let peer_key = self.get_peer_olm_key().await?;
        crypto_service
            .lock()
            .map_err(|e| ChannelError::EncryptionError(format!("Crypto mutex poisoned: {e}")))?
            .encrypt_binary(&plaintext, &peer_key)
            .map_err(|e| ChannelError::EncryptionError(e.to_string()))
    }
}

impl WebRtcChannel {
    /// Create a [`WebRtcSender`] handle for off-event-loop async sends.
    ///
    /// The sender holds `Arc` clones of the fields needed for encryption
    /// and DataChannel transmission. Safe to move into a `tokio::spawn` task.
    pub fn sender(&self) -> WebRtcSender {
        WebRtcSender {
            peer_connection: Arc::clone(&self.peer_connection),
            data_channel_id: Arc::clone(&self.data_channel_id),
            crypto_service: self.crypto_service.clone(),
            config: Arc::clone(&self.config),
            peer_olm_key: Arc::clone(&self.peer_olm_key),
        }
    }
}

/// Handle an incoming DataChannel message: decrypt and route by content type.
///
/// Extracted as a standalone async fn to avoid deep nesting in the event loop.
#[allow(clippy::too_many_arguments)]
async fn handle_dc_message(
    data: &[u8],
    browser_identity: &str,
    crypto_service: &Option<CryptoService>,
    decrypt_failures: &Arc<AtomicU32>,
    recv_tx: &Arc<Mutex<Option<mpsc::Sender<RawIncoming>>>>,
    peers: &Arc<RwLock<HashSet<PeerId>>>,
    stream_frame_tx: &Option<mpsc::Sender<StreamIncoming>>,
    pty_input_tx: &Option<mpsc::Sender<PtyInputIncoming>>,
    file_input_tx: &Option<mpsc::Sender<FileInputIncoming>>,
    hub_event_tx: &Option<crate::hub::events::HubEventTx>,
    chunk_assemblies: &mut std::collections::HashMap<u8, FileChunkAssembly>,
) {
    let plaintext = if let Some(ref cs) = crypto_service {
        let peer_olm_key = crate::relay::extract_olm_key(browser_identity);
        match cs.lock() {
            Ok(mut guard) => match guard.decrypt_binary(data, Some(peer_olm_key)) {
                Ok(pt) => {
                    decrypt_failures.store(0, Ordering::Relaxed);
                    pt
                }
                Err(e) => {
                    decrypt_failures.fetch_add(1, Ordering::Relaxed);
                    log::error!("[WebRTC-DC] Olm decryption FAILED: {e}");
                    return;
                }
            },
            Err(e) => {
                log::error!("[WebRTC-DC] Crypto mutex poisoned: {e}");
                return;
            }
        }
    } else {
        data.to_vec()
    };

    // Parse binary inner content: first byte = content type
    if plaintext.is_empty() {
        log::warn!("[WebRTC-DC] Empty decrypted content");
        return;
    }

    let body_bytes = match plaintext[0] {
        CONTENT_MSG => {
            // Control message: [CONTENT_MSG][JSON bytes]
            plaintext[1..].to_vec()
        }
        CONTENT_PTY => {
            // PTY: [CONTENT_PTY][flags][sub_id_len][sub_id][payload]
            if plaintext.len() < 4 {
                log::warn!("[WebRTC-DC] CONTENT_PTY frame too short");
                return;
            }
            let flags = plaintext[1];
            let is_input = flags & 0x02 != 0;

            if !is_input {
                log::warn!("[WebRTC-DC] Unexpected PTY output from browser");
                return;
            }

            let sub_id_len = plaintext[2] as usize;
            if plaintext.len() < 3 + sub_id_len {
                log::warn!("[WebRTC-DC] CONTENT_PTY sub_id truncated");
                return;
            }
            let sub_id = std::str::from_utf8(&plaintext[3..3 + sub_id_len]).unwrap_or("");
            let payload = plaintext[3 + sub_id_len..].to_vec();

            // Parse "terminal_{session_uuid}" and send directly to PTY
            if let Some(incoming) =
                parse_pty_input_sub_id(sub_id, payload, browser_identity.to_string())
            {
                if let Some(ref tx) = pty_input_tx {
                    match tx.try_send(incoming) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            notify_ingress_backpressure(
                                hub_event_tx,
                                browser_identity,
                                "pty_input_queue_full",
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            log::debug!("[WebRTC-DC] PTY input queue closed");
                        }
                    }
                }
            } else {
                log::warn!("[WebRTC-DC] Failed to parse PTY input sub_id: {sub_id}");
            }
            return;
        }
        CONTENT_STREAM => {
            // Stream mux: [CONTENT_STREAM][frame_type][stream_id_hi][stream_id_lo][payload]
            if plaintext.len() < 4 {
                log::warn!("[WebRTC-DC] CONTENT_STREAM frame too short");
                return;
            }
            let frame_type = plaintext[1];
            let stream_id = u16::from_be_bytes([plaintext[2], plaintext[3]]);
            let payload = plaintext[4..].to_vec();

            if let Some(ref tx) = stream_frame_tx {
                match tx.try_send(StreamIncoming {
                    browser_identity: browser_identity.to_string(),
                    frame_type,
                    stream_id,
                    payload,
                }) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        notify_ingress_backpressure(
                            hub_event_tx,
                            browser_identity,
                            "stream_frame_queue_full",
                        );
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        log::debug!("[WebRTC-DC] Stream frame queue closed");
                    }
                }
            }
            return;
        }
        CONTENT_FILE => {
            // File transfer: [CONTENT_FILE][sub_id_len][sub_id][filename_len_lo][filename_len_hi][filename][data]
            if plaintext.len() < 4 {
                log::warn!("[WebRTC-DC] CONTENT_FILE frame too short");
                return;
            }
            let sub_id_len = plaintext[1] as usize;
            if plaintext.len() < 2 + sub_id_len + 2 {
                log::warn!("[WebRTC-DC] CONTENT_FILE sub_id/filename truncated");
                return;
            }
            let sub_id = std::str::from_utf8(&plaintext[2..2 + sub_id_len]).unwrap_or("");
            let fname_offset = 2 + sub_id_len;
            let filename_len =
                u16::from_le_bytes([plaintext[fname_offset], plaintext[fname_offset + 1]]) as usize;
            let fname_start = fname_offset + 2;
            if plaintext.len() < fname_start + filename_len {
                log::warn!("[WebRTC-DC] CONTENT_FILE filename truncated");
                return;
            }
            let filename = std::str::from_utf8(&plaintext[fname_start..fname_start + filename_len])
                .unwrap_or("paste.png")
                .to_string();
            let data = plaintext[fname_start + filename_len..].to_vec();

            // Parse sub_id for agent/pty routing
            if let Some(pty_info) =
                parse_pty_input_sub_id(sub_id, Vec::new(), browser_identity.to_string())
            {
                if let Some(ref tx) = file_input_tx {
                    match tx.try_send(FileInputIncoming {
                        session_uuid: pty_info.session_uuid,
                        browser_identity: browser_identity.to_string(),
                        filename,
                        data,
                    }) {
                        Ok(()) => {}
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            notify_ingress_backpressure(
                                hub_event_tx,
                                browser_identity,
                                "file_input_queue_full",
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            log::debug!("[WebRTC-DC] File input queue closed");
                        }
                    }
                }
            } else {
                log::warn!("[WebRTC-DC] Failed to parse file input sub_id: {sub_id}");
            }
            return;
        }
        CONTENT_FILE_CHUNK => {
            // Chunked file transfer: [0x04][transfer_id][flags][payload...]
            // flags: bit 0 = START (first chunk), bit 1 = END (last chunk)
            if plaintext.len() < 4 {
                log::warn!("[WebRTC-DC] CONTENT_FILE_CHUNK frame too short");
                return;
            }
            let transfer_id = plaintext[1];
            let flags = plaintext[2];
            let is_start = (flags & 0x01) != 0;
            let is_end = (flags & 0x02) != 0;
            let payload = &plaintext[3..];

            if is_start {
                // First chunk: payload = [sub_id_len][sub_id][fname_len:2LE][fname][data...]
                // Same layout as CONTENT_FILE minus the 0x03 byte
                if payload.len() < 3 {
                    log::warn!("[WebRTC-DC] CONTENT_FILE_CHUNK START too short");
                    return;
                }
                let sub_id_len = payload[0] as usize;
                if payload.len() < 1 + sub_id_len + 2 {
                    log::warn!("[WebRTC-DC] CONTENT_FILE_CHUNK START sub_id truncated");
                    return;
                }
                let sub_id = std::str::from_utf8(&payload[1..1 + sub_id_len]).unwrap_or("");
                let fname_offset = 1 + sub_id_len;
                let filename_len =
                    u16::from_le_bytes([payload[fname_offset], payload[fname_offset + 1]]) as usize;
                let fname_start = fname_offset + 2;
                if payload.len() < fname_start + filename_len {
                    log::warn!("[WebRTC-DC] CONTENT_FILE_CHUNK START filename truncated");
                    return;
                }
                let filename =
                    std::str::from_utf8(&payload[fname_start..fname_start + filename_len])
                        .unwrap_or("paste.png")
                        .to_string();
                let file_data = &payload[fname_start + filename_len..];

                // Parse routing info
                let session_uuid = if let Some(pty_info) =
                    parse_pty_input_sub_id(sub_id, Vec::new(), browser_identity.to_string())
                {
                    pty_info.session_uuid
                } else {
                    log::warn!("[WebRTC-DC] CONTENT_FILE_CHUNK: bad sub_id: {sub_id}");
                    return;
                };

                log::debug!(
                    "[WebRTC-DC] File chunk START: transfer_id={transfer_id}, filename={filename}, first_data={}",
                    file_data.len()
                );

                let mut assembly = FileChunkAssembly {
                    session_uuid,
                    browser_identity: browser_identity.to_string(),
                    filename,
                    data: Vec::with_capacity(512 * 1024), // pre-allocate for typical file
                };
                assembly.data.extend_from_slice(file_data);

                if is_end {
                    // Single chunk with both START and END (small file sent as chunk)
                    if let Some(ref tx) = file_input_tx {
                        match tx.try_send(FileInputIncoming {
                            session_uuid: assembly.session_uuid,
                            browser_identity: assembly.browser_identity,
                            filename: assembly.filename,
                            data: assembly.data,
                        }) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                notify_ingress_backpressure(
                                    hub_event_tx,
                                    browser_identity,
                                    "file_input_queue_full",
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                log::debug!("[WebRTC-DC] File input queue closed");
                            }
                        }
                    }
                } else {
                    if let Some(replaced) = chunk_assemblies.remove(&transfer_id) {
                        log::warn!(
                            "[WebRTC-DC] Replaced in-progress file transfer on transfer_id={} (dropped {} bytes)",
                            transfer_id,
                            replaced.data.len()
                        );
                    }
                    chunk_assemblies.insert(transfer_id, assembly);
                }
            } else {
                // Middle or last chunk: payload = raw file data
                let assembly = match chunk_assemblies.get_mut(&transfer_id) {
                    Some(a) => a,
                    None => {
                        log::warn!(
                            "[WebRTC-DC] CONTENT_FILE_CHUNK: no assembly for transfer_id={transfer_id}"
                        );
                        return;
                    }
                };
                assembly.data.extend_from_slice(payload);

                if is_end {
                    let assembly = chunk_assemblies.remove(&transfer_id).unwrap();
                    log::debug!(
                        "[WebRTC-DC] File chunk END: transfer_id={transfer_id}, total={}",
                        assembly.data.len()
                    );
                    if let Some(ref tx) = file_input_tx {
                        match tx.try_send(FileInputIncoming {
                            session_uuid: assembly.session_uuid,
                            browser_identity: assembly.browser_identity,
                            filename: assembly.filename,
                            data: assembly.data,
                        }) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                notify_ingress_backpressure(
                                    hub_event_tx,
                                    browser_identity,
                                    "file_input_queue_full",
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                log::debug!("[WebRTC-DC] File input queue closed");
                            }
                        }
                    }
                }
            }
            return;
        }
        other => {
            log::warn!("[WebRTC-DC] Unknown content type: 0x{other:02x}");
            return;
        }
    };

    // Add peer
    {
        let mut peers = peers.write().await;
        peers.insert(PeerId(browser_identity.to_string()));
    }

    // Send to receive queue
    if let Some(tx) = recv_tx.lock().await.as_ref() {
        let _ = tx
            .send(RawIncoming {
                payload: body_bytes,
                sender: PeerId(browser_identity.to_string()),
            })
            .await;
    } else {
        log::error!("[WebRTC-DC] recv_tx is None! Cannot queue message");
    }
}

/// Parse a terminal subscription ID ("terminal_{session_uuid}") into a [`PtyInputIncoming`].
fn parse_pty_input_sub_id(
    sub_id: &str,
    data: Vec<u8>,
    browser_identity: String,
) -> Option<PtyInputIncoming> {
    let session_uuid = sub_id.strip_prefix("terminal_")?;
    if session_uuid.is_empty() {
        return None;
    }
    Some(PtyInputIncoming {
        session_uuid: session_uuid.to_string(),
        data,
        browser_identity,
    })
}

fn notify_ingress_backpressure(
    hub_event_tx: &Option<crate::hub::events::HubEventTx>,
    browser_identity: &str,
    source: &'static str,
) {
    log::warn!(
        "[WebRTC-DC] Ingress queue backpressure from {} for {}",
        source,
        &browser_identity[..browser_identity.len().min(8)]
    );
    if let Some(tx) = hub_event_tx {
        let _ = tx.send(crate::hub::events::HubEvent::WebRtcIngressBackpressure {
            browser_identity: browser_identity.to_string(),
            source,
        });
    }
}

/// Inject `a=max-message-size:{value}` into the application media section of an SDP string.
///
/// Browsers default to 65536 when this attribute is absent (RFC 8841 §6.1),
/// which causes `RTCErrorEvent` when sending encrypted payloads larger than 64 KB
/// (e.g. screenshot file transfers). A value of 0 means no limit.
fn inject_max_message_size(sdp: &str, value: u64) -> String {
    // Insert after the m=application line (before the next m= or at end)
    let mut result = String::with_capacity(sdp.len() + 30);
    let mut injected = false;

    for line in sdp.lines() {
        result.push_str(line);
        result.push_str("\r\n");

        if !injected && line.starts_with("m=application") {
            result.push_str(&format!("a=max-message-size:{value}\r\n"));
            injected = true;
        }
    }

    result
}
