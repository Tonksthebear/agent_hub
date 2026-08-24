//! Shared Streamable HTTP MCP server owned by the Hub.
//!
//! One localhost listener serves every agent session. Each caller presents a
//! bearer token. The server authorizes that token, then dispatches MCP methods
//! with that caller's identity. Callers do not share session UUID, Hub
//! identity, plugin scope, messages, or tools.
//!
//! Clients receive `BOTSTER_MCP_URL` and `BOTSTER_MCP_TOKEN` in the session
//! environment. Streamable HTTP is the MCP HTTP transport: one `/mcp` endpoint
//! accepts JSON-RPC POST and optional SSE GET.
//!
//! # Examples
//!
//! ```ignore
//! let callers = McpCallerRegistry::new();
//! let issued = callers.issue(McpCaller::new("sess-1", "hub-1"));
//! assert_eq!(issued.url_env, BOTSTER_MCP_URL_ENV);
//! ```

// Rust guideline compliant 2026-08-23

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::header::{HeaderMap, AUTHORIZATION, CONTENT_TYPE, ORIGIN};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use mlua::LuaSerdeExt;
use rand::RngCore;
use rmcp::model::*;
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::{
    session::local::LocalSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tower_service::Service;

use crate::hub::events::{HubEvent, HubEventTx};

/// Environment variable that carries the shared MCP HTTP URL.
pub const BOTSTER_MCP_URL_ENV: &str = "BOTSTER_MCP_URL";

/// Environment variable that carries the caller bearer token.
pub const BOTSTER_MCP_TOKEN_ENV: &str = "BOTSTER_MCP_TOKEN";

/// MCP HTTP path on the Hub-owned listener.
pub const MCP_HTTP_PATH: &str = "/mcp";

/// Prefix that distinguishes caller tokens from session UUIDs.
///
/// Changing this prefix invalidates live caller credentials. Hub restart
/// already revokes in-memory tokens; keep the prefix stable across a process.
const CALLER_TOKEN_PREFIX: &str = "btcaller_";

/// Random bytes in each caller token. 32 bytes is 256 bits of secret.
const CALLER_TOKEN_BYTES: usize = 32;

/// Largest accepted JSON-RPC body. Tool arguments stay well under this.
const BODY_LIMIT: usize = 1024 * 1024;

/// How long the HTTP layer waits for the Hub event loop to answer.
///
/// Tool calls can run for a long time. Match the stdio gateway timeout so a
/// shared HTTP caller is not cut off sooner than a former per-session process.
const HUB_DISPATCH_TIMEOUT: Duration = Duration::from_secs(86_400);

/// How long initialize and ping wait for the Hub event loop.
const HUB_QUICK_TIMEOUT: Duration = Duration::from_secs(15);

/// How long list methods wait. `tools/list` can scan workspace manifests.
///
/// 15 seconds was too short on a busy Hub event loop. 120 seconds covers a
/// disk walk without treating discovery as a long-running tool call.
const HUB_LIST_TIMEOUT: Duration = Duration::from_secs(120);

/// Failed-auth attempts allowed in [`AUTH_FAIL_WINDOW`] before HTTP 429.
///
/// This cap applies only to requests that fail authorization. A valid token
/// is never delayed or rejected by the gate.
const AUTH_FAIL_LIMIT: u32 = 20;

/// Sliding window for failed-auth counting.
const AUTH_FAIL_WINDOW: Duration = Duration::from_secs(60);

/// Socket `mcp-serve` fallback removal target.
///
/// Streamable HTTP is the durable transport. The stdio proxy and Unix-socket
/// gateway remain only until this date.
pub const SOCKET_MCP_FALLBACK_REMOVAL: &str = "2026-10-01";

/// Drop persisted credentials older than this.
///
/// A recovered live PTY reconnects well before two weeks. Tokens for
/// sessions whose processes died while the Hub was down must not live forever.
const MAX_CREDENTIAL_AGE: Duration = Duration::from_secs(14 * 24 * 60 * 60);

/// HTTP body type that can be a finished JSON reply or a live SSE stream.
type McpHttpBody = UnsyncBoxBody<Bytes, Infallible>;

/// Bearer token that identifies one MCP caller.
///
/// `Debug` redacts the secret. Tests assert that formatting never contains the
/// raw token.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CallerToken(String);

impl CallerToken {
    /// Wrap an already-issued token string.
    #[must_use]
    pub fn new(raw: impl Into<String>) -> Self {
        Self(raw.into())
    }

    /// Return the secret for Authorization headers and session env.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for CallerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallerToken(...)")
    }
}

impl std::fmt::Display for CallerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallerToken(...)")
    }
}

/// Caller identity bound to one issued credential.
///
/// This is the only identity the HTTP layer may attach to a dispatch. It must
/// not be replaced with another caller's fields after authorization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpCaller {
    /// Session UUID of the authorized caller.
    pub session_uuid: String,
    /// Hub identity of the authorized caller.
    pub hub_id: String,
    /// Unix seconds when this credential was first issued.
    pub issued_at: u64,
    /// Extra caller context from the session, never another caller.
    pub context: HashMap<String, String>,
}

impl McpCaller {
    /// Build a caller identity from session and hub identifiers.
    #[must_use]
    pub fn new(session_uuid: impl Into<String>, hub_id: impl Into<String>) -> Self {
        let session_uuid = session_uuid.into();
        let hub_id = hub_id.into();
        let mut context = HashMap::new();
        context.insert("session_uuid".to_string(), session_uuid.clone());
        context.insert("hub_id".to_string(), hub_id.clone());
        Self {
            session_uuid,
            hub_id,
            issued_at: now_unix(),
            context,
        }
    }

    /// Copy this identity into a BTreeMap for Lua MCP context.
    #[must_use]
    pub fn lua_context(&self) -> std::collections::BTreeMap<String, String> {
        let mut ctx = std::collections::BTreeMap::new();
        for (key, value) in &self.context {
            ctx.insert(key.clone(), value.clone());
        }
        ctx.insert("session_uuid".to_string(), self.session_uuid.clone());
        ctx.insert("hub_id".to_string(), self.hub_id.clone());
        ctx
    }
}

/// Issued URL and bearer token for one caller.
#[derive(Clone, Debug)]
pub struct IssuedMcpCaller {
    /// Shared Streamable HTTP URL for this Hub.
    pub url: String,
    /// Caller-specific bearer token.
    pub token: CallerToken,
}

/// Reply sent back to a waiting HTTP request.
#[derive(Debug)]
pub struct McpHttpReply {
    /// JSON-RPC result object, or `None` when `error` is set.
    pub result: Option<Value>,
    /// JSON-RPC error object, or `None` on success.
    pub error: Option<Value>,
}

impl McpHttpReply {
    /// Successful JSON-RPC result.
    #[must_use]
    pub fn result(value: Value) -> Self {
        Self {
            result: Some(value),
            error: None,
        }
    }

    /// Failed JSON-RPC error with a message.
    #[must_use]
    pub fn error(code: i64, message: impl Into<String>) -> Self {
        Self {
            result: None,
            error: Some(json!({ "code": code, "message": message.into() })),
        }
    }
}

/// Oneshot used by the HTTP layer to receive a Hub MCP reply.
pub struct McpHttpReplyTx(pub oneshot::Sender<McpHttpReply>);

impl std::fmt::Debug for McpHttpReplyTx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("McpHttpReplyTx")
    }
}

/// Which MCP list changed. The notification carries no caller-private data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpListKind {
    /// Tool list changed.
    Tools,
    /// Prompt list changed.
    Prompts,
    /// Resource list changed.
    Resources,
}

impl McpListKind {
    /// Parse a Lua kind string.
    #[must_use]
    pub fn parse(kind: &str) -> Option<Self> {
        match kind {
            "tools" => Some(Self::Tools),
            "prompts" => Some(Self::Prompts),
            "resources" => Some(Self::Resources),
            _ => None,
        }
    }
}

/// In-memory caller credentials for the live Hub process.
///
/// Tokens are restored from the Hub data directory after a restart so a live
/// agent PTY can keep using `BOTSTER_MCP_TOKEN`. Session close removes the
/// matching token from memory and from disk. Hub shutdown clears memory only.
#[derive(Clone, Debug)]
pub struct McpCallerRegistry {
    inner: Arc<Mutex<CallerState>>,
}

#[derive(Default)]
struct CallerState {
    url: Option<String>,
    persist_path: Option<PathBuf>,
    listen_port: Option<u16>,
    by_token: HashMap<String, McpCaller>,
    by_session: HashMap<String, String>,
}

/// Disk snapshot so a Hub restart can reuse the listen port and tokens.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct McpCallerSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(default)]
    callers: Vec<PersistedCaller>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedCaller {
    session_uuid: String,
    hub_id: String,
    token: String,
    #[serde(default)]
    issued_at: u64,
}

impl std::fmt::Debug for CallerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallerState")
            .field("url", &self.url)
            .field("callers", &self.by_token.len())
            .finish()
    }
}

impl Default for McpCallerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl McpCallerRegistry {
    /// Create an empty caller registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(CallerState::default())),
        }
    }

    /// Persist caller tokens under this Hub-owned path.
    ///
    /// This does not write. Call [`Self::restore_from_disk`] next, then
    /// [`Self::set_url`], so a restore is not overwritten by an empty snapshot.
    pub fn set_persist_path(&self, path: impl Into<PathBuf>) {
        self.inner
            .lock()
            .expect("McpCallerRegistry mutex poisoned")
            .persist_path = Some(path.into());
    }

    /// Restore tokens and the last listen port from disk.
    pub fn restore_from_disk(&self) {
        let path = {
            let state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
            state.persist_path.clone()
        };
        let Some(path) = path else {
            return;
        };
        let Ok(bytes) = std::fs::read(&path) else {
            return;
        };
        let Ok(snapshot) = serde_json::from_slice::<McpCallerSnapshot>(&bytes) else {
            log::warn!("[mcp-http] ignoring unreadable caller snapshot");
            return;
        };
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        state.listen_port = snapshot
            .port
            .or_else(|| port_from_url(snapshot.url.as_deref()));
        state.by_token.clear();
        state.by_session.clear();
        for entry in snapshot.callers {
            if !entry.token.starts_with(CALLER_TOKEN_PREFIX) {
                continue;
            }
            if credential_is_expired(entry.issued_at) {
                continue;
            }
            let mut caller = McpCaller::new(entry.session_uuid.clone(), entry.hub_id);
            caller.issued_at = entry.issued_at;
            state
                .by_session
                .insert(entry.session_uuid, entry.token.clone());
            state.by_token.insert(entry.token, caller);
        }
    }

    /// Drop credentials for missing sessions and write the pruned snapshot.
    pub fn prune_stale_callers(&self) {
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        let stale: Vec<String> = state
            .by_session
            .iter()
            .filter_map(|(session_uuid, token)| {
                let issued_at = state.by_token.get(token).map(|caller| caller.issued_at)?;
                if credential_is_expired(issued_at) || session_manifest_is_gone(session_uuid) {
                    Some(session_uuid.clone())
                } else {
                    None
                }
            })
            .collect();
        for session_uuid in &stale {
            if let Some(token) = state.by_session.remove(session_uuid) {
                state.by_token.remove(&token);
            }
        }
        drop(state);
        if !stale.is_empty() {
            self.persist();
        }
    }

    /// Last persisted loopback port, if any.
    #[must_use]
    pub fn preferred_port(&self) -> Option<u16> {
        self.inner
            .lock()
            .expect("McpCallerRegistry mutex poisoned")
            .listen_port
    }

    /// Publish the shared listener URL that later `issue` calls return.
    pub fn set_url(&self, url: impl Into<String>) {
        let url = url.into();
        let port = port_from_url(Some(&url));
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        state.url = Some(url);
        state.listen_port = port.or(state.listen_port);
        drop(state);
        self.persist();
    }

    /// Return the published shared URL, if the listener is up.
    #[must_use]
    pub fn url(&self) -> Option<String> {
        self.inner
            .lock()
            .expect("McpCallerRegistry mutex poisoned")
            .url
            .clone()
    }

    /// Issue or reuse a caller credential for this session.
    ///
    /// # Errors
    ///
    /// Returns an error when the shared HTTP listener has no published URL.
    pub fn issue(&self, caller: McpCaller) -> Result<IssuedMcpCaller> {
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        let url = state
            .url
            .clone()
            .ok_or_else(|| anyhow!("shared MCP HTTP listener is not running"))?;
        if let Some(existing) = state.by_session.get(&caller.session_uuid) {
            if state.by_token.contains_key(existing) {
                return Ok(IssuedMcpCaller {
                    url,
                    token: CallerToken::new(existing.clone()),
                });
            }
        }
        let token = generate_caller_token();
        state
            .by_session
            .insert(caller.session_uuid.clone(), token.clone());
        state.by_token.insert(token.clone(), caller);
        drop(state);
        self.persist();
        Ok(IssuedMcpCaller {
            url,
            token: CallerToken::new(token),
        })
    }

    /// Authorize a bearer token and return that caller only.
    #[must_use]
    pub fn authorize(&self, token: &str) -> Option<McpCaller> {
        self.inner
            .lock()
            .expect("McpCallerRegistry mutex poisoned")
            .by_token
            .get(token)
            .cloned()
    }

    /// Revoke the credential for a session.
    pub fn revoke_session(&self, session_uuid: &str) {
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        if let Some(token) = state.by_session.remove(session_uuid) {
            state.by_token.remove(&token);
        }
        drop(state);
        self.persist();
    }

    /// Drop in-memory credentials. Persist stays so a Hub restart can restore.
    pub fn clear_live(&self) {
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        state.by_token.clear();
        state.by_session.clear();
        state.url = None;
    }

    /// Drop every in-memory credential. Used on Hub shutdown.
    pub fn revoke_all(&self) {
        self.clear_live();
    }

    /// Revoke every persisted token and write an empty snapshot.
    ///
    /// Used when the preferred listen port cannot be reclaimed so a stale
    /// `BOTSTER_MCP_TOKEN` is already dead if it reaches a squatter.
    pub fn revoke_all_persisted(&self) {
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        state.by_token.clear();
        state.by_session.clear();
        drop(state);
        self.persist();
    }

    fn persist(&self) {
        let state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        let Some(path) = state.persist_path.clone() else {
            return;
        };
        let snapshot = McpCallerSnapshot {
            port: state.listen_port,
            url: state.url.clone(),
            callers: state
                .by_session
                .iter()
                .filter_map(|(session_uuid, token)| {
                    let caller = state.by_token.get(token)?;
                    Some(PersistedCaller {
                        session_uuid: session_uuid.clone(),
                        hub_id: caller.hub_id.clone(),
                        token: token.clone(),
                        issued_at: caller.issued_at,
                    })
                })
                .collect(),
        };
        drop(state);
        if let Some(parent) = path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                log::warn!("[mcp-http] create caller snapshot dir: {error}");
                return;
            }
        }
        let Ok(bytes) = serde_json::to_vec_pretty(&snapshot) else {
            return;
        };
        if let Err(error) = write_private_file(&path, &bytes) {
            log::warn!("[mcp-http] write caller snapshot: {error}");
        }
    }

    /// Number of live caller credentials.
    #[must_use]
    pub fn caller_count(&self) -> usize {
        self.inner
            .lock()
            .expect("McpCallerRegistry mutex poisoned")
            .by_token
            .len()
    }
}

/// Fan-out for Streamable HTTP SSE notifications.
///
/// Notifications name a list kind only. They never include another caller's
/// tools, messages, or identity.
#[derive(Clone, Debug)]
pub struct McpHttpFanout {
    inner: Arc<Mutex<Vec<mpsc::UnboundedSender<McpListKind>>>>,
}

impl Default for McpHttpFanout {
    fn default() -> Self {
        Self::new()
    }
}

impl McpHttpFanout {
    /// Create an empty fan-out.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn subscribe(&self) -> mpsc::UnboundedReceiver<McpListKind> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut subs = self.inner.lock().expect("McpHttpFanout mutex poisoned");
        subs.retain(|sub| !sub.is_closed());
        subs.push(tx);
        rx
    }

    /// Notify every connected SSE caller that a list changed.
    pub fn notify(&self, kind: McpListKind) {
        let mut subs = self.inner.lock().expect("McpHttpFanout mutex poisoned");
        subs.retain(|sub| sub.send(kind).is_ok());
    }
}

/// How the HTTP layer reaches Hub Lua MCP dispatch.
#[derive(Clone)]
pub(crate) enum McpHttpDispatch {
    /// Production path: enqueue work on the Hub event loop.
    Hub(HubEventTx),
    /// Test path: answer immediately without Lua.
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "the direct dispatcher supports HTTP unit tests")
    )]
    Direct(Arc<dyn Fn(McpCaller, String, Value) -> Result<Value, String> + Send + Sync>),
}

impl std::fmt::Debug for McpHttpDispatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hub(_) => f.write_str("McpHttpDispatch::Hub"),
            Self::Direct(_) => f.write_str("McpHttpDispatch::Direct"),
        }
    }
}

/// Hub-owned Streamable HTTP listener.
#[derive(Debug)]
pub struct McpHttpListener {
    url: String,
    shutdown: Option<oneshot::Sender<()>>,
}

impl McpHttpListener {
    /// Shared MCP URL, including the `/mcp` path.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Stop accepting connections. Live requests finish or see a closed socket.
    pub fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Drop for McpHttpListener {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Bind the shared Streamable HTTP listener on loopback.
///
/// # Errors
///
/// Returns an error when bind or local-address lookup fails.
pub(crate) async fn bind_listener(
    callers: McpCallerRegistry,
    fanout: McpHttpFanout,
    dispatch: McpHttpDispatch,
) -> Result<McpHttpListener> {
    let preferred = callers.preferred_port();
    let listener = bind_loopback(preferred)
        .await
        .context("bind shared MCP HTTP listener")?;
    let addr = listener
        .local_addr()
        .context("read shared MCP HTTP listen address")?;
    if preferred.is_some_and(|port| port != addr.port()) {
        log::warn!("[mcp-http] preferred port was not reclaimed; revoking persisted caller tokens");
        callers.revoke_all_persisted();
    }
    let url = format!("http://{addr}{MCP_HTTP_PATH}");
    callers.set_url(url.clone());
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let state = Arc::new(HttpState {
        callers,
        fanout,
        dispatch,
        auth_gate: AuthGate::default(),
        services: tokio::sync::Mutex::new(HashMap::new()),
    });

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accepted = listener.accept() => {
                    let Ok((stream, peer)) = accepted else {
                        break;
                    };
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(move |request| {
                            let state = Arc::clone(&state);
                            async move { handle_request(state, request, peer.ip()).await }
                        });
                        let _ = http1::Builder::new().serve_connection(io, service).await;
                    });
                }
            }
        }
    });

    Ok(McpHttpListener {
        url,
        shutdown: Some(shutdown_tx),
    })
}

struct HttpState {
    callers: McpCallerRegistry,
    fanout: McpHttpFanout,
    dispatch: McpHttpDispatch,
    auth_gate: AuthGate,
    services: tokio::sync::Mutex<HashMap<String, CallerHttpService>>,
}

type CallerHttpService = StreamableHttpService<HttpMcpHandler, LocalSessionManager>;

#[derive(Clone, Debug)]
struct HttpMcpHandler {
    caller: McpCaller,
    fanout: McpHttpFanout,
    dispatch: McpHttpDispatch,
}

impl HttpMcpHandler {
    async fn dispatch<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<T, ErrorData> {
        let reply = dispatch_method(&self.dispatch, self.caller.clone(), method, params)
            .await
            .map_err(|message| ErrorData::internal_error(message, None))?;
        if let Some(error) = reply.error {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Hub MCP request failed")
                .to_string();
            return Err(ErrorData::internal_error(
                message,
                error.get("data").cloned(),
            ));
        }
        let mut value = reply
            .result
            .ok_or_else(|| ErrorData::internal_error("Hub returned an empty MCP reply", None))?;
        if let Some(result) = value.as_object_mut() {
            result.entry("resultType".to_string()).or_insert_with(|| {
                serde_json::to_value(ResultType::COMPLETE)
                    .expect("rmcp complete result type must serialize")
            });
            if matches!(
                method,
                "tools/list"
                    | "prompts/list"
                    | "resources/list"
                    | "resources/templates/list"
                    | "resources/read"
            ) {
                result
                    .entry("ttlMs".to_string())
                    .or_insert_with(|| json!(0));
                result.entry("cacheScope".to_string()).or_insert_with(|| {
                    serde_json::to_value(CacheScope::Private)
                        .expect("rmcp private cache scope must serialize")
                });
            }
        }
        serde_json::from_value(value).map_err(|error| {
            ErrorData::internal_error(format!("Hub returned an invalid MCP result: {error}"), None)
        })
    }
}

impl ServerHandler for HttpMcpHandler {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_tool_list_changed()
                .enable_prompts()
                .enable_prompts_list_changed()
                .enable_resources()
                .enable_resources_list_changed()
                .build(),
        )
        .with_server_info(Implementation::new(
            "botster-hub",
            env!("CARGO_PKG_VERSION"),
        ))
    }

    fn on_initialized(
        &self,
        context: NotificationContext<RoleServer>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        async move {
            let mut notifications = self.fanout.subscribe();
            let peer = context.peer;
            tokio::spawn(async move {
                while let Some(kind) = notifications.recv().await {
                    let result = match kind {
                        McpListKind::Tools => peer.notify_tool_list_changed().await,
                        McpListKind::Prompts => peer.notify_prompt_list_changed().await,
                        McpListKind::Resources => peer.notify_resource_list_changed().await,
                    };
                    if let Err(error) = result {
                        log::debug!("[mcp-http] list change notification ended: {error}");
                        break;
                    }
                }
            });
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
        async move {
            let mut result: Value = self.dispatch("tools/list", json!({})).await?;
            if let Some(tools) = result.get_mut("tools").and_then(Value::as_array_mut) {
                for tool in tools {
                    if let Some(tool) = tool.as_object_mut() {
                        tool.entry("inputSchema".to_string())
                            .or_insert_with(|| json!({ "type": "object" }));
                    }
                }
            }
            serde_json::from_value(result).map_err(|error| {
                ErrorData::internal_error(
                    format!("Hub returned an invalid tools/list result: {error}"),
                    None,
                )
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResponse, ErrorData>> + Send + '_ {
        async move {
            let params = serde_json::to_value(request).map_err(|error| {
                ErrorData::invalid_params(format!("invalid tool call: {error}"), None)
            })?;
            let result: CallToolResult = self.dispatch("tools/call", params).await?;
            Ok(result.into())
        }
    }

    fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListPromptsResult, ErrorData>> + Send + '_ {
        self.dispatch("prompts/list", json!({}))
    }

    fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<GetPromptResponse, ErrorData>> + Send + '_ {
        async move {
            let params = serde_json::to_value(request).map_err(|error| {
                ErrorData::invalid_params(format!("invalid prompt request: {error}"), None)
            })?;
            let result: GetPromptResult = self.dispatch("prompts/get", params).await?;
            Ok(result.into())
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, ErrorData>> + Send + '_ {
        std::future::ready(Ok(ListResourcesResult::with_all_items(Vec::new())
            .with_ttl_ms(0)
            .with_cache_scope(CacheScope::Private)))
    }

    fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourceTemplatesResult, ErrorData>> + Send + '_
    {
        self.dispatch("resources/templates/list", json!({}))
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ReadResourceResponse, ErrorData>> + Send + '_
    {
        async move {
            let params = serde_json::to_value(request).map_err(|error| {
                ErrorData::invalid_params(format!("invalid resource request: {error}"), None)
            })?;
            let result: ReadResourceResult = self.dispatch("resources/read", params).await?;
            Ok(result.into())
        }
    }
}

struct AuthGate {
    inner: Mutex<HashMap<IpAddr, (u32, Instant)>>,
}

impl Default for AuthGate {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl AuthGate {
    fn reject_failure(&self, peer: IpAddr) -> bool {
        let mut state = self.inner.lock().expect("AuthGate mutex poisoned");
        let entry = state.entry(peer).or_insert((0, Instant::now()));
        if entry.1.elapsed() > AUTH_FAIL_WINDOW {
            *entry = (0, Instant::now());
        }
        if entry.0 >= AUTH_FAIL_LIMIT {
            return true;
        }
        entry.0 = entry.0.saturating_add(1);
        false
    }
}

async fn handle_request(
    state: Arc<HttpState>,
    request: Request<Incoming>,
    peer: IpAddr,
) -> Result<Response<McpHttpBody>, Infallible> {
    Ok(match handle_request_inner(&state, request, peer).await {
        Ok(response) => response,
        Err(response) => response,
    })
}

async fn handle_request_inner(
    state: &HttpState,
    request: Request<Incoming>,
    peer: IpAddr,
) -> Result<Response<McpHttpBody>, Response<McpHttpBody>> {
    if request.uri().path() != MCP_HTTP_PATH {
        return Err(http_error(StatusCode::NOT_FOUND, "not found"));
    }
    if !origin_allowed(request.headers()) {
        return Err(http_error(StatusCode::FORBIDDEN, "invalid Origin"));
    }

    let Some(token) = extract_bearer(request.headers()) else {
        return Err(reject_unauthorized(
            state,
            peer,
            "missing or invalid caller credential",
        ));
    };
    let Some(caller) = state.callers.authorize(&token) else {
        return Err(reject_unauthorized(
            state,
            peer,
            "stale or unknown caller credential",
        ));
    };
    let mut service = {
        let mut services = state.services.lock().await;
        services.retain(|stored_token, _| state.callers.authorize(stored_token).is_some());
        services
            .entry(token)
            .or_insert_with(|| {
                let handler = HttpMcpHandler {
                    caller,
                    fanout: state.fanout.clone(),
                    dispatch: state.dispatch.clone(),
                };
                let mut config = StreamableHttpServerConfig::default();
                config.max_request_body_bytes = BODY_LIMIT;
                config.json_response = true;
                StreamableHttpService::new(
                    move || Ok(handler.clone()),
                    Arc::new(LocalSessionManager::default()),
                    config,
                )
            })
            .clone()
    };
    let response = Service::call(&mut service, request)
        .await
        .expect("rmcp HTTP service is infallible");
    let (parts, body) = response.into_parts();
    Ok(Response::from_parts(parts, body.boxed_unsync()))
}

async fn dispatch_method(
    dispatch: &McpHttpDispatch,
    caller: McpCaller,
    method: &str,
    params: Value,
) -> Result<McpHttpReply, String> {
    match dispatch {
        McpHttpDispatch::Direct(callback) => match callback(caller, method.to_string(), params) {
            Ok(result) => Ok(McpHttpReply::result(result)),
            Err(message) => Ok(McpHttpReply::error(-32000, message)),
        },
        McpHttpDispatch::Hub(event_tx) => {
            let (tx, rx) = oneshot::channel();
            event_tx
                .send(HubEvent::McpHttpRequest {
                    caller,
                    method: method.to_string(),
                    params,
                    reply: McpHttpReplyTx(tx),
                })
                .map_err(|_| "hub event loop is not accepting MCP requests".to_string())?;
            let timeout = match method {
                "tools/call" | "prompts/get" | "resources/read" => HUB_DISPATCH_TIMEOUT,
                "tools/list" | "prompts/list" | "resources/list" | "resources/templates/list" => {
                    HUB_LIST_TIMEOUT
                }
                _ => HUB_QUICK_TIMEOUT,
            };
            tokio::time::timeout(timeout, rx)
                .await
                .map_err(|_| "hub MCP dispatch timed out".to_string())?
                .map_err(|_| "hub MCP dispatch dropped".to_string())
        }
    }
}

fn extract_bearer(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(AUTHORIZATION)?.to_str().ok()?;
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))?;
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

fn generate_caller_token() -> String {
    let mut bytes = [0u8; CALLER_TOKEN_BYTES];
    rand::rng().fill_bytes(&mut bytes);
    format!("{CALLER_TOKEN_PREFIX}{}", encode_hex(&bytes))
}

fn reject_unauthorized(state: &HttpState, peer: IpAddr, message: &str) -> Response<McpHttpBody> {
    if state.auth_gate.reject_failure(peer) {
        return http_error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many failed caller credentials",
        );
    }
    http_error(StatusCode::UNAUTHORIZED, message)
}

fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn credential_is_expired(issued_at: u64) -> bool {
    issued_at > 0 && now_unix().saturating_sub(issued_at) > MAX_CREDENTIAL_AGE.as_secs()
}

fn session_manifest_is_gone(session_uuid: &str) -> bool {
    let Some(workspaces) = crate::env::data_dir().map(|dir| dir.join("workspaces")) else {
        return false;
    };
    workspaces.is_dir() && crate::env::session_manifest_path(session_uuid).is_none()
}

async fn bind_loopback(preferred: Option<u16>) -> Result<TcpListener> {
    if let Some(port) = preferred.filter(|port| *port > 0) {
        // Retry so a Hub restart can reclaim the port after the old listener
        // closes. macOS can keep the address busy for a short TIME_WAIT.
        for attempt in 0..8 {
            match TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, port))).await {
                Ok(listener) => return Ok(listener),
                Err(error) if attempt == 7 => {
                    log::warn!("[mcp-http] preferred port {port} unavailable: {error}");
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(25 * (attempt + 1) as u64)).await;
                }
            }
        }
    }
    TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .context("bind shared MCP HTTP listener on an ephemeral port")
}

fn port_from_url(url: Option<&str>) -> Option<u16> {
    let url = url?;
    let host = url.strip_prefix("http://")?;
    let host = host.split('/').next()?;
    host.rsplit_once(':')?.1.parse().ok()
}

fn write_private_file(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write {}", path.display()))?;
    let permissions = std::fs::Permissions::from_mode(0o600);
    file.set_permissions(permissions)
        .with_context(|| format!("chmod {}", path.display()))?;
    Ok(())
}

fn origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(ORIGIN).and_then(|value| value.to_str().ok()) else {
        return true;
    };
    matches_loopback_origin(origin)
}

fn matches_loopback_origin(origin: &str) -> bool {
    const HOSTS: &[&str] = &[
        "http://127.0.0.1",
        "http://localhost",
        "http://[::1]",
        "https://127.0.0.1",
        "https://localhost",
        "https://[::1]",
    ];
    HOSTS.iter().any(|host| {
        origin.eq_ignore_ascii_case(host)
            || origin.to_ascii_lowercase().starts_with(&format!("{host}:"))
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn full_body(bytes: impl Into<Bytes>) -> McpHttpBody {
    Full::new(bytes.into())
        .map_err(|never| match never {})
        .boxed_unsync()
}

fn http_error(status: StatusCode, message: &str) -> Response<McpHttpBody> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(full_body(json!({ "error": message }).to_string()))
        .expect("error response")
}

/// Dispatch an authorized MCP method through Lua `lib.mcp`.
///
/// The caller identity is taken from the authorized credential. This function
/// must not look up another session.
pub fn dispatch_lua_mcp(
    lua: &mlua::Lua,
    caller: &McpCaller,
    method: &str,
    params: Value,
    reply: McpHttpReplyTx,
) {
    if let Err(error) = dispatch_lua_mcp_inner(lua, caller, method, params, reply) {
        log::warn!("[mcp-http] lua dispatch failed: {error}");
    }
}

fn dispatch_lua_mcp_inner(
    lua: &mlua::Lua,
    caller: &McpCaller,
    method: &str,
    params: Value,
    reply: McpHttpReplyTx,
) -> Result<()> {
    let require: mlua::Function = lua
        .globals()
        .get("require")
        .map_err(|e| anyhow!("require is not available: {e}"))?;
    let mcp: mlua::Table = require
        .call("lib.mcp")
        .map_err(|e| anyhow!("require lib.mcp: {e}"))?;
    let context_map =
        lookup_caller_context(lua, &caller.session_uuid).unwrap_or_else(|_| caller.lua_context());
    let context = crate::lua::primitives::json::json_to_lua(
        lua,
        &Value::Object(
            context_map
                .into_iter()
                .map(|(k, v)| (k, Value::String(v)))
                .collect(),
        ),
    )
    .map_err(|e| anyhow!("encode caller context: {e}"))?;

    match method {
        "tools/list" => {
            let list: mlua::Function = mcp
                .get("list_tools")
                .map_err(|e| anyhow!("mcp.list_tools: {e}"))?;
            let tools: mlua::Value = list
                .call(caller.session_uuid.as_str())
                .map_err(|e| anyhow!("mcp.list_tools: {e}"))?;
            complete_result(lua, reply, json!({ "tools": lua_to_json(lua, tools)? }));
        }
        "tools/call" => {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("tools/call missing name"))?
                .to_string();
            let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
            let args = crate::lua::primitives::json::json_to_lua(lua, &arguments)
                .map_err(|e| anyhow!("encode tool arguments: {e}"))?;
            let call_tool: mlua::Function = mcp
                .get("call_tool")
                .map_err(|e| anyhow!("mcp.call_tool: {e}"))?;
            let tx = Arc::new(Mutex::new(Some(reply)));
            let callback = lua
                .create_function(move |lua, (content, err, is_error): (mlua::Value, mlua::Value, mlua::Value)| {
                    let Some(reply) = tx.lock().expect("mcp http reply mutex").take() else {
                        return Ok(());
                    };
                    if let Some(message) = err_string(&err) {
                        let _ = reply.0.send(McpHttpReply::error(-32000, message));
                        return Ok(());
                    }
                    let content = lua_to_json(lua, content).unwrap_or(json!([]));
                    let is_error = match is_error {
                        mlua::Value::Boolean(flag) => flag,
                        _ => false,
                    };
                    let _ = reply.0.send(McpHttpReply::result(json!({
                        "content": content,
                        "isError": is_error
                    })));
                    Ok(())
                })
                .map_err(|e| anyhow!("create tools/call callback: {e}"))?;
            call_tool
                .call::<()>((name, args, context, callback))
                .map_err(|e| anyhow!("mcp.call_tool: {e}"))?;
        }
        "prompts/list" => {
            let list: mlua::Function = mcp
                .get("list_prompts")
                .map_err(|e| anyhow!("mcp.list_prompts: {e}"))?;
            let prompts: mlua::Value = list
                .call(caller.session_uuid.as_str())
                .map_err(|e| anyhow!("mcp.list_prompts: {e}"))?;
            complete_result(lua, reply, json!({ "prompts": lua_to_json(lua, prompts)? }));
        }
        "prompts/get" => {
            let name = params
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("prompts/get missing name"))?;
            let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
            let args = crate::lua::primitives::json::json_to_lua(lua, &arguments)
                .map_err(|e| anyhow!("encode prompt arguments: {e}"))?;
            let get_prompt: mlua::Function = mcp
                .get("get_prompt")
                .map_err(|e| anyhow!("mcp.get_prompt: {e}"))?;
            match get_prompt.call::<(mlua::Value, mlua::Value)>((name, args, context.clone())) {
                Ok((result, err)) => {
                    if let Some(message) = err_string(&err) {
                        let _ = reply.0.send(McpHttpReply::error(-32000, message));
                    } else {
                        complete_result(lua, reply, lua_to_json(lua, result)?);
                    }
                }
                Err(error) => {
                    let _ = reply
                        .0
                        .send(McpHttpReply::error(-32000, format!("{error}")));
                }
            }
        }
        "resources/templates/list" => {
            let list: mlua::Function = mcp
                .get("list_resource_templates")
                .map_err(|e| anyhow!("mcp.list_resource_templates: {e}"))?;
            let templates: mlua::Value = list
                .call(caller.session_uuid.as_str())
                .map_err(|e| anyhow!("mcp.list_resource_templates: {e}"))?;
            complete_result(
                lua,
                reply,
                json!({ "resourceTemplates": lua_to_json(lua, templates)? }),
            );
        }
        "resources/read" => {
            let uri = params
                .get("uri")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("resources/read missing uri"))?
                .to_string();
            let read_resource: mlua::Function = mcp
                .get("read_resource")
                .map_err(|e| anyhow!("mcp.read_resource: {e}"))?;
            let tx = Arc::new(Mutex::new(Some(reply)));
            let callback = lua
                .create_function(move |lua, (contents, err): (mlua::Value, mlua::Value)| {
                    let Some(reply) = tx.lock().expect("mcp http reply mutex").take() else {
                        return Ok(());
                    };
                    if let Some(message) = err_string(&err) {
                        let _ = reply.0.send(McpHttpReply::error(-32002, message));
                        return Ok(());
                    }
                    let contents = lua_to_json(lua, contents).unwrap_or(json!([]));
                    let _ = reply
                        .0
                        .send(McpHttpReply::result(json!({ "contents": contents })));
                    Ok(())
                })
                .map_err(|e| anyhow!("create resources/read callback: {e}"))?;
            read_resource
                .call::<()>((uri, context, callback))
                .map_err(|e| anyhow!("mcp.read_resource: {e}"))?;
        }
        other => {
            let _ = reply.0.send(McpHttpReply::error(
                -32601,
                format!("method not found: {other}"),
            ));
        }
    }
    Ok(())
}

fn lookup_caller_context(
    lua: &mlua::Lua,
    session_uuid: &str,
) -> Result<std::collections::BTreeMap<String, String>> {
    let require: mlua::Function = lua
        .globals()
        .get("require")
        .map_err(|e| anyhow!("require is not available: {e}"))?;
    let mcp: mlua::Table = require
        .call("lib.mcp")
        .map_err(|e| anyhow!("require lib.mcp: {e}"))?;
    let caller_context: mlua::Function = mcp
        .get("caller_context")
        .map_err(|e| anyhow!("mcp.caller_context: {e}"))?;
    let table: mlua::Table = caller_context
        .call(session_uuid)
        .map_err(|e| anyhow!("mcp.caller_context: {e}"))?;
    let mut ctx = std::collections::BTreeMap::new();
    for pair in table.pairs::<String, String>() {
        let (key, value) = pair.map_err(|e| anyhow!("caller context pair: {e}"))?;
        if !value.is_empty() {
            ctx.insert(key, value);
        }
    }
    ctx.insert("session_uuid".to_string(), session_uuid.to_string());
    Ok(ctx)
}

fn complete_result(lua: &mlua::Lua, reply: McpHttpReplyTx, value: Value) {
    let _ = reply.0.send(McpHttpReply::result(value));
    let _ = lua;
}

fn lua_to_json(lua: &mlua::Lua, value: mlua::Value) -> Result<Value> {
    lua.from_value(value)
        .map_err(|e| anyhow!("convert Lua MCP value: {e}"))
}

fn err_string(err: &mlua::Value) -> Option<String> {
    match err {
        mlua::Value::Nil => None,
        mlua::Value::String(s) => Some(s.to_str().ok()?.to_string()),
        other => Some(format!("{other:?}")),
    }
}

/// Register `hub.issue_mcp_caller`, `hub.revoke_mcp_caller`, `hub.mcp_url`,
/// and `hub.notify_mcp_list_changed`.
///
/// # Errors
///
/// Returns an error when Lua function creation fails.
pub fn register_lua(
    lua: &mlua::Lua,
    callers: McpCallerRegistry,
    fanout: McpHttpFanout,
) -> Result<()> {
    let hub: mlua::Table = lua
        .globals()
        .get("hub")
        .unwrap_or_else(|_| lua.create_table().expect("hub table"));

    let issue_callers = callers.clone();
    let issue_fn = lua
        .create_function(move |lua, opts: mlua::Table| {
            let session_uuid: String = opts
                .get("session_uuid")
                .map_err(|_| mlua::Error::runtime("hub.issue_mcp_caller: session_uuid required"))?;
            let hub_id: String = opts.get("hub_id").unwrap_or_default();
            let mut caller = McpCaller::new(session_uuid, hub_id);
            if let Ok(context) = opts.get::<mlua::Table>("context") {
                for pair in context.pairs::<String, String>() {
                    let (key, value) = pair.map_err(mlua::Error::external)?;
                    if key != "session_uuid" && key != "hub_id" {
                        caller.context.insert(key, value);
                    }
                }
            }
            let issued = issue_callers.issue(caller).map_err(mlua::Error::external)?;
            let table = lua.create_table()?;
            table.set("url", issued.url)?;
            table.set("token", issued.token.as_str().to_string())?;
            table.set("url_env", BOTSTER_MCP_URL_ENV)?;
            table.set("token_env", BOTSTER_MCP_TOKEN_ENV)?;
            Ok(table)
        })
        .map_err(|e| anyhow!("create hub.issue_mcp_caller: {e}"))?;
    hub.set("issue_mcp_caller", issue_fn)
        .map_err(|e| anyhow!("set hub.issue_mcp_caller: {e}"))?;

    let revoke_callers = callers.clone();
    let revoke_fn = lua
        .create_function(move |_, session_uuid: String| {
            revoke_callers.revoke_session(&session_uuid);
            Ok(())
        })
        .map_err(|e| anyhow!("create hub.revoke_mcp_caller: {e}"))?;
    hub.set("revoke_mcp_caller", revoke_fn)
        .map_err(|e| anyhow!("set hub.revoke_mcp_caller: {e}"))?;

    let url_callers = callers;
    let url_fn = lua
        .create_function(move |_, ()| Ok(url_callers.url()))
        .map_err(|e| anyhow!("create hub.mcp_url: {e}"))?;
    hub.set("mcp_url", url_fn)
        .map_err(|e| anyhow!("set hub.mcp_url: {e}"))?;

    let notify_fanout = fanout;
    let notify_fn = lua
        .create_function(move |_, kind: String| {
            if let Some(kind) = McpListKind::parse(&kind) {
                notify_fanout.notify(kind);
            }
            Ok(())
        })
        .map_err(|e| anyhow!("create hub.notify_mcp_list_changed: {e}"))?;
    hub.set("notify_mcp_list_changed", notify_fn)
        .map_err(|e| anyhow!("set hub.notify_mcp_list_changed: {e}"))?;

    lua.globals()
        .set("hub", hub)
        .map_err(|e| anyhow!("set hub global: {e}"))?;
    Ok(())
}

/// Resolve the shared HTTP URL and caller token for `botster mcp-serve`.
///
/// Prefers live environment values. If `BOTSTER_MCP_URL` is missing or stale,
/// the Hub manifest `mcp_url` is used. The caller token still comes from env
/// because persist restores that same secret after a Hub restart.
#[must_use]
pub fn resolve_stdio_proxy_target() -> Option<(String, String)> {
    let token = std::env::var(BOTSTER_MCP_TOKEN_ENV)
        .ok()
        .filter(|value| !value.is_empty())?;
    let env_url = std::env::var(BOTSTER_MCP_URL_ENV)
        .ok()
        .filter(|value| !value.is_empty());
    let url = env_url.or_else(read_manifest_mcp_url)?;
    Some((url, token))
}

fn read_manifest_mcp_url() -> Option<String> {
    let path = std::env::var("BOTSTER_HUB_MANIFEST_PATH").ok()?;
    let content = std::fs::read_to_string(path).ok()?;
    let value: Value = serde_json::from_str(&content).ok()?;
    value
        .get("mcp_url")
        .and_then(Value::as_str)
        .filter(|url| !url.is_empty())
        .map(str::to_string)
}

/// Run a stdio JSON-RPC proxy to the shared HTTP server.
///
/// This is the short migration path for clients that still launch
/// `botster mcp-serve`. Removal target: [`SOCKET_MCP_FALLBACK_REMOVAL`].
/// New clients should use `BOTSTER_MCP_URL` directly.
///
/// # Errors
///
/// Returns an error when stdin/stdout proxying fails.
pub fn run_stdio_proxy(url: &str, token: &str) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_stdio_proxy_async(url, token))
}

async fn run_stdio_proxy_async(url: &str, token: &str) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::task::JoinSet;

    let client = reqwest::Client::new();
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let stdout = Arc::new(tokio::sync::Mutex::new(tokio::io::stdout()));
    let url = Arc::new(url.to_string());
    let token = Arc::new(token.to_string());
    let mut tasks = JoinSet::new();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let client = client.clone();
        let stdout = Arc::clone(&stdout);
        let url = Arc::clone(&url);
        let token = Arc::clone(&token);
        while tasks.try_join_next().is_some() {}
        tasks.spawn(async move {
            let body = match proxy_http_line(&client, &url, &token, &line).await {
                Ok(body) => body,
                Err(error) => jsonrpc_error_for_line(&line, error),
            };
            let mut out = stdout.lock().await;
            out.write_all(body.as_bytes()).await?;
            if !body.ends_with('\n') {
                out.write_all(b"\n").await?;
            }
            out.flush().await?;
            Ok::<(), anyhow::Error>(())
        });
    }
    while let Some(joined) = tasks.join_next().await {
        joined.context("stdio MCP proxy task")??;
    }
    Ok(())
}

fn jsonrpc_error_for_line(line: &str, error: impl std::fmt::Display) -> String {
    let id = serde_json::from_str::<Value>(line)
        .ok()
        .and_then(|value| value.get("id").cloned())
        .unwrap_or(Value::Null);
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": -32603, "message": error.to_string() }
    })
    .to_string()
}

async fn proxy_http_line(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    line: &str,
) -> Result<String> {
    match post_mcp_line(client, url, token, line).await {
        Ok(body) => Ok(body),
        Err(_) => {
            let retry_url = read_manifest_mcp_url().unwrap_or_else(|| url.to_string());
            post_mcp_line(client, &retry_url, token, &line)
                .await
                .with_context(|| format!("proxy MCP POST to {retry_url}"))
        }
    }
}

async fn post_mcp_line(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    line: &str,
) -> Result<String> {
    let method = serde_json::from_str::<Value>(line).ok().and_then(|value| {
        value
            .get("method")
            .and_then(Value::as_str)
            .map(str::to_string)
    });
    let key = format!("{url}\0{token}");
    let request_lock = proxy_request_lock(&key);
    let _request_guard = request_lock.lock().await;
    if method.as_deref() != Some("initialize") && proxy_session(&key).is_none() {
        initialize_proxy_session(client, url, token, &key).await?;
    }

    let mut request = client
        .post(url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .header(hyper::header::ACCEPT, "application/json, text/event-stream");
    if method.as_deref() != Some("initialize") {
        if let Some(session_id) = proxy_session(&key) {
            request = request.header("mcp-session-id", session_id);
        }
    }
    let response = request
        .body(line.to_string())
        .send()
        .await
        .with_context(|| format!("proxy MCP POST to {url}"))?;
    let status = response.status();
    if method.as_deref() == Some("initialize") && status.is_success() {
        if let Some(session_id) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
        {
            set_proxy_session(key, session_id.to_string());
        }
    }
    let body = response.text().await.unwrap_or_default();
    if status == StatusCode::UNAUTHORIZED {
        anyhow::bail!("shared MCP HTTP proxy received HTTP {status}");
    }
    if !status.is_success() && body.trim().is_empty() {
        anyhow::bail!("shared MCP HTTP proxy received HTTP {status}");
    }
    Ok(json_from_mcp_http_body(&body).unwrap_or(body))
}

fn proxy_sessions() -> &'static Mutex<HashMap<String, String>> {
    use std::sync::OnceLock;
    static SESSIONS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn proxy_request_locks() -> &'static Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>> {
    use std::sync::OnceLock;
    static LOCKS: OnceLock<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn proxy_request_lock(key: &str) -> Arc<tokio::sync::Mutex<()>> {
    let mut locks = proxy_request_locks()
        .lock()
        .expect("MCP proxy request lock map poisoned");
    Arc::clone(
        locks
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
    )
}

fn proxy_session(key: &str) -> Option<String> {
    proxy_sessions()
        .lock()
        .expect("MCP proxy session mutex poisoned")
        .get(key)
        .cloned()
}

fn set_proxy_session(key: String, session_id: String) {
    proxy_sessions()
        .lock()
        .expect("MCP proxy session mutex poisoned")
        .insert(key, session_id);
}

async fn initialize_proxy_session(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    key: &str,
) -> Result<()> {
    let response = client
        .post(url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .header(
            hyper::header::ACCEPT,
            "application/json, text/event-stream",
        )
        .json(&json!({
            "jsonrpc": "2.0",
            "id": "botster-proxy-initialize",
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "botster-stdio-proxy", "version": env!("CARGO_PKG_VERSION") }
            }
        }))
        .send()
        .await
        .with_context(|| format!("initialize proxy MCP session at {url}"))?;
    let session_id = response
        .headers()
        .get("mcp-session-id")
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| anyhow!("proxy MCP initialize did not return a session id"))?
        .to_string();
    set_proxy_session(key.to_string(), session_id.clone());
    client
        .post(url)
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .header("mcp-session-id", session_id)
        .header(hyper::header::ACCEPT, "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized"
        }))
        .send()
        .await
        .with_context(|| format!("complete proxy MCP initialization at {url}"))?;
    Ok(())
}

fn json_from_mcp_http_body(body: &str) -> Option<String> {
    if serde_json::from_str::<Value>(body).is_ok() {
        return Some(body.to_string());
    }
    body.lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim)
        .find(|data| serde_json::from_str::<Value>(data).is_ok())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_runtime() -> &'static tokio::runtime::Runtime {
        use std::sync::OnceLock;
        static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RT.get_or_init(|| tokio::runtime::Runtime::new().expect("test runtime"))
    }

    fn scoped_dispatcher() -> McpHttpDispatch {
        McpHttpDispatch::Direct(Arc::new(|caller, method, params| match method.as_str() {
            "tools/list" => Ok(json!({
                "tools": [{
                    "name": format!("whoami_{}", caller.session_uuid),
                    "description": format!("caller {}", caller.session_uuid)
                }]
            })),
            "prompts/list" => Ok(json!({ "prompts": [] })),
            "resources/templates/list" => Ok(json!({ "resourceTemplates": [] })),
            "tools/call" => {
                let name = params.get("name").and_then(Value::as_str).unwrap_or("");
                if name != format!("whoami_{}", caller.session_uuid) {
                    return Err(format!("tool not available for this caller: {name}"));
                }
                Ok(json!({
                    "content": [{
                        "type": "text",
                        "text": json!({
                            "session_uuid": caller.session_uuid,
                            "hub_id": caller.hub_id
                        }).to_string()
                    }],
                    "isError": false
                }))
            }
            other => Err(format!("unexpected method {other}")),
        }))
    }

    async fn start_server() -> (McpCallerRegistry, McpHttpFanout, McpHttpListener) {
        let callers = McpCallerRegistry::new();
        let fanout = McpHttpFanout::new();
        let listener = bind_listener(callers.clone(), fanout.clone(), scoped_dispatcher())
            .await
            .expect("bind listener");
        (callers, fanout, listener)
    }

    async fn rpc(
        url: &str,
        token: Option<&str>,
        id: u64,
        method: &str,
        params: Value,
    ) -> (StatusCode, Value) {
        let client = reqwest::Client::new();
        let token_key = token.map(|value| format!("{url}\0{value}"));
        let sessions = test_sessions();
        if method != "initialize" {
            if let (Some(token), Some(key)) = (token, token_key.as_ref()) {
                if !sessions
                    .lock()
                    .expect("test session mutex")
                    .contains_key(key)
                {
                    initialize_test_session(&client, url, token, key).await;
                }
            }
        }

        let params = if method == "initialize" {
            let protocol_version = params
                .get("protocolVersion")
                .cloned()
                .unwrap_or_else(|| json!("2025-06-18"));
            json!({
                "protocolVersion": protocol_version,
                "capabilities": {},
                "clientInfo": { "name": "botster-http-test", "version": "1" }
            })
        } else {
            params
        };
        let mut request = client
            .post(url)
            .header(hyper::header::ACCEPT, "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": method,
                "params": params
            }));
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        if method != "initialize" {
            if let Some(session_id) = token_key.as_ref().and_then(|key| {
                sessions
                    .lock()
                    .expect("test session mutex")
                    .get(key)
                    .cloned()
            }) {
                request = request.header("mcp-session-id", session_id);
            }
        }
        let response = request.send().await.expect("http post");
        let status = response.status();
        if method == "initialize" && status.is_success() {
            if let (Some(key), Some(session_id)) = (
                token_key,
                response
                    .headers()
                    .get("mcp-session-id")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string),
            ) {
                sessions
                    .lock()
                    .expect("test session mutex")
                    .insert(key, session_id);
            }
        }
        let response_body = response.text().await.unwrap_or_default();
        let body = json_from_mcp_http_body(&response_body)
            .and_then(|body| serde_json::from_str::<Value>(&body).ok())
            .unwrap_or(json!({}));
        (status, body)
    }

    fn test_sessions() -> &'static Mutex<HashMap<String, String>> {
        use std::sync::OnceLock;
        static SESSIONS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
        SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    async fn initialize_test_session(client: &reqwest::Client, url: &str, token: &str, key: &str) {
        let response = client
            .post(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(hyper::header::ACCEPT, "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": "auto-initialize",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": { "name": "botster-http-test", "version": "1" }
                }
            }))
            .send()
            .await
            .expect("initialize test MCP session");
        let session_id = response
            .headers()
            .get("mcp-session-id")
            .and_then(|value| value.to_str().ok())
            .expect("MCP session header")
            .to_string();
        test_sessions()
            .lock()
            .expect("test session mutex")
            .insert(key.to_string(), session_id.clone());
        let _ = client
            .post(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("mcp-session-id", session_id)
            .header(hyper::header::ACCEPT, "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            }))
            .send()
            .await
            .expect("send initialized notification");
    }

    async fn shared_client_tools_list(
        client: &reqwest::Client,
        url: &str,
        token: &str,
        session_id: &str,
        id: u64,
    ) -> Value {
        let response = client
            .post(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header("mcp-session-id", session_id)
            .header(hyper::header::ACCEPT, "application/json, text/event-stream")
            .json(&json!({
                "jsonrpc": "2.0",
                "id": id,
                "method": "tools/list",
                "params": {}
            }))
            .send()
            .await
            .expect("shared client tools/list");
        let body = response.text().await.expect("shared client body");
        let body = json_from_mcp_http_body(&body).unwrap_or(body);
        serde_json::from_str(&body).expect("shared client JSON-RPC response")
    }

    #[test]
    fn caller_token_debug_redacts_secret() {
        let token = CallerToken::new("btcaller_supersecret");
        let rendered = format!("{token:?}");
        assert!(rendered.contains("CallerToken"));
        assert!(!rendered.contains("supersecret"));
        assert!(!format!("{token}").contains("supersecret"));
    }

    #[test]
    fn issue_reuses_live_token_and_revokes_on_session_close() {
        let callers = McpCallerRegistry::new();
        callers.set_url("http://127.0.0.1:9/mcp");
        let first = callers
            .issue(McpCaller::new("sess-a", "hub-1"))
            .expect("issue");
        let second = callers
            .issue(McpCaller::new("sess-a", "hub-1"))
            .expect("reuse");
        assert_eq!(first.token.as_str(), second.token.as_str());
        assert_eq!(callers.caller_count(), 1);
        callers.revoke_session("sess-a");
        assert!(callers.authorize(first.token.as_str()).is_none());
        assert_eq!(callers.caller_count(), 0);
    }

    #[test]
    fn authorize_does_not_return_another_caller() {
        let callers = McpCallerRegistry::new();
        callers.set_url("http://127.0.0.1:9/mcp");
        let alice = callers
            .issue(McpCaller::new("sess-alice", "hub-1"))
            .expect("alice");
        let bob = callers
            .issue(McpCaller::new("sess-bob", "hub-1"))
            .expect("bob");
        let authorized = callers.authorize(alice.token.as_str()).expect("alice auth");
        assert_eq!(authorized.session_uuid, "sess-alice");
        assert_ne!(authorized.session_uuid, "sess-bob");
        assert_ne!(alice.token.as_str(), bob.token.as_str());
        assert!(!alice.token.as_str().contains("sess-alice"));
        assert!(!alice.url.contains("sess-alice"));
    }

    #[test]
    fn shared_listener_reuses_one_url_across_sessions() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let alice = callers
                .issue(McpCaller::new("sess-alice", "hub-1"))
                .expect("alice");
            let bob = callers
                .issue(McpCaller::new("sess-bob", "hub-1"))
                .expect("bob");
            assert_eq!(alice.url, bob.url);
            assert_eq!(alice.url, listener.url());
            assert_eq!(callers.caller_count(), 2);
        });
    }

    #[test]
    fn missing_and_stale_tokens_are_rejected() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-live", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();

            let (status, _) = rpc(&url, None, 1, "initialize", json!({})).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);

            let (status, _) = rpc(&url, Some("btcaller_unknown"), 2, "initialize", json!({})).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);

            callers.revoke_session("sess-live");
            let (status, _) = rpc(
                &url,
                Some(issued.token.as_str()),
                3,
                "initialize",
                json!({}),
            )
            .await;
            assert_eq!(status, StatusCode::UNAUTHORIZED);
        });
    }

    #[test]
    fn callers_cannot_see_each_other_tools_or_identity() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let alice = callers
                .issue(McpCaller::new("sess-alice", "hub-alice"))
                .expect("alice");
            let bob = callers
                .issue(McpCaller::new("sess-bob", "hub-bob"))
                .expect("bob");
            let url = listener.url().to_string();

            let (_, init) = rpc(&url, Some(alice.token.as_str()), 1, "initialize", json!({})).await;
            assert_eq!(init["result"]["serverInfo"]["name"], "botster-hub");
            assert!(
                !init.to_string().contains("sess-alice"),
                "initialize must not leak the caller session: {init}"
            );

            let (_, alice_tools) =
                rpc(&url, Some(alice.token.as_str()), 2, "tools/list", json!({})).await;
            let (_, bob_tools) =
                rpc(&url, Some(bob.token.as_str()), 3, "tools/list", json!({})).await;
            let alice_name = alice_tools["result"]["tools"][0]["name"].as_str().unwrap();
            let bob_name = bob_tools["result"]["tools"][0]["name"].as_str().unwrap();
            assert_eq!(alice_name, "whoami_sess-alice");
            assert_eq!(bob_name, "whoami_sess-bob");
            assert!(!alice_tools.to_string().contains("sess-bob"));
            assert!(!bob_tools.to_string().contains("sess-alice"));

            let (_, stolen) = rpc(
                &url,
                Some(alice.token.as_str()),
                4,
                "tools/call",
                json!({ "name": "whoami_sess-bob", "arguments": {} }),
            )
            .await;
            assert!(
                stolen["error"]["message"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("not available"),
                "alice must not call bob's tool: {stolen}"
            );

            let (_, own) = rpc(
                &url,
                Some(alice.token.as_str()),
                5,
                "tools/call",
                json!({ "name": "whoami_sess-alice", "arguments": {} }),
            )
            .await;
            let text = own["result"]["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("sess-alice"));
            assert!(!text.contains("sess-bob"));
        });
    }

    #[test]
    fn sequential_posts_reuse_the_same_caller_credential() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-re", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();
            let (_, first) = rpc(
                &url,
                Some(issued.token.as_str()),
                1,
                "tools/list",
                json!({}),
            )
            .await;
            let (_, second) = rpc(
                &url,
                Some(issued.token.as_str()),
                2,
                "tools/list",
                json!({}),
            )
            .await;
            assert_eq!(
                first["result"]["tools"][0]["name"],
                second["result"]["tools"][0]["name"]
            );
        });
    }

    #[test]
    fn concurrent_http_posts_complete_on_one_mcp_session() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-concurrent", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();
            let token = issued.token.as_str().to_string();
            let (_, initialized) = rpc(&url, Some(&token), 1, "initialize", json!({})).await;
            assert!(initialized.get("result").is_some());

            for sequence in 0..16 {
                let pair = async {
                    tokio::join!(
                        rpc(
                            &url,
                            Some(&token),
                            sequence * 2 + 2,
                            "tools/list",
                            json!({})
                        ),
                        rpc(
                            &url,
                            Some(&token),
                            sequence * 2 + 3,
                            "tools/list",
                            json!({})
                        )
                    )
                };
                let (first, second) = tokio::time::timeout(Duration::from_secs(2), pair)
                    .await
                    .expect("concurrent MCP requests must finish");
                assert!(first.1.get("result").is_some());
                assert!(second.1.get("result").is_some());
            }
        });
    }

    #[test]
    fn shared_http_client_can_reuse_connections_for_concurrent_posts() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-shared-client", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();
            let token = issued.token.as_str().to_string();
            let key = format!("{url}\0{token}");
            let client = reqwest::Client::new();
            initialize_test_session(&client, &url, &token, &key).await;
            let session_id = test_sessions()
                .lock()
                .expect("test session mutex")
                .get(&key)
                .cloned()
                .expect("MCP session id");

            for sequence in 0..16 {
                let pair = async {
                    tokio::join!(
                        shared_client_tools_list(
                            &client,
                            &url,
                            &token,
                            &session_id,
                            sequence * 2 + 1
                        ),
                        shared_client_tools_list(
                            &client,
                            &url,
                            &token,
                            &session_id,
                            sequence * 2 + 2
                        )
                    )
                };
                let (first, second) = tokio::time::timeout(Duration::from_secs(2), pair)
                    .await
                    .expect("shared client MCP requests must finish");
                assert!(first.get("result").is_some());
                assert!(second.get("result").is_some());
            }
        });
    }

    #[test]
    fn hub_restart_restores_port_and_caller_token() {
        test_runtime().block_on(async {
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("mcp_callers.json");
            let callers = McpCallerRegistry::new();
            callers.set_persist_path(&path);
            let fanout = McpHttpFanout::new();
            let mut listener = bind_listener(callers.clone(), fanout.clone(), scoped_dispatcher())
                .await
                .expect("bind");
            let issued = callers
                .issue(McpCaller::new("sess-recover", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();
            let token = issued.token.as_str().to_string();
            let (_, ok) = rpc(&url, Some(&token), 1, "initialize", json!({})).await;
            assert!(ok.get("result").is_some());

            listener.shutdown();
            drop(listener);
            callers.clear_live();
            assert!(callers.authorize(&token).is_none());
            tokio::time::sleep(Duration::from_millis(50)).await;

            let restored = McpCallerRegistry::new();
            restored.set_persist_path(&path);
            restored.restore_from_disk();
            assert_eq!(restored.preferred_port(), port_from_url(Some(&url)));
            let _listener = bind_listener(restored.clone(), fanout, scoped_dispatcher())
                .await
                .expect("rebind");
            assert_eq!(restored.url().as_deref(), Some(url.as_str()));
            assert_eq!(
                restored.authorize(&token).expect("restored").session_uuid,
                "sess-recover"
            );
            test_sessions()
                .lock()
                .expect("test session mutex")
                .remove(&format!("{url}\0{token}"));
            let (_, again) = rpc(&url, Some(&token), 2, "tools/list", json!({})).await;
            assert_eq!(again["result"]["tools"][0]["name"], "whoami_sess-recover");
        });
    }

    #[test]
    fn shutdown_revokes_credentials_and_closes_listener() {
        test_runtime().block_on(async {
            let (callers, _fanout, mut listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-stop", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();
            let token = issued.token.as_str().to_string();
            let (_, ok) = rpc(&url, Some(&token), 1, "initialize", json!({})).await;
            assert!(ok.get("result").is_some());

            listener.shutdown();
            callers.revoke_all();
            tokio::time::sleep(Duration::from_millis(50)).await;

            assert!(callers.authorize(&token).is_none());
            let result = reqwest::Client::new()
                .post(&url)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .json(&json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{}}))
                .send()
                .await;
            assert!(
                result.is_err() || result.unwrap().status() == StatusCode::UNAUTHORIZED,
                "shutdown must reject or close stale callers"
            );
        });
    }

    #[test]
    fn agent_configuration_uses_url_and_token_env_vars() {
        let callers = McpCallerRegistry::new();
        callers.set_url("http://127.0.0.1:9/mcp");
        let issued = callers
            .issue(McpCaller::new("sess-cfg", "hub-1"))
            .expect("issue");
        assert_eq!(BOTSTER_MCP_URL_ENV, "BOTSTER_MCP_URL");
        assert_eq!(BOTSTER_MCP_TOKEN_ENV, "BOTSTER_MCP_TOKEN");
        assert_eq!(issued.url, "http://127.0.0.1:9/mcp");
        assert!(issued.token.as_str().starts_with("btcaller_"));
        assert!(!issued.token.as_str().contains("sess-cfg"));

        let plugin = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../.agents/plugins/botster/.mcp.json");
        let config = std::fs::read_to_string(plugin).expect("plugin mcp config");
        assert!(config.contains(BOTSTER_MCP_URL_ENV));
        assert!(config.contains(BOTSTER_MCP_TOKEN_ENV));
        assert!(config.contains("\"type\": \"http\""));
        assert!(!config.contains("mcp-serve"));
        assert!(!config.contains("BOTSTER_SESSION_UUID"));
        assert_eq!(SOCKET_MCP_FALLBACK_REMOVAL, "2026-10-01");
    }

    #[test]
    fn lua_issue_and_revoke_are_session_scoped() {
        let lua = mlua::Lua::new();
        let callers = McpCallerRegistry::new();
        callers.set_url("http://127.0.0.1:9/mcp");
        register_lua(&lua, callers.clone(), McpHttpFanout::new()).expect("register");
        lua.load(
            r#"
            local first = hub.issue_mcp_caller({ session_uuid = "sess-lua", hub_id = "hub-lua" })
            local second = hub.issue_mcp_caller({ session_uuid = "sess-lua", hub_id = "hub-lua" })
            assert(first.url == second.url)
            assert(first.token == second.token)
            assert(first.url_env == "BOTSTER_MCP_URL")
            assert(first.token_env == "BOTSTER_MCP_TOKEN")
            other = hub.issue_mcp_caller({ session_uuid = "sess-other", hub_id = "hub-lua" })
            assert(other.token ~= first.token)
            hub.revoke_mcp_caller("sess-lua")
            revoked_token = first.token
            "#,
        )
        .exec()
        .expect("lua issue/revoke");
        let revoked: String = lua.globals().get("revoked_token").expect("token");
        assert!(callers.authorize(&revoked).is_none());
        assert_eq!(callers.caller_count(), 1);
    }

    #[test]
    fn handler_advertises_each_implemented_capability() {
        let handler = HttpMcpHandler {
            caller: McpCaller::new("sess-info", "hub-1"),
            fanout: McpHttpFanout::new(),
            dispatch: scoped_dispatcher(),
        };
        let info = handler.get_info();
        assert!(info.capabilities.tools.is_some());
        assert!(info.capabilities.prompts.is_some());
        assert!(info.capabilities.resources.is_some());
    }

    #[test]
    fn sse_get_holds_and_delivers_list_changed() {
        test_runtime().block_on(async {
            let (callers, fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-sse", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();
            let token = issued.token.as_str().to_string();
            let (_, initialized) = rpc(
                &url,
                Some(&token),
                1,
                "initialize",
                json!({ "protocolVersion": "2025-06-18" }),
            )
            .await;
            assert!(initialized.get("result").is_some());
            let session_key = format!("{url}\0{token}");
            let session_id = test_sessions()
                .lock()
                .expect("test session mutex")
                .get(&session_key)
                .cloned()
                .expect("MCP session id");
            let initialized_response = reqwest::Client::new()
                .post(&url)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header("mcp-session-id", &session_id)
                .header(
                    hyper::header::ACCEPT,
                    "application/json, text/event-stream",
                )
                .json(&json!({
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized"
                }))
                .send()
                .await
                .expect("initialized notification");
            assert_eq!(initialized_response.status(), StatusCode::ACCEPTED);
            let host = url
                .strip_prefix("http://")
                .and_then(|rest| rest.split('/').next())
                .expect("listen host");
            let notify = fanout.clone();
            let token_for_get = token.clone();
            let host_for_get = host.to_string();
            let join = tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut stream = tokio::net::TcpStream::connect(&host_for_get)
                    .await
                    .expect("sse connect");
                let request = format!(
                    "GET /mcp HTTP/1.1\r\nHost: {host_for_get}\r\nAuthorization: Bearer {token_for_get}\r\nMcp-Session-Id: {session_id}\r\nAccept: text/event-stream\r\n\r\n"
                );
                stream.write_all(request.as_bytes()).await.expect("sse write");
                tokio::time::sleep(Duration::from_millis(40)).await;
                notify.notify(McpListKind::Tools);
                let mut body = String::new();
                let mut buf = [0u8; 2048];
                let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                while tokio::time::Instant::now() < deadline {
                    match tokio::time::timeout(Duration::from_millis(400), stream.read(&mut buf))
                        .await
                    {
                        Ok(Ok(0)) => break,
                        Ok(Ok(n)) => {
                            body.push_str(&String::from_utf8_lossy(&buf[..n]));
                            if body.contains("notifications/tools/list_changed") {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                body
            });
            let body = join.await.expect("sse task");
            assert!(body.contains("200 OK"), "SSE must connect: {body}");
            assert!(
                body.contains("notifications/tools/list_changed"),
                "held SSE must deliver list_changed: {body}"
            );
            assert!(!body.contains("sess-sse"));
        });
    }

    #[test]
    fn foreign_origin_is_rejected() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-origin", "hub-1"))
                .expect("issue");
            let response = reqwest::Client::new()
                .post(listener.url())
                .header(AUTHORIZATION, format!("Bearer {}", issued.token.as_str()))
                .header(ORIGIN, "https://evil.example")
                .json(&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}))
                .send()
                .await
                .expect("origin post");
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
        });
    }

    #[test]
    fn initialize_negotiates_protocol_and_resources_list_is_implemented() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-proto", "hub-1"))
                .expect("issue");
            let (_, body) = rpc(
                listener.url(),
                Some(issued.token.as_str()),
                1,
                "initialize",
                json!({ "protocolVersion": "2025-06-18" }),
            )
            .await;
            assert_eq!(body["result"]["protocolVersion"], "2025-06-18");

            let (status, resources) = rpc(
                listener.url(),
                Some(issued.token.as_str()),
                2,
                "resources/list",
                json!({}),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(resources["result"]["resources"], json!([]));
        });
    }

    #[test]
    fn protocol_2026_cacheable_results_include_required_envelope() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-protocol-2026", "hub-1"))
                .expect("issue");
            let client = reqwest::Client::new();
            let url = listener.url();
            let token = issued.token.as_str();

            for (id, method, collection) in [
                (2, "tools/list", "tools"),
                (3, "prompts/list", "prompts"),
                (4, "resources/list", "resources"),
                (5, "resources/templates/list", "resourceTemplates"),
            ] {
                let response = client
                    .post(url)
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .header("mcp-protocol-version", "2026-07-28")
                    .header("mcp-method", method)
                    .header(hyper::header::ACCEPT, "application/json, text/event-stream")
                    .json(&json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "method": method,
                        "params": {
                            "_meta": {
                                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                                "io.modelcontextprotocol/clientInfo": {
                                    "name": "claude-code-compatible-test",
                                    "version": "1"
                                },
                                "io.modelcontextprotocol/clientCapabilities": {}
                            }
                        }
                    }))
                    .send()
                    .await
                    .unwrap_or_else(|error| panic!("protocol 2026 {method}: {error}"));
                assert_eq!(response.status(), StatusCode::OK, "{method}");
                let body = response
                    .text()
                    .await
                    .unwrap_or_else(|error| panic!("protocol 2026 {method} body: {error}"));
                let body = json_from_mcp_http_body(&body).unwrap_or(body);
                let response: Value = serde_json::from_str(&body)
                    .unwrap_or_else(|error| panic!("protocol 2026 {method} JSON-RPC: {error}"));
                assert_eq!(response["result"]["resultType"], "complete", "{method}");
                assert_eq!(response["result"]["ttlMs"], 0, "{method}");
                assert_eq!(response["result"]["cacheScope"], "private", "{method}");
                assert!(response["result"][collection].is_array(), "{method}");
            }
        });
    }

    fn install_mcp_stub(lua: &mlua::Lua) {
        lua.load(
            r#"
            package.preload["lib.mcp"] = function()
                return {
                    caller_context = function(session)
                        return {
                            session_uuid = session,
                            session_name = "from-manifest",
                            branch_name = "main",
                            repo = "org/repo",
                            worktree_path = "/tmp/wt",
                            workspace_id = "ws-1",
                            agent_name = "agent",
                            hub_id = "device-hub",
                        }
                    end,
                    list_tools = function(session)
                        return {{
                            name = "whoami_" .. session,
                            description = "t",
                            input_schema = { type = "object" },
                        }}
                    end,
                    call_tool = function(_, _, context, cb)
                        cb({
                            {
                                type = "text",
                                text = table.concat({
                                    context.session_name,
                                    context.repo,
                                    context.branch_name,
                                    context.worktree_path,
                                }, "|"),
                            },
                        }, nil, false)
                    end,
                    list_prompts = function(session)
                        return {{ name = "prompt_" .. session }}
                    end,
                    get_prompt = function(name, _, context)
                        if name ~= "prompt_" .. context.session_uuid then
                            return nil, "Prompt not available for this session: " .. name
                        end
                        return { description = context.session_name, messages = {} }, nil
                    end,
                    list_resource_templates = function(session)
                        return {{ uriTemplate = "botster://" .. session }}
                    end,
                    read_resource = function(uri, context, cb)
                        cb({{ uri = uri, text = context.repo }}, nil)
                    end,
                }
            end
            "#,
        )
        .exec()
        .expect("stub lib.mcp");
    }

    fn dispatch_reply(lua: &mlua::Lua, caller: &McpCaller, method: &str, params: Value) -> Value {
        let (tx, rx) = oneshot::channel();
        dispatch_lua_mcp(lua, caller, method, params, McpHttpReplyTx(tx));
        let reply = rx.blocking_recv().expect("dispatch reply");
        reply
            .result
            .unwrap_or_else(|| reply.error.unwrap_or(json!({})))
    }

    #[test]
    fn dispatch_lua_mcp_rebuilds_manifest_context_and_scopes_prompts() {
        let lua = mlua::Lua::new();
        install_mcp_stub(&lua);
        let alice = McpCaller::new("sess-alice", "hub-stale");
        let tools = dispatch_reply(&lua, &alice, "tools/list", json!({}));
        assert_eq!(tools["tools"][0]["name"], "whoami_sess-alice");

        let called = dispatch_reply(
            &lua,
            &alice,
            "tools/call",
            json!({ "name": "whoami_sess-alice", "arguments": {} }),
        );
        let text = called["content"][0]["text"].as_str().unwrap();
        assert_eq!(text, "from-manifest|org/repo|main|/tmp/wt");

        let prompts = dispatch_reply(&lua, &alice, "prompts/list", json!({}));
        assert_eq!(prompts["prompts"][0]["name"], "prompt_sess-alice");
        assert!(!prompts.to_string().contains("sess-bob"));

        let stolen = dispatch_reply(
            &lua,
            &alice,
            "prompts/get",
            json!({ "name": "prompt_sess-bob", "arguments": {} }),
        );
        assert!(
            stolen["message"]
                .as_str()
                .unwrap_or_default()
                .contains("not available"),
            "alice must not get bob's prompt: {stolen}"
        );

        let own = dispatch_reply(
            &lua,
            &alice,
            "prompts/get",
            json!({ "name": "prompt_sess-alice", "arguments": {} }),
        );
        assert_eq!(own["description"], "from-manifest");

        let templates = dispatch_reply(&lua, &alice, "resources/templates/list", json!({}));
        assert_eq!(
            templates["resourceTemplates"][0]["uriTemplate"],
            "botster://sess-alice"
        );
    }

    #[test]
    fn stdio_proxy_forwards_json_rpc_and_retries_stale_url() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-proxy", "hub-1"))
                .expect("issue");
            let client = reqwest::Client::new();
            let line = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/list",
                "params": {}
            })
            .to_string();
            let body = proxy_http_line(&client, listener.url(), issued.token.as_str(), &line)
                .await
                .expect("proxy line");
            let parsed: Value = serde_json::from_str(&body).expect("proxy json");
            assert_eq!(parsed["result"]["tools"][0]["name"], "whoami_sess-proxy");

            for _ in 0..16 {
                let first = proxy_http_line(&client, listener.url(), issued.token.as_str(), &line);
                let second = proxy_http_line(&client, listener.url(), issued.token.as_str(), &line);
                let (first, second) = tokio::join!(first, second);
                assert!(first.expect("first").contains("whoami_sess-proxy"));
                assert!(second.expect("second").contains("whoami_sess-proxy"));
            }
        });
    }

    #[test]
    fn persist_drops_revoked_session_and_survives_clear_live() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mcp_callers.json");
        let callers = McpCallerRegistry::new();
        callers.set_persist_path(&path);
        callers.set_url("http://127.0.0.1:9/mcp");
        let issued = callers
            .issue(McpCaller::new("sess-persist", "hub-1"))
            .expect("issue");
        callers.revoke_session("sess-persist");
        callers.clear_live();

        let restored = McpCallerRegistry::new();
        restored.set_persist_path(&path);
        restored.restore_from_disk();
        assert!(restored.authorize(issued.token.as_str()).is_none());

        let live = McpCallerRegistry::new();
        live.set_persist_path(&path);
        live.set_url("http://127.0.0.1:9/mcp");
        let keep = live
            .issue(McpCaller::new("sess-keep", "hub-1"))
            .expect("keep");
        live.clear_live();
        let again = McpCallerRegistry::new();
        again.set_persist_path(&path);
        again.restore_from_disk();
        assert_eq!(
            again
                .authorize(keep.token.as_str())
                .expect("kept")
                .session_uuid,
            "sess-keep"
        );
    }

    #[test]
    fn loopback_origin_helpers() {
        assert!(matches_loopback_origin("http://127.0.0.1"));
        assert!(matches_loopback_origin("http://localhost:1234"));
        assert!(!matches_loopback_origin("https://evil.example"));
        assert_eq!(port_from_url(Some("http://127.0.0.1:4321/mcp")), Some(4321));
    }

    #[test]
    fn valid_token_still_works_after_failed_auth_cap() {
        test_runtime().block_on(async {
            let (callers, _fanout, listener) = start_server().await;
            let issued = callers
                .issue(McpCaller::new("sess-gate", "hub-1"))
                .expect("issue");
            let url = listener.url().to_string();
            for id in 0..AUTH_FAIL_LIMIT {
                let (status, _) = rpc(
                    &url,
                    Some("btcaller_unknown"),
                    u64::from(id),
                    "initialize",
                    json!({}),
                )
                .await;
                assert_eq!(status, StatusCode::UNAUTHORIZED);
            }
            let (status, body) = rpc(
                &url,
                Some(issued.token.as_str()),
                100,
                "initialize",
                json!({}),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
            assert!(body.get("result").is_some());

            let (status, _) =
                rpc(&url, Some("btcaller_unknown"), 101, "initialize", json!({})).await;
            assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);

            let (status, _) = rpc(
                &url,
                Some(issued.token.as_str()),
                102,
                "initialize",
                json!({}),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        });
    }

    #[test]
    fn port_reclaim_failure_revokes_persisted_tokens() {
        test_runtime().block_on(async {
            let occupied =
                tokio::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                    .await
                    .expect("occupy");
            let port = occupied.local_addr().expect("occupied addr").port();
            let dir = tempfile::tempdir().expect("tempdir");
            let path = dir.path().join("mcp_callers.json");
            let callers = McpCallerRegistry::new();
            callers.set_persist_path(&path);
            callers.set_url(format!("http://127.0.0.1:{port}/mcp"));
            let issued = callers
                .issue(McpCaller::new("sess-squat", "hub-1"))
                .expect("issue");
            let fanout = McpHttpFanout::new();
            let listener = bind_listener(callers.clone(), fanout, scoped_dispatcher())
                .await
                .expect("bind fallback");
            assert_ne!(listener.url(), format!("http://127.0.0.1:{port}/mcp"));
            assert!(
                callers.authorize(issued.token.as_str()).is_none(),
                "stale token must be dead before a new URL is published"
            );
            drop(occupied);
        });
    }

    #[test]
    fn restore_drops_expired_credentials() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mcp_callers.json");
        let snapshot = McpCallerSnapshot {
            port: Some(9),
            url: Some("http://127.0.0.1:9/mcp".to_string()),
            callers: vec![PersistedCaller {
                session_uuid: "sess-old".to_string(),
                hub_id: "hub-1".to_string(),
                token: format!("{CALLER_TOKEN_PREFIX}{}", "ab".repeat(32)),
                issued_at: 1,
            }],
        };
        std::fs::write(&path, serde_json::to_vec(&snapshot).expect("json")).expect("write");
        let callers = McpCallerRegistry::new();
        callers.set_persist_path(&path);
        callers.restore_from_disk();
        assert_eq!(callers.caller_count(), 0);
    }

    #[test]
    fn proxy_failure_returns_jsonrpc_error_with_request_id() {
        let body = jsonrpc_error_for_line(
            &json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}).to_string(),
            "connection refused",
        );
        let parsed: Value = serde_json::from_str(&body).expect("json");
        assert_eq!(parsed["id"], 7);
        assert_eq!(parsed["error"]["code"], -32603);
        assert!(parsed["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("connection refused"));
    }
}
