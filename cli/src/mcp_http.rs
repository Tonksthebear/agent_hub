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
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use mlua::LuaSerdeExt;
use rand::RngCore;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

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

/// How long initialize and list methods wait for the Hub event loop.
const HUB_QUICK_TIMEOUT: Duration = Duration::from_secs(15);

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

    fn notification_method(self) -> &'static str {
        match self {
            Self::Tools => "notifications/tools/list_changed",
            Self::Prompts => "notifications/prompts/list_changed",
            Self::Resources => "notifications/resources/list_changed",
        }
    }
}

/// In-memory caller credentials for the live Hub process.
///
/// Tokens live only in this process. Hub shutdown drops them. Session close
/// revokes the matching token. Re-issue for the same session returns the live
/// token so env rebuild does not rotate a still-valid caller.
#[derive(Clone, Debug)]
pub struct McpCallerRegistry {
    inner: Arc<Mutex<CallerState>>,
}

#[derive(Default)]
struct CallerState {
    url: Option<String>,
    by_token: HashMap<String, McpCaller>,
    by_session: HashMap<String, String>,
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

    /// Publish the shared listener URL that later `issue` calls return.
    pub fn set_url(&self, url: impl Into<String>) {
        self.inner
            .lock()
            .expect("McpCallerRegistry mutex poisoned")
            .url = Some(url.into());
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
    }

    /// Drop every credential. Used on Hub shutdown.
    pub fn revoke_all(&self) {
        let mut state = self.inner.lock().expect("McpCallerRegistry mutex poisoned");
        state.by_token.clear();
        state.by_session.clear();
        state.url = None;
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
    #[cfg_attr(not(test), allow(dead_code))]
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
    let listener = TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
        .await
        .context("bind shared MCP HTTP listener")?;
    let addr = listener
        .local_addr()
        .context("read shared MCP HTTP listen address")?;
    let url = format!("http://{addr}{MCP_HTTP_PATH}");
    callers.set_url(url.clone());
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let state = Arc::new(HttpState {
        callers,
        fanout,
        dispatch,
    });

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else {
                        break;
                    };
                    let state = Arc::clone(&state);
                    tokio::spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = service_fn(move |request| {
                            let state = Arc::clone(&state);
                            async move { handle_request(state, request).await }
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
}

async fn handle_request(
    state: Arc<HttpState>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(match handle_request_inner(&state, request).await {
        Ok(response) => response,
        Err(response) => response,
    })
}

async fn handle_request_inner(
    state: &HttpState,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Response<Full<Bytes>>> {
    if request.uri().path() != MCP_HTTP_PATH {
        return Err(http_error(StatusCode::NOT_FOUND, "not found"));
    }

    let Some(token) = extract_bearer(request.headers()) else {
        return Err(http_error(
            StatusCode::UNAUTHORIZED,
            "missing or invalid caller credential",
        ));
    };
    let Some(caller) = state.callers.authorize(&token) else {
        return Err(http_error(
            StatusCode::UNAUTHORIZED,
            "stale or unknown caller credential",
        ));
    };

    match *request.method() {
        Method::POST => handle_post(state, caller, request).await,
        Method::GET => handle_sse_get(state, request),
        Method::DELETE => Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Full::new(Bytes::new()))
            .expect("empty response")),
        _ => Err(http_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        )),
    }
}

async fn handle_post(
    state: &HttpState,
    caller: McpCaller,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Response<Full<Bytes>>> {
    let body = Limited::new(request.into_body(), BODY_LIMIT)
        .collect()
        .await
        .map_err(|_| http_error(StatusCode::PAYLOAD_TOO_LARGE, "request body too large"))?
        .to_bytes();
    let payload: Value = serde_json::from_slice(&body)
        .map_err(|_| http_error(StatusCode::BAD_REQUEST, "invalid JSON-RPC body"))?;

    if payload.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(jsonrpc_error(None, -32600, "invalid JSON-RPC version"));
    }

    let id = payload.get("id").cloned();
    let method = payload
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method.is_empty() {
        return Err(jsonrpc_error(id, -32600, "missing method"));
    }
    if id.is_none() {
        return Ok(empty_accepted());
    }

    let params = payload.get("params").cloned().unwrap_or(json!({}));
    if method == "initialize" {
        return Ok(jsonrpc_result(id, initialize_result()));
    }
    if method == "ping" {
        return Ok(jsonrpc_result(id, json!({})));
    }

    let reply = dispatch_method(state, caller, method, params).await;
    match reply {
        Ok(McpHttpReply {
            result: Some(result),
            ..
        }) => Ok(jsonrpc_result(id, result)),
        Ok(McpHttpReply {
            error: Some(error), ..
        }) => Ok(jsonrpc_error_object(id, error)),
        Ok(_) => Err(jsonrpc_error(id, -32603, "empty MCP reply")),
        Err(message) => Err(jsonrpc_error(id, -32603, &message)),
    }
}

fn handle_sse_get(
    state: &HttpState,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Response<Full<Bytes>>> {
    let accept = request
        .headers()
        .get(hyper::header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !accept.contains("text/event-stream") {
        return Err(http_error(
            StatusCode::NOT_ACCEPTABLE,
            "GET /mcp requires text/event-stream",
        ));
    }

    let mut rx = state.fanout.subscribe();
    let mut body = String::from("event: endpoint\ndata: /mcp\n\n");
    while let Ok(kind) = rx.try_recv() {
        body.push_str(&sse_notification(kind));
    }
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "text/event-stream")
        .header("Cache-Control", "no-cache")
        .body(Full::new(Bytes::from(body)))
        .map_err(|_| http_error(StatusCode::INTERNAL_SERVER_ERROR, "sse response failed"))
}

async fn dispatch_method(
    state: &HttpState,
    caller: McpCaller,
    method: &str,
    params: Value,
) -> Result<McpHttpReply, String> {
    match &state.dispatch {
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
            let timeout = if method.starts_with("tools/call")
                || method == "prompts/get"
                || method == "resources/read"
            {
                HUB_DISPATCH_TIMEOUT
            } else {
                HUB_QUICK_TIMEOUT
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

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": "2025-03-26",
        "capabilities": {
            "tools": { "listChanged": true },
            "prompts": { "listChanged": true },
            "resources": { "listChanged": true }
        },
        "serverInfo": {
            "name": "botster-hub",
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn sse_notification(kind: McpListKind) -> String {
    format!(
        "event: message\ndata: {}\n\n",
        json!({
            "jsonrpc": "2.0",
            "method": kind.notification_method()
        })
    )
}

fn empty_accepted() -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::ACCEPTED)
        .body(Full::new(Bytes::new()))
        .expect("empty accepted response")
}

fn http_error(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Full::new(Bytes::from(
            json!({ "error": message }).to_string(),
        )))
        .expect("error response")
}

fn jsonrpc_result(id: Option<Value>, result: Value) -> Response<Full<Bytes>> {
    json_response(json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "result": result
    }))
}

fn jsonrpc_error(id: Option<Value>, code: i64, message: &str) -> Response<Full<Bytes>> {
    jsonrpc_error_object(id, json!({ "code": code, "message": message }))
}

fn jsonrpc_error_object(id: Option<Value>, error: Value) -> Response<Full<Bytes>> {
    json_response(json!({
        "jsonrpc": "2.0",
        "id": id.unwrap_or(Value::Null),
        "error": error
    }))
}

fn json_response(value: Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
        .body(Full::new(Bytes::from(value.to_string())))
        .expect("json response")
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
    let context = crate::lua::primitives::json::json_to_lua(
        lua,
        &Value::Object(
            caller
                .lua_context()
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
                .call(())
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
            match get_prompt.call::<(mlua::Value, mlua::Value)>((name, args)) {
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
                .call(())
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

/// Run a stdio JSON-RPC proxy to the shared HTTP server.
///
/// This is the short migration path for clients that still launch
/// `botster mcp-serve`. New clients should use `BOTSTER_MCP_URL` directly.
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

    let client = reqwest::Client::new();
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let response = client
            .post(url)
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(line)
            .send()
            .await
            .with_context(|| format!("proxy MCP POST to {url}"))?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() && body.trim().is_empty() {
            anyhow::bail!("shared MCP HTTP proxy received HTTP {status}");
        }
        stdout.write_all(body.as_bytes()).await?;
        if !body.ends_with('\n') {
            stdout.write_all(b"\n").await?;
        }
        stdout.flush().await?;
    }
    Ok(())
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
                    "description": format!("caller {}", caller.session_uuid),
                    "inputSchema": { "type": "object" }
                }]
            })),
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
        let mut request = reqwest::Client::new().post(url).json(&json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        }));
        if let Some(token) = token {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        }
        let response = request.send().await.expect("http post");
        let status = response.status();
        let body = response.json::<Value>().await.unwrap_or(json!({}));
        (status, body)
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
    fn reconnect_reuses_the_same_caller_credential() {
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
    fn list_changed_notification_has_no_caller_payload() {
        let tools = sse_notification(McpListKind::Tools);
        assert!(tools.contains("notifications/tools/list_changed"));
        assert!(!tools.contains("session_uuid"));
        assert!(!tools.contains("hub_id"));
        assert!(!tools.contains("btcaller_"));
    }
}
