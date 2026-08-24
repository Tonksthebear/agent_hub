//! MCP gateway — rmcp SDK-based stdio server bridging to the hub socket.
//!
//! Uses the official Rust MCP SDK (`rmcp`) for JSON-RPC framing, method
//! dispatch, and id correlation. Routes tool/prompt requests through the
//! hub socket `mcp` channel to Lua.
//!
//! Launched by an MCP client as: `botster mcp-serve`

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use anyhow::Result;
use rmcp::model::*;
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use rmcp::{ServerHandler, ServiceExt};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::socket::framing::{Frame, FrameDecoder};

/// Subscription ID used for the MCP channel on the hub socket.
const SUB_ID: &str = "mcp_bridge";

/// Base delay between socket connect retries, in milliseconds.
const CONNECT_RETRY_BASE_MS: u64 = 300;

/// Fixed delay between reconnect attempts in milliseconds.
const RECONNECT_RETRY_MS: u64 = 1_000;

/// Timeout for individual hub requests (tools/list, tools/call, etc.).
/// Tool calls can run indefinitely (agents executing commands), so this is
/// intentionally very generous. The legacy bridge had no timeout at all.
const HUB_REQUEST_TIMEOUT: Duration = Duration::from_secs(86_400);

/// Diagnostic tool exposed when the MCP server is launched outside Botster.
const DISCONNECTED_STATUS_TOOL: &str = "botster_status";

// ============================================================================
// Hub Bridge
// ============================================================================

/// Notification types forwarded from hub to MCP client.
enum HubNotification {
    ToolsListChanged,
    PromptsListChanged,
    ResourcesListChanged,
}

/// A request sent from the gateway to the connection manager.
struct BridgeRequest {
    /// Correlation key (e.g. "tools_list", "call_42").
    key: String,
    /// Pre-encoded frame bytes to send to hub.
    frame_data: Vec<u8>,
    /// Channel to receive the hub's response.
    response_tx: oneshot::Sender<Result<Value, String>>,
}

#[derive(Debug)]
struct BridgeConnectionState {
    socket_path: String,
    connected: AtomicBool,
    last_error: Mutex<Option<String>>,
}

impl BridgeConnectionState {
    fn pending(socket_path: String) -> Arc<Self> {
        Arc::new(Self {
            socket_path,
            connected: AtomicBool::new(false),
            last_error: Mutex::new(None),
        })
    }

    fn permanent_disconnected(reason: String) -> Arc<Self> {
        Arc::new(Self {
            socket_path: String::new(),
            connected: AtomicBool::new(false),
            last_error: Mutex::new(Some(reason)),
        })
    }

    fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    async fn set_connected(&self) -> bool {
        let was_connected = self.connected.swap(true, Ordering::AcqRel);
        *self.last_error.lock().await = None;
        !was_connected
    }

    async fn set_disconnected(&self, reason: String) -> bool {
        let was_connected = self.connected.swap(false, Ordering::AcqRel);
        *self.last_error.lock().await = Some(reason);
        was_connected
    }

    async fn unavailable_message(&self) -> String {
        let reason = self
            .last_error
            .lock()
            .await
            .clone()
            .unwrap_or_else(|| "hub connection is not ready yet".to_string());

        if self.socket_path.is_empty() {
            return reason;
        }

        format!(
            "Botster hub tools are temporarily unavailable: {reason}. \
The MCP bridge is still running and retrying hub socket {}.",
            self.socket_path
        )
    }
}

/// Why a hub session ended.
enum SessionExit {
    /// The gateway dropped its request channel — process is shutting down.
    RequestChannelClosed,
    /// Hub socket disconnected — should reconnect.
    HubDisconnected,
}

/// Route a hub JSON message to the appropriate pending request or notification channel.
fn handle_hub_message(
    msg: Value,
    pending: &mut HashMap<String, oneshot::Sender<Result<Value, String>>>,
    notification_tx: &mpsc::UnboundedSender<HubNotification>,
) {
    let msg_type = msg.get("type").and_then(|t| t.as_str()).unwrap_or("");

    match msg_type {
        "subscribed" => {
            log::info!("[mcp-gateway] Subscribed to hub MCP channel");
        }

        "tools_list" => {
            if let Some(tx) = pending.remove("tools_list") {
                let _ = tx.send(Ok(msg));
            }
        }

        "tool_result" => {
            let call_id = msg.get("call_id").and_then(|c| c.as_str()).unwrap_or("");
            if !call_id.is_empty() {
                if let Some(tx) = pending.remove(call_id) {
                    let _ = tx.send(Ok(msg));
                }
            }
        }

        "prompts_list" => {
            if let Some(tx) = pending.remove("prompts_list") {
                let _ = tx.send(Ok(msg));
            }
        }

        "prompt_result" => {
            let call_id = msg.get("call_id").and_then(|c| c.as_str()).unwrap_or("");
            if !call_id.is_empty() {
                if let Some(tx) = pending.remove(call_id) {
                    let _ = tx.send(Ok(msg));
                }
            }
        }

        "tools_list_changed" => {
            let _ = notification_tx.send(HubNotification::ToolsListChanged);
        }

        "prompts_list_changed" => {
            let _ = notification_tx.send(HubNotification::PromptsListChanged);
        }

        "resource_templates_list" => {
            if let Some(tx) = pending.remove("resource_templates_list") {
                let _ = tx.send(Ok(msg));
            }
        }

        "resource_result" => {
            let call_id = msg.get("call_id").and_then(|c| c.as_str()).unwrap_or("");
            if !call_id.is_empty() {
                if let Some(tx) = pending.remove(call_id) {
                    let _ = tx.send(Ok(msg));
                }
            }
        }

        "resources_list_changed" => {
            let _ = notification_tx.send(HubNotification::ResourcesListChanged);
        }

        _ => {} // Ignore other hub messages
    }
}

/// Run a single hub session over an already-connected stream.
///
/// Processes gateway requests and hub messages until the hub disconnects
/// or the gateway shuts down.
async fn run_hub_session(
    stream: tokio::net::UnixStream,
    request_rx: &mut mpsc::UnboundedReceiver<BridgeRequest>,
    notification_tx: &mpsc::UnboundedSender<HubNotification>,
    caller_context: &BTreeMap<String, String>,
) -> SessionExit {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (read_half, write_half) = stream.into_split();

    // Hub socket writer channel
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    // Hub socket reader channel
    let (hub_msg_tx, mut hub_msg_rx) = mpsc::unbounded_channel::<Value>();

    // Subscribe to the MCP channel with caller context
    let sub_frame = Frame::Json(json!({
        "type": "subscribe",
        "channel": "mcp",
        "subscriptionId": SUB_ID,
        "params": { "context": caller_context }
    }));
    // frame_rx is still in scope — send cannot fail
    frame_tx
        .send(sub_frame.encode())
        .expect("channel alive: write task not yet spawned");

    // Spawn hub socket writer
    let write_task = tokio::spawn(async move {
        let mut writer = tokio::io::BufWriter::new(write_half);
        while let Some(data) = frame_rx.recv().await {
            if writer.write_all(&data).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    });

    // Spawn hub socket reader
    let read_task = tokio::spawn(async move {
        let mut reader = tokio::io::BufReader::new(read_half);
        let mut decoder = FrameDecoder::new();
        let mut buf = [0u8; 8192];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => match decoder.feed(&buf[..n]) {
                    Ok(frames) => {
                        for frame in frames {
                            if let Frame::Json(v) = frame {
                                if hub_msg_tx.send(v).is_err() {
                                    return;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("[mcp-gateway] Frame decode error: {e}");
                        break;
                    }
                },
            }
        }
    });

    // Pending requests: correlation key → oneshot sender
    let mut pending: HashMap<String, oneshot::Sender<Result<Value, String>>> = HashMap::new();

    let exit = loop {
        tokio::select! {
            req = request_rx.recv() => {
                let Some(req) = req else {
                    break SessionExit::RequestChannelClosed;
                };

                let key = req.key;
                pending.insert(key.clone(), req.response_tx);

                if frame_tx.send(req.frame_data).is_err() {
                    // Hub writer dead — fail this request and break
                    if let Some(tx) = pending.remove(&key) {
                        let _ = tx.send(Err("hub disconnected".to_string()));
                    }
                    break SessionExit::HubDisconnected;
                }
            }

            msg = hub_msg_rx.recv() => {
                let Some(msg) = msg else {
                    break SessionExit::HubDisconnected;
                };
                handle_hub_message(msg, &mut pending, notification_tx);
            }
        }
    };

    write_task.abort();
    read_task.abort();

    // Fail all pending requests on disconnect
    for (_, tx) in pending.drain() {
        let _ = tx.send(Err("hub disconnected — reconnecting".to_string()));
    }

    exit
}

fn notify_capability_lists_changed(notification_tx: &mpsc::UnboundedSender<HubNotification>) {
    let _ = notification_tx.send(HubNotification::ToolsListChanged);
    let _ = notification_tx.send(HubNotification::PromptsListChanged);
    let _ = notification_tx.send(HubNotification::ResourcesListChanged);
}

/// Connection manager: runs the connect → session → reconnect loop.
///
/// Lives for the lifetime of the MCP process. Exits when the gateway
/// drops its request channel (process shutting down). The loop does not
/// exhaust retries because MCP stdio must remain available while the hub
/// repairs or recreates its socket path.
async fn connection_manager(
    socket_path: String,
    mut request_rx: mpsc::UnboundedReceiver<BridgeRequest>,
    notification_tx: mpsc::UnboundedSender<HubNotification>,
    caller_context: BTreeMap<String, String>,
    connection_state: Arc<BridgeConnectionState>,
) {
    let mut attempts: u32 = 0;
    loop {
        if request_rx.is_closed() {
            return;
        }

        let stream = match tokio::net::UnixStream::connect(&socket_path).await {
            Ok(s) => {
                attempts = 0;
                log::info!("[mcp-gateway] Connected to hub at {socket_path}");
                if connection_state.set_connected().await {
                    notify_capability_lists_changed(&notification_tx);
                }
                s
            }
            Err(e) => {
                attempts = attempts.saturating_add(1);
                let reason = format!("hub socket {socket_path} is not reachable: {e}");
                connection_state.set_disconnected(reason).await;
                if attempts == 1 || attempts % 30 == 0 {
                    log::warn!(
                        "[mcp-gateway] hub connect attempt {attempts} failed; retrying: {e}"
                    );
                }
                tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_BASE_MS)).await;
                continue;
            }
        };

        let exit =
            run_hub_session(stream, &mut request_rx, &notification_tx, &caller_context).await;

        if matches!(exit, SessionExit::RequestChannelClosed) {
            return;
        }

        if connection_state
            .set_disconnected(format!("hub socket {socket_path} disconnected"))
            .await
        {
            notify_capability_lists_changed(&notification_tx);
        }

        tokio::time::sleep(Duration::from_millis(RECONNECT_RETRY_MS)).await;

        while let Ok(req) = request_rx.try_recv() {
            let _ = req.response_tx.send(Err(
                "hub disconnected before the request could be sent; retrying".to_string(),
            ));
        }
    }
}

// ============================================================================
// MCP Gateway (ServerHandler)
// ============================================================================

/// MCP gateway server backed by the hub socket.
///
/// Implements `rmcp::ServerHandler` — the SDK handles JSON-RPC on stdio,
/// this struct routes requests to Lua via the hub socket protocol.
#[derive(Debug)]
pub struct McpGateway {
    /// Channel to send requests to the connection manager.
    request_tx: mpsc::UnboundedSender<BridgeRequest>,
    /// Counter for generating unique call IDs.
    call_counter: Mutex<u64>,
    /// Notification receiver — taken once in `on_initialized`.
    notification_rx: Mutex<Option<mpsc::UnboundedReceiver<HubNotification>>>,
    /// Shared hub connection state used to fail requests quickly while retrying.
    connection_state: Arc<BridgeConnectionState>,
    /// Human-readable reason this server is running without a hub connection.
    disconnected_reason: Option<String>,
}

impl McpGateway {
    /// Send a request to the hub and wait for the response.
    async fn hub_request(
        &self,
        method: &str,
        key: String,
        frame: Frame,
    ) -> Result<Value, ErrorData> {
        if let Some(reason) = &self.disconnected_reason {
            return Err(ErrorData::invalid_params(reason.clone(), None));
        }

        if !self.connection_state.is_connected() {
            return Err(ErrorData::internal_error(
                self.connection_state.unavailable_message().await,
                None,
            ));
        }

        let start = Instant::now();
        let (tx, rx) = oneshot::channel();

        log::info!("[mcp-gateway] method={method} call_id={key} sending");

        let req = BridgeRequest {
            key: key.clone(),
            frame_data: frame.encode(),
            response_tx: tx,
        };

        self.request_tx.send(req).map_err(|_| {
            ErrorData::internal_error("MCP gateway shutting down".to_string(), None)
        })?;

        let result = tokio::time::timeout(HUB_REQUEST_TIMEOUT, rx)
            .await
            .map_err(|_| {
                log::error!(
                    "[mcp-gateway] method={method} call_id={key} timeout after {}ms",
                    start.elapsed().as_millis()
                );
                ErrorData::internal_error(
                    format!(
                        "Hub request timed out after {}s",
                        HUB_REQUEST_TIMEOUT.as_secs()
                    ),
                    None,
                )
            })?
            .map_err(|_| ErrorData::internal_error("Hub connection lost".to_string(), None))?
            .map_err(|e| ErrorData::internal_error(e, None))?;

        log::info!(
            "[mcp-gateway] method={method} call_id={key} duration_ms={}",
            start.elapsed().as_millis()
        );

        Ok(result)
    }

    /// Allocate a unique call ID with the given prefix.
    async fn next_call_id(&self, prefix: &str) -> String {
        let mut counter = self.call_counter.lock().await;
        *counter += 1;
        format!("{prefix}_{}", *counter)
    }
}

/// Convert a hub tool JSON object to an rmcp `Tool`.
///
/// Hub sends: `{ name, description, input_schema: { type: "object", ... } }`
/// rmcp needs: `Tool { name, description, input_schema: Arc<JsonObject> }`
fn hub_tool_to_mcp(t: &Value) -> Option<Tool> {
    let name = t.get("name")?.as_str()?;
    let description = t.get("description").and_then(|d| d.as_str()).unwrap_or("");
    let schema = t
        .get("input_schema")
        .and_then(|s| s.as_object())
        .cloned()
        .unwrap_or_else(|| {
            serde_json::Map::from_iter([("type".to_string(), Value::String("object".to_string()))])
        });

    Some(Tool::new(name.to_string(), description.to_string(), schema))
}

fn disconnected_status_tool() -> Tool {
    Tool::new(
        DISCONNECTED_STATUS_TOOL.to_string(),
        "Explain why Botster tools are unavailable in this session.".to_string(),
        serde_json::Map::from_iter([("type".to_string(), Value::String("object".to_string()))]),
    )
}

fn disconnected_status_message(reason: &str) -> String {
    format!(
        "{reason}. This agent was not launched inside a Botster-managed session, \
so Botster hub tools, prompts, resources, messaging, and orchestration are unavailable. \
Start the agent from Botster so BOTSTER_SESSION_UUID is present."
    )
}

fn reconnecting_status_message(reason: &str) -> String {
    format!(
        "{reason} Tool, prompt, resource, messaging, and orchestration capabilities \
will appear automatically after the bridge reconnects."
    )
}

/// Convert a hub prompt JSON object to an rmcp `Prompt`.
///
/// Hub sends: `{ name, description, arguments: [{ name, description, required }] }`
fn hub_prompt_to_mcp(p: &Value) -> Option<Prompt> {
    let name = p.get("name")?.as_str()?.to_string();
    let description = p
        .get("description")
        .and_then(|d| d.as_str())
        .map(String::from);

    let arguments = p.get("arguments").and_then(|a| a.as_array()).map(|args| {
        args.iter()
            .filter_map(|arg| {
                let arg_name = arg.get("name")?.as_str()?.to_string();
                let mut pa = PromptArgument::new(arg_name);
                if let Some(desc) = arg.get("description").and_then(|d| d.as_str()) {
                    pa = pa.with_description(desc);
                }
                if let Some(req) = arg.get("required").and_then(|r| r.as_bool()) {
                    pa = pa.with_required(req);
                }
                Some(pa)
            })
            .collect::<Vec<_>>()
    });

    Some(Prompt::new(name, description, arguments))
}

/// Convert hub content array to rmcp `Content` items.
///
/// Hub content uses the same MCP wire shape (`{ type: "text", text: "..." }`,
/// `{ type: "image", data: "...", mimeType: "..." }`, etc.), so we deserialize
/// directly via serde to preserve all content types — not just text.
fn hub_content_to_mcp(content: &Value) -> Vec<ContentBlock> {
    serde_json::from_value::<Vec<ContentBlock>>(content.clone()).unwrap_or_default()
}

/// Convert hub messages array to rmcp `PromptMessage` items.
///
/// Hub messages use the same MCP wire shape, so we deserialize directly
/// to preserve all content types (text, image, resource, etc.).
fn hub_messages_to_mcp(messages: &Value) -> Vec<PromptMessage> {
    serde_json::from_value::<Vec<PromptMessage>>(messages.clone()).unwrap_or_default()
}

/// Convert a hub resource template JSON object to an rmcp `ResourceTemplate`.
///
/// Hub sends: `{ uriTemplate, name, description?, mimeType? }`
fn hub_resource_template_to_mcp(t: &Value) -> Option<ResourceTemplate> {
    let uri_template = t.get("uriTemplate")?.as_str()?;
    let name = t.get("name")?.as_str()?;
    let mut raw = ResourceTemplate::new(uri_template, name);
    if let Some(desc) = t.get("description").and_then(|d| d.as_str()) {
        raw = raw.with_description(desc);
    }
    if let Some(mime) = t.get("mimeType").and_then(|m| m.as_str()) {
        raw = raw.with_mime_type(mime);
    }
    Some(raw)
}

impl ServerHandler for McpGateway {
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
            let peer = context.peer.clone();

            // Take the notification receiver and spawn a forwarder task
            let rx = self.notification_rx.lock().await.take();
            if let Some(mut rx) = rx {
                tokio::spawn(async move {
                    while let Some(notif) = rx.recv().await {
                        let result = match notif {
                            HubNotification::ToolsListChanged => {
                                log::info!("[mcp-gateway] Forwarding tools/list_changed");
                                peer.notify_tool_list_changed().await
                            }
                            HubNotification::PromptsListChanged => {
                                log::info!("[mcp-gateway] Forwarding prompts/list_changed");
                                peer.notify_prompt_list_changed().await
                            }
                            HubNotification::ResourcesListChanged => {
                                log::info!("[mcp-gateway] Forwarding resources/list_changed");
                                peer.notify_resource_list_changed().await
                            }
                        };
                        if let Err(e) = result {
                            log::warn!("[mcp-gateway] Failed to send notification: {e}");
                        }
                    }
                });
            }
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, ErrorData>> + Send + '_ {
        async move {
            if self.disconnected_reason.is_some() {
                return Ok(ListToolsResult::with_all_items(vec![
                    disconnected_status_tool(),
                ]));
            }
            if !self.connection_state.is_connected() {
                return Ok(ListToolsResult::with_all_items(vec![
                    disconnected_status_tool(),
                ]));
            }

            let msg = self
                .hub_request(
                    "tools/list",
                    "tools_list".to_string(),
                    Frame::Json(json!({
                        "subscriptionId": SUB_ID,
                        "type": "tools_list"
                    })),
                )
                .await?;

            let tools = msg
                .get("tools")
                .and_then(|t| t.as_array())
                .map(|arr| arr.iter().filter_map(hub_tool_to_mcp).collect())
                .unwrap_or_default();

            Ok(ListToolsResult::with_all_items(tools))
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResponse, ErrorData>> + Send + '_ {
        async move {
            if let Some(reason) = &self.disconnected_reason {
                if request.name.as_ref() == DISCONNECTED_STATUS_TOOL {
                    return Ok(CallToolResult::success(vec![ContentBlock::text(
                        disconnected_status_message(reason),
                    )])
                    .into());
                }

                return Err(ErrorData::invalid_params(
                    disconnected_status_message(reason),
                    None,
                ));
            }
            if !self.connection_state.is_connected() {
                let message =
                    reconnecting_status_message(&self.connection_state.unavailable_message().await);
                if request.name.as_ref() == DISCONNECTED_STATUS_TOOL {
                    return Ok(CallToolResult::success(vec![ContentBlock::text(message)]).into());
                }

                return Err(ErrorData::internal_error(message, None));
            }

            let call_id = self.next_call_id("call").await;
            let tool_name = &request.name;
            let arguments = request
                .arguments
                .as_ref()
                .map_or_else(|| json!({}), |m| Value::Object(m.clone()));

            let msg = self
                .hub_request(
                    &format!("tools/call[{tool_name}]"),
                    call_id.clone(),
                    Frame::Json(json!({
                        "subscriptionId": SUB_ID,
                        "type": "tool_call",
                        "call_id": call_id,
                        "name": tool_name,
                        "arguments": arguments
                    })),
                )
                .await?;

            let content = hub_content_to_mcp(msg.get("content").unwrap_or(&json!([])));
            let is_error = msg
                .get("is_error")
                .and_then(|e| e.as_bool())
                .unwrap_or(false);

            let mut result = CallToolResult::success(content);
            if is_error {
                result.is_error = Some(true);
            }
            Ok(result.into())
        }
    }

    fn list_prompts(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListPromptsResult, ErrorData>> + Send + '_ {
        async move {
            if self.disconnected_reason.is_some() {
                return Ok(ListPromptsResult::with_all_items(Vec::<Prompt>::new()));
            }
            if !self.connection_state.is_connected() {
                return Ok(ListPromptsResult::with_all_items(Vec::<Prompt>::new()));
            }

            let msg = self
                .hub_request(
                    "prompts/list",
                    "prompts_list".to_string(),
                    Frame::Json(json!({
                        "subscriptionId": SUB_ID,
                        "type": "prompts_list"
                    })),
                )
                .await?;

            let prompts = msg
                .get("prompts")
                .and_then(|p| p.as_array())
                .map(|arr| arr.iter().filter_map(hub_prompt_to_mcp).collect())
                .unwrap_or_default();

            Ok(ListPromptsResult::with_all_items(prompts))
        }
    }

    fn get_prompt(
        &self,
        request: GetPromptRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<GetPromptResponse, ErrorData>> + Send + '_ {
        async move {
            if let Some(reason) = &self.disconnected_reason {
                return Err(ErrorData::invalid_params(reason.clone(), None));
            }
            if !self.connection_state.is_connected() {
                return Err(ErrorData::internal_error(
                    reconnecting_status_message(&self.connection_state.unavailable_message().await),
                    None,
                ));
            }

            let call_id = self.next_call_id("prompt_get").await;
            let prompt_name = &request.name;
            let arguments = request
                .arguments
                .as_ref()
                .map_or_else(|| json!({}), |m| Value::Object(m.clone()));

            let msg = self
                .hub_request(
                    &format!("prompts/get[{prompt_name}]"),
                    call_id.clone(),
                    Frame::Json(json!({
                        "subscriptionId": SUB_ID,
                        "type": "prompt_get",
                        "call_id": call_id,
                        "name": prompt_name,
                        "arguments": arguments
                    })),
                )
                .await?;

            let is_error = msg
                .get("is_error")
                .and_then(|e| e.as_bool())
                .unwrap_or(false);

            if is_error {
                let empty = json!([]);
                let content = msg.get("content").unwrap_or(&empty);
                let err_text = content
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|c| c.get("text"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("prompt error");
                return Err(ErrorData::internal_error(err_text.to_string(), None));
            }

            let messages = hub_messages_to_mcp(msg.get("messages").unwrap_or(&json!([])));
            let mut result = GetPromptResult::new(messages);
            if let Some(desc) = msg.get("description").and_then(|d| d.as_str()) {
                result = result.with_description(desc);
            }

            Ok(result.into())
        }
    }

    fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourcesResult, ErrorData>> + Send + '_ {
        std::future::ready(Ok(ListResourcesResult::with_all_items(Vec::new())))
    }

    fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListResourceTemplatesResult, ErrorData>> + Send + '_
    {
        async move {
            if self.disconnected_reason.is_some() {
                return Ok(ListResourceTemplatesResult::with_all_items(Vec::new()));
            }
            if !self.connection_state.is_connected() {
                return Ok(ListResourceTemplatesResult::with_all_items(Vec::new()));
            }

            let msg = self
                .hub_request(
                    "resources/templates/list",
                    "resource_templates_list".to_string(),
                    Frame::Json(json!({
                        "subscriptionId": SUB_ID,
                        "type": "resource_templates_list"
                    })),
                )
                .await?;

            let templates = msg
                .get("resourceTemplates")
                .and_then(|t| t.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(hub_resource_template_to_mcp)
                        .collect()
                })
                .unwrap_or_default();

            Ok(ListResourceTemplatesResult::with_all_items(templates))
        }
    }

    fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ReadResourceResponse, ErrorData>> + Send + '_
    {
        async move {
            if let Some(reason) = &self.disconnected_reason {
                return Err(ErrorData::resource_not_found(reason.clone(), None));
            }
            if !self.connection_state.is_connected() {
                return Err(ErrorData::resource_not_found(
                    reconnecting_status_message(&self.connection_state.unavailable_message().await),
                    None,
                ));
            }

            let call_id = self.next_call_id("resource_read").await;
            let uri = request.uri.as_str();

            let msg = self
                .hub_request(
                    &format!("resources/read[{uri}]"),
                    call_id.clone(),
                    Frame::Json(json!({
                        "subscriptionId": SUB_ID,
                        "type": "resource_read",
                        "call_id": call_id,
                        "uri": uri
                    })),
                )
                .await?;

            let is_error = msg
                .get("is_error")
                .and_then(|e| e.as_bool())
                .unwrap_or(false);

            if is_error {
                let empty = json!([]);
                let content = msg.get("content").unwrap_or(&empty);
                let err_text = content
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|c| c.get("text"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("resource read error");
                return Err(ErrorData::resource_not_found(err_text.to_string(), None));
            }

            let contents: Vec<ResourceContents> = msg
                .get("contents")
                .and_then(|c| c.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|item| {
                            let item_uri = item.get("uri").and_then(|u| u.as_str()).unwrap_or(uri);

                            if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                                let mut rc = ResourceContents::text(text, item_uri);
                                if let Some(mime) = item.get("mimeType").and_then(|m| m.as_str()) {
                                    rc = rc.with_mime_type(mime);
                                }
                                Some(rc)
                            } else if let Some(blob) = item.get("blob").and_then(|b| b.as_str()) {
                                let mut rc = ResourceContents::blob(blob, item_uri);
                                if let Some(mime) = item.get("mimeType").and_then(|m| m.as_str()) {
                                    rc = rc.with_mime_type(mime);
                                }
                                Some(rc)
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            Ok(ReadResourceResult::new(contents).into())
        }
    }
}

// ============================================================================
// Entry Point
// ============================================================================

/// Run the MCP gateway synchronously, blocking on a tokio runtime.
pub fn run(socket_path: &str) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_async(socket_path))
}

/// Run the MCP gateway without a Botster hub connection.
///
/// This mode is used when an agent harness starts the configured Botster MCP
/// server outside a Botster-managed PTY session. The server starts cleanly and
/// advertises a diagnostic status tool instead of making the harness report
/// startup failure.
pub fn run_disconnected(reason: &str) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_disconnected_async(reason.to_string()))
}

/// Async entry point: connect to hub, start rmcp server on stdio.
async fn run_async(socket_path: &str) -> Result<()> {
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let (notification_tx, notification_rx) = mpsc::unbounded_channel();

    let caller_context = crate::commands::context::build();
    let connection_state = BridgeConnectionState::pending(socket_path.to_owned());

    // Spawn the connection manager (owns the hub socket lifecycle)
    let socket_path_owned = socket_path.to_owned();
    let context_clone = caller_context.clone();
    let state_clone = Arc::clone(&connection_state);
    tokio::spawn(async move {
        connection_manager(
            socket_path_owned,
            request_rx,
            notification_tx,
            context_clone,
            state_clone,
        )
        .await;
    });

    let gateway = McpGateway {
        request_tx,
        call_counter: Mutex::new(0),
        notification_rx: Mutex::new(Some(notification_rx)),
        connection_state,
        disconnected_reason: None,
    };

    serve_stdio(gateway).await
}

async fn run_disconnected_async(reason: String) -> Result<()> {
    log::warn!("[mcp-gateway] starting disconnected: {reason}");

    let (request_tx, _request_rx) = mpsc::unbounded_channel();
    let (_notification_tx, notification_rx) = mpsc::unbounded_channel();
    let connection_state = BridgeConnectionState::permanent_disconnected(reason.clone());

    let gateway = McpGateway {
        request_tx,
        call_counter: Mutex::new(0),
        notification_rx: Mutex::new(Some(notification_rx)),
        connection_state,
        disconnected_reason: Some(reason),
    };

    serve_stdio(gateway).await
}

async fn serve_stdio(gateway: McpGateway) -> Result<()> {
    let service = gateway
        .serve(rmcp::transport::stdio())
        .await
        .map_err(|e| anyhow::anyhow!("MCP server initialization failed: {e}"))?;

    // Block until the MCP client disconnects.
    service
        .waiting()
        .await
        .map_err(|e| anyhow::anyhow!("MCP server error: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    /// Create a connected Unix socket pair.
    fn socket_pair() -> (tokio::net::UnixStream, tokio::net::UnixStream) {
        tokio::net::UnixStream::pair().expect("UnixStream::pair")
    }

    fn test_gateway(connection_state: Arc<BridgeConnectionState>) -> McpGateway {
        let (request_tx, _request_rx) = mpsc::unbounded_channel();
        let (_notification_tx, notification_rx) = mpsc::unbounded_channel();

        McpGateway {
            request_tx,
            call_counter: Mutex::new(0),
            notification_rx: Mutex::new(Some(notification_rx)),
            connection_state,
            disconnected_reason: None,
        }
    }

    /// When the hub closes its socket, the session must return `HubDisconnected`.
    #[tokio::test]
    async fn test_hub_eof_returns_hub_disconnected() {
        let (mcp_side, hub_side) = socket_pair();
        let (_req_tx, mut req_rx) = mpsc::unbounded_channel::<BridgeRequest>();
        let (notif_tx, _notif_rx) = mpsc::unbounded_channel::<HubNotification>();
        let context = BTreeMap::new();

        drop(hub_side);

        let exit = run_hub_session(mcp_side, &mut req_rx, &notif_tx, &context).await;
        assert!(
            matches!(exit, SessionExit::HubDisconnected),
            "hub EOF must return HubDisconnected"
        );
    }

    /// When the request channel closes, the session must return `RequestChannelClosed`.
    #[tokio::test]
    async fn test_request_channel_closed() {
        let (mcp_side, _hub_side) = socket_pair();
        let (req_tx, mut req_rx) = mpsc::unbounded_channel::<BridgeRequest>();
        let (notif_tx, _notif_rx) = mpsc::unbounded_channel::<HubNotification>();
        let context = BTreeMap::new();

        drop(req_tx);

        let exit = run_hub_session(mcp_side, &mut req_rx, &notif_tx, &context).await;
        assert!(
            matches!(exit, SessionExit::RequestChannelClosed),
            "request channel close must return RequestChannelClosed"
        );
    }

    /// A subscribe frame is sent at the start of every session.
    #[tokio::test]
    async fn test_subscribe_frame_sent_on_session_start() {
        let (mcp_side, hub_side) = socket_pair();
        let (_req_tx, mut req_rx) = mpsc::unbounded_channel::<BridgeRequest>();
        let (notif_tx, _notif_rx) = mpsc::unbounded_channel::<HubNotification>();
        let context = BTreeMap::new();

        let hub_task = tokio::spawn(async move {
            let mut first_frame: Option<Value> = None;
            let mut reader = tokio::io::BufReader::new(hub_side);
            let mut decoder = FrameDecoder::new();
            let mut buf = [0u8; 4096];
            'read: loop {
                match reader.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if let Ok(frames) = decoder.feed(&buf[..n]) {
                            for frame in frames {
                                if let Frame::Json(v) = frame {
                                    first_frame = Some(v);
                                    break 'read;
                                }
                            }
                        }
                    }
                }
            }
            first_frame
        });

        let exit = run_hub_session(mcp_side, &mut req_rx, &notif_tx, &context).await;
        assert!(matches!(exit, SessionExit::HubDisconnected));

        let frame = hub_task
            .await
            .expect("hub task panicked")
            .expect("hub must receive the subscribe frame");
        assert_eq!(frame["type"], "subscribe");
        assert_eq!(frame["channel"], "mcp");
        assert_eq!(frame["subscriptionId"], SUB_ID);
    }

    /// Request-response correlation: a tools_list request gets the correct response.
    #[tokio::test]
    async fn test_request_response_correlation() {
        let (mcp_side, hub_side) = socket_pair();
        let (req_tx, mut req_rx) = mpsc::unbounded_channel::<BridgeRequest>();
        let (notif_tx, _notif_rx) = mpsc::unbounded_channel::<HubNotification>();
        let context = BTreeMap::new();

        // Hub: read frames, respond to tools_list
        let hub_task = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (read_half, write_half) = hub_side.into_split();
            let mut reader = tokio::io::BufReader::new(read_half);
            let mut writer = tokio::io::BufWriter::new(write_half);
            let mut decoder = FrameDecoder::new();
            let mut buf = [0u8; 4096];

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if let Ok(frames) = decoder.feed(&buf[..n]) {
                            for frame in frames {
                                if let Frame::Json(v) = frame {
                                    let msg_type =
                                        v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                                    if msg_type == "tools_list" {
                                        let response = Frame::Json(json!({
                                            "subscriptionId": SUB_ID,
                                            "type": "tools_list",
                                            "tools": [{
                                                "name": "test_tool",
                                                "description": "A test tool",
                                                "input_schema": {
                                                    "type": "object",
                                                    "properties": {}
                                                }
                                            }]
                                        }));
                                        let data = response.encode();
                                        let _ = writer.write_all(&data).await;
                                        let _ = writer.flush().await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        // Run session in background
        let session_handle = tokio::spawn(async move {
            run_hub_session(mcp_side, &mut req_rx, &notif_tx, &context).await
        });

        // Give session time to start and subscribe
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send a tools_list request
        let (resp_tx, resp_rx) = oneshot::channel();
        let request_frame = Frame::Json(json!({
            "subscriptionId": SUB_ID,
            "type": "tools_list"
        }));
        req_tx
            .send(BridgeRequest {
                key: "tools_list".to_string(),
                frame_data: request_frame.encode(),
                response_tx: resp_tx,
            })
            .expect("send request");

        let response = tokio::time::timeout(Duration::from_secs(5), resp_rx)
            .await
            .expect("timeout waiting for response")
            .expect("channel closed")
            .expect("hub error");

        let tools = response.get("tools").and_then(|t| t.as_array());
        assert!(tools.is_some(), "response must contain tools array");
        assert_eq!(tools.expect("checked").len(), 1);
        assert_eq!(
            tools.expect("checked")[0]
                .get("name")
                .and_then(|n| n.as_str()),
            Some("test_tool")
        );

        // Clean up
        drop(req_tx);
        let _ = session_handle.await;
        hub_task.abort();
    }

    #[tokio::test]
    async fn test_hub_request_fails_fast_while_pending() {
        let connection_state = BridgeConnectionState::pending("/tmp/botster-missing.sock".into());
        connection_state
            .set_disconnected("hub socket is missing".to_string())
            .await;
        let gateway = test_gateway(Arc::clone(&connection_state));

        let err = gateway
            .hub_request(
                "tools/list",
                "tools_list".to_string(),
                Frame::Json(json!({ "type": "tools_list" })),
            )
            .await
            .expect_err("pending gateway must reject immediately");
        let rendered = format!("{err:?}");
        assert!(
            rendered.contains("temporarily unavailable"),
            "error should explain transient unavailability: {rendered}"
        );
        assert!(
            rendered.contains("retrying"),
            "error should say the bridge is still retrying: {rendered}"
        );
    }

    #[tokio::test]
    async fn test_connection_manager_keeps_retrying_until_socket_appears() {
        let temp_dir = tempfile::TempDir::new().expect("tempdir");
        let socket_path = temp_dir.path().join("hub.sock");
        let socket_path_string = socket_path.to_string_lossy().to_string();
        let connection_state = BridgeConnectionState::pending(socket_path_string.clone());
        let (_request_tx, request_rx) = mpsc::unbounded_channel::<BridgeRequest>();
        let (notification_tx, mut notification_rx) = mpsc::unbounded_channel::<HubNotification>();

        let manager_state = Arc::clone(&connection_state);
        let manager_path = socket_path_string.clone();
        let manager = tokio::spawn(async move {
            connection_manager(
                manager_path,
                request_rx,
                notification_tx,
                BTreeMap::new(),
                manager_state,
            )
            .await;
        });

        tokio::time::sleep(Duration::from_millis(CONNECT_RETRY_BASE_MS * 2)).await;
        assert!(
            !connection_state.is_connected(),
            "manager should remain pending before the socket exists"
        );

        let listener = tokio::net::UnixListener::bind(&socket_path).expect("bind socket");
        let accept_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept bridge");
            let mut reader = tokio::io::BufReader::new(stream);
            let mut decoder = FrameDecoder::new();
            let mut buf = [0u8; 4096];
            loop {
                let n = reader.read(&mut buf).await.expect("read subscribe");
                if n == 0 {
                    break;
                }
                if decoder
                    .feed(&buf[..n])
                    .expect("decode")
                    .into_iter()
                    .any(|frame| {
                        matches!(
                            frame,
                            Frame::Json(ref v)
                                if v.get("type").and_then(|t| t.as_str()) == Some("subscribe")
                        )
                    })
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let connected = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if connection_state.is_connected() {
                    break true;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("manager should connect after socket appears");
        assert!(connected);

        let first_notification = notification_rx
            .recv()
            .await
            .expect("connection should notify MCP client to refresh");
        assert!(matches!(
            first_notification,
            HubNotification::ToolsListChanged
        ));

        manager.abort();
        accept_task.abort();
    }
}
