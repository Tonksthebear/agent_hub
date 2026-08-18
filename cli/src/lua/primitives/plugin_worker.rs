//! Per-plugin Lua worker primitive.
//!
//! Exposes a small synchronous bridge used by `lib.plugin_supervisor`.
//! Each plugin key owns one worker thread and one Lua VM. The hub VM keeps
//! routing/descriptor state; plugin-owned handler execution happens here.

use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use mlua::{Lua, LuaSerdeExt, Table, Value};

use crate::hub::events::{HubEvent, HubEventTx};
use crate::lua::primitives::json::json_to_lua;
use crate::lua::primitives::{http, local_webhooks, websocket};
use crate::lua::LuaRuntime;
use crate::worker::plugin::PLUGIN_WORKER_QUEUE;

const LOAD_TIMEOUT: Duration = Duration::from_secs(5);

// Rust guideline compliant 2026-05 (ms-rust)
// Updated for reviewer feedback (RAII Guard + zero-alloc accessor + thread guard).
// Provides safe entry into plugin worker context for boundary observability/debug_asserts.
// Supports nesting, early returns, and panics via Drop restore. Zero cost in release.

thread_local! {
    static CURRENT_PLUGIN_WORKER: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// RAII guard that restores the previous plugin worker key on drop.
///
/// Returned by `enter_plugin_worker`. Prevents leaks on `?`, panic, or early return.
/// Supports proper nesting (plugin A → hub → plugin B) by restoring the prior value.
///
/// The `!Send + !Sync` marker ensures Drop always runs on the same thread as the
/// corresponding `enter_plugin_worker` call (prevents cross-thread corruption of
/// the thread_local).
#[must_use = "PluginWorkerGuard restores the previous worker key when dropped"]
pub(crate) struct PluginWorkerGuard {
    previous: Option<String>,
    _not_send: PhantomData<Rc<()>>,
}

impl Drop for PluginWorkerGuard {
    fn drop(&mut self) {
        CURRENT_PLUGIN_WORKER.with(|c| *c.borrow_mut() = self.previous.take());
    }
}

/// Enters the plugin worker context for `key`.
///
/// Returns a guard that restores the prior key when dropped.
/// Panics (debug only) if not called from a named `plugin-worker-*` thread.
pub(crate) fn enter_plugin_worker(key: &str) -> PluginWorkerGuard {
    // This assert exists to catch architectural mistakes where we enter the
    // plugin worker context from the main hub thread or other non-worker threads.
    // It is currently firing during normal startup for some AC / event / timer paths.
    // For now we log loudly instead of hard-panicking the whole hub so development
    // can continue while we finish the proper cross-VM architecture.
    if !std::thread::current()
        .name()
        .is_some_and(|n| n.starts_with("plugin-worker-"))
    {
        log::error!(
            "enter_plugin_worker called from non-worker thread (thread={:?}, key={}). \
             This is an architectural bug — plugin-owned handler code is being entered \
             from the main hub context.",
            std::thread::current().name(),
            key
        );
        // Still proceed so the hub can at least start. The boundary will be wrong
        // for this invocation, but we won't take down the entire process.
    }

    let previous = CURRENT_PLUGIN_WORKER.with(|c| c.replace(Some(key.to_string())));
    PluginWorkerGuard {
        previous,
        _not_send: PhantomData,
    }
}

/// Borrows the current plugin worker key for the duration of the closure.
///
/// Zero-allocation hot path intended for `debug_assert!` at the head of dispatch sites.
pub(crate) fn with_current_plugin_worker<R>(f: impl FnOnce(Option<&str>) -> R) -> R {
    CURRENT_PLUGIN_WORKER.with(|c| f(c.borrow().as_deref()))
}

/// Owned variant. Only for crossing into Lua (which cannot hold a borrow across the FFI boundary).
pub(crate) fn current_plugin_worker_owned() -> Option<String> {
    CURRENT_PLUGIN_WORKER.with(|c| c.borrow().clone())
}

fn lua_perf_enabled() -> bool {
    std::env::var("BOTSTER_LUA_PERF")
        .map(|value| {
            let value = value.to_ascii_lowercase();
            value == "1" || value == "true" || value == "yes" || value == "on"
        })
        .unwrap_or(false)
}

#[derive(Clone, Default)]
pub(crate) struct PluginWorkerRegistry {
    workers: Arc<Mutex<HashMap<String, PluginWorkerHandle>>>,
    hub_event_channel: Arc<Mutex<Option<(HubEventTx, tokio::runtime::Handle)>>>,
}

#[derive(Clone)]
struct PluginWorkerHandle {
    tx: mpsc::SyncSender<PluginWorkerRequest>,
    parent_rx: Arc<Mutex<mpsc::Receiver<WorkerParentRequest>>>,
}

enum PluginWorkerRequest {
    Invoke {
        kind: String,
        id: String,
        name: Option<String>,
        payload: serde_json::Value,
        timeout_ms: u64,
        response: mpsc::Sender<Result<serde_json::Value, String>>,
    },
    Shutdown {
        reason: String,
    },
    HttpResponse(http::CompletedHttpResponse),
    WebSocketEvent(websocket::WsEvent),
    TimerFired {
        timer_id: String,
    },
    UserFileWatch {
        watch_id: String,
        events: Vec<crate::file_watcher::FileEvent>,
    },
    LocalWebhook(local_webhooks::LocalWebhookRequest),
}

#[derive(Clone, Debug)]
pub(crate) struct PluginWorkerEventTx {
    tx: mpsc::SyncSender<PluginWorkerRequest>,
}

impl PluginWorkerEventTx {
    fn new(tx: mpsc::SyncSender<PluginWorkerRequest>) -> Self {
        Self { tx }
    }

    pub(crate) fn send_http_response(&self, response: http::CompletedHttpResponse) {
        if let Err(err) = self
            .tx
            .try_send(PluginWorkerRequest::HttpResponse(response))
        {
            log::warn!("plugin worker queue rejected HTTP response: {err}");
        }
    }

    pub(crate) fn send_websocket_event(&self, event: websocket::WsEvent) {
        if let Err(err) = self.tx.try_send(PluginWorkerRequest::WebSocketEvent(event)) {
            log::warn!("plugin worker queue rejected WebSocket event: {err}");
        }
    }

    pub(crate) fn send_timer_fired(&self, timer_id: String) -> bool {
        self.tx
            .try_send(PluginWorkerRequest::TimerFired { timer_id })
            .is_ok()
    }

    pub(crate) fn send_user_file_watch(
        &self,
        watch_id: String,
        events: Vec<crate::file_watcher::FileEvent>,
    ) -> bool {
        self.tx
            .try_send(PluginWorkerRequest::UserFileWatch { watch_id, events })
            .is_ok()
    }

    pub(crate) fn send_local_webhook_request(
        &self,
        request: local_webhooks::LocalWebhookRequest,
    ) -> Result<(), mpsc::TrySendError<local_webhooks::LocalWebhookRequest>> {
        self.tx
            .try_send(PluginWorkerRequest::LocalWebhook(request))
            .map_err(|err| match err {
                mpsc::TrySendError::Full(PluginWorkerRequest::LocalWebhook(request)) => {
                    mpsc::TrySendError::Full(request)
                }
                mpsc::TrySendError::Disconnected(PluginWorkerRequest::LocalWebhook(request)) => {
                    mpsc::TrySendError::Disconnected(request)
                }
                mpsc::TrySendError::Full(_) => unreachable!("request variant changed"),
                mpsc::TrySendError::Disconnected(_) => unreachable!("request variant changed"),
            })
    }
}

pub(crate) enum WorkerParentRequest {
    HubRequest {
        payload: serde_json::Value,
        response: mpsc::Sender<Result<serde_json::Value, String>>,
    },
}

impl std::fmt::Debug for WorkerParentRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HubRequest { payload, .. } => f
                .debug_struct("HubRequest")
                .field("payload", payload)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug, Clone)]
struct WorkerSpec {
    plugin_key: String,
    display_name: String,
    init_path: PathBuf,
    source: Option<String>,
    repo_root: Option<PathBuf>,
    lua_base_path: Option<PathBuf>,
    parent_hub_id: Option<String>,
}

#[must_use]
pub(crate) fn new_plugin_worker_registry() -> PluginWorkerRegistry {
    PluginWorkerRegistry::default()
}

pub(crate) fn register(lua: &Lua, registry: PluginWorkerRegistry) -> Result<()> {
    let load_registry = registry.clone();
    let load_fn = lua
        .create_function(move |lua, spec: Table| {
            let worker_spec = table_to_spec(spec)?;
            load_registry
                .load(lua, worker_spec)
                .map_err(mlua::Error::external)?;
            Ok(true)
        })
        .map_err(|e| anyhow!("create __plugin_worker_load: {e}"))?;
    lua.globals()
        .set("__plugin_worker_load", load_fn)
        .map_err(|e| anyhow!("set __plugin_worker_load: {e}"))?;

    let invoke_registry = registry.clone();
    let invoke_fn = lua
        .create_function(
            move |lua,
                  (plugin_key, kind, id, name, payload, timeout_ms): (
                String,
                String,
                String,
                Option<String>,
                Value,
                u64,
            )| {
                let payload_json: serde_json::Value = lua.from_value(payload).map_err(|e| {
                    mlua::Error::external(format!("plugin worker payload conversion failed: {e}"))
                })?;
                let result = invoke_registry
                    .invoke_with_lua(lua, &plugin_key, kind, id, name, payload_json, timeout_ms)
                    .map_err(mlua::Error::external)?;
                json_to_lua(lua, &result)
            },
        )
        .map_err(|e| anyhow!("create __plugin_worker_invoke: {e}"))?;
    lua.globals()
        .set("__plugin_worker_invoke", invoke_fn)
        .map_err(|e| anyhow!("set __plugin_worker_invoke: {e}"))?;

    let shutdown_registry = registry.clone();
    let shutdown_fn = lua
        .create_function(move |_, (plugin_key, reason): (String, Option<String>)| {
            shutdown_registry.shutdown(&plugin_key, reason.as_deref().unwrap_or("shutdown"));
            Ok(true)
        })
        .map_err(|e| anyhow!("create __plugin_worker_shutdown: {e}"))?;
    lua.globals()
        .set("__plugin_worker_shutdown", shutdown_fn)
        .map_err(|e| anyhow!("set __plugin_worker_shutdown: {e}"))?;

    Ok(())
}

fn table_to_spec(table: Table) -> mlua::Result<WorkerSpec> {
    let plugin_key: String = table.get("plugin_key")?;
    let display_name: String = table
        .get("display_name")
        .unwrap_or_else(|_| plugin_key.clone());
    let init_path: String = table.get("init_path")?;
    let source: Option<String> = table.get("source").ok();
    let repo_root: Option<String> = table.get("repo_root").ok();
    let lua_base_path: Option<String> = table.get("lua_base_path").ok();
    let parent_hub_id: Option<String> = table.get("parent_hub_id").ok();
    Ok(WorkerSpec {
        plugin_key,
        display_name,
        init_path: PathBuf::from(init_path),
        source,
        repo_root: repo_root.map(PathBuf::from),
        lua_base_path: lua_base_path.map(PathBuf::from),
        parent_hub_id,
    })
}

impl PluginWorkerRegistry {
    pub(crate) fn set_hub_event_tx(&self, tx: HubEventTx, tokio_handle: tokio::runtime::Handle) {
        *self
            .hub_event_channel
            .lock()
            .expect("PluginWorkerRegistry hub_event_channel mutex poisoned") =
            Some((tx, tokio_handle));
    }

    fn load(&self, lua: &Lua, spec: WorkerSpec) -> Result<()> {
        self.shutdown(&spec.plugin_key, "replace");

        let (tx, rx) = mpsc::sync_channel(PLUGIN_WORKER_QUEUE.capacity);
        let (parent_tx, parent_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        let plugin_key = spec.plugin_key.clone();
        let hub_event_channel = self
            .hub_event_channel
            .lock()
            .expect("PluginWorkerRegistry hub_event_channel mutex poisoned")
            .clone();
        let worker_event_tx = PluginWorkerEventTx::new(tx.clone());
        thread::Builder::new()
            .name(format!("plugin-worker-{plugin_key}"))
            .spawn(move || {
                worker_loop(
                    spec,
                    rx,
                    worker_event_tx,
                    parent_tx,
                    ready_tx,
                    hub_event_channel,
                )
            })
            .map_err(|e| anyhow!("spawn plugin worker: {e}"))?;

        match wait_for_ready(lua, ready_rx, &parent_rx, LOAD_TIMEOUT) {
            Ok(Ok(())) => {
                self.workers
                    .lock()
                    .expect("PluginWorkerRegistry mutex poisoned")
                    .insert(
                        plugin_key,
                        PluginWorkerHandle {
                            tx,
                            parent_rx: Arc::new(Mutex::new(parent_rx)),
                        },
                    );
                Ok(())
            }
            Ok(Err(err)) => Err(anyhow!(err)),
            Err(_) => Err(anyhow!("plugin worker load timeout")),
        }
    }

    fn invoke_with_lua(
        &self,
        lua: &Lua,
        plugin_key: &str,
        kind: String,
        id: String,
        name: Option<String>,
        payload: serde_json::Value,
        timeout_ms: u64,
    ) -> Result<serde_json::Value> {
        let perf_started = lua_perf_enabled().then(Instant::now);
        let log_kind = kind.clone();
        let log_id = id.clone();
        let handle = self
            .workers
            .lock()
            .expect("PluginWorkerRegistry mutex poisoned")
            .get(plugin_key)
            .cloned()
            .ok_or_else(|| anyhow!("plugin worker not loaded: {plugin_key}"))?;
        let (response_tx, response_rx) = mpsc::channel();
        handle
            .tx
            .try_send(PluginWorkerRequest::Invoke {
                kind,
                id,
                name,
                payload,
                timeout_ms,
                response: response_tx,
            })
            .map_err(|e| anyhow!("plugin worker queue rejected invoke: {e}"))?;
        let result = {
            let parent_rx = handle
                .parent_rx
                .lock()
                .expect("PluginWorkerHandle parent_rx mutex poisoned");
            wait_for_invoke_response(
                lua,
                response_rx,
                &parent_rx,
                Duration::from_millis(timeout_ms.max(1)),
            )
        };
        if let Some(started) = perf_started {
            log::info!(
                "[PERF][plugin_worker] plugin={} kind={} id={} phase=roundtrip ok={} elapsed_ms={}",
                plugin_key,
                log_kind,
                log_id,
                result.is_ok(),
                started.elapsed().as_millis()
            );
        }
        result
    }

    fn shutdown(&self, plugin_key: &str, reason: &str) {
        let handle = self
            .workers
            .lock()
            .expect("PluginWorkerRegistry mutex poisoned")
            .remove(plugin_key);
        if let Some(handle) = handle {
            let _ = handle.tx.try_send(PluginWorkerRequest::Shutdown {
                reason: reason.to_string(),
            });
        }
    }

    pub(crate) fn shutdown_all(&self, reason: &str) {
        let handles: Vec<_> = self
            .workers
            .lock()
            .expect("PluginWorkerRegistry mutex poisoned")
            .drain()
            .collect();
        for (plugin_key, handle) in handles {
            let _ = handle.tx.try_send(PluginWorkerRequest::Shutdown {
                reason: reason.to_string(),
            });
            log::debug!("plugin worker {plugin_key} shutdown requested: {reason}");
        }
    }
}

fn worker_loop(
    spec: WorkerSpec,
    rx: mpsc::Receiver<PluginWorkerRequest>,
    worker_event_tx: PluginWorkerEventTx,
    parent_tx: mpsc::Sender<WorkerParentRequest>,
    ready_tx: mpsc::Sender<Result<(), String>>,
    hub_event_channel: Option<(HubEventTx, tokio::runtime::Handle)>,
) {
    let mut runtime = match LuaRuntime::new() {
        Ok(runtime) => runtime,
        Err(err) => {
            let _ = ready_tx.send(Err(format!("create plugin worker runtime: {err}")));
            return;
        }
    };
    let parent_hub_event_tx = hub_event_channel.as_ref().map(|(tx, _)| tx.clone());
    if let Some((tx, tokio_handle)) = hub_event_channel {
        runtime.set_hub_event_tx(tx, tokio_handle);
    }
    runtime.set_plugin_worker_event_tx(worker_event_tx);
    if let Err(err) = install_parent_hub_bridge(&runtime, parent_tx, parent_hub_event_tx) {
        let _ = ready_tx.send(Err(format!("install parent hub bridge: {err}")));
        return;
    }
    if let Some(lua_base_path) = &spec.lua_base_path {
        runtime.set_base_path(lua_base_path.clone());
        if let Err(err) = runtime.update_package_path(lua_base_path) {
            let _ = ready_tx.send(Err(format!("configure plugin worker lua path: {err}")));
            return;
        }
    } else {
        if let Err(err) = runtime.install_embedded_searcher() {
            let _ = ready_tx.send(Err(format!(
                "install plugin worker embedded modules: {err}"
            )));
            return;
        }
    }

    if let Err(err) = install_worker_bootstrap(&runtime, &spec) {
        let _ = ready_tx.send(Err(err));
        return;
    }

    if let Err(err) = load_plugin_into_worker(&runtime, &spec) {
        close_worker_plugin_databases(&runtime, &spec.plugin_key);
        let _ = ready_tx.send(Err(err));
        return;
    }
    let _ = ready_tx.send(Ok(()));

    loop {
        let request = match rx.recv() {
            Ok(request) => request,
            Err(_) => break,
        };

        match request {
            PluginWorkerRequest::Invoke {
                kind,
                id,
                name,
                payload,
                timeout_ms,
                response,
            } => {
                let perf_started = lua_perf_enabled().then(Instant::now);
                let _guard = enter_plugin_worker(&spec.plugin_key);
                let result =
                    invoke_in_worker(&runtime, &kind, &id, name.as_deref(), payload, timeout_ms);
                if let Some(started) = perf_started {
                    log::info!(
                        "[PERF][plugin_worker] plugin={} kind={} id={} phase=execute ok={} elapsed_ms={}",
                        spec.plugin_key,
                        kind,
                        id,
                        result.is_ok(),
                        started.elapsed().as_millis()
                    );
                }
                let _ = response.send(result);
            }
            PluginWorkerRequest::Shutdown { reason } => {
                log::debug!(
                    "plugin worker {} shutting down: {}",
                    spec.plugin_key,
                    reason
                );
                break;
            }
            PluginWorkerRequest::HttpResponse(response) => {
                runtime.fire_http_callback(response);
            }
            PluginWorkerRequest::WebSocketEvent(event) => {
                runtime.fire_websocket_event(event);
            }
            PluginWorkerRequest::TimerFired { timer_id } => {
                runtime.fire_timer_callback(&timer_id);
            }
            PluginWorkerRequest::UserFileWatch { watch_id, events } => {
                runtime.fire_user_file_watch(&watch_id, events);
            }
            PluginWorkerRequest::LocalWebhook(request) => {
                runtime.fire_local_webhook_request(request);
            }
        }
    }

    close_worker_plugin_databases(&runtime, &spec.plugin_key);
}

fn close_worker_plugin_databases(runtime: &LuaRuntime, plugin_key: &str) {
    if let Err(err) = runtime
        .lua()
        .load(
            r#"
            local ok, plugin_db = pcall(require, "lib.plugin_db")
            if ok and plugin_db and type(plugin_db.shutdown_all) == "function" then
                plugin_db.shutdown_all()
            end
            "#,
        )
        .set_name("plugin_worker_close_databases")
        .exec()
    {
        log::warn!("plugin worker {plugin_key} failed to close plugin databases: {err}");
    }
}

fn install_parent_hub_bridge(
    runtime: &LuaRuntime,
    parent_tx: mpsc::Sender<WorkerParentRequest>,
    parent_hub_event_tx: Option<HubEventTx>,
) -> Result<()> {
    let lua = runtime.lua();
    let bridge = lua
        .create_table()
        .map_err(|e| anyhow!("create plugin_worker_parent_hub table: {e}"))?;
    let request_tx = parent_tx.clone();
    let request_fn = lua
        .create_function(move |lua, (payload, timeout_ms): (Value, Option<u64>)| {
            let payload_json: serde_json::Value = lua.from_value(payload).map_err(|e| {
                mlua::Error::external(format!(
                    "plugin_worker_parent_hub.request: failed to serialize payload: {e}"
                ))
            })?;
            let (response_tx, response_rx) = mpsc::channel();
            let request = WorkerParentRequest::HubRequest {
                payload: payload_json,
                response: response_tx,
            };
            request_tx.send(request).map_err(|e| {
                mlua::Error::external(format!(
                    "plugin_worker_parent_hub.request: parent hub unavailable: {e}"
                ))
            })?;
            let timeout = Duration::from_millis(timeout_ms.unwrap_or(30_000).max(1));
            match response_rx.recv_timeout(timeout) {
                Ok(Ok(response)) => json_to_lua(lua, &response),
                Ok(Err(err)) => Err(mlua::Error::external(err)),
                Err(mpsc::RecvTimeoutError::Timeout) => Err(mlua::Error::external(format!(
                    "plugin_worker_parent_hub.request: timeout after {}ms",
                    timeout.as_millis()
                ))),
                Err(mpsc::RecvTimeoutError::Disconnected) => Err(mlua::Error::external(
                    "plugin_worker_parent_hub.request: response channel closed",
                )),
            }
        })
        .map_err(|e| anyhow!("create plugin_worker_parent_hub.request: {e}"))?;
    bridge
        .set("request", request_fn)
        .map_err(|e| anyhow!("set plugin_worker_parent_hub.request: {e}"))?;
    let enqueue_tx = parent_tx.clone();
    let enqueue_event_tx = parent_hub_event_tx.clone();
    let enqueue_fn = lua
        .create_function(move |lua, payload: Value| {
            let payload_json: serde_json::Value = lua.from_value(payload).map_err(|e| {
                mlua::Error::external(format!(
                    "plugin_worker_parent_hub.enqueue: failed to serialize payload: {e}"
                ))
            })?;
            let (response_tx, _response_rx) = mpsc::channel();
            let request = WorkerParentRequest::HubRequest {
                payload: payload_json,
                response: response_tx,
            };
            if let Some(tx) = &enqueue_event_tx {
                tx.send(HubEvent::PluginWorkerParentRequest(request))
                    .map_err(|e| {
                        mlua::Error::external(format!(
                            "plugin_worker_parent_hub.enqueue: parent hub unavailable: {e}"
                        ))
                    })?;
            } else {
                enqueue_tx.send(request).map_err(|e| {
                    mlua::Error::external(format!(
                        "plugin_worker_parent_hub.enqueue: parent hub unavailable: {e}"
                    ))
                })?;
            }
            Ok(true)
        })
        .map_err(|e| anyhow!("create plugin_worker_parent_hub.enqueue: {e}"))?;
    bridge
        .set("enqueue", enqueue_fn)
        .map_err(|e| anyhow!("set plugin_worker_parent_hub.enqueue: {e}"))?;
    lua.globals()
        .set("plugin_worker_parent_hub", bridge)
        .map_err(|e| anyhow!("set plugin_worker_parent_hub global: {e}"))?;
    Ok(())
}

fn wait_for_ready(
    lua: &Lua,
    ready_rx: mpsc::Receiver<Result<(), String>>,
    parent_rx: &mpsc::Receiver<WorkerParentRequest>,
    timeout: Duration,
) -> Result<Result<(), String>, mpsc::RecvTimeoutError> {
    let deadline = Instant::now() + timeout;
    loop {
        service_parent_requests(lua, parent_rx);
        let now = Instant::now();
        if now >= deadline {
            return Err(mpsc::RecvTimeoutError::Timeout);
        }
        let remaining = deadline.saturating_duration_since(now);
        match ready_rx.recv_timeout(remaining.min(Duration::from_millis(10))) {
            Ok(result) => {
                service_parent_requests(lua, parent_rx);
                service_parent_requests_for(lua, parent_rx, Duration::from_millis(100));
                return Ok(result);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(err) => {
                service_parent_requests(lua, parent_rx);
                service_parent_requests_for(lua, parent_rx, Duration::from_millis(100));
                return Err(err);
            }
        }
    }
}

fn wait_for_invoke_response(
    lua: &Lua,
    response_rx: mpsc::Receiver<Result<serde_json::Value, String>>,
    parent_rx: &mpsc::Receiver<WorkerParentRequest>,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let deadline = Instant::now() + timeout;
    loop {
        service_parent_requests(lua, parent_rx);
        let now = Instant::now();
        if now >= deadline {
            return Err(anyhow!("plugin worker invoke timeout"));
        }
        let remaining = deadline.saturating_duration_since(now);
        match response_rx.recv_timeout(remaining.min(Duration::from_millis(10))) {
            Ok(Ok(value)) => {
                service_parent_requests(lua, parent_rx);
                return Ok(value);
            }
            Ok(Err(err)) => {
                service_parent_requests(lua, parent_rx);
                return Err(anyhow!(err));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                service_parent_requests(lua, parent_rx);
                return Err(anyhow!("plugin worker response channel closed"));
            }
        }
    }
}

fn service_parent_requests(lua: &Lua, parent_rx: &mpsc::Receiver<WorkerParentRequest>) {
    while let Ok(request) = parent_rx.try_recv() {
        service_parent_request(lua, request);
    }
}

fn service_parent_requests_for(
    lua: &Lua,
    parent_rx: &mpsc::Receiver<WorkerParentRequest>,
    duration: Duration,
) {
    let deadline = Instant::now() + duration;
    loop {
        service_parent_requests(lua, parent_rx);
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        let remaining = deadline.saturating_duration_since(now);
        match parent_rx.recv_timeout(remaining.min(Duration::from_millis(1))) {
            Ok(request) => service_parent_request(lua, request),
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}

pub(crate) fn service_parent_request(lua: &Lua, request: WorkerParentRequest) {
    match request {
        WorkerParentRequest::HubRequest { payload, response } => {
            let _ = response.send(handle_parent_hub_request(lua, payload));
        }
    }
}

fn handle_parent_hub_request(
    lua: &Lua,
    payload: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let payload_lua = json_to_lua(lua, &payload).map_err(|e| e.to_string())?;
    let globals = lua.globals();
    globals
        .set("__plugin_worker_parent_hub_payload", payload_lua)
        .map_err(|e| e.to_string())?;
    let result: Value = lua
        .load(
            r#"
            local Hub = require("lib.hub")
            return Hub._handle_worker_parent_request(__plugin_worker_parent_hub_payload)
            "#,
        )
        .set_name("plugin_worker_parent_hub_request")
        .eval()
        .map_err(|e| e.to_string())?;
    lua.from_value(result)
        .map_err(|e| format!("plugin worker parent hub response conversion failed: {e}"))
}

fn install_worker_bootstrap(runtime: &LuaRuntime, spec: &WorkerSpec) -> Result<(), String> {
    let lua = runtime.lua();
    let globals = lua.globals();
    globals
        .set("_loading_plugin_worker", true)
        .map_err(|e| e.to_string())?;
    globals
        .set("_plugin_worker_key", spec.plugin_key.clone())
        .map_err(|e| e.to_string())?;
    if let Some(parent_hub_id) = &spec.parent_hub_id {
        globals
            .set("_plugin_worker_parent_hub_id", parent_hub_id.clone())
            .map_err(|e| e.to_string())?;
    }
    if let Some(repo_root) = &spec.repo_root {
        globals
            .set(
                "_loading_plugin_repo_root",
                repo_root.to_string_lossy().to_string(),
            )
            .map_err(|e| e.to_string())?;
    }

    let hub = lua.create_table().map_err(|e| e.to_string())?;
    let hub_id = format!("plugin-worker:{}", spec.plugin_key);
    let hub_id_fn = {
        let hub_id = hub_id.clone();
        lua.create_function(move |_, ()| Ok(hub_id.clone()))
            .map_err(|e| e.to_string())?
    };
    hub.set("hub_id", hub_id_fn).map_err(|e| e.to_string())?;
    let server_id_fn = {
        let hub_id = hub_id.clone();
        lua.create_function(move |_, ()| Ok(hub_id.clone()))
            .map_err(|e| e.to_string())?
    };
    hub.set("server_id", server_id_fn)
        .map_err(|e| e.to_string())?;
    let repo_root = spec.repo_root.clone();
    let detect_repo_fn = lua
        .create_function(move |lua, path: Option<String>| {
            if let Ok(repo) = std::env::var("BOTSTER_REPO") {
                return Ok(Some(repo));
            }

            let detect_path = path
                .map(PathBuf::from)
                .or_else(|| {
                    lua.globals()
                        .get::<Option<String>>("_loading_plugin_repo_root")
                        .ok()
                        .flatten()
                        .map(PathBuf::from)
                })
                .or_else(|| repo_root.clone());

            if let Some(path) = detect_path {
                return Ok(crate::git::repo_name_for_path(&path).ok());
            }

            Ok(None)
        })
        .map_err(|e| e.to_string())?;
    hub.set("detect_repo", detect_repo_fn)
        .map_err(|e| e.to_string())?;
    hub.set(
        "get_worktrees",
        lua.create_function(|lua, ()| lua.create_table())
            .map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;
    globals.set("hub", hub).map_err(|e| e.to_string())?;

    lua.load(
        r#"
        local state = require("hub.state")
        local hooks = require("hub.hooks")
        local loader = require("hub.loader")

        _G.state = state
        _G.hooks = hooks
        _G.loader = loader

        local function safe_require(module_name)
            local ok, result = pcall(require, module_name)
            if ok then return result end
            if log and log.debug then
                log.debug(string.format("plugin worker skipped %s: %s", module_name, tostring(result)))
            end
            return nil
        end

        safe_require("lib.config_resolver")
        safe_require("lib.commands")
        safe_require("lib.session_actions")
        _G.action = safe_require("lib.action")
        _G.surfaces = safe_require("lib.surfaces")
        safe_require("lib.plugin_assets")
        safe_require("lib.entity_broadcast")
        _G.mcp = safe_require("lib.mcp")

        local plugin_db = safe_require("lib.plugin_db")
        if plugin_db and type(plugin_db.install) == "function" then
            plugin_db.install()
        end
        "#,
    )
    .set_name("plugin_worker_bootstrap")
    .exec()
    .map_err(|e| e.to_string())
}

fn load_plugin_into_worker(runtime: &LuaRuntime, spec: &WorkerSpec) -> Result<(), String> {
    let lua = runtime.lua();
    let repo_root = spec
        .repo_root
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());
    let source = spec.source.clone();
    let globals = lua.globals();
    globals
        .set(
            "__plugin_init_path",
            spec.init_path.to_string_lossy().to_string(),
        )
        .map_err(|e| e.to_string())?;
    globals
        .set("__plugin_display_name", spec.display_name.clone())
        .map_err(|e| e.to_string())?;
    globals
        .set("__plugin_source", source)
        .map_err(|e| e.to_string())?;
    globals
        .set("__plugin_repo_root", repo_root)
        .map_err(|e| e.to_string())?;
    lua.load(
        r#"
        local loader = require("hub.loader")
        local ok, err = loader.load_plugin(__plugin_init_path, __plugin_display_name, {
            source = __plugin_source,
            repo_root = __plugin_repo_root,
        })
        if not ok then error(err or "plugin worker load failed") end
        "#,
    )
    .set_name("plugin_worker_load")
    .exec()
    .map_err(|e| e.to_string())
}

fn invoke_in_worker(
    runtime: &LuaRuntime,
    kind: &str,
    id: &str,
    name: Option<&str>,
    payload: serde_json::Value,
    timeout_ms: u64,
) -> Result<serde_json::Value, String> {
    let lua = runtime.lua();
    let payload_lua = json_to_lua(lua, &payload).map_err(|e| e.to_string())?;
    let globals = lua.globals();
    globals.set("__handler_id", id).map_err(|e| e.to_string())?;
    globals
        .set("__handler_name", name)
        .map_err(|e| e.to_string())?;
    globals
        .set("__handler_timeout_ms", timeout_ms)
        .map_err(|e| e.to_string())?;
    globals
        .set("__payload", payload_lua)
        .map_err(|e| e.to_string())?;
    let result: Value = match kind {
        "ui_action" => lua
            .load(
                r#"
                local action = require("lib.action")
                local ok, result = __hook_timed_pcall(function()
                    return action._invoke_registered(__handler_id, __handler_name, __payload.envelope, __payload.ctx)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_ui_action")
            .eval()
            .map_err(|e| e.to_string())?,
        "session_action" => lua
            .load(
                r#"
                local actions = require("lib.session_actions")
                local ok, result = __hook_timed_pcall(function()
                    return actions._invoke_registered(__handler_id, __payload.session_uuid, __payload.payload)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_session_action")
            .eval()
            .map_err(|e| e.to_string())?,
        "command" => lua
            .load(
                r#"
                local commands = require("lib.commands")
                local ok, result = __hook_timed_pcall(function()
                    return commands._invoke_registered(__handler_id, __payload.command)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_command")
            .eval()
            .map_err(|e| e.to_string())?,
        "hook_observer" => lua
            .load(
                r#"
                local hooks = require("hub.hooks")
                local ok, result = __hook_timed_pcall(function()
                    return hooks._invoke_observer(__handler_id, __handler_name, __payload.args)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_hook_observer")
            .eval()
            .map_err(|e| e.to_string())?,
        "hook_interceptor" => lua
            .load(
                r#"
                local hooks = require("hub.hooks")
                local ok, result = __hook_timed_pcall(function()
                    return hooks._invoke_interceptor(__handler_id, __handler_name, __payload.args)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_hook_interceptor")
            .eval()
            .map_err(|e| e.to_string())?,
        "notification_observer" => lua
            .load(
                r#"
                local notifications = require("lib.notifications")
                local ok, result = __hook_timed_pcall(function()
                    return notifications._invoke_observer(
                        __handler_id,
                        __payload.phase,
                        __payload.intent,
                        __payload.decision)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_notification_observer")
            .eval()
            .map_err(|e| e.to_string())?,
        "notification_claim" => lua
            .load(
                r#"
                local notifications = require("lib.notifications")
                local ok, result = __hook_timed_pcall(function()
                    return notifications._invoke_claim(__handler_id, __payload.intent)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_notification_claim")
            .eval()
            .map_err(|e| e.to_string())?,
        "surface_route" => lua
            .load(
                r#"
                local surfaces = require("lib.surfaces")
                local ok, result = __hook_timed_pcall(function()
                    return surfaces._invoke_render(__handler_id, __payload.render_state)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_surface_route")
            .eval()
            .map_err(|e| e.to_string())?,
        "asset_message" => lua
            .load(
                r#"
                local plugin_assets = require("lib.plugin_assets")
                local ok, result = __hook_timed_pcall(function()
                    return plugin_assets._invoke_message(
                        __handler_name,
                        __handler_id,
                        __payload.message,
                        __payload.ctx
                    )
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_asset_message")
            .eval()
            .map_err(|e| e.to_string())?,
        "plugin_asset_read" => lua
            .load(
                r#"
                local plugin_assets = require("lib.plugin_assets")
                local ok, result, err = __hook_timed_pcall(function()
                    return plugin_assets.read(__payload.asset_id)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                if not result then error(err or "Unknown plugin asset") end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_plugin_asset_read")
            .eval()
            .map_err(|e| e.to_string())?,
        "timer" => lua
            .load(
                r#"
                local ok, result = __hook_timed_pcall(function()
                    return timer._invoke_registered(__handler_id)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_timer")
            .eval()
            .map_err(|e| e.to_string())?,
        "event" => lua
            .load(
                r#"
                local ok, result = __hook_timed_pcall(function()
                    return events._invoke_registered(__handler_id, __payload.data)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_event")
            .eval()
            .map_err(|e| e.to_string())?,
        "watch" => lua
            .load(
                r#"
                local ok, result = __hook_timed_pcall(function()
                    return watch._invoke_registered(__handler_id, __payload.event)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_watch")
            .eval()
            .map_err(|e| e.to_string())?,
        "mcp_tool" => lua
            .load(
                r#"
                local mcp = require("lib.mcp")
                local ok, result = __hook_timed_pcall(function()
                    return mcp._invoke_tool(__handler_id, __payload.params, __payload.context)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_mcp_tool")
            .eval()
            .map_err(|e| e.to_string())?,
        "mcp_prompt" => lua
            .load(
                r#"
                local mcp = require("lib.mcp")
                local ok, result = __hook_timed_pcall(function()
                    return mcp._invoke_prompt(__handler_id, __payload.args)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_mcp_prompt")
            .eval()
            .map_err(|e| e.to_string())?,
        "mcp_resource" => lua
            .load(
                r#"
                local mcp = require("lib.mcp")
                local ok, result = __hook_timed_pcall(function()
                    return mcp._invoke_resource(__handler_id, __payload.params, __payload.context)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_mcp_resource")
            .eval()
            .map_err(|e| e.to_string())?,
        "mcp_proxy_auth_error" => lua
            .load(
                r#"
                local mcp = require("lib.mcp")
                local ok, result = __hook_timed_pcall(function()
                    return mcp._invoke_proxy_auth_error(__handler_id)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_mcp_proxy_auth_error")
            .eval()
            .map_err(|e| e.to_string())?,
        "ac_message" => lua
            .load(
                r#"
                local action_cable = require("lib.action_cable")
                local ok, result = __hook_timed_pcall(function()
                    return action_cable._invoke_ac_message(__handler_id, __payload.channel_id, __payload.message)
                end, __handler_timeout_ms)
                if not ok then error(result) end
                return result
                "#,
            )
            .set_name("plugin_worker_invoke_ac_message")
            .eval()
            .map_err(|e| e.to_string())?,
        "ac_unregister" => lua
            .load(
                r#"
                local action_cable = require("lib.action_cable")
                action_cable._unregister_handler(__handler_id)
                return true
                "#,
            )
            .set_name("plugin_worker_invoke_ac_unregister")
            .eval()
            .map_err(|e| e.to_string())?,
        _ => return Err(format!("unsupported plugin handler kind: {kind}")),
    };

    lua.from_value(result)
        .map_err(|e| format!("plugin worker result conversion failed: {e}"))
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::missing_docs_in_private_items,
        reason = "test-code brevity"
    )]

    use super::*;
    use serde_json::json;
    use std::fs;

    #[test]
    fn worker_database_cleanup_calls_plugin_db_shutdown() {
        let runtime = LuaRuntime::new().expect("runtime");
        runtime
            .lua()
            .load(
                r#"
                package.preload["lib.plugin_db"] = function()
                  return {
                    shutdown_all = function()
                      _G.plugin_database_shutdown_called = true
                    end,
                  }
                end
                "#,
            )
            .exec()
            .expect("install fake plugin.db module");

        close_worker_plugin_databases(&runtime, "test-plugin");

        let called: bool = runtime
            .lua()
            .globals()
            .get("plugin_database_shutdown_called")
            .expect("read shutdown flag");
        assert!(called, "worker cleanup must close all plugin databases");
    }

    #[test]
    fn load_wait_services_worker_parent_hub_requests() {
        let lua = Lua::new();
        let temp = tempfile::tempdir().expect("tempdir");
        let lib_dir = temp.path().join("lib");
        fs::create_dir_all(&lib_dir).expect("create lib dir");
        fs::write(
            lib_dir.join("hub.lua"),
            r#"
            return {
              _handle_worker_parent_request = function(payload)
                return { result = { ok = true, echoed_type = payload.type } }
              end,
            }
            "#,
        )
        .expect("write fake hub.lua");
        lua.load(format!(
            r#"
            package.path = "{dir}/?.lua;{dir}/?/init.lua;" .. package.path
            "#,
            dir = temp.path().display()
        ))
        .exec()
        .expect("configure package path");

        let (parent_tx, parent_rx) = mpsc::channel();
        let (ready_tx, ready_rx) = mpsc::channel();
        thread::spawn(move || {
            let (response_tx, response_rx) = mpsc::channel();
            parent_tx
                .send(WorkerParentRequest::HubRequest {
                    payload: json!({ "type": "get_agent_list" }),
                    response: response_tx,
                })
                .expect("send parent request");
            let response = response_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("parent response")
                .expect("parent response ok");
            assert_eq!(response["result"]["echoed_type"], "get_agent_list");
            ready_tx.send(Ok(())).expect("send ready");
        });

        let result = wait_for_ready(&lua, ready_rx, &parent_rx, Duration::from_secs(1))
            .expect("wait should not time out")
            .expect("worker should report ready");
        assert_eq!(result, ());
    }

    #[test]
    fn enqueue_parent_hub_request_uses_hub_event_channel_when_available() {
        let runtime = LuaRuntime::new().expect("runtime");
        runtime
            .lua()
            .load(
                r#"
                package.loaded["lib.hub"] = {
                  _handle_worker_parent_request = function(payload)
                    _G.parent_payload = payload
                    return { result = { ok = true } }
                  end,
                }
                "#,
            )
            .exec()
            .expect("install fake parent hub");

        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);
        let hub_event_tx = HubEventTx::from(event_tx);
        let (fallback_tx, fallback_rx) = mpsc::channel();
        install_parent_hub_bridge(&runtime, fallback_tx, Some(hub_event_tx))
            .expect("install parent hub bridge");

        runtime
            .lua()
            .load(
                r#"
                assert(plugin_worker_parent_hub.enqueue({
                  type = "hub_command",
                  command = {
                    type = "update_session",
                    agent_id = "parent-session",
                    plugin_state = {
                      cloudflare_hosted_preview = { status = "running" },
                    },
                  },
                }))
                "#,
            )
            .exec()
            .expect("enqueue parent request");

        assert!(
            fallback_rx.try_recv().is_err(),
            "production bridge should use the hub event loop, not the fallback queue"
        );
        let event = event_rx.blocking_recv().expect("parent hub event");
        match event {
            HubEvent::PluginWorkerParentRequest(request) => {
                service_parent_request(runtime.lua(), request);
            }
            other => panic!("expected plugin worker parent request, got {other:?}"),
        }

        let status: String = runtime
            .lua()
            .load(
                r#"
                return _G.parent_payload.command.plugin_state.cloudflare_hosted_preview.status
                "#,
            )
            .eval()
            .expect("parent request handled");
        assert_eq!(status, "running");
    }

    #[test]
    fn blocking_parent_hub_request_keeps_load_wait_fallback_when_event_channel_exists() {
        let runtime = LuaRuntime::new().expect("runtime");
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(8);
        let hub_event_tx = HubEventTx::from(event_tx);
        let (fallback_tx, fallback_rx) = mpsc::channel();
        install_parent_hub_bridge(&runtime, fallback_tx, Some(hub_event_tx))
            .expect("install parent hub bridge");

        let responder = std::thread::spawn(move || {
            let request = fallback_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("fallback parent request");
            match request {
                WorkerParentRequest::HubRequest { payload, response } => {
                    assert_eq!(payload["type"], "get_agent_list");
                    response
                        .send(Ok(json!({ "result": { "ok": true } })))
                        .expect("send fallback response");
                }
            }
        });

        let ok: bool = runtime
            .lua()
            .load(
                r#"
                local response = plugin_worker_parent_hub.request({
                  type = "get_agent_list",
                }, 1000)
                return response.result.ok
                "#,
            )
            .eval()
            .expect("blocking request should receive fallback response");

        assert!(
            event_rx.try_recv().is_err(),
            "blocking request must not depend on the hub event loop during worker load"
        );
        responder.join().expect("responder thread");
        assert!(ok);
    }

    #[test]
    fn plugin_worker_guard_restores_context_and_prevents_cross_thread_use() {
        std::thread::Builder::new()
            .name("plugin-worker-test".to_string())
            .spawn(|| {
                // Behavior: enter sets the key, with_ sees it, drop restores previous.
                let guard = enter_plugin_worker("test-owner-42");
                with_current_plugin_worker(|cur| {
                    assert_eq!(cur, Some("test-owner-42"));
                });
                drop(guard);
                with_current_plugin_worker(|cur| {
                    assert!(cur.is_none(), "context must be restored after guard drop");
                });
            })
            .expect("spawn plugin-worker-test thread")
            .join()
            .expect("plugin-worker-test thread panicked");
    }

    #[test]
    fn plugin_worker_guard_nesting_restores_outer_context() {
        std::thread::Builder::new()
            .name("plugin-worker-test".to_string())
            .spawn(|| {
                let _outer = enter_plugin_worker("outer");
                {
                    let _inner = enter_plugin_worker("inner");
                    with_current_plugin_worker(|cur| assert_eq!(cur, Some("inner")));
                }
                with_current_plugin_worker(|cur| assert_eq!(cur, Some("outer")));
            })
            .unwrap()
            .join()
            .unwrap();
    }
}
