//! Timer primitives for Lua scripts.
//!
//! Provides one-shot and repeating timers that fire Lua callbacks.
//! In production, each timer spawns a tokio task that sends
//! `HubEvent::TimerFired` after the delay. Tests use deadline-based
//! polling via [`poll_timers`] as a fallback.
//!
//! # Usage in Lua
//!
//! ```lua
//! -- One-shot timer: fires once after 1 second
//! local id = timer.after(1, function()
//!     log.info("Timer fired!")
//! end)
//!
//! -- Repeating timer: fires every 0.5 seconds
//! local id = timer.every(0.5, function()
//!     log.info("Tick!")
//! end)
//!
//! -- Cancel a timer
//! timer.cancel(id)
//! ```
//!
//! All durations are in **seconds** (fractional values supported via `f64`).
//!
//! # Error Handling
//!
//! `timer.after` and `timer.every` return a timer ID string.
//! `timer.cancel` returns `true` if the timer was found, `false` otherwise.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use mlua::prelude::*;

use crate::hub::events::HubEvent;
use crate::lua::primitives::plugin_worker::PluginWorkerEventTx;

const MIN_REPEATING_TIMER_INTERVAL: Duration = Duration::from_millis(10);
const SLOW_RECURRING_TIMER_MS: u128 = 250;

/// A single timer entry in the registry.
struct TimerEntry {
    /// Lua registry key for the callback function.
    callback_key: LuaRegistryKey,
    /// Stable handler id used when this is a plugin-owned timer callback.
    handler_id: Option<String>,
    /// Plugin that owns the callback. Scheduling remains hub-owned.
    owner_plugin: Option<String>,
    /// When this timer should next fire (used by test-mode polling).
    fire_at: Instant,
    /// If `Some`, the timer repeats with this interval.
    repeat_interval: Option<Duration>,
    /// Lua primitive that created the timer.
    kind: TimerKind,
    /// Whether this timer has been cancelled.
    cancelled: bool,
    /// Handle for the spawned tokio timer task (production mode).
    ///
    /// `None` in test mode where timers use deadline-based polling via
    /// [`poll_timers`]. Aborted on [`timer.cancel()`] to stop the task.
    task_handle: Option<tokio::task::JoinHandle<()>>,
    /// Cancellation flag for worker-owned std threads.
    cancel_flag: Option<Arc<AtomicBool>>,
}

/// Registry of active timers.
///
/// Shared between Lua (for creating/cancelling timers) and the Hub tick
/// loop (for polling and firing callbacks).
pub struct TimerEntries {
    /// Active timers keyed by unique ID.
    entries: Vec<(String, TimerEntry)>,
    /// Counter for generating unique timer IDs.
    next_id: u64,
    /// Per-plugin stable handler id counters. Kept separate from `next_id`
    /// so hub and worker VMs derive the same handler ids while loading a plugin.
    plugin_handler_counters: HashMap<String, u64>,
    /// Event channel for instant timer delivery (production mode).
    ///
    /// When `Some`, `timer.after()` and `timer.every()` spawn tokio tasks
    /// that send [`HubEvent::TimerFired`] instead of relying on deadline
    /// scanning in [`poll_timers`].
    hub_event_tx: Option<crate::hub::events::HubEventTx>,
    /// Tokio runtime handle for spawning timer tasks from sync Lua closures.
    tokio_handle: Option<tokio::runtime::Handle>,
    /// Worker-local mailbox for timers owned by a plugin worker VM.
    worker_event_tx: Option<PluginWorkerEventTx>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TimerKind {
    After,
    Every,
    AfterIdle,
}

impl TimerKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::After => "after",
            Self::Every => "every",
            Self::AfterIdle => "after_idle",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TimerAttribution {
    pub(crate) timer_id: String,
    pub(crate) kind: &'static str,
    pub(crate) owner_plugin: Option<String>,
    pub(crate) handler_id: Option<String>,
    pub(crate) recurring: bool,
    pub(crate) interval_ms: Option<u128>,
}

fn timer_perf_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("BOTSTER_LUA_PERF")
            .map(|value| value != "0" && !value.is_empty())
            .unwrap_or(false)
    })
}

fn current_plugin_key(lua: &Lua) -> Option<String> {
    let globals = lua.globals();
    globals
        .get::<Option<String>>("_loading_plugin_key")
        .ok()
        .flatten()
        .or_else(|| {
            globals
                .get::<Option<String>>("_loading_plugin_name")
                .ok()
                .flatten()
        })
        .filter(|key| !key.is_empty())
}

fn next_plugin_handler_id(entries: &mut TimerEntries, owner: &str) -> String {
    let next = entries
        .plugin_handler_counters
        .entry(owner.to_string())
        .or_insert(0);
    let handler_id = format!("{owner}:timer_{next}");
    *next += 1;
    handler_id
}

fn duration_from_lua_seconds(primitive: &str, seconds: f64) -> LuaResult<Duration> {
    if !seconds.is_finite() || seconds < 0.0 || seconds > u64::MAX as f64 {
        return Err(LuaError::external(format!(
            "{primitive}: seconds must be a finite non-negative number"
        )));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn normalize_repeating_interval(seconds: f64) -> LuaResult<(Duration, bool)> {
    let interval = duration_from_lua_seconds("timer.every", seconds)?;
    if interval.is_zero() {
        Ok((MIN_REPEATING_TIMER_INTERVAL, true))
    } else {
        Ok((interval, false))
    }
}

fn log_zero_repeating_interval(timer_id: &str, owner_plugin: Option<&str>) {
    log::warn!(
        "[timer] recurring timer requested below-minimum interval; clamped to {}ms id={} owner_plugin={}",
        MIN_REPEATING_TIMER_INTERVAL.as_millis(),
        timer_id,
        owner_plugin.unwrap_or("-")
    );
}

fn explicit_plugin_handler_id(owner: Option<&str>, timer_id: &str) -> Option<String> {
    owner.map(|owner| format!("{owner}:{timer_id}"))
}

enum FiredTimer {
    Local(LuaRegistryKey),
    Plugin {
        owner_plugin: String,
        handler_id: String,
    },
}

fn invoke_plugin_timer(lua: &Lua, owner_plugin: &str, handler_id: &str) -> LuaResult<()> {
    let invoke: LuaFunction = lua.globals().get("__plugin_worker_invoke")?;
    invoke.call::<LuaValue>((
        owner_plugin.to_string(),
        "timer".to_string(),
        handler_id.to_string(),
        LuaValue::Nil,
        lua.create_table()?,
        5000_u64,
    ))?;
    Ok(())
}

fn fire_local_timer(lua: &Lua, key: &LuaRegistryKey) -> LuaResult<()> {
    let callback: LuaFunction = lua.registry_value(key)?;
    callback.call::<()>(())?;
    Ok(())
}

impl Default for TimerEntries {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 0,
            plugin_handler_counters: HashMap::new(),
            hub_event_tx: None,
            tokio_handle: None,
            worker_event_tx: None,
        }
    }
}

impl std::fmt::Debug for TimerEntries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimerEntries")
            .field("active_count", &self.len())
            .field("next_id", &self.next_id)
            .field("event_driven", &self.hub_event_tx.is_some())
            .finish()
    }
}

impl TimerEntries {
    /// Get the number of active (non-cancelled) timers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.iter().filter(|(_, e)| !e.cancelled).count()
    }

    /// Check if no active timers exist.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Inject the event channel and tokio handle for instant timer delivery.
    ///
    /// Called once during Hub initialization. After this, `timer.after()` and
    /// `timer.every()` spawn tokio tasks instead of relying on deadline polling.
    pub(crate) fn set_event_channel(
        &mut self,
        tx: crate::hub::events::HubEventTx,
        handle: tokio::runtime::Handle,
    ) {
        self.hub_event_tx = Some(tx);
        self.tokio_handle = Some(handle);
    }

    /// Route timer firings through the plugin worker mailbox.
    pub(crate) fn set_plugin_worker_event_tx(&mut self, tx: PluginWorkerEventTx) {
        self.worker_event_tx = Some(tx);
    }
}

fn spawn_worker_timer_once(
    tx: PluginWorkerEventTx,
    timer_id: String,
    duration: Duration,
    cancel_flag: Arc<AtomicBool>,
) {
    let _ = thread::Builder::new()
        .name(format!("botster-plugin-timer-{timer_id}"))
        .spawn(move || {
            thread::sleep(duration);
            if !cancel_flag.load(Ordering::SeqCst) {
                let _ = tx.send_timer_fired(timer_id);
            }
        });
}

fn spawn_worker_timer_repeating(
    tx: PluginWorkerEventTx,
    timer_id: String,
    interval: Duration,
    cancel_flag: Arc<AtomicBool>,
) {
    let _ = thread::Builder::new()
        .name(format!("botster-plugin-timer-{timer_id}"))
        .spawn(move || {
            while !cancel_flag.load(Ordering::SeqCst) {
                thread::sleep(interval);
                if cancel_flag.load(Ordering::SeqCst) || !tx.send_timer_fired(timer_id.clone()) {
                    break;
                }
            }
        });
}

/// Thread-safe handle to the timer registry.
pub type TimerRegistry = Arc<Mutex<TimerEntries>>;

/// Create a new shared timer registry.
#[must_use]
pub fn new_timer_registry() -> TimerRegistry {
    Arc::new(Mutex::new(TimerEntries::default()))
}

/// Register timer primitives with the Lua state.
///
/// Adds the following functions to the global `timer` table:
/// - `timer.after(seconds, callback)` -> timer_id (one-shot)
/// - `timer.every(seconds, callback)` -> timer_id (repeating)
/// - `timer.cancel(timer_id)` -> boolean
///
/// Durations are in **seconds** (`f64`), converted via
/// [`Duration::from_secs_f64`].
///
/// # Arguments
///
/// * `lua` - The Lua state to register primitives in
/// * `registry` - Shared timer registry for storing active timers
///
/// # Errors
///
/// Returns an error if Lua table or function creation fails.
pub fn register(lua: &Lua, registry: TimerRegistry) -> Result<()> {
    let timer_table = lua
        .create_table()
        .map_err(|e| anyhow!("Failed to create timer table: {e}"))?;

    // timer.after(seconds, callback) -> timer_id
    //
    // Creates a one-shot timer that fires the callback after `seconds` seconds.
    // In production (event channel available), spawns a tokio task that sleeps
    // then sends `HubEvent::TimerFired`. In test mode, uses deadline-based polling.
    let reg = Arc::clone(&registry);
    let after_fn = lua
        .create_function(move |lua, (seconds, callback): (f64, LuaFunction)| {
            let owner_plugin = current_plugin_key(lua);
            let callback_key = lua.create_registry_value(callback).map_err(|e| {
                LuaError::external(format!("timer.after: failed to store callback: {e}"))
            })?;

            let mut entries = reg.lock().expect("TimerEntries mutex poisoned");
            let id = format!("timer_{}", entries.next_id);
            entries.next_id += 1;
            let handler_id = owner_plugin
                .as_deref()
                .map(|owner| next_plugin_handler_id(&mut entries, owner));

            let duration = duration_from_lua_seconds("timer.after", seconds)?;

            // Spawn a delivery task when an event sink is available.
            // Plugin workers own plugin timer delivery. The Hub keeps a
            // matching registration for ownership and reload cleanup only.
            let cancel_flag = entries.worker_event_tx.as_ref().map(|tx| {
                let cancel_flag = Arc::new(AtomicBool::new(false));
                spawn_worker_timer_once(tx.clone(), id.clone(), duration, cancel_flag.clone());
                cancel_flag
            });
            let task_handle = match (
                owner_plugin.is_none() && entries.worker_event_tx.is_none(),
                &entries.hub_event_tx,
                &entries.tokio_handle,
            ) {
                (true, Some(tx), Some(handle)) => {
                    let tx = tx.clone();
                    let timer_id = id.clone();
                    Some(handle.spawn(async move {
                        tokio::time::sleep(duration).await;
                        let _ = tx.send(HubEvent::TimerFired { timer_id });
                    }))
                }
                _ => None,
            };

            entries.entries.push((
                id.clone(),
                TimerEntry {
                    callback_key,
                    handler_id,
                    owner_plugin,
                    fire_at: Instant::now() + duration,
                    repeat_interval: None,
                    kind: TimerKind::After,
                    cancelled: false,
                    task_handle,
                    cancel_flag,
                },
            ));

            Ok(id)
        })
        .map_err(|e| anyhow!("Failed to create timer.after function: {e}"))?;

    timer_table
        .set("after", after_fn)
        .map_err(|e| anyhow!("Failed to set timer.after: {e}"))?;

    // timer.every(seconds, callback) -> timer_id
    //
    // Creates a repeating timer that fires the callback every `seconds` seconds.
    // In production, spawns a looping tokio task. In test mode, uses polling.
    let reg2 = Arc::clone(&registry);
    let every_fn = lua
        .create_function(move |lua, (seconds, callback): (f64, LuaFunction)| {
            let owner_plugin = current_plugin_key(lua);
            let callback_key = lua.create_registry_value(callback).map_err(|e| {
                LuaError::external(format!("timer.every: failed to store callback: {e}"))
            })?;

            let (interval, clamped) = normalize_repeating_interval(seconds)?;
            let mut entries = reg2.lock().expect("TimerEntries mutex poisoned");
            let id = format!("timer_{}", entries.next_id);
            entries.next_id += 1;
            let handler_id = owner_plugin
                .as_deref()
                .map(|owner| next_plugin_handler_id(&mut entries, owner));
            if clamped {
                log_zero_repeating_interval(&id, owner_plugin.as_deref());
            }

            // Spawn looping delivery task if an event sink is available.
            // Plugin workers own plugin timer delivery. The Hub registration
            // must not schedule a second copy on the central event loop.
            let cancel_flag = entries.worker_event_tx.as_ref().map(|tx| {
                let cancel_flag = Arc::new(AtomicBool::new(false));
                spawn_worker_timer_repeating(tx.clone(), id.clone(), interval, cancel_flag.clone());
                cancel_flag
            });
            let task_handle = match (
                owner_plugin.is_none() && entries.worker_event_tx.is_none(),
                &entries.hub_event_tx,
                &entries.tokio_handle,
            ) {
                (true, Some(tx), Some(handle)) => {
                    let tx = tx.clone();
                    let timer_id = id.clone();
                    Some(handle.spawn(async move {
                        loop {
                            tokio::time::sleep(interval).await;
                            if tx
                                .send(HubEvent::TimerFired {
                                    timer_id: timer_id.clone(),
                                })
                                .is_err()
                            {
                                break;
                            }
                        }
                    }))
                }
                _ => None,
            };

            entries.entries.push((
                id.clone(),
                TimerEntry {
                    callback_key,
                    handler_id,
                    owner_plugin,
                    fire_at: Instant::now() + interval,
                    repeat_interval: Some(interval),
                    kind: TimerKind::Every,
                    cancelled: false,
                    task_handle,
                    cancel_flag,
                },
            ));

            Ok(id)
        })
        .map_err(|e| anyhow!("Failed to create timer.every function: {e}"))?;

    timer_table
        .set("every", every_fn)
        .map_err(|e| anyhow!("Failed to set timer.every: {e}"))?;

    // timer.after_idle(id, seconds, callback) -> id
    //
    // Resettable one-shot timer: each call with the same `id` cancels any
    // previous timer and starts a fresh one. The callback fires only after
    // `seconds` pass with no further call. No polling — uses a tokio sleep
    // task that is aborted and replaced on each reset.
    //
    // Use for inactivity detection: call on each activity event, the callback
    // fires only when activity stops for the specified duration.
    let reg_idle = Arc::clone(&registry);
    let after_idle_fn = lua
        .create_function(
            move |lua, (id, seconds, callback): (String, f64, LuaFunction)| {
                let owner_plugin = current_plugin_key(lua);
                let callback_key = lua.create_registry_value(callback).map_err(|e| {
                    LuaError::external(format!("timer.after_idle: failed to store callback: {e}"))
                })?;

                let duration = duration_from_lua_seconds("timer.after_idle", seconds)?;
                let mut entries = reg_idle.lock().expect("TimerEntries mutex poisoned");

                // Refresh a single slot for this ID instead of appending a new
                // cancelled entry every reset. Hot paths such as PTY idle
                // detection call after_idle on nearly every output chunk.
                // Plugin workers own plugin timer delivery. The Hub keeps the
                // stable registration without a duplicate delivery task.
                let cancel_flag = entries.worker_event_tx.as_ref().map(|tx| {
                    let cancel_flag = Arc::new(AtomicBool::new(false));
                    spawn_worker_timer_once(tx.clone(), id.clone(), duration, cancel_flag.clone());
                    cancel_flag
                });

                let task_handle = match (
                    owner_plugin.is_none() && entries.worker_event_tx.is_none(),
                    &entries.hub_event_tx,
                    &entries.tokio_handle,
                ) {
                    (true, Some(tx), Some(handle)) => {
                        let tx = tx.clone();
                        let timer_id = id.clone();
                        Some(handle.spawn(async move {
                            tokio::time::sleep(duration).await;
                            let _ = tx.send(HubEvent::TimerFired { timer_id });
                        }))
                    }
                    _ => None,
                };

                let handler_id = explicit_plugin_handler_id(owner_plugin.as_deref(), &id);

                if let Some((_, entry)) = entries.entries.iter_mut().find(|(eid, _)| *eid == id) {
                    if let Some(handle) = entry.task_handle.take() {
                        handle.abort();
                    }
                    if let Some(flag) = entry.cancel_flag.take() {
                        flag.store(true, Ordering::SeqCst);
                    }
                    let old_key = std::mem::replace(&mut entry.callback_key, callback_key);
                    let _ = lua.remove_registry_value(old_key);
                    entry.handler_id = handler_id;
                    entry.owner_plugin = owner_plugin;
                    entry.fire_at = Instant::now() + duration;
                    entry.repeat_interval = None;
                    entry.kind = TimerKind::AfterIdle;
                    entry.cancelled = false;
                    entry.task_handle = task_handle;
                    entry.cancel_flag = cancel_flag;
                } else {
                    entries.entries.push((
                        id.clone(),
                        TimerEntry {
                            callback_key,
                            handler_id,
                            owner_plugin,
                            fire_at: Instant::now() + duration,
                            repeat_interval: None,
                            kind: TimerKind::AfterIdle,
                            cancelled: false,
                            task_handle,
                            cancel_flag,
                        },
                    ));
                }

                Ok(id)
            },
        )
        .map_err(|e| anyhow!("Failed to create timer.after_idle function: {e}"))?;

    timer_table
        .set("after_idle", after_idle_fn)
        .map_err(|e| anyhow!("Failed to set timer.after_idle: {e}"))?;

    // timer.cancel(timer_id) -> boolean
    //
    // Marks a timer as cancelled and aborts its spawned task (if any).
    // Returns true if the timer was found.
    let reg3 = registry;
    let reg4 = Arc::clone(&reg3);
    let reg5 = Arc::clone(&reg3);
    let cancel_fn = lua
        .create_function(move |_, timer_id: String| {
            let mut entries = reg3.lock().expect("TimerEntries mutex poisoned");

            for (id, entry) in &mut entries.entries {
                if *id == timer_id && !entry.cancelled {
                    entry.cancelled = true;
                    // Abort the spawned timer task if running (production mode).
                    if let Some(handle) = entry.task_handle.take() {
                        handle.abort();
                    }
                    if let Some(flag) = entry.cancel_flag.take() {
                        flag.store(true, Ordering::SeqCst);
                    }
                    return Ok(true);
                }
            }

            Ok(false)
        })
        .map_err(|e| anyhow!("Failed to create timer.cancel function: {e}"))?;

    timer_table
        .set("cancel", cancel_fn)
        .map_err(|e| anyhow!("Failed to set timer.cancel: {e}"))?;

    let invoke_fn = lua
        .create_function(move |lua, handler_id: String| {
            let callback_key = {
                let entries = reg4.lock().expect("TimerEntries mutex poisoned");
                let Some((_, entry)) = entries.entries.iter().find(|(_, entry)| {
                    entry.handler_id.as_deref() == Some(handler_id.as_str()) && !entry.cancelled
                }) else {
                    return Err(LuaError::external(format!(
                        "timer handler not registered: {handler_id}"
                    )));
                };
                let callback: LuaFunction = lua.registry_value(&entry.callback_key)?;
                lua.create_registry_value(callback)?
            };

            let result = (|| {
                let callback: LuaFunction = lua.registry_value(&callback_key)?;
                callback.call::<LuaValue>(())
            })();
            let _ = lua.remove_registry_value(callback_key);
            result
        })
        .map_err(|e| anyhow!("Failed to create timer._invoke_registered function: {e}"))?;

    timer_table
        .set("_invoke_registered", invoke_fn)
        .map_err(|e| anyhow!("Failed to set timer._invoke_registered: {e}"))?;

    let unregister_by_plugin_fn = lua
        .create_function(move |lua, owner_plugin: String| {
            if owner_plugin.is_empty() {
                return Ok(0);
            }

            let removed = {
                let mut entries = reg5.lock().expect("TimerEntries mutex poisoned");

                for (_, entry) in &mut entries.entries {
                    if entry.owner_plugin.as_deref() == Some(owner_plugin.as_str()) {
                        entry.cancelled = true;
                        if let Some(handle) = entry.task_handle.take() {
                            handle.abort();
                        }
                        if let Some(flag) = entry.cancel_flag.take() {
                            flag.store(true, Ordering::SeqCst);
                        }
                    }
                }

                let mut removed = Vec::new();
                let mut index = 0;
                while index < entries.entries.len() {
                    if entries.entries[index].1.owner_plugin.as_deref()
                        == Some(owner_plugin.as_str())
                        && entries.entries[index].1.cancelled
                    {
                        removed.push(entries.entries.remove(index));
                    } else {
                        index += 1;
                    }
                }
                entries.plugin_handler_counters.remove(&owner_plugin);
                removed
            };

            let count = removed.len();
            for (_, entry) in removed {
                let _ = lua.remove_registry_value(entry.callback_key);
            }

            Ok(count)
        })
        .map_err(|e| anyhow!("Failed to create timer._unregister_by_plugin function: {e}"))?;

    timer_table
        .set("_unregister_by_plugin", unregister_by_plugin_fn)
        .map_err(|e| anyhow!("Failed to set timer._unregister_by_plugin: {e}"))?;

    lua.globals()
        .set("timer", timer_table)
        .map_err(|e| anyhow!("Failed to register timer table globally: {e}"))?;

    Ok(())
}

/// Poll all timers, fire callbacks for elapsed timers, and clean up.
///
/// Called from the Hub tick loop each tick. For each timer:
/// - Skip cancelled timers (remove them)
/// - Check if `fire_at` has elapsed
/// - Fire the callback
/// - For one-shot timers: remove after firing
/// - For repeating timers: reschedule `fire_at` to the next interval
///
/// # Deadlock Prevention
///
/// Fired timer IDs and callback keys are collected under the lock,
/// then the lock is released before calling Lua. This allows callbacks
/// to call `timer.cancel()` or create new timers without deadlocking.
///
/// # Returns
///
/// The number of timer callbacks fired.
pub fn poll_timers(lua: &Lua, registry: &TimerRegistry) -> usize {
    let now = Instant::now();

    // Phase 1: collect fired timers and clean up under the lock.
    let fired_timers: Vec<FiredTimer> = {
        let mut entries = registry.lock().expect("TimerEntries mutex poisoned");

        // Production runtimes use event-driven delivery. Polling those same
        // entries would create a second delivery path for every timer.
        if entries.hub_event_tx.is_some() || entries.worker_event_tx.is_some() {
            let removed: Vec<_> = entries
                .entries
                .drain_filter_compat()
                .into_iter()
                .map(|(_, entry)| entry.callback_key)
                .collect();
            for key in removed {
                let _ = lua.remove_registry_value(key);
            }
            return 0;
        }

        // Collect callback keys for timers that should fire
        let mut fired = Vec::new();

        for (_, entry) in &mut entries.entries {
            if entry.cancelled {
                continue;
            }

            if now >= entry.fire_at {
                if let (Some(owner_plugin), Some(handler_id)) =
                    (&entry.owner_plugin, &entry.handler_id)
                {
                    fired.push(FiredTimer::Plugin {
                        owner_plugin: owner_plugin.clone(),
                        handler_id: handler_id.clone(),
                    });
                } else {
                    // Clone the callback key for firing outside the lock
                    if let Ok(callback) = lua.registry_value::<LuaFunction>(&entry.callback_key) {
                        if let Ok(key) = lua.create_registry_value(callback) {
                            fired.push(FiredTimer::Local(key));
                        }
                    }
                }

                if let Some(interval) = entry.repeat_interval {
                    // Reschedule repeating timer
                    entry.fire_at = now + interval;
                } else {
                    // Mark one-shot timer for removal
                    entry.cancelled = true;
                }
            }
        }

        // Remove cancelled/completed entries and clean up their registry keys
        let removed: Vec<_> = entries
            .entries
            .drain_filter_compat()
            .into_iter()
            .map(|(_, entry)| entry.callback_key)
            .collect();

        for key in removed {
            let _ = lua.remove_registry_value(key);
        }

        fired
    };
    // Lock released here — callbacks can safely call timer functions.

    // Phase 2: fire callbacks without holding the lock.
    let count = fired_timers.len();

    let mut local_keys = Vec::new();
    for fired in fired_timers {
        let result = match fired {
            FiredTimer::Local(key) => {
                let result = fire_local_timer(lua, &key);
                local_keys.push(key);
                result
            }
            FiredTimer::Plugin {
                owner_plugin,
                handler_id,
            } => invoke_plugin_timer(lua, &owner_plugin, &handler_id),
        };

        if let Err(e) = result {
            log::warn!("[timer] Callback error: {e}");
        }
    }

    // Phase 3: clean up temporary registry keys.
    for key in local_keys {
        let _ = lua.remove_registry_value(key);
    }

    count
}

/// Fire the Lua callback for a single timer event.
///
/// Called from [`handle_hub_event`] for [`HubEvent::TimerFired`] events.
/// Looks up the timer entry by ID, clones the callback key, releases the
/// lock, fires the callback, then cleans up.
///
/// For one-shot timers, the entry is marked cancelled and removed.
/// For repeating timers, the entry stays alive (the looping tokio task
/// will send another `TimerFired` after the next interval).
///
/// # Deadlock Prevention
///
/// The registry lock is released before firing the Lua callback, allowing
/// the callback to call `timer.cancel()` or create new timers.
pub(crate) fn fire_single_timer(lua: &Lua, registry: &TimerRegistry, timer_id: &str) {
    let perf = timer_perf_enabled();
    let total_started = Instant::now();

    // Phase 1: look up entry under lock, clone callback, handle one-shot.
    let (fired_timer, one_shot, attribution) = {
        let mut entries = registry.lock().expect("TimerEntries mutex poisoned");

        let entry_pos = entries
            .entries
            .iter()
            .position(|(id, e)| id == timer_id && !e.cancelled);

        let Some(pos) = entry_pos else {
            // Timer was cancelled between send and receive — race is benign.
            return;
        };

        let worker_local_timer = entries.worker_event_tx.is_some();
        let entry = &mut entries.entries[pos].1;

        let fired = if !worker_local_timer {
            if let (Some(owner_plugin), Some(handler_id)) = (&entry.owner_plugin, &entry.handler_id)
            {
                FiredTimer::Plugin {
                    owner_plugin: owner_plugin.clone(),
                    handler_id: handler_id.clone(),
                }
            } else {
                // Clone the callback for firing outside the lock.
                let callback = match lua.registry_value::<LuaFunction>(&entry.callback_key) {
                    Ok(cb) => cb,
                    Err(e) => {
                        log::warn!("[timer] Failed to retrieve callback for {timer_id}: {e}");
                        return;
                    }
                };
                let cloned_key = match lua.create_registry_value(callback) {
                    Ok(k) => k,
                    Err(e) => {
                        log::warn!("[timer] Failed to clone callback for {timer_id}: {e}");
                        return;
                    }
                };
                FiredTimer::Local(cloned_key)
            }
        } else {
            // Plugin worker timers are already executing inside the worker VM,
            // so their callbacks should run locally instead of re-entering
            // the worker dispatch bridge.
            let callback = match lua.registry_value::<LuaFunction>(&entry.callback_key) {
                Ok(cb) => cb,
                Err(e) => {
                    log::warn!("[timer] Failed to retrieve callback for {timer_id}: {e}");
                    return;
                }
            };
            let cloned_key = match lua.create_registry_value(callback) {
                Ok(k) => k,
                Err(e) => {
                    log::warn!("[timer] Failed to clone callback for {timer_id}: {e}");
                    return;
                }
            };
            FiredTimer::Local(cloned_key)
        };

        let one_shot = entry.repeat_interval.is_none();
        let attribution = TimerAttribution {
            timer_id: timer_id.to_string(),
            kind: entry.kind.as_str(),
            owner_plugin: entry.owner_plugin.clone(),
            handler_id: entry.handler_id.clone(),
            recurring: entry.repeat_interval.is_some(),
            interval_ms: entry.repeat_interval.map(|duration| duration.as_millis()),
        };

        (fired, one_shot, attribution)
    };
    let lookup_done = Instant::now();
    // Lock released — callback can safely call timer functions.

    // Phase 2: fire callback.
    let result = match &fired_timer {
        FiredTimer::Local(key) => fire_local_timer(lua, key),
        FiredTimer::Plugin {
            owner_plugin,
            handler_id,
        } => invoke_plugin_timer(lua, owner_plugin, handler_id),
    };
    let callback_done = Instant::now();

    if let Err(e) = result {
        log::warn!(
            "[timer] Callback error id={} kind={} owner_plugin={} handler_id={} recurring={}: {}",
            attribution.timer_id,
            attribution.kind,
            attribution.owner_plugin.as_deref().unwrap_or("-"),
            attribution.handler_id.as_deref().unwrap_or("-"),
            attribution.recurring,
            e
        );
    }

    // Phase 3: clean up temporary registry key.
    if let FiredTimer::Local(key) = fired_timer {
        let _ = lua.remove_registry_value(key);
    }

    // Phase 4: mark completed one-shot timers, then remove cancelled entries
    // and clean up their registry keys. Plugin-owned timers must remain
    // registered until after the worker VM has looked up the handler.
    {
        let mut entries = registry.lock().expect("TimerEntries mutex poisoned");
        if one_shot {
            if let Some((_, entry)) = entries.entries.iter_mut().find(|(id, _)| id == timer_id) {
                entry.cancelled = true;
            }
        }
        let removed: Vec<_> = entries.entries.drain_filter_compat();
        for (_, entry) in removed {
            let _ = lua.remove_registry_value(entry.callback_key);
        }
    }
    let cleanup_done = Instant::now();
    let callback_ms = callback_done.duration_since(lookup_done).as_millis();

    if attribution.recurring && callback_ms >= SLOW_RECURRING_TIMER_MS {
        log::warn!(
            "[timer] slow recurring callback id={} kind={} owner_plugin={} handler_id={} interval_ms={} callback_ms={} total_ms={}",
            attribution.timer_id,
            attribution.kind,
            attribution.owner_plugin.as_deref().unwrap_or("-"),
            attribution.handler_id.as_deref().unwrap_or("-"),
            attribution
                .interval_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            callback_ms,
            cleanup_done.duration_since(total_started).as_millis()
        );
    }

    if perf {
        log::info!(
            "[PERF][lua] timer_fired id={} kind={} owner_plugin={} handler_id={} recurring={} interval_ms={} lookup_us={} callback_us={} cleanup_us={} total_us={}",
            attribution.timer_id,
            attribution.kind,
            attribution.owner_plugin.as_deref().unwrap_or("-"),
            attribution.handler_id.as_deref().unwrap_or("-"),
            attribution.recurring,
            attribution
                .interval_ms
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            lookup_done.duration_since(total_started).as_micros(),
            callback_done.duration_since(lookup_done).as_micros(),
            cleanup_done.duration_since(callback_done).as_micros(),
            cleanup_done.duration_since(total_started).as_micros()
        );
    }
}

/// Helper trait to emulate `Vec::drain_filter` on stable Rust.
trait DrainFilterCompat<T> {
    fn drain_filter_compat(&mut self) -> Vec<T>;
}

impl DrainFilterCompat<(String, TimerEntry)> for Vec<(String, TimerEntry)> {
    fn drain_filter_compat(&mut self) -> Vec<(String, TimerEntry)> {
        let mut removed = Vec::new();
        let mut i = 0;
        while i < self.len() {
            if self[i].1.cancelled {
                removed.push(self.remove(i));
            } else {
                i += 1;
            }
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_timer_registry() {
        let registry = new_timer_registry();
        let entries = registry.lock().expect("mutex");
        assert!(entries.is_empty());
        assert_eq!(entries.next_id, 0);
    }

    #[test]
    fn test_register_creates_timer_table() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, registry).expect("Should register timer primitives");

        let timer_table: LuaTable = lua
            .globals()
            .get("timer")
            .expect("timer table should exist");
        assert!(timer_table.contains_key("after").expect("key check"));
        assert!(timer_table.contains_key("every").expect("key check"));
        assert!(timer_table.contains_key("cancel").expect("key check"));
    }

    #[test]
    fn test_after_creates_oneshot_timer() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        let id: String = lua
            .load(
                r#"
                return timer.after(1.0, function()
                end)
            "#,
            )
            .eval()
            .expect("timer.after should succeed");

        assert!(id.starts_with("timer_"));

        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.entries.len(), 1);
        assert!(entries.entries[0].1.repeat_interval.is_none());
    }

    #[test]
    fn test_every_creates_repeating_timer() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        let id: String = lua
            .load(
                r#"
                return timer.every(0.5, function()
                end)
            "#,
            )
            .eval()
            .expect("timer.every should succeed");

        assert!(id.starts_with("timer_"));

        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.entries.len(), 1);
        assert_eq!(
            entries.entries[0].1.repeat_interval,
            Some(Duration::from_secs_f64(0.5))
        );
    }

    #[test]
    fn test_every_zero_interval_is_clamped() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load("timer.every(0, function() end)")
            .exec()
            .expect("timer.every should succeed");

        let entries = registry.lock().expect("mutex");
        assert_eq!(
            entries.entries[0].1.repeat_interval,
            Some(MIN_REPEATING_TIMER_INTERVAL)
        );
        assert_eq!(entries.entries[0].1.kind, TimerKind::Every);
    }

    #[test]
    fn test_timer_rejects_invalid_durations() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load(
            r#"
            assert(pcall(function() timer.after(-1, function() end) end) == false)
            assert(pcall(function() timer.every(0/0, function() end) end) == false)
            assert(pcall(function() timer.after_idle("idle:bad", math.huge, function() end) end) == false)
            "#,
        )
        .exec()
        .expect("invalid durations should be Lua errors");

        let entries = registry.lock().expect("mutex");
        assert!(entries.entries.is_empty());
    }

    #[test]
    fn test_plugin_timer_records_attribution() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.globals()
            .set("_loading_plugin_key", "demo_plugin")
            .expect("set plugin key");
        let id: String = lua
            .load("return timer.every(1, function() end)")
            .eval()
            .expect("timer.every should succeed");

        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.entries[0].0, id);
        assert_eq!(
            entries.entries[0].1.owner_plugin.as_deref(),
            Some("demo_plugin")
        );
        assert_eq!(
            entries.entries[0].1.handler_id.as_deref(),
            Some("demo_plugin:timer_0")
        );
        assert_eq!(entries.entries[0].1.kind, TimerKind::Every);
    }

    #[test]
    fn test_cancel_existing_timer() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load(
            r#"
            my_timer_id = timer.after(10, function() end)
        "#,
        )
        .exec()
        .expect("create timer");

        let cancelled: bool = lua
            .load(r#"return timer.cancel(my_timer_id)"#)
            .eval()
            .expect("cancel should succeed");

        assert!(cancelled);

        // Verify timer is marked cancelled
        let entries = registry.lock().expect("mutex");
        assert!(entries.entries[0].1.cancelled);
    }

    #[test]
    fn test_cancel_nonexistent_returns_false() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, registry).expect("Should register");

        let cancelled: bool = lua
            .load(r#"return timer.cancel("nonexistent_id")"#)
            .eval()
            .expect("cancel should not error");

        assert!(!cancelled);
    }

    #[test]
    fn test_poll_timers_empty_registry() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        let count = poll_timers(&lua, &registry);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_poll_fires_elapsed_oneshot() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        // Set up a counter
        lua.load("fired_count = 0").exec().expect("setup counter");

        // Create a timer with 0s delay (fires immediately)
        lua.load(
            r#"
            timer.after(0, function()
                fired_count = fired_count + 1
            end)
        "#,
        )
        .exec()
        .expect("create timer");

        // Small sleep to ensure the instant has passed
        std::thread::sleep(Duration::from_millis(5));

        let count = poll_timers(&lua, &registry);
        assert_eq!(count, 1);

        let fired: i32 = lua.load("return fired_count").eval().expect("read counter");
        assert_eq!(fired, 1);

        // One-shot should be removed after firing
        let entries = registry.lock().expect("mutex");
        assert!(
            entries.entries.is_empty(),
            "One-shot timer should be removed after firing"
        );
    }

    #[test]
    fn test_poll_fires_repeating_and_reschedules() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load("repeat_count = 0").exec().expect("setup counter");

        // Create a repeating timer with 0s interval. The primitive clamps
        // zero-second recurring timers to avoid hot loops.
        lua.load(
            r#"
            timer.every(0, function()
                repeat_count = repeat_count + 1
            end)
        "#,
        )
        .exec()
        .expect("create timer");

        std::thread::sleep(MIN_REPEATING_TIMER_INTERVAL + Duration::from_millis(5));

        // First poll should fire
        let count = poll_timers(&lua, &registry);
        assert_eq!(count, 1);

        // Repeating timer should still be in the registry
        {
            let entries = registry.lock().expect("mutex");
            assert_eq!(
                entries.entries.len(),
                1,
                "Repeating timer should still exist"
            );
            assert!(!entries.entries[0].1.cancelled);
        }

        std::thread::sleep(MIN_REPEATING_TIMER_INTERVAL + Duration::from_millis(5));

        // Second poll should fire again
        let count = poll_timers(&lua, &registry);
        assert_eq!(count, 1);

        let fired: i32 = lua
            .load("return repeat_count")
            .eval()
            .expect("read counter");
        assert_eq!(fired, 2);
    }

    #[test]
    fn test_poll_skips_cancelled_timers() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load("should_not_fire = false").exec().expect("setup");

        // Create and immediately cancel
        lua.load(
            r#"
            local id = timer.after(0, function()
                should_not_fire = true
            end)
            timer.cancel(id)
        "#,
        )
        .exec()
        .expect("create and cancel");

        std::thread::sleep(Duration::from_millis(5));

        let count = poll_timers(&lua, &registry);
        assert_eq!(count, 0);

        let fired: bool = lua
            .load("return should_not_fire")
            .eval()
            .expect("read flag");
        assert!(!fired, "Cancelled timer should not fire");
    }

    #[test]
    fn test_poll_does_not_fire_future_timers() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load("future_fired = false").exec().expect("setup");

        // Create a timer far in the future (60 seconds)
        lua.load(
            r#"
            timer.after(60, function()
                future_fired = true
            end)
        "#,
        )
        .exec()
        .expect("create timer");

        let count = poll_timers(&lua, &registry);
        assert_eq!(count, 0);

        let fired: bool = lua.load("return future_fired").eval().expect("read flag");
        assert!(!fired, "Future timer should not fire yet");

        // Timer should still be in registry
        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.entries.len(), 1);
    }

    #[test]
    fn test_unique_timer_ids() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        let id1: String = lua
            .load("return timer.after(10, function() end)")
            .eval()
            .expect("timer 1");

        let id2: String = lua
            .load("return timer.after(10, function() end)")
            .eval()
            .expect("timer 2");

        let id3: String = lua
            .load("return timer.every(10, function() end)")
            .eval()
            .expect("timer 3");

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_timer_entries_len_excludes_cancelled() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load(
            r#"
            timer.after(10, function() end)
            local id = timer.after(10, function() end)
            timer.cancel(id)
        "#,
        )
        .exec()
        .expect("create timers");

        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.len(), 1, "len() should exclude cancelled timers");
        assert!(!entries.is_empty());
    }

    #[test]
    fn test_after_idle_reuses_same_entry_for_same_id() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load(
            r#"
            timer.after_idle("idle:test", 10, function() end)
            timer.after_idle("idle:test", 10, function() end)
            timer.after_idle("idle:test", 10, function() end)
        "#,
        )
        .exec()
        .expect("create idle timers");

        let entries = registry.lock().expect("mutex");
        assert_eq!(
            entries.entries.len(),
            1,
            "same-id after_idle resets should not accumulate entries"
        );
        assert_eq!(entries.entries[0].0, "idle:test");
        assert!(!entries.entries[0].1.cancelled);
    }

    #[test]
    fn test_unregister_by_plugin_cancels_owned_timers_only() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        let removed: i32 = lua
            .load(
                r#"
                _G._loading_plugin_key = "owned-plugin"
                timer.after(10, function() end)
                timer.after_idle("owned:idle", 10, function() end)
                _G._loading_plugin_key = nil
                timer.after(10, function() end)
                local unowned_cancelled = timer.after(10, function() end)
                timer.cancel(unowned_cancelled)
                return timer._unregister_by_plugin("owned-plugin")
                "#,
            )
            .eval()
            .expect("unregister plugin timers");

        assert_eq!(removed, 2);

        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.entries.len(), 2);
        assert_eq!(entries.entries[0].1.owner_plugin, None);
        assert_eq!(entries.entries[1].1.owner_plugin, None);
        assert!(entries.entries[1].1.cancelled);
    }

    #[test]
    fn test_unregister_by_plugin_resets_handler_counter() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load(
            r#"
            _G._loading_plugin_key = "owned-plugin"
            timer.every(10, function() end)
            timer.every(10, function() end)
            _G._loading_plugin_key = nil
            assert(timer._unregister_by_plugin("owned-plugin") == 2)
            _G._loading_plugin_key = "owned-plugin"
            timer.every(10, function() end)
            "#,
        )
        .exec()
        .expect("reset plugin timer counter");

        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.entries.len(), 1);
        assert_eq!(
            entries.entries[0].1.handler_id.as_deref(),
            Some("owned-plugin:timer_0")
        );
    }

    #[test]
    fn plugin_owned_hub_timers_keep_registration_without_delivery_tasks() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime");
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
        let lua = Lua::new();
        let registry = new_timer_registry();

        registry.lock().expect("mutex").set_event_channel(
            crate::hub::events::HubEventTx::from(event_tx),
            runtime.handle().clone(),
        );
        register(&lua, Arc::clone(&registry)).expect("register timers");

        lua.load(
            r#"
            _G._loading_plugin_key = "owned-plugin"
            timer.after(10, function() end)
            timer.every(10, function() end)
            timer.after_idle("owned:idle", 10, function() end)
            _G._loading_plugin_key = nil
            "#,
        )
        .exec()
        .expect("register plugin timers");

        let entries = registry.lock().expect("mutex");
        assert_eq!(entries.entries.len(), 3);
        assert!(entries.entries.iter().all(|(_, entry)| {
            entry.owner_plugin.as_deref() == Some("owned-plugin") && entry.task_handle.is_none()
        }));
    }

    #[test]
    fn event_driven_timers_do_not_fire_through_poll_fallback() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("tokio runtime");
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
        let lua = Lua::new();
        let registry = new_timer_registry();

        registry.lock().expect("mutex").set_event_channel(
            crate::hub::events::HubEventTx::from(event_tx),
            runtime.handle().clone(),
        );
        register(&lua, Arc::clone(&registry)).expect("register timers");
        lua.load(
            r#"
            fired_by_poll = false
            timer.after(0, function() fired_by_poll = true end)
            "#,
        )
        .exec()
        .expect("register event-driven timer");

        assert_eq!(poll_timers(&lua, &registry), 0);
        assert!(!lua
            .globals()
            .get::<bool>("fired_by_poll")
            .expect("read callback flag"));
    }

    #[test]
    fn test_cancel_from_callback_no_deadlock() {
        let lua = Lua::new();
        let registry = new_timer_registry();

        register(&lua, Arc::clone(&registry)).expect("Should register");

        lua.load("cancel_worked = false").exec().expect("setup");

        // Create a timer that cancels another timer from its callback
        lua.load(
            r#"
            local target_id = timer.after(0, function() end)
            timer.after(0, function()
                cancel_worked = timer.cancel(target_id)
            end)
        "#,
        )
        .exec()
        .expect("create timers");

        std::thread::sleep(Duration::from_millis(5));

        // This would deadlock if we held the lock during callbacks
        let _ = poll_timers(&lua, &registry);

        // The cancel-from-callback may or may not succeed depending on
        // which timer fires first, but it must not deadlock
    }
}
