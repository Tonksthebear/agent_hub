//! Hub daemon infrastructure for PID file management and socket discovery.
//!
//! Provides utilities for detecting running hubs, managing PID files,
//! and resolving Unix socket paths for IPC.
//!
//! # File Layout
//!
//! ```text
//! {config_dir}/hubs/{hub_id}/
//!   hub.pid              # PID of the running hub process
//!
//! /tmp/botster-{uid}/
//!   {hub_id}.sock        # Unix domain socket for IPC
//! ```
//!
//! Sockets live in `/tmp` because macOS limits Unix socket paths to 104 bytes,
//! and `~/Library/Application Support/...` exceeds that.

use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// RAII exclusive lock on a per-hub lock file.
///
/// Acquired via [`try_lock_hub`] before socket bind / PID writes.
/// The OS-level `flock` is released when this struct is dropped,
/// which happens when the `Hub` that owns it is dropped or shut down.
#[derive(Debug)]
pub struct HubLock {
    /// Kept open to hold the flock. Dropped = lock released.
    _file: File,
    /// Path stored for diagnostics only.
    pub path: PathBuf,
}

/// Read-only snapshot of a hub's runtime artifacts.
///
/// This is intentionally diagnostic: it does not remove or repair files.
/// Cleanup and repair paths must opt into mutation explicitly.
#[derive(Debug, Clone)]
pub struct HubArtifactInspection {
    /// Stable local hub hash ID.
    pub hub_id: String,
    /// PID file path.
    pub pid_file_path: PathBuf,
    /// PID read from the PID file, if present and parseable.
    pub pid: Option<u32>,
    /// Whether the PID currently appears live.
    pub pid_alive: bool,
    /// Runtime manifest path.
    pub manifest_path: PathBuf,
    /// Parsed runtime manifest, if present and valid.
    pub manifest: Option<HubManifest>,
    /// Socket path advertised by the manifest, or the expected local path.
    pub socket_path: PathBuf,
    /// Whether the socket pathname exists in the filesystem.
    pub socket_path_exists: bool,
    /// Whether a new UnixStream can connect to the socket path.
    pub socket_connectable: bool,
    /// Whether the socket answered Botster's protocol hello.
    pub socket_protocol_responds: bool,
    /// Diagnostic details from the protocol probe.
    pub socket_probe_error: Option<String>,
    /// Singleton lock path.
    pub lock_file_path: PathBuf,
}

impl HubArtifactInspection {
    /// Return true when the hub process appears alive.
    #[must_use]
    pub fn process_alive(&self) -> bool {
        self.pid_alive
            || self
                .manifest
                .as_ref()
                .is_some_and(|manifest| pid_is_live(manifest.pid))
    }

    /// Return true when new clients can connect through the advertised socket.
    #[must_use]
    pub fn accepts_new_clients(&self) -> bool {
        self.socket_protocol_responds
    }
}

/// Runtime artifact owner for a single hub.
///
/// Centralizes PID, manifest, lock, and socket path operations so discovery,
/// cleanup, repair, and status do not each invent their own liveness rules.
#[derive(Debug, Clone)]
pub struct HubRuntimeArtifacts {
    hub_id: String,
}

impl HubRuntimeArtifacts {
    /// Build an artifact owner for a local hub ID.
    #[must_use]
    pub fn new(hub_id: impl Into<String>) -> Self {
        Self {
            hub_id: hub_id.into(),
        }
    }

    /// Hub ID these artifacts belong to.
    #[must_use]
    pub fn hub_id(&self) -> &str {
        &self.hub_id
    }

    /// Socket path for this hub.
    pub fn socket_path(&self) -> Result<PathBuf> {
        socket_path(&self.hub_id)
    }

    /// Write PID and manifest for the current process.
    pub fn publish_current_process(&self, server_id: Option<&str>) -> Result<()> {
        self.publish_current_process_with_mcp(server_id, None)
    }

    /// Write PID and manifest, including the shared MCP HTTP URL when ready.
    pub fn publish_current_process_with_mcp(
        &self,
        server_id: Option<&str>,
        mcp_url: Option<&str>,
    ) -> Result<()> {
        write_pid_file(&self.hub_id)?;
        write_manifest_details(&self.hub_id, server_id, mcp_url)
    }

    /// Inspect runtime artifacts without mutating them.
    pub fn inspect(&self) -> Result<HubArtifactInspection> {
        inspect_hub_artifacts(&self.hub_id)
    }

    /// Remove stale files for this hub when no live process/socket owns them.
    pub fn cleanup_stale_files(&self) {
        cleanup_stale_files(&self.hub_id);
    }

    /// Remove runtime files during owner shutdown.
    pub fn cleanup_on_shutdown(&self) {
        cleanup_on_shutdown(&self.hub_id);
    }
}

/// Path to the lock file for a hub.
pub fn lock_file_path(hub_id: &str) -> Result<PathBuf> {
    Ok(hub_dir(hub_id)?.join("hub.lock"))
}

/// Attempt to acquire an exclusive, non-blocking lock for the given hub ID.
///
/// Returns `Ok(HubLock)` on success. The lock is held for the lifetime of the
/// returned `HubLock` (RAII via `flock` on the underlying fd).
///
/// Returns `Err` if another process already holds the lock.
pub fn try_lock_hub(hub_id: &str) -> Result<HubLock> {
    let path = lock_file_path(hub_id)?;
    let file = File::create(&path)
        .with_context(|| format!("Failed to create lock file: {}", path.display()))?;

    let fd = file.as_raw_fd();
    let rc = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            anyhow::bail!(
                "Another hub is already running on this device.\n\
                 Lock file: {}\n\
                 Use `botster attach` to connect to the existing hub, \
                 or stop it first.",
                path.display()
            );
        }
        return Err(err).with_context(|| format!("Failed to lock: {}", path.display()));
    }

    // Write our PID into the lock file for diagnostics.
    let mut f = &file;
    let _ = f.write_all(format!("{}", std::process::id()).as_bytes());

    log::info!(
        "Acquired singleton lock: {} (pid={})",
        path.display(),
        std::process::id()
    );
    Ok(HubLock { _file: file, path })
}

/// Hub runtime manifest persisted under `{config_dir}/hubs/{hub_id}/manifest.json`.
///
/// This artifact lets child sessions (for example `botster mcp-serve`) resolve
/// a live hub socket by server-assigned ID instead of trusting inherited env.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HubManifest {
    /// Stable local hub hash ID (matches socket filename stem).
    pub hub_id: String,
    /// Optional server-assigned hub ID (`hub.server_id()` in Lua).
    pub server_id: Option<String>,
    /// Absolute socket path for the hub.
    pub socket_path: String,
    /// PID of the hub process that wrote this manifest.
    pub pid: u32,
    /// Last write timestamp (unix seconds).
    pub updated_at: u64,
    /// Active workspace IDs. Updated by Lua when sessions are created/closed.
    /// Empty workspaces (no sessions) are automatically removed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub workspaces: Vec<String>,
    /// Shared Streamable HTTP MCP URL owned by this Hub process.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_url: Option<String>,
}

/// Get the per-hub directory path.
///
/// Returns `{config_dir}/hubs/{hub_id}/`, creating it if needed.
pub fn hub_dir(hub_id: &str) -> Result<PathBuf> {
    let dir = crate::config::Config::config_dir()?
        .join("hubs")
        .join(hub_id);
    if !dir.exists() {
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create hub directory: {}", dir.display()))?;
    }
    Ok(dir)
}

/// Get the PID file path for a hub.
pub fn pid_file_path(hub_id: &str) -> Result<PathBuf> {
    Ok(hub_dir(hub_id)?.join("hub.pid"))
}

/// Get the manifest file path for a hub.
pub fn manifest_path(hub_id: &str) -> Result<PathBuf> {
    Ok(hub_dir(hub_id)?.join("manifest.json"))
}

/// Get the Unix socket path for a hub.
///
/// Uses `/tmp/botster-{uid}/` instead of the config dir because macOS
/// limits Unix socket paths to 104 bytes, and `~/Library/Application Support/...`
/// is too long.
pub fn socket_path(hub_id: &str) -> Result<PathBuf> {
    let uid = unsafe { libc::getuid() };
    let dir = PathBuf::from(format!("/tmp/botster-{uid}"));
    if !dir.exists() {
        // Set restrictive umask before creating directory to avoid TOCTOU
        // race between mkdir and chmod on shared /tmp.
        let old_umask = unsafe { libc::umask(0o077) };
        let result = fs::create_dir_all(&dir);
        unsafe {
            libc::umask(old_umask);
        }
        result?;
    }
    Ok(dir.join(format!("{hub_id}.sock")))
}

/// Write the current process PID to the hub's PID file.
pub fn write_pid_file(hub_id: &str) -> Result<()> {
    let path = pid_file_path(hub_id)?;
    let pid = std::process::id();
    fs::write(&path, pid.to_string())
        .with_context(|| format!("Failed to write PID file: {}", path.display()))?;
    log::info!("Wrote PID file: {} (pid={})", path.display(), pid);
    Ok(())
}

/// Write or update the hub runtime manifest.
pub fn write_manifest(hub_id: &str, server_id: Option<&str>) -> Result<()> {
    write_manifest_details(hub_id, server_id, None)
}

/// Write the hub runtime manifest, including the shared MCP URL when present.
pub fn write_manifest_details(
    hub_id: &str,
    server_id: Option<&str>,
    mcp_url: Option<&str>,
) -> Result<()> {
    let started = Instant::now();
    let socket = socket_path(hub_id)?;
    let path = manifest_path(hub_id)?;
    let manifest = HubManifest {
        hub_id: hub_id.to_string(),
        server_id: server_id
            .filter(|s| !s.is_empty())
            .map(std::string::ToString::to_string),
        socket_path: socket.to_string_lossy().into_owned(),
        pid: std::process::id(),
        updated_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        workspaces: Vec::new(),
        mcp_url: mcp_url
            .filter(|s| !s.is_empty())
            .map(std::string::ToString::to_string),
    };
    let content =
        serde_json::to_string_pretty(&manifest).context("Failed to serialize hub manifest")?;
    let bytes = content.len();
    fs::write(&path, content)
        .with_context(|| format!("Failed to write hub manifest: {}", path.display()))?;
    record_manifest_write_log(hub_id, bytes, started.elapsed());
    Ok(())
}

#[derive(Debug, Default)]
struct ManifestWriteStorm {
    writes: std::collections::VecDeque<Instant>,
    warned: bool,
}

fn manifest_write_storms() -> &'static Mutex<std::collections::HashMap<String, ManifestWriteStorm>>
{
    static STORMS: OnceLock<Mutex<std::collections::HashMap<String, ManifestWriteStorm>>> =
        OnceLock::new();
    STORMS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn record_manifest_write_log(hub_id: &str, bytes: usize, elapsed: Duration) {
    const WINDOW: Duration = Duration::from_secs(10);
    const THRESHOLD: usize = 3;
    let now = Instant::now();
    let mut storm_count = 0usize;
    let mut should_warn = false;
    if let Ok(mut storms) = manifest_write_storms().lock() {
        let state = storms.entry(hub_id.to_string()).or_default();
        while state
            .writes
            .front()
            .is_some_and(|at| now.duration_since(*at) > WINDOW)
        {
            state.writes.pop_front();
        }
        if state.writes.is_empty() {
            state.warned = false;
        }
        state.writes.push_back(now);
        storm_count = state.writes.len();
        if storm_count > THRESHOLD && !state.warned {
            state.warned = true;
            should_warn = true;
        }
    }
    log::debug!(
        "[ManifestMetrics] event=write hub={} elapsed_ms={} bytes={}",
        &hub_id[..hub_id.len().min(8)],
        elapsed.as_millis(),
        bytes
    );
    if should_warn {
        log::warn!(
            "[ManifestMetrics] event=write_storm hub={} count={} window_ms=10000",
            &hub_id[..hub_id.len().min(8)],
            storm_count
        );
    }
}

/// Read a hub runtime manifest.
///
/// Returns `None` if the manifest is missing or invalid JSON.
pub fn read_manifest(hub_id: &str) -> Option<HubManifest> {
    let path = manifest_path(hub_id).ok()?;
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

/// Inspect all daemon runtime artifacts for a hub without cleanup or repair.
pub fn inspect_hub_artifacts(hub_id: &str) -> Result<HubArtifactInspection> {
    let pid_file = pid_file_path(hub_id)?;
    let pid = fs::read_to_string(&pid_file)
        .ok()
        .and_then(|contents| contents.trim().parse().ok());
    let pid_alive = pid.is_some_and(pid_is_live);

    let manifest_file = manifest_path(hub_id)?;
    let manifest = fs::read_to_string(&manifest_file)
        .ok()
        .and_then(|content| serde_json::from_str::<HubManifest>(&content).ok());

    let socket = manifest
        .as_ref()
        .map(|m| PathBuf::from(&m.socket_path))
        .unwrap_or(socket_path(hub_id)?);
    let socket_path_exists = socket.exists();
    let socket_probe = if socket_path_exists {
        probe_hub_socket(&socket)
    } else {
        HubSocketProbe::not_connected("socket path is missing")
    };

    Ok(HubArtifactInspection {
        hub_id: hub_id.to_string(),
        pid_file_path: pid_file,
        pid,
        pid_alive,
        manifest_path: manifest_file,
        manifest,
        socket_path: socket,
        socket_path_exists,
        socket_connectable: socket_probe.connected,
        socket_protocol_responds: socket_probe.responded,
        socket_probe_error: socket_probe.error,
        lock_file_path: lock_file_path(hub_id)?,
    })
}

/// Result of probing a connectable socket for Botster protocol identity.
#[derive(Debug, Clone)]
struct HubSocketProbe {
    connected: bool,
    responded: bool,
    error: Option<String>,
}

impl HubSocketProbe {
    fn ok() -> Self {
        Self {
            connected: true,
            responded: true,
            error: None,
        }
    }

    fn connected_without_ack(error: impl Into<String>) -> Self {
        Self {
            connected: true,
            responded: false,
            error: Some(error.into()),
        }
    }

    fn not_connected(error: impl Into<String>) -> Self {
        Self {
            connected: false,
            responded: false,
            error: Some(error.into()),
        }
    }
}

/// Connect and perform a minimal Botster socket protocol handshake.
///
/// This proves more than filesystem existence or connectability: the peer must
/// decode a `hello` frame and return `hello_ack`.
fn probe_hub_socket(path: &Path) -> HubSocketProbe {
    let mut stream =
        match crate::socket::unix::connect_with_timeout(path, Duration::from_millis(500)) {
            Ok(stream) => stream,
            Err(e) => return HubSocketProbe::not_connected(format!("connect failed: {e}")),
        };

    let timeout = Duration::from_millis(500);
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));

    let hello = crate::socket::framing::Frame::Json(serde_json::json!({
        "type": "hello",
        "protocol_version": 2,
        "min_supported_version": 1,
        "client": "daemon_probe",
    }));

    if let Err(e) = stream.write_all(&hello.encode()) {
        return HubSocketProbe::connected_without_ack(format!("hello write failed: {e}"));
    }

    let mut decoder = crate::socket::framing::FrameDecoder::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                return HubSocketProbe::connected_without_ack("socket closed before hello_ack")
            }
            Ok(n) => match decoder.feed(&buf[..n]) {
                Ok(frames) => {
                    for frame in frames {
                        if matches!(
                            frame,
                            crate::socket::framing::Frame::Json(ref value)
                                if value.get("type").and_then(|v| v.as_str()) == Some("hello_ack")
                        ) {
                            return HubSocketProbe::ok();
                        }
                    }
                }
                Err(e) => {
                    return HubSocketProbe::connected_without_ack(format!("decode failed: {e}"));
                }
            },
            Err(e) => {
                return HubSocketProbe::connected_without_ack(format!(
                    "hello_ack read failed: {e}"
                ));
            }
        }
    }
}

/// Update the workspaces list in the hub manifest.
///
/// Reads the current manifest, replaces `workspaces`, and writes back.
/// If the manifest doesn't exist, this is a no-op.
pub fn update_manifest_workspaces(hub_id: &str, workspaces: Vec<String>) -> Result<()> {
    let path = manifest_path(hub_id)?;
    let content =
        fs::read_to_string(&path).with_context(|| format!("read manifest: {}", path.display()))?;
    let mut manifest: HubManifest = serde_json::from_str(&content).context("parse hub manifest")?;
    manifest.workspaces = workspaces;
    manifest.updated_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let updated = serde_json::to_string_pretty(&manifest).context("serialize hub manifest")?;
    fs::write(&path, updated).with_context(|| format!("write manifest: {}", path.display()))?;
    Ok(())
}

/// Return true if a manifest appears live enough for path resolution.
fn manifest_is_live(manifest: &HubManifest) -> bool {
    let pid_alive = pid_is_live(manifest.pid);
    let socket_path = PathBuf::from(&manifest.socket_path);
    let socket_alive = socket_path.exists();
    let socket_protocol_responds = socket_alive && probe_hub_socket(&socket_path).responded;
    pid_alive && socket_protocol_responds
}

/// Interpret `kill(pid, 0)` probe results.
///
/// `EPERM` means the process exists but the caller lacks permission to signal
/// it; this must still be treated as "alive" for daemon liveness checks.
fn kill_probe_indicates_alive(rc: i32, errno: Option<i32>) -> bool {
    if rc == 0 {
        return true;
    }
    matches!(errno, Some(code) if code == libc::EPERM)
}

/// Check whether a PID is live using `kill(pid, 0)`.
fn pid_is_live(pid: u32) -> bool {
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    let errno = if rc == 0 {
        None
    } else {
        std::io::Error::last_os_error().raw_os_error()
    };
    kill_probe_indicates_alive(rc, errno)
}

/// Resolve a hub socket by server-assigned hub ID using persisted manifests.
///
/// Returns `None` when no manifest matches the server ID.
pub fn resolve_socket_for_server_id(server_id: &str) -> Option<PathBuf> {
    let hubs_dir = crate::config::Config::config_dir().ok()?.join("hubs");
    let entries = fs::read_dir(&hubs_dir).ok()?;

    for entry in entries.flatten() {
        if !entry.file_type().map_or(false, |t| t.is_dir()) {
            continue;
        }
        let hub_id = entry.file_name().to_string_lossy().into_owned();
        let Some(manifest) = read_manifest(&hub_id) else {
            continue;
        };
        if manifest.server_id.as_deref() == Some(server_id) {
            if !manifest_is_live(&manifest) {
                continue;
            }
            return Some(PathBuf::from(manifest.socket_path));
        }
    }
    None
}

/// Resolve a hub socket by local hub ID using its persisted runtime manifest.
///
/// Returns `None` when the manifest is missing or stale (dead PID / missing socket).
pub fn resolve_socket_for_hub_id(hub_id: &str) -> Option<PathBuf> {
    let manifest = read_manifest(hub_id)?;
    if !manifest_is_live(&manifest) {
        return None;
    }
    Some(PathBuf::from(manifest.socket_path))
}

/// Read the PID from a hub's PID file.
///
/// Returns `None` if the file doesn't exist or can't be parsed.
pub fn read_pid_file(hub_id: &str) -> Option<u32> {
    let path = pid_file_path(hub_id).ok()?;
    let contents = fs::read_to_string(&path).ok()?;
    contents.trim().parse().ok()
}

/// Check if a hub process is running by verifying the PID file.
///
/// Returns `true` if:
/// 1. The PID file exists
/// 2. The PID is parseable
/// 3. The process with that PID is alive (`kill(pid, 0)` returns 0 or EPERM)
pub fn is_hub_running(hub_id: &str) -> bool {
    let Some(pid) = read_pid_file(hub_id) else {
        return false;
    };
    pid_is_live(pid)
}

/// Remove stale PID and socket files for a hub that is no longer running.
///
/// Only removes files if the recorded PID is not alive. If a hub is
/// already running, this is a no-op to avoid clobbering a live hub's files.
/// Safe to call even if files don't exist.
pub fn cleanup_stale_files(hub_id: &str) {
    let started = Instant::now();
    // If the hub is still running, don't touch its files
    if is_hub_running(hub_id) {
        log::debug!(
            "Hub {} is still running, skipping stale cleanup",
            &hub_id[..hub_id.len().min(8)]
        );
        log::debug!(
            "[RuntimeArtifactsMetrics] event=cleanup_stale_files hub={} elapsed_ms={}",
            &hub_id[..hub_id.len().min(8)],
            started.elapsed().as_millis()
        );
        return;
    }

    let socket_is_live = socket_path(hub_id)
        .ok()
        .filter(|path| path.exists())
        .is_some_and(|path| {
            crate::socket::unix::connect_with_timeout(&path, Duration::from_millis(250)).is_ok()
        });

    if let Ok(path) = pid_file_path(hub_id) {
        if path.exists() {
            let _ = fs::remove_file(&path);
            log::debug!("Removed stale PID file: {}", path.display());
        }
    }
    if let Ok(path) = socket_path(hub_id) {
        if socket_is_live {
            log::warn!(
                "Preserving live hub socket despite stale PID metadata: {}",
                path.display()
            );
            log::debug!(
                "[RuntimeArtifactsMetrics] event=cleanup_stale_files hub={} elapsed_ms={}",
                &hub_id[..hub_id.len().min(8)],
                started.elapsed().as_millis()
            );
            return;
        }
        if path.exists() {
            let _ = fs::remove_file(&path);
            log::debug!("Removed stale socket file: {}", path.display());
        }
    }
    log::debug!(
        "[RuntimeArtifactsMetrics] event=cleanup_stale_files hub={} elapsed_ms={}",
        &hub_id[..hub_id.len().min(8)],
        started.elapsed().as_millis()
    );
}

/// Remove PID and socket files on shutdown.
///
/// Called from `Hub::shutdown()` to clean up daemon files.
pub fn cleanup_on_shutdown(hub_id: &str) {
    let current_pid = std::process::id();

    // Only the owning hub process may remove runtime artifacts.
    // This prevents a stale/duplicate shutdown path from unlinking a live hub
    // socket that belongs to a different PID.
    if let Some(owner_pid) = read_pid_file(hub_id) {
        if owner_pid != current_pid {
            log::warn!(
                "Skipping daemon cleanup for hub {}: pid file owned by {} (current pid={})",
                &hub_id[..hub_id.len().min(8)],
                owner_pid,
                current_pid
            );
            return;
        }
    }

    if let Some(manifest) = read_manifest(hub_id) {
        if manifest.pid != current_pid {
            log::warn!(
                "Skipping daemon cleanup for hub {}: manifest owned by {} (current pid={})",
                &hub_id[..hub_id.len().min(8)],
                manifest.pid,
                current_pid
            );
            return;
        }
    }

    if let Ok(path) = pid_file_path(hub_id) {
        let _ = fs::remove_file(&path);
    }
    if let Ok(path) = socket_path(hub_id) {
        let _ = fs::remove_file(&path);
    }
    if let Ok(path) = manifest_path(hub_id) {
        let _ = fs::remove_file(&path);
    }
    log::info!(
        "Cleaned up daemon files for hub {}",
        &hub_id[..hub_id.len().min(8)]
    );
}

/// Remove orphaned socket files from `/tmp/botster-{uid}/`.
///
/// Scans the socket directory and removes any `.sock` files that don't
/// have a corresponding live process. This catches sockets left behind
/// by crashed processes, SIGKILL'd test processes, or any other case
/// where `cleanup_on_shutdown` didn't run.
///
/// Safe to call at startup — only removes sockets for dead processes.
pub fn cleanup_orphaned_sockets() {
    let uid = unsafe { libc::getuid() };
    let dir = PathBuf::from(format!("/tmp/botster-{uid}"));

    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };

    let mut removed = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        let Some(ext) = path.extension() else {
            continue;
        };
        if ext != "sock" {
            continue;
        }

        // Extract hub_id from filename (strip .sock)
        let Some(hub_id) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };

        // If the hub has a live PID, keep its socket.
        if is_hub_running(hub_id) {
            continue;
        }

        // Safety check: if the path is still serving a live listener, do not
        // unlink it. This protects hubs running under a different
        // BOTSTER_CONFIG_DIR from cross-deletion.
        match crate::socket::unix::connect_with_timeout(&path, Duration::from_millis(250)) {
            Ok(_) => {
                log::debug!(
                    "Preserving live socket owned outside current config dir: {}",
                    path.display()
                );
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                log::warn!(
                    "Preserving possibly live socket after timed-out probe: {}",
                    path.display()
                );
                continue;
            }
            Err(_) => {}
        }

        // No live process — remove the orphaned socket
        if fs::remove_file(&path).is_ok() {
            removed += 1;
            log::debug!("Removed orphaned socket: {}", path.display());
        }
    }

    if removed > 0 {
        log::info!(
            "Cleaned up {removed} orphaned socket(s) from {}",
            dir.display()
        );
    }
}

/// Discover all running hubs by scanning PID files.
///
/// Returns a list of `(hub_id, pid)` pairs for running hubs.
pub fn discover_running_hubs() -> Vec<(String, u32)> {
    let hubs_dir = match crate::config::Config::config_dir() {
        Ok(dir) => dir.join("hubs"),
        Err(_) => return Vec::new(),
    };

    let entries = match fs::read_dir(&hubs_dir) {
        Ok(entries) => entries,
        Err(_) => return Vec::new(),
    };

    let mut running = Vec::new();
    for entry in entries.flatten() {
        if !entry.file_type().map_or(false, |t| t.is_dir()) {
            continue;
        }
        let hub_id = entry.file_name().to_string_lossy().into_owned();

        if let Some(pid) = read_pid_file(&hub_id) {
            if pid_is_live(pid) {
                running.push((hub_id, pid));
            }
        }
    }

    running
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::socket::framing::{Frame, FrameDecoder};

    fn spawn_hello_ack_socket(path: &Path) -> std::thread::JoinHandle<()> {
        let listener = std::os::unix::net::UnixListener::bind(path).unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();

            let mut decoder = FrameDecoder::new();
            let mut buf = [0u8; 8192];
            loop {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    return;
                }
                let frames = decoder.feed(&buf[..n]).unwrap();
                for frame in frames {
                    if matches!(
                        frame,
                        Frame::Json(ref value)
                            if value.get("type").and_then(|v| v.as_str()) == Some("hello")
                    ) {
                        let ack = Frame::Json(serde_json::json!({
                            "type": "hello_ack",
                            "protocol_version": 2,
                            "min_supported_version": 1,
                        }));
                        stream.write_all(&ack.encode()).unwrap();
                        return;
                    }
                }
            }
        })
    }

    #[test]
    fn test_socket_path_format() {
        let path = socket_path("abc123").unwrap();
        let path_str = path.to_string_lossy();
        assert!(
            path_str.starts_with("/tmp/botster-"),
            "Expected /tmp/botster-*, got: {path_str}"
        );
        assert!(
            path_str.ends_with("/abc123.sock"),
            "Expected *.sock, got: {path_str}"
        );
    }

    #[test]
    fn test_pid_file_path_format() {
        let path = pid_file_path("abc123").unwrap();
        assert!(path.to_string_lossy().contains("hubs/abc123/hub.pid"));
    }

    #[test]
    fn test_read_nonexistent_pid_file() {
        assert!(read_pid_file("nonexistent_hub_id_12345").is_none());
    }

    #[test]
    fn test_is_hub_running_nonexistent() {
        assert!(!is_hub_running("nonexistent_hub_id_12345"));
    }

    #[test]
    fn test_kill_probe_indicates_alive_accepts_eperm() {
        assert!(kill_probe_indicates_alive(0, None));
        assert!(kill_probe_indicates_alive(-1, Some(libc::EPERM)));
        assert!(!kill_probe_indicates_alive(-1, Some(libc::ESRCH)));
        assert!(!kill_probe_indicates_alive(-1, None));
    }

    #[test]
    fn test_pid_file_write_read_cleanup_cycle() {
        let test_id = format!("_test_pid_{}", std::process::id());

        write_pid_file(&test_id).unwrap();
        assert_eq!(read_pid_file(&test_id), Some(std::process::id()));
        assert!(is_hub_running(&test_id));

        cleanup_on_shutdown(&test_id);
        assert!(read_pid_file(&test_id).is_none());
        assert!(!is_hub_running(&test_id));
    }

    #[test]
    fn test_cleanup_on_shutdown_skips_foreign_pid_file_owner() {
        let test_id = format!("_test_foreign_pid_{}", std::process::id());
        let pid_path = pid_file_path(&test_id).unwrap();
        let socket_path = socket_path(&test_id).unwrap();

        fs::write(&pid_path, "999999").unwrap();
        fs::write(&socket_path, b"").unwrap();

        cleanup_on_shutdown(&test_id);

        assert!(pid_path.exists(), "foreign pid file should remain");
        assert!(socket_path.exists(), "foreign socket should remain");

        let _ = fs::remove_file(&pid_path);
        let _ = fs::remove_file(&socket_path);
        if let Ok(path) = manifest_path(&test_id) {
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn test_cleanup_on_shutdown_skips_foreign_manifest_owner() {
        let test_id = format!("_test_foreign_manifest_{}", std::process::id());
        let manifest = HubManifest {
            hub_id: test_id.clone(),
            server_id: Some("server-xyz".to_string()),
            socket_path: socket_path(&test_id)
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            pid: 999999,
            updated_at: 1,
            workspaces: Vec::new(),
            mcp_url: None,
        };
        let manifest_content = serde_json::to_string_pretty(&manifest).unwrap();
        fs::write(manifest_path(&test_id).unwrap(), manifest_content).unwrap();

        let pid_path = pid_file_path(&test_id).unwrap();
        let socket_path = socket_path(&test_id).unwrap();
        fs::write(&pid_path, std::process::id().to_string()).unwrap();
        fs::write(&socket_path, b"").unwrap();

        cleanup_on_shutdown(&test_id);

        assert!(
            manifest_path(&test_id).unwrap().exists(),
            "foreign manifest should remain"
        );
        assert!(
            pid_path.exists(),
            "pid file should remain when cleanup is skipped"
        );
        assert!(
            socket_path.exists(),
            "socket should remain when cleanup is skipped"
        );

        let _ = fs::remove_file(pid_path);
        let _ = fs::remove_file(socket_path);
        let _ = fs::remove_file(manifest_path(&test_id).unwrap());
    }

    #[test]
    fn test_cleanup_stale_files_preserves_connectable_socket_with_stale_pid() {
        let test_id = format!("_test_live_socket_stale_pid_{}", std::process::id());
        let pid_path = pid_file_path(&test_id).unwrap();
        let socket_path = socket_path(&test_id).unwrap();

        fs::write(&pid_path, "999999").unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();

        cleanup_stale_files(&test_id);

        assert!(
            socket_path.exists(),
            "cleanup must not unlink a connectable live socket"
        );
        assert!(
            !pid_path.exists(),
            "stale PID metadata should still be removed"
        );

        drop(listener);
        let _ = fs::remove_file(&socket_path);
        if let Ok(path) = manifest_path(&test_id) {
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn test_inspect_hub_artifacts_reports_missing_runtime_files() {
        let test_id = format!("_test_inspect_missing_{}", std::process::id());

        let inspection = inspect_hub_artifacts(&test_id).unwrap();

        assert_eq!(inspection.hub_id, test_id);
        assert!(inspection.pid.is_none());
        assert!(!inspection.pid_alive);
        assert!(inspection.manifest.is_none());
        assert!(!inspection.socket_path_exists);
        assert!(!inspection.socket_connectable);
        assert!(!inspection.socket_protocol_responds);
        assert!(inspection.socket_probe_error.is_some());
        assert!(!inspection.process_alive());
        assert!(!inspection.accepts_new_clients());
    }

    #[test]
    fn test_inspect_hub_artifacts_verifies_socket_protocol() {
        let test_id = format!("_test_inspect_protocol_{}", std::process::id());
        write_pid_file(&test_id).unwrap();
        write_manifest(&test_id, None).unwrap();
        let socket_path = socket_path(&test_id).unwrap();
        let handle = spawn_hello_ack_socket(&socket_path);

        let inspection = inspect_hub_artifacts(&test_id).unwrap();

        assert!(inspection.socket_path_exists);
        assert!(inspection.socket_connectable);
        assert!(inspection.socket_protocol_responds);
        assert!(inspection.socket_probe_error.is_none());
        assert!(inspection.accepts_new_clients());

        handle.join().unwrap();
        cleanup_on_shutdown(&test_id);
    }

    #[test]
    fn test_inspect_hub_artifacts_rejects_non_botster_socket() {
        let test_id = format!("_test_inspect_non_botster_{}", std::process::id());
        write_pid_file(&test_id).unwrap();
        write_manifest(&test_id, None).unwrap();
        let socket_path = socket_path(&test_id).unwrap();
        let listener = std::os::unix::net::UnixListener::bind(&socket_path).unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = stream.write_all(b"not a framed botster response");
        });

        let inspection = inspect_hub_artifacts(&test_id).unwrap();

        assert!(inspection.socket_path_exists);
        assert!(inspection.socket_connectable);
        assert!(!inspection.socket_protocol_responds);
        assert!(inspection.socket_probe_error.is_some());
        assert!(!inspection.accepts_new_clients());

        handle.join().unwrap();
        cleanup_on_shutdown(&test_id);
    }

    #[test]
    fn test_resolve_socket_for_hub_id_does_not_cleanup_stale_artifacts() {
        let test_id = format!("_test_resolve_read_only_{}", std::process::id());
        let pid_path = pid_file_path(&test_id).unwrap();
        let socket_path = socket_path(&test_id).unwrap();
        let manifest_path = manifest_path(&test_id).unwrap();
        let manifest = HubManifest {
            hub_id: test_id.clone(),
            server_id: None,
            socket_path: socket_path.to_string_lossy().into_owned(),
            pid: 999999,
            updated_at: 1,
            workspaces: Vec::new(),
            mcp_url: None,
        };

        fs::write(&pid_path, "999999").unwrap();
        fs::write(&socket_path, b"").unwrap();
        fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();

        assert!(resolve_socket_for_hub_id(&test_id).is_none());
        assert!(
            pid_path.exists(),
            "read-only resolve must not remove stale pid files"
        );
        assert!(
            socket_path.exists(),
            "read-only resolve must not remove stale socket files"
        );
        assert!(
            manifest_path.exists(),
            "read-only resolve must not remove stale manifests"
        );

        let _ = fs::remove_file(pid_path);
        let _ = fs::remove_file(socket_path);
        let _ = fs::remove_file(manifest_path);
    }

    #[test]
    fn test_manifest_round_trip() {
        let test_id = format!("_test_manifest_{}", std::process::id());
        write_manifest(&test_id, Some("123")).unwrap();
        let manifest = read_manifest(&test_id).expect("manifest should exist");
        assert_eq!(manifest.hub_id, test_id);
        assert_eq!(manifest.server_id.as_deref(), Some("123"));
        assert!(manifest.socket_path.ends_with(".sock"));
        assert!(manifest.updated_at > 0);
        cleanup_on_shutdown(&manifest.hub_id);
    }

    #[test]
    fn test_resolve_socket_for_server_id() {
        let test_id = format!("_test_server_lookup_{}", std::process::id());
        write_manifest(&test_id, Some("hub-server-id-xyz")).unwrap();
        let socket = socket_path(&test_id).unwrap();
        let handle = spawn_hello_ack_socket(&socket);
        let socket =
            resolve_socket_for_server_id("hub-server-id-xyz").expect("socket should resolve");
        assert!(socket
            .to_string_lossy()
            .ends_with(&format!("/{test_id}.sock")));
        handle.join().unwrap();
        cleanup_on_shutdown(&test_id);
    }

    #[test]
    fn test_resolve_socket_for_hub_id() {
        let test_id = format!("_test_local_lookup_{}", std::process::id());
        write_manifest(&test_id, Some("ignored-server-id")).unwrap();
        let socket = socket_path(&test_id).unwrap();
        let handle = spawn_hello_ack_socket(&socket);
        let resolved = resolve_socket_for_hub_id(&test_id).expect("socket should resolve");
        assert!(resolved
            .to_string_lossy()
            .ends_with(&format!("/{test_id}.sock")));
        handle.join().unwrap();
        cleanup_on_shutdown(&test_id);
    }

    #[test]
    fn test_discover_running_hubs_includes_self() {
        let test_id = format!("_test_discover_{}", std::process::id());
        write_pid_file(&test_id).unwrap();

        let running = discover_running_hubs();
        let found = running
            .iter()
            .any(|(id, pid)| id == &test_id && *pid == std::process::id());
        assert!(
            found,
            "discover_running_hubs should find our test hub, got: {running:?}"
        );

        cleanup_on_shutdown(&test_id);
    }

    /// Verifies that `cleanup_orphaned_sockets` removes stale hub sockets.
    ///
    /// A hub socket whose stem matches no live PID file is orphaned (e.g., left
    /// behind by a crashed process). The cleanup must remove it so a restarted
    /// hub can bind a fresh socket at the same path without EADDRINUSE.
    #[test]
    fn test_cleanup_orphaned_sockets_removes_stale_hub_socket() {
        let uid = unsafe { libc::getuid() };
        let dir = PathBuf::from(format!("/tmp/botster-{uid}"));
        fs::create_dir_all(&dir).unwrap();

        // Use a unique id with no corresponding PID file — simulates a crashed hub.
        let stale_id = format!("_test_stale_{}", std::process::id());
        let stale_sock = dir.join(format!("{stale_id}.sock"));
        fs::write(&stale_sock, b"").unwrap();

        // Precondition: no PID file exists for this id.
        assert!(
            read_pid_file(&stale_id).is_none(),
            "test precondition: no PID file"
        );

        cleanup_orphaned_sockets();

        assert!(
            !stale_sock.exists(),
            "cleanup_orphaned_sockets should remove stale hub socket: {}",
            stale_sock.display()
        );
    }

    /// Verifies that `cleanup_orphaned_sockets` preserves live sockets even
    /// when there is no local PID file for the socket stem.
    ///
    /// This protects hubs started with a different `BOTSTER_CONFIG_DIR`.
    #[test]
    fn test_cleanup_orphaned_sockets_preserves_live_unknown_socket() {
        let uid = unsafe { libc::getuid() };
        let dir = PathBuf::from(format!("/tmp/botster-{uid}"));
        fs::create_dir_all(&dir).unwrap();

        let live_id = format!("_test_live_unknown_{}", std::process::id());
        let live_sock = dir.join(format!("{live_id}.sock"));
        let listener = std::os::unix::net::UnixListener::bind(&live_sock).unwrap();

        // No local PID file should exist for this synthetic id.
        assert!(
            read_pid_file(&live_id).is_none(),
            "test precondition: no local PID file"
        );

        cleanup_orphaned_sockets();

        assert!(
            live_sock.exists(),
            "cleanup_orphaned_sockets must preserve live socket: {}",
            live_sock.display()
        );

        drop(listener);
        let _ = fs::remove_file(&live_sock);
    }

    #[test]
    fn test_singleton_lock_second_acquire_fails() {
        let test_id = format!("_test_lock_dup_{}", std::process::id());

        let lock1 = try_lock_hub(&test_id).expect("first lock should succeed");

        let result = try_lock_hub(&test_id);
        assert!(result.is_err(), "second lock must fail while first is held");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Another hub is already running"),
            "expected singleton error, got: {err_msg}"
        );

        // Lock file should still exist while held.
        assert!(lock1.path.exists());

        drop(lock1);
        // After drop, a new lock should succeed.
        let lock2 = try_lock_hub(&test_id).expect("lock after drop should succeed");
        drop(lock2);

        // Clean up.
        let _ = fs::remove_file(lock_file_path(&test_id).unwrap());
        let _ = fs::remove_dir(hub_dir(&test_id).unwrap());
    }

    #[test]
    fn test_singleton_lock_released_on_drop() {
        let test_id = format!("_test_lock_drop_{}", std::process::id());

        {
            let _lock = try_lock_hub(&test_id).expect("lock should succeed");
            // Lock held inside this scope.
        }
        // Lock dropped — re-acquire must succeed.
        let lock2 = try_lock_hub(&test_id).expect("lock after drop should succeed");
        drop(lock2);

        let _ = fs::remove_file(lock_file_path(&test_id).unwrap());
        let _ = fs::remove_dir(hub_dir(&test_id).unwrap());
    }

    #[test]
    fn test_singleton_lock_early_failure_no_leak() {
        let test_id = format!("_test_lock_leak_{}", std::process::id());

        // Simulate: lock acquired, then "startup fails" (lock dropped).
        let lock = try_lock_hub(&test_id).expect("lock should succeed");
        drop(lock); // Simulates startup failure + RAII cleanup.

        // New startup attempt must succeed.
        let lock2 = try_lock_hub(&test_id).expect("lock after failed startup should succeed");
        drop(lock2);

        let _ = fs::remove_file(lock_file_path(&test_id).unwrap());
        let _ = fs::remove_dir(hub_dir(&test_id).unwrap());
    }
}
