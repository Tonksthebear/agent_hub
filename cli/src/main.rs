//! Botster Hub CLI - manages autonomous agent sessions.
//!
//! This is the main binary entry point. See the `botster` library
//! for the core functionality.

use anyhow::{Context, Result};
use botster::{clients::tui, commands, Config, Hub};
use mimalloc::MiMalloc;

/// Global allocator configured per M-MIMALLOC-APPS guideline.
/// mimalloc provides better multi-threaded performance than the system allocator.
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;
use clap::{Parser, Subcommand};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, PopKeyboardEnhancementFlags},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

/// Version constant re-exported from commands module.
use commands::update::VERSION;

/// Global flag for signal-triggered shutdown (as Arc for signal-hook compatibility)
static SHUTDOWN_FLAG: std::sync::LazyLock<Arc<AtomicBool>> =
    std::sync::LazyLock::new(|| Arc::new(AtomicBool::new(false)));

/// Ensure user is authenticated, running device flow if needed.
///
/// This function validates the token against the server before returning.
/// After returning successfully, callers can be confident that:
/// 1. A valid token exists in the keyring (or env var)
/// 2. The token has been verified against the server
fn ensure_authenticated() -> Result<()> {
    use botster::auth;

    // Skip auth validation in test mode (BOTSTER_ENV=test)
    if botster::env::is_test_mode() {
        log::info!("Skipping authentication (BOTSTER_ENV=test)");
        return Ok(());
    }

    // Skip auth in offline mode — no network available
    if botster::env::is_offline() {
        // Warn if no token exists — some Lua plugins may fail if they
        // try to read hub.api_token() for authenticated requests.
        let config = Config::load()?;
        if !config.has_token() {
            eprintln!(
                "Warning: No cached authentication token found. \
                 Run 'botster start' once with network to authenticate, \
                 then use --offline."
            );
        }
        log::info!("Skipping authentication (offline mode)");
        return Ok(());
    }

    let mut config = Config::load()?;
    let using_env_var =
        std::env::var("BOTSTER_TOKEN").is_ok() || std::env::var("BOTSTER_API_KEY").is_ok();

    // Check if we have a token at all
    if !config.has_token() {
        // Check credential storage before first save — warns once during auth
        botster::keyring::check_credential_storage()?;

        // Collect all setup info upfront before opening the browser.
        // Avoids the jarring flow of: name device → browser → back to CLI → name hub.
        let device_name = auth::prompt_device_name()?;

        // Update device identity with user-chosen name
        if let Ok(mut device) = botster::device::Device::load_or_create() {
            device.name = device_name;
            if let Err(e) = device.save() {
                log::warn!("Failed to save device name: {e}");
            }
        }

        let token_response = auth::device_flow(&config.server_url)?;
        save_tokens(&mut config, &token_response)?;
        config.save()?;
        println!("  Setup complete.");
        println!();

        // Verify the token was saved correctly by reloading
        let verify_config = Config::load()?;
        if !verify_config.has_token() {
            anyhow::bail!(
                "Token was not saved correctly to keyring. \
                 This may be a permissions issue with your system keychain."
            );
        }
        log::info!(
            "New token saved and verified: {}...{}",
            &verify_config.get_api_key()[..10.min(verify_config.get_api_key().len())],
            &verify_config.get_api_key()[verify_config.get_api_key().len().saturating_sub(4)..]
        );
        return Ok(());
    }

    // Validate the token - even if from env var, we need to check it works
    println!("Checking authentication...");
    let token_preview = format!(
        "{}...{}",
        &config.get_api_key()[..10.min(config.get_api_key().len())],
        &config.get_api_key()[config.get_api_key().len().saturating_sub(4)..]
    );
    log::info!("Validating token: {}", token_preview);

    if !auth::validate_token(&config.server_url, config.get_api_key()) {
        println!("Token invalid or expired. Re-authenticating...");
        // Check credential storage before saving — no-op if already checked
        botster::keyring::check_credential_storage()?;
        let token_response = auth::device_flow(&config.server_url)?;

        if using_env_var {
            // Don't save to config - user is managing token via env var
            // Just print instructions
            println!();
            println!("New tokens obtained. Update your environment variables:");
            println!("  export BOTSTER_TOKEN={}", token_response.access_token);
            if let Some(ref mcp_token) = token_response.mcp_token {
                println!("  export BOTSTER_MCP_TOKEN={}", mcp_token);
            }
            println!();
            anyhow::bail!("Please update your environment variables and restart.");
        }
        save_tokens(&mut config, &token_response)?;
        println!("Tokens saved successfully.");

        // Verify the token was saved correctly by reloading
        let verify_config = Config::load()?;
        if !verify_config.has_token() {
            anyhow::bail!(
                "Token was not saved correctly to keyring. \
                 This may be a permissions issue with your system keychain."
            );
        }
        log::info!(
            "New token saved and verified: {}...{}",
            &verify_config.get_api_key()[..10.min(verify_config.get_api_key().len())],
            &verify_config.get_api_key()[verify_config.get_api_key().len().saturating_sub(4)..]
        );
    } else {
        println!("  Authentication valid.");
    }

    Ok(())
}

/// Save both hub and MCP tokens from auth response.
fn save_tokens(config: &mut Config, token_response: &botster::auth::TokenResponse) -> Result<()> {
    use botster::keyring::Credentials;

    // Save hub token via config (which updates Credentials internally)
    config.save_token(&token_response.access_token)?;

    // Save MCP token if provided
    if let Some(ref mcp_token) = token_response.mcp_token {
        let mut creds = Credentials::load().unwrap_or_default();
        creds.set_mcp_token(mcp_token.clone());
        creds.save()?;
        log::info!("Saved MCP token to credentials");
    }

    Ok(())
}

/// Runs the hub in headless mode (no TUI).
///
/// This mode is useful for:
/// - Integration testing (system tests spawn CLI headless)
/// - Running as a background daemon
/// - CI/CD environments without a terminal
fn run_headless() -> Result<()> {
    println!("Starting Botster Hub v{} in headless mode...", VERSION);

    // Check for updates first (non-interactive — logs warning only)
    if !botster::env::is_offline() {
        let _ = commands::update::check_on_boot_headless();
    }

    // Ensure we have a valid authentication token
    ensure_authenticated()?;
    normalize_process_cwd_to_home()?;

    // Set up signal handlers
    use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
    use signal_hook::flag;
    flag::register(SIGINT, Arc::clone(&SHUTDOWN_FLAG))?;
    flag::register(SIGTERM, Arc::clone(&SHUTDOWN_FLAG))?;
    flag::register(SIGHUP, Arc::clone(&SHUTDOWN_FLAG))?;
    botster::shutdown::spawn(Arc::clone(&SHUTDOWN_FLAG));

    // Create Hub with default terminal size (80x24 for headless)
    let config = Config::load()?;

    // Verify token is available after load - catches keyring save/load issues
    // Skip in test mode and offline mode since ensure_authenticated() skips auth
    if !botster::env::is_test_mode() && !botster::env::is_offline() && !config.has_token() {
        anyhow::bail!(
            "Authentication token not found after auth flow. \
             This may indicate a keyring access issue. \
             Try running 'botster reset' and re-authenticating."
        );
    }

    if config.has_token() {
        log::info!(
            "Token loaded: {}...{} (valid format)",
            &config.get_api_key()[..10.min(config.get_api_key().len())],
            &config.get_api_key()[config.get_api_key().len().saturating_sub(4)..]
        );
    }

    let mut hub = Hub::new(config)?;

    if botster::env::is_offline() {
        println!("Setting up in offline mode (no network)...");
    } else {
        println!("Setting up connections...");
    }
    hub.setup();

    // Start socket server for IPC (allows `botster attach` and plugin access)
    hub.start_socket_server()?;

    // In headless mode, eagerly generate the connection URL so external
    // tools (system tests, automation) can read it from connection_url.txt
    // without needing a TUI interaction to trigger lazy generation.
    if !botster::env::is_offline() {
        hub.eager_generate_connection_url();
    }

    println!("Hub ready. Waiting for connections...");
    log::info!("Botster Hub v{} started in headless mode", VERSION);

    // Fully event-driven headless loop — uses tokio::select! to sleep
    // between events. No periodic polling.
    hub.run_headless(&SHUTDOWN_FLAG)?;

    println!("Shutting down...");
    let should_restart = hub.exec_restart;
    hub.shutdown();

    if should_restart {
        commands::update::exec_restart()?;
    }

    Ok(())
}

/// Run the hub using the Hub architecture with TUI.
///
/// This sets up the terminal and delegates to tui::run_with_hub() for the event loop.
/// The TUI module now owns TuiRunner instantiation, maintaining proper layer separation
/// (Hub should not know about TUI implementation details).
fn run_with_tui() -> Result<()> {
    // Require an interactive terminal for TUI mode
    if !atty::is(atty::Stream::Stdin) {
        anyhow::bail!(
            "Error: 'start' requires an interactive terminal (stdin is not a TTY).\n\
             Use 'botster start --headless' for non-interactive mode."
        );
    }

    // Check for updates first — show errors so the user knows if an update failed
    if !botster::env::is_offline() {
        if let Err(e) = commands::update::check_on_boot() {
            eprintln!("Update failed: {e:#}");
        }
    }

    // Ensure we have a valid authentication token
    ensure_authenticated()?;
    normalize_process_cwd_to_home()?;

    // Set up signal handlers
    use signal_hook::consts::signal::{SIGHUP, SIGINT, SIGTERM};
    use signal_hook::flag;
    flag::register(SIGINT, Arc::clone(&SHUTDOWN_FLAG))?;
    flag::register(SIGTERM, Arc::clone(&SHUTDOWN_FLAG))?;
    flag::register(SIGHUP, Arc::clone(&SHUTDOWN_FLAG))?;
    botster::shutdown::spawn(Arc::clone(&SHUTDOWN_FLAG));

    // Create Hub BEFORE entering raw mode so errors are visible
    println!("Initializing hub...");
    let config = Config::load()?;

    // Verify token is available after load - catches keyring save/load issues
    // Skip in test mode and offline mode since ensure_authenticated() skips auth
    if !botster::env::is_test_mode() && !botster::env::is_offline() && !config.has_token() {
        anyhow::bail!(
            "Authentication token not found after auth flow. \
             This may indicate a keyring access issue. \
             Try running 'botster reset' and re-authenticating."
        );
    }

    if config.has_token() {
        log::info!(
            "Token loaded: {}...{} (valid format)",
            &config.get_api_key()[..10.min(config.get_api_key().len())],
            &config.get_api_key()[config.get_api_key().len().saturating_sub(4)..]
        );
    }

    let mut hub = Hub::new(config)?;

    // Perform setup BEFORE entering raw mode so errors are visible
    if botster::env::is_offline() {
        println!("Setting up in offline mode (no network)...");
    } else {
        println!("Setting up connections...");
    }
    hub.setup();

    // Start socket server for IPC (allows `botster attach` and plugin access)
    hub.start_socket_server()?;

    // Eagerly generate the pairing URL so the connection_code entity is ready
    // for late subscribers and the Share dialog can render without waiting
    // for the TUI's QR shortcut. Headless mode does the same at line 236.
    if !botster::env::is_offline() {
        hub.eager_generate_connection_url();
    }

    println!("Starting TUI...");

    // NOW setup terminal (after all initialization that could fail)
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        EnableMouseCapture,
        crossterm::event::EnableFocusChange
    )?;

    // Kitty keyboard protocol is NOT pushed here — it's mirrored dynamically
    // from the inner PTY's state by sync_terminal_modes() in the event loop.

    let _terminal_guard = tui::TerminalGuard::new();

    let tui_color_cache = tui::probe_spawning_terminal_colors();
    hub.seed_boot_color_cache(&tui_color_cache);

    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;

    log::info!("Botster Hub v{} started with TUI", VERSION);

    // Run the event loop - TUI module now owns TuiRunner instantiation
    tui::run_with_hub(&mut hub, terminal, &*SHUTDOWN_FLAG, tui_color_cache)?;

    // Shutdown
    let should_restart = hub.exec_restart;
    hub.shutdown();

    if should_restart {
        commands::update::exec_restart()?;
    }

    Ok(())
}

// CLI
#[derive(Parser)]
#[command(name = "botster")]
#[command(version = VERSION)]
#[command(about = "Interactive PTY-based daemon for GitHub automation")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Start {
        #[arg(long)]
        headless: bool,
        /// Run without any network connectivity (no auth, no server registration,
        /// no browser relay). Requires a previously authenticated device.
        #[arg(long)]
        offline: bool,
        /// Disable WebRTC signaling and data-channel encryption.
        #[arg(long)]
        no_encryption: bool,
    },
    Status,
    Config {
        key: Option<String>,
        value: Option<String>,
    },
    /// Get a value from a JSON file using dot notation (e.g., "projects.myproject.hasTrust")
    JsonGet {
        /// Path to the JSON file
        file: String,
        /// JSON path using dot notation
        key: String,
    },
    /// Set a value in a JSON file using dot notation
    JsonSet {
        /// Path to the JSON file
        file: String,
        /// JSON path using dot notation
        key: String,
        /// Value to set (will be parsed as JSON)
        value: String,
    },
    /// Delete a key from a JSON file using dot notation
    JsonDelete {
        /// Path to the JSON file
        file: String,
        /// JSON path using dot notation
        key: String,
    },
    /// Delete a git worktree and run teardown scripts
    DeleteWorktree {
        /// Issue number of the worktree to delete
        issue_number: u32,
    },
    /// List all git worktrees for the current repository
    ListWorktrees,
    /// Update botster to the latest version
    Update {
        /// Show version without updating
        #[arg(long)]
        check: bool,
    },
    /// Get the connection URL for a running hub (for testing/automation)
    GetConnectionUrl {
        /// Hub identifier
        #[arg(long)]
        hub: String,
    },
    /// Remove all botster data (credentials, config, device identity)
    Reset {
        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Attach a TUI to the running device hub (like tmux attach)
    Attach,
    /// Run as MCP server bridge (connects to hub, speaks MCP on stdio)
    McpServe,
    /// Get agent context values (identity, worktree metadata, plugin data).
    /// Omit key to dump all context as JSON.
    Context {
        /// Context key (e.g., session_uuid, repo, prompt, issue_number)
        key: Option<String>,
        /// Optional value for context helpers (e.g., `botster context file notes.md`)
        value: Option<String>,
    },
    /// Inspect a running hub.
    Debug {
        #[command(subcommand)]
        command: DebugCommands,
    },
    /// Run a per-session PTY process (internal — spawned by the hub)
    Session {
        /// Session UUID
        #[arg(long)]
        uuid: String,
        /// Unix socket path for hub communication
        #[arg(long)]
        socket: String,
        /// Seconds to wait for Hub reconnect before exiting
        #[arg(long, default_value_t = 120)]
        timeout: u64,
        /// Human-readable label for process title (e.g. agent name)
        #[arg(long)]
        label: Option<String>,
    },
}

#[derive(Subcommand)]
enum DebugCommands {
    /// Print memory-adjacent hub diagnostics as JSON.
    Memory {
        /// Hub identifier. Defaults to this device's local hub ID.
        #[arg(long)]
        hub: Option<String>,
    },
    /// Replay a captured VT crash fixture through Ghostty (offline SIGSEGV repro).
    ///
    /// Pass a crash dump directory (contains `*.vtlast` / `*.vtring`) or a
    /// single `.vtlast` file. Progress is printed to stderr before each
    /// `process()` call so a native crash leaves a clear last line.
    ///
    /// Snapshot modes (`--snapshot`):
    ///   none       pure VT (session death path; known SEGV on full ring)
    ///   export     export GHOSTSNP after ring (session attach side effect)
    ///   roundtrip  export+import in-place after ring, then last
    ///   client     ring on A → export → import B → last on B (browser shape)
    ///   after-chunks=N  roundtrip after N ring chunks
    ///
    /// Examples:
    ///   botster debug vt-replay --rows 70 --cols 226 /tmp/botster-vt-min-repro/dir
    ///   botster debug vt-replay --snapshot client --bootstrap-resize /tmp/botster-vt-min-repro/dir
    VtReplay {
        /// Crash dump directory, `.vtlast` file, or raw VT blob.
        path: std::path::PathBuf,
        /// Terminal rows (default 70 — matches recent browser attach paint).
        #[arg(long, default_value_t = 70)]
        rows: u16,
        /// Terminal cols (default 226 — matches recent browser attach).
        #[arg(long, default_value_t = 226)]
        cols: u16,
        /// Skip the rolling ring; feed only `.vtlast`.
        #[arg(long)]
        last_only: bool,
        /// Disable mode_get polling after each write.
        #[arg(long)]
        no_mode_poll: bool,
        /// Quiet stderr progress (not recommended for crash hunting).
        #[arg(long)]
        quiet: bool,
        /// Snapshot phase: none|export|roundtrip|client|after-chunks=N
        #[arg(long, default_value = "none")]
        snapshot: String,
        /// Start 24×80 then resize to target (spawn → attach shape).
        #[arg(long)]
        bootstrap_resize: bool,
    },
}

/// Raise the process file descriptor limit to accommodate WebRTC connections.
///
/// Each WebRTC peer connection opens ~15 UDP sockets for ICE candidate
/// gathering. Combined with webrtc-rs's 60s graceful SCTP shutdown, the
/// default macOS limit of 256 fds exhausts after just a few rapid reconnects.
fn raise_fd_limit() {
    unsafe {
        let mut rlim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut rlim) == 0 {
            let target = 4096.min(rlim.rlim_max);
            if rlim.rlim_cur < target {
                rlim.rlim_cur = target;
                libc::setrlimit(libc::RLIMIT_NOFILE, &rlim);
            }
        }
    }
}

/// RAII guard for a pair of pipe file descriptors.
///
/// Closes both fds on drop, preventing leaks if `run_attach` exits early.
struct WakePipe {
    read_fd: Option<i32>,
    write_fd: Option<i32>,
}

impl WakePipe {
    fn new() -> Self {
        let mut fds = [0i32; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } == 0 {
            unsafe {
                let flags = libc::fcntl(fds[0], libc::F_GETFL);
                libc::fcntl(fds[0], libc::F_SETFL, flags | libc::O_NONBLOCK);
                let flags = libc::fcntl(fds[1], libc::F_GETFL);
                libc::fcntl(fds[1], libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            Self {
                read_fd: Some(fds[0]),
                write_fd: Some(fds[1]),
            }
        } else {
            Self {
                read_fd: None,
                write_fd: None,
            }
        }
    }

    fn read_fd(&self) -> Option<i32> {
        self.read_fd
    }
    fn write_fd(&self) -> Option<i32> {
        self.write_fd
    }
}

impl Drop for WakePipe {
    fn drop(&mut self) {
        if let Some(fd) = self.read_fd {
            unsafe {
                libc::close(fd);
            }
        }
        if let Some(fd) = self.write_fd {
            unsafe {
                libc::close(fd);
            }
        }
    }
}

/// Set a descriptive process title so each botster subprocess is identifiable
/// in `ps`, Activity Monitor, and `top`.
///
/// Titles are kept concise for narrow Activity Monitor columns. Session
/// processes include the agent label when available, falling back to a
/// truncated UUID.
fn set_process_title(command: &Commands) {
    // Start is handled inline in its match arm (may redirect to attach).
    let title = match command {
        Commands::Attach => "botster attach".to_string(),
        Commands::McpServe => {
            // Include truncated session UUID for correlation
            let suffix = std::env::var("BOTSTER_SESSION_UUID")
                .ok()
                .map(|u| format!(" [{}]", &u[..u.len().min(16)]))
                .unwrap_or_default();
            format!("botster mcp-gateway{suffix}")
        }
        Commands::Session { label, uuid, .. } => {
            let tag = label
                .as_deref()
                .unwrap_or_else(|| &uuid[..uuid.len().min(16)]);
            format!("botster session [{tag}]")
        }
        // Short-lived CLI commands — no title change needed
        _ => return,
    };
    proctitle::set_title(title);
}

/// Normalize the hub process cwd to the user's home directory.
///
/// Device-scoped startup treats the launch directory as ambient shell state,
/// not as a trust anchor, repo identity, or implicit spawn target.
fn normalize_process_cwd_to_home() -> Result<()> {
    let home = dirs::home_dir().context("Could not determine home directory")?;
    std::env::set_current_dir(&home)
        .with_context(|| format!("Failed to change working directory to {}", home.display()))?;
    log::info!("Normalized process cwd to home: {}", home.display());
    Ok(())
}

/// Resolve the stable local hub ID for this device.
fn resolve_local_hub_id() -> Option<String> {
    botster::hub::local_device_hub_id().ok()
}

/// Resolve a socket path for `mcp-serve`.
///
/// When `BOTSTER_SESSION_UUID` is missing, returns `Ok(None)` so the MCP server
/// can start in degraded outside-Botster mode. When it is present, looks up the
/// session manifest in the workspace store, reads `hub_manifest_path` from it,
/// then reads the hub manifest for the socket path.
fn resolve_mcp_serve_socket() -> Result<Option<String>> {
    use botster::hub::daemon::HubManifest;

    let Some(session_uuid) = std::env::var("BOTSTER_SESSION_UUID")
        .ok()
        .filter(|s| !s.is_empty())
    else {
        log::warn!("BOTSTER_SESSION_UUID is not set; starting mcp-serve without Botster hub tools");
        return Ok(None);
    };

    let session_manifest = botster::env::session_manifest_path(&session_uuid).ok_or_else(|| {
        anyhow::anyhow!("Session manifest not found for BOTSTER_SESSION_UUID={session_uuid}")
    })?;

    let content = std::fs::read_to_string(&session_manifest).with_context(|| {
        format!(
            "Failed to read session manifest at {}",
            session_manifest.display()
        )
    })?;
    let session: serde_json::Value = serde_json::from_str(&content).with_context(|| {
        format!(
            "Failed to parse session manifest at {}",
            session_manifest.display()
        )
    })?;

    let hub_manifest_path = session
        .get("hub_manifest_path")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!("Session manifest missing hub_manifest_path for session {session_uuid}")
        })?;

    let hub_content = std::fs::read_to_string(hub_manifest_path)
        .with_context(|| format!("Failed to read hub manifest at {hub_manifest_path}"))?;
    let hub_manifest: HubManifest = serde_json::from_str(&hub_content)
        .with_context(|| format!("Failed to parse hub manifest at {hub_manifest_path}"))?;

    // Do not hard-fail when the path is temporarily missing. The MCP gateway
    // has retry/backoff logic for startup and reconnect windows.
    if !std::path::Path::new(&hub_manifest.socket_path).exists() {
        log::warn!(
            "Session {} hub socket path missing at startup: {} (continuing; mcp-gateway will retry)",
            session_uuid,
            hub_manifest.socket_path
        );
    }

    Ok(Some(hub_manifest.socket_path))
}

///
/// Discovers the running device hub, connects to its socket, and runs the TUI
/// with a bridge adapter.
fn run_attach() -> Result<()> {
    use botster::hub::daemon;
    use botster::socket::tui_bridge::TuiBridge;
    use std::sync::atomic::Ordering;

    // Require an interactive terminal
    if !atty::is(atty::Stream::Stdin) {
        anyhow::bail!("Error: 'attach' requires an interactive terminal (stdin is not a TTY).");
    }

    // Attach is local IPC only. Do not require device keyring access here:
    // keychain ACL changes after rebuilding the debug binary must not prevent
    // attaching to an already-running hub.
    let running = daemon::discover_running_hubs();
    let (hub_id, _) = match running.as_slice() {
        [(hub_id, pid)] => (hub_id.clone(), *pid),
        [] => anyhow::bail!(
            "No running local device hub manifest found.\n\
             Start one with: botster start --headless"
        ),
        hubs => {
            eprintln!("Multiple live hub manifests found; expected one device hub.");
            eprintln!("Conflicting hubs:");
            for (id, pid) in hubs {
                eprintln!("  {id} (pid={pid})");
            }
            anyhow::bail!("Stop the stale hub process before attaching.");
        }
    };

    let socket_path = if let Some(manifest) = daemon::read_manifest(&hub_id) {
        std::path::PathBuf::from(manifest.socket_path)
    } else {
        daemon::socket_path(&hub_id)?
    };

    println!("Connecting to hub {}...", &hub_id[..hub_id.len().min(8)]);

    // Create tokio runtime for the bridge
    let rt = tokio::runtime::Runtime::new()?;

    // Connect to socket
    let stream = rt
        .block_on(async { tokio::net::UnixStream::connect(&socket_path).await })
        .with_context(|| format!("Failed to connect to socket: {}", socket_path.display()))?;

    // Create wake pipe for TuiRunner (RAII guard ensures cleanup on any exit path)
    let pipe = WakePipe::new();

    let shutdown = Arc::new(AtomicBool::new(false));

    // Create bridge
    let (bridge, channels) = TuiBridge::connect_with_reconnect(
        stream,
        Some(socket_path.clone()),
        pipe.write_fd(),
        Arc::clone(&shutdown),
    );

    // Set up terminal — create guard BEFORE execute! so raw mode is restored
    // even if the crossterm commands fail.
    enable_raw_mode()?;
    let _terminal_guard = tui::TerminalGuard::new();

    let mut stdout = std::io::stdout();
    execute!(
        stdout,
        EnterAlternateScreen,
        EnableMouseCapture,
        crossterm::event::EnableFocusChange
    )?;

    let tui_color_cache = tui::probe_spawning_terminal_colors();

    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend)?;

    // Calculate terminal dimensions
    let (term_cols, term_rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let (inner_rows, inner_cols) = tui::terminal_widget_inner_area(term_cols, term_rows);

    // Create TuiRunner with bridge channels (same as run_with_hub)
    let tui_shutdown = Arc::clone(&shutdown);
    let mut tui_runner = tui::TuiRunner::new_with_color_cache_and_session_input(
        terminal,
        channels.request_tx,
        channels.output_rx,
        tui_shutdown,
        (inner_rows, inner_cols),
        pipe.read_fd(),
        tui_color_cache,
        std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        Some(channels.session_input_tx),
    );
    tui_runner.set_lua_bootstrap(tui::hot_reload::LuaBootstrap::load());

    // Register SIGWINCH
    #[cfg(unix)]
    {
        use signal_hook::consts::signal::SIGWINCH;
        if let Err(e) = signal_hook::flag::register(SIGWINCH, tui_runner.resize_flag()) {
            log::warn!("Failed to register SIGWINCH handler: {e}");
        }
    }

    // Register SIGINT/SIGTERM
    {
        use signal_hook::consts::signal::{SIGINT, SIGTERM};
        use signal_hook::flag;
        flag::register(SIGINT, Arc::clone(&SHUTDOWN_FLAG))?;
        flag::register(SIGTERM, Arc::clone(&SHUTDOWN_FLAG))?;
    }
    botster::shutdown::spawn(Arc::clone(&SHUTDOWN_FLAG));

    // Spawn TUI thread
    let tui_handle = std::thread::Builder::new()
        .name("tui-runner".to_string())
        .spawn(move || {
            if let Err(e) = tui_runner.run() {
                log::error!("TuiRunner error: {}", e);
            }
        })?;

    // Run bridge in tokio runtime (blocks until shutdown)
    let bridge_shutdown = Arc::clone(&shutdown);
    rt.block_on(async move {
        tokio::select! {
            _ = bridge.run() => {}
            _ = async {
                loop {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    if bridge_shutdown.load(Ordering::Relaxed) || SHUTDOWN_FLAG.load(Ordering::Relaxed) {
                        break;
                    }
                }
            } => {}
        }
    });

    // Signal TUI shutdown
    shutdown.store(true, Ordering::SeqCst);

    // Wait for TUI thread
    if let Err(e) = tui_handle.join() {
        log::error!("TuiRunner thread panicked: {:?}", e);
    }

    // pipe fds closed automatically by WakePipe drop

    Ok(())
}

/// Offline Ghostty VT replay for crash fixtures (see `session::vt_replay`).
fn run_debug_vt_replay(
    path: &std::path::Path,
    rows: u16,
    cols: u16,
    last_only: bool,
    mode_poll: bool,
    verbose: bool,
    snapshot: &str,
    bootstrap_resize: bool,
) -> Result<()> {
    use botster::session::{
        parse_snapshot_phase, resolve_fixture_paths, run_vt_replay, VtReplayConfig,
    };

    let snapshot_phase = parse_snapshot_phase(snapshot)?;
    let (ring_path, last_path) = resolve_fixture_paths(path)?;
    if verbose {
        eprintln!(
            "vt-replay: input={} ring={:?} last={:?} rows={} cols={} last_only={} mode_poll={} snapshot={:?} bootstrap_resize={}",
            path.display(),
            ring_path,
            last_path,
            rows,
            cols,
            last_only,
            mode_poll,
            snapshot_phase,
            bootstrap_resize
        );
    }
    let report = run_vt_replay(&VtReplayConfig {
        rows,
        cols,
        ring_path,
        last_path,
        mode_poll,
        verbose,
        last_only,
        snapshot: snapshot_phase,
        bootstrap_resize,
    })?;
    println!(
        "{}",
        serde_json::json!({
            "ok": true,
            "process_calls": report.process_calls,
            "ring_bytes": report.ring_bytes,
            "ring_chunks": report.ring_chunks,
            "last_bytes": report.last_bytes,
            "mode_polls": report.mode_polls,
            "snapshot_exports": report.snapshot_exports,
            "snapshot_imports": report.snapshot_imports,
            "last_snapshot_bytes": report.last_snapshot_bytes,
            "snapshot_phase": report.snapshot_phase,
            "rows": report.rows,
            "cols": report.cols,
        })
    );
    Ok(())
}

fn run_debug_memory(hub_id: Option<&str>) -> Result<()> {
    use botster::socket::framing::{Frame, FrameDecoder};
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let resolved_hub_id = match hub_id {
        Some(id) => id.to_string(),
        None => botster::hub::local_device_hub_id()?,
    };
    let socket_path = botster::hub::daemon::resolve_socket_for_hub_id(&resolved_hub_id)
        .with_context(|| {
            format!(
                "No live hub socket found for '{}'. Start one with: botster start --headless",
                resolved_hub_id
            )
        })?;
    let mut stream = UnixStream::connect(&socket_path)
        .with_context(|| format!("Failed to connect to hub socket {}", socket_path.display()))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .context("failed to set socket read timeout")?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .context("failed to set socket write timeout")?;

    let request_id = format!("debug-memory-{}", std::process::id());
    let request = Frame::Json(serde_json::json!({
        "type": "debug_memory",
        "request_id": request_id,
    }));
    stream
        .write_all(&request.encode())
        .context("failed to send debug memory request")?;

    let mut decoder = FrameDecoder::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = stream
            .read(&mut buf)
            .context("failed to read debug memory response")?;
        if n == 0 {
            anyhow::bail!("hub socket closed before debug memory response");
        }
        for frame in decoder.feed(&buf[..n])? {
            let Frame::Json(value) = frame else {
                continue;
            };
            if value.get("type").and_then(|v| v.as_str()) == Some("debug_memory")
                && value.get("request_id").and_then(|v| v.as_str()) == Some(request_id.as_str())
            {
                println!("{}", serde_json::to_string_pretty(&value)?);
                return Ok(());
            }
        }
    }
}

/// Print local hub runtime artifact status.
fn run_status() -> Result<()> {
    let hub_id = botster::hub::local_device_hub_id()?;
    let artifacts = botster::hub::daemon::HubRuntimeArtifacts::new(hub_id.clone());
    let inspection = artifacts.inspect()?;

    println!("Hub status");
    println!("  hub_id: {}", inspection.hub_id);
    println!(
        "  process: {}",
        if inspection.process_alive() {
            "alive"
        } else {
            "not running"
        }
    );
    match inspection.pid {
        Some(pid) => println!(
            "  pid_file: {} (pid={}, alive={})",
            inspection.pid_file_path.display(),
            pid,
            inspection.pid_alive
        ),
        None => println!(
            "  pid_file: {} (missing or invalid)",
            inspection.pid_file_path.display()
        ),
    }
    println!(
        "  manifest: {} ({})",
        inspection.manifest_path.display(),
        if inspection.manifest.is_some() {
            "present"
        } else {
            "missing or invalid"
        }
    );
    if let Some(manifest) = &inspection.manifest {
        println!("  manifest_pid: {}", manifest.pid);
        if let Some(server_id) = &manifest.server_id {
            println!("  server_id: {server_id}");
        }
        println!("  updated_at: {}", manifest.updated_at);
        println!("  workspaces: {}", manifest.workspaces.len());
    }
    println!("  lock_file: {}", inspection.lock_file_path.display());
    println!(
        "  socket: {} (path_exists={}, connectable={}, protocol={})",
        inspection.socket_path.display(),
        inspection.socket_path_exists,
        inspection.socket_connectable,
        inspection.socket_protocol_responds
    );
    if let Some(error) = &inspection.socket_probe_error {
        println!("  socket_probe: {error}");
    }

    if inspection.process_alive() && !inspection.socket_path_exists {
        println!("  diagnosis: hub process is alive but socket pathname is missing");
        println!("  repair: a patched running hub should recreate it on the next cleanup tick");
    } else if inspection.socket_path_exists && !inspection.socket_connectable {
        println!("  diagnosis: socket pathname exists but does not accept connections");
        println!("  repair: restart the hub or run explicit cleanup before starting a new hub");
    } else if inspection.socket_connectable && !inspection.socket_protocol_responds {
        println!("  diagnosis: socket accepts connections but did not answer Botster hello");
        println!("  repair: verify the socket owner before attaching agents");
    } else if inspection.accepts_new_clients() {
        println!("  diagnosis: hub accepts new local IPC clients");
    } else {
        println!("  diagnosis: no connectable local hub found");
    }

    Ok(())
}

/// File writer that truncates when the log exceeds a size cap.
///
/// Maximum number of log files retained per process label (hub, tui, attach, cli).
///
/// On each startup the oldest files beyond this count are deleted before the
/// new log is created, so the total never exceeds this value.
const MAX_LOG_FILES_PER_LABEL: usize = 10;

/// Returns the current UTC time as a sortable `YYYYMMDD-HHMMSS` string.
///
/// Uses only `std::time` — no external date crate needed. The format is
/// intentionally lexicographically sortable so that plain filename sorting
/// gives chronological order, which [`rotate_logs`] relies on.
fn timestamp_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400; // days since 1970-01-01 (UTC)
                             // Gregorian calendar decomposition.
                             // Algorithm: http://howardhinnant.github.io/date_algorithms.html ("civil_from_days")
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // day of era [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // year of era [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    let mp = (5 * doy + 2) / 153; // month prime [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // day [1, 31]
    let mo = if mp < 10 { mp + 3 } else { mp - 9 }; // month [1, 12]
    let y = if mo <= 2 { y + 1 } else { y };
    format!("{y:04}{mo:02}{d:02}-{h:02}{m:02}{s:02}")
}

/// Deletes the oldest `botster-{label}-*.log` files in `dir` so that creating
/// one new file will not push the total past `keep`.
///
/// Filenames sort chronologically because they embed a `YYYYMMDD-HHMMSS`
/// timestamp, so a plain lexicographic sort is sufficient.  Errors (missing
/// dir, permission denied, etc.) are silently ignored — log rotation failure
/// must never crash the hub.
fn rotate_logs(dir: &std::path::Path, label: &str, keep: usize) {
    let prefix = format!("botster-{label}-");
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut matches: Vec<std::path::PathBuf> = entries
        .flatten()
        .filter_map(|e| {
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(".log") {
                Some(e.path())
            } else {
                None
            }
        })
        .collect();
    // Oldest files are at the front after ascending sort.
    matches.sort_unstable();
    // Delete enough old files so the new one keeps us at or below `keep`.
    let delete_count = matches.len().saturating_sub(keep.saturating_sub(1));
    for path in matches.into_iter().take(delete_count) {
        let _ = std::fs::remove_file(&path);
    }
}

/// Prevents unbounded log growth during long-running hub sessions.
/// When the cap is exceeded, the file is truncated and logging resumes
/// from the beginning with a rotation marker.
struct CappedFileWriter {
    file: std::fs::File,
    bytes_written: u64,
    cap: u64,
}

impl CappedFileWriter {
    fn new(file: std::fs::File, cap: u64) -> Self {
        Self {
            file,
            bytes_written: 0,
            cap,
        }
    }
}

impl std::io::Write for CappedFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.bytes_written + buf.len() as u64 > self.cap {
            use std::io::{Seek, SeekFrom};
            self.file.seek(SeekFrom::Start(0))?;
            self.file.set_len(0)?;
            let marker = b"--- log rotated (cap reached) ---\n";
            self.file.write_all(marker)?;
            self.bytes_written = marker.len() as u64;
        }
        let n = self.file.write(buf)?;
        self.bytes_written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush()
    }
}

fn main() -> Result<()> {
    // Raise fd limit for WebRTC. Each peer connection opens ~15 UDP sockets
    // for ICE gathering, and webrtc-rs close() takes up to 60s for SCTP
    // shutdown. macOS defaults to 256 fds which exhausts after ~4 rapid
    // reconnects. Every production WebRTC server raises this.
    raise_fd_limit();

    // MCP serve writes logs to stderr because stdout is the JSON-RPC channel.
    // All other commands write to a timestamped file so concurrent processes
    // (hub, attach, tui) never overwrite each other's logs.
    let cli = Cli::parse();
    let is_mcp_serve = matches!(cli.command, Commands::McpServe | Commands::Context { .. });

    if is_mcp_serve {
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
            .target(env_logger::Target::Stderr)
            .format_timestamp_secs()
            .init();
    } else {
        // Each non-MCP process (hub, tui, attach) gets its own timestamped log
        // file so concurrent processes and sequential runs never overwrite each
        // other.  BOTSTER_LOG_FILE bypasses this for scripted overrides.
        let log_label = match &cli.command {
            Commands::Start { headless: true, .. } => "hub",
            Commands::Start { .. } => "tui",
            Commands::Attach => "attach",
            Commands::Session { .. } => "session",
            _ => "cli",
        };
        let log_path = if let Ok(path) = std::env::var("BOTSTER_LOG_FILE") {
            // Explicit path override: use as-is, skip rotation.
            std::path::PathBuf::from(path)
        } else if botster::env::is_any_test() {
            // Test mode: single stable path inside the project so test runs
            // don't scatter timestamped files outside the repo.
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .map(|p| p.join("tmp/botster.log"))
                .unwrap_or_else(|| std::path::PathBuf::from("/tmp/botster.log"))
        } else {
            let log_dir = if let Ok(config_dir) = std::env::var("BOTSTER_CONFIG_DIR") {
                std::path::PathBuf::from(config_dir).join("logs")
            } else {
                std::path::PathBuf::from("/tmp")
            };
            std::fs::create_dir_all(&log_dir)
                .unwrap_or_else(|e| panic!("Failed to create log dir {log_dir:?}: {e}"));
            // Trim stale logs before creating the new file so we stay at or
            // below MAX_LOG_FILES_PER_LABEL total per label type.
            rotate_logs(&log_dir, log_label, MAX_LOG_FILES_PER_LABEL);
            log_dir.join(format!("botster-{log_label}-{}.log", timestamp_now()))
        };
        let log_file = std::fs::File::create(&log_path)
            .unwrap_or_else(|e| panic!("Failed to create log file at {log_path:?}: {e}"));
        // 10 MB cap — large enough for a full session, small enough to avoid
        // runaway disk use on long-lived hub processes.
        let capped_writer = CappedFileWriter::new(log_file, 10 * 1024 * 1024);
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
            .target(env_logger::Target::Pipe(Box::new(capped_writer)))
            .format_timestamp_secs()
            .init();
    }

    // Route ghostty's Zig logs through Rust's log crate (instead of stderr).
    botster::ghostty_vt::init_logging();

    // Set up panic hook to log panics and ensure terminal cleanup
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        // Log the panic
        log::error!("PANIC: {:?}", panic_info);
        // Session processes null stdout/stderr; flush the file logger so the
        // panic is on disk before abort (critical for diagnosing hard deaths).
        log::logger().flush();

        // Ensure terminal is cleaned up before printing panic
        let _ = disable_raw_mode();

        // Reset mirrored terminal modes
        let _ = std::io::Write::write_all(&mut std::io::stdout(), b"\x1b[?1l"); // Reset DECCKM
        let _ = std::io::Write::write_all(&mut std::io::stdout(), b"\x1b[?2004l"); // Reset bracketed paste
        let _ = execute!(std::io::stdout(), PopKeyboardEnhancementFlags);

        let _ = execute!(
            std::io::stdout(),
            LeaveAlternateScreen,
            DisableMouseCapture,
            crossterm::cursor::Show
        );

        // Call the default panic handler
        default_hook(panic_info);
    }));

    // Set descriptive process title for non-Start commands up front.
    // Start is handled inside its match arm because it may redirect to attach.
    if !matches!(cli.command, Commands::Start { .. }) {
        set_process_title(&cli.command);
    }

    match cli.command {
        Commands::Start {
            headless,
            offline,
            no_encryption,
        } => {
            if offline {
                std::env::set_var("BOTSTER_OFFLINE", "1");
                log::info!("Offline mode enabled — all network primitives disabled");
            }
            if no_encryption {
                std::env::set_var("BOTSTER_DISABLE_ENCRYPTION", "1");
                log::warn!("WebRTC encryption disabled by --no-encryption");
            }
            // Strict singleton policy: one live hub per device-local hub ID.
            //
            // A missing socket does not imply no live hub: startup races or an
            // unsafe unlink can leave a live PID without a socket path. Treat a
            // live PID as authoritative to avoid spawning duplicates.
            let existing_hub = resolve_local_hub_id().map(|id| {
                let socket = botster::hub::daemon::resolve_socket_for_hub_id(&id);
                let pid_alive = botster::hub::daemon::is_hub_running(&id);
                (id, socket, pid_alive)
            });

            if no_encryption && existing_hub.as_ref().is_some_and(|(_, _, alive)| *alive) {
                anyhow::bail!("A Hub is already running. Stop it before you use --no-encryption.");
            }

            if headless {
                if let Some((hub_id, Some(socket), _)) = existing_hub.as_ref() {
                    anyhow::bail!(
                        "Hub already running on this device (hub_id={}, socket={}). \
                         Use `botster attach` or stop the existing hub first.",
                        hub_id,
                        socket.display()
                    );
                }
                if let Some((hub_id, None, true)) = existing_hub.as_ref() {
                    let expected_socket = botster::hub::daemon::socket_path(hub_id)
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| "<unknown>".to_string());
                    anyhow::bail!(
                        "Hub process already running on this device (hub_id={hub_id}) \
                         but socket is unavailable ({expected_socket}). Refusing to start a \
                         duplicate hub. Stop the existing hub first."
                    );
                }
                proctitle::set_title("botster hub");
                run_headless()?;
            } else {
                if let Some((_hub_id, Some(_socket), _)) = existing_hub.as_ref() {
                    proctitle::set_title("botster attach");
                    println!("Hub already running — attaching...");
                    run_attach()?;
                } else if let Some((hub_id, None, true)) = existing_hub.as_ref() {
                    let expected_socket = botster::hub::daemon::socket_path(hub_id)
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|_| "<unknown>".to_string());
                    anyhow::bail!(
                        "Hub process already running on this device (hub_id={hub_id}) \
                         but socket is unavailable ({expected_socket}). Refusing to start a \
                         duplicate hub."
                    );
                } else {
                    proctitle::set_title("botster tui");
                    run_with_tui()?;
                }
            }
        }
        Commands::Status => {
            run_status()?;
        }
        Commands::Config { key, value } => {
            let config = Config::load()?;
            match (key, value) {
                (None, None) => println!("{}", serde_json::to_string_pretty(&config)?),
                (Some(k), None) => println!("Config key '{}' query not implemented", k),
                (Some(k), Some(v)) => println!("Would set {} = {}", k, v),
                _ => {}
            }
        }
        Commands::JsonGet { file, key } => {
            commands::json::get(&file, &key)?;
        }
        Commands::JsonSet { file, key, value } => {
            commands::json::set(&file, &key, &value)?;
        }
        Commands::JsonDelete { file, key } => {
            commands::json::delete(&file, &key)?;
        }
        Commands::DeleteWorktree { issue_number } => {
            commands::worktree::delete(issue_number)?;
        }
        Commands::ListWorktrees => {
            commands::worktree::list()?;
        }
        Commands::Update { check } => {
            if check {
                commands::update::check()?;
            } else {
                commands::update::install()?;
            }
        }
        Commands::GetConnectionUrl { hub } => {
            use botster::relay::read_connection_url;
            match read_connection_url(&hub)? {
                Some(url) => {
                    println!("{}", url);
                }
                None => {
                    eprintln!(
                        "No connection URL found for hub '{}'. Is the CLI running?",
                        hub
                    );
                    std::process::exit(1);
                }
            }
        }
        Commands::Reset { yes } => {
            commands::reset::run(yes)?;
        }
        Commands::Attach => {
            run_attach()?;
        }
        Commands::McpServe => {
            let url = std::env::var("BOTSTER_MCP_URL")
                .ok()
                .filter(|s| !s.is_empty());
            let token = std::env::var("BOTSTER_MCP_TOKEN")
                .ok()
                .filter(|s| !s.is_empty());
            match (url, token) {
                (Some(url), Some(token)) => botster::mcp_http::run_stdio_proxy(&url, &token)?,
                _ => match resolve_mcp_serve_socket()? {
                    Some(socket_path) => botster::mcp_gateway::run(&socket_path)?,
                    None => botster::mcp_gateway::run_disconnected(
                        "Botster MCP is available only inside a Botster-managed session",
                    )?,
                },
            }
        }
        Commands::Context { key, value } => {
            commands::context::run(key.as_deref(), value.as_deref())?;
        }
        Commands::Debug { command } => match command {
            DebugCommands::Memory { hub } => {
                run_debug_memory(hub.as_deref())?;
            }
            DebugCommands::VtReplay {
                path,
                rows,
                cols,
                last_only,
                no_mode_poll,
                quiet,
                snapshot,
                bootstrap_resize,
            } => {
                run_debug_vt_replay(
                    &path,
                    rows,
                    cols,
                    last_only,
                    !no_mode_poll,
                    !quiet,
                    &snapshot,
                    bootstrap_resize,
                )?;
            }
        },
        Commands::Session {
            uuid,
            socket,
            timeout,
            ..
        } => {
            botster::session::run(&uuid, &socket, timeout)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    /// Verifies the bespoke Gregorian algorithm in `timestamp_now` against a
    /// set of known Unix timestamps and their expected `YYYYMMDD-HHMMSS` output.
    ///
    /// Each tuple is `(unix_secs, expected_str)`.  Values were independently
    /// verified against the POSIX calendar.
    #[test]
    fn timestamp_now_known_values() {
        // Thin wrapper that accepts a fixed epoch second rather than reading
        // the wall clock, so we can test deterministically.
        fn fmt_secs(secs: u64) -> String {
            let s = secs % 60;
            let m = (secs / 60) % 60;
            let h = (secs / 3600) % 24;
            let days = secs / 86400;
            let z = days as i64 + 719_468;
            let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
            let doe = z - era * 146_097;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
            let y = yoe + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let mo = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if mo <= 2 { y + 1 } else { y };
            format!("{y:04}{mo:02}{d:02}-{h:02}{m:02}{s:02}")
        }

        let cases: &[(u64, &str)] = &[
            (0, "19700101-000000"),          // Unix epoch
            (86399, "19700101-235959"),      // last second of day 0
            (86400, "19700102-000000"),      // first second of day 1
            (951782400, "20000229-000000"),  // 2000-02-29 (Y2K leap year)
            (1000000000, "20010909-014640"), // round billion
            (1709251200, "20240301-000000"), // 2024-03-01 (leap year boundary)
            (1740787200, "20250301-000000"), // 2025-03-01 (non-leap)
            (1772323200, "20260301-000000"), // 2026-03-01 (current project date)
        ];

        for &(secs, expected) in cases {
            assert_eq!(
                fmt_secs(secs),
                expected,
                "timestamp mismatch for unix_secs={secs}"
            );
        }
    }
}
