//! Botster runtime library.
//!
//! This crate provides the local Botster runtime used by the `botster`
//! command: hub orchestration, PTY/session workers, client transports,
//! Lua plugins, and renderer adapters.
//!
//! # Architecture
//!
//! Botster splits control-plane orchestration from data-plane streaming:
//!
//! - **Hub** - Central control-plane orchestrator and lifecycle policy owner
//! - **Session I/O workers** - Per-session PTY data-plane readers and mailboxes
//! - **Client workers** - Transport-neutral stream state for TUI, browser, and socket clients
//! - **Lua runtime** - Hot-reloadable product behavior, plugins, hooks, and UI composition
//! - **Rails server** - Auth, registry, pairing/signaling, and browser shell
//!
//! # Modules
//!
//! - [`agent`] - Agent and PTY session management
//! - [`app`] - Legacy TUI state types and input handling
//! - [`server`] - Rails API client
//! - [`config`] - Configuration loading/saving

// Library modules
pub mod agent;
pub mod app;
pub mod auth;
pub mod channel;
pub mod client;
pub mod clients;
pub mod commands;
pub mod hub;
pub mod lua;
pub mod mcp_gateway;
pub mod mcp_http;
pub mod relay;
pub mod session;
pub mod socket;
pub mod ws;

pub mod compat;
pub mod config;
pub mod constants;
pub mod crypto;
pub mod device;
pub mod env;
pub mod file_watcher;
#[allow(missing_docs, missing_debug_implementations)]
pub mod ghostty_vt;
pub mod git;
pub mod keyring;
pub mod notifications;
pub mod plugin_helpers;
pub mod process;
pub mod server;
pub mod shutdown;
pub mod spawn_targets;
pub mod terminal;
pub mod terminal_widget;
pub mod terminfo;
pub mod ui_contract;
pub mod worker;

// Re-export commonly used types
pub use agent::Agent;
pub use config::Config;
pub use git::WorktreeManager;
pub use relay::{AgentInfo, TerminalMessage};
pub use spawn_targets::{SpawnTarget, SpawnTargetInspection, SpawnTargetRegistry};
pub use terminal_widget::TerminalWidget;

// Re-export Hub
pub use hub::Hub;
