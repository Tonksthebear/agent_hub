//! Runtime environment detection.
//!
//! Provides a single source of truth for determining the runtime environment
//! (test, development, production) based on the `BOTSTER_ENV` environment variable.
//!
//! # Usage
//!
//! ```rust
//! use botster::env::{Environment, is_test_mode};
//!
//! // Check current environment
//! if Environment::current().is_test() {
//!     // Skip keyring, auth, etc.
//! }
//!
//! // Or use the convenience function
//! if is_test_mode() {
//!     // Test-specific behavior
//! }
//! ```
//!
//! # Environment Variable
//!
//! Set `BOTSTER_ENV` to one of:
//! - `test` - Test mode (skips auth, uses file storage instead of keyring)
//! - `system_test` - System test mode (full auth with test server, uses file storage)
//! - `development` or `dev` - Development mode
//! - (anything else or unset) - Production mode

/// Application identity: `"botster"` in release, `"botster-dev"` in debug.
///
/// Scopes keyring service, config directory, and all on-disk storage so that
/// dev-built and release-installed binaries can coexist on the same machine
/// without stomping each other's credentials or state.
pub const APP_NAME: &str = if cfg!(debug_assertions) {
    "botster-dev"
} else {
    "botster"
};

/// Default server URL: dev server in debug builds, production in release.
pub const DEFAULT_SERVER_URL: &str = if cfg!(debug_assertions) {
    "https://dev.trybotster.com"
} else {
    "https://trybotster.com"
};

/// Runtime environment for the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Environment {
    /// Production environment (default).
    Production,
    /// Development environment.
    Development,
    /// Test environment - skips auth, uses file storage.
    Test,
    /// System test environment - full auth with test server, uses file storage.
    /// Used for Rails system tests that spawn the CLI.
    SystemTest,
}

impl Environment {
    /// Detect current environment from `BOTSTER_ENV`.
    ///
    /// Returns `Test` if `BOTSTER_ENV=test`, `SystemTest` if `BOTSTER_ENV=system_test`,
    /// `Development` if `BOTSTER_ENV=development` or `BOTSTER_ENV=dev`, otherwise `Production`.
    #[must_use]
    pub fn current() -> Self {
        match std::env::var("BOTSTER_ENV").as_deref() {
            Ok("test") => Self::Test,
            Ok("system_test") => Self::SystemTest,
            Ok("development") | Ok("dev") => Self::Development,
            _ => Self::Production,
        }
    }

    /// Returns `true` if this is the test environment (unit tests).
    #[must_use]
    pub fn is_test(self) -> bool {
        self == Self::Test
    }

    /// Returns `true` if this is the system test environment (Rails system tests).
    #[must_use]
    pub fn is_system_test(self) -> bool {
        self == Self::SystemTest
    }

    /// Returns `true` if running in any test mode (test or system_test).
    /// Use this to skip OS keyring and use file storage instead.
    #[must_use]
    pub fn is_any_test(self) -> bool {
        matches!(self, Self::Test | Self::SystemTest)
    }

    /// Returns `true` if this is the production environment.
    #[must_use]
    pub fn is_production(self) -> bool {
        self == Self::Production
    }

    /// Returns `true` if this is the development environment.
    #[must_use]
    pub fn is_development(self) -> bool {
        self == Self::Development
    }
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Production => write!(f, "production"),
            Self::Development => write!(f, "development"),
            Self::Test => write!(f, "test"),
            Self::SystemTest => write!(f, "system_test"),
        }
    }
}

/// Convenience function to check if running in test mode (unit tests only).
///
/// Equivalent to `Environment::current().is_test()`.
/// For keyring bypass, use `should_skip_keyring()` instead.
#[must_use]
pub fn is_test_mode() -> bool {
    Environment::current().is_test()
}

/// Returns `true` if running in any test mode (unit tests or system tests).
///
/// Use this for timeouts, intervals, file path fallbacks, etc.
/// Returns true for both `BOTSTER_ENV=test` and `BOTSTER_ENV=system_test`.
#[must_use]
pub fn is_any_test() -> bool {
    Environment::current().is_any_test()
}

/// Returns `true` if running in offline mode (`BOTSTER_OFFLINE=1`).
///
/// When offline, all network primitives are disabled: no auth validation,
/// no server registration, no WebRTC/ActionCable, no browser relay.
/// The hub runs as a purely local PTY manager.
#[must_use]
pub fn is_offline() -> bool {
    std::env::var("BOTSTER_OFFLINE").as_deref() == Ok("1")
}

/// Returns `true` if WebRTC signaling and data-channel encryption are disabled.
#[must_use]
pub fn is_encryption_disabled() -> bool {
    std::env::var("BOTSTER_DISABLE_ENCRYPTION").as_deref() == Ok("1")
}

/// Returns `true` if keyring should be bypassed (any test mode).
///
/// Use this instead of `is_test_mode()` when deciding whether to use
/// OS keyring vs file storage. Returns true for both `BOTSTER_ENV=test`
/// and `BOTSTER_ENV=system_test`.
#[must_use]
pub fn should_skip_keyring() -> bool {
    is_any_test()
}

/// Resolve the data directory (`~/.botster` or `~/.botster-dev`).
///
/// Checks `BOTSTER_CONFIG_DIR` env var first, then falls back to
/// `~/.{APP_NAME}` based on debug/release build.
#[must_use]
pub fn data_dir() -> Option<std::path::PathBuf> {
    if let Ok(custom) = std::env::var("BOTSTER_CONFIG_DIR") {
        return Some(std::path::PathBuf::from(custom));
    }
    let app_name = format!(".{APP_NAME}");
    dirs::home_dir().map(|d| d.join(app_name))
}

/// Find a session manifest path by UUID in the workspace store.
///
/// Scans `{data_dir}/workspaces/*/sessions/{uuid}/manifest.json`.
#[must_use]
pub fn session_manifest_path(session_uuid: &str) -> Option<std::path::PathBuf> {
    let workspaces_dir = data_dir()?.join("workspaces");
    for entry in std::fs::read_dir(&workspaces_dir).ok()?.flatten() {
        let path = entry
            .path()
            .join("sessions")
            .join(session_uuid)
            .join("manifest.json");
        if path.exists() {
            return Some(path);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_environment_display() {
        assert_eq!(Environment::Production.to_string(), "production");
        assert_eq!(Environment::Development.to_string(), "development");
        assert_eq!(Environment::Test.to_string(), "test");
        assert_eq!(Environment::SystemTest.to_string(), "system_test");
    }

    #[test]
    fn test_environment_is_methods() {
        assert!(Environment::Test.is_test());
        assert!(!Environment::Test.is_production());
        assert!(!Environment::Test.is_development());
        assert!(!Environment::Test.is_system_test());

        assert!(Environment::Production.is_production());
        assert!(!Environment::Production.is_test());

        assert!(Environment::Development.is_development());
        assert!(!Environment::Development.is_test());

        assert!(Environment::SystemTest.is_system_test());
        assert!(!Environment::SystemTest.is_test());
        assert!(!Environment::SystemTest.is_production());
    }

    #[test]
    fn test_is_any_test() {
        assert!(Environment::Test.is_any_test());
        assert!(Environment::SystemTest.is_any_test());
        assert!(!Environment::Production.is_any_test());
        assert!(!Environment::Development.is_any_test());
    }

    // ── is_offline ────────────────────────────────────────────────────────

    #[test]
    fn test_is_offline_default_false() {
        let _lock = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BOTSTER_OFFLINE");
        assert!(!is_offline(), "is_offline should be false by default");
    }

    #[test]
    fn test_is_offline_with_env_var() {
        let _lock = ENV_LOCK.lock().unwrap();
        std::env::set_var("BOTSTER_OFFLINE", "1");
        assert!(
            is_offline(),
            "is_offline should be true when BOTSTER_OFFLINE=1"
        );
        std::env::remove_var("BOTSTER_OFFLINE");
    }

    #[test]
    fn test_is_offline_wrong_value() {
        let _lock = ENV_LOCK.lock().unwrap();
        std::env::set_var("BOTSTER_OFFLINE", "true");
        assert!(
            !is_offline(),
            "is_offline should be false for values other than '1'"
        );
        std::env::remove_var("BOTSTER_OFFLINE");
    }

    #[test]
    fn test_is_encryption_disabled_uses_explicit_flag_value() {
        let _lock = ENV_LOCK.lock().unwrap();
        std::env::remove_var("BOTSTER_DISABLE_ENCRYPTION");
        assert!(!is_encryption_disabled());
        std::env::set_var("BOTSTER_DISABLE_ENCRYPTION", "1");
        assert!(is_encryption_disabled());
        std::env::remove_var("BOTSTER_DISABLE_ENCRYPTION");
    }

    // ── session_manifest_path fault injection ─────────────────────────────

    /// Serialize env-mutating tests to prevent BOTSTER_CONFIG_DIR races.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// session_manifest_path returns None when the workspaces dir doesn't exist.
    #[test]
    fn session_manifest_path_missing_workspaces_dir() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        std::env::set_var("BOTSTER_CONFIG_DIR", dir.path());
        let result = session_manifest_path("sess-nonexistent");
        assert!(result.is_none(), "missing workspaces dir must return None");
        std::env::remove_var("BOTSTER_CONFIG_DIR");
    }

    /// session_manifest_path returns None when workspaces exist but session UUID
    /// doesn't match any.
    #[test]
    fn session_manifest_path_no_matching_uuid() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let ws_dir = dir
            .path()
            .join("workspaces")
            .join("ws-1")
            .join("sessions")
            .join("other-uuid");
        std::fs::create_dir_all(&ws_dir).unwrap();
        std::fs::write(ws_dir.join("manifest.json"), "{}").unwrap();

        std::env::set_var("BOTSTER_CONFIG_DIR", dir.path());
        let result = session_manifest_path("sess-not-here");
        assert!(result.is_none(), "non-matching UUID must return None");
        std::env::remove_var("BOTSTER_CONFIG_DIR");
    }

    /// session_manifest_path finds the correct manifest across multiple workspaces.
    #[test]
    fn session_manifest_path_finds_across_workspaces() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let uuid = "sess-target-uuid";

        let decoy = dir
            .path()
            .join("workspaces")
            .join("ws-1")
            .join("sessions")
            .join("other");
        std::fs::create_dir_all(&decoy).unwrap();
        std::fs::write(decoy.join("manifest.json"), "{}").unwrap();

        let target = dir
            .path()
            .join("workspaces")
            .join("ws-2")
            .join("sessions")
            .join(uuid);
        std::fs::create_dir_all(&target).unwrap();
        std::fs::write(
            target.join("manifest.json"),
            r#"{"uuid":"sess-target-uuid"}"#,
        )
        .unwrap();

        std::env::set_var("BOTSTER_CONFIG_DIR", dir.path());
        let result = session_manifest_path(uuid);
        assert!(result.is_some(), "should find manifest in ws-2");
        assert!(
            result.unwrap().to_string_lossy().contains("ws-2"),
            "should resolve to ws-2"
        );
        std::env::remove_var("BOTSTER_CONFIG_DIR");
    }

    /// session_manifest_path returns None when the session dir exists but
    /// manifest.json is missing (stale/incomplete session directory).
    #[test]
    fn session_manifest_path_missing_manifest_file() {
        let _lock = ENV_LOCK.lock().unwrap();
        let dir = tempfile::TempDir::new().unwrap();
        let uuid = "sess-no-manifest";
        let sess_dir = dir
            .path()
            .join("workspaces")
            .join("ws-1")
            .join("sessions")
            .join(uuid);
        std::fs::create_dir_all(&sess_dir).unwrap();

        std::env::set_var("BOTSTER_CONFIG_DIR", dir.path());
        let result = session_manifest_path(uuid);
        assert!(result.is_none(), "missing manifest.json must return None");
        std::env::remove_var("BOTSTER_CONFIG_DIR");
    }
}
