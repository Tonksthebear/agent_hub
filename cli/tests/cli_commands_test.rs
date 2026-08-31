//! Integration tests for CLI commands.
//!
//! Run with: `cargo test --test cli_commands_test`

use std::process::Command;
use tempfile::TempDir;

/// Test the json-get command
#[test]
fn test_json_get_command() {
    let temp_dir = TempDir::new().unwrap();
    let json_file = temp_dir.path().join("test.json");

    // Create a test JSON file
    std::fs::write(&json_file, r#"{"name": "botster", "version": "1.0"}"#).unwrap();

    let output = Command::new("cargo")
        .args(&[
            "run",
            "--quiet",
            "--",
            "json-get",
            json_file.to_str().unwrap(),
            "name",
        ])
        .output()
        .expect("Failed to execute json-get command");

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("botster"), "Should return the name value");
}

/// Test the json-set command
#[test]
fn test_json_set_command() {
    let temp_dir = TempDir::new().unwrap();
    let json_file = temp_dir.path().join("test.json");

    // Create initial JSON file
    std::fs::write(&json_file, r#"{}"#).unwrap();

    // Set a value
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--quiet",
            "--",
            "json-set",
            json_file.to_str().unwrap(),
            "test.key",
            "\"test_value\"",
        ])
        .output()
        .expect("Failed to execute json-set command");

    assert!(output.status.success(), "Command should succeed");

    // Read back and verify
    let contents = std::fs::read_to_string(&json_file).unwrap();
    assert!(
        contents.contains("test_value"),
        "File should contain the set value"
    );
}

#[test]
fn test_json_set_array_path_command() {
    let temp_dir = TempDir::new().unwrap();
    let json_file = temp_dir.path().join("spawn_targets.json");

    std::fs::write(
        &json_file,
        r#"{"targets":[{"name":"Project","plugins":["github","telegram","mcp","vault"]}]}"#,
    )
    .unwrap();

    let output = Command::new("cargo")
        .args(&[
            "run",
            "--quiet",
            "--",
            "json-set",
            json_file.to_str().unwrap(),
            "targets.0.plugins.4",
            "\"project-pipelines\"",
        ])
        .output()
        .expect("Failed to execute json-set command");

    assert!(output.status.success(), "Command should succeed");

    let contents = std::fs::read_to_string(&json_file).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&contents).unwrap();
    assert!(parsed["targets"].is_array());
    assert!(parsed["targets"][0]["plugins"].is_array());
    assert_eq!(parsed["targets"][0]["plugins"][4], "project-pipelines");
}

/// Test the json-delete command
#[test]
fn test_json_delete_command() {
    let temp_dir = TempDir::new().unwrap();
    let json_file = temp_dir.path().join("test.json");

    // Create JSON file with a key to delete
    std::fs::write(&json_file, r#"{"key_to_delete": "value", "keep": "this"}"#).unwrap();

    // Delete the key
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--quiet",
            "--",
            "json-delete",
            json_file.to_str().unwrap(),
            "key_to_delete",
        ])
        .output()
        .expect("Failed to execute json-delete command");

    assert!(output.status.success(), "Command should succeed");

    // Read back and verify
    let contents = std::fs::read_to_string(&json_file).unwrap();
    assert!(!contents.contains("key_to_delete"), "Key should be deleted");
    assert!(contents.contains("keep"), "Other keys should remain");
}

/// Test nested JSON operations
#[test]
fn test_json_nested_operations() {
    let temp_dir = TempDir::new().unwrap();
    let json_file = temp_dir.path().join("test.json");

    // Create nested JSON structure
    std::fs::write(&json_file, r#"{"projects": {}}"#).unwrap();

    // Set a nested value
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--quiet",
            "--",
            "json-set",
            json_file.to_str().unwrap(),
            "projects.myproject.hasTrust",
            "true",
        ])
        .output()
        .expect("Failed to execute json-set command");

    assert!(output.status.success(), "Command should succeed");

    // Get the nested value
    let output = Command::new("cargo")
        .args(&[
            "run",
            "--quiet",
            "--",
            "json-get",
            json_file.to_str().unwrap(),
            "projects.myproject.hasTrust",
        ])
        .output()
        .expect("Failed to execute json-get command");

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("true"), "Should return the nested value");
}

/// Test config command without arguments (list all)
#[test]
fn test_config_list() {
    let output = Command::new("cargo")
        .args(&["run", "--quiet", "--", "config"])
        .output()
        .expect("Failed to execute config command");

    // Should succeed even if no config exists
    assert!(output.status.success() || !output.stderr.is_empty());
}

/// Test status command
#[test]
fn test_status_command() {
    let output = Command::new("cargo")
        .args(&["run", "--quiet", "--", "status"])
        .output()
        .expect("Failed to execute status command");

    // Status should always work (even if no agents running)
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Hub status"));
    assert!(stdout.contains("socket:"));
}

/// Test list-worktrees command
#[test]
fn test_list_worktrees_command() {
    let output = Command::new("cargo")
        .args(&["run", "--quiet", "--", "list-worktrees"])
        .output()
        .expect("Failed to execute list-worktrees command");

    // Should succeed (lists worktrees in current repo)
    assert!(output.status.success());
}

/// Test get-prompt command with invalid path
#[test]
fn test_get_prompt_with_invalid_path() {
    let output = Command::new("cargo")
        .args(&["run", "--quiet", "--", "get-prompt", "/nonexistent/path"])
        .output()
        .expect("Failed to execute get-prompt command");

    // The command might succeed with a default prompt or fail - either is acceptable
    // Just verify it runs without crashing
    let _status = output.status;
    // If it fails, stderr should have an error message
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!stderr.is_empty(), "Should have error message on failure");
    }
}

/// Test update command with --check flag
/// Ignored by default: requires network access to GitHub API
#[test]
#[ignore]
fn test_update_check() {
    let output = Command::new("cargo")
        .args(&["run", "--quiet", "--", "update", "--check"])
        .output()
        .expect("Failed to execute update --check command");

    // Should succeed and show version info
    assert!(output.status.success());
}
