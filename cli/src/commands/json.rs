//! JSON file manipulation commands.
//!
//! Provides CLI utilities for reading, modifying, and deleting values in JSON
//! files using dot-notation paths. Useful for managing configuration files
//! like tool settings or plugin state files.
//!
//! # Path Notation
//!
//! Keys are specified using dot notation: `projects.myproject.hasTrust`.
//! For `json-set`, numeric path segments navigate JSON arrays. Setting index
//! equal to the array length appends a new element; setting beyond the next
//! append position fails instead of filling gaps with nulls.
//!
//! # Examples
//!
//! ```bash
//! # Get a value
//! botster json-get ~/.config/example/settings.json "projects.myproject.hasTrust"
//!
//! # Set a value (creates intermediate objects if needed)
//! botster json-set ~/.config/example/settings.json "projects.myproject.hasTrust" "true"
//!
//! # Set or append inside an array
//! botster json-set ~/.config/botster/spawn_targets.json "targets.0.plugins.4" '"project-pipelines"'
//!
//! # Delete a key
//! botster json-delete ~/.config/example/settings.json "projects.myproject.hasTrust"
//! ```

use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

fn empty_container_for(next_key: &str) -> serde_json::Value {
    if next_key.parse::<usize>().is_ok() {
        serde_json::Value::Array(Vec::new())
    } else {
        serde_json::json!({})
    }
}

fn set_at_path(
    current: &mut serde_json::Value,
    keys: &[&str],
    new_value: serde_json::Value,
) -> Result<()> {
    let key = keys[0];
    let is_last = keys.len() == 1;

    match current {
        serde_json::Value::Object(obj) => {
            if is_last {
                obj.insert(key.to_string(), new_value);
                return Ok(());
            }

            let child = obj
                .entry(key.to_string())
                .or_insert_with(|| empty_container_for(keys[1]));

            if !child.is_object() && !child.is_array() {
                *child = empty_container_for(keys[1]);
            }

            set_at_path(child, &keys[1..], new_value)
        }
        serde_json::Value::Array(items) => {
            let index = key
                .parse::<usize>()
                .with_context(|| format!("Cannot navigate array with non-numeric key '{}'", key))?;

            if index > items.len() {
                anyhow::bail!(
                    "Cannot set array index {} - next append position is {}",
                    index,
                    items.len()
                );
            }

            if is_last {
                if index == items.len() {
                    items.push(new_value);
                } else {
                    items[index] = new_value;
                }
                return Ok(());
            }

            if index == items.len() {
                items.push(empty_container_for(keys[1]));
            } else if !items[index].is_object() && !items[index].is_array() {
                items[index] = empty_container_for(keys[1]);
            }

            set_at_path(&mut items[index], &keys[1..], new_value)
        }
        _ => anyhow::bail!("Cannot navigate through '{}' - not an object or array", key),
    }
}

/// Reads a value from a JSON file using dot-notation path.
///
/// Navigates through the JSON structure using the provided key path and prints
/// the resulting value as pretty-printed JSON to stdout.
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be read
/// - The file contains invalid JSON
/// - Any key in the path does not exist
///
/// # Examples
///
/// ```ignore
/// // Read "projects.myproject.hasTrust" from a JSON file
/// json::get("~/.config/example/settings.json", "projects.myproject.hasTrust")?;
/// ```
pub fn get(file_path: &str, key_path: &str) -> Result<()> {
    let path = shellexpand::tilde(file_path);
    let content = fs::read_to_string(Path::new(path.as_ref()))
        .with_context(|| format!("Failed to read {}", file_path))?;

    let mut value: serde_json::Value = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse {} as JSON", file_path))?;

    // Navigate through the key path
    for key in key_path.split('.') {
        value = value
            .get(key)
            .with_context(|| format!("Key '{}' not found in path '{}'", key, key_path))?
            .clone();
    }

    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

/// Sets a value in a JSON file using dot-notation path.
///
/// Navigates to the specified location in the JSON structure and sets the value.
/// Creates intermediate objects if they don't exist. The value is parsed as JSON
/// first; if parsing fails, it's treated as a string.
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be read or written
/// - The file contains invalid JSON
/// - An intermediate key exists but is not an object
///
/// # Examples
///
/// ```ignore
/// // Set a boolean value
/// json::set("config.json", "settings.enabled", "true")?;
///
/// // Set an object value
/// json::set("config.json", "settings.options", r#"{"key": "value"}"#)?;
/// ```
pub fn set(file_path: &str, key_path: &str, new_value: &str) -> Result<()> {
    let path = shellexpand::tilde(file_path);
    let content = fs::read_to_string(Path::new(path.as_ref()))
        .with_context(|| format!("Failed to read {}", file_path))?;

    let mut root: serde_json::Value = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse {} as JSON", file_path))?;

    // Parse the new value as JSON, fall back to string if parsing fails
    let parsed_value: serde_json::Value = serde_json::from_str(new_value)
        .unwrap_or_else(|_| serde_json::Value::String(new_value.to_string()));

    // Split the path and navigate/create structure
    let keys: Vec<&str> = key_path.split('.').collect();
    if keys.is_empty() || (keys.len() == 1 && keys[0].is_empty()) {
        anyhow::bail!("Cannot set root object");
    }
    set_at_path(&mut root, &keys, parsed_value)?;

    // Write back to file with pretty formatting
    fs::write(
        Path::new(path.as_ref()),
        serde_json::to_string_pretty(&root)?,
    )
    .with_context(|| format!("Failed to write {}", file_path))?;

    Ok(())
}

/// Deletes a key from a JSON file using dot-notation path.
///
/// Navigates to the parent of the specified key and removes it. If any
/// intermediate key doesn't exist, the operation succeeds silently (idempotent).
///
/// # Errors
///
/// Returns an error if:
/// - The file cannot be read or written
/// - The file contains invalid JSON
/// - Attempting to delete the root object
/// - An intermediate key is not an object
///
/// # Examples
///
/// ```ignore
/// // Delete a nested key
/// json::delete("config.json", "settings.deprecated_option")?;
/// ```
pub fn delete(file_path: &str, key_path: &str) -> Result<()> {
    let path = shellexpand::tilde(file_path);
    let content = fs::read_to_string(Path::new(path.as_ref()))
        .with_context(|| format!("Failed to read {}", file_path))?;

    let mut root: serde_json::Value = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse {} as JSON", file_path))?;

    // Split the path and navigate to parent
    let keys: Vec<&str> = key_path.split('.').collect();
    // Empty path or single empty key both indicate root deletion attempt
    if keys.is_empty() || (keys.len() == 1 && keys[0].is_empty()) {
        anyhow::bail!("Cannot delete root object");
    }

    let mut current = &mut root;

    // Navigate to the parent of the key we want to delete
    for (i, key) in keys.iter().enumerate() {
        if i == keys.len() - 1 {
            // Last key - delete it
            if let Some(obj) = current.as_object_mut() {
                obj.remove(*key);
            } else {
                anyhow::bail!("Cannot delete key '{}' - parent is not an object", key);
            }
        } else {
            // Navigate to next level
            if !current.is_object() {
                anyhow::bail!("Cannot navigate through '{}' - not an object", key);
            }

            let obj = current.as_object_mut().expect("checked is_object() above");
            if !obj.contains_key(*key) {
                // Key doesn't exist, nothing to delete (idempotent)
                return Ok(());
            }

            current = obj.get_mut(*key).expect("checked contains_key() above");
        }
    }

    // Write back to file with pretty formatting
    fs::write(
        Path::new(path.as_ref()),
        serde_json::to_string_pretty(&root)?,
    )
    .with_context(|| format!("Failed to write {}", file_path))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", content).unwrap();
        file
    }

    #[test]
    fn test_get_simple_key() {
        let file = create_test_file(r#"{"name": "test", "value": 42}"#);
        let path = file.path().to_str().unwrap();

        // Should succeed without panicking
        let result = get(path, "name");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_nested_key() {
        let file = create_test_file(r#"{"outer": {"inner": {"deep": "found"}}}"#);
        let path = file.path().to_str().unwrap();

        let result = get(path, "outer.inner.deep");
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_missing_key() {
        let file = create_test_file(r#"{"name": "test"}"#);
        let path = file.path().to_str().unwrap();

        let result = get(path, "missing");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_set_simple_value() {
        let file = create_test_file(r#"{"name": "old"}"#);
        let path = file.path().to_str().unwrap();

        set(path, "name", "\"new\"").unwrap();

        let content = fs::read_to_string(path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["name"], "new");
    }

    #[test]
    fn test_set_creates_intermediate_objects() {
        let file = create_test_file(r#"{}"#);
        let path = file.path().to_str().unwrap();

        set(path, "a.b.c", "\"deep\"").unwrap();

        let content = fs::read_to_string(path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["a"]["b"]["c"], "deep");
    }

    #[test]
    fn test_set_array_index_preserves_existing_array() {
        let file = create_test_file(
            r#"{"targets":[{"name":"one","plugins":["github","telegram","mcp","vault"]}]}"#,
        );
        let path = file.path().to_str().unwrap();

        set(path, "targets.0.plugins.4", "\"project-pipelines\"").unwrap();

        let content = fs::read_to_string(path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed["targets"].is_array());
        assert!(parsed["targets"][0]["plugins"].is_array());
        assert_eq!(parsed["targets"][0]["plugins"][4], "project-pipelines");
    }

    #[test]
    fn test_set_creates_array_for_missing_numeric_path() {
        let file = create_test_file(r#"{}"#);
        let path = file.path().to_str().unwrap();

        set(path, "targets.0.plugins.0", "\"project-pipelines\"").unwrap();

        let content = fs::read_to_string(path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed["targets"].is_array());
        assert!(parsed["targets"][0]["plugins"].is_array());
        assert_eq!(parsed["targets"][0]["plugins"][0], "project-pipelines");
    }

    #[test]
    fn test_set_array_index_beyond_append_position_fails() {
        let file = create_test_file(r#"{"plugins":["github"]}"#);
        let path = file.path().to_str().unwrap();

        let result = set(path, "plugins.2", "\"project-pipelines\"");

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("next append position is 1"));
    }

    #[test]
    fn test_set_boolean_value() {
        let file = create_test_file(r#"{"enabled": false}"#);
        let path = file.path().to_str().unwrap();

        set(path, "enabled", "true").unwrap();

        let content = fs::read_to_string(path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed["enabled"], true);
    }

    #[test]
    fn test_delete_key() {
        let file = create_test_file(r#"{"keep": 1, "remove": 2}"#);
        let path = file.path().to_str().unwrap();

        delete(path, "remove").unwrap();

        let content = fs::read_to_string(path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed.get("keep").is_some());
        assert!(parsed.get("remove").is_none());
    }

    #[test]
    fn test_delete_nested_key() {
        let file = create_test_file(r#"{"outer": {"keep": 1, "remove": 2}}"#);
        let path = file.path().to_str().unwrap();

        delete(path, "outer.remove").unwrap();

        let content = fs::read_to_string(path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed["outer"].get("keep").is_some());
        assert!(parsed["outer"].get("remove").is_none());
    }

    #[test]
    fn test_delete_missing_key_is_idempotent() {
        let file = create_test_file(r#"{"name": "test"}"#);
        let path = file.path().to_str().unwrap();

        // Should succeed even though key doesn't exist
        let result = delete(path, "nonexistent");
        assert!(result.is_ok());
    }

    #[test]
    fn test_delete_root_fails() {
        let file = create_test_file(r#"{"name": "test"}"#);
        let path = file.path().to_str().unwrap();

        let result = delete(path, "");
        assert!(result.is_err());
    }
}
