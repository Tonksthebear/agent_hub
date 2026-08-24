//! Tests for the per-session process architecture.
//!
//! Covers: protocol encode/decode, session-backed PtyHandle paths,
//! hub manifest serialization, and socket path formatting.

#[cfg(test)]
mod protocol_tests {
    use crate::session::protocol::*;
    use crate::session::SpawnConfig;

    #[test]
    fn frame_roundtrip() {
        let data = b"hello world";
        let encoded = encode_frame(FRAME_PTY_OUTPUT, data);
        let mut decoder = FrameDecoder::new();
        let frames = decoder.feed(&encoded);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame_type, FRAME_PTY_OUTPUT);
        assert_eq!(frames[0].payload, data);
    }

    #[test]
    fn empty_frame_roundtrip() {
        let encoded = encode_empty(FRAME_PING);
        let mut decoder = FrameDecoder::new();
        let frames = decoder.feed(&encoded);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].frame_type, FRAME_PING);
        assert!(frames[0].payload.is_empty());
    }

    #[test]
    fn json_frame_roundtrip() {
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct Resize {
            rows: u16,
            cols: u16,
        }

        let resize = Resize { rows: 24, cols: 80 };
        let encoded = encode_json(FRAME_RESIZE, &resize).unwrap();
        let mut decoder = FrameDecoder::new();
        let frames = decoder.feed(&encoded);
        assert_eq!(frames.len(), 1);
        let decoded: Resize = frames[0].json().unwrap();
        assert_eq!(decoded, resize);
    }

    #[test]
    fn partial_frame_buffering() {
        let encoded = encode_frame(FRAME_PTY_INPUT, b"test");
        let mut decoder = FrameDecoder::new();

        // Feed first 3 bytes (incomplete header)
        let frames = decoder.feed(&encoded[..3]);
        assert!(frames.is_empty());

        // Feed rest
        let frames = decoder.feed(&encoded[3..]);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload, b"test");
    }

    #[test]
    fn multiple_frames_in_one_feed() {
        let mut data = Vec::new();
        data.extend_from_slice(&encode_frame(FRAME_PTY_OUTPUT, b"one"));
        data.extend_from_slice(&encode_frame(FRAME_PTY_OUTPUT, b"two"));
        data.extend_from_slice(&encode_empty(FRAME_PONG));

        let mut decoder = FrameDecoder::new();
        let frames = decoder.feed(&data);
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].payload, b"one");
        assert_eq!(frames[1].payload, b"two");
        assert!(frames[2].payload.is_empty());
    }

    #[test]
    fn mode_flags_serialization() {
        let flags = ModeFlags {
            kitty_enabled: true,
            cursor_visible: false,
            bracketed_paste: true,
            mouse_mode: 3,
            alt_screen: true,
            focus_reporting: true,
            application_cursor: false,
        };
        let encoded = encode_json(FRAME_MODE_FLAGS, &flags).unwrap();
        let mut decoder = FrameDecoder::new();
        let frames = decoder.feed(&encoded);
        let decoded: ModeFlags = frames[0].json().unwrap();
        assert!(decoded.kitty_enabled);
        assert!(!decoded.cursor_visible);
        assert!(decoded.bracketed_paste);
        assert_eq!(decoded.mouse_mode, 3);
        assert!(decoded.alt_screen);
        assert!(decoded.focus_reporting);
        assert!(!decoded.application_cursor);
    }

    #[test]
    fn session_metadata_serialization() {
        let meta = SessionMetadata {
            session_uuid: "sess-test-123".to_string(),
            pid: 42,
            rows: 24,
            cols: 80,
            last_output_at: 1234567890,
            title: Some("working".to_string()),
            cwd: Some("/tmp/project".to_string()),
            port: Some(4321),
            mode_flags: ModeFlags {
                kitty_enabled: true,
                cursor_visible: false,
                bracketed_paste: true,
                mouse_mode: 3,
                alt_screen: true,
                focus_reporting: true,
                application_cursor: false,
            },
            recovery_identity: Some(serde_json::json!({
                "session_type": "agent",
                "workspace_id": "ws-test",
            })),
        };
        let json = serde_json::to_vec(&meta).unwrap();
        let decoded: SessionMetadata = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.session_uuid, "sess-test-123");
        assert_eq!(decoded.pid, 42);
        assert_eq!(decoded.rows, 24);
        assert_eq!(decoded.cols, 80);
        assert_eq!(decoded.last_output_at, 1234567890);
        assert_eq!(decoded.title.as_deref(), Some("working"));
        assert_eq!(decoded.cwd.as_deref(), Some("/tmp/project"));
        assert_eq!(decoded.port, Some(4321));
        assert!(decoded.mode_flags.kitty_enabled);
        assert!(!decoded.mode_flags.cursor_visible);
        assert!(decoded.mode_flags.bracketed_paste);
        assert_eq!(decoded.mode_flags.mouse_mode, 3);
        assert!(decoded.mode_flags.alt_screen);
        assert!(decoded.mode_flags.focus_reporting);
        assert!(!decoded.mode_flags.application_cursor);
        let identity = decoded.recovery_identity.expect("recovery identity");
        assert_eq!(identity["session_type"], "agent");
        assert_eq!(identity["workspace_id"], "ws-test");
    }

    #[test]
    fn legacy_session_metadata_deserializes_without_recovery_identity() {
        let json = serde_json::json!({
            "session_uuid": "sess-legacy",
            "pid": 123,
            "rows": 24,
            "cols": 80,
            "last_output_at": 0,
            "title": null,
            "cwd": null,
            "port": null,
            "mode_flags": {
                "kitty_enabled": false,
                "cursor_visible": true,
                "bracketed_paste": false,
                "mouse_mode": 0,
                "alt_screen": false,
                "focus_reporting": false,
                "application_cursor": false,
            },
        });

        let decoded: SessionMetadata = serde_json::from_value(json).unwrap();

        assert_eq!(decoded.session_uuid, "sess-legacy");
        assert!(decoded.recovery_identity.is_none());
    }

    #[test]
    fn legacy_spawn_config_deserializes_without_recovery_identity() {
        let json = serde_json::json!({
            "command": "bash",
            "args": [],
            "env": [],
            "cwd": null,
            "rows": 24,
            "cols": 80,
            "tee_path": null,
            "tee_cap": 0,
        });

        let decoded: SpawnConfig = serde_json::from_value(json).unwrap();

        assert_eq!(decoded.command, "bash");
        assert!(decoded.recovery_identity.is_none());
    }

    #[test]
    fn recovery_identity_metadata_stays_well_below_handshake_cap() {
        let meta = SessionMetadata {
            session_uuid: "sess-size-check".to_string(),
            pid: std::process::id(),
            rows: 24,
            cols: 80,
            last_output_at: 0,
            title: None,
            cwd: None,
            port: None,
            mode_flags: ModeFlags::default(),
            recovery_identity: Some(serde_json::json!({
                "schema_version": 1,
                "session_uuid": "sess-size-check",
                "session_type": "agent",
                "session_name": "codex",
                "repo": "/repo",
                "target_id": "target",
                "target_path": "/repo",
                "target_repo": "/repo",
                "branch_name": "feature/session-recovery",
                "worktree_path": "/repo",
                "workspace_id": "ws",
                "workspace_name": "Workspace",
                "agent_name": "codex",
                "owner_plugin": "project_pipelines",
                "visibility": "workspace",
                "surface": "main",
                "label": "agent",
                "in_worktree": true,
                "created_at": 1779120000,
            })),
        };

        let json = serde_json::to_vec(&meta).unwrap();

        assert!(
            json.len() < 4096,
            "recovery identity should remain tiny relative to the 64KB handshake cap"
        );
    }
}

#[cfg(test)]
mod session_frame_tests {
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::session::protocol::{encode_json, FRAME_RESIZE};
    use crate::session::{handle_hub_frame, PtyWriteCommand};
    use crate::terminal::TerminalParser;

    #[test]
    fn resize_frame_updates_parser_before_writer_thread_runs() {
        let parser = Arc::new(Mutex::new(TerminalParser::new(24, 80, 100)));
        let current_dims = Arc::new(Mutex::new((24, 80)));
        let resize_pending = AtomicBool::new(false);
        let (writer_tx, writer_rx) = std::sync::mpsc::sync_channel(8);
        let (mut stream, mut peer) = UnixStream::pair().expect("socket pair");
        let tee = Arc::new(Mutex::new(None));
        let frame = encode_json(
            FRAME_RESIZE,
            &serde_json::json!({
                "rows": 37,
                "cols": 132,
            }),
        )
        .expect("resize frame");

        let decoded = {
            let mut decoder = crate::session::protocol::FrameDecoder::new();
            decoder
                .feed(&frame)
                .into_iter()
                .next()
                .expect("decoded frame")
        };

        handle_hub_frame(
            "sess-test-color-profile",
            &decoded,
            &writer_tx,
            &parser,
            &current_dims,
            &resize_pending,
            &tee,
            &mut stream,
            &AtomicBool::new(false),
        );

        {
            let guard = parser.lock().expect("parser lock");
            assert_eq!(guard.terminal().rows(), 37);
            assert_eq!(guard.terminal().cols(), 132);
        }
        assert!(matches!(
            writer_rx.try_recv(),
            Ok(PtyWriteCommand::Resize {
                rows: 37,
                cols: 132
            })
        ));

        peer.set_read_timeout(Some(Duration::from_millis(10)))
            .expect("timeout");
        let mut buf = [0u8; 1];
        assert!(
            peer.read(&mut buf).is_err(),
            "resize should not write a reply"
        );
    }

    #[test]
    fn duplicate_resize_frame_uses_applied_dims_before_skipping() {
        let parser = Arc::new(Mutex::new(TerminalParser::new(37, 132, 100)));
        let current_dims = Arc::new(Mutex::new((24, 80)));
        let resize_pending = AtomicBool::new(false);
        let (writer_tx, writer_rx) = std::sync::mpsc::sync_channel(8);
        let (mut stream, _peer) = UnixStream::pair().expect("socket pair");
        let tee = Arc::new(Mutex::new(None));
        let frame = encode_json(
            FRAME_RESIZE,
            &serde_json::json!({
                "rows": 37,
                "cols": 132,
            }),
        )
        .expect("resize frame");

        let decoded = {
            let mut decoder = crate::session::protocol::FrameDecoder::new();
            decoder
                .feed(&frame)
                .into_iter()
                .next()
                .expect("decoded frame")
        };

        handle_hub_frame(
            "sess-test-resize",
            &decoded,
            &writer_tx,
            &parser,
            &current_dims,
            &resize_pending,
            &tee,
            &mut stream,
            &AtomicBool::new(false),
        );

        assert!(matches!(
            writer_rx.try_recv(),
            Ok(PtyWriteCommand::Resize {
                rows: 37,
                cols: 132
            })
        ));

        *current_dims.lock().expect("dims") = (37, 132);
        handle_hub_frame(
            "sess-test-resize",
            &decoded,
            &writer_tx,
            &parser,
            &current_dims,
            &resize_pending,
            &tee,
            &mut stream,
            &AtomicBool::new(false),
        );

        assert!(
            writer_rx.try_recv().is_err(),
            "applied duplicate resize should be skipped"
        );
    }
}

#[cfg(test)]
mod pty_handle_tests {
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::{Arc, Mutex};

    use tokio::sync::broadcast;

    use crate::hub::agent_handle::PtyHandle;

    /// Create a session-backed PtyHandle for testing.
    ///
    /// Snapshots return empty since there's no session process to RPC to.
    fn create_session_backed_pty(rows: u16, cols: u16) -> PtyHandle {
        let (event_tx, _rx) = broadcast::channel(64);
        let kitty_enabled = Arc::new(AtomicBool::new(false));
        let cursor_visible = Arc::new(AtomicBool::new(true));
        let resize_pending = Arc::new(AtomicBool::new(false));
        let session_connection = Arc::new(Mutex::new(None));

        PtyHandle::new_with_session(
            event_tx,
            kitty_enabled,
            cursor_visible,
            resize_pending,
            None,
            session_connection,
            Arc::new(AtomicU64::new(0)),
            Arc::new(std::sync::atomic::AtomicI64::new(0)),
            rows,
            cols,
        )
    }

    #[test]
    fn session_backed_handle_is_session_backed() {
        let handle = create_session_backed_pty(24, 80);
        assert!(handle.is_session_backed());
    }

    #[test]
    fn session_backed_handle_preserves_initial_dimensions() {
        let handle = create_session_backed_pty(59, 201);
        assert_eq!(handle.dims(), (59, 201));
    }

    #[test]
    fn session_backed_snapshot_returns_empty_without_session() {
        let handle = create_session_backed_pty(24, 80);
        let snapshot = handle.get_snapshot();
        assert!(
            snapshot.is_empty(),
            "session-backed handle without session should return empty snapshot"
        );
    }

    #[test]
    fn session_backed_resize_without_connection() {
        let handle = create_session_backed_pty(24, 80);
        // Resize with no session connection should not panic
        handle.resize_direct(30, 120);
        // Shared dimensions are updated even without a session connection
        // (the session RPC fails silently, but dims track the requested size)
        assert_eq!(handle.dims(), (30, 120));
    }
}

#[cfg(test)]
mod hub_manifest_tests {
    use crate::hub::daemon::HubManifest;

    #[test]
    fn manifest_workspaces_default_empty() {
        let json = r#"{
            "hub_id": "test",
            "socket_path": "/tmp/test.sock",
            "pid": 1234,
            "updated_at": 0
        }"#;
        let manifest: HubManifest = serde_json::from_str(json).unwrap();
        assert!(
            manifest.workspaces.is_empty(),
            "workspaces should default to empty"
        );
    }

    #[test]
    fn manifest_workspaces_roundtrip() {
        let manifest = HubManifest {
            hub_id: "test".to_string(),
            server_id: None,
            socket_path: "/tmp/test.sock".to_string(),
            pid: 1234,
            updated_at: 0,
            workspaces: vec!["ws-1".to_string(), "ws-2".to_string()],
            mcp_url: None,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        let decoded: HubManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.workspaces, vec!["ws-1", "ws-2"]);
    }

    #[test]
    fn manifest_without_workspaces_field_parses() {
        // Old manifests from before the workspaces field was added
        let json = r#"{
            "hub_id": "test",
            "socket_path": "/tmp/test.sock",
            "pid": 1234,
            "updated_at": 0
        }"#;
        let manifest: HubManifest = serde_json::from_str(json).unwrap();
        assert!(manifest.workspaces.is_empty());
    }

    #[test]
    fn manifest_empty_workspaces_not_serialized() {
        let manifest = HubManifest {
            hub_id: "test".to_string(),
            server_id: None,
            socket_path: "/tmp/test.sock".to_string(),
            pid: 1234,
            updated_at: 0,
            workspaces: Vec::new(),
            mcp_url: None,
        };
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(
            !json.contains("workspaces"),
            "empty workspaces should be skipped in serialization"
        );
        assert!(
            !json.contains("mcp_url"),
            "absent MCP URL should be skipped in serialization"
        );
    }
}

#[cfg(test)]
mod socket_path_tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::session::{
        cleanup_orphaned_session_files, read_session_pid_file, read_session_recovery_identity, run,
        session_pid_path, session_process_is_live, session_recovery_identity_path,
        session_socket_path, sessions_socket_dir, write_session_pid_file,
        write_session_recovery_identity,
    };

    fn unique_session_uuid(suffix: &str) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("sess-test-{}-{}-{ts}", std::process::id(), suffix)
    }

    #[test]
    fn session_socket_path_format() {
        let path = session_socket_path("sess-1234-abcd").unwrap();
        let filename = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(filename, "sess-1234-abcd.sock");
    }

    #[test]
    fn session_pid_path_format() {
        let path = session_pid_path("sess-1234-abcd").unwrap();
        let filename = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(filename, "sess-1234-abcd.pid");
    }

    #[test]
    fn sessions_socket_dir_exists() {
        let dir = sessions_socket_dir().unwrap();
        assert!(dir.exists(), "sessions socket dir should be created");
        assert!(
            dir.to_str().unwrap().contains("sessions"),
            "path should contain 'sessions'"
        );
    }

    #[test]
    fn session_process_is_live_requires_socket_and_live_pid() {
        let session_uuid = unique_session_uuid("live");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);

        std::fs::write(&socket_path, b"").unwrap();
        assert!(
            !session_process_is_live(&session_uuid),
            "socket alone should not count as a live session"
        );

        write_session_pid_file(&session_uuid, std::process::id()).unwrap();
        assert!(
            session_process_is_live(&session_uuid),
            "socket plus live pid should count as a live session"
        );
        assert_eq!(
            read_session_pid_file(&session_uuid).unwrap(),
            Some(std::process::id())
        );

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
    }

    #[test]
    fn cleanup_orphaned_session_files_preserves_socket_without_pid_file() {
        let session_uuid = unique_session_uuid("missing-pid");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);

        std::fs::write(&socket_path, b"").unwrap();
        cleanup_orphaned_session_files();

        assert!(
            socket_path.exists(),
            "cleanup should not remove a socket when pid metadata is missing"
        );

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
    }

    #[test]
    fn session_start_refuses_to_replace_live_socket() {
        let session_uuid = unique_session_uuid("duplicate-live");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);

        std::fs::write(&socket_path, b"placeholder").unwrap();
        write_session_pid_file(&session_uuid, std::process::id()).unwrap();

        let err = run(&session_uuid, socket_path.to_str().unwrap(), 0)
            .expect_err("startup must refuse to replace a live session socket");
        assert!(
            err.to_string()
                .contains("refusing to replace live session socket"),
            "unexpected error: {err:#}"
        );
        assert!(
            socket_path.exists(),
            "duplicate startup must not unlink the live session socket"
        );
        assert!(
            pid_path.exists(),
            "duplicate startup must not unlink the live session pid metadata"
        );

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
    }

    #[test]
    fn cleanup_orphaned_session_files_removes_socket_with_dead_identity() {
        let session_uuid = unique_session_uuid("dead-identity");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);

        std::fs::write(&socket_path, b"placeholder").unwrap();
        std::fs::write(
            &pid_path,
            format!(r#"{{"pid":{},"pgid":0,"sid":0}}"#, std::process::id()),
        )
        .unwrap();

        cleanup_orphaned_session_files();

        assert!(
            !socket_path.exists(),
            "dead identity cleanup should remove stale socket"
        );
        assert!(
            !pid_path.exists(),
            "dead identity cleanup should remove stale pid metadata"
        );
    }

    #[test]
    fn session_pid_file_is_json_identity_record() {
        let session_uuid = unique_session_uuid("identity");
        let pid_path = session_pid_path(&session_uuid).unwrap();
        let _ = std::fs::remove_file(&pid_path);

        write_session_pid_file(&session_uuid, std::process::id()).unwrap();
        let content = std::fs::read_to_string(&pid_path).unwrap();

        assert!(
            content.trim_start().starts_with('{'),
            "pid file should now serialize a structured identity record"
        );
        assert_eq!(
            read_session_pid_file(&session_uuid).unwrap(),
            Some(std::process::id())
        );

        let _ = std::fs::remove_file(&pid_path);
    }

    #[test]
    fn legacy_plaintext_session_pid_file_still_reads() {
        let session_uuid = unique_session_uuid("legacy");
        let pid_path = session_pid_path(&session_uuid).unwrap();
        let _ = std::fs::remove_file(&pid_path);

        std::fs::write(&pid_path, format!("{}\n", std::process::id())).unwrap();

        assert_eq!(
            read_session_pid_file(&session_uuid).unwrap(),
            Some(std::process::id())
        );

        let _ = std::fs::remove_file(&pid_path);
    }

    #[test]
    fn session_recovery_identity_roundtrips_as_process_owned_sidecar() {
        let session_uuid = unique_session_uuid("recovery-identity");
        let identity_path = session_recovery_identity_path(&session_uuid).unwrap();
        let _ = std::fs::remove_file(&identity_path);

        let identity = serde_json::json!({
            "schema_version": 1,
            "session_uuid": session_uuid.clone(),
            "session_type": "agent",
            "workspace_id": "ws-1",
        });

        write_session_recovery_identity(&session_uuid, &identity).unwrap();

        assert_eq!(
            read_session_recovery_identity(&session_uuid).unwrap(),
            Some(identity)
        );

        let _ = std::fs::remove_file(&identity_path);
    }

    #[test]
    fn cleanup_orphaned_session_files_removes_identity_sidecar_for_dead_session() {
        let session_uuid = unique_session_uuid("dead-recovery-identity");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();
        let identity_path = session_recovery_identity_path(&session_uuid).unwrap();

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
        let _ = std::fs::remove_file(&identity_path);

        write_session_recovery_identity(
            &session_uuid,
            &serde_json::json!({
                "schema_version": 1,
                "session_uuid": session_uuid.clone(),
            }),
        )
        .unwrap();

        cleanup_orphaned_session_files();

        assert!(
            !identity_path.exists(),
            "cleanup should remove sidecar identity when no live session socket remains"
        );
    }

    #[test]
    fn cleanup_orphaned_session_files_preserves_sidecar_for_live_session() {
        let session_uuid = unique_session_uuid("live-recovery-identity");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();
        let identity_path = session_recovery_identity_path(&session_uuid).unwrap();

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
        let _ = std::fs::remove_file(&identity_path);

        std::fs::write(&socket_path, b"placeholder").unwrap();
        write_session_pid_file(&session_uuid, std::process::id()).unwrap();
        write_session_recovery_identity(
            &session_uuid,
            &serde_json::json!({
                "schema_version": 1,
                "session_uuid": session_uuid,
            }),
        )
        .unwrap();

        cleanup_orphaned_session_files();

        assert!(
            identity_path.exists(),
            "cleanup should preserve sidecar identity while the session is live"
        );

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
        let _ = std::fs::remove_file(&identity_path);
    }

    #[test]
    fn oversized_session_recovery_identity_is_rejected() {
        let session_uuid = unique_session_uuid("oversized-recovery-identity");
        let identity_path = session_recovery_identity_path(&session_uuid).unwrap();
        let _ = std::fs::remove_file(&identity_path);

        std::fs::write(&identity_path, "x".repeat(64 * 1024 + 1)).unwrap();

        let err = read_session_recovery_identity(&session_uuid)
            .expect_err("oversized recovery identity should fail");
        assert!(
            err.to_string().contains("too large"),
            "unexpected error: {err:#}"
        );

        let _ = std::fs::remove_file(&identity_path);
    }

    #[test]
    fn malformed_session_recovery_identity_is_rejected_without_cleanup() {
        let session_uuid = unique_session_uuid("malformed-recovery-identity");
        let identity_path = session_recovery_identity_path(&session_uuid).unwrap();
        let _ = std::fs::remove_file(&identity_path);

        std::fs::write(&identity_path, "{not-json").unwrap();

        let err = read_session_recovery_identity(&session_uuid)
            .expect_err("malformed recovery identity should fail");
        assert!(
            err.to_string().contains("parse session recovery identity"),
            "unexpected error: {err:#}"
        );
        assert!(
            identity_path.exists(),
            "malformed identity should remain for inspection"
        );

        let _ = std::fs::remove_file(&identity_path);
    }

    #[test]
    fn cleanup_orphaned_session_files_removes_orphaned_identity_tmp_file() {
        let session_uuid = unique_session_uuid("tmp-recovery-identity");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();
        let dead_pid = 999_999_999_u32;
        let tmp_path = session_recovery_identity_path(&session_uuid)
            .unwrap()
            .with_file_name(format!("{session_uuid}.identity.json.{dead_pid}.tmp"));

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
        let _ = std::fs::remove_file(&tmp_path);

        std::fs::write(&tmp_path, "{}").unwrap();

        cleanup_orphaned_session_files();

        assert!(
            !tmp_path.exists(),
            "cleanup should remove orphaned temporary sidecar files"
        );
    }

    #[test]
    fn cleanup_orphaned_session_files_preserves_live_writer_identity_tmp_file() {
        let session_uuid = unique_session_uuid("live-tmp-recovery-identity");
        let socket_path = session_socket_path(&session_uuid).unwrap();
        let pid_path = session_pid_path(&session_uuid).unwrap();
        let tmp_path = session_recovery_identity_path(&session_uuid)
            .unwrap()
            .with_file_name(format!(
                "{session_uuid}.identity.json.{}.tmp",
                std::process::id()
            ));

        let _ = std::fs::remove_file(&socket_path);
        let _ = std::fs::remove_file(&pid_path);
        let _ = std::fs::remove_file(&tmp_path);

        std::fs::write(&tmp_path, "{}").unwrap();

        cleanup_orphaned_session_files();

        assert!(
            tmp_path.exists(),
            "cleanup should preserve temporary sidecar files from a live writer"
        );

        let _ = std::fs::remove_file(&tmp_path);
    }
}
