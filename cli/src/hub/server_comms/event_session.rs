use super::*;

impl Hub {
    pub(super) fn handle_session_process_exited_event(
        &mut self,
        session_uuid: String,
        exit_code: Option<i32>,
    ) {
        log::info!(
            "[Session] ProcessExited uuid='{}' exit={:?}",
            session_uuid,
            exit_code
        );

        // Enrich bare socket-EOF deaths with OS wait status when this hub
        // still owns the Child. Also skip soft-reconnect when the session
        // process is already gone (zombie / missing identity).
        let mut exit_code = exit_code;
        let mut signal: Option<i32> = None;
        let progress = crate::session::read_session_progress(&session_uuid);
        // Snapshot VT crash files early — cleanup paths must not race us later.
        let vt_artifacts = crate::session::summarize_vt_crash_artifacts(&session_uuid);

        if exit_code.is_none() {
            if let Some(status) = self.handle_cache.try_reap_session_process(&session_uuid) {
                log::info!(
                    "[Session] Reaped session process after EOF: {}",
                    status.summary()
                );
                signal = status.signal;
                exit_code = status.effective_exit_code();
            } else if !crate::session::session_process_is_live(&session_uuid) {
                // Process identity is dead (or socket gone). Blocking wait is
                // safe for tracked children; recovered sessions yield None.
                if let Some(status) = self.handle_cache.wait_reap_session_process(&session_uuid) {
                    log::info!(
                        "[Session] Wait-reaped dead session process: {}",
                        status.summary()
                    );
                    signal = status.signal;
                    exit_code = status.effective_exit_code();
                } else {
                    log::warn!(
                        "[Session] Session process not live and not tracked for '{}'; permanent exit (exit=None)",
                        &session_uuid[..session_uuid.len().min(16)]
                    );
                }
                if let Some(ref breadcrumb) = progress {
                    log::info!(
                        "[Session] Last session progress breadcrumb for '{}': {}",
                        &session_uuid[..session_uuid.len().min(16)],
                        breadcrumb.trim()
                    );
                }
                Self::log_vt_crash_artifacts(&session_uuid, signal, &vt_artifacts);
                // Permanent death — do not soft-reconnect a corpse.
                self.finalize_session_process_exit(session_uuid, exit_code, signal, progress);
                return;
            } else if let Some(session_handle) = self.handle_cache.get_session(&session_uuid) {
                let pty = session_handle.pty();
                if pty.is_session_backed() {
                    let cleared = pty.clear_session_connection();
                    log::info!(
                        "[Session] Reader died for '{}', process still live, cleared old connection={}, initiating reconnect",
                        &session_uuid[..session_uuid.len().min(16)],
                        cleared
                    );

                    self.reconnect_generation += 1;
                    let generation = self.reconnect_generation;
                    self.pending_reconnects.insert(
                        session_uuid.clone(),
                        crate::hub::ReconnectState {
                            started_at: Instant::now(),
                            attempt_started_at: None,
                            generation,
                            in_flight: false,
                        },
                    );

                    self.hub_event_metrics.record_counter("reconnect.retry", 1);
                    self.spawn_session_reconnect(session_uuid, generation);
                    return;
                }
            }
        } else {
            // Clean FRAME_PROCESS_EXITED path: still reap to clear the watch.
            if let Some(status) = self
                .handle_cache
                .try_reap_session_process(&session_uuid)
                .or_else(|| self.handle_cache.wait_reap_session_process(&session_uuid))
            {
                log::debug!(
                    "[Session] Reaped session process after clean exit frame: {}",
                    status.summary()
                );
                if signal.is_none() {
                    signal = status.signal;
                }
            }
        }

        if let Some(ref breadcrumb) = progress {
            log::info!(
                "[Session] Last session progress breadcrumb for '{}': {}",
                &session_uuid[..session_uuid.len().min(16)],
                breadcrumb.trim()
            );
        }
        Self::log_vt_crash_artifacts(&session_uuid, signal, &vt_artifacts);

        self.finalize_session_process_exit(session_uuid, exit_code, signal, progress);
    }

    /// Log VT crash dump artifacts after a hard session death (SEGV/ABRT/etc.).
    pub(super) fn log_vt_crash_artifacts(
        session_uuid: &str,
        signal: Option<i32>,
        artifacts: &crate::session::VtCrashArtifactSummary,
    ) {
        if !crate::session::vt_crash_dump::signal_warrants_vt_dump(signal) {
            // Still note paths on any death when meta exists — cheap and useful.
            if artifacts.meta_body.is_none() && artifacts.last_chunk_len.is_none() {
                return;
            }
        }
        log::error!(
            "[Session] VT crash artifacts for '{}' signal={:?} last_chunk_len={:?} ring_bytes={:?} \
             vtlast={:?} vtmeta={:?} vtring={:?}",
            &session_uuid[..session_uuid.len().min(16)],
            signal,
            artifacts.last_chunk_len,
            artifacts.ring_bytes,
            artifacts.vtlast_path,
            artifacts.vtmeta_path,
            artifacts.vtring_path
        );
        if let Some(ref hex) = artifacts.last_hex {
            log::error!(
                "[Session] VT last-chunk hex for '{}': {}",
                &session_uuid[..session_uuid.len().min(16)],
                hex
            );
        }
        if let Some(ref meta) = artifacts.meta_body {
            for line in meta.lines() {
                log::error!(
                    "[Session] VT meta '{}' {}",
                    &session_uuid[..session_uuid.len().min(16)],
                    line
                );
            }
        }
    }

    /// Permanent session death: notify PTY subscribers and Lua.
    pub(super) fn finalize_session_process_exit(
        &mut self,
        session_uuid: String,
        exit_code: Option<i32>,
        signal: Option<i32>,
        progress: Option<String>,
    ) {
        self.pending_reconnects.remove(&session_uuid);
        self.cleanup_pending_session_io_snapshots_for_session(&session_uuid);
        self.cleanup_paste_files(&session_uuid);
        if let Some(session_handle) = self.handle_cache.get_session(&session_uuid) {
            session_handle.pty().notify_process_exited(exit_code);
        }

        log::info!(
            "[Session] Permanent process exit uuid='{}' exit={:?} signal={:?}",
            session_uuid,
            exit_code,
            signal
        );

        let mut data = serde_json::json!({
            "session_uuid": session_uuid,
            "exit_code": exit_code,
        });
        if let Some(sig) = signal {
            data["signal"] = serde_json::json!(sig);
        }
        if let Some(progress) = progress {
            data["progress"] = serde_json::json!(progress.trim());
        }

        if let Err(e) = self.lua.fire_json_event("session_process_exited", &data) {
            log::error!("[Session] Failed to fire session_process_exited event: {e}");
        }
    }

    pub(super) fn handle_session_reconnect_ready_event(
        &mut self,
        session_uuid: String,
        generation: u64,
        mut conn: crate::session::connection::SessionConnection,
        metadata: crate::session::protocol::SessionMetadata,
    ) {
        let valid = self
            .pending_reconnects
            .get(&session_uuid)
            .is_some_and(|s| s.generation == generation);

        if !valid {
            log::info!(
                "[Session] Dropping stale reconnect for '{}' gen={}",
                &session_uuid[..session_uuid.len().min(16)],
                generation
            );
            self.hub_event_metrics
                .record_counter("reconnect.stale_generation", 1);
            drop(conn);
            return;
        }

        let Some(session_handle) = self.handle_cache.get_session(&session_uuid) else {
            log::warn!(
                "[Session] Session '{}' disappeared during reconnect",
                &session_uuid[..session_uuid.len().min(16)]
            );
            self.hub_event_metrics.record_counter("reconnect.failed", 1);
            self.pending_reconnects.remove(&session_uuid);
            return;
        };
        let pty = session_handle.pty();

        if let Err(e) = conn.install_reader(
            session_uuid.clone(),
            pty.event_tx_clone(),
            pty.kitty_enabled_arc(),
            pty.cursor_visible_arc(),
            pty.resize_pending_arc(),
            Arc::clone(pty.last_output_at_atomic()),
            pty.last_human_input_atomic(),
            self.hub_event_tx.clone(),
        ) {
            log::error!(
                "[Session] Failed to install reader after reconnect for '{}': {e}",
                &session_uuid[..session_uuid.len().min(16)]
            );
            self.hub_event_metrics.record_counter("reconnect.failed", 1);
            self.pending_reconnects.remove(&session_uuid);
            pty.notify_process_exited(None);
            let data = serde_json::json!({
                "session_uuid": session_uuid,
                "exit_code": null,
            });
            let _ = self.lua.fire_json_event("session_process_exited", &data);
            return;
        }

        if let Some(shared) = pty.shared_session_connection() {
            if let Ok(mut guard) = shared.lock() {
                *guard = Some(conn);
            }
        }

        pty.kitty_enabled_arc().store(
            metadata.mode_flags.kitty_enabled,
            std::sync::atomic::Ordering::Relaxed,
        );
        pty.cursor_visible_arc().store(
            metadata.mode_flags.cursor_visible,
            std::sync::atomic::Ordering::Relaxed,
        );

        self.pending_reconnects.remove(&session_uuid);
        self.hub_event_metrics.record_counter("reconnect.ready", 1);

        log::info!(
            "[Session] Reconnected to '{}' successfully",
            &session_uuid[..session_uuid.len().min(16)]
        );

        let data = serde_json::json!({
            "session_uuid": session_uuid,
            "title": metadata.title,
            "cwd": metadata.cwd,
        });
        if let Err(e) = self.lua.fire_json_event("session_reconnected", &data) {
            log::error!("[Session] Failed to fire session_reconnected: {e}");
        }

        // Resume any client attaches that waited out the reader reconnect.
        self.process_pending_terminal_attaches();
    }

    pub(super) fn handle_session_unregistered_event(&mut self, session_uuid: String) {
        // Covered by mcp_http persist_drops_revoked_session_and_survives_clear_live.
        self.mcp_callers.revoke_session(&session_uuid);
        self.cleanup_pending_session_io_snapshots_for_session(&session_uuid);
        self.cleanup_paste_files(&session_uuid);
        self.terminal_profiles.clear_session(&session_uuid);
        self.terminal_session_peers.remove(&session_uuid);
        self.terminal_subscription_peers
            .retain(|_, (tracked_session, _)| tracked_session != &session_uuid);
        let suffix = format!(":{session_uuid}");
        let worker_keys: Vec<String> = self
            .terminal_client_workers
            .keys()
            .filter(|key| key.ends_with(&suffix))
            .cloned()
            .collect();
        for key in worker_keys {
            self.remove_terminal_client_worker(&key, &session_uuid, "Session");
        }
        for worker in self.browser_client_workers.values() {
            Self::unregister_worker_session_io_sender(worker, &session_uuid, "Session");
        }
        let suffix = format!(":{session_uuid}");
        self.browser_terminal_attach_sizes
            .retain(|key, _| !key.ends_with(&suffix));
        if let Ok(mut active) = self.active_terminal_peers.lock() {
            active.remove(&session_uuid);
        }

        log::debug!("[Session] Unregistered '{}'", session_uuid);
    }
}
