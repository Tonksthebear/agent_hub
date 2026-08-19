use super::*;

impl Hub {
    pub(crate) fn register_hub_with_server(&mut self) {
        let botster_id = registration::register_hub_with_server(
            &self.hub_identifier,
            &self.config.server_url,
            self.config.get_api_key(),
            &self.device.fingerprint,
        );
        // Store server-assigned ID (used for all server communication)
        self.botster_id = Some(botster_id.clone());
        // Sync to shared copy for Lua primitives
        *self
            .shared_server_id
            .lock()
            .expect("SharedServerId mutex poisoned") = Some(botster_id.clone());
        // Keep runtime manifest aligned with server-assigned hub ID.
        let manifest_started = Instant::now();
        if let Err(e) =
            crate::hub::daemon::write_manifest(&self.hub_identifier, self.botster_id.as_deref())
        {
            self.hub_event_metrics
                .record_counter("manifest.write_error", 1);
            log::warn!("Failed to refresh hub manifest after server registration: {e}");
        }
        self.hub_event_metrics.record_span_with_threshold(
            "manifest.write",
            manifest_started.elapsed(),
            0,
            Duration::from_millis(10),
            &self.hub_identifier,
        );

        // Prefetch ICE config so the first WebRTC offer doesn't pay
        // the HTTP round-trip cost (100-300ms saved on first connection).
        let server_url = self.config.server_url.clone();
        let api_key = self.config.get_api_key().to_string();
        let hub_id = botster_id;
        self.tokio_runtime.spawn(async move {
            crate::channel::WebRtcChannel::prefetch_ice_config(&server_url, &api_key, &hub_id)
                .await;
        });
    }

    pub(crate) fn init_web_push(&mut self) {
        // Device-level VAPID keys
        match crate::relay::persistence::load_vapid_keys() {
            Ok(Some(keys)) => {
                log::info!("[WebPush] Loaded device-level VAPID keys");
                self.vapid_keys = Some(keys);
            }
            Ok(None) => {
                // Try legacy per-hub keys (migration from earlier versions)
                match crate::relay::persistence::load_legacy_hub_vapid_keys(&self.hub_identifier) {
                    Ok(Some(legacy_keys)) => {
                        log::info!("[WebPush] Migrating legacy per-hub VAPID keys to device level");
                        if let Err(e) = crate::relay::persistence::save_vapid_keys(&legacy_keys) {
                            log::error!("[WebPush] Failed to save migrated VAPID keys: {e}");
                        }
                        self.vapid_keys = Some(legacy_keys);
                    }
                    Ok(None) => {
                        log::debug!(
                            "[WebPush] No VAPID keys yet (browser will trigger generation)"
                        );
                    }
                    Err(e) => log::error!("[WebPush] Failed to load legacy VAPID keys: {e}"),
                }
            }
            Err(e) => log::error!("[WebPush] Failed to load VAPID keys: {e}"),
        }

        // Device-level push subscriptions (shared across all hubs)
        match crate::relay::persistence::load_push_subscriptions() {
            Ok(mut store) => {
                // Clean up duplicate subscriptions from browser reconnections
                let removed = store.dedup_by_endpoint();
                if removed > 0 {
                    log::info!(
                        "[WebPush] Removed {} duplicate subscription(s) (same endpoint, different identity)",
                        removed
                    );
                    if let Err(e) = crate::relay::persistence::save_push_subscriptions(&store) {
                        log::error!("[WebPush] Failed to save deduped subscriptions: {e}");
                    }
                }
                if !store.is_empty() {
                    log::info!("[WebPush] Loaded {} push subscription(s)", store.len());
                }
                self.push_subscriptions = store;
            }
            Err(e) => log::error!("[WebPush] Failed to load push subscriptions: {e}"),
        }
    }

    pub(crate) fn init_crypto_service(&mut self) {
        registration::init_crypto_service(&mut self.browser, &self.hub_identifier);
    }

    pub(crate) fn get_or_generate_connection_url(&mut self) -> Result<String, String> {
        // Extract values before mutable borrow of browser
        let server_hub_id = self.server_hub_id().to_string();
        let local_id = self.hub_identifier.clone();
        let server_url = self.config.server_url.clone();

        if crate::env::is_encryption_disabled() {
            let connection_url = format!("{server_url}/hubs/{server_hub_id}#no-encryption");
            if let Err(error) = crate::relay::write_connection_url(&local_id, &connection_url) {
                log::warn!("Failed to write connection URL: {error}");
            }
            return Ok(connection_url);
        }

        registration::write_connection_url_lazy(
            &mut self.browser,
            &self.tokio_runtime,
            &server_hub_id,
            &local_id,
            &server_url,
        )
    }
}
