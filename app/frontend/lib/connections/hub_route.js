import bridge from "workers/bridge";
import { ensureMatrixReady } from "matrix/bundle";
import { observeBrowserSocketState } from "transport/hub_signaling_client";
import { ConnectionState, BrowserStatus, CliStatus, ConnectionMode } from "connections/constants";

export { ConnectionState, BrowserStatus, CliStatus, ConnectionMode };

const TAB_ID = crypto.randomUUID();
const RECONNECT_DELAY_MS = 1000;
const SESSION_TIMEOUT_MS = 5000;
const PEER_PROBE_TIMEOUT_MS = 1500;
const PEER_PROBE_COOLDOWN_MS = 1000;
const PEER_PROBE_MISSED_PONG_LIMIT = 3;
const PLAINTEXT_OPT_IN_FRAGMENT = "#no-encryption";

const HEALTH_STATUS_MAP = {
  offline: CliStatus.OFFLINE,
  online: CliStatus.ONLINE,
  notified: CliStatus.ONLINE,
  connecting: CliStatus.ONLINE,
  connected: CliStatus.ONLINE,
  disconnected: CliStatus.OFFLINE,
};

export class HubRoute {
  static tabId = TAB_ID;

  #hubConnected = false;
  #listenersBound = false;
  #subscriptionListeners = [];
  #unsubscribers = [];
  #signalingConnected = false;
  #subscriptionPending = null;
  #subscriptionGeneration = 0;
  #sessionPending = null;
  #connectPending = null;
  #reconnectTimer = null;
  #idle = false;
  #destroyed = false;
  #browserSocketObserverCleanup = null;
  #peerHealthDirty = false;
  #peerProbePending = null;
  #peerProbeResolve = null;
  #lastPeerProbeAt = 0;
  #missedPeerPongs = 0;

  constructor(key, options = {}, manager) {
    this.key = key;
    this.options = options;
    this.manager = manager;

    this.subscriptionId = null;
    this.identityKey = null;
    this.browserIdentity = null;
    this.plaintextAllowed = window.location.hash === PLAINTEXT_OPT_IN_FRAGMENT;
    this.encryptionEnabled = null;
    this.state = ConnectionState.DISCONNECTED;
    this.browserSocketState = "disconnected";
    this.cliStatus = CliStatus.UNKNOWN;
    this.connectionMode = ConnectionMode.UNKNOWN;
    this.errorCode = null;
    this.errorReason = null;
    this.lastError = null;
    this.subscribers = new Map();
  }

  async initialize() {
    if (this.#destroyed) return;

    this.#idle = false;
    this.#setState(ConnectionState.LOADING);
    await this.#ensureMatrix();
    await this.#refreshIdentity();
    this.#bindBridgeListeners();
    await this.#connectSignaling();
    await this.#ensureConnected();
  }

  async reacquire() {
    if (this.#destroyed) return;

    this.#idle = false;
    if (!this.#listenersBound) {
      this.#bindBridgeListeners();
    }

    await this.#refreshIdentity();
    await this.#connectSignaling();
    await this.#ensureConnected();
  }

  destroy() {
    this.#destroyed = true;
    this.#clearReconnectTimer();
    this.#clearSubscription();
    this.#browserSocketObserverCleanup?.();
    this.#browserSocketObserverCleanup = null;

    for (const unsubscribe of this.#unsubscribers) {
      unsubscribe();
    }
    this.#unsubscribers = [];
    this.#listenersBound = false;

    this.subscriptionId = null;
    this.identityKey = null;
    this.browserIdentity = null;
    this.encryptionEnabled = null;
    this.#hubConnected = false;
    this.#signalingConnected = false;
    this.browserSocketState = "disconnected";
    this.cliStatus = CliStatus.UNKNOWN;
    this.connectionMode = ConnectionMode.UNKNOWN;
    this.state = ConnectionState.DISCONNECTED;

    const hubId = this.getHubId();
    if (hubId && !this.manager.hasActiveConnectionForHub(hubId)) {
      bridge.send("disconnect", { hubId }).catch(() => {});
    }

    this.emit("destroyed");
    this.subscribers.clear();
  }

  release() {
    this.manager.release(this.key);
  }

  notifyIdle() {
    this.#idle = true;
    this.#clearReconnectTimer();
    this.#clearSubscription();

    const hubId = this.getHubId();
    if (!hubId) return;
    if (this.manager.hasActiveConnectionForHub(hubId)) return;

    bridge.send("disconnect", { hubId }).catch(() => {});
  }

  on(event, callback) {
    if (!this.subscribers.has(event)) {
      this.subscribers.set(event, new Set());
    }
    this.subscribers.get(event).add(callback);
    return () => this.off(event, callback);
  }

  off(event, callback) {
    this.subscribers.get(event)?.delete(callback);
  }

  emit(event, data) {
    const callbacks = this.subscribers.get(event);
    if (!callbacks) return;

    for (const callback of callbacks) {
      try {
        callback(data);
      } catch (error) {
        console.error(`[${this.constructor.name}] Event handler error:`, error);
      }
    }
  }

  getHubId() {
    return this.options.hubId;
  }

  getError() {
    return this.errorReason;
  }

  isConnected() {
    return this.state === ConnectionState.CONNECTED;
  }

  isHubConnected() {
    return this.#hubConnected;
  }

  processMessage(message) {
    if (!message?.type) return false;

    if (message.type === "connected") {
      this.#sendData({ type: "ack", timestamp: Date.now() }).catch(() => {});
      return true;
    }

    if (message.type === "ack") {
      return true;
    }

    if (message.type === "dc_ping") {
      this.#sendData({ type: "dc_pong" }).catch(() => {});
      return true;
    }

    if (message.type === "dc_pong") {
      this.#missedPeerPongs = 0;
      this.#resolvePeerProbe(true);
      return true;
    }

    if (message.type === "cli_disconnected") {
      this.#clearSubscription();
      if (this.state === ConnectionState.CONNECTED) {
        this.emit("disconnected", this);
      }
      this.#setState(ConnectionState.CLI_DISCONNECTED);
      return true;
    }

    return false;
  }

  /**
   * Send a client command envelope through the active subscription.
   *
   * This is intentionally transport-only on the browser side: the command is
   * framed, tagged with the route subscription, and delivered to
   * cli/lua/lib/client.lua for dispatch. Browser code must not own a parallel
   * command path.
   */
  async sendCommand(type, data = {}) {
    await this.#ensureReadyToSend();
    if (!this.subscriptionId) return false;

    try {
      await this.#sendData({ type, ...data });
      return true;
    } catch (error) {
      console.error(`[${this.constructor.name}] Send failed:`, error);
      this.#scheduleReconnect();
      return false;
    }
  }

  /**
   * Send transport telemetry that is intentionally consumed before command
   * dispatch. Use this only for client-specific data-plane state, not hub
   * commands.
   */
  async sendTelemetry(type, data = {}) {
    await this.#ensureReadyToSend();
    if (!this.subscriptionId) return false;

    try {
      await this.#sendData({ type, ...data });
      return true;
    } catch (error) {
      console.error(`[${this.constructor.name}] Telemetry send failed:`, error);
      this.#scheduleReconnect();
      return false;
    }
  }

  async sendBinaryPty(data) {
    await this.#ensureReadyToSend();
    if (!this.subscriptionId) return false;

    try {
      await bridge.send("sendPtyInput", {
        hubId: this.getHubId(),
        subscriptionId: this.subscriptionId,
        data,
      });
      return true;
    } catch (error) {
      console.error(`[${this.constructor.name}] PTY send failed:`, error);
      this.#scheduleReconnect();
      return false;
    }
  }

  async sendBinaryFile(data, filename) {
    await this.#ensureReadyToSend();
    if (!this.subscriptionId) return false;

    try {
      await bridge.send("sendFileInput", {
        hubId: this.getHubId(),
        subscriptionId: this.subscriptionId,
        data,
        filename,
      });
      return true;
    } catch (error) {
      console.error(`[${this.constructor.name}] File send failed:`, error);
      this.#scheduleReconnect();
      return false;
    }
  }

  async #ensureMatrix() {
    const cryptoWorkerUrl = document.querySelector('meta[name="crypto-worker-url"]')?.content;
    const wasmJsUrl = document.querySelector('meta[name="crypto-wasm-js-url"]')?.content;
    const wasmBinaryUrl = document.querySelector('meta[name="crypto-wasm-binary-url"]')?.content;
    await ensureMatrixReady(cryptoWorkerUrl, wasmJsUrl, wasmBinaryUrl);
  }

  async #refreshIdentity() {
    const hubId = this.getHubId();
    const { hasPairing } = await bridge.hasPairing(hubId);

    if (!hasPairing) {
      this.identityKey = null;
      this.browserIdentity = `anon:${HubRoute.tabId}`;
      this.errorCode = "unpaired";
      this.errorReason = "Scan connection code";
      this.lastError = this.errorReason;
      this.emit("error", { reason: this.errorCode, message: this.errorReason });
      return;
    }

    const keyResult = await bridge.getIdentityKey(hubId);
    this.identityKey = keyResult.identityKey;
    this.browserIdentity = `${this.identityKey}:${HubRoute.tabId}`;

    if (this.errorCode === "unpaired" || this.errorCode === "session_invalid") {
      this.errorCode = null;
      this.errorReason = null;
      this.lastError = null;
    }
  }

  #bindBridgeListeners() {
    if (this.#listenersBound) return;
    this.#listenersBound = true;

    const hubId = this.getHubId();

    // Route gating must use the same browser-socket source as the UI badge.
    // Otherwise the page can look "browser connected" while ensureConnected()
    // still believes signaling is unavailable and never attempts WebRTC.
    observeBrowserSocketState((state) => {
      if (this.#destroyed) return;
      this.#setBrowserSocketState(state);
      if (state === "connected") {
        this.#markPeerHealthDirty("browser_socket_connected");
        this.#ensureConnected().catch(() => {});
      } else if (!this.isConnected()) {
        this.#setState(ConnectionState.DISCONNECTED);
      }
    }).then((cleanup) => {
      if (this.#destroyed) {
        cleanup?.();
        return;
      }
      this.#browserSocketObserverCleanup = cleanup;
    }).catch((error) => {
      console.error(`[${this.constructor.name}] Failed to observe browser socket state:`, error);
    });

    this.#unsubscribers.push(
      bridge.on("signaling:state", (event) => {
        if (event.hubId !== hubId) return;
        this.#signalingConnected = event.state === "connected";
        if (this.#signalingConnected) {
          this.#markPeerHealthDirty("signaling_connected");
          this.#ensureConnected().catch(() => {});
        }
      }),
      bridge.on("health", (event) => {
        if (event.hubId !== hubId) return;
        // A live health payload can only arrive over an active signaling
        // subscription, so treat it as proof that signaling is up even if the
        // ActionCable "connected" callback was missed or delayed.
        this.#signalingConnected = true;
        const status = HEALTH_STATUS_MAP[event.cli] || CliStatus.UNKNOWN;
        this.#setCliStatus(status);
        if (status === CliStatus.ONLINE) {
          this.#ensureConnected().catch(() => {});
        } else if (!this.isConnected()) {
          this.#setState(ConnectionState.CLI_DISCONNECTED);
        }
      }),
      bridge.on("transport:config", (event) => {
        if (event.hubId !== hubId) return;
        if (event.error === "plaintext_opt_in_required") {
          this.encryptionEnabled = null;
          this.errorCode = event.error;
          this.errorReason = "Open the no-encryption connection URL to allow plaintext transport";
          this.lastError = this.errorReason;
          this.emit("error", { reason: this.errorCode, message: this.errorReason });
          return;
        }
        if (typeof event.e2e_enabled !== "boolean") {
          this.encryptionEnabled = null;
          return;
        }
        this.encryptionEnabled = event.e2e_enabled !== false;
        if (!this.encryptionEnabled && this.errorCode === "unpaired") {
          this.errorCode = null;
          this.errorReason = null;
          this.lastError = null;
        }
        if (this.encryptionEnabled && !this.identityKey) {
          this.errorCode = "unpaired";
          this.errorReason = "Scan connection code";
          this.lastError = this.errorReason;
          this.emit("error", { reason: this.errorCode, message: this.errorReason });
          return;
        }
        this.#ensureConnected().catch(() => {});
      }),
      bridge.on("connection:state", (event) => {
        if (event.hubId !== hubId) return;
        if (event.state === "connected") {
          if (Number.isFinite(event.peerReadyMs) && localStorage.getItem("botster:debug:webrtcTiming") === "1") {
            console.warn(`[HubRoute] WebRTC peer ready for hub ${hubId} in ${event.peerReadyMs}ms`);
          }
          this.#clearReconnectTimer();
          this.#peerHealthDirty = false;
          this.#missedPeerPongs = 0;
          if (event.mode) this.#setConnectionMode(event.mode);
          if (this.#idle) return;
          this.#ensureSubscribed({ replay: true }).catch((error) => {
            console.error(`[${this.constructor.name}] Subscribe failed after peer connect:`, error);
            this.#scheduleReconnect();
          });
        } else {
          const wasConnected = this.state === ConnectionState.CONNECTED;
          this.#clearSubscription();
          this.#setState(ConnectionState.DISCONNECTED);
          if (wasConnected) {
            this.emit("disconnected", this);
          }
          if (this.browserSocketState === "connected" && this.cliStatus === CliStatus.ONLINE) {
            this.#scheduleReconnect();
          }
        }
      }),
      bridge.on("connection:stalled", (event) => {
        if (event.hubId !== hubId) return;
        this.#markPeerHealthDirty("connection_stalled");
        this.#ensureConnected().catch(() => {});
      }),
      bridge.on("connection:mode", (event) => {
        if (event.hubId !== hubId) return;
        this.#setConnectionMode(event.mode || ConnectionMode.UNKNOWN);
      }),
      bridge.on("subscription:ready", (event) => {
        if (event.hubId !== hubId) return;
        if (event.subscriptionId !== this.subscriptionId) return;
        if (Number.isFinite(event.subscribeReadyMs) && localStorage.getItem("botster:debug:webrtcTiming") === "1") {
          console.warn(`[HubRoute] WebRTC subscribe ready for hub ${hubId} in ${event.subscribeReadyMs}ms`);
        }
      }),
      bridge.on("session:invalid", (event) => {
        if (event.hubId !== hubId) return;
        this.errorCode = "session_invalid";
        this.errorReason = event.message || "Session invalid";
        this.lastError = this.errorReason;
        this.#clearSubscription();
        this.#setState(ConnectionState.ERROR);
        this.emit("error", { reason: this.errorCode, message: this.errorReason });
      }),
      bridge.on("session:refreshed", (event) => {
        if (event.hubId !== hubId) return;
        if (this.errorCode === "session_invalid") {
          this.errorCode = null;
          this.errorReason = null;
          this.lastError = null;
          this.#setState(ConnectionState.DISCONNECTED);
        }
        this.#ensureConnected().catch(() => {});
      }),
      bridge.on("push:status", (event) => {
        if (event.hubId !== hubId) return;
        this.emit("push:status", event);
      }),
      bridge.on("push:vapid_key", (event) => {
        if (event.hubId !== hubId) return;
        this.emit("push:vapid_key", event);
      }),
      bridge.on("push:sub_ack", (event) => {
        if (event.hubId !== hubId) return;
        this.emit("push:sub_ack", event);
      }),
      bridge.on("push:vapid_keys", (event) => {
        if (event.hubId !== hubId) return;
        this.emit("push:vapid_keys", event);
      }),
      bridge.on("push:test_ack", (event) => {
        if (event.hubId !== hubId) return;
        this.emit("push:test_ack", event);
      }),
      bridge.on("push:disable_ack", (event) => {
        if (event.hubId !== hubId) return;
        this.emit("push:disable_ack", event);
      }),
    );

    const onVisibilityChange = () => {
      if (document.visibilityState !== "visible") return;
      this.#markPeerHealthDirty("visibilitychange");
      this.#ensureConnected().catch(() => {});
    };
    const onPageShow = () => {
      this.#markPeerHealthDirty("pageshow");
      this.#ensureConnected().catch(() => {});
    };

    document.addEventListener("visibilitychange", onVisibilityChange);
    window.addEventListener("pageshow", onPageShow);
    this.#unsubscribers.push(
      () => document.removeEventListener("visibilitychange", onVisibilityChange),
      () => window.removeEventListener("pageshow", onPageShow),
    );
  }

  async #connectSignaling() {
    if (this.#destroyed) return;

    this.#setState(ConnectionState.CONNECTING);

    const result = await bridge.send("connectSignaling", {
      hubId: this.getHubId(),
      browserIdentity: this.browserIdentity,
      allowPlaintext: this.plaintextAllowed,
    });

    this.#hubConnected = true;
    this.#setBrowserSocketState(result?.browserSocketState || "disconnected");

    if (result?.state === "connected") {
      this.#signalingConnected = true;
      this.#setConnectionMode(result?.mode || this.connectionMode);
    }
  }

  async #ensureConnected() {
    if (this.#destroyed) return;
    if (this.#idle) return;
    if (this.errorCode === "session_invalid") return;
    if (!this.#hubConnected) return;
    if (this.encryptionEnabled === null) return;
    if (this.encryptionEnabled && !this.identityKey) return;
    // WebRTC is gated only by the raw browser->Rails socket plus live hub
    // health. If both are ready, attempt the peer even if prior callbacks were
    // delayed or missed.
    if (this.browserSocketState !== "connected") return;
    if (this.cliStatus !== CliStatus.ONLINE) return;

    if (this.subscriptionId) {
      if (this.#peerHealthDirty) {
        const healthy = await this.#probeExistingPeer();
        if (!healthy) {
          await this.#rebuildPeer("probe_failed");
        } else {
          this.#peerHealthDirty = false;
          this.#setState(ConnectionState.CONNECTED);
          return;
        }
      }

      if (this.subscriptionId) {
        this.#peerHealthDirty = false;
        await this.#ensureSubscribed();
        this.#setState(ConnectionState.CONNECTED);
        return;
      }
    }

    if (this.#connectPending) {
      return this.#connectPending;
    }

    this.#connectPending = (async () => {
      this.#setState(ConnectionState.CONNECTING);
      await this.#ensureActiveSession();
      await bridge.send("connectPeer", { hubId: this.getHubId() });
      // Reused shared peers may already be connected and therefore not emit a
      // fresh connection:state event for this route. Ensure the route's
      // subscription exists even when connectPeer() is effectively a no-op.
      await this.#ensureSubscribed({ replay: true });
      this.#peerHealthDirty = false;
    })();

    try {
      await this.#connectPending;
    } finally {
      this.#connectPending = null;
    }
  }

  async #ensureReadyToSend() {
    if (this.subscriptionId && this.isConnected()) return;
    await this.#ensureConnected();
  }

  async #probeExistingPeer() {
    const now = Date.now();
    if (this.#peerProbePending) {
      return this.#peerProbePending;
    }

    if (now - this.#lastPeerProbeAt < PEER_PROBE_COOLDOWN_MS) {
      return true;
    }

    this.#peerProbePending = (async () => {
      this.#lastPeerProbeAt = Date.now();

      const peer = await bridge.send("probePeerHealth", { hubId: this.getHubId() });
      if (!peer?.alive) {
        this.#missedPeerPongs = PEER_PROBE_MISSED_PONG_LIMIT;
        return false;
      }

      if (!this.subscriptionId) {
        return false;
      }

      const pong = await this.#awaitPeerPong();
      if (pong) {
        this.#missedPeerPongs = 0;
        return true;
      }

      this.#missedPeerPongs += 1;
      if (this.#missedPeerPongs < PEER_PROBE_MISSED_PONG_LIMIT) {
        console.warn(
          `[${this.constructor.name}] Peer pong missed (${this.#missedPeerPongs}/${PEER_PROBE_MISSED_PONG_LIMIT}); keeping peer because local transport is alive`,
        );
        return true;
      }

      console.warn(
        `[${this.constructor.name}] Peer pong missed ${this.#missedPeerPongs} times; rebuilding peer`,
      );
      return false;
    })();

    try {
      return await this.#peerProbePending;
    } catch (error) {
      console.warn(`[${this.constructor.name}] Peer probe failed:`, error);
      return false;
    } finally {
      this.#peerProbePending = null;
    }
  }

  async #awaitPeerPong() {
    if (!this.subscriptionId) return false;

    const pongPromise = new Promise((resolve) => {
      let timer = null;
      const resolver = (alive) => {
        clearTimeout(timer);
        if (this.#peerProbeResolve === resolver) {
          this.#peerProbeResolve = null;
        }
        resolve(alive);
      };

      timer = setTimeout(() => {
        resolver(false);
      }, PEER_PROBE_TIMEOUT_MS);

      this.#peerProbeResolve = resolver;
    });

    try {
      await this.#sendData({ type: "dc_ping", timestamp: Date.now() });
    } catch (error) {
      this.#resolvePeerProbe(false);
      return false;
    }

    return pongPromise;
  }

  async #rebuildPeer(reason) {
    this.#clearSubscription();
    this.#resolvePeerProbe(false);
    this.#peerHealthDirty = true;
    this.#missedPeerPongs = 0;

    try {
      await bridge.send("disconnectPeer", { hubId: this.getHubId() });
    } catch (error) {
      console.warn(`[${this.constructor.name}] Failed to disconnect stale peer (${reason}):`, error);
    }
  }

  #markPeerHealthDirty(_reason) {
    this.#peerHealthDirty = true;
  }

  #resolvePeerProbe(alive) {
    const resolve = this.#peerProbeResolve;
    if (!resolve) return;
    this.#peerProbeResolve = null;
    resolve(alive);
  }

  async #ensureActiveSession() {
    if (!this.encryptionEnabled) return true;

    if (this.#sessionPending) {
      return this.#sessionPending;
    }

    this.#sessionPending = (async () => {
      const hubId = this.getHubId();
      const hasSession = await bridge.hasSession(hubId, this.browserIdentity);
      if (hasSession.hasSession) return true;

      const bundleArrived = await bridge.send("awaitFreshBundle", {
        hubId,
        timeoutMs: 750,
      });
      if (!bundleArrived?.refreshed) {
        await bridge.send("requestFreshBundle", { hubId });
      }

      const refreshed = await bridge.hasSession(hubId, this.browserIdentity);
      if (!refreshed.hasSession) {
        throw new Error("No active session");
      }
      return true;
    })();

    try {
      return await this.#sessionPending;
    } finally {
      this.#sessionPending = null;
    }
  }

  /**
   * Whether peer reconnect / ensureConnected may (re)subscribe this route.
   * Terminal routes override this after process_exited so dead sessions do not
   * re-enter the hub attach path and thrash SessionIo.
   */
  shouldResubscribeOnPeerConnect() {
    return true;
  }

  async #ensureSubscribed({ replay = false } = {}) {
    if (this.#idle || this.#destroyed) return;
    if (!this.shouldResubscribeOnPeerConnect()) {
      return;
    }

    if (this.subscriptionId) {
      this.#setupSubscriptionListeners();
      if (replay) {
        return this.#sendSubscribe(this.subscriptionId, this.#subscriptionGeneration, {
          resetMissedPongs: false,
        });
      }
      this.#setState(ConnectionState.CONNECTED);
      return;
    }

    if (this.#subscriptionPending) {
      return this.#subscriptionPending;
    }

    this.subscriptionId = this.computeSubscriptionId();
    const subscriptionId = this.subscriptionId;
    const generation = this.#subscriptionGeneration;
    this.#setupSubscriptionListeners();
    if (localStorage.getItem("botster:debug:webrtcTiming") === "1") {
      console.warn(`[HubRoute] WebRTC subscribe start for hub ${this.getHubId()}`);
    }

    this.#subscriptionPending = this.#sendSubscribe(subscriptionId, generation, {
      resetMissedPongs: true,
    });

    return this.#subscriptionPending;
  }

  #sendSubscribe(subscriptionId, generation, { resetMissedPongs = false } = {}) {
    if (this.#subscriptionPending) {
      return this.#subscriptionPending;
    }

    const subscribePromise = bridge.send("subscribe", {
      hubId: this.getHubId(),
      channel: this.channelName(),
      params: this.channelParams(),
      subscriptionId,
    }).then(() => {
      // Stale attempt: a newer generation may have reclaimed the same stable
      // terminal_* id. Do not unsubscribe/clear listeners for the new owner.
      if (this.#destroyed || generation !== this.#subscriptionGeneration) {
        return;
      }
      if (this.subscriptionId !== subscriptionId) {
        bridge.clearSubscriptionListeners(subscriptionId);
        bridge.send("unsubscribe", { subscriptionId }).catch(() => {});
        return;
      }
      if (resetMissedPongs) {
        this.#missedPeerPongs = 0;
      }
      const wasConnected = this.state === ConnectionState.CONNECTED;
      this.#setState(ConnectionState.CONNECTED);
      if (!wasConnected) {
        this.emit("connected", this);
      }
    }).catch((error) => {
      if (generation !== this.#subscriptionGeneration) {
        return;
      }
      if (this.subscriptionId === subscriptionId) {
        this.#clearSubscription();
      } else {
        bridge.clearSubscriptionListeners(subscriptionId);
      }
      throw error;
    }).finally(() => {
      // Only clear pending if this attempt still owns it (soft re-subscribe
      // may have replaced #subscriptionPending with a newer promise).
      if (this.#subscriptionPending === subscribePromise) {
        this.#subscriptionPending = null;
      }
    });

    this.#subscriptionPending = subscribePromise;
    return this.#subscriptionPending;
  }

  #setupSubscriptionListeners() {
    this.#clearSubscriptionListeners();

    const subscriptionId = this.subscriptionId;
    if (!subscriptionId) return;

    this.#subscriptionListeners.push(
      bridge.onSubscriptionMessage(subscriptionId, (message) => {
        if (message instanceof Uint8Array) {
          this.handleMessage({ type: "raw_output", data: message });
          return;
        }

        if (this.processMessage(message)) return;
        this.handleMessage(message);
      }),
    );
  }

  #clearSubscriptionListeners() {
    for (const unsubscribe of this.#subscriptionListeners) {
      unsubscribe();
    }
    this.#subscriptionListeners = [];
  }

  #clearSubscription() {
    this.#subscriptionGeneration++;
    // Drop in-flight subscribe so reacquire after soft process_exited does not
    // await a stale promise that unsubscribes on generation mismatch.
    this.#subscriptionPending = null;
    const subscriptionId = this.subscriptionId;
    this.subscriptionId = null;
    this.#resolvePeerProbe(false);
    this.#clearSubscriptionListeners();
    if (subscriptionId) {
      bridge.clearSubscriptionListeners(subscriptionId);
      bridge.send("unsubscribe", { subscriptionId }).catch(() => {});
    }
  }

  async #sendData(message) {
    if (!this.subscriptionId) {
      throw new Error("No subscription");
    }

    const fullMessage = {
      subscriptionId: this.subscriptionId,
      ...message,
    };

    const jsonBytes = new TextEncoder().encode(JSON.stringify(fullMessage));
    const plaintext = new Uint8Array(1 + jsonBytes.length);
    plaintext[0] = 0x00;
    plaintext.set(jsonBytes, 1);

    await bridge.send("sendData", { hubId: this.getHubId(), plaintext });
  }

  #scheduleReconnect() {
    if (this.#reconnectTimer || this.#destroyed) return;

    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      this.#ensureConnected().catch(() => {});
    }, RECONNECT_DELAY_MS);
  }

  #clearReconnectTimer() {
    if (this.#reconnectTimer) {
      clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = null;
    }
  }

  #setState(nextState) {
    const prevState = this.state;
    if (nextState === prevState) return;

    this.state = nextState;
    if (nextState !== ConnectionState.ERROR) {
      this.lastError = this.errorReason;
    }

    const stateInfo = { state: nextState, prevState, error: this.errorReason };
    this.emit("stateChange", stateInfo);
    this.manager.notifySubscribers(this.key, stateInfo);
  }

  #setBrowserSocketState(nextState) {
    const prevState = this.browserSocketState;
    if (nextState === prevState) return;
    this.browserSocketState = nextState;
    this.emit("browserSocketStateChange", { status: nextState, prevStatus: prevState });
  }

  #setCliStatus(nextStatus) {
    const prevStatus = this.cliStatus;
    if (nextStatus === prevStatus) return;
    this.cliStatus = nextStatus;
    this.emit("cliStatusChange", { status: nextStatus, prevStatus });
  }

  #setConnectionMode(nextMode) {
    const prevMode = this.connectionMode;
    if (nextMode === prevMode) return;
    this.connectionMode = nextMode || ConnectionMode.UNKNOWN;
    this.emit("connectionModeChange", {
      mode: this.connectionMode,
      prevMode,
    });
  }

  channelName() {
    throw new Error("channelName() must be implemented by subclass");
  }

  computeSubscriptionId() {
    throw new Error("computeSubscriptionId() must be implemented by subclass");
  }

  channelParams() {
    return {};
  }

  handleMessage(message) {
    this.emit("message", message);
  }
}
