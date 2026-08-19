/**
 * WebRTC transport coordinator.
 *
 * The browser imports this one module as the public transport entrypoint.
 * The implementation is split into hard-boundary helper modules:
 * - HubSignalingClient: ActionCable and browser socket state
 * - HubPeerLifecycle: RTCPeerConnection and ICE lifecycle
 * - HubChannelProtocol: DataChannel message protocol
 */

import bridge from "workers/bridge"
import { parseBinaryBundle } from "matrix/bundle"
import { HubSignalingClient } from "transport/hub_signaling_client"
import { HubChannelProtocol } from "transport/hub_channel_protocol"
import { HubPeerLifecycle, PEER_LOST_REASONS } from "transport/hub_peer_lifecycle"

let instance = null

export const TransportState = {
  DISCONNECTED: "disconnected",
  CONNECTING: "connecting",
  CONNECTED: "connected",
  ERROR: "error",
}

const ConnectionMode = {
  UNKNOWN: "unknown",
  DIRECT: "direct",
  RELAYED: "relayed",
}

const CONTENT_MSG = 0x00
const CONTENT_PTY = 0x01
const CONTENT_STREAM = 0x02
const CONTENT_FILE = 0x03
const CONTENT_FILE_CHUNK = 0x04
const MSG_TYPE_BUNDLE_REFRESH = 0x02

const DISCONNECT_GRACE_PERIOD_MS = 3000
const STALLED_EVENT_COOLDOWN_MS = 2000
const ICE_RESTART_DELAY_MS = 1000
const ICE_RESTART_MAX_ATTEMPTS = 3
const ICE_RESTART_BACKOFF_MULTIPLIER = 2
const MAX_PENDING_REMOTE_ICE = 128
const ICE_CONFIG_CACHE_TTL_MS = 60_000
const ICE_CONFIG_FETCH_TIMEOUT_MS = 3000
const PEER_SETUP_TIMEOUT_MS = 15_000
const TRANSPORT_CONFIG_FALLBACK_MS = 5_000
const DIRECT_CHURN_WINDOW_MS = 60_000
const DIRECT_CHURN_THRESHOLD = 1
const RELAY_FALLBACK_MS = 5 * 60_000

function base64ToBytes(b64) {
  const binary = atob(b64)
  return Uint8Array.from(binary, c => c.charCodeAt(0))
}

function buildControlFrame(payload) {
  const jsonBytes = new TextEncoder().encode(JSON.stringify(payload))
  const frame = new Uint8Array(1 + jsonBytes.length)
  frame[0] = CONTENT_MSG
  frame.set(jsonBytes, 1)
  return frame
}

class HubPeerConnection {
  #connections = new Map()
  #connectPromises = new Map()
  #peerConnectPromises = new Map()
  #bundleRefreshPromises = new Map()
  #dataMessageChains = new Map()
  #serverReadyWaiters = new Map()
  #eventListeners = new Map()
  #subscriptionListeners = new Map()
  #subscriptionIdCounter = 0
  #graceTimers = new Map()
  #signalingClient
  #channelProtocol
  #peerLifecycle

  constructor() {
    this.#signalingClient = new HubSignalingClient({
      notify: (event, payload) => this.#emit(event, payload),
    })

    this.#channelProtocol = new HubChannelProtocol({
      callbacks: {
        emit: (event, payload) => this.#emit(event, payload),
        getConnection: (hubId) => this.#connections.get(hubId),
        createSession: (hubId, bundle, browserIdentity) =>
          bridge.createSession(String(hubId), bundle, browserIdentity),
        decryptBinary: (hubId, raw) => bridge.decryptBinary(String(hubId), raw),
        isCurrentPeerGeneration: (hubId, generation) =>
          this.#isCurrentPeerGeneration(hubId, generation),
        markServerReady: (hubId, generation) => this.#markServerReady(hubId, generation),
      },
      constants: {
        CONTENT_MSG,
        CONTENT_PTY,
        CONTENT_STREAM,
        MSG_TYPE_BUNDLE_REFRESH,
      },
    })

    this.#peerLifecycle = new HubPeerLifecycle({
      callbacks: {
        emit: (event, payload) => this.#emit(event, payload),
        getConnection: (hubId) => this.#connections.get(hubId),
        getSignalingSubscription: (hubId) => this.#signalingClient.getSubscription(hubId),
        getIceConfig: (hubId, conn) => this.#getIceConfig(hubId, conn),
        encryptSignal: (hubId, payload) => this.#encryptSignal(hubId, payload),
        setupDataChannel: (hubId, dataChannel, generation) =>
          this.#setupDataChannel(hubId, dataChannel, generation),
        peerLost: (hubId, reason) => this.#handlePeerLost(hubId, reason),
      },
      constants: {
        ConnectionMode,
        TransportState,
        ICE_RESTART_DELAY_MS,
        ICE_RESTART_MAX_ATTEMPTS,
        ICE_RESTART_BACKOFF_MULTIPLIER,
        MAX_PENDING_REMOTE_ICE,
        PEER_SETUP_TIMEOUT_MS,
        DIRECT_CHURN_WINDOW_MS,
        DIRECT_CHURN_THRESHOLD,
        RELAY_FALLBACK_MS,
      },
    })

    window.addEventListener("beforeunload", () => {
      for (const timer of this.#graceTimers.values()) {
        clearTimeout(timer)
      }
      this.#graceTimers.clear()

      for (const [hubId, conn] of this.#connections) {
        clearTimeout(conn.transportConfigTimer)
        this.#peerLifecycle.teardownPeer(conn)
        this.#signalingClient.disconnect(hubId)
      }

      this.#connections.clear()
      this.#connectPromises.clear()
      this.#peerConnectPromises.clear()
      this.#bundleRefreshPromises.clear()
    })
  }

  static get instance() {
    if (!instance) {
      instance = new HubPeerConnection()
    }
    return instance
  }

  async connect(hubId, browserIdentity) {
    this.#cancelGracePeriod(hubId)

    let conn = this.#connections.get(hubId)
    if (conn?.pc) return { state: conn.state }
    if (conn && !conn.pc) return this.connectPeer(hubId)

    const pending = this.#connectPromises.get(hubId)
    if (pending) return pending

    const promise = (async () => {
      await this.connectSignaling(hubId, browserIdentity)
      return this.connectPeer(hubId)
    })()
    this.#connectPromises.set(hubId, promise)

    try {
      return await promise
    } finally {
      this.#connectPromises.delete(hubId)
    }
  }

  async connectSignaling(hubId, browserIdentity, allowPlaintext = false) {
    this.#cancelGracePeriod(hubId)

    const existing = this.#connections.get(hubId)
    if (existing) {
      existing.allowPlaintext ||= allowPlaintext
      this.#prefetchIceConfig(hubId, existing)
      this.#signalingClient.emitBrowserSocketStateForHub(hubId)
      if (existing.lastHealth) {
        queueMicrotask(() => this.#emit("health", { hubId, ...existing.lastHealth }))
      }
      return {
        state: existing.state,
        browserSocketState: this.#signalingClient.browserSocketState,
      }
    }

    const conn = {
      pc: null,
      dataChannel: null,
      state: TransportState.DISCONNECTED,
      mode: ConnectionMode.UNKNOWN,
      hubId,
      browserIdentity,
      allowPlaintext,
      subscriptions: new Map(),
      pendingCandidates: [],
      iceRestartAttempts: 0,
      iceRestartTimer: null,
      iceDisconnectedTimer: null,
      iceDisrupted: false,
      decryptFailures: 0,
      activeFileTransferIds: new Set(),
      nextFileTransferId: 0,
      lastStalledAt: 0,
      iceConfig: null,
      iceConfigFetchedAt: 0,
      iceConfigPromise: null,
      peerSetupTimer: null,
      peerSetupStartedAt: 0,
      offerSentAt: null,
      peerGeneration: 0,
      serverReady: false,
      recentDirectDisconnects: [],
      forceRelayUntil: 0,
      signalingConnected: true,
      lastHealth: null,
      encryptionEnabled: null,
      transportConfigPending: true,
      transportConfigTimer: null,
    }

    this.#connections.set(hubId, conn)
    await this.#createSignalingChannel(hubId, browserIdentity)
    this.#prefetchIceConfig(hubId, conn)
    this.#signalingClient.emitBrowserSocketStateForHub(hubId)

    return {
      state: TransportState.DISCONNECTED,
      browserSocketState: this.#signalingClient.browserSocketState,
    }
  }

  async connectPeer(hubId) {
    let conn = this.#connections.get(hubId)
    if (!conn) throw new Error(`No signaling connection for hub ${hubId}`)

    if (conn.pc) {
      const pcState = conn.pc.connectionState
      const dcState = conn.dataChannel?.readyState
      const dcAlive = dcState === "open" || dcState === "connecting"
      const dead = pcState === "closed" || pcState === "failed" || pcState === "disconnected" ||
        (pcState === "connected" && !dcAlive)

      if (dead) {
        this.#handlePeerLost(hubId, PEER_LOST_REASONS.STALE_PEER_ON_CONNECT)
        conn = this.#connections.get(hubId)
      } else {
        return { state: conn.state }
      }
    }

    const pending = this.#peerConnectPromises.get(hubId)
    if (pending) return pending

    const promise = this.#peerLifecycle.connect(hubId, conn)
    this.#peerConnectPromises.set(hubId, promise)

    try {
      return await promise
    } finally {
      this.#peerConnectPromises.delete(hubId)
    }
  }

  probePeerHealth(hubId) {
    return this.#peerLifecycle.probeHealth(hubId)
  }

  disconnectPeer(hubId) {
    this.#peerLifecycle.disconnectPeer(hubId)
  }

  async disconnect(hubId) {
    if (!this.#connections.has(hubId)) return
    if (this.#graceTimers.has(hubId)) return

    console.debug(`[WebRTCTransport] Starting ${DISCONNECT_GRACE_PERIOD_MS}ms grace period for hub ${hubId}`)
    const timer = setTimeout(() => {
      this.#graceTimers.delete(hubId)
      this.#closeConnection(hubId)
    }, DISCONNECT_GRACE_PERIOD_MS)
    this.#graceTimers.set(hubId, timer)
  }

  async requestFreshBundle(hubId, timeoutMs = 5000) {
    const conn = this.#connections.get(hubId)
    if (!conn) throw new Error(`No signaling connection for hub ${hubId}`)
    if (conn.encryptionEnabled === false) return { refreshed: false }
    if (!conn.signalingConnected) throw new Error(`Signaling not connected for hub ${hubId}`)

    const subscription = this.#signalingClient.getSubscription(hubId)
    if (!subscription) throw new Error(`No signaling subscription for hub ${hubId}`)

    const pending = this.#bundleRefreshPromises.get(hubId)
    if (pending) return pending

    const requestedAt = performance.now()
    const promise = new Promise((resolve, reject) => {
      let settled = false

      const cleanup = () => {
        clearTimeout(timer)
        offRefreshed()
        offInvalid()
        this.#bundleRefreshPromises.delete(hubId)
      }

      const offRefreshed = this.on("session:refreshed", (event) => {
        if (settled || event.hubId !== hubId) return
        settled = true
        cleanup()
        console.debug(
          `[WebRTCTransport] Fresh bundle request completed for hub ${hubId} in ${Math.round(performance.now() - requestedAt)}ms`,
        )
        resolve({ refreshed: true })
      })

      const offInvalid = this.on("session:invalid", (event) => {
        if (settled || event.hubId !== hubId) return
        settled = true
        cleanup()
        reject(new Error(event.message || `Fresh bundle request failed for hub ${hubId}`))
      })

      const timer = setTimeout(() => {
        if (settled) return
        settled = true
        cleanup()
        reject(new Error(`Fresh bundle request timed out for hub ${hubId}`))
      }, timeoutMs)

      try {
        subscription.perform("request_bundle", {})
      } catch (error) {
        settled = true
        cleanup()
        reject(error)
      }
    })

    this.#bundleRefreshPromises.set(hubId, promise)
    return promise
  }

  async awaitFreshBundle(hubId, timeoutMs = 750) {
    const conn = this.#connections.get(hubId)
    if (!conn || !conn.signalingConnected) {
      return { refreshed: false }
    }

    const waitStartedAt = performance.now()
    return new Promise((resolve) => {
      let settled = false

      const cleanup = () => {
        clearTimeout(timer)
        offRefreshed()
        offInvalid()
      }

      const offRefreshed = this.on("session:refreshed", (event) => {
        if (settled || event.hubId !== hubId) return
        settled = true
        cleanup()
        console.debug(
          `[WebRTCTransport] Auto fresh bundle arrived for hub ${hubId} in ${Math.round(performance.now() - waitStartedAt)}ms`,
        )
        resolve({ refreshed: true })
      })

      const offInvalid = this.on("session:invalid", (event) => {
        if (settled || event.hubId !== hubId) return
        settled = true
        cleanup()
        resolve({ refreshed: false, invalid: true, message: event.message })
      })

      const timer = setTimeout(() => {
        if (settled) return
        settled = true
        cleanup()
        resolve({ refreshed: false })
      }, timeoutMs)
    })
  }

  async subscribe(hubId, channelName, params, providedSubscriptionId = null, plaintext = null) {
    const conn = this.#connections.get(hubId)
    if (!conn) throw new Error(`No connection for hub ${hubId}`)

    const subscriptionId = providedSubscriptionId || `sub_${++this.#subscriptionIdCounter}_${Date.now()}`
    conn.subscriptions.set(subscriptionId, { channelName, params })

    if (conn.dataChannel?.readyState !== "open") {
      await this.#channelProtocol.waitForDataChannel(conn.dataChannel)
    }
    await this.#waitForServerReady(hubId, conn)
    const subscribeStartedAt = performance.now()

    if (!plaintext) {
      throw new Error("Cannot subscribe without a payload")
    }

    const confirmed = this.#channelProtocol.waitForSubscriptionConfirmed(subscriptionId)
    try {
      const wire = await this.#encodeDataChannelFrame(hubId, plaintext)
      conn.dataChannel.send(wire.buffer)
    } catch (error) {
      this.#channelProtocol.clearPendingSubscription(subscriptionId)
      conn.subscriptions.delete(subscriptionId)
      throw error
    }
    await confirmed
    const subscribeReadyMs = Math.round(performance.now() - subscribeStartedAt)
    this.#emit("subscription:confirmed", { subscriptionId })
    this.#emit("subscription:ready", { hubId, subscriptionId, subscribeReadyMs })

    return { subscriptionId }
  }

  async unsubscribe(subscriptionId) {
    for (const [hubId, conn] of this.#connections) {
      if (!conn.subscriptions.has(subscriptionId)) continue

      if (conn.dataChannel?.readyState === "open") {
        try {
          const plaintext = buildControlFrame({ type: "unsubscribe", subscriptionId })
          const wire = await this.#encodeDataChannelFrame(hubId, plaintext)
          conn.dataChannel.send(wire.buffer)
        } catch (error) {
          console.warn("[WebRTCTransport] Failed to send unsubscribe:", error)
        }
      }

      conn.subscriptions.delete(subscriptionId)
      this.clearSubscriptionListeners(subscriptionId)
      return { unsubscribed: true }
    }

    return { unsubscribed: false }
  }

  async sendRaw(subscriptionId, message) {
    for (const [hubId, conn] of this.#connections) {
      if (!conn.subscriptions.has(subscriptionId)) continue
      if (conn.dataChannel?.readyState !== "open") {
        throw new Error("DataChannel not open")
      }

      const plaintext = buildControlFrame({ subscriptionId, data: message })
      const wire = await this.#encodeDataChannelFrame(hubId, plaintext)
      conn.dataChannel.send(wire.buffer)
      return { sent: true }
    }

    throw new Error(`Subscription ${subscriptionId} not found`)
  }

  async sendData(hubId, plaintext) {
    const conn = this.#connections.get(hubId)
    if (!conn) throw new Error(`No connection for hub ${hubId}`)
    if (conn.dataChannel?.readyState !== "open") {
      throw new Error("DataChannel not open")
    }

    const wire = await this.#encodeDataChannelFrame(hubId, plaintext)
    conn.dataChannel.send(wire.buffer)
    return { sent: true }
  }

  async sendStreamFrame(hubId, frameType, streamId, payload) {
    const conn = this.#connections.get(hubId)
    if (!conn) throw new Error(`No connection for hub ${hubId}`)
    if (conn.dataChannel?.readyState !== "open") {
      throw new Error("DataChannel not open")
    }

    const plaintext = new Uint8Array(4 + (payload?.length || 0))
    plaintext[0] = CONTENT_STREAM
    plaintext[1] = frameType
    plaintext[2] = (streamId >> 8) & 0xFF
    plaintext[3] = streamId & 0xFF
    if (payload?.length) plaintext.set(payload, 4)

    const wire = await this.#encodeDataChannelFrame(hubId, plaintext)
    conn.dataChannel.send(wire.buffer)
  }

  async sendPtyInput(hubId, subscriptionId, data) {
    const conn = this.#connections.get(hubId)
    if (!conn) throw new Error(`No connection for hub ${hubId}`)
    if (conn.dataChannel?.readyState !== "open") {
      throw new Error("DataChannel not open")
    }

    const subIdBytes = new TextEncoder().encode(subscriptionId)
    const dataBytes = typeof data === "string" ? new TextEncoder().encode(data) : data
    const plaintext = new Uint8Array(3 + subIdBytes.length + dataBytes.length)
    plaintext[0] = CONTENT_PTY
    plaintext[1] = 0x02
    plaintext[2] = subIdBytes.length
    plaintext.set(subIdBytes, 3)
    plaintext.set(dataBytes, 3 + subIdBytes.length)

    const wire = await this.#encodeDataChannelFrame(hubId, plaintext)
    conn.dataChannel.send(wire.buffer)

    if (conn.dataChannel.bufferedAmount > 4096) {
      const now = Date.now()
      if (!conn.lastStalledAt || now - conn.lastStalledAt >= STALLED_EVENT_COOLDOWN_MS) {
        conn.lastStalledAt = now
        this.#emit("connection:stalled", { hubId })
      }
    }
  }

  async sendFileInput(hubId, subscriptionId, data, filename) {
    const conn = this.#connections.get(hubId)
    if (!conn) throw new Error(`No connection for hub ${hubId}`)
    if (conn.dataChannel?.readyState !== "open") {
      throw new Error("DataChannel not open")
    }

    const subIdBytes = new TextEncoder().encode(subscriptionId)
    const filenameBytes = new TextEncoder().encode(filename)
    const plaintext = new Uint8Array(1 + 1 + subIdBytes.length + 2 + filenameBytes.length + data.length)

    let offset = 0
    plaintext[offset++] = CONTENT_FILE
    plaintext[offset++] = subIdBytes.length
    plaintext.set(subIdBytes, offset)
    offset += subIdBytes.length
    plaintext[offset++] = filenameBytes.length & 0xFF
    plaintext[offset++] = (filenameBytes.length >> 8) & 0xFF
    plaintext.set(filenameBytes, offset)
    offset += filenameBytes.length
    plaintext.set(data, offset)

    const maxMessageSize = conn.pc?.sctp?.maxMessageSize || 262144
    const chunkLimit = Math.max(maxMessageSize - 256, 16384)

    if (plaintext.length <= chunkLimit) {
      const wire = await this.#encodeDataChannelFrame(hubId, plaintext)
      conn.dataChannel.send(wire.buffer)
      return
    }

    const transferId = this.#channelProtocol.allocateFileTransferId(conn)
    const headerLen = 1 + 1 + subIdBytes.length + 2 + filenameBytes.length
    const header = plaintext.slice(1, headerLen)
    const fileData = plaintext.slice(headerLen)
    const dataChunkSize = chunkLimit - 4

    try {
      let pos = 0
      while (pos < fileData.length) {
        const isFirst = pos === 0
        const end = Math.min(pos + (isFirst ? dataChunkSize - header.length : dataChunkSize), fileData.length)
        const isLast = end >= fileData.length
        const flags = (isFirst ? 0x01 : 0) | (isLast ? 0x02 : 0)

        let chunk
        if (isFirst) {
          chunk = new Uint8Array(3 + header.length + (end - pos))
          chunk[0] = CONTENT_FILE_CHUNK
          chunk[1] = transferId
          chunk[2] = flags
          chunk.set(header, 3)
          chunk.set(fileData.slice(pos, end), 3 + header.length)
        } else {
          chunk = new Uint8Array(3 + (end - pos))
          chunk[0] = CONTENT_FILE_CHUNK
          chunk[1] = transferId
          chunk[2] = flags
          chunk.set(fileData.slice(pos, end), 3)
        }

        const wire = await this.#encodeDataChannelFrame(hubId, chunk)
        conn.dataChannel.send(wire.buffer)
        pos = end
      }
    } finally {
      conn.activeFileTransferIds.delete(transferId)
    }
  }

  getConnectionMode(hubId) {
    return this.#connections.get(hubId)?.mode || ConnectionMode.UNKNOWN
  }

  on(eventName, callback) {
    if (!this.#eventListeners.has(eventName)) {
      this.#eventListeners.set(eventName, new Set())
    }
    this.#eventListeners.get(eventName).add(callback)

    return () => {
      const listeners = this.#eventListeners.get(eventName)
      if (!listeners) return
      listeners.delete(callback)
    }
  }

  onSubscriptionMessage(subscriptionId, callback) {
    if (!this.#subscriptionListeners.has(subscriptionId)) {
      this.#subscriptionListeners.set(subscriptionId, new Set())
    }
    this.#subscriptionListeners.get(subscriptionId).add(callback)

    return () => {
      const listeners = this.#subscriptionListeners.get(subscriptionId)
      if (!listeners) return
      listeners.delete(callback)
    }
  }

  clearSubscriptionListeners(subscriptionId) {
    this.#subscriptionListeners.delete(subscriptionId)
    this.#channelProtocol.clearPendingSubscription(subscriptionId)
  }

  #cancelGracePeriod(hubId) {
    const timer = this.#graceTimers.get(hubId)
    if (!timer) return

    console.debug(`[WebRTCTransport] Cancelled grace period for hub ${hubId} (reacquired)`)
    clearTimeout(timer)
    this.#graceTimers.delete(hubId)
  }

  #closeConnection(hubId) {
    const conn = this.#connections.get(hubId)
    if (!conn) return

    console.debug(`[WebRTCTransport] Closing connection for hub ${hubId}`)
    clearTimeout(conn.transportConfigTimer)
    this.#handlePeerLost(hubId, PEER_LOST_REASONS.GRACE_EXPIRED)
    this.#signalingClient.disconnect(hubId)
    this.#connections.delete(hubId)
  }

  #handlePeerLost(hubId, reason) {
    const conn = this.#connections.get(hubId)
    if (!conn?.pc) {
      console.debug(`[WebRTCTransport] Ignoring duplicate peer lost for hub ${hubId} (${reason})`)
      return
    }

    this.#peerLifecycle.teardownPeer(conn)
    this.#peerConnectPromises.delete(hubId)
    this.#dataMessageChains.delete(hubId)
    this.#emit("connection:state", { hubId, state: "disconnected", reason })
  }

  #setupDataChannel(hubId, dataChannel, generation) {
    dataChannel.binaryType = "arraybuffer"

    dataChannel.onopen = () => {
      if (!this.#isCurrentPeerGeneration(hubId, generation)) return
      console.debug(`[WebRTCTransport] DataChannel open for hub ${hubId}`)
      const conn = this.#connections.get(hubId)
      let peerReadyMs = null
      if (conn) {
        conn.state = TransportState.CONNECTED
        conn.serverReady = false
        const timingStartedAt = conn.offerSentAt ?? conn.peerSetupStartedAt
        if (timingStartedAt != null) {
          peerReadyMs = Math.round(performance.now() - timingStartedAt)
          console.debug(
            `[WebRTCTransport] Peer ready for hub ${hubId} in ${peerReadyMs}ms`,
          )
        }
      }

      const mode = conn?.mode
      this.#emit("connection:state", { hubId, state: "connected", mode, peerReadyMs })
      if (mode) this.#emit("connection:mode", { hubId, mode })
    }

    dataChannel.onclose = () => {
      if (!this.#isCurrentPeerGeneration(hubId, generation)) return
      console.debug(`[WebRTCTransport] DataChannel closed for hub ${hubId}`)
      this.#handlePeerLost(hubId, PEER_LOST_REASONS.DATACHANNEL_CLOSE)
    }

    dataChannel.onerror = (error) => {
      if (!this.#isCurrentPeerGeneration(hubId, generation)) return
      console.error("[WebRTCTransport] DataChannel error:", error)
      this.#handlePeerLost(hubId, PEER_LOST_REASONS.DATACHANNEL_ERROR)
    }

    dataChannel.onmessage = (event) => {
      this.#enqueueDataChannelMessage(hubId, event.data, generation)
    }
  }

  #isCurrentPeerGeneration(hubId, generation) {
    const conn = this.#connections.get(hubId)
    return !!conn && conn.peerGeneration === generation
  }

  #markServerReady(hubId, generation) {
    if (!this.#isCurrentPeerGeneration(hubId, generation)) return

    const conn = this.#connections.get(hubId)
    if (conn) {
      conn.serverReady = true
    }

    const waiters = this.#serverReadyWaiters.get(hubId) || []
    this.#serverReadyWaiters.delete(hubId)
    for (const waiter of waiters) {
      clearTimeout(waiter.timer)
      waiter.resolve()
    }

    this.#emit("connection:server-ready", { hubId })
  }

  #waitForServerReady(hubId, conn, timeoutMs = 3000) {
    if (conn.serverReady) return Promise.resolve()

    return new Promise((resolve, reject) => {
      const waiter = {
        resolve,
        timer: setTimeout(() => {
          const waiters = this.#serverReadyWaiters.get(hubId) || []
          this.#serverReadyWaiters.set(
            hubId,
            waiters.filter((entry) => entry !== waiter),
          )
          reject(new Error(`DataChannel server ready timeout for hub ${hubId}`))
        }, timeoutMs),
      }

      const waiters = this.#serverReadyWaiters.get(hubId) || []
      waiters.push(waiter)
      this.#serverReadyWaiters.set(hubId, waiters)
    })
  }

  #enqueueDataChannelMessage(hubId, data, generation) {
    if (!this.#isCurrentPeerGeneration(hubId, generation)) return

    const previous = this.#dataMessageChains.get(hubId) || Promise.resolve()
    const current = previous
      .catch(() => {})
      .then(() => this.#channelProtocol.handleDataChannelMessage(hubId, data, generation))
      .catch((error) => {
        console.error("[WebRTCTransport] Message handler error:", error)
      })

    this.#dataMessageChains.set(hubId, current)
  }

  #emit(eventName, data) {
    const listeners = this.#eventListeners.get(eventName)
    if (listeners) {
      for (const callback of listeners) {
        try {
          callback(data)
        } catch (error) {
          console.error("[WebRTCTransport] Event listener error:", error)
        }
      }
    }

    if (eventName === "subscription:message" && data.subscriptionId) {
      const subListeners = this.#subscriptionListeners.get(data.subscriptionId)
      if (subListeners) {
        for (const callback of subListeners) {
          try {
            callback(data.message)
          } catch (error) {
            console.error("[WebRTCTransport] Subscription listener error:", error)
          }
        }
      }
    }
  }

  #hasFreshIceConfig(conn) {
    return !!conn?.iceConfig && (Date.now() - conn.iceConfigFetchedAt) < ICE_CONFIG_CACHE_TTL_MS
  }

  #prefetchIceConfig(hubId, conn) {
    if (!conn || conn.iceConfigPromise || this.#hasFreshIceConfig(conn)) return

    conn.iceConfigPromise = this.#fetchIceConfig(hubId)
      .then((iceConfig) => {
        conn.iceConfig = iceConfig
        conn.iceConfigFetchedAt = Date.now()
        return iceConfig
      })
      .catch((error) => {
        console.debug(`[WebRTCTransport] ICE prefetch failed for hub ${hubId}: ${error?.message || error}`)
        return null
      })
      .finally(() => {
        conn.iceConfigPromise = null
      })
  }

  async #getIceConfig(hubId, conn) {
    if (this.#hasFreshIceConfig(conn)) return conn.iceConfig

    if (conn.iceConfigPromise) {
      const prefetched = await conn.iceConfigPromise
      if (prefetched) return prefetched
      if (this.#hasFreshIceConfig(conn)) return conn.iceConfig
    }

    try {
      const iceConfig = await this.#fetchIceConfig(hubId)
      conn.iceConfig = iceConfig
      conn.iceConfigFetchedAt = Date.now()
      return iceConfig
    } catch (error) {
      if (conn.iceConfig) {
        console.warn(`[WebRTCTransport] ICE fetch failed for hub ${hubId}, using stale cached config:`, error)
        return conn.iceConfig
      }
      throw error
    }
  }

  async #fetchIceConfig(hubId) {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), ICE_CONFIG_FETCH_TIMEOUT_MS)

    let response
    try {
      response = await fetch(`/hubs/${hubId}/webrtc`, {
        credentials: "include",
        signal: controller.signal,
      })
    } catch (error) {
      if (error?.name === "AbortError") {
        throw new Error(`Failed to fetch ICE config: timeout after ${ICE_CONFIG_FETCH_TIMEOUT_MS}ms`)
      }
      throw error
    } finally {
      clearTimeout(timeout)
    }

    if (!response.ok) {
      throw new Error(`Failed to fetch ICE config: ${response.status}`)
    }

    return response.json()
  }

  async #createSignalingChannel(hubId, browserIdentity) {
    console.debug(`[WebRTCTransport] Creating signaling channel: hub=${hubId}, identity=${browserIdentity?.slice(0, 16)}...`)

    return this.#signalingClient.connect(hubId, browserIdentity, {
      onMessage: (data) => {
        this.#handleSignalingMessage(hubId, data)
      },
      onState: (state) => {
        const conn = this.#connections.get(hubId)
        if (!conn) return
        conn.signalingConnected = state === "connected"
        if (state === "connected") {
          this.#markTransportConfigPending(hubId, conn)
        }
      },
    })
  }

  async #handleSignalingMessage(hubId, data) {
    if (data.type === "health") {
      const conn = this.#connections.get(hubId)
      if (conn) conn.lastHealth = data
      if (data.cli === "offline" && conn) {
        this.#markTransportConfigPending(hubId, conn, false)
      } else if (data.cli === "online" && conn?.transportConfigPending) {
        this.#signalingClient.getSubscription(hubId)?.perform("request_transport_config", {})
        this.#scheduleTransportConfigFallback(hubId, conn)
      }
      this.#emit("health", { hubId, ...data })
      return
    }

    if (data.type !== "signal") return

    if (data.envelope?.type === "transport_config") {
      const conn = this.#connections.get(hubId)
      if (!conn?.transportConfigPending) return
      clearTimeout(conn.transportConfigTimer)
      conn.transportConfigTimer = null
      const nextEncryptionEnabled = data.envelope.e2e_enabled !== false
      if (!nextEncryptionEnabled && !conn.allowPlaintext) {
        conn.transportConfigPending = false
        this.#emit("transport:config", {
          hubId,
          e2e_enabled: null,
          error: "plaintext_opt_in_required",
        })
        return
      }

      const previousEncryptionEnabled = conn.encryptionEnabled
      conn.transportConfigPending = false
      conn.encryptionEnabled = nextEncryptionEnabled
      if (
        typeof previousEncryptionEnabled === "boolean" &&
        previousEncryptionEnabled !== nextEncryptionEnabled &&
        conn.pc
      ) {
        this.#handlePeerLost(hubId, "transport_config_changed")
      }
      this.#emit("transport:config", {
        hubId,
        e2e_enabled: conn.encryptionEnabled,
      })
      return
    }

    if (data.envelope?.t === 2 && data.envelope?.b) {
      console.debug("[WebRTCTransport] Received bundle refresh from CLI via ActionCable")
      try {
        const bundleBytes = base64ToBytes(data.envelope.b)
        const bundle = parseBinaryBundle(bundleBytes)
        const conn = this.#connections.get(hubId)
        await bridge.createSession(String(hubId), bundle, conn?.browserIdentity || null)
        if (conn) conn.decryptFailures = 0
        this.#emit("session:refreshed", { hubId })
      } catch (error) {
        console.error("[WebRTCTransport] Bundle refresh via AC failed:", error.message)
        this.#emit("session:invalid", { hubId, message: error.message })
      }
      return
    }

    try {
      const decrypted = await this.#decryptSignalEnvelope(hubId, data.envelope)
      if (!decrypted) return

      if (decrypted.type === "answer") {
        console.debug("[WebRTCTransport] Received answer via ActionCable")
        await this.#peerLifecycle.handleAnswer(hubId, decrypted.sdp)
        return
      }

      if (decrypted.type === "ice") {
        console.debug("[WebRTCTransport] Received ICE candidate via ActionCable")
        await this.#peerLifecycle.handleIceCandidate(hubId, decrypted.candidate)
        return
      }

      if (decrypted.type === "ice_batch") {
        console.debug(`[WebRTCTransport] Received ${decrypted.candidates?.length || 0} batched ICE candidates via ActionCable`)
        for (const candidate of (decrypted.candidates || [])) {
          await this.#peerLifecycle.handleIceCandidate(hubId, candidate)
        }
      }
    } catch (error) {
      console.error("[WebRTCTransport] Signal decryption/handling error:", error)
    }
  }

  async #decryptSignalEnvelope(hubId, envelope) {
    if (this.#connections.get(hubId)?.encryptionEnabled === false) {
      return envelope
    }
    try {
      const { plaintext } = await bridge.decrypt(String(hubId), envelope)
      return typeof plaintext === "string" ? JSON.parse(plaintext) : plaintext
    } catch (error) {
      console.error("[WebRTCTransport] Signal decryption failed:", error.message || error)
      return null
    }
  }

  async #encryptSignal(hubId, payload) {
    if (this.#connections.get(hubId)?.encryptionEnabled === false) {
      return payload
    }
    const { encrypted } = await bridge.encrypt(String(hubId), payload)
    return encrypted
  }

  async #encodeDataChannelFrame(hubId, plaintext) {
    const bytes = plaintext instanceof Uint8Array ? plaintext : new Uint8Array(plaintext)
    if (this.#connections.get(hubId)?.encryptionEnabled === false) {
      return bytes
    }
    const { data } = await bridge.encryptBinary(String(hubId), bytes)
    return data instanceof Uint8Array ? data : new Uint8Array(data)
  }

  #markTransportConfigPending(hubId, conn, scheduleFallback = true) {
    clearTimeout(conn.transportConfigTimer)
    conn.transportConfigTimer = null
    conn.transportConfigPending = true
    this.#emit("transport:config", { hubId, e2e_enabled: null })
    if (scheduleFallback) this.#scheduleTransportConfigFallback(hubId, conn)
  }

  #scheduleTransportConfigFallback(hubId, conn) {
    if (conn.transportConfigTimer || !conn.transportConfigPending) return
    conn.transportConfigTimer = setTimeout(() => {
      conn.transportConfigTimer = null
      if (!conn.transportConfigPending) return
      conn.encryptionEnabled ??= true
      this.#emit("transport:config", {
        hubId,
        e2e_enabled: conn.encryptionEnabled,
        fallback: true,
      })
    }, TRANSPORT_CONFIG_FALLBACK_MS)
  }
}

export { HubPeerConnection }
export default HubPeerConnection.instance
