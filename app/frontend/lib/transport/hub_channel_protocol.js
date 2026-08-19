import { parseBinaryBundle } from "matrix/bundle"

export class HubChannelProtocol {
  #pendingSubscriptions = new Map()
  #callbacks
  #constants

  constructor({ callbacks, constants }) {
    this.#callbacks = callbacks
    this.#constants = constants
  }

  #isCurrentPeerGeneration(hubId, generation) {
    if (typeof this.#callbacks.isCurrentPeerGeneration !== "function") return true
    return this.#callbacks.isCurrentPeerGeneration(hubId, generation)
  }

  clearPendingSubscription(subscriptionId) {
    const pending = this.#pendingSubscriptions.get(subscriptionId)
    if (pending?.timeout) {
      clearTimeout(pending.timeout)
    }
    this.#pendingSubscriptions.delete(subscriptionId)
  }

  async waitForDataChannel(dataChannel) {
    if (dataChannel?.readyState === "open") return
    if (!dataChannel || dataChannel.readyState === "closed" || dataChannel.readyState === "closing") {
      throw new Error("DataChannel closed")
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup()
        reject(new Error("DataChannel timeout"))
      }, 30000)

      const cleanup = () => {
        clearTimeout(timeout)
        dataChannel.removeEventListener("open", onOpen)
        dataChannel.removeEventListener("close", onClose)
        dataChannel.removeEventListener("error", onClose)
      }

      const onOpen = () => {
        cleanup()
        resolve()
      }

      const onClose = () => {
        cleanup()
        reject(new Error("DataChannel closed"))
      }

      dataChannel.addEventListener("open", onOpen)
      dataChannel.addEventListener("close", onClose)
      dataChannel.addEventListener("error", onClose)
    })
  }

  async waitForSubscriptionConfirmed(subscriptionId) {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.#pendingSubscriptions.delete(subscriptionId)
        reject(new Error(`Subscription confirmation timeout for ${subscriptionId}`))
      }, 10000)

      this.#pendingSubscriptions.set(subscriptionId, {
        resolve: () => {
          clearTimeout(timeout)
          this.#pendingSubscriptions.delete(subscriptionId)
          resolve()
        },
        reject,
        timeout,
      })
    })
  }

  handleSubscriptionConfirmed(subscriptionId) {
    const pending = this.#pendingSubscriptions.get(subscriptionId)
    if (pending) {
      console.debug(`[WebRTCTransport] Subscription confirmed: ${subscriptionId}`)
      pending.resolve()
    }
  }

  allocateFileTransferId(conn) {
    for (let i = 0; i < 256; i++) {
      const candidate = conn.nextFileTransferId & 0xFF
      conn.nextFileTransferId = (candidate + 1) & 0xFF

      if (!conn.activeFileTransferIds.has(candidate)) {
        conn.activeFileTransferIds.add(candidate)
        return candidate
      }
    }

    throw new Error("Too many concurrent file transfers")
  }

  async handleDataChannelMessage(hubId, data, generation) {
    const {
      CONTENT_MSG,
      CONTENT_PTY,
      CONTENT_STREAM,
      MSG_TYPE_BUNDLE_REFRESH,
    } = this.#constants

    try {
      if (!this.#isCurrentPeerGeneration(hubId, generation)) return

      const raw = data instanceof ArrayBuffer ? new Uint8Array(data) : new Uint8Array(data.buffer || data)

      const conn = this.#callbacks.getConnection(hubId)
      const encryptionEnabled = conn?.encryptionEnabled !== false

      if (encryptionEnabled && raw.length > 0 && raw[0] === MSG_TYPE_BUNDLE_REFRESH) {
        const bundleBytes = raw.slice(1)
        console.debug("[WebRTCTransport] Received bundle refresh from CLI via DataChannel")
        try {
          const bundle = parseBinaryBundle(bundleBytes)
          const conn = this.#callbacks.getConnection(hubId)
          await this.#callbacks.createSession(hubId, bundle, conn?.browserIdentity || null)
          if (conn) conn.decryptFailures = 0
          this.#callbacks.emit("session:refreshed", { hubId })
        } catch (error) {
          console.error("[WebRTCTransport] Bundle refresh failed:", error.message)
          this.#callbacks.emit("session:invalid", { hubId, message: error.message })
        }
        return
      }

      if (raw.length > 0) {
        let plaintext
        if (!encryptionEnabled) {
          plaintext = raw
        } else {
          try {
            const result = await this.#callbacks.decryptBinary(hubId, raw)
            plaintext = result.data

            if (!this.#isCurrentPeerGeneration(hubId, generation)) return

            if (conn) conn.decryptFailures = 0
          } catch (error) {
            console.error("[WebRTCTransport] Olm decryption failed:", error.message || error)
            return
          }
        }

        if (!plaintext || plaintext.length === 0) return

        const contentType = plaintext[0]

        if (contentType === CONTENT_MSG) {
          const json = new TextDecoder().decode(plaintext.slice(1))
          const msg = JSON.parse(json)
          this.#routeControlMessage(hubId, msg, generation)
          return
        }

        if (contentType === CONTENT_PTY) {
          await this.#handlePtyBinary(plaintext)
          return
        }

        if (contentType === CONTENT_STREAM) {
          if (plaintext.length < 4) return
          const frameType = plaintext[1]
          const streamId = (plaintext[2] << 8) | plaintext[3]
          const payload = plaintext.slice(4)
          this.#callbacks.emit("stream:frame", { hubId, frameType, streamId, payload })
          return
        }

        console.warn("[WebRTCTransport] Unknown content type:", contentType)
        return
      }

    } catch (error) {
      console.error("[WebRTCTransport] Failed to handle message:", error)
    }
  }

  async #handlePtyBinary(plaintext) {
    if (plaintext.length < 4) return

    const flags = plaintext[1]
    const compressed = (flags & 0x01) !== 0
    const subIdLen = plaintext[2]
    const subIdStart = 3
    const payloadStart = subIdStart + subIdLen

    if (plaintext.length < payloadStart) return

    const subscriptionId = new TextDecoder().decode(plaintext.slice(subIdStart, payloadStart))
    const payload = plaintext.slice(payloadStart)

    let rawBytes
    if (compressed) {
      const stream = new Blob([payload])
        .stream()
        .pipeThrough(new DecompressionStream("gzip"))
      rawBytes = new Uint8Array(await new Response(stream).arrayBuffer())
    } else {
      rawBytes = payload instanceof Uint8Array ? payload : new Uint8Array(payload)
    }

    this.#callbacks.emit("subscription:message", {
      subscriptionId,
      message: rawBytes,
    })
  }

  #routeControlMessage(hubId, msg, generation) {
    if (!this.#isCurrentPeerGeneration(hubId, generation)) return

    if (msg.type === "subscribed" && msg.subscriptionId) {
      this.handleSubscriptionConfirmed(msg.subscriptionId)
      return
    }

    if (msg.type === "dc_ready") {
      this.#callbacks.markServerReady(hubId, generation)
      return
    }

    if (msg.type === "vapid_pub") {
      this.#callbacks.emit("push:vapid_key", { hubId, key: msg.key })
      return
    }
    if (msg.type === "push_sub_ack") {
      this.#callbacks.emit("push:sub_ack", { hubId })
      return
    }
    if (msg.type === "vapid_keys") {
      this.#callbacks.emit("push:vapid_keys", { hubId, pub: msg.pub, priv: msg.priv })
      return
    }
    if (msg.type === "push_test_ack") {
      this.#callbacks.emit("push:test_ack", { hubId, sent: msg.sent })
      return
    }
    if (msg.type === "push_disable_ack") {
      this.#callbacks.emit("push:disable_ack", { hubId })
      return
    }
    if (msg.type === "push_status") {
      this.#callbacks.emit("push:status", {
        hubId,
        hasKeys: msg.has_keys,
        browserSubscribed: msg.browser_subscribed,
        vapidPub: msg.vapid_pub,
      })
      return
    }

    if (msg.subscriptionId) {
      this.#callbacks.emit("subscription:message", {
        subscriptionId: msg.subscriptionId,
        message: msg.data || msg,
      })
      return
    }

    if (msg.type === "health" || msg.type === "dc_ping" || msg.type === "dc_pong") {
      const conn = this.#callbacks.getConnection(hubId)
      if (!conn) return

      for (const subId of conn.subscriptions.keys()) {
        this.#callbacks.emit("subscription:message", {
          subscriptionId: subId,
          message: msg,
        })
      }
    }
  }
}
