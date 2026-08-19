import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

const mocks = vi.hoisted(() => {
  const subscriptions = {
    create: vi.fn((_params, callbacks) => ({
      perform: vi.fn(),
      unsubscribe: vi.fn(),
      ...callbacks,
    })),
  }

  return {
    subscriptions,
    createConsumer: vi.fn(() => ({
      connection: {
        webSocket: {},
        isOpen: () => true,
        isActive: () => true,
        open: vi.fn(),
        close: vi.fn(),
        installEventHandlers: vi.fn(),
      },
      subscriptions,
    })),
    bridge: {
      createSession: vi.fn(),
      decryptBinary: vi.fn(),
      encryptBinary: vi.fn((_hubId, data) => Promise.resolve({ data })),
      decrypt: vi.fn(),
      encrypt: vi.fn(() => Promise.resolve({ encrypted: { t: 1, b: "signal" } })),
    },
  }
})

vi.mock("@rails/actioncable", () => ({
  createConsumer: mocks.createConsumer,
}))

vi.mock("workers/bridge", () => ({
  default: mocks.bridge,
}))

class MockDataChannel {
  constructor() {
    this.readyState = "connecting"
    this.binaryType = ""
    this.onopen = null
    this.onclose = null
    this.onerror = null
    this.onmessage = null
    this.send = vi.fn()
  }
}

class MockRTCPeerConnection {
  static instances = []

  constructor() {
    this.connectionState = "new"
    this.iceConnectionState = "new"
    this.onicecandidate = null
    this.oniceconnectionstatechange = null
    this.onconnectionstatechange = null
    this.dataChannel = new MockDataChannel()
    this.close = vi.fn(() => {
      this.connectionState = "closed"
    })
    this.createDataChannel = vi.fn(() => this.dataChannel)
    this.createOffer = vi.fn(() => Promise.resolve({ type: "offer", sdp: "offer-sdp" }))
    this.setLocalDescription = vi.fn(() => Promise.resolve())
    this.setRemoteDescription = vi.fn(() => Promise.resolve())
    this.addIceCandidate = vi.fn(() => Promise.resolve())
    this.restartIce = vi.fn()
    this.getStats = vi.fn(() => Promise.resolve(new Map()))
    MockRTCPeerConnection.instances.push(this)
  }
}

async function flushPromises() {
  await Promise.resolve()
  await Promise.resolve()
}

describe("HubPeerConnection peer lost transitions", () => {
  let HubPeerConnection
  let transport
  let events

  beforeEach(async () => {
    vi.useFakeTimers()
    vi.spyOn(console, "debug").mockImplementation(() => {})
    vi.spyOn(console, "error").mockImplementation(() => {})
    vi.spyOn(console, "warn").mockImplementation(() => {})
    MockRTCPeerConnection.instances = []
    globalThis.RTCPeerConnection = MockRTCPeerConnection
    globalThis.RTCSessionDescription = class {
      constructor(description) {
        Object.assign(this, description)
      }
    }
    globalThis.RTCIceCandidate = class {
      constructor(candidate) {
        Object.assign(this, candidate)
      }
    }
    globalThis.fetch = vi.fn(() => Promise.resolve({
      ok: true,
      json: () => Promise.resolve({ ice_servers: [] }),
    }))
    mocks.subscriptions.create.mockClear()
    mocks.bridge.encrypt.mockClear()
    mocks.bridge.encryptBinary.mockClear()
    mocks.bridge.decryptBinary.mockClear()

    ;({ HubPeerConnection } = await import("../lib/transport/hub_peer_connection"))
    transport = new HubPeerConnection()
    events = []
    transport.on("connection:state", (event) => events.push(event))
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
    delete globalThis.RTCPeerConnection
    delete globalThis.RTCSessionDescription
    delete globalThis.RTCIceCandidate
    delete globalThis.fetch
  })

  async function connectPeer(hubId = "hub-1") {
    await transport.connectSignaling(hubId, "browser-identity")
    await transport.connectPeer(hubId)
    return MockRTCPeerConnection.instances.at(-1)
  }

  function disconnectedEvents() {
    return events.filter((event) => event.state === "disconnected")
  }

  function connectedEvents() {
    return events.filter((event) => event.state === "connected")
  }

  function encodeControlMessage(message) {
    const body = new TextEncoder().encode(JSON.stringify(message))
    const plaintext = new Uint8Array(1 + body.length)
    plaintext[0] = 0
    plaintext.set(body, 1)
    return plaintext
  }

  function encodePtyMessage(subscriptionId, payload) {
    const subBytes = new TextEncoder().encode(subscriptionId)
    const payloadBytes = new TextEncoder().encode(payload)
    const plaintext = new Uint8Array(3 + subBytes.length + payloadBytes.length)
    plaintext[0] = 1
    plaintext[1] = 0
    plaintext[2] = subBytes.length
    plaintext.set(subBytes, 3)
    plaintext.set(payloadBytes, 3 + subBytes.length)
    return plaintext
  }

  it("emits peer-ready timing when the data channel opens", async () => {
    const pc = await connectPeer()

    pc.dataChannel.onopen()

    expect(connectedEvents()).toHaveLength(1)
    expect(connectedEvents()[0]).toMatchObject({
      hubId: "hub-1",
      state: "connected",
      peerReadyMs: expect.any(Number),
    })
    expect(connectedEvents()[0].peerReadyMs).toBeGreaterThanOrEqual(0)
  })

  it("uses plaintext signaling and data after the health handshake disables encryption", async () => {
    await transport.connectSignaling("hub-plain", "anon:tab", true)
    const subscription = mocks.subscriptions.create.mock.results.at(-1).value
    subscription.received({
      type: "signal",
      envelope: { type: "transport_config", e2e_enabled: false },
    })

    await transport.connectPeer("hub-plain")
    const pc = MockRTCPeerConnection.instances.at(-1)

    expect(mocks.bridge.encrypt).not.toHaveBeenCalled()
    expect(subscription.perform).toHaveBeenCalledWith("signal", {
      envelope: { type: "offer", sdp: "offer-sdp" },
    })

    pc.dataChannel.readyState = "open"
    pc.dataChannel.onmessage({ data: encodeControlMessage({ type: "dc_ready" }) })
    await flushPromises()

    const subscribe = transport.subscribe(
      "hub-plain",
      "terminal",
      {},
      "sub-plain",
      encodeControlMessage({
        type: "subscribe",
        subscriptionId: "sub-plain",
        channel: "terminal",
        params: {},
      }),
    )
    await flushPromises()
    pc.dataChannel.onmessage({
      data: encodeControlMessage({ type: "subscribed", subscriptionId: "sub-plain" }),
    })

    await expect(subscribe).resolves.toEqual({ subscriptionId: "sub-plain" })
    expect(mocks.bridge.encryptBinary).not.toHaveBeenCalled()
    expect(mocks.bridge.decryptBinary).not.toHaveBeenCalled()
  })

  it("requires fresh transport config after signaling reconnects", async () => {
    const configs = []
    transport.on("transport:config", (event) => configs.push(event))
    await transport.connectSignaling("hub-config", "browser-identity", true)
    const subscription = mocks.subscriptions.create.mock.results.at(-1).value

    subscription.received({
      type: "signal",
      envelope: { type: "transport_config", e2e_enabled: false },
    })
    subscription.connected()

    expect(configs).toEqual([
      { hubId: "hub-config", e2e_enabled: false },
      { hubId: "hub-config", e2e_enabled: null },
    ])
  })

  it("rejects plaintext mode for a paired browser without explicit opt-in", async () => {
    const configs = []
    transport.on("transport:config", (event) => configs.push(event))
    await transport.connectSignaling("hub-paired", "browser-identity")
    const subscription = mocks.subscriptions.create.mock.results.at(-1).value

    subscription.received({
      type: "signal",
      envelope: { type: "transport_config", e2e_enabled: false },
    })

    expect(configs.at(-1)).toEqual({
      hubId: "hub-paired",
      e2e_enabled: null,
      error: "plaintext_opt_in_required",
    })
    expect(MockRTCPeerConnection.instances).toHaveLength(0)
  })

  it("tears down the peer when a pending transport mode changes", async () => {
    await transport.connectSignaling("hub-change", "browser-identity", true)
    const subscription = mocks.subscriptions.create.mock.results.at(-1).value
    subscription.received({
      type: "signal",
      envelope: { type: "transport_config", e2e_enabled: false },
    })
    await transport.connectPeer("hub-change")
    const pc = MockRTCPeerConnection.instances.at(-1)

    subscription.connected()
    subscription.received({
      type: "signal",
      envelope: { type: "transport_config", e2e_enabled: true },
    })

    expect(pc.close).toHaveBeenCalledOnce()
    expect(disconnectedEvents().at(-1)).toMatchObject({
      hubId: "hub-change",
      reason: "transport_config_changed",
    })
  })

  it("emits subscription-ready timing after data channel subscribe ack", async () => {
    const pc = await connectPeer()
    const readyEvents = []
    transport.on("subscription:ready", (event) => readyEvents.push(event))
    pc.dataChannel.readyState = "open"

    mocks.bridge.decryptBinary.mockResolvedValueOnce({
      data: encodeControlMessage({ type: "dc_ready" }),
    })
    pc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    await flushPromises()

    const subscribe = transport.subscribe("hub-1", "terminal", {}, "sub-1", new Uint8Array([1, 2, 3]))
    await flushPromises()

    mocks.bridge.decryptBinary.mockResolvedValueOnce({
      data: encodeControlMessage({ type: "subscribed", subscriptionId: "sub-1" }),
    })
    pc.dataChannel.onmessage({ data: new Uint8Array([0]) })

    await expect(subscribe).resolves.toEqual({ subscriptionId: "sub-1" })
    expect(readyEvents).toHaveLength(1)
    expect(readyEvents[0]).toMatchObject({
      hubId: "hub-1",
      subscriptionId: "sub-1",
      subscribeReadyMs: expect.any(Number),
    })
    expect(readyEvents[0].subscribeReadyMs).toBeGreaterThanOrEqual(0)
  })

  it("registers subscription confirmation before sending subscribe frame", async () => {
    const pc = await connectPeer()
    pc.dataChannel.readyState = "open"

    mocks.bridge.decryptBinary.mockResolvedValueOnce({
      data: encodeControlMessage({ type: "dc_ready" }),
    })
    pc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    await flushPromises()

    mocks.bridge.decryptBinary.mockResolvedValueOnce({
      data: encodeControlMessage({ type: "subscribed", subscriptionId: "sub-fast" }),
    })
    pc.dataChannel.send.mockImplementationOnce(() => {
      pc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    })

    await expect(
      transport.subscribe("hub-1", "terminal", {}, "sub-fast", new Uint8Array([1, 2, 3])),
    ).resolves.toEqual({ subscriptionId: "sub-fast" })
  })

  it("clears pending subscription confirmation when subscribe send throws", async () => {
    const pc = await connectPeer()
    pc.dataChannel.readyState = "open"

    mocks.bridge.decryptBinary.mockResolvedValueOnce({
      data: encodeControlMessage({ type: "dc_ready" }),
    })
    pc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    await flushPromises()

    pc.dataChannel.send.mockImplementationOnce(() => {
      throw new Error("send failed")
    })

    await expect(
      transport.subscribe("hub-1", "terminal", {}, "sub-send-failed", new Uint8Array([1, 2, 3])),
    ).rejects.toThrow("send failed")

    await vi.advanceTimersByTimeAsync(10_000)
  })

  it("ignores dc_ready from an old peer generation after reconnect", async () => {
    const serverReadyEvents = []
    transport.on("connection:server-ready", (event) => serverReadyEvents.push(event))
    const firstPc = await connectPeer()
    let resolveOldDecrypt
    const oldDecrypt = new Promise((resolve) => {
      resolveOldDecrypt = resolve
    })

    mocks.bridge.decryptBinary.mockReturnValueOnce(oldDecrypt)
    firstPc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    await flushPromises()

    firstPc.dataChannel.onclose()
    const secondPc = await connectPeer()
    resolveOldDecrypt({
      data: encodeControlMessage({ type: "dc_ready" }),
    })
    await flushPromises()

    expect(serverReadyEvents).toEqual([])

    mocks.bridge.decryptBinary.mockResolvedValueOnce({
      data: encodeControlMessage({ type: "dc_ready" }),
    })
    secondPc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    await flushPromises()

    expect(serverReadyEvents).toEqual([{ hubId: "hub-1" }])
  })

  it("ignores decrypted binary payloads from an old peer generation after reconnect", async () => {
    const subscriptionMessages = []
    transport.on("subscription:message", (event) => subscriptionMessages.push(event))
    const firstPc = await connectPeer()
    let resolveOldDecrypt
    const oldDecrypt = new Promise((resolve) => {
      resolveOldDecrypt = resolve
    })

    mocks.bridge.decryptBinary.mockReturnValueOnce(oldDecrypt)
    firstPc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    await flushPromises()

    firstPc.dataChannel.onclose()
    const secondPc = await connectPeer()
    resolveOldDecrypt({
      data: encodePtyMessage("sub-old", "old-output"),
    })
    await flushPromises()

    expect(subscriptionMessages).toEqual([])

    mocks.bridge.decryptBinary.mockResolvedValueOnce({
      data: encodePtyMessage("sub-new", "new-output"),
    })
    secondPc.dataChannel.onmessage({ data: new Uint8Array([0]) })
    await flushPromises()

    expect(subscriptionMessages).toHaveLength(1)
    expect(subscriptionMessages[0].subscriptionId).toBe("sub-new")
    expect(new TextDecoder().decode(subscriptionMessages[0].message)).toBe("new-output")
  })

  it("emits one datachannel_close event and tears down peer timers", async () => {
    const pc = await connectPeer()
    const dc = pc.dataChannel

    dc.onclose()

    expect(disconnectedEvents()).toEqual([
      { hubId: "hub-1", state: "disconnected", reason: "datachannel_close" },
    ])
    expect(pc.close).toHaveBeenCalledTimes(1)
    expect(pc.onconnectionstatechange).toBeNull()
    expect(dc.onclose).toBeNull()
    expect(vi.getTimerCount()).toBe(0)
  })

  it("emits once when onclose is followed by pc closed", async () => {
    const pc = await connectPeer()
    const onConnectionStateChange = pc.onconnectionstatechange

    pc.dataChannel.onclose()
    pc.connectionState = "closed"
    onConnectionStateChange()

    expect(disconnectedEvents()).toHaveLength(1)
    expect(disconnectedEvents()[0].reason).toBe("datachannel_close")
  })

  it("emits once when onerror is followed by onclose", async () => {
    const pc = await connectPeer()
    const onclose = pc.dataChannel.onclose

    pc.dataChannel.onerror(new Error("boom"))
    onclose()

    expect(disconnectedEvents()).toHaveLength(1)
    expect(disconnectedEvents()[0].reason).toBe("datachannel_error")
  })

  it("peer setup timer emits peer_setup_timeout and clears peer connect promise", async () => {
    await transport.connectSignaling("hub-1", "browser-identity")
    const firstConnect = transport.connectPeer("hub-1")
    await flushPromises()
    const firstPc = MockRTCPeerConnection.instances.at(-1)

    await vi.advanceTimersByTimeAsync(15_000)
    await expect(firstConnect).resolves.toEqual({ state: "connecting" })

    expect(disconnectedEvents()).toEqual([
      { hubId: "hub-1", state: "disconnected", reason: "peer_setup_timeout" },
    ])

    await transport.connectPeer("hub-1")
    expect(MockRTCPeerConnection.instances.at(-1)).not.toBe(firstPc)
  })

  it("probePeerHealth cleans failed pc, and later disconnectPeer emits no second event", async () => {
    const pc = await connectPeer()
    pc.connectionState = "failed"

    expect(transport.probePeerHealth("hub-1")).toEqual({
      alive: false,
      pcState: "failed",
      dcState: "connecting",
    })
    transport.disconnectPeer("hub-1")

    expect(disconnectedEvents()).toHaveLength(1)
    expect(disconnectedEvents()[0].reason).toBe("probe_dead")
  })

  it("explicit disconnectPeer emits explicit_disconnect", async () => {
    await connectPeer()

    transport.disconnectPeer("hub-1")

    expect(disconnectedEvents()).toEqual([
      { hubId: "hub-1", state: "disconnected", reason: "explicit_disconnect" },
    ])
  })

  it("keeps signaling and connection entry after peer lost, then starts a fresh peer attempt", async () => {
    const pc = await connectPeer()
    const subscription = mocks.subscriptions.create.mock.results[0].value

    pc.dataChannel.onclose()
    await transport.connectPeer("hub-1")

    expect(subscription.unsubscribe).not.toHaveBeenCalled()
    expect(MockRTCPeerConnection.instances).toHaveLength(2)
    expect(disconnectedEvents()).toHaveLength(1)
  })

  it("clears a stale peer on connect and starts a fresh attempt", async () => {
    const pc = await connectPeer()
    pc.connectionState = "failed"

    await transport.connectPeer("hub-1")

    expect(disconnectedEvents()).toEqual([
      { hubId: "hub-1", state: "disconnected", reason: "stale_peer_on_connect" },
    ])
    expect(MockRTCPeerConnection.instances).toHaveLength(2)
    expect(MockRTCPeerConnection.instances.at(-1)).not.toBe(pc)
  })

  it("ICE stuck disconnected emits ice_disconnect_stuck", async () => {
    const pc = await connectPeer()

    pc.iceConnectionState = "disconnected"
    pc.oniceconnectionstatechange()
    await vi.advanceTimersByTimeAsync(5_000)

    expect(disconnectedEvents()).toEqual([
      { hubId: "hub-1", state: "disconnected", reason: "ice_disconnect_stuck" },
    ])
  })
})
