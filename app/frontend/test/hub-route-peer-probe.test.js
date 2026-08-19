import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => {
  const listeners = new Map();
  const subscriptionListeners = new Map();
  const sendCalls = [];

  const bridge = {
    hasPairing: vi.fn(() => Promise.resolve({ hasPairing: true })),
    getIdentityKey: vi.fn(() => Promise.resolve({ identityKey: "browser-key" })),
    hasSession: vi.fn(() => Promise.resolve({ hasSession: true })),
    encryptBinary: vi.fn(() => Promise.resolve({ data: new Uint8Array([1, 2, 3]) })),
    send: vi.fn((type, payload) => {
      sendCalls.push([type, payload]);
      if (type === "connectSignaling") {
        queueMicrotask(() => {
          bridge.emit("transport:config", { hubId: payload.hubId, e2e_enabled: true });
          bridge.emit("health", { hubId: payload.hubId, cli: "online" });
        });
        return Promise.resolve({
          state: "connected",
          browserSocketState: "connected",
          mode: "direct",
        });
      }
      if (type === "probePeerHealth") {
        return Promise.resolve({ alive: true, pcState: "connected", dcState: "open" });
      }
      return Promise.resolve({});
    }),
    on: vi.fn((event, callback) => {
      if (!listeners.has(event)) listeners.set(event, new Set());
      listeners.get(event).add(callback);
      return () => listeners.get(event)?.delete(callback);
    }),
    emit(event, payload) {
      for (const callback of listeners.get(event) || []) {
        callback(payload);
      }
    },
    onSubscriptionMessage: vi.fn((subscriptionId, callback) => {
      subscriptionListeners.set(subscriptionId, callback);
      return () => subscriptionListeners.delete(subscriptionId);
    }),
    emitSubscription(subscriptionId, message) {
      subscriptionListeners.get(subscriptionId)?.(message);
    },
    clearSubscriptionListeners: vi.fn((subscriptionId) => {
      subscriptionListeners.delete(subscriptionId);
    }),
    reset() {
      listeners.clear();
      subscriptionListeners.clear();
      sendCalls.length = 0;
      bridge.hasPairing.mockReset();
      bridge.hasPairing.mockResolvedValue({ hasPairing: true });
      bridge.getIdentityKey.mockClear();
      bridge.hasSession.mockClear();
      bridge.encryptBinary.mockClear();
      bridge.send.mockReset();
      bridge.send.mockImplementation((type, payload) => {
        sendCalls.push([type, payload]);
        if (type === "connectSignaling") {
          queueMicrotask(() => {
            bridge.emit("transport:config", { hubId: payload.hubId, e2e_enabled: true });
            bridge.emit("health", { hubId: payload.hubId, cli: "online" });
          });
          return Promise.resolve({
            state: "connected",
            browserSocketState: "connected",
            mode: "direct",
          });
        }
        if (type === "probePeerHealth") {
          return Promise.resolve({ alive: true, pcState: "connected", dcState: "open" });
        }
        return Promise.resolve({});
      });
      bridge.on.mockClear();
      bridge.onSubscriptionMessage.mockClear();
      bridge.clearSubscriptionListeners.mockClear();
    },
    sendCalls,
  };

  return {
    bridge,
    ensureMatrixReady: vi.fn(() => Promise.resolve()),
    observeBrowserSocketState: vi.fn((callback) => {
      callback("connected");
      return Promise.resolve(() => {});
    }),
  };
});

vi.mock("workers/bridge", () => ({
  default: mocks.bridge,
}));

vi.mock("matrix/bundle", () => ({
  ensureMatrixReady: mocks.ensureMatrixReady,
}));

vi.mock("transport/hub_signaling_client", () => ({
  observeBrowserSocketState: mocks.observeBrowserSocketState,
}));

async function flushPromises() {
  await Promise.resolve();
  await Promise.resolve();
}

describe("HubRoute peer health probes", () => {
  let HubRoute;
  let TestHubRoute;
  let route;
  let manager;

  beforeEach(async () => {
    vi.useFakeTimers();
    vi.spyOn(console, "warn").mockImplementation(() => {});
    vi.spyOn(console, "error").mockImplementation(() => {});
    mocks.bridge.reset();
    document.body.innerHTML = "";
    window.localStorage.clear();
    window.location.hash = "";

    ({ HubRoute } = await import("../lib/connections/hub_route"));
    TestHubRoute = class TestHubRoute extends HubRoute {
      channelName() {
        return "hub";
      }

      computeSubscriptionId() {
        return "hub:hub-1";
      }

      channelParams() {
        return { hub_id: this.getHubId() };
      }
    };

    manager = {
      hasActiveConnectionForHub: vi.fn(() => true),
      notifySubscribers: vi.fn(),
      release: vi.fn(),
    };
    route = new TestHubRoute("hub-1", { hubId: "hub-1" }, manager);
    await route.initialize();
    await flushPromises();
  });

  afterEach(() => {
    route?.destroy();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  function disconnectPeerCalls() {
    return mocks.bridge.sendCalls.filter(([type]) => type === "disconnectPeer");
  }

  async function emitStallAndWaitForProbe() {
    mocks.bridge.emit("connection:stalled", { hubId: "hub-1" });
    await flushPromises();
    await vi.advanceTimersByTimeAsync(1501);
    await flushPromises();
  }

  it("keeps an open local peer after one missed encrypted pong", async () => {
    await emitStallAndWaitForProbe();

    expect(disconnectPeerCalls()).toHaveLength(0);
    expect(route.subscriptionId).toBe("hub:hub-1");
  });

  it("connects an unpaired browser when the live hub disables encryption", async () => {
    route.destroy();
    route = null;
    mocks.bridge.reset();
    mocks.bridge.hasPairing.mockResolvedValue({ hasPairing: false });
    window.location.hash = "#no-encryption";
    mocks.bridge.send.mockImplementation((type, payload) => {
      mocks.bridge.sendCalls.push([type, payload]);
      if (type === "connectSignaling") {
        queueMicrotask(() => {
          mocks.bridge.emit("transport:config", {
            hubId: payload.hubId,
            e2e_enabled: false,
          });
          mocks.bridge.emit("health", { hubId: payload.hubId, cli: "online" });
        });
        return Promise.resolve({
          state: "connected",
          browserSocketState: "connected",
          mode: "direct",
        });
      }
      return Promise.resolve({});
    });

    route = new TestHubRoute("hub-1", { hubId: "hub-1" }, manager);
    await route.initialize();
    await flushPromises();

    expect(mocks.bridge.sendCalls).toContainEqual([
      "connectSignaling",
      {
        hubId: "hub-1",
        browserIdentity: expect.stringMatching(/^anon:/),
        allowPlaintext: true,
      },
    ]);
    expect(mocks.bridge.sendCalls.some(([type]) => type === "connectPeer")).toBe(true);
    expect(mocks.bridge.hasSession).not.toHaveBeenCalled();
  });

  it("passes the Hub connection URL plaintext opt-in to the transport", async () => {
    route.destroy();
    route = null;
    mocks.bridge.reset();
    window.location.hash = "#no-encryption";

    route = new TestHubRoute("hub-1", { hubId: "hub-1" }, manager);
    await route.initialize();
    await flushPromises();

    expect(mocks.bridge.sendCalls).toContainEqual([
      "connectSignaling",
      {
        hubId: "hub-1",
        browserIdentity: expect.any(String),
        allowPlaintext: true,
      },
    ]);
  });

  it("rebuilds after repeated missed encrypted pongs while local peer is open", async () => {
    await emitStallAndWaitForProbe();
    await vi.advanceTimersByTimeAsync(1001);
    await emitStallAndWaitForProbe();
    await vi.advanceTimersByTimeAsync(1001);
    await emitStallAndWaitForProbe();

    expect(disconnectPeerCalls()).toHaveLength(1);
  });

  it("resets missed pong count when a pong arrives", async () => {
    await emitStallAndWaitForProbe();
    route.processMessage({ type: "dc_pong" });
    await vi.advanceTimersByTimeAsync(1001);
    await emitStallAndWaitForProbe();
    await vi.advanceTimersByTimeAsync(1001);
    await emitStallAndWaitForProbe();

    expect(disconnectPeerCalls()).toHaveLength(0);
  });

  it("does not replay an active subscription after a successful visibility probe", async () => {
    mocks.bridge.sendCalls.length = 0;
    mocks.bridge.send.mockClear();

    document.dispatchEvent(new Event("visibilitychange"));
    await flushPromises();
    route.processMessage({ type: "dc_pong" });
    await flushPromises();

    expect(disconnectPeerCalls()).toHaveLength(0);
    expect(
      mocks.bridge.sendCalls.filter(([type]) => type === "subscribe"),
    ).toHaveLength(0);
    expect(route.subscriptionId).toBe("hub:hub-1");
  });

  it("replays subscribe when an existing route sees a peer connected event", async () => {
    mocks.bridge.sendCalls.length = 0;
    mocks.bridge.send.mockClear();
    mocks.bridge.onSubscriptionMessage.mockClear();

    mocks.bridge.emit("connection:state", {
      hubId: "hub-1",
      state: "connected",
      mode: "direct",
    });
    await flushPromises();

    expect(
      mocks.bridge.sendCalls.filter(([type]) => type === "subscribe"),
    ).toContainEqual([
      "subscribe",
      {
        hubId: "hub-1",
        channel: "hub",
        params: { hub_id: "hub-1" },
        subscriptionId: "hub:hub-1",
      },
    ]);
    expect(mocks.bridge.onSubscriptionMessage).toHaveBeenCalledWith(
      "hub:hub-1",
      expect.any(Function),
    );
    expect(route.subscriptionId).toBe("hub:hub-1");
  });

  it("does not replay subscribe for an idle route when the peer reconnects", async () => {
    route.notifyIdle();
    mocks.bridge.sendCalls.length = 0;
    mocks.bridge.send.mockClear();

    mocks.bridge.emit("connection:state", {
      hubId: "hub-1",
      state: "connected",
      mode: "direct",
    });
    await flushPromises();

    expect(
      mocks.bridge.sendCalls.filter(([type]) => type === "subscribe"),
    ).toHaveLength(0);
    expect(route.subscriptionId).toBeNull();
  });

  it("unsubscribes if route teardown wins a pending subscribe confirmation", async () => {
    route.destroy();
    route = null;
    mocks.bridge.reset();

    let resolveSubscribe;
    mocks.bridge.send.mockImplementation((type, payload) => {
      mocks.bridge.sendCalls.push([type, payload]);
      if (type === "connectSignaling") {
        queueMicrotask(() => {
          mocks.bridge.emit("transport:config", { hubId: payload.hubId, e2e_enabled: true });
          mocks.bridge.emit("health", { hubId: payload.hubId, cli: "online" });
        });
        return Promise.resolve({
          state: "connected",
          browserSocketState: "connected",
          mode: "direct",
        });
      }
      if (type === "subscribe") {
        return new Promise((resolve) => {
          resolveSubscribe = resolve;
        });
      }
      return Promise.resolve({});
    });

    const pendingRoute = new TestHubRoute("hub-1", { hubId: "hub-1" }, manager);
    const initialize = pendingRoute.initialize();
    for (let i = 0; i < 10 && !resolveSubscribe; i += 1) {
      await flushPromises();
    }

    pendingRoute.destroy();
    resolveSubscribe({});
    await initialize;
    await flushPromises();

    expect(
      mocks.bridge.sendCalls.filter(([type]) => type === "unsubscribe"),
    ).toContainEqual(["unsubscribe", { subscriptionId: "hub:hub-1" }]);
    expect(pendingRoute.subscriptionId).toBeNull();
  });
});
