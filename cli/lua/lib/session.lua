-- Session base class for managing a single PTY session.
--
-- Base class for Agent and Accessory. Owns:
-- - Session UUID generation (sess-{epoch}-{seq}-{rand} format)
-- - BOTSTER_SESSION_UUID env injection into PTY env
-- - Session manifest sync to workspace store
-- - Workspace manifest sync
-- - PTY session lifecycle (spawn, close)
-- - Metadata key-value store (set_meta, get_meta)
-- - Environment variable building (build_env)
-- - Core identity fields
-- - Session registry (keyed by session_uuid)
-- - Broker scrollback replay
--
-- Subclasses (Agent, Accessory) call Session._init(self, config) for shared
-- initialization, then add type-specific fields.
--
-- This module is hot-reloadable; state is persisted via hub.state.
-- Uses state.class() for persistent metatable -- existing instances
-- automatically see new/changed methods after hot-reload.

local state = require("hub.state")
local hooks = require("hub.hooks")
local TargetContext = require("lib.target_context")

local Session = state.class("Session")

-- Session registry keyed by session_uuid (persistent across reloads)
local sessions = state.get("agent_registry", {})

local function plugin_worker_hub_proxy_enabled()
    local parent_hub_id = rawget(_G, "_plugin_worker_parent_hub_id")
    return type(parent_hub_id) == "string" and parent_hub_id ~= ""
end

local proxy_cache = {
    loaded_at = 0,
    by_key = {},
}

local function proxy_session_info(info)
    if type(info) ~= "table" then return info end
    if info.__botster_session_proxy then return info end
    info.__botster_session_proxy = true
    if type(info.metadata) ~= "table" then info.metadata = {} end
    info.info = function(self) return self end
    info.get_meta = function(self, key)
        local metadata = type(self.metadata) == "table" and self.metadata or {}
        return metadata[key]
    end
    info.set_meta = function(self, key, value)
        if type(self.metadata) ~= "table" then self.metadata = {} end
        self.metadata[key] = value
        return require("lib.hub").get():update_session(self.session_uuid or self.id, {
            metadata = self.metadata,
        })
    end
    info.update = function(self, fields)
        return require("lib.hub").get():update_session(self.session_uuid or self.id, fields or {})
    end
    info.close = function(self, delete_worktree)
        return require("lib.hub").get():delete_agent(self.session_uuid or self.id, delete_worktree or false)
    end
    return info
end

local function proxy_session_list(opts)
    opts = opts or {}
    local key = opts.include_system and "include_system" or "default"
    local now = os.clock()
    local cached = proxy_cache.by_key[key]
    if cached and (now - cached.loaded_at) <= 1 then
        return cached.list
    end

    local list = require("lib.hub").get():list_agents(opts)
    local out = {}
    for _, info in ipairs(list or {}) do
        out[#out + 1] = proxy_session_info(info)
    end
    proxy_cache.by_key[key] = {
        loaded_at = now,
        list = out,
    }
    return out
end

-- Reserve forwarded ports from a high, non-common range and probe localhost
-- availability before assigning them to accessories.
local FORWARD_PORT_MIN = 46000
local FORWARD_PORT_MAX = 61999
local port_state = state.get("agent_port_state", {
    next_port = FORWARD_PORT_MIN,
    reserved = {},
})

local function normalize_port_state()
    if type(port_state.reserved) ~= "table" then
        port_state.reserved = {}
    end
    if type(port_state.next_port) ~= "number"
        or port_state.next_port < FORWARD_PORT_MIN
        or port_state.next_port > FORWARD_PORT_MAX then
        port_state.next_port = FORWARD_PORT_MIN
    end
end

local function collect_reserved_ports()
    normalize_port_state()
    local seen = {}
    for port_key, reserved in pairs(port_state.reserved) do
        local port = tonumber(port_key)
        if reserved and port then
            seen[port] = true
        end
    end
    for _, session in pairs(sessions) do
        local port = session and session._port or nil
        if type(port) == "number" then
            seen[port] = true
        end
    end
    local ports = {}
    for port, _ in pairs(seen) do
        ports[#ports + 1] = port
    end
    table.sort(ports)
    return ports
end

local function reserve_forward_port()
    normalize_port_state()
    local start = port_state.next_port
    local excluded = collect_reserved_ports()
    local port, err = config.find_available_port(start, FORWARD_PORT_MAX, excluded)
    if not port and start > FORWARD_PORT_MIN then
        port, err = config.find_available_port(FORWARD_PORT_MIN, start - 1, excluded)
    end
    if not port then
        return nil, err or string.format(
            "No available localhost port in preferred range %d-%d",
            FORWARD_PORT_MIN, FORWARD_PORT_MAX)
    end
    port_state.next_port = (port < FORWARD_PORT_MAX) and (port + 1) or FORWARD_PORT_MIN
    port_state.reserved[tostring(port)] = true
    return port
end

local function release_forward_port(port)
    normalize_port_state()
    if type(port) == "number" then
        port_state.reserved[tostring(port)] = nil
    end
end

--- Update the hub manifest with currently active workspace IDs.
-- Called whenever the session registry changes (create/close).
local function sync_manifest_workspaces()
    if not hub.update_manifest_workspaces then return end
    local ws_ids = {}
    local seen = {}
    for _, session in pairs(sessions) do
        local ws_id = session.canonical == false and nil or session._workspace_id
        if ws_id and not seen[ws_id] then
            seen[ws_id] = true
            ws_ids[#ws_ids + 1] = ws_id
        end
    end
    pcall(hub.update_manifest_workspaces, ws_ids)
end

-- =============================================================================
-- UUID Generation
-- =============================================================================

-- Monotonic counter for collision-safe UUID generation (persistent across reloads)
local uuid_state = state.get("agent_uuid_counter", { seq = 0 })

--- Generate a collision-safe session UUID.
-- Format: "sess-{epoch}-{seq}-{random128}"
-- Combines second-level time + process-local monotonic counter + 128 bits of
-- randomness (4 independent draws). The counter alone prevents collisions under
-- burst creation; the random salt prevents collisions across process restarts
-- that might reset the counter.
-- @return string
local function generate_session_uuid()
    uuid_state.seq = uuid_state.seq + 1
    return string.format("sess-%d-%04x-%08x%08x%08x%08x",
        os.time(),
        uuid_state.seq,
        math.random(0, 0xFFFFFFFF),
        math.random(0, 0xFFFFFFFF),
        math.random(0, 0xFFFFFFFF),
        math.random(0, 0xFFFFFFFF))
end

--- Default display name for anonymous orchestration workspaces.
-- Keeps workspace identity stable via workspace_id while making UI labels readable.
-- @param branch_name string|nil
-- @return string
local function default_workspace_name(branch_name)
    if branch_name and branch_name ~= "" then
        return branch_name
    end
    return "General"
end

-- =============================================================================
-- Shared Initialization
-- =============================================================================

--- Initialize shared session fields and spawn the PTY.
-- Called by subclass constructors (Agent.new, Accessory.new).
-- self must already have its metatable set by the subclass.
--
-- Config table:
--   repo            string   (required)  "owner/repo"
--   branch_name     string   (required)
--   worktree_path   string   (required)
--   target_id       string   (optional)  admitted spawn target ID
--   target_path     string   (required)  admitted spawn target root
--   target_repo     string   (optional)  live repo identity for the target
--   session_type    string   (optional)  "agent" (default) or "accessory"
--   session         table    (required)  single session config:
--                              { name, command, init_script, definition_dir, notifications, forward_port }
--   prompt          string   (optional)  task description
--   metadata        table    (optional)  plugin key-value store (e.g., issue_number, invocation_url)
--   workspace       string   (optional)  workspace name (e.g. "owner/repo#42")
--   workspace_id    string   (optional)  pre-resolved workspace ID
--   workspace_expect_new boolean (optional) reject reusing an active workspace with same name
--   workspace_metadata table (optional)  plugin data stored on workspace manifest
--   env             table    (optional)  base environment variables
--   dims            table    (optional)  { rows = 24, cols = 80 }
--   agent_name      string   (optional)  config agent name (e.g., "codex")
--   label           string   (optional)  human-readable session label
--   owner_plugin    string   (optional)  plugin that owns this session
--   visibility      string   (optional)  "workspace" (default), "plugin", or "hidden"
--   surface         string   (optional)  owning surface for plugin-scoped navigation
--
-- @param self The instance (metatable already set by subclass)
-- @param config Table of session configuration
function Session._init(self, config)
    assert(config.branch_name, "Session._init requires config.branch_name")
    assert(config.worktree_path, "Session._init requires config.worktree_path")
    assert(config.session, "Session._init requires config.session")

    local session_type = config.session_type or "agent"
    local session_config = config.session
    local session_name = session_config.name or session_type
    local session_uuid = generate_session_uuid()

    -- Build metadata table
    local metadata = {}
    if config.metadata then
        for k, v in pairs(config.metadata) do
            metadata[k] = v
        end
    end
    -- Backward compat: accept legacy top-level fields into metadata
    if config.issue_number and not metadata.issue_number then
        metadata.issue_number = config.issue_number
    end
    if config.invocation_url and not metadata.invocation_url then
        metadata.invocation_url = config.invocation_url
    end

    local target = TargetContext.resolve({
        explicit = {
            target_id = config.target_id,
            target_path = config.target_path,
            target_repo = config.target_repo,
        },
        metadata = metadata,
        require_target_id = true,
        require_target_path = true,
        default_target_repo = config.repo,
    })
    assert(target, "Session._init requires config.target_id and config.target_path")
    metadata = TargetContext.with_metadata(metadata, target)
    config.repo = config.repo or target.repo

    -- Workspace name can be explicitly provided, or inferred from a supplied workspace_id.
    local workspace_name = config.workspace
    local pre_resolved_workspace_id = config.workspace_id
    local workspace_expect_new = config.workspace_expect_new or false

    -- Set all shared fields on self
    self.session_uuid = session_uuid
    self.session_type = session_type
    self.session_name = session_name
    self.repo = config.repo
    self.target_id = target.target_id
    self.target_path = target.target_path
    self.target_repo = target.target_repo
    self.branch_name = config.branch_name
    self.worktree_path = config.worktree_path
    self.prompt = config.prompt
    self.metadata = metadata
    self._workspace_name = workspace_name
    self._workspace_metadata = config.workspace_metadata or {}
    self.agent_name = config.agent_name
    self.label = config.label
    self.task = config.task
    self.owner_plugin = config.owner_plugin or metadata.owner_plugin
    self.visibility = config.visibility or metadata.visibility or "workspace"
    self.surface = config.surface or metadata.surface
    self.recovery_source = config.recovery_source or "created"
    self.canonical = config.canonical
    if self.canonical == nil then self.canonical = true end
    self.created_at = os.time()
    self.status = "running"
    self.title = nil          -- window title from OSC 0/2 (set by pty_title_changed hook)
    self.cwd = nil            -- current working directory from OSC 7 (set by pty_cwd_changed hook)
    self.notification = false -- true when OSC notification fired, cleared by client
    self.output_activity = "idle" -- "active" when recent PTY output was observed
    self.session = nil        -- single PtySessionHandle
    self._session_config = session_config  -- original session config from creation
    self.session_dir = session_config.definition_dir

    local key = self.session_uuid

    local git_path = config.worktree_path .. "/.git"
    local is_worktree = fs.exists(git_path) and not fs.is_dir(git_path)
    self._is_worktree = is_worktree

    -- Resolve device data_dir for workspace store.
    -- Subclass .new() receives a `config` parameter that shadows the global `config`,
    -- so access the global via _G to get the actual device data directory.
    local data_dir = _G.config and _G.config.data_dir and _G.config.data_dir() or nil
    self._data_dir = data_dir

    -- Build environment variables.
    local env = self:build_env(config.env)
    self.hub_socket = env.BOTSTER_HUB_SOCKET
    self.hub_manifest_path = env.BOTSTER_HUB_MANIFEST_PATH

    -- Initialize Central Session Store.
    if data_dir then
        local ws = require("lib.workspace_store")
        ws.init_dir(data_dir)

        local workspace_id = pre_resolved_workspace_id
        if workspace_id then
            local existing = ws.read_workspace(data_dir, workspace_id)
            if existing then
                workspace_name = workspace_name or existing.name
                self.target_id = self.target_id or existing.target_id
                self.target_path = self.target_path or existing.target_path
                self.target_repo = self.target_repo or existing.target_repo
                self.metadata = TargetContext.with_metadata(self.metadata, TargetContext.from_session(self))
            else
                log.warn(string.format("Workspace %s not found; creating standalone workspace", tostring(workspace_id)))
                workspace_id = nil
            end
        end

        if not workspace_id and workspace_name then
            local ok_ws, ws_id, ws_err = pcall(function()
                local id, _, _, err = ws.ensure_workspace(data_dir, {
                    name = workspace_name,
                    target_id = self.target_id,
                    target_path = self.target_path,
                    target_repo = self.target_repo,
                    metadata = self._workspace_metadata,
                    created_at = os.date("!%Y-%m-%dT%H:%M:%SZ", self.created_at),
                    expect_new = workspace_expect_new,
                })
                return id, err
            end)
            if ok_ws and ws_id then
                workspace_id = ws_id
            else
                log.warn(string.format("Failed to ensure workspace manifest: %s",
                    tostring(ws_err or ws_id)))
            end
        end

        -- Standalone session: reuse the default active workspace for this
        -- branch/target instead of minting a fresh anonymous workspace ID.
        if not workspace_id then
            if not workspace_name or workspace_name == "" then
                workspace_name = default_workspace_name(config.branch_name)
            end
            local ok_ws, ws_id, ws_err = pcall(function()
                local id, _, _, err = ws.ensure_workspace(data_dir, {
                    name = workspace_name,
                    target_id = self.target_id,
                    target_path = self.target_path,
                    target_repo = self.target_repo,
                    metadata = self._workspace_metadata,
                    created_at = os.date("!%Y-%m-%dT%H:%M:%SZ", self.created_at),
                    expect_new = false,
                })
                return id, err
            end)
            if ok_ws and ws_id then
                workspace_id = ws_id
            else
                log.warn(string.format("Failed to ensure default workspace manifest: %s",
                    tostring(ws_err or ws_id)))
                workspace_id = ws.generate_workspace_id()
                local anon_manifest = {
                    id         = workspace_id,
                    name       = workspace_name,
                    target_id  = self.target_id,
                    target_path = self.target_path,
                    target_repo = self.target_repo,
                    status     = "active",
                    created_at = os.date("!%Y-%m-%dT%H:%M:%SZ", self.created_at),
                    updated_at = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time()),
                    metadata   = {},
                }
                pcall(ws.write_workspace, data_dir, workspace_id, anon_manifest)
                pcall(function()
                    local hooks_mod = require("hub.hooks")
                    hooks_mod.notify("workspace_created", {
                        workspace_id = workspace_id,
                        name = workspace_name,
                        manifest = anon_manifest,
                    })
                end)
            end
        end

        self._workspace_id = workspace_id
        self._workspace_name = workspace_name
        self:_sync_workspace_manifest()
        self:_sync_session_manifest()
        ws.append_event(data_dir, self._workspace_id, session_uuid, "created")
    end

    -- Determine dimensions
    local rows = 24
    local cols = 80
    if config.dims then
        rows = config.dims.rows or 24
        cols = config.dims.cols or 80
    end

    -- Spawn the single PTY session
    -- Shallow-copy env for session-specific overrides
    local session_env = {}
    for k, v in pairs(env) do
        session_env[k] = v
    end

    local spawn_config = {
        worktree_path = config.worktree_path,
        cwd = config.worktree_path,
        command = session_config.command or "bash",
        args = type(session_config.args) == "table" and session_config.args or {},
        env = session_env,
        detect_notifications = session_config.notifications or false,
        session_uuid = key,
        session_name = session_name,
        label = self.label,
        rows = rows,
        cols = cols,
    }
    spawn_config.recovery_identity = {
        schema_version = 1,
        session_uuid = self.session_uuid,
        session_type = self.session_type,
        session_name = self.session_name,
        repo = self.repo,
        target_id = self.target_id,
        target_path = self.target_path,
        target_repo = self.target_repo,
        branch_name = self.branch_name,
        worktree_path = self.worktree_path,
        workspace_id = self._workspace_id,
        workspace_name = self._workspace_name,
        agent_name = self.agent_name,
        owner_plugin = self.owner_plugin,
        visibility = self.visibility,
        surface = self.surface,
        label = self.label,
        in_worktree = self._is_worktree or false,
        created_at = self.created_at,
    }

    -- Build init_commands from init_script (absolute path from config resolver)
    spawn_config.init_commands = {}
    if session_config.init_script then
        if fs.exists(session_config.init_script) then
            spawn_config.init_commands[#spawn_config.init_commands + 1] =
                "source " .. session_config.init_script
        else
            log.debug(string.format("Init script not found: %s", session_config.init_script))
        end
    end
    if type(session_config.init_commands) == "table" then
        for _, cmd in ipairs(session_config.init_commands) do
            if type(cmd) == "string" and cmd ~= "" then
                spawn_config.init_commands[#spawn_config.init_commands + 1] = cmd
            end
        end
    end

    -- Allocate a high, currently bindable port for forwarded sessions.
    local port = nil
    if session_config.forward_port then
        local port_err
        port, port_err = reserve_forward_port()
        if not port then
            error(string.format("Failed to allocate forwarded port: %s", tostring(port_err)))
        end
        spawn_config.port = port
        session_env.PORT = tostring(port)
    end

    -- Interceptor: plugins can inspect context or block spawn (return nil)
    local spawn_ctx = {
        worktree_path = config.worktree_path,
        branch = config.branch_name,
        session_uuid = session_uuid,
        cwd = spawn_config.cwd,
        command = spawn_config.command,
        args = spawn_config.args,
        env = spawn_config.env,
        init_commands = spawn_config.init_commands,
        detect_notifications = spawn_config.detect_notifications,
        rows = spawn_config.rows,
        cols = spawn_config.cols,
        port = spawn_config.port,
        session_type = session_type,
        session_name = session_name,
        repo = config.repo,
        target_id = self.target_id,
        target_path = self.target_path,
        target_repo = self.target_repo,
        workspace_id = self._workspace_id,
        workspace_name = self._workspace_name,
        metadata = metadata,
        owner_plugin = self.owner_plugin,
        visibility = self.visibility,
        surface = self.surface,
    }
    local spawn_result = hooks.call("before_pty_spawn", spawn_ctx)
    if spawn_result == nil then
        if port then
            release_forward_port(port)
        end
        error(string.format("PTY spawn blocked by interceptor for %s", key))
    end
    spawn_config.worktree_path = spawn_result.worktree_path or spawn_config.worktree_path
    spawn_config.cwd = spawn_result.cwd or spawn_config.cwd
    spawn_config.command = spawn_result.command or spawn_config.command
    spawn_config.args = spawn_result.args or spawn_config.args
    spawn_config.env = spawn_result.env or spawn_config.env
    spawn_config.init_commands = spawn_result.init_commands or spawn_config.init_commands
    spawn_config.detect_notifications = spawn_result.detect_notifications
    if spawn_config.detect_notifications == nil then
        spawn_config.detect_notifications = session_config.notifications or false
    end
    spawn_config.rows = spawn_result.rows or spawn_config.rows
    spawn_config.cols = spawn_result.cols or spawn_config.cols
    spawn_config.port = spawn_result.port or spawn_config.port

    local ok, handle = pcall(hub.spawn_session, spawn_config, session_uuid)
    if not ok or not handle then
        if port then
            release_forward_port(port)
        end
        error(string.format(
            "Failed to spawn session process for %s: %s",
            key, tostring(handle)
        ))
    end

    self.session = handle
    self._port = port

    log.info(string.format("Session %s: spawned '%s' (uuid=%s, type=%s)", key, session_name, session_uuid, session_type))

    -- Register with HandleCache via hub.register_session()
    local reg_ok, reg_index = pcall(hub.register_session, session_uuid, handle, {
        session_type = session_type,
        session_name = session_name,
        label = self.label or "",
        workspace_id = self._workspace_id,
        observe_output = self.metadata and self.metadata.observe_output == true,
    })
    if reg_ok then
        log.info(string.format("Session %s: registered with HandleCache index %s",
            key, tostring(reg_index)))
    else
        log.error(string.format("Session %s: failed to register: %s", key, tostring(reg_index)))
    end

    self:_sync_session_manifest()
    log.info(string.format("Session %s: registered (uuid=%s)", key, session_uuid))

    -- Register in session registry (keyed by session_uuid)
    sessions[session_uuid] = self
    sync_manifest_workspaces()

    -- Notify observers
    hooks.notify("after_agent_create", self)

    log.info(string.format("Session created: %s (uuid=%s, type=%s)", key, session_uuid, session_type))
end

--- Initialize a session from a persisted manifest during session recovery.
-- Hydrates all fields from the manifest + live handle without spawning a
-- new PTY or generating a new UUID. The resulting instance is a first-class
-- session, identical to a freshly created one.
--
-- @param self The instance (metatable already set by subclass)
-- @param config Table with manifest fields plus recovery-specific keys:
--   handle (PtySessionHandle); dims ({ rows, cols })
function Session._init_recovered(self, config)
    local recovered_target = TargetContext.resolve({
        explicit = {
            target_id = config.target_id,
            target_path = config.target_path,
            target_repo = config.target_repo,
        },
        metadata = config.metadata,
        default_target_repo = config.repo,
    }) or {
        target_id = config.target_id,
        target_path = config.target_path,
        target_repo = config.target_repo or config.repo,
    }

    self.session_uuid    = config.session_uuid
    self.session_type    = config.session_type or "agent"
    self.session_name    = config.session_name or self.session_type
    self.repo            = config.repo or TargetContext.default_repo_label(recovered_target)
    self.target_id       = recovered_target.target_id
    self.target_path     = recovered_target.target_path
    self.target_repo     = recovered_target.target_repo
    self.branch_name     = config.branch_name
    self.worktree_path   = config.worktree_path
    self.prompt          = config.prompt
    self.agent_name      = config.agent_name
    self.owner_plugin    = config.owner_plugin or (config.metadata and config.metadata.owner_plugin)
    self.visibility      = config.visibility or (config.metadata and config.metadata.visibility) or "workspace"
    self.surface         = config.surface or (config.metadata and config.metadata.surface)
    self.created_at      = config.created_at or os.time()
    self.status          = "running"
    self.title           = config.title
    self.cwd             = config.cwd
    self.label           = config.label
    self.task            = config.task
    self.notification    = false
    self.output_activity = "idle"
    self.session         = config.handle
    self.recovery_source = config.recovery_source or "manifest"
    self.canonical       = config.canonical
    if self.canonical == nil then self.canonical = true end
    self._session_config = nil
    self._port           = config.port
    if self._port == nil and config.handle and type(config.handle.port) == "function" then
        local ok, handle_port = pcall(function() return config.handle:port() end)
        if ok then
            self._port = handle_port
        end
    end
    self._workspace_id   = config.workspace_id
    self._workspace_name = config.workspace_name
    self._workspace_metadata = {}

    -- Metadata: manifest metadata enriched with resolved target identity.
    local metadata = {}
    if config.metadata then
        for k, v in pairs(config.metadata) do metadata[k] = v end
    end
    metadata = TargetContext.with_metadata(metadata, recovered_target)
    self.metadata = metadata

    -- Filesystem checks
    if config.worktree_path then
        local git_path = config.worktree_path .. "/.git"
        self._is_worktree = config.in_worktree or (fs.exists(git_path) and not fs.is_dir(git_path))
    else
        self._is_worktree = false
    end

    local data_dir = _G.config and _G.config.data_dir and _G.config.data_dir() or nil
    self._data_dir = data_dir

    -- Derive hub_socket and hub_manifest_path (hub_discovery is a Rust global)
    local local_hub_id = hub.hub_id and hub.hub_id() or nil
    if local_hub_id and hub_discovery then
        if hub_discovery.socket_path then
            local ok, socket_path = pcall(hub_discovery.socket_path, local_hub_id)
            if ok and type(socket_path) == "string" and socket_path ~= "" then
                self.hub_socket = socket_path
            end
        end
        if hub_discovery.manifest_path then
            local ok, manifest_path = pcall(hub_discovery.manifest_path, local_hub_id)
            if ok and type(manifest_path) == "string" and manifest_path ~= "" then
                self.hub_manifest_path = manifest_path
            end
        end
    end

    local key = self.session_uuid

    -- Register with HandleCache
    local reg_ok, reg_index = pcall(hub.register_session, self.session_uuid, config.handle, {
        session_type      = self.session_type,
        session_name      = self.session_name,
        label             = self.label or "",
        workspace_id      = self._workspace_id,
    })
    if not reg_ok then
        error(string.format("Session %s: recovery register failed: %s", key, tostring(reg_index)))
    end
    log.info(string.format("Session %s: recovered (index=%s)", key, tostring(reg_index)))

    -- Register in session registry
    sessions[self.session_uuid] = self
    sync_manifest_workspaces()

    log.info(string.format("Session recovered: %s (uuid=%s, type=%s)",
        key, self.session_uuid, self.session_type))

    -- Re-issue the caller credential so a Hub restart restores the same
    -- persisted token. The live agent process already has that token in env.
    if type(hub.issue_mcp_caller) == "function" then
        pcall(hub.issue_mcp_caller, {
            session_uuid = self.session_uuid,
            hub_id = hub.hub_id() or hub.server_id() or "",
        })
    end
end

-- =============================================================================
-- Instance Methods
-- =============================================================================

--- Update one or more session fields and sync the manifest.
-- This is the only way external code should mutate session state.
-- @param fields table  Key-value pairs to update (e.g., { title = "foo", cwd = "/tmp" })
-- Fields that are runtime-only and don't need manifest sync to disk.
local RUNTIME_ONLY_FIELDS = {
    output_activity = true,
    notification = true,
    plugin_state = true,
}

function Session:update(fields)
    local changed = false
    local changed_fields = {}
    local needs_sync = false
    for k, v in pairs(fields) do
        if self[k] ~= v then
            self[k] = v
            changed = true
            changed_fields[k] = v
            if not RUNTIME_ONLY_FIELDS[k] then
                needs_sync = true
            end
        end
    end
    if changed then
        if needs_sync then
            self:_sync_session_manifest()
        end
        hooks.notify("session_updated", { session_uuid = self.session_uuid, fields = changed_fields })

        -- Wire protocol — emit a single entity_patch frame per
        -- Session:update covering exactly the changed fields plus any
        -- re-derived ones (display_name when title/agent_name/branch_name
        -- moved). System sessions stay off the wire — the EB filter on
        -- the `session` registration drops them.
        require("lib.entity_model").patch_session(self, changed_fields)
        require("lib.session_actions").publish_for_session(self)
    end
end

--- Derive output activity from the session process' last PTY output timestamp.
-- The session process reports facts (`last_output_at`); the Lua session model
-- owns the product-level active/idle threshold used by clients.
-- @param active_window_ms number Milliseconds after output that count as active
-- @param now_ms number Current epoch milliseconds
-- @return string "active"|"idle"
function Session:derive_output_activity(active_window_ms, now_ms)
    if not self.session then
        return "idle"
    end

    local ok, last_output_at = pcall(function()
        return self.session:last_output_at()
    end)
    if not ok or type(last_output_at) ~= "number" or last_output_at <= 0 then
        return "idle"
    end

    if (now_ms - last_output_at) <= active_window_ms then
        return "active"
    end
    return "idle"
end

--- Publish the current client-facing session entity.
--
-- Workspace rename/move changes live on the private `_workspace_*` fields,
-- so they cannot safely flow through Session:update without creating shadow
-- public fields. Use a full upsert after those identity changes so every
-- client sees the same payload it would receive on subscribe/reconnect.
function Session:publish_entity()
    require("lib.entity_model").publish_session(self)
end

--- Set a metadata value and sync session manifest.
-- @param key string Metadata key
-- @param value any Metadata value
function Session:set_meta(key, value)
    self.metadata[key] = value
    self:_sync_session_manifest()
end

--- Get a metadata value.
-- @param key string Metadata key
-- @return any Metadata value or nil
function Session:get_meta(key)
    return self.metadata[key]
end

--- Sync the Central Session Store session manifest.
-- Writes self:info() shape so session recovery can load it directly.
-- @param opts table|nil Options. Set refresh_workspace_status=false when
-- syncing title/cwd metadata-only fields that cannot affect workspace
-- liveness/status. Do not disable refresh for status, workspace, or lifecycle
-- changes.
function Session:_sync_session_manifest(opts)
    opts = opts or {}
    if self.canonical == false then
        log.debug(string.format(
            "Session %s: skip session manifest sync for non-canonical recovered session (recovery_source=%s)",
            tostring(self.session_uuid),
            tostring(self.recovery_source)
        ))
        return
    end
    if not self._data_dir or not self._workspace_id then
        log.debug(string.format("Session %s: skip manifest sync (data_dir=%s, workspace_id=%s)",
            tostring(self.session_uuid),
            tostring(self._data_dir),
            tostring(self._workspace_id)))
        return
    end
    local ws = require("lib.workspace_store")

    -- Start from the canonical info() shape — this is the contract.
    local manifest = self:info()

    -- Add persistence-only fields not in info()
    manifest.prompt            = self.prompt
    manifest.hub_id            = hub.hub_id() or hub.server_id()
    manifest.hub_manifest_path = self.hub_manifest_path
    manifest.role              = "developer"

    -- Normalize status for persistence
    if manifest.status == "running" then manifest.status = "active" end

    -- Timestamps as ISO 8601 for human readability
    manifest.created_at = os.date("!%Y-%m-%dT%H:%M:%SZ", self.created_at)
    manifest.updated_at = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time())

    local pty_dimensions = {}
    if self.session then
        local ok_dims, rows, cols = pcall(function() return self.session:dimensions() end)
        if ok_dims and rows and cols then
            pty_dimensions["0"] = { rows = rows, cols = cols }
        end
    end
    manifest.pty_dimensions  = pty_dimensions

    local plugin_metadata = {}
    local internal_keys = {
        tee_log_path = true,
    }
    for k, v in pairs(self.metadata) do
        if not internal_keys[k] then
            plugin_metadata[k] = v
        end
    end
    manifest.metadata = plugin_metadata

    -- Strip runtime-only fields that shouldn't persist
    manifest.port           = nil
    manifest.notification   = nil
    manifest.output_activity = nil

    log.info(string.format("Session %s: writing manifest to %s/workspaces/%s/sessions/%s/manifest.json",
        self.session_uuid, tostring(self._data_dir), tostring(self._workspace_id), self.session_uuid))
    local ok, err = pcall(ws.write_session,
        self._data_dir, self._workspace_id, self.session_uuid, manifest)
    if not ok then
        log.warn(string.format("Failed to sync session manifest: %s", tostring(err)))
        return
    end
    log.info(string.format("Session %s: manifest written", self.session_uuid))
    if opts.refresh_workspace_status ~= false then
        pcall(ws.refresh_workspace_status, self._data_dir, self._workspace_id)
    end
end

--- Sync the Central Session Store workspace manifest.
function Session:_sync_workspace_manifest()
    if self.canonical == false then
        log.debug(string.format(
            "Session %s: skip workspace manifest sync for non-canonical recovered session (recovery_source=%s)",
            tostring(self.session_uuid),
            tostring(self.recovery_source)
        ))
        return
    end
    if not self._data_dir or not self._workspace_id then return end
    local ws = require("lib.workspace_store")
    local current = ws.read_workspace(self._data_dir, self._workspace_id) or {}
    local merged_metadata = {}
    for k, v in pairs(current.metadata or {}) do
        merged_metadata[k] = v
    end
    for k, v in pairs(self._workspace_metadata or {}) do
        merged_metadata[k] = v
    end

    local manifest = {
        id         = self._workspace_id,
        name       = self._workspace_name or current.name or default_workspace_name(self.branch_name),
        target_id  = self.target_id or current.target_id,
        target_path = self.target_path or current.target_path,
        target_repo = self.target_repo or current.target_repo,
        status     = current.status or "active",
        created_at = current.created_at or os.date("!%Y-%m-%dT%H:%M:%SZ", self.created_at),
        updated_at = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time()),
        metadata   = merged_metadata,
    }

    local ok, err = pcall(ws.write_workspace, self._data_dir, self._workspace_id, manifest)
    if not ok then
        log.warn(string.format("Failed to sync workspace manifest: %s", tostring(err)))
        return
    end
    pcall(ws.refresh_workspace_status, self._data_dir, self._workspace_id)
end

--- Move this live session into a different workspace.
-- Creates the target workspace when only workspace_name is provided.
-- @param opts table { workspace_id, workspace_name, workspace_metadata, expect_new }
-- @return table|nil result
-- @return string|nil error
function Session:move_to_workspace(opts)
    opts = opts or {}
    if self.canonical == false then
        local message = string.format(
            "Session %s: refusing workspace move for non-canonical recovered session (recovery_source=%s)",
            tostring(self.session_uuid),
            tostring(self.recovery_source)
        )
        log.warn(message)
        return nil, message
    end
    if not self._data_dir then
        return nil, "No data_dir configured for workspace operations"
    end

    local ws = require("lib.workspace_store")
    local target_workspace_id = opts.workspace_id
    local target_workspace_name = opts.workspace_name
    local target_workspace_metadata = opts.workspace_metadata or {}
    local expect_new = opts.expect_new or false

    if target_workspace_id == "" then target_workspace_id = nil end
    if target_workspace_name == "" then target_workspace_name = nil end

    if not target_workspace_id and not target_workspace_name then
        return nil, "workspace_id or workspace_name is required"
    end

    local now = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time())
    local old_workspace_id = self._workspace_id
    local old_workspace_name = self._workspace_name
    local workspace_id = target_workspace_id
    local workspace_name = target_workspace_name
    local session_target = TargetContext.from_session(self)

    if workspace_id then
        local target_manifest = ws.read_workspace(self._data_dir, workspace_id)
        if not target_manifest then
            return nil, string.format("Workspace '%s' not found", tostring(workspace_id))
        end
        if not TargetContext.matches(target_manifest, session_target) then
            return nil, string.format("Workspace '%s' belongs to a different target", tostring(workspace_id))
        end

        if workspace_name and workspace_name ~= target_manifest.name then
            local renamed = ws.rename_workspace(self._data_dir, workspace_id, workspace_name)
            if renamed then
                target_manifest.name = workspace_name
            else
                return nil, string.format("Failed to rename workspace '%s'", tostring(workspace_id))
            end
        end
        workspace_name = workspace_name or target_manifest.name or default_workspace_name(self.branch_name)
    else
        local created_id, created_manifest, _, ensure_err = ws.ensure_workspace(self._data_dir, {
            name = workspace_name,
            target_id = self.target_id,
            target_path = self.target_path,
            target_repo = self.target_repo,
            metadata = target_workspace_metadata,
            created_at = now,
            expect_new = expect_new,
        })
        if not created_id then
            return nil, ensure_err or string.format("Failed to create workspace '%s'", tostring(workspace_name))
        end
        workspace_id = created_id
        workspace_name = created_manifest and created_manifest.name or workspace_name
    end

    if old_workspace_id == workspace_id and old_workspace_name == workspace_name then
        return {
            workspace_id = workspace_id,
            workspace_name = workspace_name,
            previous_workspace_id = old_workspace_id,
            previous_workspace_name = old_workspace_name,
            changed = false,
        }, nil
    end

    -- Keep metadata in sync for plugins that look up by workspace fields.
    self.metadata = self.metadata or {}
    self.metadata.workspace_id = workspace_id
    self.metadata.workspace = workspace_name

    -- Merge any workspace metadata updates onto this session.
    self._workspace_metadata = self._workspace_metadata or {}
    for k, v in pairs(target_workspace_metadata) do
        self._workspace_metadata[k] = v
    end

    self._workspace_id = workspace_id
    self._workspace_name = workspace_name

    -- Update HandleCache metadata so Rust-side lookups see the new workspace_id.
    -- Use update_session_metadata (lightweight) instead of register_session to avoid
    -- creating a duplicate reader thread on the same socket.
    local ok_meta, meta_err = pcall(hub.update_session_metadata, self.session_uuid, {
        label = self.label or "",
        workspace_id = workspace_id,
    })
    if not ok_meta then
        log.warn(string.format("Session %s: failed to update HandleCache metadata: %s",
            self.session_uuid, tostring(meta_err)))
    end

    self:_sync_workspace_manifest()
    self:_sync_session_manifest()
    ws.append_event(self._data_dir, workspace_id, self.session_uuid, "moved")

    if old_workspace_id and old_workspace_id ~= workspace_id then
        local old_manifest = ws.read_session(self._data_dir, old_workspace_id, self.session_uuid)
        if old_manifest then
            old_manifest.status = "closed"
            old_manifest.updated_at = now
            old_manifest.moved_to_workspace_id = workspace_id
            old_manifest.moved_to_workspace_name = workspace_name
            pcall(ws.write_session, self._data_dir, old_workspace_id, self.session_uuid, old_manifest)
            ws.append_event(self._data_dir, old_workspace_id, self.session_uuid, "moved")
        end
        pcall(ws.refresh_workspace_status, self._data_dir, old_workspace_id)
    end
    pcall(ws.refresh_workspace_status, self._data_dir, workspace_id)

    self:publish_entity()

    return {
        workspace_id = workspace_id,
        workspace_name = workspace_name,
        previous_workspace_id = old_workspace_id,
        previous_workspace_name = old_workspace_name,
        changed = true,
    }, nil
end

--- Close the session and clean up resources.
-- @param delete_worktree boolean Whether to queue worktree deletion
function Session:close(delete_worktree)
    local key = self.session_uuid

    -- Notify observers
    hooks.notify("before_agent_close", self)

    if type(hub.revoke_mcp_caller) == "function" then
        pcall(hub.revoke_mcp_caller, self.session_uuid)
    end

    -- Unregister from HandleCache
    local ok, err = pcall(hub.unregister_session, self.session_uuid)
    if not ok then
        log.warn(string.format("Session %s: failed to unregister: %s", key, tostring(err)))
    end

    -- Kill the PTY session
    if self.session then
        local ok2, err2 = pcall(function() self.session:kill() end)
        if not ok2 then
            log.warn(string.format("Session %s: error killing PTY: %s", key, tostring(err2)))
        end
    end
    if self._port then
        release_forward_port(self._port)
    end
    self.session = nil
    self.status = "closed"

    -- Remove from registry
    sessions[self.session_uuid] = nil
    sync_manifest_workspaces()

    -- Mark the Central Session Store session as closed.
    -- Explicit user close is authoritative; hub restart recovery does not use
    -- Session:close(), so a closed manifest here is a do-not-recover tombstone.
    if self.canonical == false then
        log.debug(string.format(
            "Session %s: skip close manifest tombstone for non-canonical recovered session (recovery_source=%s)",
            tostring(self.session_uuid),
            tostring(self.recovery_source)
        ))
    elseif self._data_dir and self._workspace_id then
        local ws = require("lib.workspace_store")
        local manifest = ws.read_session(self._data_dir, self._workspace_id, self.session_uuid)
        if manifest then
            manifest.status     = "closed"
            manifest.updated_at = os.date("!%Y-%m-%dT%H:%M:%SZ", os.time())
            pcall(ws.write_session,
                self._data_dir, self._workspace_id, self.session_uuid, manifest)
            pcall(ws.refresh_workspace_status, self._data_dir, self._workspace_id)
            -- Check if workspace transitioned to closed
            local updated_ws = ws.read_workspace(self._data_dir, self._workspace_id)
            if updated_ws and updated_ws.status == "closed" then
                hooks.notify("workspace_closed", {
                    workspace_id = self._workspace_id,
                    name = updated_ws.name or self._workspace_name,
                })
            end
        end
    end

    -- Queue worktree deletion if requested
    if delete_worktree then
        if self.canonical == false then
            log.warn(string.format(
                "Session %s: refusing worktree deletion for non-canonical recovered session (recovery_source=%s)",
                tostring(self.session_uuid),
                tostring(self.recovery_source)
            ))
        else
            local ok3, err3 = pcall(worktree.delete, self.worktree_path, self.branch_name)
            if not ok3 then
                log.warn(string.format("Session %s: failed to delete worktree: %s",
                    key, tostring(err3)))
            end
            hooks.notify("worktree_deleted", {
                path = self.worktree_path,
                branch = self.branch_name,
                session_uuid = self.session_uuid,
            })
        end
    end

    -- Notify observers
    hooks.notify("after_agent_close", self)

    log.info(string.format("Session closed: %s (uuid=%s, delete_worktree=%s)",
        key, self.session_uuid, tostring(delete_worktree or false)))
end

--- Build environment variables for spawned sessions.
-- @param base_env table Optional base env vars to merge
-- @return table Environment variables
function Session:build_env(base_env)
    local env = {}
    if base_env then
        for k, v in pairs(base_env) do
            env[k] = v
        end
    end

    -- Preserve terminal identity/capability hints from the Botster process so
    -- child CLIs can make the same color/UI decisions they would outside Botster.
    local passthrough_env = {
        "COLORTERM",
        "TERM_PROGRAM",
        "TERM_PROGRAM_VERSION",
        "LC_TERMINAL",
        "LC_TERMINAL_VERSION",
        "COLORFGBG",
    }
    for _, key in ipairs(passthrough_env) do
        if env[key] == nil then
            local value = os.getenv(key)
            if value and value ~= "" then
                env[key] = value
            end
        end
    end

    -- Terminal identity: use bundled xterm-ghostty terminfo when available.
    -- Botster IS the terminal — don't inherit TERM from the host.
    local ti = _G.config and _G.config.terminfo and _G.config.terminfo() or { term = "xterm-256color" }
    env.TERM = env.TERM or ti.term
    if ti.terminfo_dir then
        env.TERMINFO = env.TERMINFO or ti.terminfo_dir
    end
    -- Ensure UTF-8 locale for child processes.
    if not env.LANG and not os.getenv("LANG") then
        env.LANG = "en_US.UTF-8"
    end
    env.BOTSTER_WORKTREE_PATH = self.worktree_path
    if self.target_id then env.BOTSTER_TARGET_ID = self.target_id end
    if self.target_path then env.BOTSTER_TARGET_PATH = self.target_path end
    if self.target_repo then env.BOTSTER_TARGET_REPO = self.target_repo end
    env.BOTSTER_SESSION_UUID = self.session_uuid
    -- Shared HTTP MCP: one Hub listener, one caller credential per session.
    if type(hub.issue_mcp_caller) == "function" then
        local creds_ok, creds = pcall(hub.issue_mcp_caller, {
            session_uuid = self.session_uuid,
            hub_id = hub.hub_id() or hub.server_id() or "",
        })
        if creds_ok and type(creds) == "table" and creds.url and creds.token then
            env.BOTSTER_MCP_URL = creds.url
            env.BOTSTER_MCP_TOKEN = creds.token
        end
    end
    -- Ensure child processes (e.g., botster mcp-serve) use the same binary and
    -- data directory as the hub, even if a different build is installed on PATH.
    if self._data_dir then
        env.BOTSTER_CONFIG_DIR = self._data_dir
    end
    -- Prepend the running binary's directory to PATH so `botster` resolves to
    -- the same binary the hub is running (debug vs release parity).
    local exe_dir = hub.exe_dir and hub.exe_dir()
    if exe_dir and exe_dir ~= "" then
        local current_path = env.PATH or os.getenv("PATH") or ""
        env.PATH = exe_dir .. ":" .. current_path
    end
    env.BOTSTER_HUB_ID = hub.server_id() or ""
    local local_hub_id = hub.hub_id and hub.hub_id() or nil
    if local_hub_id and hub_discovery and hub_discovery.socket_path then
        local ok, socket_path = pcall(hub_discovery.socket_path, local_hub_id)
        if ok and type(socket_path) == "string" and socket_path ~= "" then
            env.BOTSTER_HUB_SOCKET = socket_path
        end
    end
    if local_hub_id and hub_discovery and hub_discovery.manifest_path then
        local ok, manifest_path = pcall(hub_discovery.manifest_path, local_hub_id)
        if ok and type(manifest_path) == "string" and manifest_path ~= "" then
            env.BOTSTER_HUB_MANIFEST_PATH = manifest_path
        end
    end
    if self.prompt and self.prompt ~= "" then
        env.BOTSTER_PROMPT = self.prompt
    end
    -- Fire filter hook for customization
    env = hooks.call("filter_agent_env", env, self) or env
    return env
end

--- Resolve spawn target name from registry (hub runtime only).
-- @return string|nil Target name or nil if unavailable
function Session:_resolve_target_name()
    local registry = rawget(_G, "spawn_targets")
    if registry and type(registry.get) == "function" and self.target_id then
        local ok, tgt = pcall(registry.get, self.target_id)
        if ok and tgt then return tgt.name end
    end
    return nil
end

--- Get session metadata for clients.
-- Returns a serializable table of session info.
-- @return table Session info
function Session:info()
    local port = self._port

    -- Build display name: prefer OSC title (set by running script), fall back to
    -- agent_name (from config), then branch_name.
    -- Label is a separate field — it does NOT override display_name.
    local display_name
    if self.title and self.title ~= "" then
        display_name = self.title
    elseif self.agent_name and self.agent_name ~= "" then
        display_name = self.agent_name
    else
        display_name = self.branch_name
    end

    return {
        id = self.session_uuid,
        session_uuid = self.session_uuid,
        session_type = self.session_type,
        session_name = self.session_name,
        display_name = display_name,
        title = self.title,
        cwd = self.cwd,
        agent_name = self.agent_name,
        repo = self.repo,
        target_id = self.target_id,
        target_name = self:_resolve_target_name(),
        target_path = self.target_path,
        target_repo = self.target_repo,
        metadata = self.metadata,
        plugin_state = self.plugin_state,
        owner_plugin = self.owner_plugin,
        visibility = self.visibility or "workspace",
        surface = self.surface,
        workspace_name = self._workspace_name,
        workspace_id = self._workspace_id,
        branch_name = self.branch_name,
        worktree_path = self.worktree_path,
        session_dir = self.session_dir,
        in_worktree = self._is_worktree or false,
        status = self.status,
        notification = self.notification or false,
        port = port,
        system_session = Session.is_system_session(self),
        created_at = self.created_at,
        label = self.label,
        task = self.task,
        output_activity = self.output_activity or "idle",
        recovery_source = self.recovery_source,
        canonical = self.canonical,
    }
end

-- =============================================================================
-- Module-Level Functions (on the Session class table)
-- =============================================================================

--- Get a session by session_uuid (primary lookup).
-- @param session_uuid string Session UUID
-- @return Session subclass instance or nil
function Session.get(session_uuid)
    if plugin_worker_hub_proxy_enabled() then
        for _, sess in ipairs(proxy_session_list({ include_system = true })) do
            if sess.session_uuid == session_uuid or sess.id == session_uuid then
                return sess
            end
        end
        return nil
    end
    return sessions[session_uuid]
end


--- List all sessions in creation order.
-- @return array of Session subclass instances
function Session.list()
    if plugin_worker_hub_proxy_enabled() then
        return proxy_session_list()
    end
    local result = {}
    for _, sess in pairs(sessions) do
        table.insert(result, sess)
    end
    table.sort(result, function(a, b)
        return (a.created_at or 0) < (b.created_at or 0)
    end)
    return result
end


--- Find sessions by metadata key-value pair.
-- @param key string Metadata key to match
-- @param value any Value to match
-- @return array of Session subclass instances
function Session.find_by_meta(key, value)
    if plugin_worker_hub_proxy_enabled() then
        local result = {}
        for _, sess in ipairs(proxy_session_list()) do
            if sess.metadata and sess.metadata[key] == value then
                result[#result + 1] = sess
            end
        end
        return result
    end
    local result = {}
    for _, sess in ipairs(Session.list()) do
        if sess.metadata and sess.metadata[key] == value then
            result[#result + 1] = sess
        end
    end
    return result
end

--- Find all running sessions matching a workspace name.
-- @param name string  Workspace name (e.g. "owner/repo#42")
-- @param target table|nil Optional target context to scope the lookup
-- @return array of Session subclass instances
function Session.find_by_workspace(name, target)
    if plugin_worker_hub_proxy_enabled() then
        local result = {}
        for _, sess in ipairs(proxy_session_list()) do
            if sess.workspace_name == name and (not target or TargetContext.matches(sess, target)) then
                result[#result + 1] = sess
            end
        end
        return result
    end
    local result = {}
    for _, sess in ipairs(Session.list()) do
        if sess._workspace_name == name and (not target or TargetContext.matches(sess, target)) then
            result[#result + 1] = sess
        end
    end
    return result
end


--- Count active sessions.
-- @return number
function Session.count()
    if plugin_worker_hub_proxy_enabled() then
        return #proxy_session_list()
    end
    local count = 0
    for _ in pairs(sessions) do
        count = count + 1
    end
    return count
end

--- Whether a session/info record is internal-only and should be hidden from
-- normal client payloads.
-- @param subject table|nil Session instance or info table
-- @return boolean
function Session.is_system_session(subject)
    local metadata = type(subject) == "table" and subject.metadata or nil
    return type(metadata) == "table"
        and (metadata.system_session == true or metadata.system_session == "true")
end

--- Get info tables for all sessions (for client broadcast).
-- By default, internal system sessions are excluded.
-- @param opts table|nil { include_system = boolean }
-- @return array of info tables sorted by creation time
function Session.all_info(opts)
    if plugin_worker_hub_proxy_enabled() then
        return proxy_session_list(opts)
    end
    local result = {}
    for _, entry in pairs(sessions) do
        if (opts and opts.include_system) or not Session.is_system_session(entry) then
            result[#result + 1] = entry:info()
        end
    end
    table.sort(result, function(a, b)
        local ac = tonumber(a.created_at) or 0
        local bc = tonumber(b.created_at) or 0
        if ac == bc then
            return tostring(a.id or "") < tostring(b.id or "")
        end
        return ac < bc
    end)
    return result
end

-- =============================================================================
-- Lifecycle Hooks for Hot-Reload
-- =============================================================================

function Session._before_reload()
    log.info("session.lua reloading (persistent metatable -- instances auto-upgrade)")
end

function Session._after_reload()
    log.info(string.format("session.lua reloaded -- %d sessions preserved", Session.count()))
end

return Session
