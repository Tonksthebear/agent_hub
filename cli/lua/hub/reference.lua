-- hub/reference.lua — Single source of truth for Botster's plugin API surface.
--
-- Used by MCP prompts (mcp_defaults.lua) to dynamically generate reference
-- sections. Update this file when primitives, hooks, or events change —
-- all prompts automatically reflect the update.
--
-- Data only, no side effects. Safe to require() from anywhere.

local M = {}

-- =============================================================================
-- 1. RUST PRIMITIVES (globals registered by cli/src/lua/primitives/)
-- =============================================================================

M.primitives = {
    {
        name = "log",
        description = "Structured logging (target: \"lua\")",
        methods = {
            { sig = "log.info(msg)",  desc = "Info level" },
            { sig = "log.warn(msg)",  desc = "Warning level" },
            { sig = "log.error(msg)", desc = "Error level" },
            { sig = "log.debug(msg)", desc = "Debug level" },
        },
    },
    {
        name = "json",
        description = "JSON encode/decode and file operations",
        methods = {
            { sig = "json.encode(value)",               desc = "Table → compact JSON string" },
            { sig = "json.decode(str)",                  desc = "JSON string → table" },
            { sig = "json.encode_pretty(value)",         desc = "Pretty-printed JSON" },
            { sig = "json.file_get(path, key_path)",     desc = "Read nested value from JSON file (dot notation)" },
            { sig = "json.file_set(path, key_path, val)", desc = "Set nested value in JSON file" },
            { sig = "json.file_delete(path, key_path)",  desc = "Delete key from JSON file" },
        },
    },
    {
        name = "fs",
        description = "File system operations",
        methods = {
            { sig = "fs.read(path)",          desc = "Read file as UTF-8 string → (str, nil) or (nil, err)" },
            { sig = "fs.read_bytes(path)",    desc = "Read file as binary" },
            { sig = "fs.write(path, content)", desc = "Write file, creates parent dirs" },
            { sig = "fs.append(path, content)", desc = "Append to file, creates if absent" },
            { sig = "fs.exists(path)",        desc = "Check if path exists → boolean" },
            { sig = "fs.copy(src, dst)",      desc = "Copy file" },
            { sig = "fs.listdir(path)",       desc = "List directory entries (no . or ..)" },
            { sig = "fs.is_dir(path)",        desc = "Check if path is a directory" },
            { sig = "fs.rename(from, to)",    desc = "Rename/move file or directory" },
            { sig = "fs.rmdir(path)",         desc = "Recursively remove directory" },
            { sig = "fs.delete(path)",        desc = "Delete a file" },
            { sig = "fs.mkdir(path)",         desc = "Create directory (recursive)" },
            { sig = "fs.stat(path)",          desc = "File metadata (size, modified, etc.)" },
        },
    },
    {
        name = "http",
        description = "HTTP client (sync convenience + async with callback)",
        methods = {
            { sig = "http.get(url)",             desc = "Sync GET → {status, body, headers}" },
            { sig = "http.post(url, data?)",     desc = "Sync POST with optional JSON body" },
            { sig = "http.put(url, data?)",      desc = "Sync PUT" },
            { sig = "http.delete(url)",          desc = "Sync DELETE" },
            { sig = "http.request(opts, cb)",    desc = "Async: opts={method,url,headers?,body?,timeout_ms?}, cb(resp,err)" },
        },
    },
    {
        name = "timer",
        description = "One-shot, repeating, and idle-aware timers",
        methods = {
            { sig = "timer.after(secs, cb)",            desc = "One-shot timer, fires once after delay → timer_id" },
            { sig = "timer.every(secs, cb)",            desc = "Repeating timer → timer_id" },
            { sig = "timer.after_idle(id, secs, cb)",   desc = "Resettable idle timer — resets on each call with same id" },
            { sig = "timer.cancel(timer_id)",           desc = "Cancel a timer → boolean" },
        },
    },
    {
        name = "secrets",
        description = "AES-256-GCM encrypted secret storage",
        methods = {
            { sig = "secrets.set(namespace, key, value)", desc = "Store encrypted secret" },
            { sig = "secrets.get(namespace, key)",        desc = "Retrieve secret → (str, nil) or (nil, err)" },
            { sig = "secrets.delete(namespace, key)",     desc = "Delete a secret" },
        },
    },
    {
        name = "config",
        description = "Hub configuration and environment",
        methods = {
            { sig = "config.get(key)",     desc = "Read config key → (value, nil) or (nil, err)" },
            { sig = "config.set(key, val)", desc = "Write config key" },
            { sig = "config.all()",        desc = "Entire config as table" },
            { sig = "config.lua_path()",   desc = "Lua scripts base path" },
            { sig = "config.data_dir()",   desc = "~/.botster directory path" },
            { sig = "config.template_catalog_path()", desc = "Explicit local template catalog root" },
            { sig = "config.server_url()", desc = "Botster server URL" },
            { sig = "config.env(key)",     desc = "Read environment variable" },
            { sig = "config.find_available_port(start, finish, excluded?)", desc = "Find first bindable localhost port in range" },
        },
    },
    {
        name = "events",
        description = "General-purpose pub/sub event system",
        methods = {
            { sig = "events.on(name, cb)",    desc = "Subscribe to event → subscription_id" },
            { sig = "events.off(sub_id)",     desc = "Unsubscribe by ID" },
            { sig = "events.has(name)",       desc = "Check if callbacks registered" },
            { sig = "events.emit(name, ...)", desc = "Fire all callbacks synchronously → count" },
        },
    },
    {
        name = "worktree",
        description = "Git worktree operations",
        methods = {
            { sig = "worktree.list()",             desc = "All worktrees → array of {path, branch}" },
            { sig = "worktree.exists(branch)",     desc = "Check if worktree exists for branch" },
            { sig = "worktree.find(branch)",       desc = "Find worktree path for branch → string or nil" },
            { sig = "worktree.repo_root()",        desc = "Repo root directory" },
            { sig = "worktree.create_async(opts)", desc = "Async create; opts.repo_root targets another repo" },
            { sig = "worktree.delete(path, branch)", desc = "Async delete worktree" },
        },
    },
    {
        name = "watch",
        description = "Filesystem directory watching",
        methods = {
            { sig = "watch.directory(path, opts?, cb)", desc = "Watch dir for changes. cb({path,kind}), kind: create/modify/rename/delete → watch_id" },
            { sig = "watch.unwatch(watch_id)",          desc = "Stop watching" },
        },
    },
    {
        name = "websocket",
        description = "WebSocket client connections",
        methods = {
            { sig = "websocket.connect(url, callbacks)", desc = "Open WebSocket. callbacks: {on_open, on_message, on_close, on_error} → conn_id" },
            { sig = "websocket.send(conn_id, text)",     desc = "Send text frame" },
            { sig = "websocket.close(conn_id)",          desc = "Graceful close" },
        },
    },
    {
        name = "action_cable",
        description = "ActionCable WebSocket (Rails)",
        methods = {
            { sig = "action_cable.connect(opts?)",                  desc = "Connect to hub's ActionCable. opts={crypto=true} for E2E → conn_id" },
            { sig = "action_cable.subscribe(conn, chan, params, cb)", desc = "Subscribe to channel. cb(msg, ch_id) → channel_id" },
            { sig = "action_cable.perform(ch_id, action, data)",    desc = "Perform action on channel" },
            { sig = "action_cable.unsubscribe(ch_id)",              desc = "Unsubscribe from channel" },
            { sig = "action_cable.close(conn_id)",                  desc = "Close connection and all channels" },
        },
    },
    {
        name = "hub",
        description = "Hub state and operations",
        methods = {
            { sig = "hub.hub_id()",                desc = "Local hub identifier (stable hash)" },
            { sig = "hub.server_id()",             desc = "Server-assigned hub ID (after registration)" },
            { sig = "hub.detect_repo()",           desc = "Current repo name (owner/name)" },
            { sig = "hub.api_token()",             desc = "Hub's API bearer token" },
            { sig = "hub.get_worktrees()",         desc = "Available worktrees from cache" },
            { sig = "hub.spawn_session(config)",   desc = "Spawn a new session process" },
            { sig = "hub.connect_session(uuid, conn)", desc = "Connect to a session socket" },
            { sig = "hub.register_session(uuid, handle, meta)", desc = "Register PTY handle with hub" },
            { sig = "hub.unregister_session(uuid)", desc = "Unregister PTY handle" },
            { sig = "hub.issue_mcp_caller(opts)", desc = "Issue or reuse the session caller credential for the shared HTTP MCP URL" },
            { sig = "hub.revoke_mcp_caller(uuid)", desc = "Revoke the session caller credential" },
            { sig = "hub.mcp_url()", desc = "Shared Streamable HTTP MCP URL, or nil" },
            { sig = "hub.notify_mcp_list_changed(kind)", desc = "Notify HTTP MCP callers that tools, prompts, or resources changed" },
            { sig = "hub.update_session_metadata(uuid, meta)", desc = "Update label/workspace_id without re-registering" },
            { sig = "hub.is_offline()",            desc = "Check if hub is in offline mode" },
            { sig = "hub.quit()",                  desc = "Request hub shutdown" },
            { sig = "hub.graceful_restart()",      desc = "Restart hub (sessions survive)" },
            { sig = "hub.resolve_command_path(command)", desc = "Resolve an executable against the live PATH" },
            { sig = "Hub.get():prepare_plugin_command(opts)", desc = "Async plugin command prep; fires plugin_command_prepared with error_kind=command_blank|command_missing|config_write_failed|task_failed" },
        },
    },
    {
        name = "pty",
        description = "Direct PTY operations",
        methods = {
            { sig = "pty.spawn(config)",                 desc = "Spawn a PTY process → PtySessionHandle" },
            { sig = "pty.request_pty_snapshot(uuid)",     desc = "Request terminal snapshot from session" },
        },
    },
    {
        name = "connection",
        description = "Pairing code management (mobile/browser)",
        methods = {
            { sig = "connection.get_url()",          desc = "Get cached connection URL" },
            { sig = "connection.generate()",          desc = "Lazy-generate a new connection code" },
            { sig = "connection.regenerate()",         desc = "Force regeneration" },
            { sig = "connection.copy_to_clipboard()", desc = "Copy URL to system clipboard (OSC 52)" },
        },
    },
    {
        name = "push",
        description = "Web push notifications to subscribed browsers",
        methods = {
            { sig = "push.send(opts)", desc = "Send push. opts={kind (required), title?, body?, url?, icon?, tag?}" },
        },
    },
    {
        name = "hub_discovery",
        description = "Discover other running hubs on this machine",
        methods = {
            { sig = "hub_discovery.list()",              desc = "All running hubs → array of {id, pid, socket}" },
            { sig = "hub_discovery.is_running(hub_id)",  desc = "Check if specific hub has live process" },
            { sig = "hub_discovery.socket_path(hub_id)", desc = "Unix socket path for hub" },
            { sig = "hub_discovery.manifest_path(hub_id)", desc = "Manifest path for hub" },
        },
    },
    {
        name = "hub_client",
        description = "Hub-to-hub socket connections (for orchestration)",
        methods = {
            { sig = "hub_client.connect(hub_id, cb)",         desc = "Connect to another hub. cb(message) → conn_id" },
            { sig = "hub_client.send(conn_id, data)",         desc = "Send JSON frame to connected hub" },
            { sig = "hub_client.request(conn_id, data, timeout_ms)", desc = "Blocking request/response with timeout" },
            { sig = "hub_client.close(conn_id)",              desc = "Close connection" },
        },
    },
    {
        name = "spawn_targets",
        description = "Admitted spawn target registry",
        methods = {
            { sig = "spawn_targets.list()",          desc = "All targets → array" },
            { sig = "spawn_targets.get(id)",         desc = "Get target by ID" },
            { sig = "spawn_targets.add(path, name, plugins)", desc = "Add a new target" },
            { sig = "spawn_targets.update(id, name, enabled, plugins)", desc = "Update target" },
            { sig = "spawn_targets.enable(id)",      desc = "Enable target" },
            { sig = "spawn_targets.disable(id)",     desc = "Disable target" },
            { sig = "spawn_targets.remove(id)",      desc = "Remove target" },
        },
    },
    {
        name = "update",
        description = "CLI self-update",
        methods = {
            { sig = "update.check()", desc = "Check for available update" },
            { sig = "update.install()", desc = "Install available update" },
        },
    },
    {
        name = "webrtc",
        description = "WebRTC peer communication (browser clients)",
        methods = {
            { sig = "webrtc.on_peer_connected(cb)",    desc = "Register callback for peer connect. cb(peer_id)" },
            { sig = "webrtc.on_peer_disconnected(cb)", desc = "Register callback for peer disconnect" },
            { sig = "webrtc.on_message(cb)",           desc = "Register callback for peer messages. cb(peer_id, msg)" },
            { sig = "webrtc.send(peer_id, table)",     desc = "Send JSON to peer" },
            { sig = "webrtc.send_binary(peer_id, data)", desc = "Send binary to peer" },
        },
    },
    {
        name = "socket",
        description = "Unix socket IPC (hub-to-hub server side)",
        methods = {
            { sig = "socket.on_client_connected(cb)",    desc = "cb(client_id) on new socket client" },
            { sig = "socket.on_client_disconnected(cb)", desc = "cb(client_id) on disconnect" },
            { sig = "socket.on_message(cb)",             desc = "cb(client_id, message) on incoming JSON" },
            { sig = "socket.send(client_id, table)",     desc = "Send JSON to client" },
            { sig = "socket.send_binary(client_id, data)", desc = "Send binary to client" },
        },
    },
    {
        name = "tui",
        description = "TUI terminal connection",
        methods = {
            { sig = "tui.on_connected(cb)",    desc = "TUI connection ready" },
            { sig = "tui.on_disconnected(cb)", desc = "TUI disconnected" },
            { sig = "tui.on_message(cb)",      desc = "cb(message) on TUI message" },
            { sig = "tui.send(table)",         desc = "Send JSON to TUI" },
            { sig = "tui.send_binary(data)",   desc = "Send binary to TUI" },
        },
    },
}

-- =============================================================================
-- 2. HOOKS (observers + interceptors via hub/hooks.lua)
-- =============================================================================

M.hooks = {
    observers = {
        -- Agent lifecycle
        { name = "after_agent_create",     data = "agent (live Session instance)",    desc = "Agent PTY spawned and registered" },
        { name = "before_agent_close",     data = "agent (live Session instance)",    desc = "About to close agent (worktree still exists)" },
        { name = "after_agent_close",      data = "agent (live Session instance)",    desc = "Agent fully closed and unregistered" },
        { name = "agent_created",          data = "session info table",                desc = "Intrinsic session info emitted on create/recovery; client transports may decorate it further" },
        { name = "agent_deleted",          data = "session_uuid string",              desc = "Agent removed from registry" },
        { name = "agent_lifecycle",        data = "{session_uuid, status, ...}",      desc = "Status change during creation flow" },
        { name = "session_updated",        data = "{session_uuid}",                   desc = "Any field changed via Session:update()" },

        -- Client lifecycle
        { name = "client_connected",       data = "{peer_id, transport}",             desc = "New client connected (transport: 'webrtc' or 'tui')" },
        { name = "client_disconnected",    data = "{peer_id, transport}",             desc = "Client disconnected" },
        { name = "client_subscribed",      data = "{peer_id, channel, sub_id}",       desc = "Client subscribed to a session" },
        { name = "after_client_subscribe", data = "{client, sub_id, agent}",          desc = "Subscription complete (has client object)" },
        { name = "client_unsubscribed",    data = "{peer_id, sub_id}",               desc = "Client unsubscribed from a session" },
        { name = "before_client_disconnect", data = "{peer_id}",                     desc = "About to disconnect client" },

        -- PTY events (fired from Rust)
        { name = "pty_title_changed",      data = "{session_uuid, session_name, title}",     desc = "Terminal title changed (OSC 0/2)" },
        { name = "pty_cwd_changed",        data = "{session_uuid, session_name, cwd}",       desc = "Working directory changed (OSC 7)" },
        { name = "pty_prompt",             data = "{session_uuid, session_name, mark, exit_code?, command?}", desc = "Shell prompt mark (OSC 133). mark: prompt_start|command_start|command_executed|command_finished" },
        { name = "pty_cursor_visibility",  data = "{session_uuid, session_name, visible}",   desc = "Cursor shown/hidden (DECTCEM)" },
        { name = "_pty_notification_raw",  data = "{session_uuid, session_name, type, message?, title?, body?}", desc = "Raw PTY notification before enrichment and scoped notification policy (internal)" },
        { name = "pty_notification",       data = "{session_uuid, type, message?, title?, body?, has_focus, already_notified, notification_policy?, notification_owner?}", desc = "Final PTY notification delivery after scoped policy accepts" },
        { name = "pty_notification_suppressed", data = "{session_uuid, type, message?, title?, body?, notification_policy?, notification_owner?}", desc = "Final PTY notification suppression after scoped policy rejects delivery" },
        { name = "pty_input",             data = "{session_uuid}",                           desc = "Keystroke cleared a pending notification" },

        -- Commands
        { name = "after_hub_command",      data = "{command, client, sub_id, success, error}", desc = "After a hub command executed" },

        -- Worktree / workspace
        { name = "worktree_created",       data = "{path, branch}",                  desc = "Worktree created (from hooks.notify in agents.lua)" },
        { name = "worktree_deleted",       data = "{path, branch}",                  desc = "Worktree deleted during agent close" },
        { name = "workspace_closed",       data = "{workspace_id, name}",            desc = "Last session in workspace closed" },

        -- Inter-hub
        { name = "hub_rpc_request",        data = "(client_id, msg)",                desc = "Incoming RPC from another hub via socket" },

        -- Plugin lifecycle
        { name = "plugin_status_changed",  data = "{name, status, error?}",          desc = "Plugin loaded, errored, enabled, or disabled" },
    },

    interceptors = {
        { name = "before_agent_create",     data = "{issue_or_branch, prompt, agent_name, ...}", returns = "modified params or nil to block" },
        { name = "before_agent_delete",     data = "{session_uuid, delete_worktree}",              returns = "modified config or nil to block" },
        { name = "before_command",          data = "{type, args, peer_id}",                        returns = "modified command or nil to block" },
        { name = "before_hub_command",      data = "command table",                                returns = "modified or nil to block" },
        { name = "before_client_subscribe", data = "{client, sub_id, ...}",                        returns = "modified context or nil to block" },
        { name = "before_pty_spawn",        data = "{command, args, cwd, env, init_commands, metadata, ...}", returns = "modified spawn context or nil to block" },
        { name = "filter_agent_env",        data = "(env_table, agent)",                           returns = "modified env table" },
    },
}

-- =============================================================================
-- 3. EVENTS (events.on/events.emit system — distinct from hooks)
-- =============================================================================

M.events = {
    { name = "shutdown",               data = "nil",                           desc = "Hub shutting down" },
    { name = "process_exited",         data = "{session_uuid, session_name, exit_code}", desc = "PTY process exited" },
    { name = "session_process_exited", data = "{session_uuid, exit_code}",     desc = "Session process exited (distinct from PTY)" },
    { name = "session_reconnected",    data = "{session_uuid, title?, cwd?}", desc = "Hub reconnected to session after reader death" },
    { name = "connection_code_ready",  data = "{url, qr_ascii}",              desc = "Pairing QR code generated" },
    { name = "connection_code_error",  data = "error string",                  desc = "Pairing code generation failed" },
    { name = "hub_recovery_state",     data = "{state, server_hub_id?, error?}", desc = "Hub recovery lifecycle (recovering/ready/error)" },
    { name = "sessions_discovered",    data = "{sockets=[{uuid,name},...]}",   desc = "Live sessions found on hub restart" },
    { name = "worktree_created",       data = "{branch, path, ...}",           desc = "Async worktree creation succeeded" },
    { name = "worktree_create_failed", data = "{branch, error}",              desc = "Async worktree creation failed" },
    { name = "outgoing_signal",        data = "{browser_identity, envelope}",  desc = "Encrypted signaling message to relay" },
    { name = "mcp_tools_changed",      data = "nil",                           desc = "MCP tool registry changed" },
    { name = "mcp_prompts_changed",    data = "nil",                           desc = "MCP prompt registry changed" },
    { name = "hub_connected",          data = "{hub_id, conn_id}",            desc = "Remote hub connected (orchestration)" },
}

-- =============================================================================
-- 4. SESSION:INFO() FIELDS
-- =============================================================================

M.session_info_fields = {
    { name = "id",             type = "string",      desc = "Generic entity ID; equals session_uuid for session entities" },
    { name = "session_uuid",   type = "string",      desc = "Session UUID — primary identifier" },
    { name = "session_type",   type = "string",      desc = "'agent' or 'accessory'" },
    { name = "session_name",   type = "string",      desc = "PTY session name" },
    { name = "display_name",   type = "string",      desc = "Best name: OSC title > agent_name > branch_name" },
    { name = "title",          type = "string?",     desc = "Terminal title set by running program (OSC 0/2)" },
    { name = "cwd",            type = "string?",     desc = "Current working directory (OSC 7)" },
    { name = "agent_name",     type = "string?",     desc = "Config agent name (e.g. 'codex')" },
    { name = "repo",           type = "string?",     desc = "Repository identifier (owner/repo)" },
    { name = "target_id",      type = "string?",     desc = "Spawn target ID" },
    { name = "target_name",    type = "string?",     desc = "Spawn target display name" },
    { name = "target_path",    type = "string?",     desc = "Spawn target root path" },
    { name = "target_repo",    type = "string?",     desc = "Live repo identity for the target" },
    { name = "metadata",       type = "table",       desc = "Plugin key-value store" },
    { name = "recovery_source", type = "string",      desc = "'created', 'manifest', or 'process_identity'" },
    { name = "canonical",      type = "boolean",     desc = "False when recovered from degraded process identity instead of manifest" },
    { name = "workspace_name", type = "string?",     desc = "Workspace display name" },
    { name = "workspace_id",   type = "string?",     desc = "Workspace ID" },
    { name = "branch_name",    type = "string",      desc = "Git branch name" },
    { name = "worktree_path",  type = "string",      desc = "Filesystem path to worktree" },
    { name = "in_worktree",    type = "boolean",     desc = "True if running in a git worktree" },
    { name = "status",         type = "string?",     desc = "Current status" },
    { name = "notification",   type = "boolean",     desc = "Pending notification flag" },
    { name = "port",           type = "number?",     desc = "Forwarded port (if configured)" },
    { name = "created_at",     type = "number",      desc = "Unix timestamp of creation" },
    { name = "label",          type = "string?",     desc = "User-assigned label" },
    { name = "task",           type = "string?",     desc = "Current task description" },
    { name = "output_activity", type = '"active"|"idle"', desc = "Recent PTY output activity" },
}

-- =============================================================================
-- 5. MCP API (from lib/mcp.lua — tools and prompts only, skip resources)
-- =============================================================================

M.mcp_api = {
    tools = {
        { sig = "mcp.tool(name, schema, handler)",            desc = "Register a tool. handler(params, context) → string or table" },
        { sig = "mcp.remove_tool(name)",                      desc = "Remove a tool by name" },
        { sig = "mcp.list_tools(session_uuid?)",              desc = "List tools (optionally scoped to session's plugins)" },
        { sig = "mcp.count()",                                desc = "Count registered tools" },
    },
    prompts = {
        { sig = "mcp.prompt(name, schema, handler)",          desc = "Register a prompt. handler(args) → string or message shape" },
        { sig = "mcp.remove_prompt(name)",                    desc = "Remove a prompt by name" },
        { sig = "mcp.list_prompts(session_uuid?)",             desc = "List prompts (optionally scoped to session's plugins)" },
        { sig = "mcp.get_prompt(name, args, context?)",        desc = "Execute a prompt handler with optional caller context" },
        { sig = "mcp.list_resource_templates(session_uuid?)",  desc = "List resource templates (optionally scoped to session's plugins)" },
        { sig = "mcp.caller_context(session_uuid)",            desc = "Build live MCP caller context from the session manifest" },
        { sig = "mcp.count_prompts()",                        desc = "Count registered prompts" },
    },
    proxy = {
        { sig = "mcp.proxy(url, opts)",       desc = "Register a remote MCP server as a proxy (merges its tools)" },
        { sig = "mcp.remove_proxy(url)",      desc = "Remove a proxy" },
    },
}

-- =============================================================================
-- 6. LUA LIB CLASSES
-- =============================================================================

M.lua_libs = {
    {
        name = "Hub proxy",
        require_path = "lib.hub",
        description = "Local/remote hub orchestration helper for plugins.",
        class_methods = {
            { sig = "Hub.get(hub_id?)", desc = "Return local hub or transparent remote proxy" },
        },
        instance_methods = {
            { sig = "hub:create_agent(opts)", desc = "Spawn a new session with target/workspace/prompt metadata" },
            { sig = "hub:send_message(session_uuid, text, session?)", desc = "Write raw PTY text to an existing session" },
            { sig = "hub:notify(session_uuid, opts)", desc = "Attention-only notification; durable data stays in the owning plugin. opts={source,title,body,hint,action}" },
            { sig = "hub:post(session_uuid, opts)", desc = "Durable generic inbox message; recipient drains with Agent.receive_messages()/receive_messages()" },
            { sig = "hub:receive_messages(session_uuid)", desc = "Drain generic inbox messages for a session" },
            { sig = "hub:list_owned_sessions(owner_plugin)", desc = "Recover plugin-owned sessions after reload" },
            { sig = "hub:entity_snapshot(entity_type, items, opts?)", desc = "Publish plugin-owned entity_snapshot after a plugin refresh" },
            { sig = "hub:entity_upsert(entity_type, entity, opts?)", desc = "Publish plugin-owned entity_upsert; entity.id must be a non-empty string" },
            { sig = "hub:entity_patch(entity_type, id, patch, opts?)", desc = "Publish plugin-owned entity_patch with sparse top-level fields" },
            { sig = "hub:entity_remove(entity_type, id, opts?)", desc = "Publish plugin-owned entity_remove" },
        },
    },
    {
        name = "Agent / Session",
        require_path = "lib.agent",
        description = "Agent inherits from Session. Use Agent in plugins.",
        class_methods = {
            { sig = "Agent.get(session_uuid)",       desc = "Lookup by session_uuid → instance or nil" },
            { sig = "Agent.list()",                  desc = "All sessions in creation order" },
            { sig = "Agent.find_by_meta(key, val)",  desc = "Find sessions by metadata key-value" },
            { sig = "Agent.all_info()",              desc = "Array of intrinsic session info tables" },
            { sig = "Agent.count()",                 desc = "Count of active sessions" },
            { sig = "Agent.receive_messages(uuid)",  desc = "Drain an agent's inbox → array of envelopes" },
        },
        instance_methods = {
            { sig = "agent:info()",                  desc = "Serializable session info table (see info fields reference)" },
            { sig = "agent:close(delete_worktree?)", desc = "Close the session" },
            { sig = "agent:update(fields)",          desc = "Update fields (label, task, status, etc.)" },
            { sig = "agent:set_meta(key, value)",    desc = "Set metadata key-value" },
            { sig = "agent:get_meta(key)",           desc = "Get metadata value" },
            { sig = "agent.session_uuid",            desc = "Session UUID (field, not method)" },
        },
    },
    {
        name = "commands",
        require_path = "lib.commands",
        description = "Hub command registry (TUI palette + programmatic dispatch)",
        methods = {
            { sig = "commands.register(type, handler, opts?)", desc = "Register a command. handler(client, sub_id, command). opts={description}" },
            { sig = "commands.unregister(type)",               desc = "Remove a command" },
            { sig = "commands.list()",                         desc = "List all registered commands" },
        },
    },
    {
        name = "hooks",
        require_path = "hub.hooks",
        description = "Observer and interceptor system",
        methods = {
            { sig = "hooks.on(event, name, fn, opts?)",        desc = "Register observer. opts={priority}" },
            { sig = "hooks.off(event, name)",                  desc = "Remove observer" },
            { sig = "hooks.intercept(event, name, fn, opts?)", desc = "Register interceptor. opts={timeout_ms, priority}" },
            { sig = "hooks.unintercept(event, name)",          desc = "Remove interceptor" },
            { sig = "hooks.enable(event, name)",               desc = "Re-enable a disabled hook" },
            { sig = "hooks.disable(event, name)",              desc = "Disable without removing" },
            { sig = "hooks.list(event)",                       desc = "List hooks for an event" },
            { sig = "hooks.list_events()",                     desc = "List all event names with hooks" },
        },
    },
    {
        name = "hub.state",
        require_path = "hub.state",
        description = "In-memory key-value store, survives hot-reloads",
        methods = {
            { sig = 'state.get("key", default_table)', desc = "Get or initialize a persistent table" },
        },
    },
}

-- =============================================================================
-- FORMATTERS — Turn data into prompt-ready markdown sections
-- =============================================================================

--- Format primitives into a compact reference section.
--- @param names table|nil Optional array of primitive names to include. nil = all.
--- @return string Markdown text
function M.format_primitives(names)
    local lines = { "## Available Primitives", "" }
    for _, p in ipairs(M.primitives) do
        if not names or M._contains(names, p.name) then
            table.insert(lines, string.format("**%s** — %s", p.name, p.description))
            local sigs = {}
            for _, m in ipairs(p.methods) do
                table.insert(sigs, "  " .. m.sig)
            end
            table.insert(lines, table.concat(sigs, "\n"))
            table.insert(lines, "")
        end
    end
    return table.concat(lines, "\n")
end

--- Format hooks into a reference section.
--- @param opts table|nil {observers=bool, interceptors=bool}. nil = both.
--- @return string Markdown text
function M.format_hooks(opts)
    opts = opts or {}
    local show_obs = opts.observers ~= false
    local show_int = opts.interceptors ~= false
    local lines = { "## Hooks" }

    if show_obs then
        table.insert(lines, "")
        table.insert(lines, "### Observers (hooks.on)")
        table.insert(lines, "Fire-and-forget. Cannot block or transform data.")
        table.insert(lines, "")
        for _, h in ipairs(M.hooks.observers) do
            -- Skip internal hooks (prefixed with _)
            if h.name:sub(1, 1) ~= "_" then
                table.insert(lines, string.format("  %-28s %s", h.name, h.desc))
                table.insert(lines, string.format("  %-28s data: %s", "", h.data))
            end
        end
    end

    if show_int then
        table.insert(lines, "")
        table.insert(lines, "### Interceptors (hooks.intercept)")
        table.insert(lines, "Synchronous, blocking. Return modified value to allow, nil to block.")
        table.insert(lines, "")
        for _, h in ipairs(M.hooks.interceptors) do
            table.insert(lines, string.format("  %-28s → %s", h.name, h.returns))
            table.insert(lines, string.format("  %-28s data: %s", "", h.data))
        end
    end

    return table.concat(lines, "\n")
end

--- Format events into a reference section.
--- @return string Markdown text
function M.format_events()
    local lines = {
        "## Events (events.on / events.emit)",
        "",
        "General pub/sub, distinct from hooks. Use events.on(name, callback) to subscribe.",
        "",
    }
    for _, e in ipairs(M.events) do
        table.insert(lines, string.format("  %-26s %s", e.name, e.desc))
        if e.data ~= "nil" then
            table.insert(lines, string.format("  %-26s data: %s", "", e.data))
        end
    end
    return table.concat(lines, "\n")
end

--- Format Session:info() fields.
--- @return string Markdown text
function M.format_info_fields()
    local lines = {
        "## agent:info() Fields",
        "",
        "Returns a serializable table. All fields:",
        "",
    }
    for _, f in ipairs(M.session_info_fields) do
        table.insert(lines, string.format("  %-18s %-10s %s", f.name, f.type, f.desc))
    end
    return table.concat(lines, "\n")
end

--- Format MCP API reference.
--- @param sections table|nil Optional array: {"tools", "prompts", "proxy"}. nil = all.
--- @return string Markdown text
function M.format_mcp_api(sections)
    local lines = { "## MCP API", "" }
    local show = {}
    if sections then
        for _, s in ipairs(sections) do show[s] = true end
    end

    for _, section_name in ipairs({"tools", "prompts", "proxy"}) do
        local items = M.mcp_api[section_name]
        if items and (not sections or show[section_name]) then
            table.insert(lines, string.format("### %s", section_name:sub(1, 1):upper() .. section_name:sub(2)))
            table.insert(lines, "")
            for _, item in ipairs(items) do
                table.insert(lines, string.format("  %-50s %s", item.sig, item.desc))
            end
            table.insert(lines, "")
        end
    end
    return table.concat(lines, "\n")
end

--- Format Lua lib class reference.
--- @param names table|nil Optional array of lib names to include. nil = all.
--- @return string Markdown text
function M.format_lua_libs(names)
    local lines = { "## Lua Libraries", "" }
    for _, lib in ipairs(M.lua_libs) do
        if not names or M._contains(names, lib.name) then
            table.insert(lines, string.format("**%s** (`require(\"%s\")`) — %s", lib.name, lib.require_path, lib.description))
            table.insert(lines, "")
            if lib.class_methods then
                for _, m in ipairs(lib.class_methods) do
                    table.insert(lines, string.format("  %-45s %s", m.sig, m.desc))
                end
            end
            if lib.instance_methods then
                for _, m in ipairs(lib.instance_methods) do
                    table.insert(lines, string.format("  %-45s %s", m.sig, m.desc))
                end
            end
            if lib.methods then
                for _, m in ipairs(lib.methods) do
                    table.insert(lines, string.format("  %-45s %s", m.sig, m.desc))
                end
            end
            table.insert(lines, "")
        end
    end
    return table.concat(lines, "\n")
end

-- Internal helper
function M._contains(tbl, val)
    for _, v in ipairs(tbl) do
        if v == val then return true end
    end
    return false
end

return M
