-- cli/lua/lib/plugin_db.lua
--
-- plugin.db() — public persistence API for Lua plugins.
--
-- Plugin authors call `plugin.db{...}` once during plugin load. The module:
--   1. Resolves the db path: {config.data_dir()}/plugin-data/<plugin_name>/db.sqlite
--      (or ":memory:" if `memory = true` was declared).
--   2. Opens a sqlite connection via vendored `sqlite.lua` and applies default
--      PRAGMAs (WAL, synchronous=NORMAL, foreign_keys=ON, busy_timeout=5000).
--   3. Runs the declared schema + migrations by comparing `PRAGMA user_version`
--      to `spec.version`. Upgrade path applies additive changes + the author's
--      `migrations[N]` step function, per step, inside a single transaction.
--      Steady-state (current == target) reconciles additive changes only and
--      errors on non-additive drift (removed/retyped/nullability-changed cols).
--   4. Attaches per-model `sqlite.tbl` wrappers to the db instance so plugin
--      code writes `db.messages:insert{...}` / `db.messages:get{...}`.
--   5. Caches the db handle keyed by plugin name so hot-reloading a plugin
--      reuses the connection instead of leaking fds.
--
-- See `docs/plugin-authoring.md` for the author-facing guide.

local M = {}

local sqlite = require("vendor.sqlite")
-- Require the tbl constructor directly — `sqlite.tbl` (through the base
-- module's __index) resolves to `db:tbl`, a method closure, not the class.
local sqlite_tbl = require("sqlite.tbl")
local sqlite_db_class = require("sqlite.db")
local sqlite_defs = require("sqlite.defs")
local hub_state = require("hub.state")

-- Patch `sqlite.defs.wrap_stmts` once at module load. Upstream's implementation
-- wraps its callback in bare BEGIN/COMMIT without checking either statement or
-- rolling back after a callback error. A failed statement can therefore leave
-- the connection inside an open transaction and block every later writer.
--
-- When the user calls
-- `db:execute(fn)` and `fn` invokes `db.t:insert{...}` (which internally calls
-- `wrap_stmts`) the inner BEGIN errors quietly and the inner COMMIT then
-- closes the user's outer transaction — silently defeating rollback. We swap
-- in a shim that provides checked BEGIN/COMMIT/ROLLBACK handling and skips its
-- own transaction when a connection is already inside a plugin-db user
-- transaction, so the outer tx controls commit/rollback.
-- `sqlite.defs` installs a metatable __index that falls through to FFI
-- symbols (`sqlite3_<k>`) for any missing key; use rawget to probe for our
-- patch sentinel without accidentally triggering a bogus symbol lookup.
if not rawget(sqlite_defs, "__plugin_db_wrap_patched") then
    -- Set of connection pointers currently inside `db:execute(fn)`.
    -- Keyed by `tostring(conn_ptr)` so the FFI cdata comparison works.
    local in_user_tx = {}
    rawset(sqlite_defs, "__plugin_db_user_tx_set", in_user_tx)
    rawset(sqlite_defs, "wrap_stmts", function(conn_ptr, fn)
        if in_user_tx[tostring(conn_ptr)] then
            return fn()
        end

        local function execute_control(statement)
            local code = sqlite_defs.exec_stmt(conn_ptr, statement)
            if code ~= sqlite_defs.flags.ok then
                error(string.format(
                    "plugin.db: %s failed: %s",
                    statement,
                    sqlite_defs.last_errmsg(conn_ptr)
                ), 0)
            end
        end

        execute_control("BEGIN")
        local results = table.pack(pcall(fn))
        if not results[1] then
            local callback_error = results[2]
            local rollback_ok, rollback_error = pcall(execute_control, "ROLLBACK")
            if not rollback_ok then
                error(string.format(
                    "%s; rollback also failed: %s",
                    tostring(callback_error),
                    tostring(rollback_error)
                ), 0)
            end
            error(callback_error, 0)
        end

        local commit_ok, commit_error = pcall(execute_control, "COMMIT")
        if not commit_ok then
            local rollback_ok, rollback_error = pcall(execute_control, "ROLLBACK")
            if not rollback_ok then
                error(string.format(
                    "%s; rollback also failed: %s",
                    tostring(commit_error),
                    tostring(rollback_error)
                ), 0)
            end
            error(commit_error, 0)
        end

        return table.unpack(results, 2, results.n)
    end)
    rawset(sqlite_defs, "__plugin_db_wrap_patched", true)
end
local USER_TX_SET = rawget(sqlite_defs, "__plugin_db_user_tx_set")

-- ============================================================================
-- Module state — backed by hub.state so it survives module hot-reload
-- ============================================================================
--
-- `lib.plugin_db` is a hot-reloadable library module; core handlers reload it
-- via `loader.reload("lib.plugin_db")` when the file changes. Plain local
-- tables here would be torn down on reload, leaking cached sqlite fds and
-- detaching the `_G.plugin.db` global from the current closure. Storing state
-- in `hub.state` (which is a protected module — never reloaded) gives us the
-- same table identity across reload so both the old and new module closures
-- operate on one cache.
--
-- Cache shape: plugin_name -> { wrapper = raw_db, memory = bool, uri = string }
local db_handles = hub_state.get("plugin_db.handles", {})

-- Guards against adding duplicate `events.on("shutdown", ...)` subscriptions
-- on repeated install() calls. `hooks.on` is replaceable by name so it doesn't
-- need its own guard. Wrapped in a table so the module-level local keeps a
-- stable reference to the (mutable) state across reloads.
local events_subscribed = hub_state.get("plugin_db.events_subscribed", { value = false })

-- Default PRAGMAs, split by criticality.
--
-- `busy_timeout` MUST run first so any subsequent statement that grabs a WAL
-- or file lock inherits the 5 s waiter. `journal_mode = WAL` and
-- `foreign_keys = ON` are load-bearing — silently running without them is a
-- correctness bug, so those fail LOUDLY. `synchronous = NORMAL` is a perf
-- pragma only; a failure downgrades us to whatever sqlite's default is and
-- is logged at WARN but does not refuse the plugin load.
local CRITICAL_PRAGMAS = {
    { "busy_timeout",  "5000" },  -- first: subsequent pragmas inherit the timeout
    { "journal_mode",  "WAL" },
    { "foreign_keys",  "ON" },
}
local OPTIONAL_PRAGMAS = {
    { "synchronous",   "NORMAL" },
}

-- Model names that would collide with sqlite.db methods. `wrap_db` rawsets
-- each model as an instance field; if a plugin declared `models.close`, the
-- rawset would shadow `sqlite.db:close` and break the shutdown/close path
-- (FFI fd leak). Reserved set keeps the wrapper's own surface predictable too.
local RESERVED_MODEL_NAMES = {
    close = true, open = true, with_open = true, isopen = true, isclose = true,
    status = true, eval = true, execute = true, exists = true, create = true,
    drop = true, schema = true, insert = true, update = true, delete = true,
    select = true, tbl = true, table = true, extend = true, new = true,
    lib = true, flags = true,
    uri = true, conn = true, closed = true, opts = true, modified = true,
    created = true, tbl_schemas = true, db = true,
}

-- ============================================================================
-- Schema DSL normalization
-- ============================================================================

--- Normalize a column declaration into a full table form.
--
-- Accepts:
--   true                             -> { type = "integer", required = true, primary = true }
--   "text"                           -> { type = "text" }
--   { "text", required = true, ... } -> { type = "text", required = true, ... }
--   { type = "text", ... }           -> { type = "text", ... }
--
-- @param decl any Column declaration from spec.models.<tbl>.<col>
-- @return table Normalized declaration
local function normalize_col(decl)
    if decl == true then
        return { type = "integer", required = true, primary = true }
    elseif type(decl) == "string" then
        return { type = decl }
    elseif type(decl) ~= "table" then
        error(string.format("plugin.db: invalid column declaration type=%s", type(decl)), 2)
    end

    local n = {}
    for k, v in pairs(decl) do
        if k == 1 and n.type == nil then
            n.type = v
        else
            n[k] = v
        end
    end
    -- "pk" alias for "primary"
    if n.pk and n.primary == nil then n.primary = n.pk end
    return n
end

--- Build a single column definition string for CREATE TABLE / ADD COLUMN.
-- Mirrors sqlite.lua's `opts_to_str` subset, scoped to what our DSL supports.
-- @param col_name string
-- @param decl any Raw column declaration
-- @return string e.g. "body text not null default ''"
local function column_def(col_name, decl)
    local n = normalize_col(decl)
    local parts = { col_name }
    if n.type and n.type ~= "" then
        table.insert(parts, n.type)
    end
    if n.unique then table.insert(parts, "unique") end
    if n.required then table.insert(parts, "not null") end
    if n.primary then table.insert(parts, "primary key") end
    if n.default ~= nil then
        local v = n.default
        local rendered
        if type(v) == "string" then
            -- Wrap in single quotes, doubling any internal single quotes.
            rendered = "'" .. v:gsub("'", "''") .. "'"
        elseif type(v) == "boolean" then
            rendered = v and "1" or "0"
        else
            rendered = tostring(v)
        end
        table.insert(parts, "default " .. rendered)
    end
    if n.reference then
        table.insert(parts, ("references %s"):format(n.reference:gsub("%.", "(") .. ")"))
    end
    if n.on_delete then
        table.insert(parts, "on delete " .. n.on_delete)
    end
    if n.on_update then
        table.insert(parts, "on update " .. n.on_update)
    end
    return table.concat(parts, " ")
end

--- Build a CREATE TABLE statement for a model schema.
-- @param tbl_name string
-- @param schema table { col_name = decl, ... }
-- @return string SQL
local function create_table_sql(tbl_name, schema)
    local cols = {}
    for col_name, col_decl in pairs(schema) do
        table.insert(cols, column_def(col_name, col_decl))
    end
    return string.format(
        "CREATE TABLE IF NOT EXISTS %s (%s)",
        tbl_name, table.concat(cols, ", ")
    )
end

-- ============================================================================
-- Schema introspection
-- ============================================================================

--- Does a table with this name exist in sqlite_master?
local function table_exists(db, tbl_name)
    local rows = db:eval(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        tbl_name
    )
    return type(rows) == "table" and #rows > 0
end

--- Return the columns of a table keyed by name:
-- { [col_name] = { name, type, required, primary, default } }.
local function existing_cols(db, tbl_name)
    local rows = db:eval(string.format("PRAGMA table_info(%s)", tbl_name))
    local cols = {}
    if type(rows) == "table" then
        for _, r in ipairs(rows) do
            cols[r.name] = {
                name = r.name,
                type = (r.type or ""):lower(),
                required = r.notnull == 1,
                primary = (r.pk or 0) > 0,
                default = r.dflt_value,
            }
        end
    end
    return cols
end

-- ============================================================================
-- Additive schema reconciliation
-- ============================================================================

--- Reconcile declared `models` against the live schema.
--
-- In `strict=true` mode (steady-state, current_version == target_version) any
-- non-additive drift raises a Lua error with a printable diff. In `strict=false`
-- mode (inside an upgrade step, BEFORE the author's migration fn runs) only
-- additive changes are applied — non-additive mismatches are silently tolerated
-- because the migration fn about to run is expected to resolve them.
--
-- Additive changes: new tables (CREATE TABLE IF NOT EXISTS), new columns
-- (ALTER TABLE ADD COLUMN). Adding a NOT NULL column without a default is
-- rejected regardless of strict flag since sqlite itself cannot apply it.
local function reconcile_additive(db, plugin_name, models, strict)
    models = models or {}
    local mismatches = {}
    local add_col_issues = {}
    local changes = {}

    for tbl_name, schema_in in pairs(models) do
        if not table_exists(db, tbl_name) then
            table.insert(changes, {
                kind = "create_table",
                tbl = tbl_name,
                sql = create_table_sql(tbl_name, schema_in),
            })
        else
            local existing = existing_cols(db, tbl_name)
            local declared = {}
            for col_name, col_decl in pairs(schema_in) do
                declared[col_name] = normalize_col(col_decl)
            end

            -- Shared cols: check type + nullability
            for col_name, decl in pairs(declared) do
                local ex = existing[col_name]
                if ex then
                    local decl_type = (decl.type or ""):lower()
                    if decl_type ~= "" and decl_type ~= ex.type then
                        table.insert(mismatches, string.format(
                            "  %s.%s: type changed (%s -> %s)",
                            tbl_name, col_name, ex.type, decl_type
                        ))
                    end
                    local decl_req = decl.required == true
                    if decl_req ~= ex.required then
                        table.insert(mismatches, string.format(
                            "  %s.%s: nullability changed (%s -> %s)",
                            tbl_name, col_name,
                            ex.required and "required" or "nullable",
                            decl_req and "required" or "nullable"
                        ))
                    end
                end
            end

            -- Removed cols: existing but not declared
            for col_name, _ in pairs(existing) do
                if not declared[col_name] then
                    table.insert(mismatches, string.format(
                        "  %s.%s: column was removed",
                        tbl_name, col_name
                    ))
                end
            end

            -- Added cols: declared but not existing
            for col_name, decl in pairs(declared) do
                if not existing[col_name] then
                    if decl.required and decl.default == nil then
                        table.insert(add_col_issues, string.format(
                            "  %s.%s: required column with no default (sqlite refuses ADD COLUMN NOT NULL without DEFAULT)",
                            tbl_name, col_name
                        ))
                    else
                        table.insert(changes, {
                            kind = "add_column",
                            tbl = tbl_name,
                            col = col_name,
                            sql = string.format(
                                "ALTER TABLE %s ADD COLUMN %s",
                                tbl_name, column_def(col_name, schema_in[col_name])
                            ),
                        })
                    end
                end
            end
        end
    end

    if strict and #mismatches > 0 then
        error(string.format(
            "plugin.db: plugin '%s' schema mismatch.\nNon-additive changes detected:\n%s\nTo apply these changes, bump version and provide migrations[new_version] = function(db) ... end.",
            plugin_name, table.concat(mismatches, "\n")
        ), 0)
    end

    if #add_col_issues > 0 then
        error(string.format(
            "plugin.db: plugin '%s' cannot add required column(s) without a default value:\n%s\nOptions:\n  - Add a default: col = { 'text', required = true, default = '' }\n  - Make it optional: col = { 'text' }\n  - Bump version and backfill via migrations[next] = function(db) ... end",
            plugin_name, table.concat(add_col_issues, "\n")
        ), 0)
    end

    for _, change in ipairs(changes) do
        db:eval(change.sql)
        if change.kind == "create_table" then
            log.info(string.format(
                "db.additive_change plugin=%s table=%s kind=new_table",
                plugin_name, change.tbl
            ))
        elseif change.kind == "add_column" then
            log.info(string.format(
                "db.additive_change plugin=%s table=%s col=%s kind=new_column",
                plugin_name, change.tbl, change.col
            ))
        end
    end
end

-- ============================================================================
-- Migration runner
-- ============================================================================

--- Read the current `user_version` PRAGMA for the db. 0 when unset (fresh db).
local function read_user_version(db)
    local rows = db:eval("PRAGMA user_version")
    if type(rows) == "table" and rows[1] and rows[1].user_version then
        return rows[1].user_version
    end
    return 0
end

--- Run migration steps `current+1 .. target`, one transaction per step.
-- Each step: non-strict additive reconcile -> author's migrations[i] (if any)
-- -> PRAGMA user_version = i. On Lua error inside the step, ROLLBACK and
-- propagate with step diagnostics; the db stays at the previous version.
--
-- Marks the connection as "in user tx" via USER_TX_SET for the duration of
-- each step. This tells the patched `wrap_stmts` to skip its own BEGIN/COMMIT
-- if the migration fn happens to call `db:insert/update/delete` (which would
-- otherwise commit the outer tx early and break rollback-on-error).
local function run_migrations(db, plugin_name, spec, current, target)
    local ptr_key = tostring(db.conn)
    for i = current + 1, target do
        local step_start_ms = (os.clock() * 1000)
        db:eval("BEGIN")
        USER_TX_SET[ptr_key] = true
        local ok, err = pcall(function()
            -- Apply additive changes for version i (non-strict: the migration fn
            -- about to run may legitimately DROP/rename columns; let it run
            -- without the steady-state strictness check).
            reconcile_additive(db, plugin_name, spec.models or {}, false)

            -- Invoke the author's migration function for this step.
            local mig_fn = spec.migrations and spec.migrations[i]
            if mig_fn then
                if type(mig_fn) ~= "function" then
                    error(string.format(
                        "migrations[%d] must be a function, got %s",
                        i, type(mig_fn)
                    ), 0)
                end
                mig_fn(db)
            end

            -- Record the new version. PRAGMA user_version is a valid pragma in
            -- sqlite.lua's whitelist; it persists to the db header.
            db:eval(string.format("PRAGMA user_version = %d", i))
        end)
        USER_TX_SET[ptr_key] = nil

        if not ok then
            pcall(function() db:eval("ROLLBACK") end)
            error(string.format(
                "plugin.db: plugin '%s' migration %d (v%d -> v%d) failed: %s\nTransaction rolled back. Database remains at v%d.\nFix the migration function and reload the plugin.",
                plugin_name, i, i - 1, i, tostring(err), i - 1
            ), 0)
        end
        db:eval("COMMIT")

        local dur_ms = math.floor((os.clock() * 1000) - step_start_ms + 0.5)
        log.info(string.format(
            "db.migration plugin=%s applied=%d->%d duration_ms=%d",
            plugin_name, i - 1, i, dur_ms
        ))
    end
end

--- Apply the declared version target to a freshly opened (or cached) db.
-- Dispatches between downgrade error / upgrade migrations / steady-state
-- reconcile based on the current user_version.
--
-- After a successful upgrade, runs the STRICT reconcile one more time against
-- the final `spec.models`. This catches the case where a plugin declared a
-- new table/column in `models` but forgot to create it in `migrations[N]` —
-- without this pass the first load silently succeeds with a broken schema and
-- the error only surfaces on the next load.
local function apply_schema(db, plugin_name, spec, uri)
    local target = spec.version or 1
    local current = read_user_version(db)

    if target < current then
        error(string.format(
            "plugin.db: plugin '%s' declares version=%d but database is at version %d.\nDowngrades are not supported.\nOptions:\n  - Bump the declared version to %d or higher.\n  - Delete %s to reset and start fresh.",
            plugin_name, target, current, current, uri
        ), 0)
    elseif target > current then
        run_migrations(db, plugin_name, spec, current, target)
        -- Final validation pass against the declared models: strict.
        reconcile_additive(db, plugin_name, spec.models or {}, true)
    else
        -- current == target: steady-state reconcile is strict.
        reconcile_additive(db, plugin_name, spec.models or {}, true)
    end
end

-- ============================================================================
-- Wrapper: attach tbl proxies + override execute for transaction-fn semantics.
-- ============================================================================

--- Attach per-model `sqlite.tbl` wrappers as instance fields on the raw db so
--- plugin code writes `db.messages:insert{...}` etc. Also override `execute`
--- to accept a function arg (BEGIN / pcall / COMMIT-or-ROLLBACK). Idempotent:
--- safe to call multiple times on the same db (e.g. on hot-reload after the
--- plugin's models set changed).
local function wrap_db(raw_db, spec)
    -- Attach tbl proxies for every declared model. rawset bypasses the
    -- sqlite_db class metatable's __index, so instance reads of `db.messages`
    -- resolve to our tbl before falling back to sqlite.db methods.
    local models = spec.models or {}
    for tbl_name, _ in pairs(models) do
        if rawget(raw_db, tbl_name) == nil then
            -- Deliberately pass `nil` for schema so sqlite.tbl skips its own
            -- auto-alter logic. Our migration runner has already ensured the
            -- physical schema; the tbl wrapper is purely an ergonomic front.
            rawset(raw_db, tbl_name, sqlite_tbl.new(tbl_name, nil, raw_db))
        end
    end

    -- Install execute(fn) transaction semantics (idempotent). The sentinel
    -- marker lets us distinguish the wrapped function from sqlite.lua's
    -- class-level execute across hot-reloads.
    --
    -- Upstream sqlite.lua's `db:execute(string)` does single-statement exec;
    -- its internal `wrap_stmts` helper wraps in BEGIN/COMMIT but does NOT
    -- rollback on error, leaving the transaction open on failure. Our override
    -- adds proper pcall + ROLLBACK semantics for the function form while
    -- passing strings through to the upstream implementation unchanged.
    if rawget(raw_db, "__plugin_db_execute_wrapped") ~= true then
        local class_execute = sqlite_db_class.execute
        rawset(raw_db, "execute", function(self, arg)
            if type(arg) ~= "function" then
                return class_execute(self, arg)
            end
            local ptr_key = tostring(self.conn)
            USER_TX_SET[ptr_key] = true
            class_execute(self, "BEGIN")
            local ok, result = pcall(arg, self)
            if ok then
                class_execute(self, "COMMIT")
                USER_TX_SET[ptr_key] = nil
                return result
            else
                pcall(class_execute, self, "ROLLBACK")
                USER_TX_SET[ptr_key] = nil
                error(result, 0)
            end
        end)
        rawset(raw_db, "__plugin_db_execute_wrapped", true)
    end

    return raw_db
end

-- ============================================================================
-- Handle lifecycle (close, shutdown, plugin_unloading)
-- ============================================================================

local function close_and_evict(name)
    local entry = db_handles[name]
    if not entry then return end
    pcall(function() entry.wrapper:close() end)
    db_handles[name] = nil
end

function M._on_plugin_unloading(payload)
    if type(payload) == "table" and type(payload.name) == "string" then
        close_and_evict(payload.name)
    end
end

function M.shutdown_all()
    local names = {}
    for name, _ in pairs(db_handles) do table.insert(names, name) end
    for _, name in ipairs(names) do close_and_evict(name) end
end

-- Test-only: reset module state so each test starts clean. The underlying
-- hub.state tables are emptied in place so their identity is preserved for
-- any closures that captured them.
function M._reset_for_tests()
    M.shutdown_all()
    for k in pairs(db_handles) do db_handles[k] = nil end
    events_subscribed.value = false
end

-- ============================================================================
-- Main entry: plugin.db{}
-- ============================================================================

--- Resolve the database URI for a plugin.
-- memory = true   -> ":memory:"
-- else            -> "{config.data_dir()}/plugin-data/<plugin_name>/db.sqlite"
-- The parent directory is created with fs.mkdir (mkdir -p semantics).
local function resolve_uri(plugin_name, memory)
    if memory then
        return ":memory:"
    end
    if not (config and type(config.data_dir) == "function") then
        error(string.format(
            "plugin.db: cannot resolve config.data_dir() for plugin '%s'",
            plugin_name
        ), 0)
    end
    local data_dir = config.data_dir()
    if not data_dir or data_dir == "" then
        error(string.format(
            "plugin.db: config.data_dir() returned nil/empty for plugin '%s'",
            plugin_name
        ), 0)
    end
    local plugin_dirname = plugin_name:gsub("[^%w%._-]", "_")
    local plugin_dir = data_dir .. "/plugin-data/" .. plugin_dirname
    local ok, err = fs.mkdir(plugin_dir)
    if not ok then
        error(string.format(
            "plugin.db: failed to create plugin data directory '%s': %s",
            plugin_dir, tostring(err)
        ), 0)
    end
    return plugin_dir .. "/db.sqlite"
end

--- Public API: `plugin.db{ version=..., models=..., migrations=..., memory=... }`.
-- Returns the sqlite db object with tbl wrappers attached. Cached per plugin
-- name so hot-reload reuses the fd.
function M.db(spec)
    spec = spec or {}
    local plugin_name = rawget(_G, "_loading_plugin_name")
    if type(plugin_name) ~= "string" or plugin_name == "" then
        error(
            "plugin.db: must be called during plugin load, not from callbacks or deferred code.\nCache the db handle at the top of your plugin.lua:\n  local db = plugin.db{ ... }\n  -- then reference `db` inside callbacks, hooks, MCP tool handlers, etc.",
            2
        )
    end

    -- Reject model names that would shadow sqlite.db instance methods. This
    -- is a load-time check so the error message mentions the plugin by name
    -- instead of surfacing as a mysterious "attempt to call nil" later.
    if type(spec.models) == "table" then
        local reserved = {}
        for tbl_name, _ in pairs(spec.models) do
            if RESERVED_MODEL_NAMES[tbl_name] then
                table.insert(reserved, "'" .. tbl_name .. "'")
            end
        end
        if #reserved > 0 then
            error(string.format(
                "plugin.db: plugin '%s' declared model(s) with reserved name(s): %s.\nThese names collide with sqlite.db methods (close/eval/insert/...) and would break the db handle if attached.\nRename the model(s) to something plural or domain-specific (e.g. 'messages' instead of 'insert').",
                plugin_name, table.concat(reserved, ", ")
            ), 2)
        end
    end

    local memory = spec.memory == true

    -- Cache hit: reuse connection, reconcile against the latest spec.
    local cached = db_handles[plugin_name]
    if cached then
        if cached.memory ~= memory then
            error(string.format(
                "plugin.db: plugin '%s' re-declared with memory=%s but existing handle was opened with memory=%s.\nChange one of the declarations. The handle is cached per plugin name across hot-reloads.",
                plugin_name, tostring(memory), tostring(cached.memory)
            ), 2)
        end
        -- Re-apply schema (reconcile or migrate forward as needed) and re-wrap
        -- so any newly declared models pick up tbl proxies.
        apply_schema(cached.wrapper, plugin_name, spec, cached.uri)
        return wrap_db(cached.wrapper, spec)
    end

    -- Fresh open.
    local uri = resolve_uri(plugin_name, memory)
    local raw = sqlite.new(uri)
    local open_ok, open_err = pcall(function() raw:open() end)
    if not open_ok then
        error(string.format(
            "plugin.db: plugin '%s' failed to open db at '%s': %s",
            plugin_name, uri, tostring(open_err)
        ), 2)
    end

    -- Critical PRAGMAs: any failure is a correctness bug, refuse the load.
    -- busy_timeout goes first so the WAL lock-taking statements inherit it.
    for _, pair in ipairs(CRITICAL_PRAGMAS) do
        local pragma_ok, pragma_err = pcall(function()
            raw:eval(string.format("PRAGMA %s = %s", pair[1], pair[2]))
        end)
        if not pragma_ok then
            pcall(function() raw:close() end)
            error(string.format(
                "plugin.db: plugin '%s' failed to apply PRAGMA %s = %s: %s",
                plugin_name, pair[1], pair[2], tostring(pragma_err)
            ), 2)
        end
    end

    -- Optional PRAGMAs: perf knobs only; warn but continue on failure.
    for _, pair in ipairs(OPTIONAL_PRAGMAS) do
        local pragma_ok, pragma_err = pcall(function()
            raw:eval(string.format("PRAGMA %s = %s", pair[1], pair[2]))
        end)
        if not pragma_ok then
            log.warn(string.format(
                "plugin.db: PRAGMA %s = %s failed for plugin '%s' (non-fatal): %s",
                pair[1], pair[2], plugin_name, tostring(pragma_err)
            ))
        end
    end

    -- Author-supplied overrides. These are plugin-managed, so a typo or bad
    -- value shouldn't refuse the plugin load — warn and continue.
    if type(spec.pragmas) == "table" then
        for k, v in pairs(spec.pragmas) do
            local pragma_ok, pragma_err = pcall(function()
                raw:eval(string.format("PRAGMA %s = %s", k, tostring(v)))
            end)
            if not pragma_ok then
                log.warn(string.format(
                    "plugin.db: user PRAGMA %s = %s failed for plugin '%s' (non-fatal): %s",
                    k, tostring(v), plugin_name, tostring(pragma_err)
                ))
            end
        end
    end

    -- Run schema apply BEFORE caching so a migration failure leaves no stale
    -- handle. If apply_schema errors, the db is closed here and the error
    -- propagates — the loader marks the plugin errored.
    local schema_ok, schema_err = pcall(apply_schema, raw, plugin_name, spec, uri)
    if not schema_ok then
        pcall(function() raw:close() end)
        error(schema_err, 0)
    end

    local wrapped = wrap_db(raw, spec)
    db_handles[plugin_name] = {
        wrapper = wrapped,
        memory = memory,
        uri = uri,
    }
    return wrapped
end

-- ============================================================================
-- Installation: wires _G.plugin.db + subscribes to lifecycle hooks.
-- ============================================================================

--- Install `_G.plugin.db` and subscribe to `plugin_unloading` + `shutdown` so
--- cached handles close cleanly. Called once from `cli/lua/hub/init.lua`
--- BEFORE any plugin is loaded AND again on `_after_reload` so the fresh
--- module's closures replace the stale ones.
--
-- Idempotency notes:
--   * `_G.plugin.db = M.db` is assigned every call so reloads swap to the
--     fresh closure (without this, the global keeps pointing at the old
--     module's `db_handles` — invisible edits + the stale-closure bug codex
--     flagged).
--   * `hooks.on(event, name, fn)` replaces the previous entry with the same
--     name, so repeated calls are safe and keep the handler up-to-date with
--     the latest closure.
--   * `events.on("shutdown", fn)` has no by-name replacement in Lua. We
--     guard on `events_subscribed.value` (backed by hub.state) so exactly
--     one subscription persists across reloads. The subscribed callback
--     resolves `M` lazily via `package.loaded` so it always runs the latest
--     `shutdown_all` against the shared `db_handles` table.
function M.install()
    _G.plugin = _G.plugin or {}
    _G.plugin.db = M.db

    -- Every install() re-registers the hook so the fresh closure wins.
    if hooks and type(hooks.on) == "function" then
        hooks.on("plugin_unloading", "plugin_db.close_handle", function(payload)
            local mod = package.loaded["lib.plugin_db"] or M
            mod._on_plugin_unloading(payload)
        end)
    end

    if events and type(events.on) == "function" and not events_subscribed.value then
        events.on("shutdown", function()
            local mod = package.loaded["lib.plugin_db"] or M
            mod.shutdown_all()
        end)
        events_subscribed.value = true
    end
end

-- ============================================================================
-- Hot-reload lifecycle
-- ============================================================================
-- Core Lua libraries under `lib/` are hot-reloaded by
-- `handlers.module_watcher`. `loader.reload("lib.plugin_db")` drops the old
-- module from `package.loaded` and re-`require`s this file. Without the hook
-- below, the old `_G.plugin.db` closure (and the subscribed hook callbacks)
-- keep pointing at the torn-down module. `_after_reload` reinstalls the
-- global + replaces the hook callback with the fresh closure.
--
-- The underlying db handle cache lives in `hub.state` so it's preserved
-- across the reload cycle — no fds are leaked and no plugin's data is lost.

function M._before_reload()
    if log and log.info then
        log.info("lib.plugin_db reloading (handles + subscriptions persist via hub.state)")
    end
end

function M._after_reload()
    M.install()
    if log and log.info then
        log.info("lib.plugin_db reloaded (_G.plugin.db and plugin_unloading hook re-wired)")
    end
end

return M
