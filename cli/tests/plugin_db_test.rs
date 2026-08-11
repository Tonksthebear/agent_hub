//! Integration tests for `cli/lua/lib/plugin_db.lua` — the public `plugin.db{}`
//! persistence API for Lua plugins.
//!
//! The wrapper layers onto the vendored `sqlite.lua` (PR B.1) and provides:
//!   * path derivation under `{data_dir}/plugin-data/<name>/db.sqlite`
//!   * default PRAGMAs (WAL + NORMAL sync + foreign_keys + busy_timeout)
//!   * declarative schema reconciliation (additive adds vs strict mismatches)
//!   * a migration runner driven by `PRAGMA user_version`
//!   * per-plugin handle cache (hot-reload safe)
//!   * a `memory = true` shortcut that skips file I/O for tests
//!
//! These tests exercise the public surface end-to-end: they spin up a real
//! LuaJIT VM with FFI, register the `fs`/`log`/`config` primitives, and load
//! `require('lib.plugin_db').install()` the same way `hub/init.lua` does in
//! production. Each test isolates via its own `BOTSTER_CONFIG_DIR` tempdir.
//!
//! Run with: `cd cli && ./test.sh -- plugin_db`.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::needless_borrows_for_generic_args,
    clippy::redundant_closure_for_method_calls,
    clippy::too_many_lines,
    reason = "test-code brevity: assertion-heavy tests that exercise a large API"
)]

use std::path::PathBuf;
use std::sync::Mutex;

use mlua::{Lua, LuaOptions, StdLib, Table};
use tempfile::TempDir;

/// Serialises tests that mutate process env (`BOTSTER_CONFIG_DIR`). Each test
/// takes this lock before setting env and dropping the VM.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn lock_env() -> std::sync::MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn cli_manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Build a LuaJIT VM wired up the same way a production hub builds its one,
/// minus primitives we don't need here.
///
/// Mirrors `LuaRuntime::new` for the bits that matter to `plugin.db`:
///   * `unsafe_new_with(ALL_SAFE | FFI)` — required so `vendor.sqlite` can load.
///   * `fs.mkdir` — used to create `plugins/<name>/` parent dirs.
///   * `log.{info,warn,error,debug}` — used by the migration runner.
///   * `config.data_dir()` — resolves to `$BOTSTER_CONFIG_DIR` or `~/.botster`.
///   * `package.path` wired to `cli/lua/` so `require('vendor.sqlite')` and
///     `require('lib.plugin_db')` resolve.
fn new_test_lua() -> Lua {
    let lua =
        unsafe { Lua::unsafe_new_with(StdLib::ALL_SAFE | StdLib::FFI, LuaOptions::default()) };

    botster::lua::primitives::fs::register(&lua).expect("register fs");
    botster::lua::primitives::log::register(&lua).expect("register log");
    botster::lua::primitives::config::register(&lua).expect("register config");

    // Stub hooks + events. Real implementations live elsewhere; plugin_db.lua
    // only needs `hooks.on(event, name, fn)` and `events.on(event, fn)` to
    // subscribe. The stubs record subscriptions so tests can fire them.
    let globals = lua.globals();
    lua.load(
        r#"
        _G.hooks = {
            _subs = {},
            on = function(event, name, fn)
                _G.hooks._subs[event] = _G.hooks._subs[event] or {}
                _G.hooks._subs[event][name] = fn
            end,
            notify = function(event, payload)
                local subs = _G.hooks._subs[event] or {}
                for _, fn in pairs(subs) do fn(payload) end
            end,
        }
        _G.events = {
            _subs = {},
            on = function(event, fn)
                _G.events._subs[event] = _G.events._subs[event] or {}
                table.insert(_G.events._subs[event], fn)
                return tostring(event) .. "-" .. tostring(#_G.events._subs[event])
            end,
            fire = function(event)
                for _, fn in ipairs(_G.events._subs[event] or {}) do fn() end
            end,
        }
        "#,
    )
    .exec()
    .expect("install hooks/events stubs");

    let lua_base = cli_manifest_dir().join("lua");
    let base = lua_base.to_string_lossy();
    let package: Table = globals.get("package").unwrap();
    let current_path: String = package.get("path").unwrap();
    let new_path = format!(
        "{base}/?.lua;{base}/?/init.lua;{base}/lib/?.lua;{base}/vendor/?.lua;{base}/vendor/?/init.lua;{current_path}"
    );
    package.set("path", new_path).unwrap();

    lua.load(
        r#"
        local plugin_db = require('lib.plugin_db')
        plugin_db._reset_for_tests()
        plugin_db.install()
        "#,
    )
    .exec()
    .expect("install plugin_db");

    lua
}

/// Set the loading-plugin name for the duration of the test. Mirrors what the
/// real plugin loader does around a plugin's `init.lua` chunk.
fn set_loading_plugin(lua: &Lua, name: &str) {
    lua.globals().set("_loading_plugin_name", name).unwrap();
}

fn set_config_dir(dir: &std::path::Path) {
    // SAFETY: Tests serialise via ENV_LOCK so no other thread observes the
    // mutation concurrently.
    unsafe { std::env::set_var("BOTSTER_CONFIG_DIR", dir) };
}

// ============================================================================
// 1. test_db_open_creates_file
// ============================================================================

#[test]
fn db_open_creates_file_under_data_dir() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());

    let lua = new_test_lua();
    set_loading_plugin(&lua, "messaging");

    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = {
                messages = {
                    id = true,
                    body = { 'text', required = true, default = '' },
                },
            },
        }
        assert(db ~= nil, "plugin.db returned nil")
        "#,
    )
    .exec()
    .expect("open fresh db");

    let expected = tmp.path().join("plugin-data/messaging/db.sqlite");
    assert!(
        expected.exists(),
        "expected db file at {}",
        expected.display()
    );
}

#[test]
fn db_open_sanitizes_scoped_plugin_key_for_data_dir() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());

    let lua = new_test_lua();
    set_loading_plugin(
        &lua,
        "repo:/Users/jason/Rails/jupiter:rails-worktree-lifecycle",
    );

    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = {
                events = {
                    id = true,
                    body = { 'text', default = '' },
                },
            },
        }
        assert(db ~= nil, "plugin.db returned nil")
        "#,
    )
    .exec()
    .expect("open scoped plugin db");

    let expected = tmp
        .path()
        .join("plugin-data/repo__Users_jason_Rails_jupiter_rails-worktree-lifecycle/db.sqlite");
    assert!(
        expected.exists(),
        "expected scoped db file at {}",
        expected.display()
    );
}

// ============================================================================
// 2. test_insert_and_get
// ============================================================================

#[test]
fn insert_and_get_round_trip_preserves_types() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "insert_get");

    let (author, body, channel, ts): (String, String, i64, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    messages = {
                        id = true,
                        channel_id = { 'integer', required = true },
                        author = { 'text', required = true },
                        body = { 'text', required = true },
                        created_at = { 'integer', required = true },
                    },
                },
            }
            db.messages:insert{
                channel_id = 7,
                author = 'jason',
                body = 'hello world',
                created_at = 1776900000,
            }
            local rows = db.messages:get{}
            assert(#rows == 1, 'expected 1 row, got ' .. tostring(#rows))
            local r = rows[1]
            return r.author, r.body, r.channel_id, r.created_at
            "#,
        )
        .eval()
        .expect("insert + get");

    assert_eq!(author, "jason");
    assert_eq!(body, "hello world");
    assert_eq!(channel, 7);
    assert_eq!(ts, 1_776_900_000);
}

// ============================================================================
// 3. test_where_clause
// ============================================================================

#[test]
fn get_with_where_filters_rows() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "wherec");

    let count: i64 = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    messages = {
                        id = true,
                        channel_id = { 'integer', required = true },
                        body = { 'text', required = true, default = '' },
                    },
                },
            }
            for i = 1, 5 do
                db.messages:insert{ channel_id = (i % 2) + 1, body = 'm' .. i }
            end
            local rows = db.messages:get{ where = { channel_id = 2 } }
            return #rows
            "#,
        )
        .eval()
        .expect("filter by where");

    // Channels assigned 2,1,2,1,2 (i=1..5 → (i%2)+1). Three in channel 2.
    assert_eq!(count, 3);
}

// ============================================================================
// 4. test_update
// ============================================================================

#[test]
fn update_changes_matching_row_only() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "updt");

    let (total, edited, other): (i64, String, String) = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    messages = {
                        id = true,
                        body = { 'text', required = true, default = '' },
                    },
                },
            }
            db.messages:insert{ body = 'a' }
            db.messages:insert{ body = 'b' }
            db.messages:update{ where = { id = 1 }, set = { body = 'edited' } }
            local all = db.messages:get{}
            local by_id = {}
            for _, r in ipairs(all) do by_id[r.id] = r.body end
            return #all, by_id[1], by_id[2]
            "#,
        )
        .eval()
        .expect("update");

    assert_eq!(total, 2, "update must not insert extra rows");
    assert_eq!(edited, "edited");
    assert_eq!(other, "b");
}

// ============================================================================
// 5. test_remove
// ============================================================================

#[test]
fn remove_deletes_matching_rows() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "rm");

    let (before, after): (i64, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    messages = {
                        id = true,
                        body = { 'text', required = true, default = '' },
                    },
                },
            }
            db.messages:insert{ body = 'a' }
            db.messages:insert{ body = 'b' }
            db.messages:insert{ body = 'c' }
            local before = #db.messages:get{}
            db.messages:remove{ id = 2 }
            local after = #db.messages:get{}
            return before, after
            "#,
        )
        .eval()
        .expect("remove");

    assert_eq!(before, 3);
    assert_eq!(after, 2);
}

// ============================================================================
// 6. test_transaction (db:execute(fn) rollback semantics)
// ============================================================================

#[test]
fn execute_fn_rolls_back_on_error() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "tx");

    let (rolled_back, later_insert_works): (bool, bool) = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    counters = {
                        id = true,
                        name = { 'text', required = true },
                    },
                },
            }

            -- attempt a transaction that errors mid-way
            local ok = pcall(function()
                db:execute(function(self)
                    self.counters:insert{ name = 'should_rollback' }
                    error('deliberate abort')
                end)
            end)
            assert(not ok, 'pcall should have seen the error')

            -- connection must still be usable: insert outside a tx
            local rows_before = db.counters:get{}
            local rolled_back = (#rows_before == 0)

            local insert_ok, _ = pcall(function()
                db.counters:insert{ name = 'fresh' }
            end)
            return rolled_back, insert_ok
            "#,
        )
        .eval()
        .expect("transaction rollback");

    assert!(rolled_back, "error inside db:execute(fn) must ROLLBACK");
    assert!(
        later_insert_works,
        "connection must be usable after rollback (tx not stuck open)"
    );
}

#[test]
fn failed_table_write_rolls_back_and_connection_remains_usable() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "failed-write");

    let (failed, later_insert_works, row_count): (bool, bool, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    counters = {
                        id = true,
                        name = { 'text', required = true },
                    },
                },
            }

            db:execute([[
                CREATE TRIGGER reject_counters
                BEFORE INSERT ON counters
                BEGIN
                    SELECT RAISE(ABORT, 'reject insert');
                END
            ]])
            local ok = pcall(function()
                db.counters:insert{ name = 'blocked' }
            end)
            db:execute('DROP TRIGGER reject_counters')

            local later_ok = pcall(function()
                db.counters:insert{ name = 'fresh' }
            end)
            return not ok, later_ok, #db.counters:get{}
            "#,
        )
        .eval()
        .expect("recover after failed table write");

    assert!(failed, "the trigger must reject the first table write");
    assert!(
        later_insert_works,
        "a failed table write must not leave an open transaction"
    );
    assert_eq!(row_count, 1, "only the later table write must persist");
}

#[test]
fn locked_table_write_rolls_back_and_connection_remains_usable() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    let database_path = tmp.path().join("contention.sqlite");
    lua.globals()
        .set("contention_database_path", database_path.to_string_lossy())
        .unwrap();

    let (locked, later_insert_works, row_count): (bool, bool, i64) = lua
        .load(
            r#"
            local sqlite = require('vendor.sqlite')
            local first = sqlite.new(contention_database_path)
            local second = sqlite.new(contention_database_path)
            first:open()
            second:open()
            first:execute('PRAGMA journal_mode = WAL')
            second:execute('PRAGMA busy_timeout = 10')
            first:execute('CREATE TABLE counters (id INTEGER PRIMARY KEY, name TEXT NOT NULL)')

            first:execute('BEGIN IMMEDIATE')
            local ok = pcall(function()
                second:insert('counters', { name = 'blocked' })
            end)
            first:execute('ROLLBACK')

            local later_ok = pcall(function()
                second:insert('counters', { name = 'fresh' })
            end)
            local rows = second:eval('SELECT * FROM counters')
            second:close()
            first:close()
            return not ok, later_ok, #rows
            "#,
        )
        .eval()
        .expect("recover after SQLite lock");

    assert!(locked, "the first connection must block the second write");
    assert!(
        later_insert_works,
        "a locked table write must not leave an open transaction"
    );
    assert_eq!(row_count, 1, "only the later table write must persist");
}

#[test]
fn failed_transaction_commit_rolls_back_and_connection_remains_usable() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "failed-commit");

    let (commit_failed, later_transaction_works, child_count): (bool, bool, i64) = lua
        .load(
            r#"
            local db = plugin.db{ memory = true, version = 1 }
            db:execute([[
                CREATE TABLE parents (id INTEGER PRIMARY KEY)
            ]])
            db:execute([[
                CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER NOT NULL,
                    FOREIGN KEY (parent_id) REFERENCES parents(id)
                        DEFERRABLE INITIALLY DEFERRED
                )
            ]])

            local ok = pcall(function()
                db:execute(function(self)
                    self:eval('INSERT INTO children (id, parent_id) VALUES (1, 999)')
                end)
            end)

            local later_ok = pcall(function()
                db:execute(function(self)
                    self:eval('INSERT INTO parents (id) VALUES (1)')
                    self:eval('INSERT INTO children (id, parent_id) VALUES (2, 1)')
                end)
            end)
            local rows = db:eval('SELECT * FROM children')
            return not ok, later_ok, #rows
            "#,
        )
        .eval()
        .expect("recover after failed transaction commit");

    assert!(commit_failed, "the deferred foreign key must reject the commit");
    assert!(
        later_transaction_works,
        "a failed commit must not leave an open transaction"
    );
    assert_eq!(child_count, 1, "the failed transaction must not persist");
}

// ============================================================================
// 7. test_eval_escape_hatch (raw SQL with placeholders)
// ============================================================================

#[test]
fn eval_raw_sql_with_placeholders() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "ev");

    let (row_body, count): (String, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    messages = {
                        id = true,
                        body = { 'text', required = true },
                    },
                },
            }
            db:eval("INSERT INTO messages (body) VALUES (?)", 'via_eval')
            local rows = db:eval("SELECT body FROM messages WHERE body = ?", 'via_eval')
            return rows[1].body, #rows
            "#,
        )
        .eval()
        .expect("eval with args");

    assert_eq!(row_body, "via_eval");
    assert_eq!(count, 1);
}

// ============================================================================
// 8. test_additive_migration_new_column — file-backed
// ============================================================================

#[test]
fn reload_with_new_column_adds_it_and_preserves_data() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "addcol");

    // First load: only `body`.
    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = {
                messages = {
                    id = true,
                    body = { 'text', required = true, default = '' },
                },
            },
        }
        db.messages:insert{ body = 'first' }
        db.messages:insert{ body = 'second' }
        "#,
    )
    .exec()
    .expect("v1 load");

    // Simulate hot-reload: clear the cached handle (new Lua VM, same data dir).
    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "addcol");

    let (count, author_for_first, author_for_second): (i64, String, String) = lua
        .load(
            r#"
            local db = plugin.db{
                version = 1,
                models = {
                    messages = {
                        id = true,
                        body = { 'text', required = true, default = '' },
                        -- New column, still nullable with a default.
                        author = { 'text', default = 'anonymous' },
                    },
                },
            }
            local rows = db.messages:get{}
            local by_body = {}
            for _, r in ipairs(rows) do by_body[r.body] = r.author end
            return #rows, by_body['first'] or '', by_body['second'] or ''
            "#,
        )
        .eval()
        .expect("v1 reload with new column");

    assert_eq!(count, 2, "existing rows must survive ADD COLUMN");
    assert_eq!(author_for_first, "anonymous");
    assert_eq!(author_for_second, "anonymous");
}

// ============================================================================
// 9. test_additive_migration_new_table
// ============================================================================

#[test]
fn reload_with_new_table_creates_it_and_preserves_original() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "addtbl");

    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = {
                messages = {
                    id = true,
                    body = { 'text', required = true, default = '' },
                },
            },
        }
        db.messages:insert{ body = 'original' }
        "#,
    )
    .exec()
    .expect("v1 load");

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "addtbl");

    let (messages_count, channels_count, first_msg_body): (i64, i64, String) = lua
        .load(
            r#"
            local db = plugin.db{
                version = 1,
                models = {
                    messages = {
                        id = true,
                        body = { 'text', required = true, default = '' },
                    },
                    channels = {
                        id = true,
                        name = { 'text', required = true, default = '' },
                    },
                },
            }
            db.channels:insert{ name = 'general' }
            local msgs = db.messages:get{}
            return #msgs, #db.channels:get{}, msgs[1].body
            "#,
        )
        .eval()
        .expect("reload with new table");

    assert_eq!(messages_count, 1);
    assert_eq!(channels_count, 1);
    assert_eq!(first_msg_body, "original");
}

// ============================================================================
// 10. test_version_bump_runs_migration_fn (+ idempotent on subsequent reload)
// ============================================================================

#[test]
fn version_bump_runs_migration_fn_exactly_once() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "versbump");

    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = {
                messages = {
                    id = true,
                    author = { 'text', required = true, default = '' },
                },
            },
        }
        db.messages:insert{ author = 'JASON' }
        db.messages:insert{ author = 'Alice' }
        "#,
    )
    .exec()
    .expect("v1 load");

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "versbump");

    // Reload at v2 with a migration that lowercases author. Track invocations
    // in a persistent registry file so a second reload at v2 can prove it did
    // not re-run. (A simple in-Lua counter resets across VMs.)
    let (a_lower, b_lower, run_count_first): (String, String, i64) = lua
        .load(
            r#"
            _G._mig_runs = 0
            local db = plugin.db{
                version = 2,
                models = {
                    messages = {
                        id = true,
                        author = { 'text', required = true, default = '' },
                    },
                },
                migrations = {
                    [2] = function(db)
                        _G._mig_runs = _G._mig_runs + 1
                        db:eval("UPDATE messages SET author = lower(author)")
                    end,
                },
            }
            local rows = db.messages:get{}
            local by_id = {}
            for _, r in ipairs(rows) do by_id[r.id] = r.author end
            return by_id[1] or '', by_id[2] or '', _G._mig_runs
            "#,
        )
        .eval()
        .expect("v1 -> v2 migration");
    assert_eq!(a_lower, "jason");
    assert_eq!(b_lower, "alice");
    assert_eq!(run_count_first, 1, "migration[2] should run exactly once");

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "versbump");

    let run_count_second: i64 = lua
        .load(
            r#"
            _G._mig_runs = 0
            plugin.db{
                version = 2,
                models = {
                    messages = {
                        id = true,
                        author = { 'text', required = true, default = '' },
                    },
                },
                migrations = {
                    [2] = function(db)
                        _G._mig_runs = _G._mig_runs + 1
                    end,
                },
            }
            return _G._mig_runs
            "#,
        )
        .eval()
        .expect("reload at v2 should not re-run migration");
    assert_eq!(
        run_count_second, 0,
        "reload at same version must not re-run migrations"
    );
}

// ============================================================================
// 11. test_user_version_honored (reload at same version = no migration)
// ============================================================================

#[test]
fn reload_at_same_version_does_not_re_run_migrations() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "uvhonored");

    lua.load(
        r#"
        plugin.db{
            version = 3,
            models = {
                t = {
                    id = true,
                    v = { 'integer', required = true, default = 0 },
                },
            },
            migrations = {
                [2] = function(db) db:eval("UPDATE t SET v = v + 1") end,
                [3] = function(db) db:eval("UPDATE t SET v = v + 10") end,
            },
        }
        "#,
    )
    .exec()
    .expect("initial load at v3 (fresh db, migrations 1->2->3 apply)");

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "uvhonored");

    let uv: i64 = lua
        .load(
            r#"
            local ran = {}
            local db = plugin.db{
                version = 3,
                models = {
                    t = {
                        id = true,
                        v = { 'integer', required = true, default = 0 },
                    },
                },
                migrations = {
                    [2] = function() ran[#ran+1] = 2 end,
                    [3] = function() ran[#ran+1] = 3 end,
                },
            }
            assert(#ran == 0, 'no migrations should re-run: ' .. table.concat(ran, ','))
            local r = db:eval("PRAGMA user_version")
            return r[1].user_version
            "#,
        )
        .eval::<i64>()
        .expect("read user_version on reload");
    assert_eq!(uv, 3, "user_version should be 3 after reload at v3");
}

// ============================================================================
// 12. test_downgrade_refused
// ============================================================================

#[test]
fn declaring_lower_version_refuses_load() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "downgr");

    lua.load(
        r#"
        plugin.db{
            version = 3,
            models = {
                t = {
                    id = true,
                    v = { 'integer', required = true, default = 0 },
                },
            },
        }
        "#,
    )
    .exec()
    .expect("initial v3 load");

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "downgr");

    let err = lua
        .load(
            r#"
            plugin.db{
                version = 2,
                models = {
                    t = {
                        id = true,
                        v = { 'integer', required = true, default = 0 },
                    },
                },
            }
            "#,
        )
        .exec()
        .err()
        .expect("downgrade must raise");

    let msg = err.to_string();
    assert!(
        msg.contains("Downgrades are not supported")
            && msg.contains("version=2")
            && msg.contains("database is at version 3"),
        "downgrade error message missing expected content: {msg}"
    );
}

// ============================================================================
// 13. test_non_additive_mismatch_refused
// ============================================================================

#[test]
fn non_additive_change_without_version_bump_is_refused() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "strict");

    lua.load(
        r#"
        plugin.db{
            version = 1,
            models = {
                t = {
                    id = true,
                    name = { 'text', required = true, default = '' },
                },
            },
        }
        "#,
    )
    .exec()
    .expect("initial v1 load");

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "strict");

    let err = lua
        .load(
            r#"
            -- Change `name` from text to integer at the same version.
            plugin.db{
                version = 1,
                models = {
                    t = {
                        id = true,
                        name = { 'integer', required = true, default = 0 },
                    },
                },
            }
            "#,
        )
        .exec()
        .err()
        .expect("non-additive change must raise");

    let msg = err.to_string();
    assert!(
        msg.contains("Non-additive changes detected")
            && msg.contains("type changed")
            && msg.contains("text")
            && msg.contains("integer"),
        "schema-mismatch message missing expected content: {msg}"
    );
}

// ============================================================================
// 14. test_hot_reload_preserves_data_and_handle (same connection reused)
// ============================================================================

#[test]
fn hot_reload_same_vm_reuses_handle_and_data() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "hot");

    // Open the db, stash a sentinel on the returned instance, insert a row.
    // Then call `plugin.db{...}` again — with the same plugin name — and
    // confirm the returned value IS the same sentinel-bearing table.
    let (same_handle, row_count): (bool, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                version = 1,
                models = {
                    things = {
                        id = true,
                        label = { 'text', required = true, default = '' },
                    },
                },
            }
            rawset(db, '__test_marker', 'original')
            db.things:insert{ label = 'one' }

            local db2 = plugin.db{
                version = 1,
                models = {
                    things = {
                        id = true,
                        label = { 'text', required = true, default = '' },
                    },
                },
            }
            local same = rawget(db2, '__test_marker') == 'original'
            local rows = db2.things:get{}
            return same, #rows
            "#,
        )
        .eval()
        .expect("hot reload");

    assert!(
        same_handle,
        "plugin.db should return the SAME db instance for a repeated call with the same plugin name"
    );
    assert_eq!(
        row_count, 1,
        "data must persist across the second plugin.db call"
    );
}

// ============================================================================
// 15. test_plugin_disable_keeps_file (nice-to-have)
// ============================================================================

#[test]
fn plugin_unload_evicts_cache_but_leaves_file() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "disabletest");

    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = {
                kv = {
                    id = true,
                    k = { 'text', required = true, default = '' },
                },
            },
        }
        db.kv:insert{ k = 'hello' }
        "#,
    )
    .exec()
    .expect("initial load + insert");

    let expected = tmp.path().join("plugin-data/disabletest/db.sqlite");
    assert!(expected.exists(), "db file must be created on first load");

    // Simulate hub/loader firing plugin_unloading — our stub's `hooks.notify`
    // invokes subscribed callbacks, which the install() wiring routes to
    // plugin_db._on_plugin_unloading.
    lua.load(r#"hooks.notify('plugin_unloading', { name = 'disabletest' })"#)
        .exec()
        .expect("fire plugin_unloading");

    assert!(
        expected.exists(),
        "db file must persist through an unload (we just evict the cached handle)"
    );

    // Re-require the plugin and confirm the row is still there.
    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = {
                kv = {
                    id = true,
                    k = { 'text', required = true, default = '' },
                },
            },
        }
        local rows = db.kv:get{}
        assert(#rows == 1 and rows[1].k == 'hello',
               'row must survive eviction + re-open')
        "#,
    )
    .exec()
    .expect("reopen after unload");
}

// ============================================================================
// 16. test_foreign_keys_enforced (nice-to-have)
// ============================================================================

#[test]
fn foreign_keys_enforced_via_on_delete_cascade() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "fk");

    let (before, after): (i64, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                memory = true,
                version = 1,
                models = {
                    channels = {
                        id = true,
                        name = { 'text', required = true, default = '' },
                    },
                    messages = {
                        id = true,
                        channel_id = {
                            'integer',
                            required = true,
                            reference = 'channels.id',
                            on_delete = 'cascade',
                        },
                        body = { 'text', required = true, default = '' },
                    },
                },
            }
            db.channels:insert{ name = 'general' }
            db.messages:insert{ channel_id = 1, body = 'hi' }
            db.messages:insert{ channel_id = 1, body = 'hello' }
            local before = #db.messages:get{}
            db.channels:remove{ id = 1 }
            local after = #db.messages:get{}
            return before, after
            "#,
        )
        .eval()
        .expect("FK cascade");

    assert_eq!(before, 2);
    assert_eq!(
        after, 0,
        "ON DELETE CASCADE should have removed both messages when parent channel was deleted"
    );
}

// ============================================================================
// 17. test_wal_mode_active — all four default pragmas applied (contract test)
// ============================================================================
//
// Codex flagged two PRAGMA-related concerns:
//   (a) `busy_timeout` must be set BEFORE any WAL/lock-taking pragma so
//       subsequent statements inherit the waiter.
//   (b) Every default PRAGMA must actually take effect; we can't rely on the
//       journal_mode + foreign_keys spot-check that the earlier test did.
//
// This test queries all four and asserts each one reflects the configured
// value, so a regression in ordering or silent-swallow would trip.

#[test]
fn all_default_pragmas_applied_in_correct_order() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "pragmas");

    let (jmode, fkeys, busy_timeout, sync_mode): (String, i64, i64, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                version = 1,
                models = {
                    t = { id = true },
                },
            }
            local jm      = db:eval("PRAGMA journal_mode")
            local fk      = db:eval("PRAGMA foreign_keys")
            local busy    = db:eval("PRAGMA busy_timeout")
            local sync    = db:eval("PRAGMA synchronous")
            return jm[1].journal_mode, fk[1].foreign_keys,
                   busy[1].timeout, sync[1].synchronous
            "#,
        )
        .eval()
        .expect("query all four default pragmas");

    assert_eq!(
        jmode, "wal",
        "journal_mode should be WAL for file-backed db"
    );
    assert_eq!(fkeys, 1, "foreign_keys should be ON (= 1)");
    assert_eq!(
        busy_timeout, 5000,
        "busy_timeout should be 5000 ms — must be set BEFORE WAL so contention is bounded"
    );
    assert_eq!(
        sync_mode, 1,
        "synchronous should be NORMAL (= 1), not FULL (= 2)"
    );
}

// ============================================================================
// 18. test_must_be_called_during_plugin_load
// ============================================================================

#[test]
fn calling_plugin_db_outside_plugin_load_errors_clearly() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    // Intentionally NOT setting _loading_plugin_name.

    let err = lua
        .load(
            r#"
            plugin.db{
                version = 1,
                models = { t = { id = true } },
            }
            "#,
        )
        .exec()
        .err()
        .expect("must raise when _loading_plugin_name is unset");

    let msg = err.to_string();
    assert!(
        msg.contains("must be called during plugin load"),
        "missing guidance about plugin-load context: {msg}"
    );
}

// ============================================================================
// 19. test_memory_vs_file_redeclared_rejected
// ============================================================================

#[test]
fn redeclaring_memory_flag_is_rejected() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "memswap");

    let err = lua
        .load(
            r#"
            plugin.db{ memory = true, version = 1, models = { t = { id = true } } }
            plugin.db{ memory = false, version = 1, models = { t = { id = true } } }
            "#,
        )
        .exec()
        .err()
        .expect("memory flag flip must raise");

    let msg = err.to_string();
    assert!(
        msg.contains("re-declared with memory=")
            && msg.contains("existing handle was opened with memory="),
        "memory-flag mismatch message missing expected content: {msg}"
    );
}

// ============================================================================
// 20. test_lib_plugin_db_hot_reload_preserves_handle_and_rewires_global
// ============================================================================
//
// Core lib/*.lua modules are hot-reloaded by `handlers.module_watcher` via
// `loader.reload("lib.plugin_db")`. Codex flagged that without `_before_reload`
// / `_after_reload` hooks:
//   (a) `_G.plugin.db` stays bound to the old module's closure, so edits to
//       plugin_db.lua are invisible at runtime.
//   (b) the cached db handles (module-level local) vanish, silently leaking
//       sqlite fds.
//
// This test simulates the reload by clearing `package.loaded["lib.plugin_db"]`
// and calling the reload lifecycle hooks the same way `hub.loader.reload`
// does, then verifies:
//   - a plugin that already had a db open before reload can still call
//     `plugin.db{...}` and get the SAME cached handle (sentinel preserved),
//   - previously inserted data is still readable,
//   - `_G.plugin.db` points at the NEW module's closure (added a marker to
//     the fresh module during reload and confirm the global saw it).

#[test]
fn lib_plugin_db_hot_reload_preserves_handle_and_rewires_global() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "hotmod");

    // Initial load: open db, stash a sentinel on the instance, insert a row.
    lua.load(
        r#"
        local db = plugin.db{
            version = 1,
            models = { logs = {
                id = true,
                msg = { 'text', required = true, default = '' },
            } },
        }
        rawset(db, '__pre_reload_sentinel', 'before-reload')
        db.logs:insert{ msg = 'survived' }
        "#,
    )
    .exec()
    .expect("pre-reload load + insert");

    // Simulate `loader.reload("lib.plugin_db")` — the same sequence
    // `cli/lua/hub/loader.lua:M.reload` runs for a lib module:
    //   1. call `_before_reload` on the current module
    //   2. drop it from package.loaded
    //   3. require() it again (fresh evaluation)
    //   4. call `_after_reload` on the new module
    lua.load(
        r#"
        local old = package.loaded['lib.plugin_db']
        assert(type(old._before_reload) == 'function',
               'plugin_db must export _before_reload')
        old._before_reload()
        package.loaded['lib.plugin_db'] = nil

        local fresh = require('lib.plugin_db')
        assert(fresh ~= old, 'require after drop should return a new module table')
        assert(type(fresh._after_reload) == 'function',
               'plugin_db must export _after_reload')
        fresh._after_reload()
        _G._reloaded_module = fresh
        "#,
    )
    .exec()
    .expect("simulate hot-reload lifecycle");

    // Post-reload: the global must point at the fresh module, the handle
    // must be reused (same sentinel), and the row must still be visible.
    let (global_rewired, same_handle, row_survived): (bool, bool, bool) = lua
        .load(
            r#"
            local db2 = plugin.db{
                version = 1,
                models = { logs = {
                    id = true,
                    msg = { 'text', required = true, default = '' },
                } },
            }
            local same_handle = rawget(db2, '__pre_reload_sentinel') == 'before-reload'

            -- Global points at the fresh module's closure, not the old one.
            local global_rewired = (plugin.db == _G._reloaded_module.db)

            local rows = db2.logs:get{}
            local row_survived = (#rows == 1 and rows[1].msg == 'survived')

            return global_rewired, same_handle, row_survived
            "#,
        )
        .eval()
        .expect("post-reload assertions");

    assert!(
        global_rewired,
        "_G.plugin.db must point at the FRESH module after reload — edits to plugin_db.lua wouldn't take effect otherwise"
    );
    assert!(
        same_handle,
        "cached sqlite connection must survive module hot-reload — otherwise we leak fds and break in-flight plugin state"
    );
    assert!(
        row_survived,
        "previously inserted row must still be readable through the new module's closure"
    );
}

// ============================================================================
// 21. test_reserved_model_name_rejected
// ============================================================================
//
// A plugin that declared a model named after a sqlite.db method (e.g. `close`,
// `insert`, `eval`) would silently break the db handle because `wrap_db`
// rawsets each model as an instance field, shadowing the class method. We
// reject at load time with a clear message.

#[test]
fn reserved_model_names_rejected_with_plugin_scoped_error() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "reserved");

    let err = lua
        .load(
            r#"
            plugin.db{
                memory = true,
                version = 1,
                models = {
                    close = { id = true },   -- would shadow sqlite.db:close
                },
            }
            "#,
        )
        .exec()
        .err()
        .expect("reserved model name should raise");

    let msg = err.to_string();
    assert!(
        msg.contains("reserved name") && msg.contains("'close'") && msg.contains("reserved"),
        "reserved-name error missing expected content: {msg}"
    );
}

// ============================================================================
// 21. test_migration_rolls_back_on_failure_with_mixed_methods
// ============================================================================
//
// If a migration fn calls sqlite.db:insert/update/delete (which internally
// wrap in BEGIN/COMMIT via `wrap_stmts`), the outer migration step's tx must
// still roll back atomically on a later error. Without the USER_TX_SET
// guard in run_migrations, the inner COMMIT would close our outer tx early
// and leave partial state committed.

#[test]
fn migration_step_rollback_is_atomic_across_wrap_stmts_calls() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "migatomic");

    lua.load(
        r#"
        plugin.db{
            version = 1,
            models = { t = { id = true, v = { 'integer', default = 0 } } },
        }
        "#,
    )
    .exec()
    .expect("v1 initial load");

    // Reload at v2 with a migration that first inserts via sqlite.db:insert
    // (uses wrap_stmts) and then errors. The whole step must roll back —
    // when we reopen at v1 (not v2) the row must NOT be present.
    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "migatomic");

    let err = lua
        .load(
            r#"
            plugin.db{
                version = 2,
                models = { t = { id = true, v = { 'integer', default = 0 } } },
                migrations = {
                    [2] = function(db)
                        -- This goes through sqlite.db:insert -> wrap_stmts,
                        -- which would (without our patch) commit the outer tx.
                        db:insert('t', { v = 999 })
                        -- Then explicitly fail the step.
                        error('intentional failure', 0)
                    end,
                },
            }
            "#,
        )
        .exec()
        .err()
        .expect("migration step should propagate the error");
    let msg = err.to_string();
    assert!(
        msg.contains("migration 2 (v1 -> v2) failed"),
        "missing expected migration-failure preamble: {msg}"
    );

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "migatomic");

    let (user_version, row_count): (i64, i64) = lua
        .load(
            r#"
            local db = plugin.db{
                version = 1,
                models = { t = { id = true, v = { 'integer', default = 0 } } },
            }
            local uv = db:eval('PRAGMA user_version')[1].user_version
            -- sqlite.lua's eval returns `true` for an empty SELECT, not {},
            -- so use COUNT(*) for an unconditional row-count.
            local count = db:eval('SELECT COUNT(*) AS n FROM t')[1].n
            return uv, count
            "#,
        )
        .eval()
        .expect("reopen at v1 after failed upgrade");

    assert_eq!(
        user_version, 1,
        "user_version must stay at 1 when the v2 migration rolled back"
    );
    assert_eq!(
        row_count, 0,
        "the db:insert inside the failed migration must have rolled back"
    );
}

// ============================================================================
// 22. test_required_column_without_default_errors_on_add
// ============================================================================

#[test]
fn adding_required_column_without_default_errors_clearly() {
    let _lock = lock_env();
    let tmp = TempDir::new().unwrap();
    set_config_dir(tmp.path());
    let lua = new_test_lua();
    set_loading_plugin(&lua, "reqnodefault");

    lua.load(
        r#"
        plugin.db{
            version = 1,
            models = {
                t = {
                    id = true,
                    a = { 'text', required = true, default = '' },
                },
            },
        }
        "#,
    )
    .exec()
    .expect("initial load");

    drop(lua);
    let lua = new_test_lua();
    set_loading_plugin(&lua, "reqnodefault");

    let err = lua
        .load(
            r#"
            plugin.db{
                version = 1,
                models = {
                    t = {
                        id = true,
                        a = { 'text', required = true, default = '' },
                        -- New required column without default: sqlite refuses
                        -- ADD COLUMN NOT NULL without DEFAULT.
                        b = { 'text', required = true },
                    },
                },
            }
            "#,
        )
        .exec()
        .err()
        .expect("required-without-default should raise");

    let msg = err.to_string();
    assert!(
        msg.contains("cannot add required column") && msg.contains("b") && msg.contains("default"),
        "required-without-default message missing expected content: {msg}"
    );
}
