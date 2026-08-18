//! Regression tests for generic session process-exit handling.
//!
//! `handlers.connections` used to look up only Agent sessions on
//! `process_exited`. Plugin-owned accessories are generic Session instances,
//! so auto-close policy must route through `Session.get`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_docs_in_private_items,
    reason = "test-code brevity"
)]

use std::path::PathBuf;

use mlua::Lua;

fn lua_src_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua")
}

fn new_lua() -> Lua {
    let lua = Lua::new();
    let dir = lua_src_dir();
    let setup = format!(
        r#"package.path = "{dir}/?.lua;{dir}/?/init.lua;" .. package.path"#,
        dir = dir.display()
    );
    lua.load(&setup).exec().expect("set package.path");
    lua
}

fn run_process_exited_case(auto_close_literal: &str) -> (i64, i64, String) {
    let lua = new_lua();
    let script = format!(
        r#"
        local callbacks = {{}}

        log = {{
          debug = function(...) end,
          info = function(...) end,
          warn = function(...) end,
          error = function(...) end,
        }}

        hooks = {{
          on = function(...) end,
          off = function(...) end,
          notify = function(...) end,
        }}

        timer = {{
          after_idle = function(_key, _delay, fn) fn() end,
          every = function(_delay, _fn) return "timer:output_activity" end,
          cancel = function(_id) end,
        }}

        events = {{
          on = function(name, fn)
            callbacks[name] = fn
            return name .. ":sub"
          end,
          off = function(_id) end,
        }}

        package.loaded["hub.state"] = {{
          get = function(_key, default) return default end,
        }}
        package.loaded["lib.agent"] = {{
          get = function(_uuid) return nil end,
          list = function() return {{}} end,
        }}
        package.loaded["lib.entity_model"] = {{
          publish_session = function(...) end,
          remove_session = function(...) end,
          patch_session = function(...) end,
          upsert_session_workspace = function(...) end,
          upsert_workspace = function(...) end,
          patch_workspace = function(...) end,
          upsert_connection_code = function(...) end,
          upsert_hub = function(...) end,
        }}
        package.loaded["lib.terminal_clients"] = {{
          set_focused = function(...) end,
          get_focused_sessions = function(...) return {{}} end,
          is_any_focused = function(...) return false end,
        }}
        package.loaded["lib.entity_broadcast"] = {{
          set_broadcaster = function(...) end,
        }}
        package.loaded["lib.surfaces"] = {{
          build_route_registry_payload = function()
            return {{ type = "ui_route_registry", routes = {{}} }}
          end,
          path = function(...) return nil end,
        }}
        local calls = {{ close = 0, update = 0, status = "" }}
        local session = {{
          get_meta = function(_self, key)
            if key == "auto_close_on_exit" then return {auto_close_literal} end
            return nil
          end,
          close = function(_self, delete_worktree)
            calls.close = calls.close + 1
            calls.delete_worktree = tostring(delete_worktree)
          end,
          update = function(_self, fields)
            calls.update = calls.update + 1
            calls.status = fields.status or ""
          end,
        }}
        package.loaded["lib.session"] = {{
          get = function(uuid)
            if uuid == "sess-accessory" then return session end
            return nil
          end,
          list = function() return {{}} end,
          is_system_session = function(...) return false end,
        }}

        require("handlers.connections")
        callbacks.process_exited({{ session_uuid = "sess-accessory", exit_code = 0 }})

        return calls.close, calls.update, calls.status
        "#,
    );
    lua.load(&script).eval().expect("run process_exited case")
}

#[test]
fn process_exited_auto_closes_generic_session_when_metadata_true() {
    let (close_count, update_count, status) = run_process_exited_case("true");

    assert_eq!(close_count, 1);
    assert_eq!(update_count, 0);
    assert_eq!(status, "");
}

#[test]
fn process_exited_marks_generic_session_exited_without_auto_close() {
    let (close_count, update_count, status) = run_process_exited_case("false");

    assert_eq!(close_count, 0);
    assert_eq!(update_count, 1);
    assert_eq!(status, "exited");
}

#[test]
fn process_exited_accepts_string_true_for_auto_close_metadata() {
    let (close_count, update_count, status) = run_process_exited_case(r#""true""#);

    assert_eq!(close_count, 1);
    assert_eq!(update_count, 0);
    assert_eq!(status, "");
}

#[test]
fn pty_title_changed_patches_clients_after_short_wire_debounce() {
    let lua = new_lua();
    let script = r#"
        local hook_callbacks = {}

        log = {
          debug = function(...) end,
          info = function(...) end,
          warn = function(...) end,
          error = function(...) end,
        }

        hooks = {
          on = function(name, id, fn) hook_callbacks[name] = fn end,
          off = function(...) end,
          notify = function(...) end,
        }

        local timer_calls = { after_idle = 0, after = 0, cancel = 0 }
        local after_callbacks = {}
        local after_delays = {}
        timer = {
          after = function(delay, fn)
            timer_calls.after = timer_calls.after + 1
            after_callbacks[#after_callbacks + 1] = fn
            after_delays[#after_delays + 1] = delay
            return "timer:osc_patch:" .. tostring(timer_calls.after)
          end,
          after_idle = function(_key, _delay, _fn)
            timer_calls.after_idle = timer_calls.after_idle + 1
          end,
          every = function(_delay, _fn)
            return "timer:output_activity"
          end,
          cancel = function(_id)
            timer_calls.cancel = timer_calls.cancel + 1
          end,
        }

        events = {
          on = function(name, _fn) return name .. ":sub" end,
          off = function(_id) end,
        }

        package.loaded["hub.state"] = {
          get = function(_key, default) return default end,
        }

        local session = {
          session_uuid = "sess-title",
          title = nil,
          cwd = nil,
          sync_count = 0,
          sync_refresh_workspace_status = nil,
          _sync_session_manifest = function(self, opts)
            self.sync_count = self.sync_count + 1
            self.sync_refresh_workspace_status = opts and opts.refresh_workspace_status
          end,
        }

        package.loaded["lib.agent"] = {
          get = function(uuid)
            if uuid == "sess-title" then return session end
            return nil
          end,
          list = function() return {} end,
        }
        package.loaded["lib.session"] = {
          get = function(uuid)
            if uuid == "sess-title" then return session end
            return nil
          end,
          list = function() return {} end,
          is_system_session = function(...) return false end,
        }

        local patches = {}
        package.loaded["lib.entity_model"] = {
          publish_session = function(...) end,
          remove_session = function(...) end,
          patch_session = function(target_session, fields)
            patches[#patches + 1] = require("lib.client_session_payload").project_fields(fields, target_session)
          end,
          upsert_session_workspace = function(...) end,
          upsert_workspace = function(...) end,
          patch_workspace = function(...) end,
          upsert_connection_code = function(...) end,
          upsert_hub = function(...) end,
        }
        package.loaded["lib.session_actions"] = {
          publish_count = 0,
          publish_for_session = function(...)
            package.loaded["lib.session_actions"].publish_count = package.loaded["lib.session_actions"].publish_count + 1
          end,
        }
        package.loaded["lib.terminal_clients"] = {
          set_focused = function(...) end,
          get_focused_sessions = function(...) return {} end,
          is_any_focused = function(...) return false end,
        }
        package.loaded["lib.entity_broadcast"] = {
          set_broadcaster = function(...) end,
        }
        package.loaded["lib.surfaces"] = {
          build_route_registry_payload = function()
            return { type = "ui_route_registry", routes = {} }
          end,
          path = function(...) return nil end,
        }

        local connections = require("handlers.connections")
        hook_callbacks.pty_title_changed({ session_uuid = "sess-title", title = "⠋ Working" })
        hook_callbacks.pty_title_changed({ session_uuid = "sess-title", title = "⠙ Working" })
        hook_callbacks.pty_cwd_changed({ session_uuid = "sess-title", cwd = "/tmp/botster" })
        local immediate_patch_count = #patches
        after_callbacks[1]()
        hook_callbacks.pty_title_changed({ session_uuid = "sess-title", title = "⠙ Working" })
        local after_same_value_timer_count = timer_calls.after
        hook_callbacks.pty_title_changed({ session_uuid = "sess-title", title = "Done" })
        local rearmed_patch_count = #patches
        after_callbacks[2]()
        connections._before_reload()

        return {
          title = session.title,
          cwd = session.cwd,
          immediate_patch_count = immediate_patch_count,
          rearmed_patch_count = rearmed_patch_count,
          patch_count = #patches,
          patch_title = patches[1].title,
          patch_cwd = patches[1].cwd,
          patch_display_name = patches[1].display_name,
          reload_patch_title = patches[2].title,
          sync_count = session.sync_count,
          sync_refresh_workspace_status = session.sync_refresh_workspace_status,
          after_idle_count = timer_calls.after_idle,
          patch_delay = after_delays[1],
          manifest_delay = after_delays[2],
          after_count = timer_calls.after,
          after_same_value_timer_count = after_same_value_timer_count,
          cancel_count = timer_calls.cancel,
          publish_count = package.loaded["lib.session_actions"].publish_count,
        }
        "#;

    let result: mlua::Table = lua.load(script).eval().expect("run pty title patch case");

    assert_eq!(result.get::<String>("title").unwrap(), "Done");
    assert_eq!(result.get::<String>("cwd").unwrap(), "/tmp/botster");
    assert_eq!(result.get::<i64>("immediate_patch_count").unwrap(), 0);
    assert_eq!(result.get::<i64>("rearmed_patch_count").unwrap(), 1);
    assert_eq!(result.get::<i64>("patch_count").unwrap(), 2);
    assert_eq!(result.get::<String>("patch_title").unwrap(), "⠙ Working");
    assert_eq!(result.get::<String>("patch_cwd").unwrap(), "/tmp/botster");
    assert_eq!(
        result.get::<String>("patch_display_name").unwrap(),
        "⠙ Working"
    );
    assert_eq!(result.get::<String>("reload_patch_title").unwrap(), "Done");
    assert_eq!(result.get::<i64>("sync_count").unwrap(), 1);
    assert!(!result.get::<bool>("sync_refresh_workspace_status").unwrap());
    assert_eq!(result.get::<i64>("after_idle_count").unwrap(), 0);
    assert_eq!(result.get::<f64>("patch_delay").unwrap(), 0.2);
    assert_eq!(result.get::<f64>("manifest_delay").unwrap(), 5.0);
    assert_eq!(result.get::<i64>("after_count").unwrap(), 3);
    assert_eq!(
        result.get::<i64>("after_same_value_timer_count").unwrap(),
        2
    );
    assert_eq!(result.get::<i64>("cancel_count").unwrap(), 3);
    assert_eq!(result.get::<i64>("publish_count").unwrap(), 2);
}

#[test]
fn pty_title_changed_flushes_pending_manifest_sync_before_reload() {
    let lua = new_lua();
    let script = r#"
        local hook_callbacks = {}

        log = {
          debug = function(...) end,
          info = function(...) end,
          warn = function(...) end,
          error = function(...) end,
        }

        hooks = {
          on = function(name, id, fn) hook_callbacks[name] = fn end,
          off = function(...) end,
          notify = function(...) end,
        }

        local timer_calls = { after_idle = 0, after = 0, cancel = 0 }
        local after_callbacks = {}
        local after_delays = {}
        timer = {
          after = function(delay, fn)
            timer_calls.after = timer_calls.after + 1
            after_callbacks[#after_callbacks + 1] = fn
            after_delays[#after_delays + 1] = delay
            return "timer:osc_patch:" .. tostring(timer_calls.after)
          end,
          after_idle = function(_key, _delay, _fn)
            timer_calls.after_idle = timer_calls.after_idle + 1
          end,
          every = function(_delay, _fn)
            return "timer:output_activity"
          end,
          cancel = function(_id)
            timer_calls.cancel = timer_calls.cancel + 1
          end,
        }

        events = {
          on = function(name, _fn) return name .. ":sub" end,
          off = function(_id) end,
        }

        package.loaded["hub.state"] = {
          get = function(_key, default) return default end,
        }

        local session = {
          session_uuid = "sess-title",
          title = nil,
          sync_count = 0,
          sync_refresh_workspace_status = nil,
          _sync_session_manifest = function(self, opts)
            self.sync_count = self.sync_count + 1
            self.sync_refresh_workspace_status = opts and opts.refresh_workspace_status
          end,
        }

        package.loaded["lib.agent"] = {
          get = function(uuid)
            if uuid == "sess-title" then return session end
            return nil
          end,
          list = function() return {} end,
        }
        package.loaded["lib.session"] = {
          get = function(uuid)
            if uuid == "sess-title" then return session end
            return nil
          end,
          list = function() return {} end,
          is_system_session = function(...) return false end,
        }

        package.loaded["lib.entity_model"] = {
          publish_session = function(...) end,
          remove_session = function(...) end,
          patch_session = function(...) end,
          upsert_session_workspace = function(...) end,
          upsert_workspace = function(...) end,
          patch_workspace = function(...) end,
          upsert_connection_code = function(...) end,
          upsert_hub = function(...) end,
        }
        package.loaded["lib.session_actions"] = {
          publish_for_session = function(...) end,
        }
        package.loaded["lib.terminal_clients"] = {
          set_focused = function(...) end,
          get_focused_sessions = function(...) return {} end,
          is_any_focused = function(...) return false end,
        }
        package.loaded["lib.entity_broadcast"] = {
          set_broadcaster = function(...) end,
        }
        package.loaded["lib.surfaces"] = {
          build_route_registry_payload = function()
            return { type = "ui_route_registry", routes = {} }
          end,
          path = function(...) return nil end,
        }

        local connections = require("handlers.connections")
        hook_callbacks.pty_title_changed({ session_uuid = "sess-title", title = "Reload Soon" })
        connections._before_reload()

        return {
          title = session.title,
          sync_count = session.sync_count,
          sync_refresh_workspace_status = session.sync_refresh_workspace_status,
          after_idle_count = timer_calls.after_idle,
          patch_delay = after_delays[1],
          manifest_delay = after_delays[2],
          after_count = timer_calls.after,
          cancel_count = timer_calls.cancel,
        }
        "#;

    let result: mlua::Table = lua
        .load(script)
        .eval()
        .expect("run reload pending manifest sync case");

    assert_eq!(result.get::<String>("title").unwrap(), "Reload Soon");
    assert_eq!(result.get::<i64>("sync_count").unwrap(), 1);
    assert!(!result.get::<bool>("sync_refresh_workspace_status").unwrap());
    assert_eq!(result.get::<i64>("after_idle_count").unwrap(), 0);
    assert_eq!(result.get::<f64>("patch_delay").unwrap(), 0.2);
    assert_eq!(result.get::<f64>("manifest_delay").unwrap(), 5.0);
    assert_eq!(result.get::<i64>("after_count").unwrap(), 2);
    assert!(result.get::<i64>("cancel_count").unwrap() >= 2);
}
