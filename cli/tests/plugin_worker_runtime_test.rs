//! Integration tests for isolated plugin worker execution.

#![expect(clippy::unwrap_used, reason = "test-code brevity")]

use std::fs;
use std::time::{Duration, Instant};

use botster::lua::LuaRuntime;
use tempfile::TempDir;

#[test]
fn plugin_owned_ui_action_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local action = require("lib.action")

        action.on("demo.worker", "main", function()
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                return action.result{ message = "worker:" .. key }
            end
            return action.result{ ok = false, error = "hub closure ran" }
        end, { timeout_ms = 2000 })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local result = require("lib.action").dispatch({{ id = "demo.worker" }}, {{}})
            assert(result.handled == true)
            assert(result.ok == true, result.error)
            assert(result.message == "worker:worker-plugin", result.message)
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_ui_action_receives_serializable_context_only() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-action-context-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local action = require("lib.action")

        action.on("demo.worker.context", "main", function(_envelope, ctx)
            return action.result{
                message = tostring(ctx.sub_id) .. ":" .. tostring(ctx.target_surface)
                    .. ":" .. tostring(ctx.client == nil),
            }
        end, { timeout_ms = 2000 })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-action-context-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local result = require("lib.action").dispatch({{ id = "demo.worker.context" }}, {{
                sub_id = "hub-sub",
                target_surface = "pipelines",
                client = {{ send = function() end }},
            }})
            assert(result.handled == true)
            assert(result.ok == true, result.error)
            assert(result.message == "hub-sub:pipelines:true", result.message)
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_session_action_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-session-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local actions = require("lib.session_actions")

        actions.register("demo.session.worker", {
            label = "Worker Session",
            timeout_ms = 250,
            run = function()
                local key = rawget(_G, "_plugin_worker_key")
                if key then
                    return "worker:" .. key
                end
                return nil, "hub closure ran"
            end,
        })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-session-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            package.loaded["lib.session"] = {{
                get = function(session_uuid)
                    return {{
                        info = function()
                            return {{ session_uuid = session_uuid, created_at = 1 }}
                        end,
                    }}
                end,
                all_info = function()
                    return {{ {{ session_uuid = "sess-worker", created_at = 1 }} }}
                end,
            }}

            local ok_run, result = require("lib.session_actions").run("sess-worker", "demo.session.worker", {{
                client = {{ send = function() error("client should not cross worker boundary") end }},
                sub_id = "sub-worker",
                params = {{}},
            }})
            assert(ok_run == true, tostring(result))
            assert(result == "worker:worker-session-plugin", tostring(result))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_notification_claim_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-notification-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local notifications = require("lib.notifications")

        notifications.claim({
            name = "demo.notification.worker",
            scope = { all_sessions = true },
            capabilities = { "notifications.global_claim" },
            timeout_ms = 250,
            handler = function(intent)
                local key = rawget(_G, "_plugin_worker_key")
                if key then
                    return {
                        core = "replace",
                        custom = {
                            title = "worker:" .. key,
                            body = intent.message,
                            push = false,
                        },
                    }
                end
                return { core = "replace", custom = { title = "hub closure ran" } }
            end,
        })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-notification-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local decision = require("lib.notifications").evaluate({{
                session_uuid = "sess-worker",
                message = "permission required",
            }})
            assert(decision.core == "replace", tostring(decision.core))
            assert(decision.custom.title == "worker:worker-notification-plugin", decision.custom.title)
            assert(decision.custom.body == "permission required", decision.custom.body)
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_worker_session_action_can_prepare_plugin_command_on_parent_hub() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-parent-hub-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local Hub = require("lib.hub")
        local actions = require("lib.session_actions")

        actions.register("demo.session.parent_hub", {
            label = "Parent Hub",
            timeout_ms = 2000,
            run = function(session_uuid)
                local parent = Hub.get()
                parent:prepare_plugin_command({
                    request_id = "prep-" .. session_uuid,
                    command = "cloudflared",
                    context = { parent_session_uuid = session_uuid },
                })
                return "queued"
            end,
        })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            hub = hub or {{}}
            hub.hub_id = hub.hub_id or function() return "hub-test" end
            hub.server_id = hub.server_id or function() return "hub-test" end
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-parent-hub-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            hub.prepare_plugin_command = function(opts)
                _G.prepare_request = opts
            end
            package.loaded["lib.session"] = {{
                get = function(session_uuid)
                    return {{
                        info = function()
                            return {{ session_uuid = session_uuid, created_at = 1 }}
                        end,
                    }}
                end,
                all_info = function()
                    return {{ {{ session_uuid = "sess-worker", created_at = 1 }} }}
                end,
            }}

            local ok_run, result = require("lib.session_actions").run("sess-worker", "demo.session.parent_hub", {{ params = {{}} }})
            assert(ok_run == true, tostring(result))
            assert(result == "queued", tostring(result))
            assert(prepare_request.request_id == "prep-sess-worker", tostring(prepare_request and prepare_request.request_id))
            assert(prepare_request.command == "cloudflared", tostring(prepare_request and prepare_request.command))
            assert(prepare_request.context.parent_session_uuid == "sess-worker")
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_worker_session_get_can_resolve_system_connector_sessions() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-system-session-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local Session = require("lib.session")
        local actions = require("lib.session_actions")

        actions.register("demo.session.system_lookup", {
            label = "System Lookup",
            timeout_ms = 2000,
            run = function(_session_uuid, _action_id, params)
                assert(rawget(_G, "_plugin_worker_parent_hub_id"), "parent hub bridge missing")
                local connector_uuid = params.connector_uuid or (params.params and params.params.connector_uuid)
                local connector = Session.get(connector_uuid)
                if not connector then
                    return nil, "connector missing: " .. tostring(connector_uuid)
                end
                return {
                    session_uuid = connector.session_uuid,
                    owner_plugin = connector.metadata and connector.metadata.owner_plugin,
                    target_session_uuid = connector:get_meta("target_session_uuid"),
                }
            end,
        })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            hub = hub or {{}}
            hub.hub_id = hub.hub_id or function() return "hub-test" end
            hub.server_id = hub.server_id or function() return "hub-test" end

            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-system-session-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local all_info_calls = {{}}
            local Hub = require("lib.hub")
            Hub._handle_worker_parent_request = function(payload)
                assert(payload.type == "get_agent_list", tostring(payload.type))
                local opts = payload.opts or {{}}
                all_info_calls[#all_info_calls + 1] = opts
                local rows = {{
                    {{
                        session_uuid = "parent-session",
                        id = "parent-session",
                        metadata = {{}},
                    }},
                    {{
                        session_uuid = "connector-session",
                        id = "connector-session",
                        metadata = {{
                            system_session = true,
                            owner_plugin = "cloudflare-hosted-preview",
                            system_kind = "cloudflare_hosted_preview_connector",
                            target_session_uuid = "parent-session",
                        }},
                    }},
                }}
                return {{ result = rows }}
            end

            local response = __plugin_worker_invoke(
                "worker-system-session-plugin",
                "session_action",
                "demo.session.system_lookup",
                nil,
                {{
                    session_uuid = "parent-session",
                    payload = {{ params = {{ connector_uuid = "connector-session" }} }},
                }},
                2000
            )
            assert(response.ok == true, tostring(response.error))
            local result = response.result
            assert(result.session_uuid == "connector-session", tostring(result.session_uuid))
            assert(result.owner_plugin == "cloudflare-hosted-preview", tostring(result.owner_plugin))
            assert(result.target_session_uuid == "parent-session", tostring(result.target_session_uuid))
            assert(all_info_calls[#all_info_calls].include_system == true)
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_worker_update_session_enqueues_parent_hub_command_without_blocking() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-update-session-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    let marker_path = tmp.path().join("parent-update-dispatched.txt");
    fs::write(
        &init_path,
        r#"
        local Hub = require("lib.hub")
        local actions = require("lib.session_actions")

        actions.register("demo.session.update_parent", {
            label = "Update Parent",
            timeout_ms = 2000,
            run = function(session_uuid)
                local result = Hub.get():update_session(session_uuid, {
                    plugin_state = {
                        cloudflare_hosted_preview = {
                            status = "running",
                            url = "https://preview.trycloudflare.com",
                        },
                    },
                })
                assert(result.status == "queued", tostring(result.status))
                return result.status
            end,
        })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            hub = hub or {{}}
            hub.hub_id = hub.hub_id or function() return "hub-test" end
            hub.server_id = hub.server_id or function() return "hub-test" end

            local marker_path = {marker_path}
            local Hub = require("lib.hub")
            Hub._handle_worker_parent_request = function(payload)
                if payload.type == "get_agent_list" then
                    return {{
                        result = {{
                            {{
                                session_uuid = "parent-session",
                                id = "parent-session",
                                port = 4567,
                                metadata = {{}},
                            }},
                        }},
                    }}
                end
                assert(payload.type == "hub_command", tostring(payload.type))
                local command = payload.command
                assert(command.type == "update_session", tostring(command.type))
                assert(command.agent_id == "parent-session", tostring(command.agent_id))
                assert(command.plugin_state.cloudflare_hosted_preview.status == "running")
                local file = assert(io.open(marker_path, "w"))
                file:write(command.plugin_state.cloudflare_hosted_preview.url)
                file:close()
                return {{
                    result = {{
                        ok = true,
                        status = "queued",
                        request_id = command.request_id,
                    }},
                }}
            end

            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-update-session-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local response = __plugin_worker_invoke(
                "worker-update-session-plugin",
                "session_action",
                "demo.session.update_parent",
                nil,
                {{
                    session_uuid = "parent-session",
                    payload = {{ params = {{}} }},
                }},
                2000
            )
            assert(response.ok == true, tostring(response.error))
            assert(response.result == "queued", tostring(response.result))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
            marker_path = serde_json::to_string(&marker_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker_path.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    let marker = fs::read_to_string(marker_path).expect("parent update dispatch marker");
    assert_eq!(marker, "https://preview.trycloudflare.com");
}

#[test]
fn plugin_owned_command_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-command-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local commands = require("lib.commands")

        commands.register("demo_worker_command", function(_, _, command)
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                return "worker:" .. key .. ":" .. tostring(command.value)
            end
            error("hub closure ran")
        end, { timeout_ms = 2000 })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-command-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local ok_dispatch, result = require("lib.commands").dispatch(nil, nil, {{
                type = "demo_worker_command",
                value = "ok",
            }})
            assert(ok_dispatch == true, tostring(result))
            assert(result == "worker:worker-command-plugin:ok", tostring(result))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_hook_interceptor_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-hook-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local hooks = require("hub.hooks")

        hooks.intercept("demo_worker_intercept", "main", function(payload)
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                return { value = "worker:" .. key .. ":" .. tostring(payload.value) }
            end
            error("hub closure ran")
        end, { timeout_ms = 2000 })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-hook-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local hooks = require("hub.hooks")
            local listed = hooks.list("demo_worker_intercept")
            assert(#listed == 1, tostring(#listed))
            assert(listed[1].owner_plugin == "worker-hook-plugin", tostring(listed[1].owner_plugin))

            local result = hooks.call("demo_worker_intercept", {{ value = "ok" }})
            assert(type(result) == "table", tostring(result))
            assert(result.value == "worker:worker-hook-plugin:ok", tostring(result.value))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_surface_route_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-surface-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let asset_path = plugin_dir.join("graph.html");
    fs::write(&asset_path, "<html>worker graph</html>").unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local surfaces = require("lib.surfaces")
        local plugin_assets = require("lib.plugin_assets")

        surfaces.register("worker_surface", {
            label = "Worker Surface",
            timeout_ms = 2000,
            routes = {
                {
                    path = "/item/:id",
                    render = function(state, ctx)
                        local key = rawget(_G, "_plugin_worker_key")
                        if key then
                            local asset_url = plugin_assets.expose_file("graph", __ASSET_PATH__, {
                                content_type = "text/html",
                            })
                            return {
                                type = "text",
                                props = {
                                    text = "worker:" .. key .. ":" .. tostring(state.params.id) .. ":" .. ctx.path("/item/:id", { id = state.params.id }) .. ":" .. asset_url,
                                },
                            }
                        end
                        error("hub closure ran")
                    end,
                },
            },
        })

        return {}
        "#
        .replace(
            "__ASSET_PATH__",
            &serde_json::to_string(&asset_path.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-surface-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local surfaces = require("lib.surfaces")
            local entry = surfaces.get("worker_surface")
            assert(entry.owner_plugin == "worker-surface-plugin", tostring(entry.owner_plugin))

            local tree = surfaces.render_node("worker_surface", {{
                hub_id = "hub-test",
                path = "/item/42",
            }})
            assert(tree.type == "text", tostring(tree.type))
            assert(
                tree.props.text:match("^worker:worker%-surface%-plugin:42:/hubs/hub%-test/worker_surface/item/42:botster%-plugin%-asset://worker%-surface%-plugin:graph%?v="),
                tostring(tree.props.text)
            )
            local result, read_err = require("lib.plugin_assets").read("worker-surface-plugin:graph")
            assert(result ~= nil, tostring(read_err))
            assert(result.content == "<html>worker graph</html>", tostring(result and result.content))
            assert(result.content_type == "text/html", tostring(result.content_type))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn hub_visible_plugin_surfaces_survive_plugin_worker_bootstrap() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        );
        std::env::set_var("BOTSTER_REPO", "botster/test");
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-bootstrap-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local surfaces = require("lib.surfaces")

        local repo = hub.detect_repo()
        if repo ~= "botster/test" then
            error("unexpected repo: " .. tostring(repo))
        end

        local db = plugin.db({
            version = 1,
            memory = true,
            models = {
                entries = {
                    id = true,
                    label = "text",
                },
            },
        })
        db.entries:insert({ label = "booted" })

        mcp.tool("worker_boot_probe", {
            description = "worker bootstrap probe",
            input_schema = { type = "object", properties = {} },
        }, function()
            return rawget(_G, "_plugin_worker_key") or "hub"
        end)

        surfaces.register("worker_boot_surface", {
            label = "Worker Boot Surface",
            routes = {
                {
                    path = "/",
                    render = function()
                        return {
                            type = "text",
                            props = {
                                text = rawget(_G, "_plugin_worker_key") or "hub",
                            },
                        }
                    end,
                },
            },
        })

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            _G.hub = {{
                hub_id = function() return "hub-test" end,
                server_id = function() return "hub-test" end,
                detect_repo = function() return "botster/test" end,
            }}
            _G.mcp = require("lib.mcp")
            local plugin_db = require("lib.plugin_db")
            plugin_db.install()

            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-bootstrap-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local surfaces = require("lib.surfaces")
            local entry = surfaces.get("worker_boot_surface")
            assert(entry ~= nil, "surface was unregistered after worker load")
            assert(entry.owner_plugin == "worker-bootstrap-plugin", tostring(entry.owner_plugin))
            assert(entry.path ~= nil, "surface path missing")
            assert(entry.clients == nil or entry.clients.web == true, "surface not web-visible")

            local payload = surfaces.build_route_registry_payload("hub-test")
            local found = false
            for _, route in ipairs(payload.routes) do
                if route.surface == "worker_boot_surface" then found = true end
            end
            assert(found == true, "surface missing from hub route registry payload: " .. json.encode(payload))

            local tree = surfaces.render_node("worker_boot_surface", {{
                hub_id = "hub-test",
                path = "/",
            }})
            assert(tree.props.text == "worker-bootstrap-plugin", tostring(tree.props.text))

            local content, tool_err = require("lib.mcp").call_tool("worker_boot_probe", {{}}, {{}})
            assert(tool_err == nil, tostring(tool_err))
            assert(content[1].text == "worker-bootstrap-plugin", tostring(content[1] and content[1].text))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_worker_can_query_parent_hub_during_bootstrap() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-parent-query-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local Hub = require("lib.hub")
        local sessions = Hub.get():list_agents()
        if #sessions ~= 1 then
            error("expected one parent session, got " .. tostring(#sessions))
        end
        if sessions[1].session_uuid ~= "sess-parent" then
            error("unexpected parent session: " .. tostring(sessions[1].session_uuid))
        end
        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            _G.hub = {{
                hub_id = function() return "hub-parent" end,
                server_id = function() return "hub-parent" end,
                detect_repo = function() return "botster/test" end,
            }}
            package.loaded["lib.agent"] = {{
                list = function() return {{}} end,
                get = function() return nil end,
                all_info = function()
                    return {{
                        {{
                            session_uuid = "sess-parent",
                            label = "Parent Session",
                            metadata = {{}},
                            status = "running",
                        }},
                    }}
                end,
            }}

            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-parent-query-plugin", {{ source = "device" }})
            assert(ok, tostring(err))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_asset_message_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-asset-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let asset_path = plugin_dir.join("frame.html");
    fs::write(&asset_path, "<html></html>").unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        format!(
            r#"
            local action = require("lib.action")
            local plugin_assets = require("lib.plugin_assets")

            plugin_assets.expose_file("frame", {asset_path}, {{ content_type = "text/html" }})
            plugin_assets.on_message("ping", function(payload, ctx)
                local key = rawget(_G, "_plugin_worker_key")
                if key then
                    return action.result{{
                        message = "worker:" .. key .. ":" .. tostring(payload.value) .. ":" .. tostring(ctx.asset_id) .. ":" .. tostring(ctx.target_surface),
                    }}
                end
                error("hub closure ran")
            end, {{ timeout_ms = 2000 }})

            return {{}}
            "#,
            asset_path = serde_json::to_string(&asset_path.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-asset-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local plugin_assets = require("lib.plugin_assets")
            plugin_assets._install_action_handler()

            local result = require("lib.action").dispatch({{
                id = "botster.plugin_asset.message",
                payload = {{
                    assetId = "worker-asset-plugin:frame",
                    action = "ping",
                    payload = {{ value = "ok" }},
                }},
            }}, {{
                sub_id = "sub-asset",
                target_surface = "asset_surface",
            }})

            assert(result.handled == true, tostring(result.error))
            assert(result.ok == true, tostring(result.error))
            assert(
                result.message == "worker:worker-asset-plugin:ok:worker-asset-plugin:frame:asset_surface",
                tostring(result.message)
            )
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_timer_handler_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-timer-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        timer.after(60, function()
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                return "worker:" .. key
            end
            error("hub closure ran")
        end)

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-timer-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local result = __plugin_worker_invoke(
                "worker-timer-plugin",
                "timer",
                "worker-timer-plugin:timer_0",
                nil,
                {{}},
                2000
            )
            assert(result == "worker:worker-timer-plugin", tostring(result))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_timer_fires_after_delay_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-timer-delay-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    let marker_path = tmp.path().join("timer-fired.txt");
    fs::write(
        &init_path,
        format!(
            r#"
            local marker_path = {marker_path}
            timer.after(0.05, function()
                local key = rawget(_G, "_plugin_worker_key")
                local file = assert(io.open(marker_path, "a"))
                file:write((key or "hub") .. "\n")
                file:close()
            end)

            return {{}}
            "#,
            marker_path = serde_json::to_string(&marker_path.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-timer-delay-plugin", {{ source = "device" }})
            assert(ok, tostring(err))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !marker_path.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }

    std::thread::sleep(std::time::Duration::from_millis(150));

    let marker = fs::read_to_string(marker_path).expect("plugin worker timer marker");
    assert_eq!(marker, "worker-timer-delay-plugin\n");
}

#[test]
fn core_hook_timers_do_not_inherit_plugin_loading_context() {
    // SAFETY: This integration filter runs this test in isolation and the
    // runtime must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(
            r#"
            local hooks = require("hub.hooks")
            hooks.on("surfaces_changed", "broadcast_ui_route_registry", function()
                timer.after_idle("ui_route_registry_broadcast", 10, function() end)
            end)

            _G._loading_plugin_key = "project-pipelines"
            hooks.notify("surfaces_changed", {})
            assert(_G._loading_plugin_key == "project-pipelines")
            _G._loading_plugin_key = nil

            local removed = timer._unregister_by_plugin("project-pipelines")
            local cancelled = timer.cancel("ui_route_registry_broadcast")
            assert(removed == 0, tostring(removed))
            assert(cancelled == true)
            return true
            "#,
        )
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_event_handler_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-event-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        events.on("demo_worker_event", function(payload)
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                assert(payload.value == "ok", tostring(payload.value))
                return true
            end
            error("hub event closure ran")
        end)

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-event-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local count = events.emit("demo_worker_event", {{ value = "ok" }})
            assert(count == 1, tostring(count))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_worker_can_enqueue_parent_event_without_blocking_callback() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let target_dir = tmp.path().join("target-plugin");
    let emitter_dir = tmp.path().join("emitter-plugin");
    fs::create_dir_all(&target_dir).unwrap();
    fs::create_dir_all(&emitter_dir).unwrap();

    let target_init = target_dir.join("init.lua");
    fs::write(
        &target_init,
        r#"
        events.on("worker_parent_target", function(payload)
            return true
        end)

        return {}
        "#,
    )
    .unwrap();

    let emitter_init = emitter_dir.join("init.lua");
    fs::write(
        &emitter_init,
        r#"
        events.on("worker_parent_emit", function()
            assert(rawget(_G, "_plugin_worker_key") == "emitter-plugin")
            assert(plugin_worker_parent_hub.enqueue({
                type = "emit_event",
                event = "worker_parent_target",
                data = { value = "ok" },
            }))
        end)

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({target_init}, "target-plugin", {{ source = "device" }})
            assert(ok, tostring(err))
            assert(events.has("worker_parent_target"), "target event should be registered in parent hub")
            local target_count = events.emit("worker_parent_target", {{ value = "ok" }})
            assert(target_count == 1, "target event direct emit count: " .. tostring(target_count))
            ok, err = loader.load_plugin({emitter_init}, "emitter-plugin", {{ source = "device" }})
            assert(ok, tostring(err))
            assert(events.has("worker_parent_emit"), "emitter event should be registered in parent hub")
            target_count = events.emit("worker_parent_target", {{ value = "ok" }})
            assert(target_count == 1, "target event direct emit after emitter count: " .. tostring(target_count))

            local count = events.emit("worker_parent_emit", {{}})
            assert(count == 1, tostring(count))
            return true
            "#,
            target_init = serde_json::to_string(&target_init.to_string_lossy()).unwrap(),
            emitter_init = serde_json::to_string(&emitter_init.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}

#[test]
fn plugin_owned_watch_callback_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-watch-plugin");
    let watched_dir = tmp.path().join("watched");
    fs::create_dir_all(&plugin_dir).unwrap();
    fs::create_dir_all(&watched_dir).unwrap();
    let output_path = tmp.path().join("watch-callback.txt");
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        format!(
            r#"
            local watched_dir = {watched_dir}
            local output_path = {output_path}

            watch.directory(watched_dir, {{ pattern = "*.txt" }}, function(event)
                local key = rawget(_G, "_plugin_worker_key")
                if key then
                    fs.write(output_path, "worker:" .. key .. ":" .. tostring(event.kind) .. ":" .. tostring(event.path))
                else
                    error("hub watch closure ran")
                end
            end)

            return {{}}
            "#,
            watched_dir = serde_json::to_string(&watched_dir.to_string_lossy()).unwrap(),
            output_path = serde_json::to_string(&output_path.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-watch-plugin", {{ source = "device" }})
            assert(ok, tostring(err))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();

    fs::write(watched_dir.join("changed.txt"), "ok").unwrap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut content = String::new();
    while Instant::now() < deadline {
        if let Ok(value) = fs::read_to_string(&output_path) {
            content = value;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        content.starts_with("worker:worker-watch-plugin:"),
        "{content}"
    );
    assert!(content.contains("changed.txt"), "{content}");
}

#[test]
fn plugin_owned_http_callback_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-http-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let output_path = tmp.path().join("http-callback.txt");
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        format!(
            r#"
            local output_path = {output_path}

            events.on("demo_worker_http", function()
                local key = rawget(_G, "_plugin_worker_key")
                if not key then error("hub event closure ran") end

                http.request("NOPE", "http://127.0.0.1/", function(_resp, err)
                    local callback_key = rawget(_G, "_plugin_worker_key")
                    if callback_key then
                        fs.write(output_path, "worker:" .. callback_key .. ":" .. tostring(err))
                    else
                        fs.write(output_path, "hub callback ran")
                    end
                end)
            end)

            return {{}}
            "#,
            output_path = serde_json::to_string(&output_path.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-http-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local count = events.emit("demo_worker_http", {{ value = "ok" }})
            assert(count == 1, tostring(count))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut content = String::new();
    while Instant::now() < deadline {
        if let Ok(value) = fs::read_to_string(&output_path) {
            content = value;
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    assert!(
        content.starts_with("worker:worker-http-plugin:Unsupported HTTP method: NOPE"),
        "{content}"
    );
}

#[test]
fn plugin_owned_local_webhook_runs_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-webhook-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let url_path = tmp.path().join("webhook-url.txt");
    let output_path = tmp.path().join("webhook-request.txt");
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        format!(
            r#"
            local url_path = {url_path}
            local output_path = {output_path}

            local route = local_webhooks.register({{
                id = "demo.local-webhook",
                methods = {{ "POST" }},
                path = "/hooks/<route_token>",
                body_limit = 1024,
                timeout_ms = 2000,
                response_mode = "handler",
            }}, function(request)
                local key = rawget(_G, "_plugin_worker_key")
                if not key then error("hub webhook closure ran") end
                fs.write(output_path, table.concat({{
                    key,
                    request.method,
                    request.body,
                    request.headers["content-type"] or "",
                    request.remote_addr,
                }}, "|"))
                return {{
                    status = 201,
                    headers = {{ ["x-worker"] = key }},
                    body = "handled:" .. request.route_id,
                }}
            end)

            if route.url then fs.write(url_path, route.url) end
            return {{}}
            "#,
            url_path = serde_json::to_string(&url_path.to_string_lossy()).unwrap(),
            output_path = serde_json::to_string(&output_path.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-webhook-plugin", {{ source = "device" }})
            assert(ok, tostring(err))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut url = String::new();
    while Instant::now() < deadline {
        if let Ok(content) = fs::read_to_string(&url_path) {
            url = content;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(url.starts_with("http://127.0.0.1:"), "{url}");

    let response = reqwest::blocking::Client::new()
        .post(url.trim())
        .header("content-type", "application/x-botster-test")
        .body("payload=ok")
        .send()
        .unwrap();
    assert_eq!(response.status().as_u16(), 201);
    assert_eq!(
        response.headers().get("x-worker").unwrap(),
        "worker-webhook-plugin"
    );
    assert_eq!(response.text().unwrap(), "handled:demo.local-webhook");

    let content = fs::read_to_string(output_path).unwrap();
    assert_eq!(
        content,
        "worker-webhook-plugin|POST|payload=ok|application/x-botster-test|127.0.0.1"
    );
}

#[test]
fn plugin_owned_local_webhook_response_modes_and_failures() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-webhook-modes-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let url_path = tmp.path().join("webhook-urls.txt");
    let ack_path = tmp.path().join("webhook-ack.txt");
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        format!(
            r#"
            local url_path = {url_path}
            local ack_path = {ack_path}

            local ack = local_webhooks.register({{
                id = "demo.ack",
                methods = {{ "POST" }},
                path = "/ack/<route_token>",
                response_mode = "ack",
            }}, function(request)
                fs.write(ack_path, request.route_id .. ":" .. request.body)
                return {{ status = 299, body = "ignored" }}
            end)

            local fail = local_webhooks.register({{
                id = "demo.fail",
                methods = {{ "POST" }},
                path = "/fail/<route_token>",
                response_mode = "handler",
            }}, function(_request)
                error("private failure detail")
            end)

            local timeout = local_webhooks.register({{
                id = "demo.timeout",
                methods = {{ "POST" }},
                path = "/timeout/<route_token>",
                timeout_ms = 1,
                response_mode = "handler",
            }}, function(_request)
                local deadline = os.clock() + 0.15
                while os.clock() < deadline do end
                return {{ status = 200, body = "too late" }}
            end)

            if ack.url and fail.url and timeout.url then
                fs.write(url_path, table.concat({{ ack.url, fail.url, timeout.url }}, "\n"))
            end
            return {{}}
            "#,
            url_path = serde_json::to_string(&url_path.to_string_lossy()).unwrap(),
            ack_path = serde_json::to_string(&ack_path.to_string_lossy()).unwrap(),
        ),
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-webhook-modes-plugin", {{ source = "device" }})
            assert(ok, tostring(err))
            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut urls = String::new();
    while Instant::now() < deadline {
        if let Ok(content) = fs::read_to_string(&url_path) {
            urls = content;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let urls: Vec<&str> = urls.lines().collect();
    assert_eq!(urls.len(), 3, "{urls:?}");

    let client = reqwest::blocking::Client::new();
    let ack = client.post(urls[0]).body("ack-body").send().unwrap();
    assert_eq!(ack.status().as_u16(), 202);
    assert_eq!(ack.text().unwrap(), "accepted");

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut ack_content = String::new();
    while Instant::now() < deadline {
        if let Ok(content) = fs::read_to_string(&ack_path) {
            ack_content = content;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(ack_content, "demo.ack:ack-body");

    let failure = client.post(urls[1]).body("fail").send().unwrap();
    assert_eq!(failure.status().as_u16(), 500);
    assert_eq!(failure.text().unwrap(), "webhook handler failed");

    let timeout = client.post(urls[2]).body("slow").send().unwrap();
    assert_eq!(timeout.status().as_u16(), 504);
    assert_eq!(timeout.text().unwrap(), "webhook handler timeout");
}

#[test]
fn plugin_owned_mcp_handlers_run_in_plugin_worker_vm() {
    // SAFETY: This integration filter runs this test in isolation and the
    // worker VM must resolve the repository Lua modules instead of user config.
    unsafe {
        std::env::set_var(
            "BOTSTER_LUA_PATH",
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("lua"),
        )
    };

    let tmp = TempDir::new().unwrap();
    let plugin_dir = tmp.path().join("worker-mcp-plugin");
    fs::create_dir_all(&plugin_dir).unwrap();
    let init_path = plugin_dir.join("init.lua");
    fs::write(
        &init_path,
        r#"
        local mcp = require("lib.mcp")

        mcp.tool("demo_worker_tool", {
            description = "Worker tool",
            input_schema = { type = "object", properties = {} },
            timeout_ms = 2000,
        }, function(params, ctx)
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                return "tool:" .. key .. ":" .. tostring(params.value) .. ":" .. tostring(ctx.session_uuid)
            end
            error("hub tool closure ran")
        end)

        mcp.prompt("demo-worker-prompt", {
            description = "Worker prompt",
            arguments = {},
            timeout_ms = 2000,
        }, function(args)
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                return "prompt:" .. key .. ":" .. tostring(args.value)
            end
            error("hub prompt closure ran")
        end)

        mcp.resource("botster://demo/{id}", {
            name = "Worker Resource",
            description = "Worker resource",
            mimeType = "text/plain",
            timeout_ms = 2000,
        }, function(params, ctx)
            local key = rawget(_G, "_plugin_worker_key")
            if key then
                return "resource:" .. key .. ":" .. tostring(params.id) .. ":" .. tostring(ctx.session_uuid)
            end
            error("hub resource closure ran")
        end)

        return {}
        "#,
    )
    .unwrap();

    let runtime = LuaRuntime::new().unwrap();
    runtime
        .lua()
        .load(format!(
            r#"
            local loader = require("hub.loader")
            local ok, err = loader.load_plugin({init_path}, "worker-mcp-plugin", {{ source = "device" }})
            assert(ok, tostring(err))

            local mcp = require("lib.mcp")
            local content, tool_err = mcp.call_tool("demo_worker_tool", {{ value = "ok" }}, {{ session_uuid = "sess-mcp" }})
            assert(tool_err == nil, tostring(tool_err))
            assert(content[1].text == "tool:worker-mcp-plugin:ok:sess-mcp", tostring(content[1].text))

            local prompt, prompt_err = mcp.get_prompt("demo-worker-prompt", {{ value = "ok" }})
            assert(prompt_err == nil, tostring(prompt_err))
            assert(prompt.messages[1].content.text == "prompt:worker-mcp-plugin:ok", tostring(prompt.messages[1].content.text))

            local contents, resource_err = mcp.read_resource("botster://demo/42", {{ session_uuid = "sess-mcp" }})
            assert(resource_err == nil, tostring(resource_err))
            assert(contents[1].text == "resource:worker-mcp-plugin:42:sess-mcp", tostring(contents[1].text))

            return true
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval::<bool>()
        .unwrap();
}
