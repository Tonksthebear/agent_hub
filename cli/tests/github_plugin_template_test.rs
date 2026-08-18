//! Regression tests for the GitHub plugin catalog template shape.

use mlua::{Lua, LuaSerdeExt, Value};
use serde_json::{json, Value as JsonValue};

fn create_lua_vm() -> Lua {
    let lua = Lua::new();

    botster::lua::primitives::fs::register(&lua).expect("fs register");
    botster::lua::primitives::json::register(&lua).expect("json register");
    botster::lua::primitives::log::register(&lua).expect("log register");

    let lua_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("lua")
        .to_str()
        .unwrap()
        .to_string();
    lua.load(format!(
        r#"package.path = "{lua_dir}/?.lua;{lua_dir}/?/init.lua;" .. package.path"#
    ))
    .exec()
    .expect("set package.path");

    lua
}

#[test]
fn catalog_plugin_github_template_catalog_entry_is_a_multi_file_plugin() {
    let catalog_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates");
    let catalog_root = catalog_root.to_str().unwrap();

    let lua = create_lua_vm();

    let files: Vec<String> = lua
        .load(format!(
            r#"
            local catalog = require("lib.template_catalog")
            local templates = catalog.list({{ source_root = "{catalog_root}" }})
            local out = {{}}
            for _, template in ipairs(templates) do
              if template.dest:match("^plugins/github/") then
                out[#out + 1] = template.dest
              end
            end
            table.sort(out)
            return out
            "#
        ))
        .eval()
        .expect("template catalog should load GitHub plugin template files");

    assert_eq!(
        files,
        vec![
            "plugins/github/event_routing.lua",
            "plugins/github/init.lua",
            "plugins/github/mcp_proxy.lua",
            "plugins/github/notifications.lua",
        ]
    );
}

#[test]
fn catalog_plugin_github_template_starts_mcp_without_repo_detection() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github");
    let init_path = plugin_root.join("init.lua");

    let lua = create_lua_vm();
    let result_lua: Value = lua
        .load(format!(
            r#"
            _G.__github_test = {{ mcp_started = false, notifications_registered = false, routed_repo = nil }}
            package.preload["mcp_proxy"] = function()
              return {{
                start = function() _G.__github_test.mcp_started = true end,
                stop = function() end,
              }}
            end
            package.preload["notifications"] = function()
              return {{
                register = function() _G.__github_test.notifications_registered = true end,
              }}
            end
            package.preload["event_routing"] = function()
              return {{
                start = function(repo) _G.__github_test.routed_repo = repo end,
                stop = function() end,
              }}
            end
            hub = {{ detect_repo = function() return nil end }}
            log = {{ info = function(_) end }}

            local chunk = assert(loadfile({init_path}))
            local plugin = chunk()
            return {{
              mcp_started = _G.__github_test.mcp_started,
              notifications_registered = _G.__github_test.notifications_registered,
              routed_repo_missing = _G.__github_test.routed_repo == nil,
              has_reload_hook = type(plugin._before_reload) == "function",
            }}
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .expect("GitHub plugin should load without detected repo");
    let result: JsonValue = lua
        .from_value(result_lua)
        .expect("GitHub plugin result should convert to JSON");

    assert_eq!(
        result,
        json!({
            "mcp_started": true,
            "notifications_registered": true,
            "routed_repo_missing": true,
            "has_reload_hook": true,
        })
    );
}

#[test]
fn catalog_plugin_github_template_starts_event_routing_when_repo_is_detected() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github");
    let init_path = plugin_root.join("init.lua");

    let lua = create_lua_vm();
    let routed_repos: Vec<String> = lua
        .load(format!(
            r#"
            _G.__github_test = {{ routed_repos = nil }}
            package.preload["mcp_proxy"] = function()
              return {{
                start = function() end,
                stop = function() end,
              }}
            end
            package.preload["notifications"] = function()
              return {{ register = function() end }}
            end
            package.preload["event_routing"] = function()
              return {{
                start = function(repos) _G.__github_test.routed_repos = repos end,
                stop = function() end,
              }}
            end
            hub = {{ detect_repo = function() return "owner/repo" end }}
            log = {{ info = function(_) end }}

            local chunk = assert(loadfile({init_path}))
            chunk()
            return _G.__github_test.routed_repos
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .expect("GitHub plugin should route events for detected repo");

    assert_eq!(routed_repos, vec!["owner/repo"]);
}

#[test]
fn catalog_plugin_github_template_starts_event_routing_for_spawn_target_repos() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github");
    let init_path = plugin_root.join("init.lua");

    let lua = create_lua_vm();
    let routed_repos: Vec<String> = lua
        .load(format!(
            r#"
            _G.__github_test = {{ routed_repos = nil }}
            package.preload["mcp_proxy"] = function()
              return {{
                start = function() end,
                stop = function() end,
              }}
            end
            package.preload["notifications"] = function()
              return {{ register = function() end }}
            end
            package.preload["event_routing"] = function()
              return {{
                start = function(repos) _G.__github_test.routed_repos = repos end,
                stop = function() end,
              }}
            end
            hub = {{
              detect_repo = function(path)
                if path == "/repos/two" then return "owner/two" end
                return nil
              end
            }}
            spawn_targets = {{
              list = function()
                return {{
                  {{ path = "/repos/one", enabled = true }},
                  {{ path = "/repos/two", enabled = true }},
                  {{ path = "/repos/disabled", enabled = false }},
                }}
              end,
              inspect = function(path)
                if path == "/repos/one" then
                  return {{ repo_name = "owner/one" }}
                end
                return {{}}
              end,
            }}
            log = {{ info = function(_) end }}

            local chunk = assert(loadfile({init_path}))
            chunk()
            return _G.__github_test.routed_repos
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .expect("GitHub plugin should route events for spawn target repos");

    assert_eq!(routed_repos, vec!["owner/one", "owner/two"]);
}

#[test]
fn catalog_plugin_github_template_normalizes_spawn_target_repos() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github");
    let init_path = plugin_root.join("init.lua");

    let lua = create_lua_vm();
    let routed_repos: Vec<String> = lua
        .load(format!(
            r#"
            _G.__github_test = {{ routed_repos = nil }}
            package.preload["mcp_proxy"] = function()
              return {{
                start = function() end,
                stop = function() end,
              }}
            end
            package.preload["notifications"] = function()
              return {{ register = function() end }}
            end
            package.preload["event_routing"] = function()
              return {{
                start = function(repos) _G.__github_test.routed_repos = repos end,
                stop = function() end,
              }}
            end
            hub = {{
              detect_repo = function()
                return "https://github.com/owner/current.git"
              end
            }}
            spawn_targets = {{
              list = function()
                return {{
                  {{ repo = "git@github.com:owner/current.git", enabled = true }},
                  {{ repo = "https://github.com/owner/second.git", enabled = true }},
                  {{ repo = "not-a-github-repo", enabled = true }},
                }}
              end,
            }}
            log = {{ info = function(_) end }}

            local chunk = assert(loadfile({init_path}))
            chunk()
            return _G.__github_test.routed_repos
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .expect("GitHub plugin should normalize routed repos");

    assert_eq!(routed_repos, vec!["owner/current", "owner/second"]);
}

#[test]
fn catalog_plugin_github_template_refreshes_routing_for_new_spawn_target_repo() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github");
    let init_path = plugin_root.join("init.lua");

    let lua = create_lua_vm();
    let result: JsonValue = lua
        .load(format!(
            r#"
            _G.__github_test = {{ routed_repos = {{}}, timer_cb = nil, timer_cancelled = nil }}
            local include_second = false
            package.preload["mcp_proxy"] = function()
              return {{
                start = function() end,
                stop = function() end,
              }}
            end
            package.preload["notifications"] = function()
              return {{ register = function() end }}
            end
            package.preload["event_routing"] = function()
              return {{
                start = function(repos)
                  _G.__github_test.routed_repos[#_G.__github_test.routed_repos + 1] = repos
                end,
                stop = function() end,
              }}
            end
            hub = {{ detect_repo = function() return "owner/current" end }}
            spawn_targets = {{
              list = function()
                local targets = {{
                  {{ repo = "owner/current", enabled = true }},
                }}
                if include_second then
                  targets[#targets + 1] = {{ repo = "owner/second", enabled = true }}
                end
                return targets
              end,
            }}
            timer = {{
              every = function(_, cb)
                _G.__github_test.timer_cb = cb
                return "timer-1"
              end,
              cancel = function(id)
                _G.__github_test.timer_cancelled = id
              end,
            }}
            log = {{ info = function(_) end, warn = function(_) end }}

            local chunk = assert(loadfile({init_path}))
            local plugin = chunk()
            include_second = true
            _G.__github_test.timer_cb()
            plugin._before_reload()
            return {{
              routed_repos = _G.__github_test.routed_repos,
              timer_cancelled = _G.__github_test.timer_cancelled,
            }}
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .and_then(|value: Value| lua.from_value(value))
        .expect("GitHub plugin should refresh route subscriptions for newly admitted repos");

    assert_eq!(result["routed_repos"][0], json!(["owner/current"]));
    assert_eq!(
        result["routed_repos"][1],
        json!(["owner/current", "owner/second"])
    );
    assert_eq!(result["timer_cancelled"], json!("timer-1"));
}

#[test]
fn catalog_plugin_github_template_caches_repo_inspection_until_targets_change() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github");
    let init_path = plugin_root.join("init.lua");

    let lua = create_lua_vm();
    let result: JsonValue = lua
        .load(format!(
            r#"
            _G.__github_test = {{ timer_cb = nil, inspect_count = 0, detect_count = 0 }}
            local target_path = "/repos/one"
            package.preload["mcp_proxy"] = function()
              return {{ start = function() end, stop = function() end }}
            end
            package.preload["notifications"] = function()
              return {{ register = function() end }}
            end
            package.preload["event_routing"] = function()
              return {{ start = function() end, stop = function() end }}
            end
            hub = {{
              detect_repo = function(path)
                _G.__github_test.detect_count = _G.__github_test.detect_count + 1
                if path then return path == "/repos/one" and "owner/one" or "owner/two" end
                return "owner/current"
              end,
            }}
            spawn_targets = {{
              list = function()
                return {{ {{ id = "target", path = target_path, enabled = true }} }}
              end,
              inspect = function(path)
                _G.__github_test.inspect_count = _G.__github_test.inspect_count + 1
                return {{ repo_name = path == "/repos/one" and "owner/one" or "owner/two" }}
              end,
            }}
            timer = {{
              every = function(_, cb) _G.__github_test.timer_cb = cb return "timer-1" end,
              cancel = function() end,
            }}
            log = {{ info = function() end, warn = function() end }}

            assert(loadfile({init_path}))()
            _G.__github_test.timer_cb()
            _G.__github_test.timer_cb()
            local unchanged_inspect_count = _G.__github_test.inspect_count
            local unchanged_detect_count = _G.__github_test.detect_count
            target_path = "/repos/two"
            _G.__github_test.timer_cb()
            return {{
              unchanged_inspect_count = unchanged_inspect_count,
              unchanged_detect_count = unchanged_detect_count,
              changed_inspect_count = _G.__github_test.inspect_count,
              changed_detect_count = _G.__github_test.detect_count,
            }}
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .and_then(|value: Value| lua.from_value(value))
        .expect("GitHub plugin should cache unchanged spawn target inspection");

    assert_eq!(result["unchanged_inspect_count"], json!(1));
    assert_eq!(result["unchanged_detect_count"], json!(2));
    assert_eq!(result["changed_inspect_count"], json!(2));
    assert_eq!(result["changed_detect_count"], json!(3));
}

#[test]
fn catalog_plugin_github_mcp_proxy_normalizes_create_pull_request_draft_false() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github");
    let proxy_path = plugin_root.join("mcp_proxy.lua");

    let lua = create_lua_vm();
    let result_lua: Value = lua
        .load(format!(
            r#"
            package.preload["hub.state"] = function()
              return {{ get = function() return {{}} end }}
            end
            secrets = {{
              get = function(scope, key)
                if scope == "github" and key == "mcp_url" then return "https://example.test/mcp" end
                if scope == "github" and key == "mcp_token" then return "token" end
                return nil
              end,
              set = function() end,
            }}
            hub = {{ api_token = function() return nil end }}
            config = {{ server_url = function() return "https://trybotster.test" end }}
            http = {{ post = function() return nil, "unused" end }}
            log = {{
              debug = function() end,
              info = function() end,
              warn = function() end,
            }}
            timer = {{ every = function() return "timer" end, cancel = function() end }}
            mcp = {{
              proxy = function(url, opts)
                _G.__github_mcp_proxy = {{ url = url, opts = opts }}
              end,
            }}

            local proxy = assert(loadfile({proxy_path}))()
            proxy.start()
            local transform = _G.__github_mcp_proxy.opts.transform_arguments

            local explicit_false = transform("github_create_pull_request", {{
              title = "PR",
              draft = false,
            }})
            local explicit_true = transform("github_create_pull_request", {{
              title = "PR",
              draft = true,
            }})
            local omitted = transform("github_create_pull_request", {{
              title = "PR",
            }})
            local other_tool = transform("github_create_issue", {{
              title = "Issue",
              draft = false,
            }})

            return {{
              proxy_url = _G.__github_mcp_proxy.url,
              false_draft_removed = explicit_false.draft == nil,
              false_title_preserved = explicit_false.title == "PR",
              true_draft_preserved = explicit_true.draft == true,
              omitted_still_omitted = omitted.draft == nil,
              other_tool_unchanged = other_tool.draft == false,
            }}
            "#,
            proxy_path = serde_json::to_string(&proxy_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .expect("GitHub MCP proxy should normalize draft false at plugin boundary");
    let result: JsonValue = lua
        .from_value(result_lua)
        .expect("GitHub MCP proxy result should convert to JSON");

    assert_eq!(
        result,
        json!({
            "proxy_url": "https://example.test/mcp",
            "false_draft_removed": true,
            "false_title_preserved": true,
            "true_draft_preserved": true,
            "omitted_still_omitted": true,
            "other_tool_unchanged": true,
        })
    );
}

#[test]
fn catalog_plugin_project_pipelines_template_catalog_entry_is_a_multi_file_plugin() {
    let catalog_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates");
    let catalog_root = catalog_root.to_str().unwrap();

    let lua = create_lua_vm();

    let files: Vec<String> = lua
        .load(format!(
            r#"
            local catalog = require("lib.template_catalog")
            local templates = catalog.list({{ source_root = "{catalog_root}" }})
            local out = {{}}
            for _, template in ipairs(templates) do
              if template.dest:match("^plugins/project%-pipelines/") then
                out[#out + 1] = template.dest
              end
            end
            table.sort(out)
            return out
            "#
        ))
        .eval()
        .expect("template catalog should load Project Pipelines plugin template files");

    assert_eq!(
        files,
        vec![
            "plugins/project-pipelines/README.md",
            "plugins/project-pipelines/init.lua",
            "plugins/project-pipelines/project_pipelines/db.lua",
            "plugins/project-pipelines/project_pipelines/engine.lua",
            "plugins/project-pipelines/project_pipelines/entities.lua",
            "plugins/project-pipelines/project_pipelines/entity_contract.lua",
            "plugins/project-pipelines/project_pipelines/github_integration.lua",
            "plugins/project-pipelines/project_pipelines/mcp.lua",
            "plugins/project-pipelines/project_pipelines/notification_policy.lua",
            "plugins/project-pipelines/project_pipelines/repo.lua",
            "plugins/project-pipelines/project_pipelines/stack_delivery.lua",
            "plugins/project-pipelines/project_pipelines/util.lua",
            "plugins/project-pipelines/project_pipelines/web/actions.lua",
            "plugins/project-pipelines/project_pipelines/web/screens/home.lua",
            "plugins/project-pipelines/project_pipelines/web/screens/new.lua",
            "plugins/project-pipelines/project_pipelines/web/screens/pipelines.lua",
            "plugins/project-pipelines/project_pipelines/web/screens/project.lua",
            "plugins/project-pipelines/project_pipelines/web/screens/run.lua",
            "plugins/project-pipelines/project_pipelines/web/screens/ticket.lua",
            "plugins/project-pipelines/project_pipelines/web/surface.lua",
            "plugins/project-pipelines/project_pipelines/web/ui.lua",
        ]
    );
}

#[test]
fn catalog_plugin_botster_bugs_template_is_stateless_live_ingress() {
    let catalog_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates");
    let plugin_root = catalog_root.join("plugins/botster-bugs");
    let init_path = plugin_root.join("init.lua");
    let catalog_root = catalog_root.to_str().unwrap();

    let lua = create_lua_vm();

    let result_lua: Value = lua
        .load(format!(
            r#"
            local catalog = require("lib.template_catalog")
            local templates = catalog.list({{ source_root = "{catalog_root}" }})
            local files = {{}}
            for _, template in ipairs(templates) do
              if template.dest:match("^plugins/botster%-bugs/") then
                files[#files + 1] = template.dest
              end
            end
            table.sort(files)

            local calls = {{ tools = {{}}, created = nil, posted = nil, notified = nil, db_called = false }}
            mcp = {{
              tool = function(name, spec, handler)
                calls.tools[name] = {{ spec = spec, handler = handler }}
              end,
            }}
            plugin = {{
              db = function()
                calls.db_called = true
                error("botster-bugs must not use plugin.db")
              end,
            }}
            log = {{ info = function(_) end, warn = function(_) end }}
            spawn_targets = {{
              get = function(id)
                if id == "tgt_botster" then return {{ id = id, name = "trybotster", path = "/repo/trybotster" }} end
              end,
              list = function()
                return {{ {{ id = "tgt_botster", name = "trybotster", path = "/repo/trybotster" }} }}
              end,
              inspect = function(path)
                if path == "/repo/trybotster" then return {{ repo_name = "Tonksthebear/trybotster" }} end
                return {{}}
              end,
            }}

            package.preload["lib.agent"] = function()
              return {{
                get = function(session_uuid)
                  if session_uuid == "caller" then
                    return {{
                      info = function()
                        return {{
                          session_uuid = "caller",
                          label = "reporter",
                          target_id = "tgt_botster",
                          workspace_name = "Dev",
                          worktree_path = "/repo/trybotster",
                        }}
                      end,
                    }}
                  end
                end,
              }}
            end
            package.preload["lib.hub"] = function()
              local hub = {{
                owned = {{}},
                list_owned_sessions = function(self, owner)
                  calls.owner = owner
                  return self.owned
                end,
                create_agent = function(self, opts)
                  calls.created = opts
                  return {{ request_id = opts.request_id, status = "pending" }}
                end,
                post = function(self, session_uuid, opts)
                  calls.posted = {{ session_uuid = session_uuid, opts = opts }}
                  return {{ msg_id = "msg-1", status = "delivered" }}
                end,
                notify = function(self, session_uuid, opts)
                  calls.notified = {{ session_uuid = session_uuid, opts = opts }}
                  return {{ status = "delivered" }}
                end,
              }}
              return {{ get = function() return hub end, _hub = hub }}
            end

            local chunk = assert(loadfile({init_path}))
            chunk()
            local tool = assert(calls.tools.file_botster_bug, "file_botster_bug registered")
            local created_result = tool.handler({{
              title = "terminal pane freezes",
              description = "The terminal pane stops repainting after reconnect.",
              evidence = "observed in browser console",
            }}, {{ session_uuid = "caller", hub_id = "hub-1" }})

            local Hub = require("lib.hub")
            Hub._hub.owned = {{ {{
              session_uuid = "orchestrator-session",
              status = "running",
              metadata = {{ role = "orchestrator", target_id = "tgt_botster" }},
            }} }}
            local posted_result = tool.handler({{
              title = "status badge stale",
              description = "The badge stays active after the session closes.",
            }}, {{ session_uuid = "caller", hub_id = "hub-1" }})

            return {{
              files = files,
              db_called = calls.db_called,
              owner = calls.owner,
              created_result = created_result,
              created = calls.created,
              posted_result = posted_result,
              posted = calls.posted,
              notified = calls.notified,
            }}
            "#,
            init_path = serde_json::to_string(&init_path.to_string_lossy()).unwrap(),
        ))
        .eval()
        .expect("Botster Bugs plugin should load and route reports without durable storage");
    let result: JsonValue = lua
        .from_value(result_lua)
        .expect("Botster Bugs plugin result should convert to JSON");

    assert_eq!(
        result["files"],
        json!([
            "plugins/botster-bugs/README.md",
            "plugins/botster-bugs/init.lua",
        ])
    );
    assert_eq!(result["db_called"], json!(false));
    assert_eq!(result["owner"], json!("botster-bugs"));
    assert_eq!(
        result["created_result"]["routed"],
        json!("new_orchestrator")
    );
    assert_eq!(result["created"]["agent_name"], json!("codex"));
    assert_eq!(result["created"]["workspace_name"], json!("Botster Bugs"));
    assert_eq!(result["created"]["target_id"], json!("tgt_botster"));
    assert_eq!(
        result["created"]["metadata"],
        json!({
            "owner_plugin": "botster-bugs",
            "visibility": "workspace",
            "surface": "botster-bugs",
            "role": "orchestrator",
        })
    );
    assert!(result["created"]["prompt"]
        .as_str()
        .is_some_and(|prompt| prompt.contains("project_pipelines_create_ticket")));
    assert_eq!(
        result["posted_result"]["routed"],
        json!("existing_orchestrator")
    );
    assert_eq!(
        result["posted"]["session_uuid"],
        json!("orchestrator-session")
    );
    assert_eq!(
        result["posted"]["opts"]["payload"]["kind"],
        json!("botster_bug_report")
    );
    assert_eq!(
        result["notified"]["session_uuid"],
        json!("orchestrator-session")
    );
}

#[test]
fn catalog_plugin_project_pipelines_dynamic_state_uses_plugin_entities_not_forced_tree_refreshes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/project-pipelines");
    let engine = std::fs::read_to_string(root.join("project_pipelines/engine.lua"))
        .expect("read project pipelines engine");
    let entities = std::fs::read_to_string(root.join("project_pipelines/entities.lua"))
        .expect("read project pipelines entities");
    let entity_contract =
        std::fs::read_to_string(root.join("project_pipelines/entity_contract.lua"))
            .expect("read project pipelines entity contract");
    let readme =
        std::fs::read_to_string(root.join("README.md")).expect("read project pipelines readme");
    let normalized_readme = readme.split_whitespace().collect::<Vec<_>>().join(" ");

    assert!(
        !engine.contains("broadcast_ui_tree_snapshots")
            && !engine.contains("send_ui_tree_snapshots"),
        "Project Pipelines mutators must not force data-only ui_tree_snapshot refreshes"
    );
    assert!(
        normalized_readme.contains(
            "If a scaffold publishes plugin entities, include or document an `entity_contract.lua` module"
        ) && normalized_readme.contains("docs/plugin-entities.md#shipping-a-model")
            && normalized_readme.contains("Use singular, plugin-owned entity names")
            && normalized_readme.contains("UI screens must not perform render-time `repo.*` reads")
            && normalized_readme.contains(
                "Modal field values that are not submitted yet are browser-local presentation state",
            )
            && normalized_readme
                .contains("Project Pipelines migrations are cold-turkey at the section boundary"),
        "README should point entity-backed templates at the canonical shipping convention"
    );
    for (entity_type, lua_key) in [
        ("project-pipelines.ticket", "ticket"),
        ("project-pipelines.run", "run"),
        ("project-pipelines.run_step", "run_step"),
        ("project-pipelines.gate_result", "gate_result"),
        ("project-pipelines.review", "review"),
        ("project-pipelines.finding", "finding"),
        ("project-pipelines.artifact", "artifact"),
        ("project-pipelines.question", "question"),
        ("project-pipelines.event", "event"),
        ("project-pipelines.pipeline_step", "pipeline_step"),
        ("project-pipelines.pipeline_gate", "pipeline_gate"),
    ] {
        assert!(
            entity_contract.contains(&format!("{lua_key} = M.owner ..")),
            "entity_contract.lua should publish {entity_type}"
        );
        assert!(
            entities.contains("M.types = contract.types"),
            "entities.lua should consume entity type names from the contract"
        );
        assert!(
            readme.contains(entity_type),
            "README should document {entity_type}"
        );
    }
}

#[test]
fn catalog_plugin_project_pipelines_automates_merge_from_pipeline_policy() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/project-pipelines");
    let db = std::fs::read_to_string(root.join("project_pipelines/db.lua"))
        .expect("read project pipelines db");
    let repo = std::fs::read_to_string(root.join("project_pipelines/repo.lua"))
        .expect("read project pipelines repo");
    let engine = std::fs::read_to_string(root.join("project_pipelines/engine.lua"))
        .expect("read project pipelines engine");
    let mcp = std::fs::read_to_string(root.join("project_pipelines/mcp.lua")).expect("read mcp");
    let pipeline_screen =
        std::fs::read_to_string(root.join("project_pipelines/web/screens/pipelines.lua"))
            .expect("read pipeline screen");
    let ticket_screen =
        std::fs::read_to_string(root.join("project_pipelines/web/screens/ticket.lua"))
            .expect("read ticket screen");

    assert!(
        db.contains("merge_policy = { \"text\" }"),
        "pipeline schema should persist merge_policy"
    );
    assert!(
        repo.contains("merge_policy = true") && repo.contains("merge_policy must be direct or pr"),
        "repo should allow and validate direct/pr merge policy"
    );
    assert!(
        mcp.contains("merge_policy = { type = \"string\", enum = { \"direct\", \"pr\" } }"),
        "MCP CRUD should expose direct/pr merge policy"
    );
    assert!(
        engine.contains("return M.request_merge({ ticket_id = run.ticket_id }, {})"),
        "completed runs should automatically request the merge agent"
    );
    assert!(
        engine.contains("This pipeline requires a direct merge to main")
            && engine.contains("This pipeline requires a PR"),
        "merge agent prompt should branch on pipeline policy"
    );
    assert!(
        engine.contains("Prefer the smallest surgical change")
            && engine.contains("production path proof")
            && engine.contains("Treat stub wiring as incomplete")
            && engine.contains("fixed sleeps on hot paths"),
        "pipeline prompts should enforce surgical scope, production-path proof, stub-wiring rejection, and hot-path sleep scrutiny"
    );
    assert!(
        mcp.contains("State assumptions explicitly")
            && mcp.contains("actual production/user/runtime path"),
        "MCP role prompts should preserve discipline and production-path expectations"
    );
    assert!(
        engine.contains("local function ticket_workspace_name")
            && engine.contains("Pipeline - ")
            && engine.contains("workspace_name = params.workspace_name or ticket_workspace_name(ticket, params.ticket_id)")
            && engine.contains("workspace_name = run.workspace_name or ticket_workspace_name(ticket, run.id)"),
        "pipeline agents should default into a ticket-named workspace while preserving run workspace reuse"
    );
    assert!(
        pipeline_screen.contains("Merge policy")
            && pipeline_screen.contains("Merge directly to main")
            && pipeline_screen.contains("Open PR with Botster MCP"),
        "pipeline editor should let users choose merge policy"
    );
    assert!(
        !ticket_screen.contains("Approve merge"),
        "ticket UI should not require a separate merge approval click"
    );
}

#[test]
fn catalog_plugin_project_pipelines_detail_bind_lists_use_entity_store_paths() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/project-pipelines/project_pipelines/web/screens");
    let project = std::fs::read_to_string(root.join("project.lua")).expect("read project screen");
    let home = std::fs::read_to_string(root.join("home.lua")).expect("read home screen");
    let surface = std::fs::read_to_string(root.parent().unwrap().join("surface.lua"))
        .expect("read project pipelines surface");

    for (label, source, screen) in [
        (
            "home projects",
            r#"source = "/project-pipelines.project""#,
            &home,
        ),
        (
            "home tickets",
            r#"source = "/project-pipelines.ticket""#,
            &home,
        ),
        (
            "home pipelines",
            r#"source = "/project-pipelines.pipeline""#,
            &home,
        ),
        (
            "project timeline tickets",
            r#"source = "/project-pipelines.ticket""#,
            &project,
        ),
    ] {
        assert!(
            screen.contains(source),
            "{label} should bind dynamic rows from plugin entity stores"
        );
    }
    assert!(
        project.contains(r#"where = { project_id = project_id }"#),
        "project ticket timeline should filter the entity stream with bind_list where"
    );
    assert!(
        !project.contains(r#""-timeline-ticket-" .. ticket.id"#),
        "bind_list item templates must not capture repo-local ticket variables before entity bindings resolve"
    );
    assert!(
        !project.contains("Chronological Timeline"),
        "project view should show dependency-ordered tickets, not a separate chronological timeline"
    );
    assert!(
        surface.contains(r#"source = "/project-pipelines.project""#)
            && surface.contains(r#"source = "/project-pipelines.question""#)
            && surface.contains(r#"source = "/project-pipelines.ticket""#),
        "sidebar volatile rows should be backed by plugin entity stores"
    );
    assert!(
        !surface.contains("repo.open_questions_with_tickets()")
            && !surface.contains("repo.standalone_tickets()"),
        "sidebar should not pre-render stale project/question/ticket snapshots from repo queries"
    );
}

#[test]
fn catalog_plugin_project_pipelines_entity_publish_snapshots_and_deltas_use_plugin_entity_frames() {
    let plugin_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/project-pipelines");
    let plugin_root = plugin_root.to_str().unwrap();

    let lua = create_lua_vm();
    let script = r#"
            package.path = "__PLUGIN_ROOT__/?.lua;__PLUGIN_ROOT__/?/init.lua;" .. package.path

            local registrations = {}
            local frames = {}

            package.loaded["lib.entity_broadcast"] = {
              register = function(entity_type, opts)
                registrations[#registrations + 1] = {
                  entity_type = entity_type,
                  id_field = opts.id_field,
                  owner_plugin = opts.owner_plugin,
                  default = opts.default,
                }
              end,
            }

            package.loaded["lib.hub"] = {
              get = function()
                return {
                  entity_snapshot = function(_self, entity_type, items, opts)
                    frames[#frames + 1] = { type = "entity_snapshot", entity_type = entity_type, items = items, owner_plugin = opts.owner_plugin }
                  end,
                  entity_upsert = function(_self, entity_type, entity, opts)
                    frames[#frames + 1] = { type = "entity_upsert", entity_type = entity_type, entity = entity, owner_plugin = opts.owner_plugin }
                  end,
                  entity_remove = function(_self, entity_type, id, opts)
                    frames[#frames + 1] = { type = "entity_remove", entity_type = entity_type, id = id, owner_plugin = opts.owner_plugin }
                  end,
                }
              end,
            }

            package.loaded["project_pipelines.web.ui"] = {
              target_label = function(target_id) return target_id or "No target" end,
              status_tone = function(status) return status == "done" and "success" or "muted" end,
              ticket_notification_count = function() return 0 end,
              visible_tickets = function(repo) return repo.standalone_tickets() end,
            }

            local rows_by_name = {
              tickets = {
                { id = "ticket-1", title = "Standalone Ticket", status = "open", target_id = "target-1", created_at = 1, updated_at = 2 },
                { id = "ticket-2", title = "Project Ticket", status = "open", project_id = "project-1", target_id = "target-1", created_at = 1, updated_at = 2 },
              },
              runs = { { id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipe-1", status = "active", current_step_id = "step-1", created_at = 1, updated_at = 2 } },
              pipeline_steps = { { id = "step-1", pipeline_id = "pipe-1", position = 1, kind = "agent", name = "Implement", agent_name = "codex", prompt = "Do it", created_at = 1, updated_at = 2 } },
              pipeline_gates = { { id = "gate-1", step_id = "step-1", kind = "attestation", prompt = "Evidence", required_fields = "[\"summary\"]", created_at = 1, updated_at = 2 } },
              gate_results = { { id = "gate-result-1", run_id = "run-1", run_step_id = "run-step-1", step_id = "step-1", gate_id = "gate-1", status = "passed", evidence = "{\"summary\":\"ok\"}", created_at = 3 } },
              run_steps = { { id = "run-step-1", run_id = "run-1", step_id = "step-1", sequence = 1, status = "active", created_at = 1, updated_at = 2 } },
              artifacts = { { id = "artifact-1", run_id = "run-1", kind = "note", payload = "{\"a\":1}", created_at = 3 } },
              events = { { id = "event-1", run_id = "run-1", kind = "gate.submitted", payload = "{\"gate_id\":\"gate-1\"}", created_at = 4 } },
            }

            local db = {}
            function db:eval(sql, params)
              if sql:find("FROM tickets") then return rows_by_name.tickets end
              if sql:find("FROM runs") then return rows_by_name.runs end
              if sql:find("FROM pipeline_steps") then return rows_by_name.pipeline_steps end
              if sql:find("FROM pipeline_gates") then return rows_by_name.pipeline_gates end
              if sql:find("FROM gate_results") then return rows_by_name.gate_results end
              if sql:find("FROM run_steps") then return rows_by_name.run_steps end
              if sql:find("FROM artifacts") then return rows_by_name.artifacts end
              if sql:find("FROM events") then return rows_by_name.events end
              return {}
            end
            package.loaded["project_pipelines.db"] = db

            package.loaded["project_pipelines.repo"] = {
              standalone_tickets = function() return rows_by_name.tickets end,
              ticket_runs = function() return rows_by_name.runs end,
              get_step = function() return rows_by_name.pipeline_steps[1] end,
            }

            local entities = require("project_pipelines.entities")
            entities.register()
            entities.publish_snapshots()
            entities.upsert(entities.types.run_step, rows_by_name.run_steps[1])
            entities.upsert(entities.types.run_step, { id = 123, run_id = "run-1" })
            entities.remove(entities.types.run_step, "run-step-1")

            return { registrations = registrations, frames = frames }
            "#
    .replace("__PLUGIN_ROOT__", plugin_root);
    let value: Value = lua
        .load(script)
        .eval()
        .expect("project pipelines entities behavior script");
    let result: JsonValue = lua
        .from_value(value)
        .expect("project pipelines entities result json");

    assert_eq!(result["registrations"].as_array().unwrap().len(), 19);
    assert!(result["registrations"]
        .as_array()
        .unwrap()
        .iter()
        .all(|registration| {
            registration["id_field"] == json!("id")
                && registration["owner_plugin"] == json!("project-pipelines")
        }));

    let frames = result["frames"].as_array().unwrap();
    assert_eq!(
        frames.len(),
        6,
        "4 default snapshots + upsert + remove; detail/history snapshots are targeted-only by default"
    );
    assert!(
        frames
            .iter()
            .all(|frame| frame["owner_plugin"] == json!("project-pipelines")),
        "every Project Pipelines entity frame must carry owner_plugin"
    );

    let snapshots: Vec<&JsonValue> = frames
        .iter()
        .filter(|frame| frame["type"] == json!("entity_snapshot"))
        .collect();
    assert_eq!(
        snapshots.len(),
        4,
        "publish_snapshots should publish only default working-set entity families"
    );
    assert!(snapshots.iter().all(|frame| {
        frame["entity_type"]
            .as_str()
            .is_some_and(|entity_type| entity_type.starts_with("project-pipelines."))
    }));

    let ticket_snapshot = snapshots
        .iter()
        .find(|frame| frame["entity_type"] == json!("project-pipelines.ticket"))
        .expect("ticket snapshot");
    assert_eq!(ticket_snapshot["items"].as_array().unwrap().len(), 2);
    assert_eq!(ticket_snapshot["items"][1]["id"], json!("ticket-2"));
    assert_eq!(
        ticket_snapshot["items"][1]["project_id"],
        json!("project-1")
    );

    for detail_type in [
        "project-pipelines.run",
        "project-pipelines.run_step",
        "project-pipelines.gate_result",
        "project-pipelines.review",
        "project-pipelines.finding",
        "project-pipelines.artifact",
        "project-pipelines.pr_link",
        "project-pipelines.event",
        "project-pipelines.project_target",
        "project-pipelines.ticket_dependency",
        "project-pipelines.pipeline_step",
        "project-pipelines.pipeline_gate",
        "project-pipelines.checklist",
        "project-pipelines.checklist_item",
    ] {
        assert!(
            snapshots
                .iter()
                .all(|frame| frame["entity_type"] != json!(detail_type)),
            "{detail_type} should be targeted/detail-only in default publishes"
        );
        assert!(
            result["registrations"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |registration| registration["entity_type"] == json!(detail_type)
                        && registration["default"] == json!(false)
                ),
            "{detail_type} should register default=false"
        );
    }

    assert_eq!(frames[4]["type"], json!("entity_upsert"));
    assert_eq!(
        frames[4]["entity_type"],
        json!("project-pipelines.run_step")
    );
    assert_eq!(frames[4]["entity"]["id"], json!("run-step-1"));
    assert_eq!(frames[5]["type"], json!("entity_remove"));
    assert_eq!(
        frames[5]["entity_type"],
        json!("project-pipelines.run_step")
    );
    assert_eq!(frames[5]["id"], json!("run-step-1"));
}

#[test]
fn catalog_plugin_github_event_routing_template_uses_hub_api_ingress() {
    let template = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("catalog/templates/plugins/github/event_routing.lua"),
    )
    .expect("read github event routing template");

    assert!(
        template.contains(r#"require("lib.hub")"#),
        "GitHub template should route application commands through the standard hub API"
    );
    assert!(
        template.contains("Hub.get():create_agent") && template.contains("Hub.get():delete_agent"),
        "GitHub template should dispatch canonical commands through Hub.get()"
    );
    assert!(
        !template.contains("InternalClient.dispatch"),
        "GitHub template should not bypass the worker-parent hub boundary"
    );
    assert!(
        !template.contains(r#"events.emit("command_message""#),
        "GitHub template must not use the legacy command_message bypass"
    );
}

#[test]
fn catalog_plugin_github_event_routing_template_shares_one_action_cable_connection() {
    let template_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github/event_routing.lua");
    let template_path = template_path.to_str().unwrap();

    let lua = create_lua_vm();

    let result: JsonValue = lua
        .load(format!(
            r#"
            package.loaded["lib.agent"] = {{ find_by_workspace = function() return {{}} end }}
            package.loaded["hub.state"] = {{ get = function() return {{}} end }}
            package.loaded["lib.hub"] = {{ get = function() return {{}} end }}

            local connects = 0
            local channels = {{}}
            action_cable = {{
              connect = function()
                connects = connects + 1
                return "conn-" .. tostring(connects)
              end,
              subscribe = function(conn, _, params, _)
                channels[#channels + 1] = {{ conn = conn, repo = params.repo }}
                return "chan-" .. tostring(#channels)
              end,
              close = function() end,
            }}

            local routing = dofile("{template_path}")
            routing.start({{ "owner/one", "owner/two", "owner/one" }})
            return {{ connects = connects, channels = channels }}
            "#
        ))
        .eval()
        .and_then(|value: Value| lua.from_value(value))
        .expect("GitHub event routing should share one ActionCable connection");

    assert_eq!(result["connects"], json!(1));
    assert_eq!(
        result["channels"],
        json!([
            { "conn": "conn-1", "repo": "owner/one" },
            { "conn": "conn-1", "repo": "owner/two" },
        ])
    );
}

#[test]
fn catalog_plugin_github_event_routing_template_notifies_matching_agent_before_create() {
    let template_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github/event_routing.lua");
    let template_path = template_path.to_str().unwrap();

    let lua = create_lua_vm();

    let result: String = lua
        .load(format!(
            r#"
            local notifications = {{}}
            local creates = 0
            local deletes = 0
            local acked = false
            local callback = nil

            package.loaded["lib.agent"] = {{
              find_by_workspace = function(name)
                if name == "owner/repo#42" then
                  return {{
                    {{
                      session_uuid = "sess-existing",
                    }},
                  }}
                end
                return {{}}
              end,
            }}
            package.loaded["hub.state"] = {{
              get = function() return {{}} end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  send_message = function(_, session_uuid, text)
                    assert(session_uuid == "sess-existing")
                    notifications[#notifications + 1] = text
                  end,
                  create_agent = function()
                    creates = creates + 1
                  end,
                  delete_agent = function()
                    deletes = deletes + 1
                  end,
                }}
              end,
            }}
            action_cable = {{
              connect = function() return "conn-1" end,
              subscribe = function(_, _, _, cb)
                callback = cb
                return "chan-1"
              end,
              perform = function(_, action, data)
                if action == "ack" and data.id == 7 then acked = true end
              end,
              close = function() end,
            }}

            local routing = dofile("{template_path}")
            routing.start("owner/repo")
            callback({{
              id = 7,
              event_type = "issue_comment",
              payload = {{
                issue_number = 42,
                prompt = "Please inspect this",
              }},
            }}, "chan-1")

            assert(#notifications == 1)
            assert(notifications[1]:match("Please inspect this"))
            assert(notifications[1]:match("👀"))
            assert(creates == 0)
            assert(deletes == 0)
            assert(acked == true)
            return "ok"
            "#
        ))
        .eval()
        .expect("GitHub template should notify matching agents instead of spawning");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_github_event_routing_emits_pr_review_submitted() {
    let template_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github/event_routing.lua");
    let template_path = template_path.to_str().unwrap();

    let lua = create_lua_vm();

    let result: JsonValue = lua
        .load(format!(
            r#"
            local emitted = {{}}
            local acked = false
            local callback = nil

            package.loaded["lib.agent"] = {{ find_by_workspace = function() return {{}} end }}
            package.loaded["hub.state"] = {{ get = function() return {{}} end }}
            package.loaded["lib.hub"] = {{ get = function() return {{}} end }}
            events = {{
              emit = function(name, event)
                emitted[#emitted + 1] = {{ name = name, event = event }}
                return 1
              end,
            }}
            action_cable = {{
              connect = function() return "conn-1" end,
              subscribe = function(_, _, _, cb)
                callback = cb
                return "chan-1"
              end,
              perform = function(_, action, data)
                if action == "ack" and data.id == 9 then acked = true end
              end,
              close = function() end,
            }}

            local routing = dofile("{template_path}")
            routing.start("owner/repo")
            callback({{
              id = 9,
              event_type = "pull_request_review",
              repo = "owner/repo",
              payload = {{
                action = "submitted",
                repo = "owner/repo",
                pr_number = 42,
                pr_url = "https://github.com/owner/repo/pull/42",
                review_id = 123,
                review_html_url = "https://github.com/owner/repo/pull/42#pullrequestreview-123",
                reviewer = "reviewer",
                state = "changes_requested",
                body = "Please fix the failing path.",
                submitted_at = "2026-05-14T20:00:00Z",
              }},
            }}, "chan-1")

            assert(acked == true)
            return {{ emitted = emitted }}
            "#
        ))
        .eval()
        .and_then(|value: Value| lua.from_value(value))
        .expect("GitHub event routing should emit PR review submissions");

    assert_eq!(result["emitted"][0]["name"], json!("pr_review_submitted"));
    assert_eq!(result["emitted"][0]["event"]["repo"], json!("owner/repo"));
    assert_eq!(result["emitted"][0]["event"]["pr_number"], json!(42));
    assert_eq!(
        result["emitted"][0]["event"]["state"],
        json!("changes_requested")
    );
    assert_eq!(result["emitted"][0]["event"]["reviewer"], json!("reviewer"));
}

#[test]
fn catalog_plugin_github_event_routing_emits_lifecycle_through_parent_hub() {
    let template_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github/event_routing.lua");
    let template_path = template_path.to_str().unwrap();

    let lua = create_lua_vm();

    let result: JsonValue = lua
        .load(format!(
            r#"
            local parent_requests = {{}}
            local local_emits = 0
            local acked = false
            local callback = nil

            package.loaded["lib.agent"] = {{ find_by_workspace = function() return {{}} end }}
            package.loaded["hub.state"] = {{ get = function() return {{}} end }}
            package.loaded["lib.hub"] = {{ get = function() return {{}} end }}
            plugin_worker_parent_hub = {{
              request = function(payload)
                parent_requests[#parent_requests + 1] = payload
                return {{ result = {{ delivered = 1 }} }}
              end,
              enqueue = function(payload)
                parent_requests[#parent_requests + 1] = payload
                return true
              end,
            }}
            events = {{
              emit = function()
                local_emits = local_emits + 1
                return 0
              end,
            }}
            action_cable = {{
              connect = function() return "conn-1" end,
              subscribe = function(_, _, _, cb)
                callback = cb
                return "chan-1"
              end,
              perform = function(_, action, data)
                if action == "ack" and data.id == 12 then acked = true end
              end,
              close = function() end,
            }}

            local routing = dofile("{template_path}")
            routing.start("owner/repo")
            callback({{
              id = 12,
              event_type = "pull_request_review",
              repo = "owner/repo",
              payload = {{
                action = "submitted",
                repo = "owner/repo",
                pr_number = 42,
                state = "commented",
              }},
            }}, "chan-1")

            return {{ parent_requests = parent_requests, local_emits = local_emits, acked = acked }}
            "#
        ))
        .eval()
        .and_then(|value: Value| lua.from_value(value))
        .expect("GitHub lifecycle events should emit through parent hub from plugin workers");

    assert_eq!(result["parent_requests"][0]["type"], json!("emit_event"));
    assert_eq!(
        result["parent_requests"][0]["event"],
        json!("pr_review_submitted")
    );
    assert_eq!(
        result["parent_requests"][0]["data"]["pr_number"],
        json!(42)
    );
    assert_eq!(result["local_emits"], json!(0));
    assert_eq!(result["acked"], json!(true));
}

#[test]
fn catalog_plugin_github_event_routing_emits_pr_comment_and_does_not_spawn_agent() {
    let template_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github/event_routing.lua");
    let template_path = template_path.to_str().unwrap();

    let lua = create_lua_vm();

    let result: JsonValue = lua
        .load(format!(
            r#"
            local emitted = {{}}
            local acked = false
            local callback = nil
            local find_calls = 0
            local create_calls = 0

            package.loaded["lib.agent"] = {{
              find_by_workspace = function()
                find_calls = find_calls + 1
                return {{}}
              end,
            }}
            package.loaded["hub.state"] = {{ get = function() return {{}} end }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function()
                    create_calls = create_calls + 1
                  end,
                }}
              end,
            }}
            events = {{
              emit = function(name, event)
                emitted[#emitted + 1] = {{ name = name, event = event }}
                return 1
              end,
            }}
            action_cable = {{
              connect = function() return "conn-1" end,
              subscribe = function(_, _, _, cb)
                callback = cb
                return "chan-1"
              end,
              perform = function(_, action, data)
                if action == "ack" and data.id == 10 then acked = true end
              end,
              close = function() end,
            }}

            local routing = dofile("{template_path}")
            routing.start("owner/repo")
            callback({{
              id = 10,
              event_type = "pull_request_comment",
              repo = "owner/repo",
              payload = {{
                action = "created",
                repo = "owner/repo",
                pr_number = 42,
                pr_url = "https://github.com/owner/repo/pull/42",
                comment_id = 456,
                comment_html_url = "https://github.com/owner/repo/pull/42#issuecomment-456",
                comment_body = "Can this be clearer?",
                comment_author = "reviewer",
                created_at = "2026-05-20T12:00:00Z",
                updated_at = "2026-05-20T12:00:00Z",
              }},
            }}, "chan-1")

            assert(acked == true)
            assert(find_calls == 0)
            assert(create_calls == 0)
            return {{ emitted = emitted, find_calls = find_calls, create_calls = create_calls }}
            "#
        ))
        .eval()
        .and_then(|value: Value| lua.from_value(value))
        .expect("GitHub event routing should emit PR comments without generic agent fallback");

    assert_eq!(result["emitted"][0]["name"], json!("pr_comment"));
    assert_eq!(result["emitted"][0]["event"]["repo"], json!("owner/repo"));
    assert_eq!(result["emitted"][0]["event"]["pr_number"], json!(42));
    assert_eq!(
        result["emitted"][0]["event"]["comment_body"],
        json!("Can this be clearer?")
    );
    assert_eq!(result["find_calls"], json!(0));
    assert_eq!(result["create_calls"], json!(0));
}

#[test]
fn catalog_plugin_github_event_routing_does_not_ack_lifecycle_without_consumer() {
    let template_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("catalog/templates/plugins/github/event_routing.lua");
    let template_path = template_path.to_str().unwrap();

    let lua = create_lua_vm();

    let result: JsonValue = lua
        .load(format!(
            r#"
            local acked = false
            local callback = nil
            local create_calls = 0

            package.loaded["lib.agent"] = {{
              find_by_workspace = function()
                error("pending lifecycle events must not fall back to generic agent routing")
              end,
            }}
            package.loaded["hub.state"] = {{ get = function() return {{}} end }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function()
                    create_calls = create_calls + 1
                  end,
                }}
              end,
            }}
            events = {{
              emit = function()
                return 0
              end,
            }}
            action_cable = {{
              connect = function() return "conn-1" end,
              subscribe = function(_, _, _, cb)
                callback = cb
                return "chan-1"
              end,
              perform = function(_, action, data)
                if action == "ack" and data.id == 11 then acked = true end
              end,
              close = function() end,
            }}

            local routing = dofile("{template_path}")
            routing.start("owner/repo")
            callback({{
              id = 11,
              event_type = "pull_request_review",
              repo = "owner/repo",
              payload = {{
                action = "submitted",
                repo = "owner/repo",
                pr_number = 42,
                state = "changes_requested",
              }},
            }}, "chan-1")

            return {{ acked = acked, create_calls = create_calls }}
            "#
        ))
        .eval()
        .and_then(|value: Value| lua.from_value(value))
        .expect("GitHub lifecycle events should remain pending until a consumer receives them");

    assert_eq!(result["acked"], json!(false));
    assert_eq!(result["create_calls"], json!(0));
}
