//! Project Pipelines plugin catalog regression tests.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::missing_docs_in_private_items,
    reason = "test-code brevity"
)]

use std::path::PathBuf;

use botster::lua::primitives::log;
use mlua::Lua;

fn project_root_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli has repo parent")
        .to_path_buf()
}

#[test]
fn catalog_plugin_project_pipelines_notification_policy_scopes_to_owned_sessions() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local claim = nil
            local pushes = {{}}
            local logs = {{}}
            log = {{
              info = function(message)
                logs[#logs + 1] = message
              end,
              warn = function() end,
            }}
            push = {{
              send = function(payload)
                pushes[#pushes + 1] = payload
              end,
            }}
            package.loaded["lib.notifications"] = {{
              claim = function(opts)
                claim = opts
              end,
            }}
            package.loaded["lib.surfaces"] = {{
              path = function(surface, path, params)
                assert(surface == "pipelines")
                assert(path == "/tickets/:ticket_id")
                return "/hubs/hub-test/pipelines/tickets/" .. params.ticket_id
              end,
            }}

            local policy = require("project_pipelines.notification_policy")
            policy.register()

            assert(claim.name == "project_pipelines.notification_policy")
            assert(claim.scope.owner_plugin == "project-pipelines")
            assert(claim.scope.all_sessions == nil)

            local suppressed = claim.handler({{ session_uuid = "sess-1", type = "osc9", message = "Task complete" }})
            assert(suppressed.core == "suppress")
            assert(suppressed.reason == "project_pipelines_routine_cli_notification")
            assert(#logs == 1)
            assert(logs[1]:match("notification suppressed"))
            assert(logs[1]:match("sess%-1"))
            assert(logs[1]:match("Task complete"))

            local permission = claim.handler({{ message = "PERMISSION needed" }})
            assert(permission.core == "replace")
            assert(permission.reason == "project_pipelines_allowed_permission")

            local approval = claim.handler({{ body = "Approval Requested before command" }})
            assert(approval.core == "replace")
            assert(approval.reason == "project_pipelines_allowed_approval_requested")

            local edit = policy.evaluate({{ title = "Codex wants to edit files" }})
            assert(edit.core == "replace")
            assert(edit.reason == "project_pipelines_allowed_wants_to_edit")

            local keyword = policy.evaluate({{ keywords = {{ "APPROVAL REQUESTED" }}, message = "tool is waiting" }})
            assert(keyword.core == "replace")
            assert(keyword.reason == "project_pipelines_allowed_approval_requested")

            local phase_text = policy.evaluate({{ message = "Phase changed to review" }})
            assert(phase_text.core == "suppress")
            assert(#logs == 2)
            assert(logs[2]:match("Phase changed to review"))

            policy.notify_phase_transition({{
              run_id = "run-1",
              ticket_id = "ticket-1",
              ticket = {{ title = "Ship feature" }},
              step = {{ name = "Review" }},
            }})
            assert(pushes[1].kind == "project_pipelines_phase_transition")
            assert(pushes[1].title == "Pipeline phase changed")
            assert(pushes[1].body:match("Review"))

            policy.notify_question_asked({{
              question = {{ id = "question-1", ticket_id = "ticket-1", question = "Which path should I take?" }},
              ticket = {{ title = "Ship feature" }},
            }})
            assert(pushes[2].kind == "project_pipelines_question")
            assert(pushes[2].title == "Pipeline question asked")
            assert(pushes[2].body:match("Which path should I take"))

            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines notification policy behavior");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_accessory_options_use_live_ticket_worktree() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local captured_repo_root = nil
            package.loaded["lib.config_resolver"] = {{
              list_accessories = function(device_root, repo_root)
                assert(device_root == "/device-root")
                captured_repo_root = repo_root
                return {{ "rails-server" }}
              end,
            }}
            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                if session_uuid ~= "sess-live" then return nil end
                return {{
                  info = function()
                    return {{
                      worktree_path = "/tmp/hyperflex-ticket-worktree",
                    }}
                  end,
                }}
              end,
            }}
            config = {{
              data_dir = function() return "/device-root" end,
            }}
            -- target_path is never stored on the ticket; the UI derives the
            -- repo-config scan root from target_id via the spawn target registry.
            spawn_targets = {{
              list = function()
                return {{
                  {{ id = "tgt-hyperflex", name = "Hyperflex", path = "/repo/hyperflex", enabled = true }},
                }}
              end,
              get = function(id)
                if id == "tgt-hyperflex" then
                  return {{ id = id, name = "Hyperflex", path = "/repo/hyperflex" }}
                end
                return nil
              end,
            }}

            local view = require("project_pipelines.web.ui")

            -- target_repo_path resolves the repo root from target_id alone.
            assert(view.target_repo_path("tgt-hyperflex") == "/repo/hyperflex")
            assert(view.target_repo_path("tgt-unknown") == nil)
            assert(view.target_repo_path(nil) == nil)

            -- A live ticket session's worktree wins over the derived repo root.
            local config_path = view.worktree_path_for_sessions(
              {{ "sess-live" }}, view.target_repo_path("tgt-hyperflex"))
            assert(config_path == "/tmp/hyperflex-ticket-worktree")

            local options = view.accessory_options("terminal", config_path)
            assert(captured_repo_root == "/tmp/hyperflex-ticket-worktree")
            assert(options[1].value == "terminal")
            assert(options[2].value == "rails-server")

            -- With no live session, it falls back to the derived repo root.
            local fallback = view.worktree_path_for_sessions(
              {{ "missing" }}, view.target_repo_path("tgt-hyperflex"))
            assert(fallback == "/repo/hyperflex")

            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines accessory options should resolve scan path from target_id");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_spawn_controls_resolve_options_from_target_id() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            -- Below-the-view dependencies the real ui.lua resolves through.
            package.loaded["lib.config_resolver"] = {{
              list_accessories = function(device_root, repo_root)
                assert(device_root == "/device-root", "accessory scan must use the device data dir")
                if repo_root == "/repo/hyperflex" then
                  return {{ "rails-server" }}
                end
                return {{}}
              end,
              list_agents = function(_device_root, repo_root)
                if repo_root == "/repo/hyperflex" then
                  return {{ "hyperflex-impl" }}
                end
                return {{}}
              end,
            }}
            -- No live ticket session, so the scan path falls back to the
            -- target's repo root derived purely from target_id.
            package.loaded["lib.agent"] = {{ get = function() return nil end }}
            package.loaded["project_pipelines.repo"] = {{}}
            package.loaded["project_pipelines.web.actions"] = {{
              draft = function() return {{}} end,
            }}
            config = {{ data_dir = function() return "/device-root" end }}
            spawn_targets = {{
              list = function()
                return {{
                  {{ id = "tgt-hyperflex", name = "Hyperflex", path = "/repo/hyperflex", enabled = true }},
                }}
              end,
              get = function(id)
                if id == "tgt-hyperflex" then
                  return {{ id = id, name = "Hyperflex", path = "/repo/hyperflex" }}
                end
                return nil
              end,
            }}

            -- Capture ui.select props so we can inspect the rendered option lists.
            local selects = {{}}
            local function node(kind)
              return function(props) return {{ kind = kind, props = props }} end
            end
            ui = {{
              button = node("button"),
              dialog = node("dialog"),
              stack = node("stack"),
              text = node("text"),
              textarea = node("textarea"),
              badge = node("badge"),
              inline = node("inline"),
              panel = node("panel"),
              empty_state = node("empty_state"),
              status_dot = node("status_dot"),
              select = function(props)
                selects[#selects + 1] = props
                return {{ kind = "select", props = props }}
              end,
              action = function(name, payload) return {{ kind = "action", name = name, payload = payload }} end,
              local_state = function(key, default) return {{ kind = "local_state", key = key, default = default }} end,
              responsive = function(map) return {{ kind = "responsive", map = map }} end,
              bind = function(path) return {{ bind = path }} end,
            }}

            local screen = require("project_pipelines.web.screens.ticket")
            local controls = screen.spawn_session_controls(
              {{ id = "ticket-1", status = "open", target_id = "tgt-hyperflex" }},
              {{}},
              {{ session_uuids = {{}} }})
            assert(type(controls) == "table" and #controls > 0, "spawn controls must render nodes")

            local accessory_options, agent_options
            for _, props in ipairs(selects) do
              if props.id == "ticket-ticket-1-spawn-accessory" then accessory_options = props.options end
              if props.id == "ticket-ticket-1-spawn-agent" then agent_options = props.options end
            end
            assert(accessory_options ~= nil, "accessory select did not render")
            assert(agent_options ~= nil, "agent select did not render")

            local function has(options, value)
              for _, option in ipairs(options or {{}}) do
                if option.value == value then return true end
              end
              return false
            end

            -- The accessory configured under the target's repo is presented,
            -- resolved purely from ticket.target_id with no stored target_path.
            assert(has(accessory_options, "rails-server"), "custom repo accessory must be presented")
            assert(has(accessory_options, "terminal"), "built-in terminal must remain present")
            -- Agent options reach repo-level definitions through the same derived path.
            assert(has(agent_options, "hyperflex-impl"), "repo-level agent must be presented")

            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("spawn controls should resolve agent/accessory options from target_id");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_refreshes_merged_checkouts_with_explicit_gates() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local gates = {{}}
            local notifications = {{}}
            local appended = {{}}
            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() end,
              notify_question_asked = function() end,
            }}
            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                if session_uuid ~= "sess-orchestrator" then return nil end
                return {{
                  info = function()
                    return {{ worktree_path = "/repo/project" }}
                  end,
                }}
              end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  run_command_gate = function(_, opts)
                    gates[#gates + 1] = opts
                    return {{ started = true }}
                  end,
                  notify = function(_, session_uuid, payload)
                    notifications[#notifications + 1] = {{ session_uuid = session_uuid, payload = payload }}
                  end,
                }}
              end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              get_ticket = function()
                return {{ id = "ticket-1", title = "Ship", project_id = "project-1", target_id = "target-1" }}
              end,
              latest_ticket_run = function() return nil end,
              ticket_session_uuids = function() return {{}} end,
              close_ticket = function() return {{ id = "ticket-1", status = "closed" }} end,
              get_question_orchestrator = function(project_id)
                if project_id == "project-1" then
                  return {{ session_uuid = "sess-orchestrator", scope = "project" }}
                end
                return nil
              end,
              resolve_target_path = function() return "/repo/project" end,
              append_event = function(kind, attrs)
                appended[#appended + 1] = {{ kind = kind, attrs = attrs }}
              end,
            }}

            local engine = require("project_pipelines.engine")
            engine.close_ticket("ticket-1", {{
              merge_confirmed = true,
              merge_commit = "abc123",
              base_branch = "main",
            }})

            assert(#notifications == 1, "merge delivery must send one alert")
            assert(notifications[1].payload.title == "Pipeline ticket merged")
            assert(notifications[1].payload.action.kind == "ticket_merged")
            assert(#gates == 1, "one checkout path must start one refresh")
            assert(gates[1].command == "git branch --show-current")
            assert(not gates[1].command:find(";", 1, true))
            assert(gates[1].context.operation == "merge_source_branch")

            local function context(operation)
              return {{
                owner_plugin = "project-pipelines",
                operation = operation,
                request_id = "refresh-1",
                session_uuid = "sess-orchestrator",
                checkout_path = "/repo/project",
                base_branch = "main",
                ticket_id = "ticket-1",
              }}
            end

            engine.handle_command_gate_completed({{
              success = true,
              stdout_tail = "",
              context = context("merge_source_branch"),
            }})
            assert(appended[#appended].attrs.payload.pull_status == "skipped_detached")
            assert(#gates == 1, "detached HEAD must not start another gate")
            assert(#notifications == 1, "detached HEAD must not add an alert")

            engine.handle_command_gate_completed({{
              success = true,
              stdout_tail = "feature\n",
              context = context("merge_source_branch"),
            }})
            assert(appended[#appended].attrs.payload.pull_status == "skipped_other_branch")
            assert(#gates == 1, "another branch must not start another gate")

            engine.handle_command_gate_completed({{
              success = true,
              stdout_tail = "main\n",
              context = context("merge_source_branch"),
            }})
            assert(gates[#gates].command == "git status --porcelain")
            local status_context = gates[#gates].context

            engine.handle_command_gate_completed({{
              success = true,
              stdout_tail = " M local.lua\n",
              context = status_context,
            }})
            assert(appended[#appended].attrs.payload.pull_status == "skipped_local_changes")
            assert(#notifications == 1, "local changes must not add an alert")

            engine.handle_command_gate_completed({{
              success = true,
              stdout_tail = "",
              context = status_context,
            }})
            local pull_gate = gates[#gates]
            assert(pull_gate.command == "git pull --no-rebase --no-autostash --no-edit origin 'main'")
            assert(not pull_gate.command:find(";", 1, true))
            assert(not pull_gate.command:find("=", 1, true))
            assert(pull_gate.env.GIT_TERMINAL_PROMPT == "0")
            assert(pull_gate.env.GIT_MERGE_AUTOEDIT == "no")

            engine.handle_command_gate_completed({{
              success = false,
              exit_status = 1,
              error = "pull failed",
              context = pull_gate.context,
            }})
            local conflict_gate = gates[#gates]
            assert(conflict_gate.command == "git diff --name-only --diff-filter=U")
            assert(#notifications == 1, "pull failure must wait for Git conflict evidence")

            engine.handle_command_gate_completed({{
              success = true,
              stdout_tail = "",
              context = conflict_gate.context,
            }})
            assert(appended[#appended].attrs.payload.pull_status == "failed")
            assert(#notifications == 2)
            assert(notifications[2].payload.title == "Merged ticket checkout refresh failed")
            assert(not notifications[2].payload.body:lower():find("conflict", 1, true))

            engine.handle_command_gate_completed({{
              success = true,
              stdout_tail = "engine.lua\n",
              context = context("merge_source_conflicts"),
            }})
            assert(appended[#appended].attrs.payload.pull_status == "conflicted")
            assert(#notifications == 3)
            assert(notifications[3].payload.title == "Merged ticket has checkout conflicts")

            engine.handle_command_gate_completed({{
              success = true,
              context = context("merge_source_pull"),
            }})
            assert(appended[#appended].attrs.payload.pull_status == "updated")
            assert(#notifications == 3, "a successful refresh must not duplicate the merge alert")

            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("merged checkout refresh should use explicit asynchronous command gates");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_pr_policy_rejects_closing_open_pr() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() end,
              notify_question_asked = function() end,
            }}
            package.loaded["lib.hub"] = {{ get = function() return {{}} end }}
            package.loaded["lib.agent"] = {{ get = function() return nil end }}

            package.loaded["project_pipelines.repo"] = {{
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship via PR", status = "open" }}
              end,
              latest_ticket_run = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipeline-1", status = "done" }}
              end,
              get_pipeline = function(pipeline_id)
                assert(pipeline_id == "pipeline-1")
                return {{ id = "pipeline-1", merge_policy = "pr" }}
              end,
              list_pr_links = function(filters)
                assert(filters.ticket_id == "ticket-1")
                if filters.status == "merged" then return {{}} end
                if filters.status == "open" then
                  return {{ {{ id = "pr-1", ticket_id = "ticket-1", status = "open", repo = "owner/repo", pr_number = 42 }} }}
                end
                return {{}}
              end,
              ticket_session_uuids = function()
                error("close should reject before closing sessions")
              end,
              close_ticket = function()
                error("open linked PR must not close ticket")
              end,
            }}

            local engine = require("project_pipelines.engine")
            local ok, err = pcall(engine.close_ticket, "ticket-1", {{ merge_confirmed = true, pr_url = "https://github.com/owner/repo/pull/42" }})
            assert(ok == false)
            assert(tostring(err):match("linked PR is still open"))

            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("PR-policy tickets should not close while linked PR is open");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_can_spawn_ticket_session_from_engine() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local created_agent = nil
            local appended = {{}}

            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() end,
              notify_question_asked = function() end,
            }}
            package.loaded["lib.agent"] = {{
              get = function()
                return {{
                  info = function()
                    return {{
                      worktree_path = "/tmp/ticket-worktree",
                      branch_name = "project-pipelines/ticket-1",
                      workspace_id = "workspace-1",
                      workspace_name = "Pipeline - Ship",
                    }}
                  end,
                }}
              end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function(_, opts)
                    created_agent = opts
                    return {{ session_uuid = "session-1", status = "queued" }}
                  end,
                }}
              end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship", target_id = "target-1" }}
              end,
              latest_ticket_run = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "run-1", ticket_id = "ticket-1" }}
              end,
              ticket_session_uuids = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ "existing-session" }}
              end,
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                assert(kind == "ticket.manual_session_linked")
                return {{}}
              end,
              append_event = function(kind, event)
                table.insert(appended, {{ kind = kind, event = event }})
              end,
            }}

            local engine = require("project_pipelines.engine")
            local result = engine.spawn_ticket_session({{ ticket_id = "ticket-1", agent_name = "codex" }}, {{}})
            assert(result.session.session_uuid == "session-1")
            assert(created_agent.from_worktree == "/tmp/ticket-worktree")
            assert(created_agent.issue_or_branch == "project-pipelines/ticket-1")
            assert(created_agent.metadata.owner_plugin == "project-pipelines")
            assert(created_agent.metadata.ticket_id == "ticket-1")
            assert(appended[1].kind == "ticket.manual_agent_requested")
            assert(appended[2].kind == "ticket.manual_session_linked")
            assert(appended[2].event.ticket_id == "ticket-1")
            assert(appended[2].event.payload.session_uuid == "session-1")

            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines should expose a clean engine spawn path");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_merge_prompt_describes_pr_steward_role() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local created_agent = nil

            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() end,
              notify_question_asked = function() end,
            }}
            package.loaded["lib.agent"] = {{
              get = function() return nil end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function(_, opts)
                    created_agent = opts
                    return {{ session_uuid = "sess-merge", status = "queued" }}
                  end,
                }}
              end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship PR Loop", target_id = "target-1" }}
              end,
              latest_ticket_run = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipeline-1", status = "done", target_id = "target-1" }}
              end,
              get_pipeline = function(pipeline_id)
                assert(pipeline_id == "pipeline-1")
                return {{ id = "pipeline-1", merge_policy = "pr" }}
              end,
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                assert(kind == "ticket.merge_requested")
                return {{}}
              end,
              append_event = function(kind, event)
                assert(kind == "ticket.merge_requested")
                assert(event.payload.merge_policy == "pr")
              end,
            }}

            local engine = require("project_pipelines.engine")
            local response = engine.request_merge({{ ticket_id = "ticket-1" }}, {{}})

            assert(response.merge_policy == "pr")
            assert(created_agent ~= nil)
            assert(created_agent.metadata.role == "merge")
            assert(created_agent.prompt:match("merge agent and PR steward"))
            assert(created_agent.prompt:match("orchestrator between the human"))
            assert(created_agent.prompt:match("Do not implement code changes yourself"))
            assert(created_agent.prompt:match("After opening or updating the PR, you remain the PR steward"))
            assert(created_agent.prompt:match("keep the conversation on the PR"))
            assert(created_agent.prompt:match("Delegate implementation work to the existing implementer"))
            assert(created_agent.prompt:match("architectural/product reasoning to the planner"))
            assert(created_agent.prompt:match("project_pipelines_get_ticket"))
            assert(created_agent.prompt:match("list_hubs"))
            assert(created_agent.prompt:match("post_message"))
            assert(created_agent.prompt:match("notify_session"))
            assert(created_agent.prompt:match("project_pipelines_ask_agent only when no existing ticket agent owns the needed context"))
            assert(created_agent.prompt:match("Do not create a new run or a new PR"))
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("merge prompt should describe the PR steward role");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_links_async_manual_session_creation() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local appended = nil
            package.loaded["project_pipelines.repo"] = {{
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                assert(kind == "ticket.manual_session_linked")
                return {{}}
              end,
              append_event = function(kind, event)
                appended = {{ kind = kind, event = event }}
              end,
            }}
            package.loaded["project_pipelines.entities"] = {{}}
            package.loaded["project_pipelines.notification_policy"] = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{}}
              end,
            }}
            package.loaded["lib.agent"] = {{
              get = function()
                return nil
              end,
            }}

            local engine = require("project_pipelines.engine")
            engine.handle_agent_created({{
              session_uuid = "sess-manual",
              session_type = "accessory",
              session_name = "rails-server",
              request_id = "project-pipelines:ticket-1:manual:123",
              metadata = {{
                request_id = "project-pipelines:ticket-1:manual:123",
                run_id = "run-1",
                role = "manual-accessory",
              }},
            }})

            assert(appended.kind == "ticket.manual_session_linked")
            assert(appended.event.ticket_id == "ticket-1")
            assert(appended.event.run_id == "run-1")
            assert(appended.event.payload.session_uuid == "sess-manual")
            assert(appended.event.payload.request_id == "project-pipelines:ticket-1:manual:123")
            assert(appended.event.payload.session_type == "accessory")
            assert(appended.event.payload.accessory_name == "rails-server")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("manual session creation should be linked back to the ticket");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_deletes_only_manual_ticket_sessions() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");
    botster::lua::primitives::json::register(&lua).expect("register json");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local deleted = nil
            local appended = nil
            package.loaded["project_pipelines.repo"] = {{
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship" }}
              end,
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                if kind == "ticket.manual_session_linked" then
                  return {{
                    {{ kind = kind, payload = '{{"session_uuid":"sess-manual"}}' }},
                  }}
                end
                if kind == "ticket.manual_session_removed" then
                  return {{}}
                end
                error("unexpected event kind " .. tostring(kind))
              end,
              append_event = function(kind, event)
                appended = {{ kind = kind, event = event }}
              end,
            }}
            package.loaded["project_pipelines.entities"] = {{}}
            package.loaded["project_pipelines.notification_policy"] = {{}}
            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                assert(session_uuid == "sess-manual")
                return {{ session_uuid = session_uuid }}
              end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  delete_agent = function(_, session_uuid, delete_worktree)
                    deleted = {{ session_uuid = session_uuid, delete_worktree = delete_worktree }}
                    return "Delete requested"
                  end,
                }}
              end,
            }}

            local engine = require("project_pipelines.engine")
            local result = engine.delete_manual_ticket_session({{ ticket_id = "ticket-1", session_uuid = "sess-manual" }}, {{}})

            assert(result.removed == true)
            assert(result.closed == true)
            assert(deleted.session_uuid == "sess-manual")
            assert(deleted.delete_worktree == false)
            assert(appended.kind == "ticket.manual_session_removed")
            assert(appended.event.ticket_id == "ticket-1")
            assert(appended.event.payload.session_uuid == "sess-manual")
            assert(appended.event.payload.closed == true)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("manual ticket sessions should be deletable without deleting the worktree");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_home_shows_linked_pr_records() {
    let root = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let home = std::fs::read_to_string(root.join("project_pipelines/web/screens/home.lua"))
        .expect("read home screen");
    let entities = std::fs::read_to_string(root.join("project_pipelines/entities.lua"))
        .expect("read entities");

    assert!(
        home.contains(r#"source = "/project-pipelines.ticket""#)
            && home.contains(r#"where = { status = "open", latest_run_status = "done" }"#),
        "home PR rows should be driven by the ticket entity stream"
    );
    for binding in [
        r#"ui.bind("@/latest_run_path")"#,
        r#"ui.bind("@/merge_status_label")"#,
        r#"ui.bind("@/merge_status_tone")"#,
        r#"ui.bind("@/merge_detail_label")"#,
        r#"ui.bind("@/merge_pr_url")"#,
        r#"ui.bind("@/merge_session_path")"#,
    ] {
        assert!(
            home.contains(binding),
            "home PR rows should bind projected PR field {binding}"
        );
    }
    for binding in [
        r#"ui.bind_if("@/has_latest_run""#,
        r#"ui.bind_if("@/has_merge_pr_url""#,
        r#"ui.bind_if("@/has_merge_session""#,
    ] {
        assert!(
            home.contains(binding),
            "home PR rows should condition row actions on projected field {binding}"
        );
    }
    assert!(
        !home.contains("TODO(entity-shape)"),
        "home PR rows should not carry the entity-shape TODO after action fields are projected"
    );
    assert!(
        !home.contains("repo.list_pr_links") && !home.contains("latest_ticket_pr_link"),
        "home render must not reintroduce synchronous repo PR lookups"
    );
    assert!(
        entities.contains("entity.merge_pr_label = pr_label")
            && entities.contains("entity.latest_run_path = latest and")
            && entities.contains("entity.merge_session_path = entity.has_merge_session")
            && entities.contains(r#"entity.merge_status_label = pr_label"#)
            && entities.contains(r#""PR needs review""#),
        "ticket entities should project linked PR labels and review status for home rows"
    );
}

#[test]
fn catalog_plugin_project_pipelines_spawn_modal_uses_local_presentation_state() {
    let root = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let ticket_screen =
        std::fs::read_to_string(root.join("project_pipelines/web/screens/ticket.lua"))
            .expect("read ticket screen");
    let surface = std::fs::read_to_string(root.join("project_pipelines/web/surface.lua"))
        .expect("read surface");
    let actions = std::fs::read_to_string(root.join("project_pipelines/web/actions.lua"))
        .expect("read actions");

    assert!(ticket_screen.contains("ui.local_state(agent_dialog_key, false)"));
    assert!(ticket_screen.contains(r#"local default_agent_name = "codex""#));
    assert!(ticket_screen.contains(r#"local default_prompt = """#));
    assert!(ticket_screen.contains(r#"local default_accessory_name = "terminal""#));
    assert!(ticket_screen.contains("value = ui.local_state(agent_name_key, default_agent_name)"));
    assert!(ticket_screen.contains("value = ui.local_state(prompt_key, default_prompt)"));
    assert!(ticket_screen
        .contains("value = ui.local_state(accessory_name_key, default_accessory_name)"));
    assert!(
        ticket_screen.contains("agent_name = ui.local_state(agent_name_key, default_agent_name)")
    );
    assert!(ticket_screen.contains("prompt = ui.local_state(prompt_key, default_prompt)"));
    assert!(ticket_screen
        .contains("accessory_name = ui.local_state(accessory_name_key, default_accessory_name)"));
    assert!(ticket_screen.contains("botster.presentation.set"));
    assert!(ticket_screen.contains("botster.presentation.clear"));
    assert!(ticket_screen.contains("project_pipelines.spawn_ticket_session"));
    assert!(
        !ticket_screen.contains("project_pipelines.update_ticket_session_draft"),
        "spawn modal field edits should stay browser-local instead of round-tripping controlled values through the hub"
    );
    assert!(
        !surface.contains("/tickets/:ticket_id/spawn"),
        "spawn modal state should not be encoded as a plugin route"
    );
    assert!(
        actions.contains("presentation = {")
            && actions.contains("-spawn-agent-open")
            && actions.contains(r#"prefix .. "agent_name""#)
            && actions.contains(r#"prefix .. "prompt""#)
            && actions.contains(r#"prefix .. "accessory_name""#),
        "successful spawn should clear browser-local modal and draft field state"
    );
    assert!(
        actions.contains("agent_name = payload.agent_name")
            && actions.contains("prompt = payload.prompt")
            && actions.contains("accessory_name = payload.accessory_name"),
        "spawn action should use browser-resolved local payload values"
    );
    assert!(
        !actions.contains("project_pipelines.update_ticket_session_draft")
            && !actions.contains("draft[prefix"),
        "spawn dialog should not keep a dead hub draft fallback"
    );
    assert!(
        ticket_screen.contains("ui.session_row"),
        "ticket-owned session rows should use the native session row so generic plugin session actions stay available"
    );
    let engine =
        std::fs::read_to_string(root.join("project_pipelines/engine.lua")).expect("read engine");
    assert!(
        engine.contains("set_surface_subpath") && !engine.contains("broadcast_ui_tree_snapshots"),
        "ticket session presentation changes may rerender the active surface but must not broadcast data-only tree snapshots"
    );
}

#[test]
fn catalog_plugin_project_pipelines_closes_linked_ticket_on_pr_merged_event() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local closed = nil
            local marked = nil

            package.loaded["project_pipelines.repo"] = {{
              find_pr_link = function(attrs)
                assert(attrs.provider == "github")
                assert(attrs.repo == "owner/repo")
                assert(attrs.pr_number == 42)
                return {{
                  id = "pr-1",
                  provider = "github",
                  repo = "owner/repo",
                  pr_number = 42,
                  pr_url = "https://github.com/owner/repo/pull/42",
                  ticket_id = "ticket-1",
                  run_id = "run-1",
                  status = "open",
                }}
              end,
              mark_pr_link_merged = function(link_id, attrs)
                assert(link_id == "pr-1")
                marked = attrs
                return {{
                  id = "pr-1",
                  provider = "github",
                  repo = "owner/repo",
                  pr_number = 42,
                  pr_url = attrs.pr_url,
                  ticket_id = "ticket-1",
                  run_id = "run-1",
                  status = "merged",
                  merge_commit = attrs.merge_commit,
                }}
              end,
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", status = "open" }}
              end,
            }}

            package.loaded["project_pipelines.engine"] = {{
              close_ticket = function(ticket_id, attrs)
                assert(ticket_id == "ticket-1")
                assert(attrs.merge_confirmed == true)
                assert(attrs.source_event == "pr_merged")
                assert(attrs.pr_url == "https://github.com/owner/repo/pull/42")
                assert(attrs.merge_commit == "abc123")
                closed = attrs
                return {{ id = "ticket-1", status = "closed" }}
              end,
            }}

            local integration = require("project_pipelines.github_integration")
            local response = integration.handle_pr_merged({{
              provider = "github",
              repo = "owner/repo",
              pr_number = 42,
              pr_url = "https://github.com/owner/repo/pull/42",
              merge_commit = "abc123",
            }})

            assert(response.ok == true)
            assert(marked.merge_commit == "abc123")
            assert(closed.repo == "owner/repo")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("linked PR merge should close ticket");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_ignores_unlinked_pr_merged_event() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            package.loaded["project_pipelines.repo"] = {{
              find_pr_link = function() return nil end,
            }}

            package.loaded["project_pipelines.engine"] = {{
              close_ticket = function()
                error("unlinked PR merge must not close a ticket")
              end,
            }}

            local integration = require("project_pipelines.github_integration")
            local response = integration.handle_pr_merged({{
              repo = "owner/repo",
              pr_number = 42,
            }})

            assert(response.ok == false)
            assert(response.reason == "pr_not_linked")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("unlinked PR merge should be ignored");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_routes_pr_review_to_live_merge_steward() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");
    botster::lua::primitives::json::register(&lua).expect("register json");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local events = {{}}
            local posted = nil
            local notified = nil

            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                if session_uuid == "sess-merge" then
                  return {{ info = function() return {{ session_uuid = session_uuid }} end }}
                end
                return nil
              end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  post = function(_, session_uuid, message)
                    posted = {{ session_uuid = session_uuid, message = message }}
                  end,
                  notify = function(_, session_uuid, notification)
                    notified = {{ session_uuid = session_uuid, notification = notification }}
                  end,
                }}
              end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() end,
            }}
            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              find_pr_link = function(attrs)
                assert(attrs.provider == "github")
                assert(attrs.repo == "owner/repo")
                assert(attrs.pr_number == 42)
                return {{
                  id = "pr-1",
                  provider = "github",
                  repo = "owner/repo",
                  pr_number = 42,
                  pr_url = "https://github.com/owner/repo/pull/42",
                  ticket_id = "ticket-1",
                  run_id = "run-1",
                  status = "open",
                }}
              end,
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship review loop", status = "open", target_id = "target-1" }}
              end,
              get_run = function(run_id)
                assert(run_id == "run-1")
                return {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipeline-1", status = "done", target_id = "target-1", current_step_id = false, current_run_step_id = false }}
              end,
              latest_ticket_run = function() error("linked run should be used") end,
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                if kind == "ticket.merge_agent_linked" then
                  return {{ {{ payload = "{{\"session_uuid\":\"sess-merge\"}}" }} }}
                end
                return {{
                }}
              end,
              pipeline_steps = function()
                error("merge steward should triage before implementation fallback")
              end,
              create_run_step_visit = function()
                error("merge steward path must not create an implementation visit")
              end,
              update_run = function()
                error("merge steward path must not reactivate the run directly")
              end,
              append_event = function(kind, event)
                events[#events + 1] = {{ kind = kind, event = event }}
              end,
            }}

            local integration = require("project_pipelines.github_integration")
            local response = integration.handle_pr_review_submitted({{
              provider = "github",
              repo = "owner/repo",
              pr_number = 42,
              pr_url = "https://github.com/owner/repo/pull/42",
              review_id = 123,
              review_html_url = "https://github.com/owner/repo/pull/42#pullrequestreview-123",
              reviewer = "reviewer",
              state = "changes_requested",
              body = "Please fix the failing path.",
            }})

            assert(response.ok == true)
            assert(response.status == "steward_prompted")
            assert(response.pr_steward.session_uuid == "sess-merge")
            assert(posted.session_uuid == "sess-merge")
            assert(posted.message.type == "task")
            assert(posted.message.payload.source_event == "pr_review_submitted")
            assert(posted.message.payload.review_state == "changes_requested")
            assert(posted.message.payload.instructions:match("👀"))
            assert(posted.message.payload.instructions:match("Please fix the failing path%."))
            assert(posted.message.payload.instructions:match("You are an orchestrator, not the implementer"))
            assert(posted.message.payload.instructions:match("Do not implement PR feedback yourself"))
            assert(posted.message.payload.instructions:match("ask clarifying follow%-up questions and provide answers on that PR thread"))
            assert(posted.message.payload.instructions:match("delegate to the existing implementer"))
            assert(posted.message.payload.instructions:match("architectural/product reasoning"))
            assert(posted.message.payload.instructions:match("post_message"))
            assert(posted.message.payload.instructions:match("notify_session"))
            assert(notified.session_uuid == "sess-merge")
            assert(notified.notification.title == "PR changes requested")
            assert(notified.notification.body:match("👀"))
            assert(notified.notification.action.name == "project_pipelines_current_context")
            assert(events[1].kind == "ticket.pr_review_submitted")
            assert(events[2].kind == "ticket.pr_review_steward_prompted")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("PR review changes should route to the live merge steward");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_routes_pr_comment_to_live_merge_steward() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");
    botster::lua::primitives::json::register(&lua).expect("register json");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local events = {{}}
            local posted = nil
            local notified = nil

            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                if session_uuid == "sess-merge" then
                  return {{ info = function() return {{ session_uuid = session_uuid }} end }}
                end
                return nil
              end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  post = function(_, session_uuid, message)
                    posted = {{ session_uuid = session_uuid, message = message }}
                  end,
                  notify = function(_, session_uuid, notification)
                    notified = {{ session_uuid = session_uuid, notification = notification }}
                  end,
                }}
              end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() end,
            }}
            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              find_pr_link = function(attrs)
                assert(attrs.provider == "github")
                assert(attrs.repo == "owner/repo")
                assert(attrs.pr_number == 42)
                return {{
                  id = "pr-1",
                  provider = "github",
                  repo = "owner/repo",
                  pr_number = 42,
                  pr_url = "https://github.com/owner/repo/pull/42",
                  ticket_id = "ticket-1",
                  run_id = "run-1",
                  status = "open",
                }}
              end,
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship review loop", status = "open", target_id = "target-1" }}
              end,
              get_run = function(run_id)
                assert(run_id == "run-1")
                return {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipeline-1", status = "done", target_id = "target-1", current_step_id = false, current_run_step_id = false }}
              end,
              latest_ticket_run = function() error("linked run should be used") end,
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                if kind == "ticket.merge_agent_linked" then
                  return {{ {{ payload = "{{\"session_uuid\":\"sess-merge\"}}" }} }}
                end
                return {{}}
              end,
              pipeline_steps = function()
                error("PR comment steward path must not inspect implementation steps")
              end,
              create_run_step_visit = function()
                error("PR comment steward path must not create an implementation visit")
              end,
              update_run = function()
                error("PR comment steward path must not reactivate the run directly")
              end,
              append_event = function(kind, event)
                events[#events + 1] = {{ kind = kind, event = event }}
              end,
            }}

            local integration = require("project_pipelines.github_integration")
            local response = integration.handle_pr_comment({{
              provider = "github",
              repo = "owner/repo",
              pr_number = 42,
              pr_url = "https://github.com/owner/repo/pull/42",
              comment_id = 456,
              comment_url = "https://github.com/owner/repo/pull/42#issuecomment-456",
              comment_author = "reviewer",
              comment_body = "Can this be clearer?",
              action = "created",
              created_at = "2026-05-20T12:00:00Z",
            }})

            assert(response.ok == true)
            assert(response.status == "steward_prompted")
            assert(response.pr_steward.session_uuid == "sess-merge")
            assert(posted.session_uuid == "sess-merge")
            assert(posted.message.type == "task")
            assert(posted.message.payload.source_event == "pr_comment")
            assert(posted.message.payload.comment_id == 456)
            assert(posted.message.payload.instructions:match("👀"))
            assert(posted.message.payload.instructions:match("Can this be clearer%?"))
            assert(posted.message.payload.instructions:match("answer on that PR thread"))
            assert(notified.session_uuid == "sess-merge")
            assert(notified.notification.body:match("👀"))
            assert(notified.notification.action.name == "project_pipelines_current_context")
            assert(events[1].kind == "ticket.pr_comment")
            assert(events[2].kind == "ticket.pr_comment_steward_prompted")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("PR comments should route to the live merge steward");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_records_pr_comment_without_merge_steward() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local events = {{}}
            local posted = nil
            local notified = nil

            package.loaded["lib.agent"] = {{
              get = function() return nil end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  post = function(_, session_uuid, message)
                    posted = {{ session_uuid = session_uuid, message = message }}
                  end,
                  notify = function(_, session_uuid, notification)
                    notified = {{ session_uuid = session_uuid, notification = notification }}
                  end,
                }}
              end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() error("PR comment without steward must not transition phase") end,
            }}
            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              find_pr_link = function(attrs)
                assert(attrs.provider == "github")
                assert(attrs.repo == "owner/repo")
                assert(attrs.pr_number == 42)
                return {{
                  id = "pr-1",
                  provider = "github",
                  repo = "owner/repo",
                  pr_number = 42,
                  pr_url = "https://github.com/owner/repo/pull/42",
                  ticket_id = "ticket-1",
                  run_id = "run-1",
                  status = "open",
                }}
              end,
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship review loop", status = "open", target_id = "target-1" }}
              end,
              get_run = function(run_id)
                assert(run_id == "run-1")
                return {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipeline-1", status = "done", target_id = "target-1", current_step_id = false, current_run_step_id = false }}
              end,
              latest_ticket_run = function() error("linked run should be used") end,
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                assert(kind == "ticket.merge_agent_linked" or kind == "ticket.merge_requested")
                return {{}}
              end,
              pipeline_steps = function()
                error("PR comment without steward must not inspect implementation steps")
              end,
              create_run_step_visit = function()
                error("PR comment without steward must not create an implementation visit")
              end,
              update_run = function()
                error("PR comment without steward must not mutate the run")
              end,
              latest_step_session = function()
                error("PR comment without steward must not look up implementation sessions")
              end,
              append_event = function(kind, event)
                events[#events + 1] = {{ kind = kind, event = event }}
              end,
            }}

            local integration = require("project_pipelines.github_integration")
            local response = integration.handle_pr_comment({{
              provider = "github",
              repo = "owner/repo",
              pr_number = 42,
              comment_id = 456,
              comment_body = "Can this be clearer?",
              action = "created",
            }})

            assert(response.ok == true)
            assert(response.status == "recorded")
            assert(response.reason == "no_steward")
            assert(posted == nil)
            assert(notified == nil)
            assert(#events == 1)
            assert(events[1].kind == "ticket.pr_comment")
            assert(events[1].event.payload.comment_id == 456)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("PR comments without a steward should be recorded only");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_falls_back_to_existing_implementer_when_no_merge_steward() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local events = {{}}
            local visits = {{}}
            local posted = nil
            local notified = nil
            local dependency_status = "open"
            local spawn_fails = false
            local visit_status = nil

            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                if session_uuid == "sess-impl" and not spawn_fails then
                  return {{ info = function() return {{ session_uuid = session_uuid }} end }}
                end
                return nil
              end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  post = function(_, session_uuid, message)
                    posted = {{ session_uuid = session_uuid, message = message }}
                  end,
                  notify = function(_, session_uuid, notification)
                    notified = {{ session_uuid = session_uuid, notification = notification }}
                  end,
                  create_agent = function()
                    error("spawn unavailable")
                  end,
                }}
              end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() end,
            }}
            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              find_pr_link = function(attrs)
                assert(attrs.provider == "github")
                assert(attrs.repo == "owner/repo")
                assert(attrs.pr_number == 42)
                return {{
                  id = "pr-1",
                  provider = "github",
                  repo = "owner/repo",
                  pr_number = 42,
                  pr_url = "https://github.com/owner/repo/pull/42",
                  ticket_id = "ticket-1",
                  run_id = "run-1",
                  status = "open",
                }}
              end,
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{ id = "ticket-1", title = "Ship review loop", status = "open", target_id = "target-1" }}
              end,
              get_run = function(run_id)
                assert(run_id == "run-1")
                return {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipeline-1", status = "done", target_id = "target-1", current_step_id = false, current_run_step_id = false }}
              end,
              latest_ticket_run = function() error("linked run should be used") end,
              ticket_events = function(ticket_id, kind)
                assert(ticket_id == "ticket-1")
                assert(kind == "ticket.merge_agent_linked" or kind == "ticket.merge_requested")
                return {{}}
              end,
              pipeline_steps = function(pipeline_id)
                assert(pipeline_id == "pipeline-1")
                return {{
                  {{ id = "impl", kind = "agent", name = "Implement", agent_name = "codex", prompt = "Build the change" }},
                  {{ id = "review", kind = "agent", name = "Review", agent_name = "codex" }},
                }}
              end,
              ticket_dependencies = function(ticket_id)
                assert(ticket_id == "ticket-1")
                return {{
                  {{ id = "dependency-1", ticket_id = ticket_id, depends_on_ticket_id = "ticket-prerequisite", depends_on_title = "Prerequisite", depends_on_status = dependency_status }},
                }}
              end,
              create_run_step_visit = function(run_id, step_id, attrs)
                assert(run_id == "run-1")
                assert(step_id == "impl")
                local visit = {{ id = "visit-2", run_id = run_id, step_id = step_id, status = attrs.status, sequence = 2 }}
                visit_status = attrs.status
                visits[#visits + 1] = visit
                return visit
              end,
              update_run = function(run_id, attrs)
                assert(run_id == "run-1")
                if attrs.status == "active" then
                  assert(attrs.current_step_id == "impl")
                  assert(attrs.current_run_step_id == "visit-2")
                else
                  assert(attrs.status == "blocked")
                end
                return {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipeline-1", status = attrs.status, target_id = "target-1", current_step_id = "impl", current_run_step_id = "visit-2" }}
              end,
              append_event = function(kind, event)
                events[#events + 1] = {{ kind = kind, event = event }}
              end,
              get_run_step_visit = function(run_step_id)
                return {{ id = run_step_id, run_id = "run-1", step_id = "impl", status = visit_status }}
              end,
              latest_step_session = function(run_id, step_id)
                assert(run_id == "run-1")
                assert(step_id == "impl")
                return {{ id = "visit-1", agent_session_uuid = "sess-impl" }}
              end,
              update_run_step_visit = function(run_step_id, attrs)
                assert(run_step_id == "visit-2")
                if attrs.agent_session_uuid then
                  assert(attrs.agent_session_uuid == "sess-impl")
                elseif attrs.status then
                  visit_status = attrs.status
                end
                return {{ id = run_step_id, run_id = "run-1", step_id = "impl", status = visit_status, agent_session_uuid = attrs.agent_session_uuid }}
              end,
            }}

            local integration = require("project_pipelines.github_integration")
            local blocked = integration.handle_pr_review_submitted({{
              provider = "github",
              repo = "owner/repo",
              pr_number = 42,
              pr_url = "https://github.com/owner/repo/pull/42",
              review_id = 123,
              review_html_url = "https://github.com/owner/repo/pull/42#pullrequestreview-123",
              reviewer = "reviewer",
              state = "changes_requested",
              body = "Please fix the failing path.",
            }})

            assert(blocked.ok == false)
            assert(blocked.status == "blocked")
            assert(blocked.reason == "ticket_dependencies")
            assert(blocked.source == "pr_review_submitted")
            assert(blocked.target_step_id == "impl")
            assert(blocked.unmet_dependencies[1].reason == "open_ticket")
            assert(#visits == 0)
            assert(posted == nil)
            assert(notified == nil)
            assert(events[1].kind == "ticket.pr_review_submitted")
            assert(events[2].kind == "step.advance_blocked")

            dependency_status = "closed"
            local response = integration.handle_pr_review_submitted({{
              provider = "github",
              repo = "owner/repo",
              pr_number = 42,
              pr_url = "https://github.com/owner/repo/pull/42",
              review_id = 124,
              review_html_url = "https://github.com/owner/repo/pull/42#pullrequestreview-124",
              reviewer = "reviewer",
              state = "changes_requested",
              body = "Please fix the failing path.",
            }})

            assert(response.ok == true)
            assert(response.status == "reactivated")
            assert(response.agent.reused == true)
            assert(posted.session_uuid == "sess-impl")
            assert(posted.message.payload.instructions:match("👀"))
            assert(posted.message.payload.instructions:match("Please fix the failing path%."))
            assert(notified.notification.title == "PR changes requested")
            assert(#visits == 1)
            assert(events[3].kind == "ticket.pr_review_submitted")
            assert(events[4].kind == "step.activated")
            assert(events[5].kind == "step.agent_prompted")

            spawn_fails = true
            local failed = integration.handle_pr_review_submitted({{
              provider = "github",
              repo = "owner/repo",
              pr_number = 42,
              pr_url = "https://github.com/owner/repo/pull/42",
              review_id = 125,
              review_html_url = "https://github.com/owner/repo/pull/42#pullrequestreview-125",
              reviewer = "reviewer",
              state = "changes_requested",
              body = "Please retry.",
            }})
            assert(failed.ok == false)
            assert(failed.status == "blocked")
            assert(failed.run_step.id == "visit-2")
            assert(failed.run_step.status == "blocked")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("PR review changes should return the linked run to implementer");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_home_render_uses_bounded_notified_session_lookup() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local calls = {{
              agent_list = 0,
              ticket_session_links_for_uuids = 0,
              ticket_session_uuids_by_ticket = 0,
              list_tickets = 0,
            }}

            ui = {{
              bind = function(path) return {{ bind = path }} end,
              bind_if = function(path, node) return {{ bind_if = path, node = node }} end,
              action = function(name, payload) return {{ name = name, payload = payload }} end,
              list_item = function(props) return {{ type = "list_item", props = props }} end,
              text = function(props) return {{ type = "text", props = props }} end,
              badge = function(props) return {{ type = "badge", props = props }} end,
              status_dot = function(props) return {{ type = "status_dot", props = props }} end,
              button = function(props) return {{ type = "button", props = props }} end,
              list = function(props) return {{ type = "list", props = props }} end,
              bind_list = function(props) return {{ type = "bind_list", props = props }} end,
              stack = function(props) return {{ type = "stack", props = props }} end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              page_header = function(props) return {{ type = "page_header", props = props }} end,
              panel = function(child) return {{ type = "panel", child = child }} end,
              section = function(title, children) return {{ type = "section", title = title, children = children }} end,
              empty = function(title) return {{ type = "empty", title = title }} end,
              badge = function(text, tone) return {{ type = "badge", text = text, tone = tone }} end,
              row = function(children) return {{ type = "row", children = children }} end,
              action_row = function(children) return {{ type = "action_row", children = children }} end,
              session_info = function() return nil end,
            }}

            package.loaded["project_pipelines.repo"] = {{
              list_runs = function()
                return {{
                  {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipe-1", current_step_id = "step-1", status = "active" }},
                }}
              end,
              get_ticket = function(id) return {{ id = id, title = "Ticket " .. id }} end,
              get_pipeline = function(id) return {{ id = id, name = "Pipeline " .. id }} end,
              get_step = function(id) return {{ id = id, name = "Step " .. id }} end,
              get_run_step_visit = function() return nil end,
              latest_ticket_run = function() return nil end,
              latest_merge_pr_artifact = function() return nil end,
              ticket_events = function() return {{}} end,
              visible_tickets = function() return {{}} end,
              has_open_questions = function() return false end,
              ticket_session_links_for_uuids = function(uuids)
                calls.ticket_session_links_for_uuids = calls.ticket_session_links_for_uuids + 1
                assert(#uuids == 1)
                assert(uuids[1] == "sess-notified")
                return {{
                  ["sess-notified"] = {{ {{ id = "ticket-1", title = "Important ticket" }} }},
                }}
              end,
              ticket_session_uuids_by_ticket = function()
                calls.ticket_session_uuids_by_ticket = calls.ticket_session_uuids_by_ticket + 1
                error("home render must not scan every ticket/session link")
              end,
              list_tickets = function()
                calls.list_tickets = calls.list_tickets + 1
                error("home render must not list every ticket for notification rows")
              end,
            }}

            package.loaded["lib.agent"] = {{
              list = function()
                calls.agent_list = calls.agent_list + 1
                return {{
                  {{ info = function() return {{ session_uuid = "sess-notified", notification = true, label = "Needs attention" }} end }},
                  {{ info = function() return {{ session_uuid = "sess-quiet", notification = false, label = "Quiet" }} end }},
                }}
              end,
            }}

            local home = require("project_pipelines.web.screens.home")
            home.render({{}}, {{ path = function(path) return "/pipelines" .. path end }})

            assert(calls.agent_list == 0)
            assert(calls.ticket_session_links_for_uuids == 0)
            assert(calls.ticket_session_uuids_by_ticket == 0)
            assert(calls.list_tickets == 0)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines home render should use bounded session lookup");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_home_bind_lists_use_entity_empty_templates() {
    let home = std::fs::read_to_string(project_root_dir().join(
        "catalog/templates/plugins/project-pipelines/project_pipelines/web/screens/home.lua",
    ))
    .expect("read project pipelines home screen");
    let surface = std::fs::read_to_string(
        project_root_dir()
            .join("catalog/templates/plugins/project-pipelines/project_pipelines/web/surface.lua"),
    )
    .expect("read project pipelines surface");

    assert_eq!(
        home.matches("ui.bind_list").count(),
        7,
        "home should keep the seven entity-backed bind_list sections"
    );
    assert_eq!(
        home.matches("empty_template = view.empty").count(),
        7,
        "each home bind_list should provide an empty_template"
    );

    for source in [
        "/project-pipelines.attention_item",
        "/project-pipelines.question",
        "/project-pipelines.run",
        "/project-pipelines.ticket",
        "/project-pipelines.project",
        "/project-pipelines.pipeline",
    ] {
        assert!(
            home.contains(source),
            "home should keep entity source {source}"
        );
    }

    assert!(
        !home.contains("repo."),
        "home should not call repo.* at render time"
    );
    for snippet in [
        r#"source = "/project-pipelines.run""#,
        r#"ui.bind_if("@/has_ticket""#,
        r#"path = ui.bind("@/ticket_path")"#,
        r#"ui.bind_if("@/has_project""#,
        r#"path = ui.bind("@/project_path")"#,
        r#"label = "Run""#,
        r#"path = ui.bind("@/path")"#,
        r#"ui.bind_if("@/has_current_agent""#,
        r#"path = ui.bind("@/current_agent_path")"#,
        r#"source = "/project-pipelines.attention_item""#,
        r#"Questions and agent notifications will appear here."#,
    ] {
        assert!(
            home.contains(snippet),
            "Running Pipelines rows should expose entity-backed action snippet: {snippet}"
        );
    }

    for snippet in [
        r#"project-pipelines.needs-attention"#,
        r#"source = "/project-pipelines.attention_item""#,
        r#"Questions and agent notifications will appear here."#,
    ] {
        assert!(
            surface.contains(snippet),
            "dashboard should expose the Needs Attention widget snippet: {snippet}"
        );
    }
}

#[test]
fn catalog_plugin_project_pipelines_notification_counts_only_use_current_agents() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            package.loaded["lib.config_resolver"] = {{
              list_agents = function() return {{}} end,
              list_accessories = function() return {{}} end,
            }}
            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                return {{
                  info = function()
                    return {{ session_uuid = session_uuid, notification = session_uuid == "sess-current" }}
                  end,
                }}
              end,
            }}

            local current_lookup = 0
            local legacy_lookup = 0
            local repo = {{
              current_ticket_agent_session_uuids = function(ticket_id)
                current_lookup = current_lookup + 1
                assert(ticket_id == "ticket-1")
                return {{ "sess-current" }}
              end,
              ticket_session_uuids = function()
                legacy_lookup = legacy_lookup + 1
                return {{ "sess-current", "sess-stale" }}
              end,
              current_ticket_agent_session_uuids_by_ticket = function(ticket_ids)
                current_lookup = current_lookup + 1
                assert(ticket_ids[1] == "ticket-1")
                return {{
                  ["ticket-1"] = {{ "sess-current" }},
                }}, {{ ["sess-current"] = true }}
              end,
              ticket_session_uuids_by_ticket = function()
                legacy_lookup = legacy_lookup + 1
                return {{
                  ["ticket-1"] = {{ "sess-current", "sess-stale" }},
                }}, {{ ["sess-current"] = true, ["sess-stale"] = true }}
              end,
            }}

            local view = require("project_pipelines.web.ui")
            assert(view.ticket_notification_count("ticket-1", repo) == 1)
            local counts = view.ticket_notification_counts(repo, {{ "ticket-1" }})
            assert(counts["ticket-1"] == 1)
            assert(current_lookup == 2)
            assert(legacy_lookup == 0)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines notification counts should use current assigned agents");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_attention_items_include_questions_and_notified_current_agents()
{
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local frames = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  entity_snapshot = function(_self, entity_type, items, opts)
                    frames[entity_type] = {{ items = items, owner_plugin = opts.owner_plugin }}
                  end,
                }}
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              session_has_notification = function(session_uuid)
                return session_uuid == "sess-current"
              end,
            }}

            local db = {{}}
            function db:eval(sql, _params)
              local compact = (sql:gsub("%s+", " "))
              if compact:find("FROM questions q JOIN tickets t") then
                return {{
                  {{ id = "question-1", ticket_id = "ticket-1", ticket_title = "Question ticket", question = "Need a decision?", status = "open", blocking = 1, updated_at = 20 }},
                }}
              end
              if compact:find("FROM runs r JOIN run_steps rs") then
                return {{
                  {{ run_id = "run-1", ticket_id = "ticket-1", run_step_id = "run-step-current", agent_session_uuid = "sess-current", step_name = "Implement", ticket_title = "Notified ticket", run_updated_at = 30 }},
                  {{ run_id = "run-2", ticket_id = "ticket-2", run_step_id = "run-step-quiet", agent_session_uuid = "sess-quiet", step_name = "Review", ticket_title = "Quiet ticket", run_updated_at = 40 }},
                }}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.snapshot(entities.types.attention_item)

            local frame = frames[entities.types.attention_item]
            assert(frame.owner_plugin == "project-pipelines")
            assert(#frame.items == 2)

            local seen = {{}}
            for _, item in ipairs(frame.items) do
              seen[item.id] = item
            end
            assert(seen["question:question-1"])
            assert(seen["question:question-1"].badge_label == "blocking question")
            assert(seen["agent:run-step-current"])
            assert(seen["agent:run-step-current"].path == "/pipelines/tickets/ticket-1/sessions/sess-current")
            assert(not seen["agent:run-step-quiet"])
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines attention items should include open questions and notified current agents");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_run_screen_handles_stale_run_id_from_entities() {
    let run =
        std::fs::read_to_string(project_root_dir().join(
            "catalog/templates/plugins/project-pipelines/project_pipelines/web/screens/run.lua",
        ))
        .expect("read project pipelines run screen");

    assert!(
        run.contains(r#"source = "/project-pipelines.run""#)
            && run.contains("where = { id = run_id }")
            && run.contains("empty_template = view.panel")
            && run.contains("No run entity exists for run_id"),
        "run detail should render a stale/wrong run_id notice from an entity-backed filtered list"
    );
    assert!(
        run.contains(r#"ui.bind_if(run_path .. "/id""#),
        "run detail content should only render when the selected run entity exists"
    );
    assert!(
        !run.contains(r#"require("project_pipelines.repo")"#) && !run.contains("repo."),
        "run detail screen must not reintroduce project_pipelines.repo reads"
    );
}

#[test]
fn catalog_plugin_project_pipelines_entity_contract_covers_registered_types_and_screen_bindings() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let home_path = plugin_dir.join("project_pipelines/web/screens/home.lua");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            plugin = {{
              db = function()
                return {{ eval = function() return {{}} end }}
              end,
            }}

            local registered = {{}}
            package.loaded["lib.entity_broadcast"] = {{
              register = function(entity_type, opts)
                registered[#registered + 1] = entity_type
                assert(opts.id_field == "id")
              end,
            }}

            local contract = require("project_pipelines.entity_contract")
            local entities = require("project_pipelines.entities")
            assert(entities.types == contract.types)
            entities.register()

            local expected_types = {{}}
            local expected_type_count = 0
            for _, entity_type in pairs(contract.types) do
              expected_types[entity_type] = true
              expected_type_count = expected_type_count + 1
              assert(entity_type:match("^" .. contract.owner:gsub("%-", "%%-") .. "%."), entity_type)
            end
            for _, entity_type in ipairs(registered) do
              assert(expected_types[entity_type], "registered entity type missing from contract: " .. tostring(entity_type))
              expected_types[entity_type] = nil
            end
            assert(#registered == expected_type_count)
            for entity_type in pairs(expected_types) do
              error("contract entity type was not registered: " .. tostring(entity_type))
            end

            assert(contract.screens.home == contract.home_screen)

            local function read_file(path)
              local file = assert(io.open(path, "r"))
              local contents = file:read("*a")
              file:close()
              return contents
            end

            local function assert_screen_contract(screen_name, sections, path)
              assert(type(sections) == "table", screen_name .. " contract must be a table")
              local screen = read_file(path)

              local source_counts = {{}}
              local sections_by_source = {{}}
              local record_fields_by_source = {{}}
              for _, section in ipairs(sections) do
                assert(section.name and section.name ~= "", "screen contract section missing name: " .. screen_name)
                assert(section.source and section.source ~= "", "screen contract section missing source: " .. section.name)
                if section.mode == "record" then
                  record_fields_by_source[section.source] = record_fields_by_source[section.source] or {{}}
                  for _, field in ipairs(section.fields or {{}}) do
                    record_fields_by_source[section.source][field] = true
                  end
                else
                  sections_by_source[section.source] = sections_by_source[section.source] or {{}}
                  table.insert(sections_by_source[section.source], section)
                  source_counts[section.source] = (source_counts[section.source] or 0) + 1
                end
              end

              local function table_set(values)
                local set = {{}}
                for _, value in ipairs(values or {{}}) do
                  set[value] = true
                end
                return set
              end

              local function extract_call_blocks(source_text, call_name)
                local blocks = {{}}
                local start_at = 1
                while true do
                  local call_start, call_end = source_text:find(call_name .. "%s*%{{", start_at)
                  if not call_start then
                    break
                  end
                  local depth = 0
                  local block_end = nil
                  for index = call_end, #source_text do
                    local char = source_text:sub(index, index)
                    if char == "{{" then
                      depth = depth + 1
                    elseif char == "}}" then
                      depth = depth - 1
                      if depth == 0 then
                        block_end = index
                        break
                      end
                    end
                  end
                  assert(block_end, screen_name .. " unterminated " .. call_name .. " block")
                  table.insert(blocks, source_text:sub(call_start, block_end))
                  start_at = block_end + 1
                end
                return blocks
              end

              local actual_source_counts = {{}}
              for _, block in ipairs(extract_call_blocks(screen, "ui%.bind_list")) do
                local source = block:match('source%s*=%s*"([^"]+)"')
                assert(source, screen_name .. " bind_list missing source")
                actual_source_counts[source] = (actual_source_counts[source] or 0) + 1
                local section = sections_by_source[source] and sections_by_source[source][actual_source_counts[source]]
                assert(section, screen_name .. " bind_list source missing from contract: " .. source)
                local section_fields = table_set(section.fields)
                local section_where_fields = table_set(section.where_fields)

                local where_block = block:match("where%s*=%s*%{{(.-)%}}")
                if where_block then
                  for field in where_block:gmatch("([%w_]+)%s*=") do
                    assert(section_where_fields[field],
                      screen_name .. " where field missing from contract for " .. section.name .. ": " .. field)
                  end
                end
                for field in block:gmatch('ui%.bind%("%@/([%w_]+)"%)') do
                  assert(section_fields[field],
                    screen_name .. " bound field missing from contract for " .. section.name .. ": " .. field)
                end
                for field in block:gmatch('ui%.bind_if%("%@/([%w_]+)"') do
                  assert(section_fields[field],
                    screen_name .. " bind_if field missing from contract for " .. section.name .. ": " .. field)
                end
              end

              for source, expected_count in pairs(source_counts) do
                assert(actual_source_counts[source] == expected_count,
                  screen_name .. " source count drift for " .. source .. ": expected "
                    .. tostring(expected_count) .. " got " .. tostring(actual_source_counts[source]))
              end

              local path_vars = {{}}
              for var, source in screen:gmatch('local%s+([%w_]+_path)%s*=%s*"(/[^"]+)/"%s*%.%.') do
                path_vars[var] = source
              end
              for var, field in screen:gmatch('ui%.bind%(%s*([%w_]+_path)%s*%.%.%s*"%/([%w_]+)"%s*%)') do
                local source = path_vars[var]
                assert(source, screen_name .. " bound field path variable missing source: " .. var)
                assert(record_fields_by_source[source] and record_fields_by_source[source][field],
                  screen_name .. " bound record field missing from contract for " .. source .. ": " .. field)
              end
              for var, field in screen:gmatch('ui%.bind_if%(%s*([%w_]+_path)%s*%.%.%s*"%/([%w_]+)"') do
                local source = path_vars[var]
                assert(source, screen_name .. " bind_if path variable missing source: " .. var)
                assert(record_fields_by_source[source] and record_fields_by_source[source][field],
                  screen_name .. " bound record bind_if field missing from contract for " .. source .. ": " .. field)
              end
              for path in screen:gmatch('ui%.bind%(%s*"(/[^"]+)"%s*%)') do
                local source = path:match("^(/[^/]+)")
                local field = path:match("/([%w_]+)$")
                if source and field then
                  assert(record_fields_by_source[source] and record_fields_by_source[source][field],
                    screen_name .. " literal bound record field missing from contract for " .. source .. ": " .. field)
                end
              end
            end

            assert_screen_contract("home", contract.screens.home, "{home_path}")
            assert_screen_contract("pipelines", contract.screens.pipelines, "{pipelines_path}")
            assert_screen_contract("project", contract.screens.project, "{project_path}")
            assert_screen_contract("ticket", contract.screens.ticket, "{ticket_path}")
            assert_screen_contract("run", contract.screens.run, "{run_path}")

            local function assert_repo_rendered_screen(screen_name, path)
              local metadata = assert(contract.repo_rendered_screens and contract.repo_rendered_screens[screen_name],
                screen_name .. " repo-rendered screen missing contract metadata")
              assert(type(metadata.reason) == "string" and metadata.reason ~= "",
                screen_name .. " repo-rendered metadata needs a reason")
              assert(type(metadata.repo_calls) == "table" and #metadata.repo_calls > 0,
                screen_name .. " repo-rendered metadata needs repo_calls")
              assert(type(metadata.migration_sources) == "table" and #metadata.migration_sources > 0,
                screen_name .. " repo-rendered metadata needs migration_sources")
              local screen = read_file(path)
              if contract.screens[screen_name] == nil then
                assert(not screen:match("ui%.bind_list%s*%{{"),
                  screen_name .. " has bind_list rows; move migrated sections into contract.screens")
              end
              for _, call in ipairs(metadata.repo_calls) do
                assert(screen:match("repo%." .. call .. "%s*%("),
                  screen_name .. " repo-rendered metadata lists missing repo call: " .. call)
              end
              for _, source in ipairs(metadata.migration_sources) do
                assert(source:match("^/" .. contract.owner:gsub("%-", "%%-") .. "%."),
                  screen_name .. " migration source should be a project-pipelines entity path: " .. tostring(source))
              end
            end

            assert_repo_rendered_screen("new", "{new_path}")

            return "ok"
            "#,
            plugin_dir = plugin_dir.display(),
            home_path = home_path.display(),
            pipelines_path = plugin_dir
                .join("project_pipelines/web/screens/pipelines.lua")
                .display(),
            project_path = plugin_dir
                .join("project_pipelines/web/screens/project.lua")
                .display(),
            ticket_path = plugin_dir
                .join("project_pipelines/web/screens/ticket.lua")
                .display(),
            new_path = plugin_dir
                .join("project_pipelines/web/screens/new.lua")
                .display(),
            run_path = plugin_dir
                .join("project_pipelines/web/screens/run.lua")
                .display()
        ))
        .eval()
        .expect("Project Pipelines entity contract should cover registered types and screen bindings");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_run_snapshot_bounds_relationship_lookups() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local frames = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  entity_snapshot = function(_self, entity_type, items, opts)
                    frames[#frames + 1] = {{ entity_type = entity_type, items = items, owner_plugin = opts.owner_plugin }}
                  end,
                }}
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              status_tone = function() return "muted" end,
            }}

            local expected_ids = {{
              tickets = {{ ["ticket-1"] = true, ["ticket-2"] = true }},
              projects = {{ ["project-1"] = true }},
              pipelines = {{ ["pipe-1"] = true, ["pipe-2"] = true }},
              pipeline_steps = {{ ["step-1"] = true, ["step-2"] = true }},
              run_steps = {{ ["run-step-1"] = true }},
            }}
            local bounded = {{ tickets = false, projects = false, pipelines = false, pipeline_steps = false }}

            local function normalize(sql)
              return (sql:gsub("%s+", " "))
            end

            local function assert_bound(name, sql, params)
              sql = normalize(sql)
              assert(sql:find(" WHERE id IN %("), name .. " lookup must be scoped by referenced ids: " .. sql)
              assert(type(params) == "table", name .. " lookup must bind ids")
              for _, id in ipairs(params) do
                assert(expected_ids[name][id], name .. " lookup loaded unreferenced id: " .. tostring(id))
              end
              bounded[name] = true
            end

            local db = {{}}
            function db:eval(sql, params)
              local compact = normalize(sql)
              if compact:find("FROM runs ORDER BY") then
                return {{
                  {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "pipe-1", status = "active", current_step_id = "step-1", current_run_step_id = "run-step-1", created_at = 1, updated_at = 2 }},
                  {{ id = "run-2", ticket_id = "ticket-2", pipeline_id = "pipe-2", status = "done", current_step_id = "step-2", created_at = 3, updated_at = 4 }},
                }}
              end
              if compact:find("FROM tickets") then
                assert_bound("tickets", sql, params)
                return {{
                  {{ id = "ticket-1", title = "Ticket One", project_id = "project-1" }},
                  {{ id = "ticket-2", title = "Ticket Two" }},
                }}
              end
              if compact:find("FROM projects") then
                assert_bound("projects", sql, params)
                return {{ {{ id = "project-1" }} }}
              end
              if compact:find("FROM pipelines") then
                assert_bound("pipelines", sql, params)
                return {{
                  {{ id = "pipe-1", name = "Primary" }},
                  {{ id = "pipe-2", name = "Secondary" }},
                }}
              end
              if compact:find("FROM pipeline_steps") then
                assert_bound("pipeline_steps", sql, params)
                return {{
                  {{ id = "step-1", name = "Implement" }},
                  {{ id = "step-2", name = "Review" }},
                }}
              end
              if compact:find("FROM run_steps") then
                assert_bound("run_steps", sql, params)
                return {{ {{ id = "run-step-1", agent_session_uuid = "sess-1" }} }}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.snapshot(entities.types.run)

            assert(#frames == 1)
            assert(frames[1].entity_type == entities.types.run)
            assert(frames[1].owner_plugin == "project-pipelines")
            assert(#frames[1].items == 2)
            assert(frames[1].items[1].ticket_title == "Ticket One")
            assert(frames[1].items[1].pipeline_name == "Primary")
            assert(frames[1].items[1].current_step_name == "Implement")
            assert(frames[1].items[1].current_agent_session_uuid == "sess-1")
            assert(frames[1].items[2].ticket_title == "Ticket Two")
            assert(frames[1].items[2].pipeline_name == "Secondary")
            assert(frames[1].items[2].current_step_name == "Review")

            for name, was_bounded in pairs(bounded) do
              assert(was_bounded, name .. " relationship lookup was not exercised")
            end

            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines run entity relationship lookup bounds");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_default_snapshots_use_visible_working_set() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local frames = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  entity_snapshot = function(_self, entity_type, items, opts)
                    frames[entity_type] = {{ items = items, owner_plugin = opts.owner_plugin }}
                  end,
                }}
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              target_label = function(target_id) return target_id or "No target" end,
              status_tone = function() return "muted" end,
              status_label = function(status) return tostring(status or "") end,
              status_state = function() return "neutral" end,
              ticket_notification_counts = function() return {{}} end,
            }}

            local saw = {{
              tickets = false,
              projects = false,
              project_targets = false,
              dependencies = false,
              questions = false,
            }}

            local function normalize(sql)
              return (sql:gsub("%s+", " "))
            end

            local db = {{}}
            function db:eval(sql, _params)
              local compact = normalize(sql)
              if compact:find("FROM tickets t LEFT JOIN projects p") then
                saw.tickets = compact:find("COALESCE%(t%.status, 'open'%) != 'closed'") ~= nil
                  and compact:find("COALESCE%(p%.status, 'open'%) != 'closed'") ~= nil
                return {{
                  {{ id = "ticket-open", title = "Open ticket", status = "open", target_id = "target-1", created_at = 1, updated_at = 2 }},
                }}
              end
              if compact:find("FROM projects WHERE COALESCE%(status, 'open'%) != 'closed'") then
                saw.projects = true
                return {{
                  {{ id = "project-open", name = "Open project", status = "open" }},
                }}
              end
              if compact:find("FROM project_targets pt LEFT JOIN projects p") then
                saw.project_targets = compact:find("COALESCE%(p%.status, 'open'%) != 'closed'") ~= nil
                  and compact:find("pt%.project_id IS NULL") ~= nil
                return {{
                  {{ id = "target-row", project_id = "project-open", target_id = "target-1" }},
                }}
              end
              if compact:find("FROM ticket_dependencies td JOIN tickets t") then
                saw.dependencies = compact:find("COALESCE%(t%.status, 'open'%) != 'closed'") ~= nil
                return {{
                  {{ id = "dep-1", ticket_id = "ticket-open", depends_on_ticket_id = "ticket-dep", depends_on_title = "Dependency", depends_on_status = "open" }},
                }}
              end
              if compact:find("FROM questions q JOIN tickets t") then
                saw.questions = compact:find("q.status = 'open'") ~= nil
                  and compact:find("COALESCE%(t%.status, 'open'%) != 'closed'") ~= nil
                return {{
                  {{ id = "question-open", ticket_id = "ticket-open", question = "Proceed?", status = "open", blocking = 1 }},
                }}
              end
              if compact:find("FROM tickets WHERE id = %? LIMIT 1") then
                return {{ {{ id = "ticket-open", title = "Open ticket" }} }}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.snapshot(entities.types.ticket)
            entities.snapshot(entities.types.project)
            entities.snapshot(entities.types.project_target)
            entities.snapshot(entities.types.ticket_dependency)
            entities.snapshot(entities.types.question)

            assert(frames[entities.types.ticket].owner_plugin == "project-pipelines")
            assert(#frames[entities.types.ticket].items == 1)
            assert(frames[entities.types.ticket].items[1].id == "ticket-open")
            assert(#frames[entities.types.project].items == 1)
            assert(frames[entities.types.project].items[1].id == "project-open")
            assert(#frames[entities.types.project_target].items == 1)
            assert(#frames[entities.types.ticket_dependency].items == 1)
            assert(#frames[entities.types.question].items == 1)

            for key, value in pairs(saw) do
              assert(value, "visible working-set query was not used for " .. key)
            end
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines visible working-set snapshots");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_visibility_upsert_removes_hidden_default_entities() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local removes = {{}}
            local upserts = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  entity_remove = function(_self, entity_type, id, opts)
                    removes[#removes + 1] = {{ entity_type = entity_type, id = id, opts = opts }}
                  end,
                  entity_upsert = function(_self, entity_type, entity, opts)
                    upserts[#upserts + 1] = {{ entity_type = entity_type, entity = entity, opts = opts }}
                  end,
                }}
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              target_label = function(target_id) return target_id or "No target" end,
              status_tone = function() return "muted" end,
              status_label = function(status) return tostring(status or "") end,
              status_state = function() return "neutral" end,
              ticket_notification_count = function() return 0 end,
            }}

            package.loaded["project_pipelines.db"] = {{
              eval = function(_self, sql, param)
                if sql:match("FROM projects WHERE id = %? LIMIT 1") then
                  if param == "closed-project" then
                    return {{ {{ id = "closed-project", status = "closed" }} }}
                  end
                  return {{ {{ id = param, status = "open" }} }}
                end
                if sql:match("SELECT id, status, project_id FROM tickets WHERE id = %? LIMIT 1") then
                  if param == "closed-ticket" then
                    return {{ {{ id = "closed-ticket", status = "closed" }} }}
                  end
                  return {{ {{ id = param, status = "open" }} }}
                end
                return {{}}
              end,
            }}

            local entities = require("project_pipelines.entities")
            entities.upsert(entities.types.ticket, {{
              id = "closed-ticket",
              title = "Closed",
              status = "closed",
            }})
            entities.upsert(entities.types.project, {{
              id = "closed-project",
              name = "Closed",
              status = "closed",
            }})
            entities.upsert(entities.types.project_target, {{
              id = "target-closed",
              project_id = "closed-project",
              target_id = "target-1",
            }})
            entities.upsert(entities.types.question, {{
              id = "question-answered",
              ticket_id = "open-ticket",
              status = "answered",
            }})

            assert(#upserts == 0)
            assert(#removes == 4)
            assert(removes[1].entity_type == entities.types.ticket and removes[1].id == "closed-ticket")
            assert(removes[2].entity_type == entities.types.project and removes[2].id == "closed-project")
            assert(removes[3].entity_type == entities.types.project_target and removes[3].id == "target-closed")
            assert(removes[4].entity_type == entities.types.question and removes[4].id == "question-answered")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines hidden upserts should publish removes");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_keeps_run_snapshots_complete_for_direct_routes() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local frames = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  entity_snapshot = function(_self, entity_type, items, opts)
                    frames[entity_type] = {{ items = items, opts = opts }}
                  end,
                }}
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              status_tone = function() return "muted" end,
              status_label = function(status) return tostring(status or "") end,
              status_state = function() return "neutral" end,
            }}

            local saw_complete_run_query = false
            local saw_complete_run_step_query = false
            local db = {{}}
            function db:eval(sql, param)
              local compact = (sql:gsub("%s+", " "))
              if compact == "SELECT * FROM runs ORDER BY updated_at DESC, created_at DESC, id DESC" then
                saw_complete_run_query = true
                return {{
                  {{ id = "run-closed-ticket", ticket_id = "closed-ticket", pipeline_id = "pipeline-1", status = "blocked" }},
                }}
              end
              if compact == "SELECT * FROM run_steps ORDER BY run_id ASC, COALESCE(sequence, 0) ASC, created_at ASC, id ASC" then
                saw_complete_run_step_query = true
                return {{
                  {{ id = "step-closed-ticket", run_id = "run-closed-ticket", status = "blocked" }},
                }}
              end
              if compact:find("FROM tickets WHERE id = %? LIMIT 1") then
                return {{ {{ id = param, title = "Closed ticket", status = "closed", project_id = "closed-project" }} }}
              end
              if compact:find("FROM projects WHERE id = %? LIMIT 1") then
                return {{ {{ id = param, name = "Closed project", status = "closed" }} }}
              end
              if compact:find("FROM pipelines WHERE id = %? LIMIT 1") then
                return {{ {{ id = param, name = "Pipeline" }} }}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.snapshot(entities.types.run)
            entities.snapshot(entities.types.run_step)

            assert(saw_complete_run_query)
            assert(saw_complete_run_step_query)
            assert(#frames[entities.types.run].items == 1)
            assert(frames[entities.types.run].items[1].id == "run-closed-ticket")
            assert(#frames[entities.types.run_step].items == 1)
            assert(frames[entities.types.run_step].items[1].id == "step-closed-ticket")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines run snapshots should keep direct-route rows");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_default_publish_excludes_detail_history_families() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local frames = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  entity_snapshot = function(_self, entity_type, items, opts)
                    frames[#frames + 1] = {{ entity_type = entity_type, items = items, opts = opts }}
                  end,
                }}
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              status_tone = function() return "muted" end,
              status_label = function(status) return tostring(status or "") end,
              status_state = function() return "neutral" end,
              ticket_notification_count = function() return 0 end,
            }}

            local forbidden = {{
              run_steps = "run_step",
              reviews = "review",
              review_findings = "finding",
              gate_results = "gate_result",
              artifacts = "artifact",
              events = "event",
              project_targets = "project_target",
              ticket_dependencies = "ticket_dependency",
              pr_links = "pr_link",
              checklists = "checklist",
              checklist_items = "checklist_item",
            }}
            local db = {{}}
            function db:eval(sql, _params)
              for table_name, entity_name in pairs(forbidden) do
                if sql:find("FROM " .. table_name) then
                  error("default publish should not query " .. entity_name .. ": " .. sql)
                end
              end
              if sql:find("FROM tickets") then return {{}} end
              if sql:find("FROM projects") then return {{}} end
              if sql:find("FROM pipelines") then return {{}} end
              if sql:find("FROM questions") then return {{}} end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.publish_snapshots()

            local seen = {{}}
            for _, frame in ipairs(frames) do
              seen[frame.entity_type] = true
            end
            assert(seen[entities.types.ticket])
            assert(seen[entities.types.project])
            assert(seen[entities.types.pipeline])
            assert(seen[entities.types.question])
            assert(not seen[entities.types.run])
            assert(not seen[entities.types.run_step])
            assert(not seen[entities.types.pipeline_step])
            assert(not seen[entities.types.pipeline_gate])
            assert(not seen[entities.types.project_target])
            assert(not seen[entities.types.ticket_dependency])
            assert(not seen[entities.types.review])
            assert(not seen[entities.types.finding])
            assert(not seen[entities.types.gate_result])
            assert(not seen[entities.types.artifact])
            assert(not seen[entities.types.pr_link])
            assert(not seen[entities.types.checklist])
            assert(not seen[entities.types.checklist_item])
            assert(not seen[entities.types.event])
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines default publish should stay bounded");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_registers_targeted_detail_hydration() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local registered = {{}}
            package.loaded["lib.entity_broadcast"] = {{
              register = function(entity_type, opts)
                registered[entity_type] = opts
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              status_tone = function() return "muted" end,
              status_label = function(status) return tostring(status or "") end,
              status_state = function() return "neutral" end,
              ticket_notification_count = function() return 0 end,
            }}

            local function normalize(sql)
              return (sql:gsub("%s+", " "))
            end

            local saw = {{ run = false, run_step = false, review = false, pr_link = false }}
            local db = {{}}
            function db:eval(sql, param)
              local compact = normalize(sql)
              if compact == "SELECT * FROM runs WHERE id = ? LIMIT 1" then
                saw.run = param == "historical-run"
                return {{ {{ id = "historical-run", ticket_id = "closed-ticket", pipeline_id = "pipeline-1", status = "blocked" }} }}
              end
              if compact:find("FROM run_steps WHERE run_id = %?") then
                saw.run_step = param == "historical-run"
                return {{ {{ id = "step-1", run_id = "historical-run", step_id = "pipeline-step-1", status = "blocked", sequence = 1 }} }}
              end
              if compact == "SELECT * FROM reviews WHERE run_id = ? ORDER BY created_at ASC, id ASC" then
                saw.review = param == "historical-run"
                return {{ {{ id = "review-1", run_id = "historical-run", verdict = "needs_work" }} }}
              end
              if compact:find("FROM pr_links WHERE ticket_id = %?") then
                saw.pr_link = param == "open-ticket"
                return {{ {{ id = "pr-1", ticket_id = "open-ticket", pr_url = "https://example.com/pull/1", status = "open" }} }}
              end
              if compact:find("SELECT id, title, project_id FROM tickets WHERE id IN") then
                return {{ {{ id = "closed-ticket", title = "Closed ticket", project_id = "closed-project" }} }}
              end
              if compact:find("SELECT id FROM projects WHERE id IN") then
                return {{ {{ id = "closed-project" }} }}
              end
              if compact:find("SELECT id, name FROM pipelines WHERE id IN") then
                return {{ {{ id = "pipeline-1", name = "Pipeline" }} }}
              end
              if compact:find("FROM tickets WHERE id = %? LIMIT 1") then
                if param == "open-ticket" then
                  return {{ {{ id = param, title = "Open ticket", status = "open", project_id = "" }} }}
                end
                return {{ {{ id = param, title = "Closed ticket", status = "closed", project_id = "closed-project" }} }}
              end
              if compact:find("FROM projects WHERE id = %? LIMIT 1") then
                return {{ {{ id = param, name = "Closed project", status = "closed" }} }}
              end
              if compact:find("FROM pipelines WHERE id = %? LIMIT 1") then
                return {{ {{ id = param, name = "Pipeline" }} }}
              end
              if compact:find("FROM pipeline_steps WHERE id = %? LIMIT 1") then
                return {{ {{ id = param, name = "Implement", kind = "agent" }} }}
              end
              if compact:find("FROM pipeline_steps") then
                return {{ {{ id = "pipeline-step-1", name = "Implement", kind = "agent" }} }}
              end
              if compact:find("SELECT id, ticket_id, pipeline_id FROM runs") then
                return {{ {{ id = "historical-run", ticket_id = "closed-ticket", pipeline_id = "pipeline-1" }} }}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.register()

            local run_items = registered[entities.types.run].query({{ id = "historical-run" }})
            local step_items = registered[entities.types.run_step].query({{ where = {{ run_id = "historical-run" }} }})
            local review_items = registered[entities.types.review].query({{ where = {{ run_id = "historical-run" }} }})
            local pr_link_items = registered[entities.types.pr_link].query({{ where = {{ ticket_id = "open-ticket", has_pr_url = true }} }})

            assert(saw.run)
            assert(saw.run_step)
            assert(saw.review)
            assert(saw.pr_link)
            assert(#run_items == 1 and run_items[1].id == "historical-run")
            assert(run_items[1].ticket_title == "Closed ticket")
            assert(#step_items == 1 and step_items[1].run_id == "historical-run")
            assert(step_items[1].ticket_id == "closed-ticket")
            assert(#review_items == 1 and review_items[1].id == "review-1")
            assert(#pr_link_items == 1 and pr_link_items[1].id == "pr-1")
            assert(pr_link_items[1].has_pr_url == true)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines targeted detail hydration");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_registers_targeted_overview_hydration() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local registered = {{}}
            package.loaded["lib.entity_broadcast"] = {{
              register = function(entity_type, opts)
                registered[entity_type] = opts
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              status_tone = function() return "muted" end,
              status_label = function(status) return tostring(status or "") end,
              status_state = function() return "neutral" end,
              ticket_notification_count = function() return 0 end,
            }}

            local function normalize(sql)
              return (sql:gsub("%s+", " "))
            end

            local db = {{}}
            function db:eval(sql, param)
              local compact = normalize(sql)
              if compact:find("FROM tickets t LEFT JOIN projects p") then
                return {{
                  {{ id = "ticket-merge", title = "Ready", status = "open", project_id = "project-1", target_id = "target-1", created_at = 1 }},
                  {{ id = "ticket-standalone", title = "Standalone", status = "open", project_id = "", target_id = "target-1", created_at = 2 }},
                }}
              end
              if compact == "SELECT * FROM runs WHERE status = ? ORDER BY updated_at DESC, created_at DESC, id DESC" then
                return {{
                  {{ id = "run-active", ticket_id = "ticket-standalone", pipeline_id = "pipeline-1", status = "active" }},
                }}
              end
              if compact == "SELECT * FROM runs ORDER BY updated_at DESC, created_at DESC, id DESC" then
                return {{
                  {{ id = "run-active", ticket_id = "ticket-standalone", pipeline_id = "pipeline-1", status = "active" }},
                  {{ id = "run-done", ticket_id = "ticket-merge", pipeline_id = "pipeline-1", status = "done" }},
                }}
              end
              if compact:find("SELECT %* FROM runs WHERE ticket_id IN") then
                return {{
                  {{ id = "run-done", ticket_id = "ticket-merge", pipeline_id = "pipeline-1", status = "done" }},
                  {{ id = "run-active", ticket_id = "ticket-standalone", pipeline_id = "pipeline-1", status = "active" }},
                }}
              end
              if compact:find("SELECT id, title, project_id FROM tickets WHERE id IN") then
                return {{
                  {{ id = "ticket-standalone", title = "Standalone", project_id = "" }},
                  {{ id = "ticket-merge", title = "Ready", project_id = "project-1" }},
                }}
              end
              if compact:find("SELECT id FROM projects WHERE id IN") then
                return {{ {{ id = "project-1" }} }}
              end
              if compact:find("SELECT id, name FROM pipelines WHERE id IN") then
                return {{ {{ id = "pipeline-1", name = "Pipeline" }} }}
              end
              if compact == "SELECT * FROM projects WHERE COALESCE(status, 'open') = ? ORDER BY updated_at DESC, created_at DESC" then
                return {{ {{ id = "project-1", name = "Project", status = "open" }} }}
              end
              if compact == "SELECT * FROM projects WHERE COALESCE(status, 'open') != 'closed' ORDER BY updated_at DESC, created_at DESC" then
                return {{ {{ id = "project-1", name = "Project", status = "open" }} }}
              end
              if compact:find("FROM questions q") then
                return {{ {{ id = "question-1", ticket_id = "ticket-standalone", status = "open", question = "Proceed?", kind = "human" }} }}
              end
              if compact:find("SELECT id, title FROM tickets WHERE id IN") then
                return {{ {{ id = "ticket-standalone", title = "Standalone" }} }}
              end
              if compact:find("FROM ticket_dependencies") then
                return {{}}
              end
              if compact:find("FROM events") then
                return {{}}
              end
              if compact:find("FROM pr_links") then
                return {{}}
              end
              if compact:find("FROM pipeline_steps") then
                return {{}}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.register()

            local ticket_items = registered[entities.types.ticket].query({{ where = {{ status = "open", standalone = true }} }})
            local merge_items = registered[entities.types.ticket].query({{ where = {{ status = "open", latest_run_status = "done" }} }})
            local run_items = registered[entities.types.run].query({{ where = {{ status = "active" }} }})
            local project_items = registered[entities.types.project].query({{ where = {{ status = "open" }} }})
            local question_items = registered[entities.types.question].query({{ where = {{ status = "open" }} }})

            local saw_standalone = false
            local saw_done = false
            for _, item in ipairs(ticket_items) do
              if item.standalone == true then saw_standalone = true end
            end
            for _, item in ipairs(merge_items) do
              if item.latest_run_status == "done" then saw_done = true end
            end

            assert(#ticket_items >= 1, "overview ticket scoped query should return working-set ticket rows")
            assert(saw_standalone, "ticket entities should expose standalone for EB scope filtering")
            assert(#merge_items >= 1, "overview merge scoped query should return decorated ticket rows")
            assert(saw_done, "ticket entities should expose latest_run_status for EB scope filtering")
            assert(#run_items >= 1 and run_items[1].status == "active")
            assert(#project_items == 1 and project_items[1].status == "open")
            assert(#question_items == 1 and question_items[1].status == "open")
            assert(question_items[1].ticket_title == "Standalone")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("Project Pipelines targeted overview hydration");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_run_entities_decorate_relationship_and_agent_fields() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local upserts = {{}}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  entity_upsert = function(_self, entity_type, entity, opts)
                    upserts[#upserts + 1] = {{ entity_type = entity_type, entity = entity, opts = opts }}
                  end,
                }}
              end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              status_tone = function(status)
                return status == "blocked" and "danger" or "muted"
              end,
              session_has_notification = function(session_uuid)
                return session_uuid == "sess-agent"
              end,
            }}
            package.loaded["project_pipelines.db"] = {{
              eval = function(_self, sql, param)
                if sql:match("FROM tickets") then
                  if param == "ticket-1" then
                    return {{ {{ id = "ticket-1", title = "Implement pipelines", project_id = "project-1" }} }}
                  end
                  return {{}}
                end
                if sql:match("FROM projects") then
                  if param == "project-1" then
                    return {{ {{ id = "project-1" }} }}
                  end
                  return {{}}
                end
                if sql:match("FROM pipelines") then
                  if param == "pipeline-1" then
                    return {{ {{ id = "pipeline-1", name = "Default pipeline" }} }}
                  end
                  return {{}}
                end
                if sql:match("FROM pipeline_steps") then
                  if param == "step-1" then
                    return {{ {{ id = "step-1", name = "Implement" }} }}
                  end
                  return {{}}
                end
                if sql:match("FROM run_steps") then
                  if param == "run-step-1" then
                    return {{ {{ id = "run-step-1", agent_session_uuid = "sess-agent" }} }}
                  end
                  return {{}}
                end
                return {{}}
              end,
            }}

            local entities = require("project_pipelines.entities")
            entities.upsert(entities.types.run, {{
              id = "run-1",
              ticket_id = "ticket-1",
              pipeline_id = "pipeline-1",
              current_step_id = "step-1",
              current_run_step_id = "run-step-1",
              status = "active",
            }})

            local decorated = upserts[1].entity
            assert(upserts[1].entity_type == "project-pipelines.run")
            assert(upserts[1].opts.owner_plugin == "project-pipelines")
            assert(decorated.ticket_title == "Implement pipelines")
            assert(decorated.pipeline_name == "Default pipeline")
            assert(decorated.current_step_name == "Implement")
            assert(decorated.detail_label == "Default pipeline - current step: Implement")
            assert(decorated.label == "Implement pipelines - Default pipeline (active)")
            assert(decorated.path == "/pipelines/runs/run-1")
            assert(decorated.has_ticket == true)
            assert(decorated.ticket_path == "/pipelines/tickets/ticket-1")
            assert(decorated.has_project == true)
            assert(decorated.project_path == "/pipelines/projects/project-1")
            assert(decorated.current_agent_session_uuid == "sess-agent")
            assert(decorated.has_current_agent == true)
            assert(decorated.current_agent_has_notification == true)
            assert(decorated.current_agent_button_label == "Current agent needs attention")
            assert(decorated.current_agent_button_tone == "danger")
            assert(decorated.current_agent_path == "/pipelines/tickets/ticket-1/sessions/sess-agent")

            entities.upsert(entities.types.run, {{
              id = "run-stale-refs",
              ticket_id = "ticket-missing",
              pipeline_id = "pipeline-missing",
              status = "blocked",
            }})

            local fallback = upserts[2].entity
            assert(fallback.ticket_title == "ticket-missing")
            assert(fallback.pipeline_name == "pipeline-missing")
            assert(fallback.current_step_name == "No current step")
            assert(fallback.has_ticket == false)
            assert(fallback.has_project == false)
            assert(fallback.has_current_agent == false)
            assert(fallback.current_agent_has_notification == false)
            assert(fallback.current_agent_button_label == "Current agent")
            assert(fallback.current_agent_button_tone == "accent")
            assert(fallback.current_agent_path == nil)
            assert(fallback.status_tone == "danger")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display(),
        ))
        .eval()
        .expect("Project Pipelines run entities should decorate relationship and agent fields");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_mcp_mutators_return_without_bulk_snapshot_publish() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local handlers = {{}}
            local publish_snapshots = 0

            mcp = {{
              tool = function(name, _spec, handler)
                handlers[name] = handler
              end,
              prompt = function(_name, _spec, _handler) end,
            }}

            package.loaded["project_pipelines.repo"] = setmetatable({{
              prune_legacy_seed_data = function() end,
              update_ticket = function(ticket_id, fields)
                assert(ticket_id == "ticket-1")
                assert(fields.title == "Updated")
                return {{ id = ticket_id, title = fields.title }}
              end,
            }}, {{
              __index = function()
                return function() return {{}} end
              end,
            }})

            package.loaded["project_pipelines.engine"] = setmetatable({{
              publish_entity_snapshots = function()
                publish_snapshots = publish_snapshots + 1
                error("MCP mutators must not publish full entity snapshots synchronously")
              end,
            }}, {{
              __index = function()
                return function() return {{}} end
              end,
            }})

            package.loaded["lib.config_resolver"] = {{
              list_agents = function() return {{}} end,
            }}

            require("project_pipelines.mcp").register()

            local result = handlers.project_pipelines_update_ticket({{
              ticket_id = "ticket-1",
              title = "Updated",
            }}, {{}})
            assert(result.ok == true)
            assert(result.result.id == "ticket-1")
            assert(publish_snapshots == 0)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines MCP mutators should not publish bulk snapshots synchronously");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_repo_publishes_targeted_entity_deltas() {
    let root = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let repo = std::fs::read_to_string(root.join("project_pipelines/repo.lua"))
        .expect("read project pipelines repo");

    assert!(
        !repo.contains("publish_entity_snapshot(\"ticket\")"),
        "ticket mutations should publish targeted ticket entity upserts, not full ticket snapshots"
    );
    assert!(
        repo.contains("local function publish_ticket_project_family"),
        "dependency mutations should republish only the affected ticket family"
    );
    assert!(
        repo.contains("publish_entity(\"pipeline_step\", M.get_step(step_id))"),
        "pipeline creation should upsert created step entities after transitions are persisted"
    );
    assert!(
        repo.contains("publish_entity(\"pipeline_gate\", decode_gate_row(M.get_gate(gate_id)))"),
        "pipeline and step creation should upsert created gate entities"
    );
    assert!(
        repo.contains("remove_entity(\"pipeline_gate\", gate_id)")
            && repo.contains("remove_entity(\"pipeline_step\", step_id)")
            && repo.contains("remove_entity(\"pipeline\", pipeline_id)"),
        "pipeline deletion should remove child gate and step entities before the pipeline entity"
    );
}

#[test]
fn catalog_plugin_project_pipelines_submit_review_reminds_current_step_to_advance() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local handlers = {{}}
            local create_review_args = nil
            local run = {{
              id = "run-1",
              current_step_id = "review",
              current_run_step_id = "visit-review",
            }}

            mcp = {{
              tool = function(name, _spec, handler)
                handlers[name] = handler
              end,
              prompt = function(_name, _spec, _handler) end,
            }}

            package.loaded["project_pipelines.repo"] = setmetatable({{
              prune_legacy_seed_data = function() end,
              get_run = function(run_id)
                assert(run_id == "run-1")
                return run
              end,
              create_review = function(params)
                create_review_args = params
                return {{
                  id = "review-1",
                  run_id = params.run_id,
                  run_step_id = params.run_step_id or run.current_run_step_id,
                  step_id = params.step_id,
                  reviewer_session_uuid = params.reviewer_session_uuid,
                  verdict = params.verdict,
                  summary = params.summary or "",
                }}
              end,
            }}, {{
              __index = function()
                return function() return {{}} end
              end,
            }})

            package.loaded["project_pipelines.engine"] = setmetatable({{}}, {{
              __index = function()
                return function() return {{}} end
              end,
            }})

            package.loaded["lib.config_resolver"] = {{
              list_agents = function() return {{}} end,
            }}

            require("project_pipelines.mcp").register()

            local result = handlers.project_pipelines_submit_review({{
              run_id = "run-1",
              step_id = "review",
              verdict = "approved",
            }}, {{ session_uuid = "sess-reviewer" }})

            assert(create_review_args.reviewer_session_uuid == "sess-reviewer")
            assert(result.ok == true)
            assert(result.result.review.id == "review-1")
            assert(result.result.review.run_step_id == "visit-review")
            assert(result.result.requires_advance == true)
            assert(result.result.next_tool == "project_pipelines_request_step_advance")
            assert(result.result.next_tool_params.run_id == "run-1")
            assert(result.result.next_tool_params.evidence.review_id == "review-1")
            assert(string.find(result.result.message, "does not advance the pipeline", 1, true) ~= nil)

            run.current_step_id = "implement"
            local historical = handlers.project_pipelines_submit_review({{
              run_id = "run-1",
              step_id = "review",
              verdict = "changes_required",
            }}, {{ session_uuid = "sess-reviewer" }})

            assert(historical.ok == true)
            assert(historical.result.requires_advance == false)
            assert(historical.result.next_tool == nil)
            assert(historical.result.reason == "review_not_current_step")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines submit review should return explicit advancement guidance");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_start_run_queues_agent_and_links_later_by_request_id() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local create_agent_calls = 0
            local events = {{}}
            local run = nil
            local visit = nil

            local ticket = {{
              id = "ticket-1",
              title = "Async spawn",
              target_id = "target-1",
            }}
            local pipeline = {{ id = "pipe-1", name = "Default" }}
            local step = {{
              id = "step-1",
              kind = "agent",
              name = "Implement",
              agent_name = "codex",
              prompt = "Do the work",
            }}

            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}

            package.loaded["project_pipelines.repo"] = {{
              prune_legacy_seed_data = function() end,
              get_ticket = function(id)
                assert(id == "ticket-1")
                return ticket
              end,
              open_ticket_run = function() return nil end,
              blocking_ticket_dependencies = function() return {{}} end,
              closed_ticket_dependencies = function() return {{}} end,
              ticket_dependencies = function() return {{}} end,
              get_pipeline = function(id)
                assert(id == "pipe-1")
                return pipeline
              end,
              pipeline_steps = function(id)
                assert(id == "pipe-1")
                return {{ step }}
              end,
              create_run = function(attrs)
                run = {{
                  id = "run-1",
                  ticket_id = attrs.ticket_id,
                  pipeline_id = attrs.pipeline_id,
                  target_id = attrs.target_id,
                  workspace_id = attrs.workspace_id,
                  workspace_name = attrs.workspace_name,
                  base_ticket_id = attrs.base_ticket_id,
                  base_run_id = attrs.base_run_id,
                  base_ref = attrs.base_ref,
                  base_target_path = attrs.base_target_path,
                  status = "queued",
                }}
                return run
              end,
              next_step = function(seen_run)
                assert(seen_run.id == "run-1")
                return step
              end,
              create_run_step_visit = function(run_id, step_id, attrs)
                assert(run_id == "run-1")
                assert(step_id == "step-1")
                visit = {{
                  id = "visit-1",
                  run_id = run_id,
                  step_id = step_id,
                  status = attrs.status,
                  sequence = 1,
                }}
                return visit
              end,
              update_run = function(run_id, attrs)
                assert(run_id == "run-1")
                for key, value in pairs(attrs or {{}}) do
                  run[key] = value
                end
                return run
              end,
              get_run = function(run_id)
                assert(run_id == "run-1")
                return run
              end,
              get_run_step_visit = function(visit_id)
                assert(visit_id == "visit-1")
                return visit
              end,
              latest_step_session = function() return nil end,
              append_event = function(kind, event)
                events[#events + 1] = {{ kind = kind, event = event }}
              end,
              update_run_step = function(_run_id, _step_id, attrs)
                assert(attrs.agent_session_uuid == nil, "start_run must not link a session synchronously")
              end,
              update_run_step_visit = function(_visit_id, attrs)
                assert(attrs.agent_session_uuid == nil, "start_run must not link a session synchronously")
              end,
            }}

            package.loaded["lib.agent"] = {{
              get = function() return nil end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function(_, opts)
                    create_agent_calls = create_agent_calls + 1
                    assert(opts.request_id == "project-pipelines:run-1:step-1:agent")
                    assert(opts.metadata.owner_plugin == "project-pipelines")
                    assert(opts.metadata.run_id == "run-1")
                    assert(opts.metadata.step_id == "step-1")
                    assert(opts.base_ref == nil)
                    assert(opts.base_target_path == nil)
                    return {{ ok = true, status = "queued", request_id = opts.request_id }}
                  end,
                }}
              end,
            }}

            local engine = require("project_pipelines.engine")
            local result = engine.start_run({{ ticket_id = "ticket-1", pipeline_id = "pipe-1" }})

            assert(create_agent_calls == 1)
            assert(result.activation.ok == true)
            assert(result.activation.agent.status == "queued")
            assert(result.activation.agent.request_id == "project-pipelines:run-1:step-1:agent")

            local requested = nil
            for _, entry in ipairs(events) do
              assert(entry.kind ~= "step.agent_spawned")
              if entry.kind == "step.agent_requested" then
                requested = entry.event
              end
            end
            assert(requested ~= nil)
            assert(requested.payload.request_id == "project-pipelines:run-1:step-1:agent")
            assert(requested.payload.status == "queued")
            assert(requested.payload.session_uuid == nil)
            assert(visit ~= nil and visit.agent_session_uuid == nil)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines start_run should queue agent creation");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_start_run_threads_stacked_base_metadata() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local run = nil
            local ticket = {{
              id = "ticket-2",
              title = "Stacked change",
              target_id = "target-1",
            }}
            local step = {{
              id = "step-1",
              kind = "agent",
              name = "Implement",
              agent_name = "codex",
            }}

            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}

            package.loaded["project_pipelines.repo"] = {{
              prune_legacy_seed_data = function() end,
              get_ticket = function(id) return ticket end,
              open_ticket_run = function() return nil end,
              blocking_ticket_dependencies = function() return {{}} end,
              closed_ticket_dependencies = function() return {{}} end,
              ticket_dependencies = function() return {{}} end,
              get_pipeline = function(id) return {{ id = id }} end,
              pipeline_steps = function() return {{ step }} end,
              create_run = function(attrs)
                assert(attrs.base_ticket_id == "ticket-1")
                assert(attrs.base_run_id == "run-11")
                assert(attrs.base_ref == "project-pipelines/ticket-1")
                assert(attrs.base_target_path == "/worktrees/pr-11")
                run = {{
                  id = "run-2",
                  ticket_id = attrs.ticket_id,
                  pipeline_id = attrs.pipeline_id,
                  target_id = attrs.target_id,
                  base_ticket_id = attrs.base_ticket_id,
                  base_run_id = attrs.base_run_id,
                  base_ref = attrs.base_ref,
                  base_target_path = attrs.base_target_path,
                }}
                return run
              end,
              next_step = function() return step end,
              create_run_step_visit = function() return {{ id = "visit-2", sequence = 1 }} end,
              update_run = function(_run_id, attrs)
                for key, value in pairs(attrs or {{}}) do run[key] = value end
                return run
              end,
              get_run = function() return run end,
              get_run_step_visit = function() return {{ id = "visit-2" }} end,
              latest_step_session = function() return nil end,
              append_event = function() end,
            }}

            package.loaded["lib.agent"] = {{ get = function() return nil end }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function(_, opts)
                    assert(opts.issue_or_branch == "project-pipelines/ticket-2")
                    assert(opts.base_ref == "project-pipelines/ticket-1")
                    assert(opts.base_target_path == "/worktrees/pr-11")
                    assert(opts.metadata.base_ticket_id == "ticket-1")
                    assert(opts.metadata.base_run_id == "run-11")
                    assert(opts.metadata.base_ref == "project-pipelines/ticket-1")
                    assert(opts.metadata.base_target_path == "/worktrees/pr-11")
                    return {{ ok = true, status = "queued", request_id = opts.request_id }}
                  end,
                }}
              end,
            }}

            local engine = require("project_pipelines.engine")
            local started = engine.start_run({{
              ticket_id = "ticket-2",
              pipeline_id = "pipe-1",
              base_ticket_id = "ticket-1",
              base_run_id = "run-11",
              base_ref = "project-pipelines/ticket-1",
              base_target_path = "/worktrees/pr-11",
            }})

            assert(started.run.base_ref == "project-pipelines/ticket-1")
            assert(started.activation.ok == true)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines should preserve explicit stacked base metadata");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_start_run_explicit_base_ref_does_not_stack_on_closed_dependency(
) {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local run = nil
            local tickets = {{
              ["ticket-child"] = {{ id = "ticket-child", title = "Child", target_id = "target-1" }},
              ["ticket-parent"] = {{ id = "ticket-parent", title = "Parent", target_id = "target-1" }},
            }}
            local step = {{ id = "step-1", kind = "agent", name = "Implement", agent_name = "codex" }}

            package.loaded["project_pipelines.entities"] = {{ register = function() end, publish_snapshots = function() end }}
            package.loaded["project_pipelines.repo"] = {{
              prune_legacy_seed_data = function() end,
              get_ticket = function(id) return tickets[id] end,
              open_ticket_run = function() return nil end,
              blocking_ticket_dependencies = function() return {{}} end,
              closed_ticket_dependencies = function(ticket_id)
                assert(ticket_id == "ticket-child")
                return {{ {{ ticket_id = "ticket-child", depends_on_ticket_id = "ticket-parent", depends_on_status = "closed" }} }}
              end,
              ticket_dependencies = function() return {{}} end,
              get_pipeline = function(id) return {{ id = id }} end,
              pipeline_steps = function() return {{ step }} end,
              create_run = function(attrs)
                assert(attrs.base_ticket_id == nil)
                assert(attrs.base_run_id == nil)
                assert(attrs.base_ref == "main")
                assert(attrs.base_target_path == nil)
                run = attrs
                run.id = "run-child"
                return run
              end,
              next_step = function() return step end,
              create_run_step_visit = function() return {{ id = "visit-child", sequence = 1 }} end,
              update_run = function(_run_id, attrs)
                for key, value in pairs(attrs or {{}}) do run[key] = value end
                return run
              end,
              get_run = function() return run end,
              get_run_step_visit = function() return {{ id = "visit-child" }} end,
              latest_step_session = function() return nil end,
              append_event = function() end,
              latest_ticket_run = function()
                error("closed dependencies must not infer stacked base runs")
              end,
              latest_merge_pr_artifact = function()
                error("closed dependencies must not infer stacked base refs")
              end,
            }}
            package.loaded["lib.agent"] = {{ get = function() return nil end }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function(_, opts)
                    assert(opts.base_ref == "main")
                    assert(opts.base_target_path == nil)
                    assert(opts.metadata.base_ticket_id == nil)
                    assert(opts.metadata.base_run_id == nil)
                    assert(opts.metadata.base_ref == "main")
                    return {{ ok = true, status = "queued", request_id = opts.request_id }}
                  end,
                }}
              end,
            }}

            local started = require("project_pipelines.engine").start_run({{
              ticket_id = "ticket-child",
              pipeline_id = "pipe-1",
              base_ref = "main",
            }})
            assert(started.run.base_ticket_id == nil)
            assert(started.run.base_run_id == nil)
            assert(started.run.base_ref == "main")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines should honor explicit base refs without stacking on closed dependencies");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_start_run_blocks_open_dependencies() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            package.loaded["project_pipelines.entities"] = {{
              register = function() end,
              publish_snapshots = function() end,
            }}
            package.loaded["project_pipelines.notification_policy"] = {{}}
            package.loaded["lib.agent"] = {{ get = function() return nil end }}
            package.loaded["lib.hub"] = {{ get = function() return {{}} end }}
            package.loaded["project_pipelines.repo"] = {{
              prune_legacy_seed_data = function() end,
              get_ticket = function(id)
                assert(id == "ticket-child")
                return {{ id = id, title = "Child", target_id = "target-1" }}
              end,
              open_ticket_run = function() return nil end,
              ticket_dependencies = function(ticket_id)
                assert(ticket_id == "ticket-child")
                return {{ {{ ticket_id = "ticket-child", depends_on_ticket_id = "ticket-parent", depends_on_title = "Parent", depends_on_status = "open" }} }}
              end,
              get_pipeline = function()
                error("dependency gating must run before pipeline lookup")
              end,
            }}

            local result = require("project_pipelines.engine").start_run({{
              ticket_id = "ticket-child",
              pipeline_id = "pipe-1",
              base_ref = "main",
            }})

            assert(result.ok == false)
            assert(result.status == "blocked")
            assert(result.reason == "ticket_dependencies")
            assert(result.source == "start_run")
            assert(result.unmet_dependencies[1].depends_on_ticket_id == "ticket-parent")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines should block start_run while dependencies are open");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_public_dependency_write_blocks_advance_before_side_effects() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local handlers = {{}}
            local dependencies = {{}}
            local dependency_status = "open"
            local events = {{}}
            local mutation_calls = 0
            local notification_calls = 0
            local create_agent_calls = 0
            local merge_requests = 0
            local final_advance = false
            local run = {{
              id = "run-child",
              ticket_id = "ticket-child",
              pipeline_id = "pipe-1",
              target_id = "target-1",
              status = "active",
              current_step_id = "plan",
              current_run_step_id = "visit-plan",
            }}
            local steps = {{
              plan = {{ id = "plan", pipeline_id = "pipe-1", kind = "agent", name = "Plan", agent_name = "codex" }},
              implement = {{ id = "implement", pipeline_id = "pipe-1", kind = "agent", name = "Implement", agent_name = "codex" }},
              command = {{ id = "command", pipeline_id = "pipe-1", kind = "command", name = "Command", command = "true" }},
            }}

            json = {{
              decode = function() return {{}} end,
              encode = function() return "{{}}" end,
            }}
            mcp = {{
              tool = function(name, _spec, handler) handlers[name] = handler end,
              prompt = function() end,
            }}

            local repo = {{
              prune_legacy_seed_data = function() end,
              add_ticket_dependency = function(ticket_id, depends_on_ticket_id)
                local dependency = {{
                  id = "dependency-public",
                  ticket_id = ticket_id,
                  depends_on_ticket_id = depends_on_ticket_id,
                  created_at = 1,
                }}
                dependencies[#dependencies + 1] = dependency
                return dependency
              end,
              ticket_dependencies = function(ticket_id)
                local joined = {{}}
                for _, dependency in ipairs(dependencies) do
                  if dependency.ticket_id == ticket_id then
                    joined[#joined + 1] = {{
                      id = dependency.id,
                      ticket_id = dependency.ticket_id,
                      depends_on_ticket_id = dependency.depends_on_ticket_id,
                      depends_on_title = dependency_status and "Prerequisite" or nil,
                      depends_on_status = dependency_status,
                    }}
                  end
                end
                return joined
              end,
              get_run = function(id) assert(id == "run-child"); return run end,
              get_step = function(id) return steps[id] end,
              get_ticket = function(id) return {{ id = id, title = id, target_id = "target-1" }} end,
              get_pipeline = function() return {{ id = "pipe-1", merge_policy = "direct" }} end,
              step_gates = function() return {{}} end,
              latest_review_for_run_step = function() return nil end,
              next_step = function()
                if final_advance then return nil end
                return steps.implement
              end,
              append_event = function(kind, attrs)
                events[#events + 1] = {{ kind = kind, attrs = attrs }}
              end,
              update_run_step_visit = function(id, attrs)
                mutation_calls = mutation_calls + 1
                return {{ id = id, status = attrs.status }}
              end,
              update_run_step = function()
                mutation_calls = mutation_calls + 1
              end,
              create_run_step_visit = function(run_id, step_id, attrs)
                mutation_calls = mutation_calls + 1
                assert(run_id == "run-child")
                return {{ id = "visit-implement", run_id = run_id, step_id = step_id, status = attrs.status, sequence = 2 }}
              end,
              update_run = function(id, attrs)
                mutation_calls = mutation_calls + 1
                assert(id == "run-child")
                for key, value in pairs(attrs) do run[key] = value end
                if attrs.status == "done" then
                  run.current_step_id = nil
                  run.current_run_step_id = nil
                end
                return run
              end,
              get_run_step_visit = function(id)
                return {{ id = id, run_id = "run-child", step_id = run.current_step_id, status = run.status }}
              end,
              latest_step_session = function() return nil end,
              list_pr_links = function() return {{}} end,
            }}
            package.loaded["project_pipelines.repo"] = setmetatable(repo, {{
              __index = function() return function() return {{}} end end,
            }})
            package.loaded["project_pipelines.entities"] = {{ register = function() end, publish_snapshots = function() end }}
            package.loaded["project_pipelines.notification_policy"] = {{
              notify_phase_transition = function() notification_calls = notification_calls + 1 end,
            }}
            package.loaded["lib.config_resolver"] = {{ list_agents = function() return {{}} end }}
            package.loaded["lib.agent"] = {{ get = function() return nil end }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function(_, opts)
                    create_agent_calls = create_agent_calls + 1
                    return {{ status = "queued", request_id = opts.request_id }}
                  end,
                }}
              end,
            }}

            local engine = require("project_pipelines.engine")
            engine.request_merge = function()
              merge_requests = merge_requests + 1
              return {{ status = "queued" }}
            end
            require("project_pipelines.mcp").register()

            local added = handlers.project_pipelines_add_ticket_dependency({{
              ticket_id = "ticket-child",
              depends_on_ticket_id = "ticket-prerequisite",
            }}, {{}})
            assert(added.ok == true)
            assert(#dependencies == 1)
            assert(dependencies[1].depends_on_ticket_id == "ticket-prerequisite")

            local function reset_attempt(status)
              dependency_status = status
              events = {{}}
              mutation_calls = 0
              notification_calls = 0
              create_agent_calls = 0
              run.status = "active"
              run.current_step_id = "plan"
              run.current_run_step_id = "visit-plan"
            end

            for _, status in ipairs({{ "open", "active", "blocked" }}) do
              reset_attempt(status)
              local response = handlers.project_pipelines_request_step_advance({{
                run_id = "run-child",
                summary = "advance",
              }}, {{}}).result
              assert(response.ok == false)
              assert(response.status == "blocked")
              assert(response.reason == "ticket_dependencies")
              assert(response.step_id == "plan")
              assert(response.run_step_id == "visit-plan")
              assert(response.target_step_id == "implement")
              assert(response.source == "request_step_advance")
              assert(response.unmet_dependencies[1].reason == "open_ticket")
              assert(response.unmet_dependencies[1].depends_on_status == status)
              assert(response.unmet_dependencies[1].prompt:find("Close dependency ticket", 1, true))
              assert(#events == 1 and events[1].kind == "step.advance_blocked")
              assert(events[1].attrs.payload.reason == "ticket_dependencies")
              assert(mutation_calls == 0)
              assert(notification_calls == 0)
              assert(create_agent_calls == 0)
              assert(run.current_step_id == "plan" and run.current_run_step_id == "visit-plan")
            end

            reset_attempt(nil)
            local missing = handlers.project_pipelines_request_step_advance({{ run_id = "run-child" }}, {{}}).result
            assert(missing.ok == false)
            assert(missing.unmet_dependencies[1].reason == "unavailable_ticket")
            assert(missing.unmet_dependencies[1].dependency_id == "dependency-public")
            assert(missing.unmet_dependencies[1].depends_on_ticket_id == "ticket-prerequisite")
            assert(missing.unmet_dependencies[1].prompt:find("Restore or remove unavailable dependency ticket", 1, true))
            assert(mutation_calls == 0 and notification_calls == 0 and create_agent_calls == 0)

            reset_attempt("open")
            local forced = handlers.project_pipelines_request_step_advance({{
              run_id = "run-child",
              next_step_id = "implement",
              override_unmet_gates = true,
              override_reason = "force route",
            }}, {{}}).result
            assert(forced.ok == false and forced.reason == "ticket_dependencies")
            assert(#events == 1 and events[1].kind == "step.advance_blocked")
            assert(mutation_calls == 0)

            local direct = engine.activate_step(run, steps.implement)
            assert(direct.ok == false and direct.source == "activate_step")
            assert(mutation_calls == 0 and notification_calls == 0 and create_agent_calls == 0)
            local direct_command = engine.activate_step(run, steps.command)
            assert(direct_command.ok == false and direct_command.target_step_id == "command")
            assert(mutation_calls == 0 and notification_calls == 0 and create_agent_calls == 0)

            reset_attempt("closed")
            local advanced = handlers.project_pipelines_request_step_advance({{ run_id = "run-child" }}, {{}}).result
            assert(advanced.ok == true)
            assert(run.current_step_id == "implement")
            assert(create_agent_calls == 1)

            reset_attempt("open")
            final_advance = true
            local completed = handlers.project_pipelines_request_step_advance({{ run_id = "run-child" }}, {{}}).result
            assert(completed.ok == true)
            assert(completed.activation.status == "done")
            assert(run.status == "done" and run.current_step_id == nil and run.current_run_step_id == nil)
            assert(merge_requests == 1)
            final_advance = false
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("public ticket dependency rows should gate production advance and direct activation paths");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_step_advance_can_override_to_specific_step() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local run = {{
              id = "run-verify",
              ticket_id = "ticket-verify",
              pipeline_id = "pipe-1",
              target_id = "target-1",
              current_step_id = "verify",
              current_run_step_id = "visit-verify",
            }}
            local steps = {{
              verify = {{ id = "verify", pipeline_id = "pipe-1", kind = "agent", name = "Verify", agent_name = "codex" }},
              implement = {{ id = "implement", pipeline_id = "pipe-1", kind = "agent", name = "Implement", agent_name = "codex" }},
            }}
            local events = {{}}

            json = {{
              decode = function(raw)
                if raw == "[]" then return {{}} end
                if raw == "{{}}" then return {{}} end
                return {{}}
              end,
              encode = function(_) return "{{}}" end,
            }}

            package.loaded["project_pipelines.entities"] = {{ register = function() end, publish_snapshots = function() end }}
            package.loaded["project_pipelines.repo"] = {{
              get_run = function(run_id)
                assert(run_id == "run-verify")
                return run
              end,
              get_run_step_visit = function(run_step_id)
                return {{ id = run_step_id, run_id = run.id, step_id = run.current_step_id }}
              end,
              get_step = function(step_id) return steps[step_id] end,
              step_gates = function(step_id)
                assert(step_id == "verify")
                return {{ {{ id = "gate-verify", kind = "attestation", prompt = "Verify it", required_fields = "[]" }} }}
              end,
              latest_gate_result = function()
                return {{ id = "gate-result-1", status = "failed", summary = "verification failed", evidence = "{{}}" }}
              end,
              latest_review_for_run_step = function() return nil end,
              append_event = function(kind, attrs)
                events[#events + 1] = {{ kind = kind, attrs = attrs }}
              end,
              update_run_step_visit = function(run_step_id, attrs)
                assert(run_step_id == "visit-verify" or run_step_id == "visit-implement")
                return {{ id = run_step_id, run_id = run.id, step_id = run_step_id == "visit-verify" and "verify" or "implement", status = attrs.status }}
              end,
              create_run_step_visit = function(run_id, step_id, attrs)
                assert(run_id == "run-verify")
                assert(step_id == "implement")
                assert(attrs.status == "active")
                return {{ id = "visit-implement", run_id = run_id, step_id = step_id, sequence = 2, status = "active" }}
              end,
              update_run = function(run_id, attrs)
                assert(run_id == "run-verify")
                for key, value in pairs(attrs or {{}}) do run[key] = value end
                return run
              end,
              get_ticket = function(ticket_id)
                assert(ticket_id == "ticket-verify")
                return {{ id = ticket_id, title = "Verify fallback" }}
              end,
              latest_step_session = function() return nil end,
              ticket_dependencies = function() return {{}} end,
            }}
            package.loaded["lib.agent"] = {{ get = function() return nil end }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  create_agent = function(_, opts)
                    assert(opts.request_id == "project-pipelines:run-verify:implement:agent")
                    assert(opts.agent_name == "codex")
                    return {{ status = "queued", request_id = opts.request_id }}
                  end,
                }}
              end,
            }}

            local blocked = require("project_pipelines.engine").request_step_advance({{
              run_id = "run-verify",
              next_step_id = "implement",
              summary = "verification failed",
            }}, {{}})
            assert(blocked.ok == false)
            assert(blocked.status == "blocked")
            assert(blocked.requested_next_step.id == "implement")
            assert(blocked.next_tool_params.override_unmet_gates == true)

            local advanced = require("project_pipelines.engine").request_step_advance({{
              run_id = "run-verify",
              next_step_id = "implement",
              override_unmet_gates = true,
              override_reason = "Verification failed; send back to implementation.",
              summary = "route back",
            }}, {{}})
            assert(advanced.ok == true)
            assert(advanced.next_step.id == "implement")
            assert(run.current_step_id == "implement")
            assert(run.current_run_step_id == "visit-implement")

            local saw_override = false
            for _, event in ipairs(events) do
              if event.kind == "step.advance_override" then
                saw_override = true
                assert(event.attrs.payload.next_step_id == "implement")
                assert(event.attrs.payload.reason == "Verification failed; send back to implementation.")
              end
            end
            assert(saw_override == true)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines should allow explicit recovery routing to a specific step");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_retry_step_agent_requeues_current_visit() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local run = {{
              id = "run-verify",
              ticket_id = "ticket-verify",
              pipeline_id = "pipe-1",
              status = "blocked",
              current_step_id = "verify",
              current_run_step_id = "visit-verify",
              target_id = "target-1",
              workspace_name = "Pipeline - Verify",
            }}
            local visit = {{
              id = "visit-verify",
              run_id = "run-verify",
              step_id = "verify",
              status = "blocked",
              agent_session_uuid = "sess-dead",
            }}
            local step = {{
              id = "verify",
              kind = "agent",
              name = "Verify",
              agent_name = "codex",
              prompt = "Verify the work",
            }}
            local ticket = {{ id = "ticket-verify", title = "Retry verify", target_id = "target-1" }}
            local events = {{}}
            local create_agent_calls = 0
            local post_calls = 0
            local notify_calls = 0
            local dependency_status = "open"

            package.loaded["project_pipelines.entities"] = {{ register = function() end, publish_snapshots = function() end }}
            package.loaded["project_pipelines.repo"] = {{
              get_run = function(id) assert(id == "run-verify"); return run end,
              get_step = function(id) assert(id == "verify"); return step end,
              get_ticket = function(id) assert(id == "ticket-verify"); return ticket end,
              get_run_step_visit = function(id) assert(id == "visit-verify"); return visit end,
              get_run_step = function(run_id, step_id) assert(run_id == "run-verify"); assert(step_id == "verify"); return visit end,
              ticket_dependencies = function(ticket_id)
                assert(ticket_id == "ticket-verify")
                return {{
                  {{ id = "dependency-verify", ticket_id = ticket_id, depends_on_ticket_id = "ticket-parent", depends_on_title = "Parent", depends_on_status = dependency_status }},
                }}
              end,
              latest_step_session = function()
                return dependency_status == "closed" and {{ agent_session_uuid = "sess-live" }} or nil
              end,
              update_run = function(id, attrs)
                assert(id == "run-verify")
                for key, value in pairs(attrs or {{}}) do run[key] = value end
                return run
              end,
              update_run_step_visit = function(id, attrs)
                assert(id == "visit-verify")
                for key, value in pairs(attrs or {{}}) do visit[key] = value end
                return visit
              end,
              append_event = function(kind, event)
                events[#events + 1] = {{ kind = kind, event = event }}
              end,
            }}
            package.loaded["lib.agent"] = {{
              get = function(session_uuid)
                if session_uuid == "sess-live" then return {{}} end
                return nil
              end,
            }}
            package.loaded["lib.hub"] = {{
              get = function()
                return {{
                  post = function(_, session_uuid, message)
                    assert(session_uuid == "sess-live")
                    assert(message.payload.step_id == "verify")
                    post_calls = post_calls + 1
                  end,
                  notify = function(_, session_uuid)
                    assert(session_uuid == "sess-live")
                    notify_calls = notify_calls + 1
                  end,
                  create_agent = function(_, opts)
                    create_agent_calls = create_agent_calls + 1
                    assert(opts.request_id == "project-pipelines:run-verify:verify:agent")
                    assert(opts.metadata.run_id == "run-verify")
                    assert(opts.metadata.step_id == "verify")
                    return {{ ok = true, status = "queued", request_id = opts.request_id }}
                  end,
                }}
              end,
            }}

            local engine = require("project_pipelines.engine")
            local blocked = engine.retry_step_agent({{
              run_id = "run-verify",
              reason = "spawn failed before linking",
            }}, {{ session_uuid = "sess-human" }})

            assert(blocked.ok == false)
            assert(blocked.status == "blocked")
            assert(blocked.reason == "ticket_dependencies")
            assert(blocked.source == "retry_step_agent")
            assert(blocked.unmet_dependencies[1].reason == "open_ticket")
            assert(run.status == "blocked")
            assert(visit.status == "blocked")
            assert(visit.agent_session_uuid == "sess-dead")
            assert(create_agent_calls == 0)
            assert(events[1].kind == "step.advance_blocked")

            dependency_status = "closed"
            local result = engine.retry_step_agent({{
              run_id = "run-verify",
              reason = "dependency closed; retry explicitly",
            }}, {{ session_uuid = "sess-human" }})

            assert(result.ok == true)
            assert(result.agent.reused == true)
            assert(create_agent_calls == 0)
            assert(post_calls == 1)
            assert(notify_calls == 1)
            assert(run.status == "active")
            assert(run.current_run_step_id == "visit-verify")
            assert(visit.status == "active")
            assert(visit.agent_session_uuid == "sess-live")

            local saw_retry = false
            local saw_prompted = false
            for _, event in ipairs(events) do
              if event.kind == "step.agent_retry_requested" then
                saw_retry = true
                assert(event.event.payload.run_step_id == "visit-verify")
                assert(event.event.payload.requested_by_session_uuid == "sess-human")
              elseif event.kind == "step.agent_prompted" then
                saw_prompted = true
                assert(event.event.payload.session_uuid == "sess-live")
              end
            end
            assert(saw_retry)
            assert(saw_prompted)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines should gate retry and then reuse the live session after explicit recovery");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_pipeline_edit_meta_keeps_archived_badge_without_version() {
    let lua = Lua::new();
    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            ui = {{
              action = function(name, payload) return {{ name = name, payload = payload }} end,
              badge = function(props) return {{ type = "badge", text = props.text, tone = props.tone }} end,
              bind = function(path) return {{ bind = path }} end,
              bind_if = function(path, node) return {{ bind_if = path, node = node }} end,
              button = function(props) return {{ type = "button", props = props }} end,
              checkbox = function(props) return {{ type = "checkbox", props = props }} end,
              panel = function(props) return {{ type = "panel", props = props }} end,
              select = function(props) return {{ type = "select", props = props }} end,
              stack = function(props) return {{ type = "stack", props = props }} end,
              text = function(props) return {{ type = "text", props = props }} end,
              text_input = function(props) return {{ type = "text_input", props = props }} end,
              textarea = function(props) return {{ type = "textarea", props = props }} end,
            }}

            package.loaded["project_pipelines.web.ui"] = {{
              action_row = function(children) return {{ type = "action_row", children = children }} end,
              agent_options = function() return {{}} end,
              badge = function(text, tone) return {{ type = "badge", text = tostring(text), tone = tone }} end,
              empty = function(title) return {{ type = "empty", title = title }} end,
              field_action = function(name, payload) return {{ name = name, payload = payload }} end,
              page_header = function(props) return {{ type = "page_header", props = props }} end,
              panel = function(child) return {{ type = "panel", child = child }} end,
              row = function(children) return {{ type = "row", children = children }} end,
            }}
            package.loaded["project_pipelines.web.actions"] = {{
              feedback = function() return {{}} end,
            }}
            package.loaded["project_pipelines.repo"] = {{
              get_pipeline = function(id)
                return {{ id = id, name = "Old pipeline", archived_at = 123, version_label = "" }}
              end,
              pipeline_steps = function(id)
                return {{ {{ id = "step-1", pipeline_id = id, name = "Implement", kind = "agent", position = 1 }} }}
              end,
              step_gates = function() return {{}} end,
            }}

            local screen = require("project_pipelines.web.screens.pipelines")
            local tree = screen.edit({{ params = {{ pipeline_id = "old" }} }}, {{ path = function(path) return path end }})
            local header = tree.props.children[1]
            local meta = header.props.meta
            assert(#meta == 3, "archived badge must not be truncated by a nil version meta entry")
            assert(meta[1].text == "old")
            assert(meta[2].text == "1 steps")
            assert(meta[3].text == "archived")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("archived pipeline edit header should preserve meta badges");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_pipeline_entity_queries_filter_active_and_archived() {
    let lua = Lua::new();
    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local registered = {{}}
            package.loaded["lib.entity_broadcast"] = {{
              register = function(entity_type, opts)
                registered[entity_type] = opts
              end,
            }}
            package.loaded["project_pipelines.web.ui"] = {{
              status_tone = function() return "muted" end,
              status_label = function(status) return tostring(status or "") end,
              status_state = function() return "neutral" end,
              ticket_notification_count = function() return 0 end,
            }}

            local function normalize(sql)
              return (sql:gsub("%s+", " "))
            end
            local db = {{}}
            function db:eval(sql, param)
              local compact = normalize(sql)
              if compact == "SELECT * FROM pipelines WHERE archived_at IS NULL ORDER BY created_at ASC, id ASC" then
                return {{ {{ id = "active", name = "Active", created_at = 1 }} }}
              end
              if compact == "SELECT * FROM pipelines WHERE archived_at IS NOT NULL ORDER BY created_at ASC, id ASC" then
                return {{ {{ id = "archived", name = "Archived", archived_at = 123, version_label = "v1", created_at = 0 }} }}
              end
              if compact == "SELECT * FROM pipelines WHERE id = ? LIMIT 1" then
                assert(param == "archived")
                return {{ {{ id = "archived", name = "Archived", archived_at = 123, version_label = "v1" }} }}
              end
              if compact:find("FROM pipeline_steps") then
                return {{}}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local entities = require("project_pipelines.entities")
            entities.register()
            local active = registered[entities.types.pipeline].query({{ where = {{ active = true }} }})
            local archived = registered[entities.types.pipeline].query({{ where = {{ archived = true }} }})
            local direct = registered[entities.types.pipeline].query({{ id = "archived" }})

            assert(#active == 1 and active[1].id == "active" and active[1].active == true)
            assert(#archived == 1 and archived[1].id == "archived" and archived[1].archived == true)
            assert(#direct == 1 and direct[1].id == "archived" and direct[1].archived == true)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("pipeline entity queries should expose active, archived, and direct archived hydration");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_repo_filters_default_lists_but_preserves_ticket_history_names()
{
    let lua = Lua::new();
    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local function normalize(sql)
              return (sql:gsub("%s+", " "))
            end
            local db = {{}}
            function db:eval(sql, param)
              local compact = normalize(sql)
              if compact == "SELECT * FROM pipelines WHERE archived_at IS NULL ORDER BY created_at ASC" then
                return {{ {{ id = "active", name = "Active", created_at = 2 }} }}
              end
              if compact == "SELECT * FROM pipelines ORDER BY created_at ASC" then
                return {{
                  {{ id = "archived", name = "Archived", archived_at = 123, created_at = 1 }},
                  {{ id = "active", name = "Active", created_at = 2 }},
                }}
              end
              if compact == "SELECT * FROM pipelines WHERE archived_at IS NULL ORDER BY created_at ASC LIMIT 1" then
                return {{ {{ id = "active", name = "Active", created_at = 2 }} }}
              end
              if compact == "SELECT * FROM runs WHERE ticket_id = ? ORDER BY created_at DESC" then
                assert(param == "ticket-1")
                return {{ {{ id = "run-1", ticket_id = "ticket-1", pipeline_id = "archived", status = "active", created_at = 10 }} }}
              end
              if compact:find("SELECT %* FROM pipelines WHERE id IN") then
                return {{ {{ id = "archived", name = "Archived", archived_at = 123 }} }}
              end
              if compact == "SELECT * FROM pipeline_steps" then
                return {{}}
              end
              if compact:find("FROM run_steps") then
                return {{}}
              end
              if compact:find("FROM events") then
                return {{}}
              end
              if compact:find("FROM questions") then
                return {{}}
              end
              if compact:find("FROM ticket_dependencies") then
                return {{}}
              end
              if compact:find("FROM tickets") then
                return {{}}
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local repo = require("project_pipelines.repo")
            local active = repo.list_pipelines()
            local all = repo.list_all_pipelines()
            local default = repo.get_default_pipeline()
            local overview = repo.ticket_detail_overview("ticket-1")

            assert(#active == 1 and active[1].id == "active")
            assert(#all == 2 and all[1].id == "archived")
            assert(default.id == "active")
            assert(#overview.pipelines == 1 and overview.pipelines[1].id == "active")
            assert(overview.pipelines_by_id.archived and overview.pipelines_by_id.archived.name == "Archived")
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("repo should filter normal lists while preserving archived names for ticket history");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_db_v10_migration_adds_nullable_pipeline_columns() {
    let lua = Lua::new();
    log::register(&lua).expect("register log");

    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local spec = nil
            plugin = {{
              db = function(opts)
                spec = opts
                return {{ eval = function() end }}
              end,
            }}
            log = {{ warn = function() end }}

            require("project_pipelines.db")
            assert(spec.version == 10)
            for _, column in ipairs({{ "version_label", "archived_at", "replacement_pipeline_id", "supersedes_pipeline_id" }}) do
              assert(spec.models.pipelines[column] ~= nil, "fresh model missing " .. column)
            end

            local alters = {{}}
            local migration_db = {{}}
            function migration_db:eval(sql)
              if sql == "PRAGMA table_info(pipelines)" then
                return {{
                  {{ name = "id" }},
                  {{ name = "name" }},
                  {{ name = "description" }},
                  {{ name = "merge_policy" }},
                  {{ name = "created_at" }},
                  {{ name = "updated_at" }},
                }}
              end
              alters[#alters + 1] = sql
            end
            spec.migrations[10](migration_db)
            assert(#alters == 4)
            assert(alters[1]:find("ADD COLUMN version_label text", 1, true))
            assert(alters[2]:find("ADD COLUMN archived_at integer", 1, true))
            assert(alters[3]:find("ADD COLUMN replacement_pipeline_id text", 1, true))
            assert(alters[4]:find("ADD COLUMN supersedes_pipeline_id text", 1, true))
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines v10 migration should add nullable pipeline columns");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_repo_validates_replacement_links() {
    let lua = Lua::new();
    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local pipelines = {{
              old = {{ id = "old", name = "Old", merge_policy = "direct" }},
              new = {{ id = "new", name = "New", merge_policy = "direct" }},
            }}
            json = {{
              encode = function() return "{{}}" end,
              decode = function() return {{}} end,
            }}
            local updates = {{}}
            local db = {{
              pipelines = {{
                where = function(_self, query)
                  return pipelines[query.id]
                end,
                update = function(_self, spec)
                  updates[#updates + 1] = spec
                  for key, value in pairs(spec.set or {{}}) do
                    pipelines[spec.where.id][key] = value
                  end
                end,
              }},
              events = {{
                insert = function() end,
              }},
            }}
            function db:eval(sql, param)
              if sql:find("UPDATE pipelines SET replacement_pipeline_id = NULL") then
                pipelines[param].replacement_pipeline_id = nil
              elseif sql:find("UPDATE pipelines SET supersedes_pipeline_id = NULL") then
                pipelines[param].supersedes_pipeline_id = nil
              elseif sql:find("UPDATE pipelines SET archived_at = NULL") then
                pipelines[param].archived_at = nil
              end
              return {{}}
            end
            package.loaded["project_pipelines.db"] = db

            local repo = require("project_pipelines.repo")
            local ok_self, err_self = pcall(repo.update_pipeline, "old", {{ replacement_pipeline_id = "old" }})
            assert(ok_self == false and tostring(err_self):find("cannot reference the same pipeline", 1, true))
            local ok_missing, err_missing = pcall(repo.update_pipeline, "old", {{ replacement_pipeline_id = "missing" }})
            assert(ok_missing == false and tostring(err_missing):find("not found: missing", 1, true))
            local updated = repo.update_pipeline("old", {{ replacement_pipeline_id = "new", supersedes_pipeline_id = "" }})
            assert(updated.replacement_pipeline_id == "new")
            assert(#updates >= 1)
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("repo should validate pipeline replacement links");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_archive_schema_and_allowlists_are_wired() {
    let root = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let db = std::fs::read_to_string(root.join("project_pipelines/db.lua"))
        .expect("read project pipelines db");
    let repo = std::fs::read_to_string(root.join("project_pipelines/repo.lua"))
        .expect("read project pipelines repo");
    let actions = std::fs::read_to_string(root.join("project_pipelines/web/actions.lua"))
        .expect("read project pipelines actions");
    let entity_contract =
        std::fs::read_to_string(root.join("project_pipelines/entity_contract.lua"))
            .expect("read project pipelines entity contract");

    assert!(db.contains("version = 10"));
    for column in [
        "version_label",
        "archived_at",
        "replacement_pipeline_id",
        "supersedes_pipeline_id",
    ] {
        assert!(
            db.contains(&format!("\"{column}\""))
                && db.contains(&format!("{column} = {{ \"text\""))
                || column == "archived_at" && db.contains("archived_at = { \"integer\""),
            "db model and migration should include {column}"
        );
        assert!(
            repo.contains(&format!("{column} = true")),
            "repo update allow-list missing {column}"
        );
        assert!(
            actions.contains(&format!("{column} = true")) || column == "archived_at",
            "web action allow-list missing {column}"
        );
        assert!(
            entity_contract.contains(column),
            "entity contract missing {column}"
        );
    }
    assert!(
        actions.contains("archived = true"),
        "web action allow-list missing archived toggle"
    );
    assert!(repo.contains("function M.list_all_pipelines()"));
    assert!(repo.contains("WHERE archived_at IS NULL ORDER BY created_at ASC"));
    assert!(
        repo.contains("referenced_pipeline_ids"),
        "ticket overview should preserve historical pipeline names"
    );
}

#[test]
fn catalog_plugin_project_pipelines_mcp_archive_contract_is_explicit() {
    let lua = Lua::new();
    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            local handlers = {{}}
            mcp = {{
              tool = function(name, _spec, handler)
                handlers[name] = handler
              end,
              prompt = function() end,
            }}
            log = {{ warn = function() end, info = function() end }}
            package.loaded["lib.config_resolver"] = {{ list_agents = function() return {{}} end }}
            package.loaded["project_pipelines.engine"] = {{ start_run = function() end }}
            package.loaded["project_pipelines.repo"] = {{
              prune_legacy_seed_data = function() end,
              list_pipelines = function()
                return {{ {{ id = "active", name = "Active" }} }}
              end,
              list_all_pipelines = function()
                return {{
                  {{ id = "active", name = "Active" }},
                  {{ id = "archived", name = "Archived", archived_at = 123 }},
                }}
              end,
              get_pipeline_definition = function(id)
                if id == "archived" then
                  return {{ id = "archived", name = "Archived", archived_at = 123, steps = {{}} }}
                end
                return {{ id = id, name = "Active", steps = {{}} }}
              end,
              pipeline_is_archived = function(pipeline)
                return pipeline and pipeline.archived_at ~= nil
              end,
              update_pipeline = function(_id, fields)
                assert(fields.archived == true)
                assert(fields.version_label == "v2")
                assert(fields.replacement_pipeline_id == "active")
                return fields
              end,
            }}

            require("project_pipelines.mcp").register()
            local default_list = handlers.project_pipelines_list_pipelines({{}})
            assert(#default_list.result == 1 and default_list.result[1].id == "active")
            local all_list = handlers.project_pipelines_list_pipelines({{ include_archived = true }})
            assert(#all_list.result == 2 and all_list.result[2].id == "archived")
            local hidden = handlers.project_pipelines_get_pipeline({{ pipeline_id = "archived" }})
            assert(hidden.ok == true and hidden.result == nil)
            local explicit = handlers.project_pipelines_get_pipeline({{ pipeline_id = "archived", include_archived = true }})
            assert(explicit.result.id == "archived")
            handlers.project_pipelines_update_pipeline({{
              pipeline_id = "archived",
              archived = true,
              version_label = "v2",
              replacement_pipeline_id = "active",
            }})
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines MCP archive contract should be explicit");

    assert_eq!(result, "ok");
}

#[test]
fn catalog_plugin_project_pipelines_start_run_rejects_archived_pipelines() {
    let lua = Lua::new();
    let plugin_dir = project_root_dir().join("catalog/templates/plugins/project-pipelines");
    let result: String = lua
        .load(format!(
            r#"
            package.path = "{plugin_dir}/?.lua;{plugin_dir}/?/init.lua;" .. package.path

            package.loaded["project_pipelines.entities"] = {{ register = function() end, publish_snapshots = function() end }}
            package.loaded["project_pipelines.notification_policy"] = {{}}
            package.loaded["lib.hub"] = {{ get = function() return {{}} end }}
            package.loaded["lib.agent"] = {{}}
            package.loaded["project_pipelines.repo"] = {{
              prune_legacy_seed_data = function() end,
              get_default_pipeline = function()
                return {{ id = "active", name = "Active" }}
              end,
              get_ticket = function(id)
                return {{ id = id, target_id = "target-1", title = "Ticket" }}
              end,
              open_ticket_run = function() return nil end,
              ticket_dependencies = function() return {{}} end,
              get_pipeline = function(id)
                if id == "archived" then
                  return {{ id = "archived", archived_at = 123 }}
                end
                return {{ id = id }}
              end,
              pipeline_is_archived = function(pipeline)
                return pipeline and pipeline.archived_at ~= nil
              end,
              pipeline_steps = function(_id) return {{}} end,
            }}

            local engine = require("project_pipelines.engine")
            local ok_archived, err_archived = pcall(engine.start_run, {{ ticket_id = "ticket-1", pipeline_id = "archived" }})
            assert(ok_archived == false)
            assert(tostring(err_archived):find("archived pipeline definitions are unavailable", 1, true))

            local ok_default, err_default = pcall(engine.start_run, {{ ticket_id = "ticket-1" }})
            assert(ok_default == false)
            assert(tostring(err_default):find("pipeline has no steps: active", 1, true))
            return "ok"
            "#,
            plugin_dir = plugin_dir.display()
        ))
        .eval()
        .expect("project pipelines start_run should reject archived pipelines");

    assert_eq!(result, "ok");
}
