//! Rust-hosted Lua tests for MCP proxy behavior.

use mlua::Lua;

fn create_lua_vm() -> Lua {
    let lua = Lua::new();

    botster::lua::primitives::json::register(&lua).expect("json register");
    botster::lua::primitives::log::register(&lua).expect("log register");

    let lua_dir = std::env::current_dir()
        .unwrap()
        .join("lua")
        .to_str()
        .unwrap()
        .to_string();
    lua.load(format!(
        r#"package.path = "{lua_dir}/?.lua;{lua_dir}/?/init.lua;" .. package.path"#
    ))
    .exec()
    .expect("set package.path");

    lua.load(
        r#"
        _G.hub = { is_offline = function() return false end }
        _G.events = { emit = function() return true end }
        _G.timer = {
          cancel = function() return true end,
          after = function(_, callback)
            callback()
            return "timer"
          end,
        }
        "#,
    )
    .exec()
    .expect("stub hub/events/timer");

    lua
}

#[test]
fn proxied_tool_call_forwards_botster_context_in_meta() {
    let lua = create_lua_vm();

    let ok: bool = lua
        .load(
            r#"
            _G.requests = {}
            _G.http = {
              request = function(req, callback)
                table.insert(_G.requests, req)
                local decoded = json.decode(req.body)

                if decoded.method == "initialize" then
                  callback({
                    status = 200,
                    headers = { ["Mcp-Session-Id"] = "remote-session" },
                    body = json.encode({ jsonrpc = "2.0", id = decoded.id, result = {} }),
                  }, nil)
                elseif decoded.method == "tools/list" then
                  callback({
                    status = 200,
                    headers = {},
                    body = json.encode({
                      jsonrpc = "2.0",
                      id = decoded.id,
                      result = {
                        tools = {
                          {
                            name = "remote_tool",
                            description = "Remote tool",
                            inputSchema = { type = "object", properties = {} },
                          },
                        },
                      },
                    }),
                  }, nil)
                elseif decoded.method == "tools/call" then
                  _G.forwarded_call = decoded
                  callback({
                    status = 200,
                    headers = {},
                    body = json.encode({
                      jsonrpc = "2.0",
                      id = decoded.id,
                      result = {
                        content = { { type = "text", text = "ok" } },
                        isError = false,
                      },
                    }),
                  }, nil)
                else
                  error("unexpected method: " .. tostring(decoded.method))
                end
              end,
            }

            local mcp = require("lib.mcp")
            mcp.proxy("https://example.test/mcp")
            mcp.call_tool("remote_tool", { value = 1 }, {
              session_uuid = "sess-1234567890abcdef",
              agent_name = "codex",
              session_name = "codex",
              branch_name = "realignment",
              repo = "owner/repo",
              workspace_id = "ws-123",
              worktree_path = "/tmp/worktree",
            }, function(content, err)
              _G.callback_err = err
              _G.callback_text = content and content[1] and content[1].text
            end)

            local params = _G.forwarded_call.params
            local meta = params and params._meta and params._meta.botster
            return _G.callback_err == nil
              and _G.callback_text == "ok"
              and params.name == "remote_tool"
              and params.arguments.value == 1
              and meta.session_uuid == "sess-1234567890abcdef"
              and meta.agent_name == "codex"
              and meta.session_name == "codex"
              and meta.branch_name == "realignment"
              and meta.repo == "owner/repo"
              and meta.workspace_id == "ws-123"
              and meta.worktree_path == "/tmp/worktree"
            "#,
        )
        .eval()
        .expect("proxied tool call should forward botster metadata");

    assert!(
        ok,
        "proxied MCP tools/call should include _meta.botster context"
    );
}

#[test]
fn proxied_tool_call_applies_generic_argument_transform() {
    let lua = create_lua_vm();

    let ok: bool = lua
        .load(
            r#"
            _G.http = {
              request = function(req, callback)
                local decoded = json.decode(req.body)

                if decoded.method == "initialize" then
                  callback({
                    status = 200,
                    headers = { ["Mcp-Session-Id"] = "remote-session" },
                    body = json.encode({ jsonrpc = "2.0", id = decoded.id, result = {} }),
                  }, nil)
                elseif decoded.method == "tools/list" then
                  callback({
                    status = 200,
                    headers = {},
                    body = json.encode({
                      jsonrpc = "2.0",
                      id = decoded.id,
                      result = {
                        tools = {
                          {
                            name = "remote_tool",
                            description = "Remote tool",
                            inputSchema = { type = "object", properties = {} },
                          },
                        },
                      },
                    }),
                  }, nil)
                elseif decoded.method == "tools/call" then
                  _G.forwarded_call = decoded
                  callback({
                    status = 200,
                    headers = {},
                    body = json.encode({
                      jsonrpc = "2.0",
                      id = decoded.id,
                      result = {
                        content = { { type = "text", text = "ok" } },
                        isError = false,
                      },
                    }),
                  }, nil)
                else
                  error("unexpected method: " .. tostring(decoded.method))
                end
              end,
            }

            local mcp = require("lib.mcp")
            mcp.proxy("https://example.test/mcp", {
              transform_arguments = function(name, arguments)
                if name == "remote_tool" and arguments.flag == false then
                  local out = {}
                  for key, value in pairs(arguments) do
                    out[key] = value
                  end
                  out.flag = nil
                  return out
                end
                return arguments
              end,
            })
            mcp.call_tool("remote_tool", { value = 1, flag = false }, {}, function(content, err)
              _G.callback_err = err
              _G.callback_text = content and content[1] and content[1].text
            end)

            local arguments = _G.forwarded_call.params.arguments
            return _G.callback_err == nil
              and _G.callback_text == "ok"
              and arguments.value == 1
              and arguments.flag == nil
            "#,
        )
        .eval()
        .expect("proxied tool call should apply argument transform");

    assert!(
        ok,
        "proxied MCP tools/call should apply generic argument transform before forwarding"
    );
}

#[test]
fn proxied_tool_call_preserves_remote_error_content_blocks() {
    let lua = create_lua_vm();

    let ok: bool = lua
        .load(
            r#"
            _G.http = {
              request = function(req, callback)
                local decoded = json.decode(req.body)

                if decoded.method == "initialize" then
                  callback({
                    status = 200,
                    headers = { ["Mcp-Session-Id"] = "remote-session" },
                    body = json.encode({ jsonrpc = "2.0", id = decoded.id, result = {} }),
                  }, nil)
                elseif decoded.method == "tools/list" then
                  callback({
                    status = 200,
                    headers = {},
                    body = json.encode({
                      jsonrpc = "2.0",
                      id = decoded.id,
                      result = {
                        tools = {
                          {
                            name = "github_mark_pull_request_ready_for_review",
                            description = "Mark PR ready",
                            inputSchema = { type = "object", properties = {} },
                          },
                        },
                      },
                    }),
                  }, nil)
                elseif decoded.method == "tools/call" then
                  callback({
                    status = 200,
                    headers = {},
                    body = json.encode({
                      jsonrpc = "2.0",
                      id = decoded.id,
                      result = {
                        content = {
                          { type = "text", text = "Marking owner/repo#84 ready for review..." },
                          { type = "text", text = "GitHub did not confirm that owner/repo#84 is ready for review." },
                        },
                        isError = true,
                      },
                    }),
                  }, nil)
                else
                  error("unexpected method: " .. tostring(decoded.method))
                end
              end,
            }

            local mcp = require("lib.mcp")
            mcp.proxy("https://example.test/mcp")
            mcp.call_tool("github_mark_pull_request_ready_for_review", {
              repo = "owner/repo",
              pr_number = 84,
            }, {}, function(content, err, is_error)
              _G.callback_err = err
              _G.callback_is_error = is_error
              _G.callback_first_text = content and content[1] and content[1].text
              _G.callback_second_text = content and content[2] and content[2].text
            end)

            return _G.callback_err == nil
              and _G.callback_is_error == true
              and _G.callback_first_text == "Marking owner/repo#84 ready for review..."
              and _G.callback_second_text == "GitHub did not confirm that owner/repo#84 is ready for review."
            "#,
        )
        .eval()
        .expect("proxied tool call should preserve remote isError content");

    assert!(
        ok,
        "proxied MCP tools/call should preserve every remote isError content block"
    );
}

#[test]
fn caller_context_uses_live_session_and_scopes_prompts() {
    let lua = create_lua_vm();
    let ok: bool = lua
        .load(
            r#"
            package.preload["lib.session"] = function()
              return {
                get = function(uuid)
                  if uuid == "sess-live" then
                    return {
                      session_uuid = uuid,
                      session_name = "live-name",
                      agent_name = "cursor",
                      branch_name = "main",
                      repo = "org/repo",
                      _workspace_id = "ws-moved",
                      worktree_path = "/tmp/wt",
                      target_id = "tgt-1",
                      target_path = "/tmp/repo",
                    }
                  end
                end,
              }
            end
            package.preload["lib.config_resolver"] = function()
              return {
                resolve_all = function()
                  return {
                    agents = {
                      cursor = { manifest = { plugins = { "allowed-plugin" } } },
                    },
                  }
                end,
              }
            end
            _G.config = { data_dir = function() return "/tmp" end }
            _G.spawn_targets = {
              get = function()
                return { plugins = { "allowed-plugin" } }
              end,
            }
            _G.hub.hub_id = function() return "device-hub" end
            _G.hub.server_id = function() return "cloud-hub" end

            local mcp = require("lib.mcp")
            _G._loading_plugin_name = "allowed-plugin"
            mcp.prompt("visible-prompt", { description = "ok" }, function()
              return "visible"
            end)
            mcp.resource("botster://allowed/{id}", { name = "allowed" }, function()
              return {}
            end)
            _G._loading_plugin_name = "secret-plugin"
            mcp.prompt("hidden-prompt", { description = "no" }, function()
              return "hidden"
            end)
            mcp.resource("botster://secret/{id}", { name = "secret" }, function()
              return {}
            end)
            _G._loading_plugin_name = nil

            local context = mcp.caller_context("sess-live")
            assert(context.hub_id == "device-hub", "hub_id must match session manifest rule")
            assert(context.session_name == "live-name")
            assert(context.workspace_id == "ws-moved")
            assert(context.repo == "org/repo")
            assert(context.branch_name == "main")
            assert(context.worktree_path == "/tmp/wt")

            local prompts = mcp.list_prompts("sess-live")
            local names = {}
            for _, prompt in ipairs(prompts) do names[prompt.name] = true end
            assert(names["visible-prompt"], "allowed prompt must be listed")
            assert(not names["hidden-prompt"], "other plugin prompt must stay hidden")

            local hidden, err = mcp.get_prompt("hidden-prompt", {}, { session_uuid = "sess-live" })
            assert(hidden == nil)
            assert(tostring(err):find("not available", 1, true))

            local templates = mcp.list_resource_templates("sess-live")
            local uris = {}
            for _, tmpl in ipairs(templates) do uris[tmpl.uriTemplate] = true end
            assert(uris["botster://allowed/{id}"])
            assert(not uris["botster://secret/{id}"])
            return true
            "#,
        )
        .eval()
        .expect("caller context and scoped prompts");
    assert!(ok);
}
