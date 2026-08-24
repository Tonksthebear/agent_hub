---
name: botster-customize-mcp
description: Use when exposing MCP tools, prompts, or resource templates from Botster Lua plugins.
---

# Botster Customize MCP

Botster Lua plugins register MCP tools and prompts through `lib.mcp`. Agents
connect to them through the Hub-owned Streamable HTTP MCP server configured by
this agent plugin. Each caller presents `BOTSTER_MCP_TOKEN`. Tool calls stay
caller-specific.

## Tool

```lua
mcp.tool("tool_name", {
  description = "One-line description shown to the agent",
  input_schema = {
    type = "object",
    properties = {
      file = { type = "string", description = "Path to read" },
      limit = { type = "number", description = "Max results" },
    },
    required = { "file" },
  },
}, function(params, context)
  return {
    file = params.file,
    session_uuid = context.session_uuid,
  }
end)
```

The MCP context includes the calling `session_uuid` and `hub_id`. Use that
context for caller-scoped behavior; do not let a caller drain another agent's
private inbox unless the tool is explicitly administrative.

## Prompt

```lua
mcp.prompt("hub-context", {
  description = "Inject current hub state",
  arguments = {
    { name = "focus", description = "Optional focus area", required = false },
  },
}, function(args)
  return {
    description = "Current Botster hub state",
    messages = {
      {
        role = "user",
        content = {
          type = "text",
          text = "Hub context goes here",
        },
      },
    },
  }
end)
```

## Rules

- Last registration wins for a tool or prompt name.
- Plugin hot-reload clears and re-registers that plugin's MCP surface.
- Return structured data for agent coordination.
- Keep tool schemas narrow and explicit.
- Use plugin scoping to control which sessions can see plugin tools.

## MCP Registry API

- `mcp.tool(name, schema, handler)`
- `mcp.remove_tool(name)`
- `mcp.list_tools()`
- `mcp.call_tool(name, params, context)`
- `mcp.count()`
- `mcp.prompt(name, schema, handler)`
- `mcp.remove_prompt(name)`
- `mcp.list_prompts()`
- `mcp.get_prompt(name, args)`
- `mcp.count_prompts()`
- `mcp.resource(uri_template, schema, handler)`
- `mcp.remove_resource(uri_template)`
- `mcp.list_resource_templates()`
- `mcp.read_resource(uri, context)`
- `mcp.count_resources()`
- `mcp.proxy(url, opts)`
- `mcp.remove_proxy(url)`
- `mcp.begin_batch()`
- `mcp.end_batch()`
- `mcp.reset(source)`

## Context And Scoping

MCP tools are constructed per session from plugin scope:

- session UUID identifies the caller.
- target plugin ceiling limits available plugins.
- agent manifest plugin list can further restrict tools.
- if both target and agent list plugins, the visible set is their intersection.

Tools outside the resolved plugin set should not exist for that session.

## References

- `docs/lua/primitives.md` — `mcp` API and MCP server config.
- `docs/specs/device-hub-spawn-targets.md` — session plugin scoping model.
- `cli/lua/lib/mcp.lua` — canonical registry implementation.
