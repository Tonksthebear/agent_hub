---
name: botster-install
description: Use when explaining first-time Botster installation for an agent, including device configuration, repo configuration, spawn targets, and initial agent definitions.
---

# Botster Install

Use this skill to guide a first-time Botster installation for an agent. The
goal is to explain the moving parts and help the user create the minimum
configuration needed for Botster to spawn agents in an admitted repo.

This skill is not a runtime health check. Do not turn first-time installation
help into MCP smoke tests or hub debugging unless the user explicitly asks for
diagnosis.

## Agent Plugin State

The agent plugin should provide this MCP server automatically:

```toml
[mcp_servers.botster]
url = "${BOTSTER_MCP_URL}"
bearer_token_env_var = "BOTSTER_MCP_TOKEN"
env_vars = ["BOTSTER_MCP_URL", "BOTSTER_MCP_TOKEN"]
default_tools_approval_mode = "approve"
default_tools_enabled = true
```

Do not add duplicate Botster MCP aliases. The Hub owns one HTTP listener.
Each session receives its own `BOTSTER_MCP_TOKEN`. Duplicate aliases make
tool discovery noisy without adding capability.

## CLI Installation

Use the root README's install script as the normal CLI installation path:

```bash
curl -fsSL https://raw.githubusercontent.com/Tonksthebear/trybotster/main/install.sh | sh
```

That script detects the platform, downloads the latest release, verifies the
checksum, and installs `botster` to `/usr/local/bin`.

Manual download is a fallback path only: download the matching release binary,
`chmod +x`, and move it to a directory in `PATH`.

## Device Configuration

Botster's device configuration lives in the active Botster config directory.
Use release paths for normal usage and debug paths for development builds:

- Release: `~/.botster/`
- Debug: `~/.botster-dev/`

Device configuration is where reusable, machine-wide Botster setup belongs:

- Agent definitions under `agents/<name>/initialization`.
- Device plugins under `plugins/<name>/init.lua`.
- User Lua customization under `lua/user/`.
- Spawn target admission for repos this device is allowed to run agents in.

Keep device configuration generic enough to reuse across repos. Repo-specific
automation belongs in repo configuration.

## Repo Configuration

Repo configuration lives under `<repo>/.botster/` when a project needs local
Botster behavior. Use it for repo-specific plugins, workflow defaults, or
coordination helpers that should travel with the repository.

Repo configuration should not duplicate the agent plugin's MCP server entry.
The agent plugin connects the agent runtime to the hub; repo configuration
customizes how Botster behaves for that repo.

## Spawn Targets

Botster should only spawn agents in admitted targets. A first-time install
should explain that the user needs to admit the repo or workspace root they
want Botster to manage before spawning agents there.

Spawn target setup answers these questions:

- Which repo paths may Botster use?
- Which worktree path should a spawned agent enter?
- Which branch or worktree creation policy should apply?
- Which agent definition should be used for that target?

## Agent Definitions

An agent definition describes how Botster starts a particular agent CLI. For a
first useful setup, create or select one definition under the active Botster
config directory, usually `agents/<name>/initialization`.

The initialization should:

- Change into `botster context worktree_path`.
- Launch the agent CLI with the task prompt from `botster context prompt`.
- Preserve the environment Botster provides to the spawned process, including
  `BOTSTER_SESSION_UUID`.

Do not hard-code a single repo path or prompt into a reusable agent definition.
Use Botster context values so the same definition can be used across admitted
spawn targets.

## Coordination Plugins

For multi-agent workflows, explain that runtime capabilities come from Botster
Lua plugins such as `orchestrator` and `messaging`. Installing this agent
plugin only connects an agent runtime to Botster and provides guidance skills;
it does not replace hub-side Lua plugins.

## First-Time Installation Checklist

1. Install the Botster CLI with the root README install script.
2. Install this agent plugin so the agent receives the `botster` MCP server
   configuration and Botster skills.
3. Choose the active Botster config directory: `~/.botster/` for release or
   `~/.botster-dev/` for debug builds.
4. Admit the repo or workspace root as a spawn target.
5. Create or select an agent definition under `agents/<name>/initialization`.
6. Add repo-local `.botster/` configuration only when the repo needs local
   Botster behavior.
7. Enable coordination plugins if the user wants multi-agent workflows.

## Boundaries

Botster is the hub and PTY orchestrator. Agent plugins configure agent-side
integration; Botster Lua plugins provide runtime MCP tools. Keep those layers
separate even when the user experience is one plugin install.
