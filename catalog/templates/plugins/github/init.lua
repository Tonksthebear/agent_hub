-- @template GitHub Integration
-- @description Subscribe to GitHub events and trigger agent workflows from issues/PRs
-- @category plugins
-- @dest plugins/github/init.lua
-- @scope device
-- @version 3.1.1

-- GitHub Integration plugin entrypoint.
--
-- Product-specific GitHub behavior lives in this plugin directory. Core only
-- provides generic primitives: ActionCable, MCP proxying, hooks, HTTP, timers,
-- secrets, and agent/workspace helpers.

local mcp_proxy = require("mcp_proxy")
local notifications = require("notifications")
local event_routing = require("event_routing")

local ROUTE_REFRESH_SECS = 30
local route_refresh_timer = nil
local routed_repos_key = nil
local repo_detection_cache = {
    current_repo_checked = false,
    current_repo = nil,
    targets_key = nil,
    target_repos = {},
}

local function normalize_repo(repo)
    if type(repo) ~= "string" then
        return nil
    end
    repo = repo:gsub("^%s+", ""):gsub("%s+$", "")
    repo = repo:gsub("^git@github%.com:", "")
    repo = repo:gsub("^https://github%.com/", "")
    repo = repo:gsub("^http://github%.com/", "")
    repo = repo:gsub("%.git$", "")
    if repo:match("^[%w_.-]+/[%w_.-]+$") then
        return repo
    end
    return nil
end

local function add_repo(out, seen, repo)
    repo = normalize_repo(repo)
    if not repo or seen[repo] then
        return
    end
    seen[repo] = true
    out[#out + 1] = repo
end

local function spawn_targets_key(targets)
    local rows = {}
    for _, target in ipairs(targets or {}) do
        if type(target) == "table" then
            rows[#rows + 1] = table.concat({
                target.enabled == false and "disabled" or "enabled",
                tostring(target.id or target.name or ""),
                tostring(target.path or ""),
                tostring(target.repo or ""),
                tostring(target.target_repo or ""),
            }, "\0")
        end
    end
    table.sort(rows)
    return table.concat(rows, "\n")
end

local function detect_spawn_target_repos()
    local out = {}
    local seen = {}
    if not repo_detection_cache.current_repo_checked then
        repo_detection_cache.current_repo = hub.detect_repo()
        repo_detection_cache.current_repo_checked = true
    end
    add_repo(out, seen, repo_detection_cache.current_repo)

    local registry = rawget(_G, "spawn_targets")
    if type(registry) ~= "table" or type(registry.list) ~= "function" then
        return out
    end

    local ok, targets = pcall(registry.list)
    if not ok or type(targets) ~= "table" then
        return out
    end

    local key = spawn_targets_key(targets)
    if key == repo_detection_cache.targets_key then
        for _, repo in ipairs(repo_detection_cache.target_repos) do
            add_repo(out, seen, repo)
        end
        return out
    end

    local target_repos = {}
    local target_seen = {}
    for _, target in ipairs(targets) do
        if type(target) == "table" and target.enabled ~= false then
            add_repo(target_repos, target_seen, target.repo or target.target_repo)
            if type(target.path) == "string" and target.path ~= "" then
                local inspected = nil
                if type(registry.inspect) == "function" then
                    local inspect_ok, result = pcall(registry.inspect, target.path)
                    if inspect_ok and type(result) == "table" then
                        inspected = result
                    end
                end
                add_repo(target_repos, target_seen, inspected and inspected.repo_name)
                add_repo(target_repos, target_seen, hub.detect_repo(target.path))
            end
        end
    end

    repo_detection_cache.targets_key = key
    repo_detection_cache.target_repos = target_repos
    for _, repo in ipairs(target_repos) do
        add_repo(out, seen, repo)
    end

    return out
end

local function repos_key(repos)
    return table.concat(repos or {}, "\n")
end

local function refresh_event_routing(reason)
    local repos = detect_spawn_target_repos()
    local key = repos_key(repos)
    if key == routed_repos_key then
        return repos
    end

    routed_repos_key = key
    if #repos > 0 then
        event_routing.start(repos)
        log.info(string.format("GitHub plugin loaded for %s", table.concat(repos, ", ")))
    else
        event_routing.stop()
        log.info("GitHub plugin loaded without repo event routing")
    end
    if reason == "refresh" then
        log.info("GitHub plugin refreshed repo event routing")
    end
    return repos
end

mcp_proxy.start()
notifications.register()
refresh_event_routing("load")

if timer and type(timer.every) == "function" then
    route_refresh_timer = timer.every(ROUTE_REFRESH_SECS, function()
        local ok, err = pcall(refresh_event_routing, "refresh")
        if not ok then
            log.warn("GitHub plugin failed to refresh repo event routing: " .. tostring(err))
        end
    end)
end

return {
    _before_reload = function()
        if route_refresh_timer and timer and type(timer.cancel) == "function" then
            timer.cancel(route_refresh_timer)
            route_refresh_timer = nil
        end
        event_routing.stop()
        mcp_proxy.stop()
    end,
}
