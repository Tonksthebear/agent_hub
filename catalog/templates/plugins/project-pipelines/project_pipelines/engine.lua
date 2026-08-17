-- @template Project Pipelines
-- @description Project and ticket pipeline management with gates, reviews, questions, and agent handoffs
-- @category plugins
-- @dest plugins/project-pipelines/project_pipelines/engine.lua
-- @scope device
-- @version 1.1.0

local repo = require("project_pipelines.repo")
local util = require("project_pipelines.util")
local entities = require("project_pipelines.entities")
local notification_policy = require("project_pipelines.notification_policy")
local Hub = require("lib.hub")
local Agent = require("lib.agent")

local OWNER = "project-pipelines"
local SURFACE = "pipelines"
local GITHUB_FORWARD_MARKER = "👀"

local function github_forwarded(text)
    if util.is_blank(text) then
        return GITHUB_FORWARD_MARKER
    end
    return GITHUB_FORWARD_MARKER .. " " .. tostring(text)
end

local function lua_pattern_escape(value)
    return tostring(value):gsub("([^%w])", "%%%1")
end

local function agent_branch_for(ticket, run, step)
    local base = ticket and ticket.id or run.id
    base = tostring(base):gsub("[^%w._-]", "-")
    base = base:gsub("%-+", "-"):gsub("^%-+", ""):gsub("%-+$", "")
    if util.is_blank(base) then
        base = tostring(run.id):gsub("[^%w._-]", "-")
    end
    return "project-pipelines/" .. base
end

local function ticket_branch_for(ticket, fallback)
    local base = ticket and ticket.id or fallback
    base = tostring(base):gsub("[^%w._-]", "-")
    base = base:gsub("%-+", "-"):gsub("^%-+", ""):gsub("%-+$", "")
    if util.is_blank(base) then
        base = tostring(fallback or "ticket"):gsub("[^%w._-]", "-")
    end
    return "project-pipelines/" .. base
end

local function ticket_workspace_name(ticket, fallback)
    local title = ticket and ticket.title or fallback or "Ticket"
    title = tostring(title):gsub("%s+", " "):gsub("^%s+", ""):gsub("%s+$", "")
    if util.is_blank(title) then
        title = tostring(ticket and ticket.id or fallback or "Ticket")
    end
    if #title > 64 then
        title = title:sub(1, 61):gsub("%s+$", "") .. "..."
    end
    return "Pipeline - " .. title
end

local function run_base_attrs(params)
    return {
        base_ticket_id = params.base_ticket_id,
        base_run_id = params.base_run_id or params.parent_run_id,
        base_ref = params.base_ref,
        base_target_path = params.base_target_path,
    }
end

local function run_merge_policy(run)
    local pipeline = run and repo.get_pipeline(run.pipeline_id) or {}
    local merge_policy = pipeline.merge_policy or "direct"
    if merge_policy ~= "direct" and merge_policy ~= "pr" then
        return "direct"
    end
    return merge_policy
end

local function live_ticket_worktree(ticket_id)
    for _, session_uuid in ipairs(repo.ticket_session_uuids(ticket_id)) do
        local session = Agent.get(session_uuid)
        if session and session.info then
            local ok, info = pcall(session.info, session)
            if ok and info and not util.is_blank(info.worktree_path) then
                return info.worktree_path, info.branch_name, info.workspace_id, info.workspace_name
            end
        end
    end
    return nil, nil, nil, nil
end

-- Invoke parent-owned command gates through the Hub facade. Workers do not
-- expose hub.run_command_gate on the isolated global hub table.
local function invoke_run_command_gate(opts)
    local hub_obj = Hub.get()
    if hub_obj and type(hub_obj.run_command_gate) == "function" then
        return hub_obj:run_command_gate(opts)
    end
    if type(hub) == "table" and type(hub.run_command_gate) == "function" then
        return hub.run_command_gate(opts)
    end
    error("run_command_gate unavailable: Hub facade and hub global are both nil")
end

-- Restore tracked .gitignore from HEAD only when the working copy is empty or
-- missing while HEAD still has content. Never truncate. Never overwrite a
-- non-empty intentional edit.
local function ensure_gitignore_hygiene(cwd, context)
    if util.is_blank(cwd) then
        return
    end
    local request_id = string.format(
        "%s:hygiene:gitignore:%s:%s",
        OWNER,
        tostring((context and context.run_id) or "none"),
        tostring(os.time())
    )
    -- shell: restore only when tracked and working copy is empty/missing
    local command = table.concat({
        "if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then exit 0; fi",
        "if ! git cat-file -e HEAD:.gitignore 2>/dev/null; then exit 0; fi",
        "if [ ! -s .gitignore ]; then git checkout HEAD -- .gitignore; fi",
    }, "; ")
    local ok, err = pcall(function()
        invoke_run_command_gate{
            request_id = request_id,
            command = command,
            cwd = cwd,
            timeout_secs = 30,
            context = {
                owner_plugin = OWNER,
                operation = "gitignore_hygiene",
                run_id = context and context.run_id or nil,
                ticket_id = context and context.ticket_id or nil,
            },
        }
    end)
    if not ok then
        log.warn("[project-pipelines] gitignore hygiene failed: " .. tostring(err))
    end
end

local function cargo_target_dir_for(cwd)
    if util.is_blank(cwd) or not tostring(cwd):find(":", 1, true) then
        return nil
    end
    local tmp = os.getenv("TMPDIR") or os.getenv("TMP") or "/tmp"
    local slug = tostring(cwd):gsub("[^%w]+", "-"):gsub("^%-", ""):gsub("%-$", "")
    if #slug > 48 then
        slug = slug:sub(1, 48)
    end
    if util.is_blank(slug) then
        slug = "pipeline"
    end
    return tmp:gsub("/+$", "") .. "/botster-cargo-target/" .. slug
end

local function ensure_worktree_hygiene(cwd, context)
    if util.is_blank(cwd) then
        return
    end
    ensure_gitignore_hygiene(cwd, context)
end

local M = {}

function M.register_entities()
    entities.register()
end

function M.publish_entity_snapshots()
    entities.publish_snapshots()
end

local function refresh_surfaces(ctx)
    if ctx and ctx.client and type(ctx.client.set_surface_subpath) == "function" then
        local subpath = "/"
        if type(ctx.client.surface_subpaths) == "table" and type(ctx.client.surface_subpaths[SURFACE]) == "string" then
            subpath = ctx.client.surface_subpaths[SURFACE]
        end
        pcall(ctx.client.set_surface_subpath, ctx.client, SURFACE, subpath, { rebroadcast = true })
        return ctx
    end

    local ok_snapshot, TreeSnapshot = pcall(require, "lib.tree_snapshot")
    if ok_snapshot and TreeSnapshot and type(TreeSnapshot.invalidate) == "function" then
        pcall(TreeSnapshot.invalidate, SURFACE)
    end
    return ctx
end

local function step_gate_status(run, step, gate)
    if gate.kind == "review_clear" then
        local review = repo.latest_review_for_run_step(run.current_run_step_id)
        if review and (review.verdict == "changes_required" or review.verdict == "blocked") then
            return { satisfied = true, status = "passed", source = "review_transition", review = review }
        end
        local blockers = repo.open_blocking_findings(run.id)
        if #blockers == 0 then
            return { satisfied = true, status = "passed", source = "review_clear" }
        end
        return { satisfied = false, status = "blocked", source = "review_clear", findings = blockers }
    end

    local latest = repo.latest_gate_result(run.id, gate.id, run.current_run_step_id)
    if latest and latest.status == "passed" then
        local missing = {}
        local evidence = util.decode(latest.evidence, {})
        for _, field in ipairs(util.decode(gate.required_fields, {})) do
            if field == "summary" then
                if util.is_blank(latest.summary) then
                    table.insert(missing, field)
                end
            elseif field == "evidence" then
                if type(evidence) ~= "table" or next(evidence) == nil then
                    table.insert(missing, field)
                end
            elseif type(evidence) ~= "table" or util.is_blank(evidence[field]) then
                table.insert(missing, field)
            end
        end
        if #missing > 0 then
            return { satisfied = false, status = "missing_required_fields", result = latest, missing_fields = missing }
        end
        return { satisfied = true, status = latest.status, result = latest }
    end
    if latest and latest.status == "failed" then
        return { satisfied = false, status = latest.status, result = latest }
    end
    return { satisfied = false, status = latest and latest.status or "missing", result = latest }
end

local function unmet_gates(run, step)
    local unmet = {}
    for _, gate in ipairs(repo.step_gates(step.id)) do
        local status = step_gate_status(run, step, gate)
        if not status.satisfied then
            table.insert(unmet, {
                id = gate.id,
                kind = gate.kind,
                prompt = gate.prompt,
                required_fields = util.decode(gate.required_fields, {}),
                status = status.status,
                missing_fields = status.missing_fields,
                finding_count = status.findings and #status.findings or nil,
            })
        end
    end
    return unmet
end

local function has_review_kickback(run, step)
    local review = repo.latest_review_for_run_step(run.current_run_step_id)
    if not review then
        return false
    end
    if review.verdict == "changes_required" then
        return not util.is_blank(step.on_changes_requested_step_id)
    end
    if review.verdict == "blocked" then
        return not util.is_blank(step.on_blocked_step_id or step.on_changes_requested_step_id)
    end
    return false
end

local function forced_next_step(run, params)
    if util.is_blank(params.next_step_id) then
        return nil
    end
    local step = repo.get_step(params.next_step_id)
    if not step then
        error("next_step_id not found: " .. tostring(params.next_step_id))
    end
    if step.pipeline_id ~= run.pipeline_id then
        error("next_step_id must belong to the same pipeline")
    end
    return step
end

local function context_from_params(params, context)
    local session_uuid = context and context.session_uuid or params.session_uuid
    local run_id = params.run_id
    local assignment = nil
    if util.is_blank(run_id) and not util.is_blank(session_uuid) then
        assignment = repo.find_active_assignment(session_uuid)
        run_id = assignment and assignment.run_id or nil
    end

    local run = not util.is_blank(run_id) and repo.get_run(run_id) or nil
    local ticket_id = params.ticket_id or (run and run.ticket_id)
    local ticket = not util.is_blank(ticket_id) and repo.get_ticket(ticket_id) or nil
    if not ticket then
        error("ticket_id or active pipeline assignment is required")
    end

    local run_step = run and run.current_run_step_id and repo.get_run_step_visit(run.current_run_step_id) or nil
    return {
        session_uuid = session_uuid,
        assignment = assignment,
        run = run,
        ticket = ticket,
        run_step = run_step,
        step_id = run_step and run_step.step_id or (assignment and assignment.step_id),
    }
end

function M.context_for(run_id, session_uuid)
    local assignment = nil
    if util.is_blank(run_id) then
        assignment = repo.find_active_assignment(session_uuid)
        run_id = assignment and assignment.run_id or nil
    end
    util.assert_present(run_id, "run_id")

    local run = repo.get_run(run_id)
    if not run then
        error("run not found: " .. tostring(run_id))
    end
    local ticket = repo.get_ticket(run.ticket_id)
    local pipeline = repo.get_pipeline(run.pipeline_id)
    local current_step = run.current_step_id and repo.get_step(run.current_step_id) or nil
    local current_run_step = run.current_run_step_id and repo.get_run_step_visit(run.current_run_step_id) or nil
    local gates = current_step and repo.step_gates(current_step.id) or {}
    local visible_gates = {}
    for _, gate in ipairs(gates) do
        table.insert(visible_gates, {
            id = gate.id,
            kind = gate.kind,
            prompt = gate.prompt,
            required_fields = util.decode(gate.required_fields, {}),
            latest_result = repo.latest_gate_result(run.id, gate.id, run.current_run_step_id),
        })
    end

    return {
        run = run,
        ticket = ticket,
        pipeline = pipeline,
        current_step = current_step,
        current_run_step = current_run_step,
        gates = visible_gates,
        run_steps = repo.run_steps(run.id),
        reviews = repo.run_reviews(run.id),
        current_step_reviews = repo.run_step_reviews(run.current_run_step_id),
        latest_review_for_current_step = repo.latest_review_for_run_step(run.current_run_step_id),
        findings = repo.run_findings(run.id),
        open_findings = repo.open_findings(run.id),
        artifacts = repo.run_artifacts(run.id),
        open_questions = repo.ticket_questions(run.ticket_id, "open"),
        questions = repo.ticket_questions(run.ticket_id),
        question_answers = repo.question_answers({ run_id = run.id, asked_by_session_uuid = session_uuid }),
        dependencies = repo.ticket_dependencies(run.ticket_id),
        blocking_dependencies = repo.blocking_ticket_dependencies(run.ticket_id),
        ticket_checklists = repo.list_checklists{ scope = "ticket", owner_id = run.ticket_id },
        run_checklists = repo.list_checklists{ scope = "run", owner_id = run.id },
        recent_events = repo.run_events(run.id, 25),
        assignment = assignment,
    }
end

-- Shared fail-closed ticket-dependency preflight for activation, advance, retry,
-- start_run, and last-line spawn defense. Reads normalized
-- repo.ticket_dependencies rows (depends_on_status from LEFT JOIN).
local function unmet_ticket_dependencies(ticket_id)
    local unmet = {}
    local dependencies = repo.ticket_dependencies(ticket_id)
    for _, dependency in ipairs(dependencies) do
        if dependency.depends_on_status ~= "closed" then
            local unavailable = util.is_blank(dependency.depends_on_status)
            local reason = unavailable and "unavailable_ticket" or "open_ticket"
            local label = dependency.depends_on_title or dependency.depends_on_ticket_id
            local prompt
            if unavailable then
                prompt = "Restore or remove unavailable dependency ticket " .. tostring(dependency.depends_on_ticket_id) .. " before continuing."
            else
                prompt = "Close dependency ticket " .. tostring(label) .. " (" .. tostring(dependency.depends_on_ticket_id) .. ", status: " .. tostring(dependency.depends_on_status) .. ") before continuing."
            end
            unmet[#unmet + 1] = {
                dependency_id = dependency.id,
                depends_on_ticket_id = dependency.depends_on_ticket_id,
                depends_on_title = dependency.depends_on_title,
                depends_on_status = dependency.depends_on_status,
                reason = reason,
                prompt = prompt,
            }
        end
    end
    return unmet
end

local function dependency_block(run, attrs)
    local unmet = unmet_ticket_dependencies(run.ticket_id)
    if #unmet == 0 then
        return nil
    end

    local payload = {
        step_id = attrs.step_id,
        run_step_id = attrs.run_step_id,
        target_step_id = attrs.target_step_id,
        source = attrs.source,
        reason = "ticket_dependencies",
        unmet_dependencies = unmet,
    }
    repo.append_event("step.advance_blocked", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = payload,
    })
    return {
        ok = false,
        status = "blocked",
        reason = "ticket_dependencies",
        step_id = attrs.step_id,
        run_step_id = attrs.run_step_id,
        target_step_id = attrs.target_step_id,
        source = attrs.source,
        unmet_dependencies = unmet,
    }
end

local function spawn_step_agent(run, step, opts)
    opts = opts or {}
    if util.is_blank(step.agent_name) then
        return nil, "step has no agent_name"
    end
    if util.is_blank(run.target_id) then
        return nil, "target_id is required before spawning agent steps"
    end

    -- Last-line defense for callers that already mutated state (retry) or that
    -- skip activate preflight. Returns a structured dependency block (not a
    -- plain string) so callers preserve reason=ticket_dependencies + ids.
    -- activate_step runs this check before target side effects when
    -- skip_dependency_check is not set; after side effects it passes
    -- skip_dependency_check=true so a race cannot leave a half-activated target.
    if opts.skip_dependency_check ~= true then
        local blocked = dependency_block(run, {
            step_id = step.id,
            run_step_id = run.current_run_step_id,
            target_step_id = step.id,
            source = opts.dependency_source or "spawn_step_agent",
        })
        if blocked then
            return nil, blocked
        end
    end

    local ticket = repo.get_ticket(run.ticket_id)
    local current_visit = run.current_run_step_id and repo.get_run_step_visit(run.current_run_step_id) or nil
    local existing = repo.latest_step_session(run.id, step.id)
    local role_prompt = table.concat({
        "You are running a Botster project pipeline step.",
        "Pipeline step: " .. step.name .. " (" .. step.id .. ")",
        "Ticket: " .. (ticket and ticket.title or run.ticket_id),
        "Role instructions: " .. (step.prompt or ""),
        "Use the project_pipelines_current_context MCP tool to inspect ticket, run, gates, reviews, artifacts, findings, questions, and question answers.",
        "Use Project Pipelines checklists to prove workflow discipline. Vault checklists should record which vault/project notes constrained the work, convention conflicts or none, verification evidence, and whether durable knowledge was captured.",
        "State assumptions explicitly in artifacts/reviews. If the ticket has multiple plausible meanings or you would need to ignore/waive part of it, ask a human question instead of choosing silently.",
        "Prefer the smallest surgical change that satisfies the ticket intent. Do not add speculative abstractions, optional configurability, broad refactors, or adjacent cleanup unless the ticket requires them.",
        "Define and preserve verifiable success criteria. Every changed line should trace to the ticket intent, a required convention, or cleanup made necessary by your own change.",
        "Prove the actual runtime or user path changed. Evidence that code exists is not enough; identify where the production entry point uses the new behavior, or document why the ticket is intentionally scaffold-only.",
        "If you need human help, call project_pipelines_ask_human with a concise blocking question, then wait for a Project Pipelines notification and call project_pipelines_receive_question_answers.",
        "If you need another agent's opinion, call project_pipelines_ask_agent with the specific question and context you want reviewed, then wait for a Project Pipelines notification and call project_pipelines_receive_question_answers.",
        "When work is ready for advancement, submit the required gate evidence with project_pipelines_submit_gate, then call project_pipelines_request_step_advance.",
        "If you are reviewing, check correctness, regressions, architecture fit, missing tests, documentation gaps, overcomplication, hidden assumptions, dead code, deprecated code paths, and unwired implementation. Do not accept pre-existing failures as a blanket excuse; require exact evidence that a failure is unrelated or send the work back. Call project_pipelines_submit_review with findings or approval.",
    }, "\n\n")
    if not util.is_blank(opts.extra_prompt) then
        role_prompt = role_prompt .. "\n\n" .. github_forwarded(opts.extra_prompt)
    end

    if existing and existing.agent_session_uuid and Agent.get(existing.agent_session_uuid) then
        local ok, err = pcall(function()
            Hub.get():post(existing.agent_session_uuid, {
                type = "task",
                from_agent_id = OWNER,
                from_label = "Project Pipelines",
                payload = {
                    run_id = run.id,
                    ticket_id = run.ticket_id,
                    step_id = step.id,
                    instructions = role_prompt,
                },
            })
            Hub.get():notify(existing.agent_session_uuid, {
                source = OWNER,
                title = opts.notification_title or "Pipeline step returned",
                body = opts.notification_body or ("Ticket " .. tostring(ticket and ticket.title or run.ticket_id) .. " is back at " .. tostring(step.name) .. "."),
                action = {
                    kind = "mcp_tool",
                    name = "project_pipelines_current_context",
                    params = { run_id = run.id },
                },
            })
        end)
        repo.append_event("step.agent_prompted", {
            run_id = run.id,
            ticket_id = run.ticket_id,
            payload = {
                step_id = step.id,
                session_uuid = existing.agent_session_uuid,
                delivered = ok,
                error = ok and nil or tostring(err),
                source_event = opts.source_event,
            },
        })
        if current_visit then
            repo.update_run_step_visit(current_visit.id, { agent_session_uuid = existing.agent_session_uuid })
        end
        return { session_uuid = existing.agent_session_uuid, reused = true }, nil
    end

    local request_id = string.format("%s:%s:%s:agent", OWNER, run.id, step.id)
    local ok, created = pcall(function()
        return Hub.get():create_agent{
            request_id = request_id,
            agent_name = step.agent_name,
            issue_or_branch = agent_branch_for(ticket, run, step),
            base_ref = run.base_ref,
            base_target_path = run.base_target_path,
            target_id = run.target_id,
            workspace_id = run.workspace_id,
            workspace_name = run.workspace_name or ticket_workspace_name(ticket, run.id),
            label = step.name .. " - " .. (ticket and ticket.title or run.id),
            prompt = role_prompt,
            metadata = {
                owner_plugin = OWNER,
                visibility = "plugin",
                surface = SURFACE,
                ticket_id = run.ticket_id,
                run_id = run.id,
                step_id = step.id,
                pipeline_id = run.pipeline_id,
                base_ticket_id = run.base_ticket_id,
                base_run_id = run.base_run_id,
                base_ref = run.base_ref,
                base_target_path = run.base_target_path,
                role = step.id,
            },
        }
    end)

    if not ok then
        return nil, tostring(created)
    end

    repo.append_event("step.agent_requested", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = {
            step_id = step.id,
            agent_name = step.agent_name,
            request_id = request_id,
            status = created and created.status or "queued",
        },
    })
    return {
        ok = true,
        request_id = request_id,
        status = created and created.status or "queued",
    }, nil
end

local function live_question_orchestrator(project_id, excluded_session_uuid)
    local candidates = {}
    if not util.is_blank(project_id) then
        candidates[#candidates + 1] = repo.get_question_orchestrator(project_id)
    end
    candidates[#candidates + 1] = repo.get_question_orchestrator(nil)
    local first_assignment = nil
    for _, assignment in ipairs(candidates) do
        if assignment then
            first_assignment = first_assignment or assignment
            local session = assignment.session_uuid ~= excluded_session_uuid
                and Agent.get(assignment.session_uuid) or nil
            if session then
                return assignment, session, first_assignment
            end
        end
    end
    return nil, nil, first_assignment
end

local function session_label(session, session_uuid)
    if type(session) ~= "table" then
        return session_uuid
    end
    local info = nil
    if type(session.info) == "function" then
        local ok, value = pcall(session.info, session)
        if ok then info = value end
    end
    return session.label
        or session.display_name
        or session.name
        or session.agent_name
        or (type(info) == "table" and (info.label or info.display_name or info.name or info.agent_name))
        or session_uuid
end

function M.claim_question_orchestrator(params, context)
    params = params or {}
    local session_uuid = context and context.session_uuid or params.session_uuid
    util.assert_present(session_uuid, "calling session_uuid")
    local session = Agent.get(session_uuid)
    if not session then
        error("calling agent session is not active: " .. tostring(session_uuid))
    end

    local current = repo.get_question_orchestrator(params.project_id)
    local replace = params.replace == true
    if current and current.session_uuid ~= session_uuid and not Agent.get(current.session_uuid) then
        replace = true
    end
    local assignment = repo.claim_question_orchestrator{
        project_id = params.project_id,
        session_uuid = session_uuid,
        session_label = session_label(session, session_uuid),
        replace = replace,
    }
    refresh_surfaces(context)
    return {
        assignment = assignment,
        scope = assignment.scope,
        message = assignment.scope == "project"
            and "This session now owns questions for project " .. tostring(assignment.project_id) .. "."
            or "This session now owns globally unassigned Project Pipelines questions.",
    }
end

function M.release_question_orchestrator(params, context)
    params = params or {}
    local session_uuid = context and context.session_uuid or params.session_uuid
    util.assert_present(session_uuid, "calling session_uuid")
    local assignment = repo.get_question_orchestrator(params.project_id)
    if not assignment then
        return { released = false, reason = "not_claimed" }
    end
    if assignment.session_uuid ~= session_uuid and params.force ~= true then
        error("question orchestrator is owned by " .. tostring(assignment.session_label or assignment.session_uuid))
    end
    repo.release_question_orchestrator(params.project_id)
    refresh_surfaces(context)
    return { released = true, assignment = assignment }
end

function M.question_orchestrator_status()
    local assignments = {}
    for _, assignment in ipairs(repo.list_question_orchestrators()) do
        assignment.active = Agent.get(assignment.session_uuid) ~= nil
        assignments[#assignments + 1] = assignment
    end
    return assignments
end

function M.escalate_question(params, context)
    params = params or {}
    local question = repo.get_question(util.assert_present(params.question_id, "question_id"))
    if not question then
        error("question not found: " .. tostring(params.question_id))
    end
    if question.status ~= "open" then
        error("question is not open: " .. tostring(question.status))
    end
    local session_uuid = context and context.session_uuid or params.session_uuid
    local ticket = repo.get_ticket(question.ticket_id)
    local run = question.run_id and repo.get_run(question.run_id) or nil
    local updated = repo.update_question(question.id, {
        kind = "human",
        status = "open",
    })
    repo.append_event("question.escalated", {
        run_id = question.run_id,
        ticket_id = question.ticket_id,
        payload = {
            question_id = question.id,
            reason = params.reason,
            by_session_uuid = session_uuid,
            previous_advisor_session_uuid = question.advisor_session_uuid,
        },
    })
    notification_policy.notify_question_asked({
        question = updated,
        ticket = ticket,
        run = run,
        step_id = question.step_id,
    })
    refresh_surfaces(context)
    return updated
end

function M.ask_human(params, context)
    local resolved = context_from_params(params or {}, context or {})
    local question = repo.create_question{
        ticket_id = resolved.ticket.id,
        run_id = resolved.run and resolved.run.id or nil,
        run_step_id = resolved.run_step and resolved.run_step.id or nil,
        step_id = resolved.step_id,
        kind = "human",
        question = util.assert_present(params.question, "question"),
        asked_by_session_uuid = resolved.session_uuid,
        blocking = params.blocking ~= false,
    }
    local assignment, orchestrator, attempted_assignment =
        live_question_orchestrator(resolved.ticket.project_id, resolved.session_uuid)
    local routed = false
    local route_error = nil
    if orchestrator and assignment.session_uuid ~= resolved.session_uuid then
        local ok, err = pcall(function()
            Hub.get():post(assignment.session_uuid, {
                type = "task",
                from_agent_id = OWNER,
                from_label = "Project Pipelines",
                payload = {
                    question_id = question.id,
                    ticket_id = question.ticket_id,
                    run_id = question.run_id,
                    project_id = resolved.ticket.project_id,
                    question = question.question,
                    instructions = table.concat({
                        "You are the active Project Pipelines question orchestrator.",
                        "Investigate this question using the ticket and run context, delegating to the context-owning ticket session when useful.",
                        "Answer with project_pipelines_answer_question.",
                        "If human judgment is required, call project_pipelines_escalate_question instead of guessing.",
                    }, " "),
                },
            })
            Hub.get():notify(assignment.session_uuid, {
                source = OWNER,
                title = "Pipeline question needs an answer",
                body = tostring(resolved.ticket.title or resolved.ticket.id) .. ": " .. tostring(question.question),
                action = {
                    kind = "mcp_tool",
                    name = "project_pipelines_get_ticket",
                    params = { ticket_id = resolved.ticket.id },
                },
            })
        end)
        routed = ok
        route_error = ok and nil or tostring(err)
        if ok then
            question = repo.update_question(question.id, {
                advisor_session_uuid = assignment.session_uuid,
            })
        end
    end
    repo.append_event(routed and "question.orchestrator_routed" or "question.orchestrator_fallback", {
        run_id = question.run_id,
        ticket_id = question.ticket_id,
        payload = {
            question_id = question.id,
            session_uuid = (assignment or attempted_assignment) and (assignment or attempted_assignment).session_uuid or nil,
            scope = (assignment or attempted_assignment) and (assignment or attempted_assignment).scope or nil,
            reason = not attempted_assignment and "unclaimed"
                or (not orchestrator and "inactive_or_asking_session")
                or route_error,
        },
    })
    if not routed then
        notification_policy.notify_question_asked({
            question = question,
            ticket = resolved.ticket,
            run = resolved.run,
            step_id = resolved.step_id,
        })
    end
    refresh_surfaces()
    return question
end

function M.ask_agent(params, context)
    local resolved = context_from_params(params or {}, context or {})
    local question = repo.create_question{
        ticket_id = resolved.ticket.id,
        run_id = resolved.run and resolved.run.id or nil,
        run_step_id = resolved.run_step and resolved.run_step.id or nil,
        step_id = resolved.step_id,
        kind = "agent",
        question = util.assert_present(params.question, "question"),
        asked_by_session_uuid = resolved.session_uuid,
        blocking = params.blocking == true,
    }

    local prompt_parts = {
        "You are a Project Pipelines question advisor.",
        "Question ID: " .. question.id,
        "Ticket: " .. resolved.ticket.title,
        "Question:",
        question.question,
        "Use project_pipelines_get_ticket and project_pipelines_current_context if you need more context.",
        "Answer by calling project_pipelines_answer_question with this question_id. Keep the answer direct and actionable.",
    }
    if resolved.run then
        table.insert(prompt_parts, 4, "Run: " .. resolved.run.id)
    end
    local prompt = table.concat(prompt_parts, "\n\n")

    local request_id = string.format("%s:%s:question:%s:agent", OWNER, resolved.ticket.id, question.id)
    local ok, created = pcall(function()
        return Hub.get():create_agent{
            request_id = request_id,
            agent_name = params.agent_name or "grok",
            issue_or_branch = ticket_branch_for(resolved.ticket, question.id),
            base_ref = resolved.run and resolved.run.base_ref or nil,
            base_target_path = resolved.run and resolved.run.base_target_path or nil,
            target_id = resolved.ticket.target_id,
            workspace_id = resolved.run and resolved.run.workspace_id or params.workspace_id,
            workspace_name = params.workspace_name or (resolved.run and resolved.run.workspace_name) or ticket_workspace_name(resolved.ticket, question.id),
            label = "Question - " .. resolved.ticket.title,
            prompt = prompt,
            metadata = {
                owner_plugin = OWNER,
                visibility = "plugin",
                surface = SURFACE,
                ticket_id = resolved.ticket.id,
                run_id = resolved.run and resolved.run.id or nil,
                base_ref = resolved.run and resolved.run.base_ref or nil,
                base_target_path = resolved.run and resolved.run.base_target_path or nil,
                question_id = question.id,
                role = "question_advisor",
            },
        }
    end)
    if not ok then
        repo.update_question(question.id, { status = "open", answer = "Advisor spawn failed: " .. tostring(created) })
        error(tostring(created))
    end

    repo.append_event("question.agent_requested", {
        run_id = resolved.run and resolved.run.id or nil,
        ticket_id = resolved.ticket.id,
        payload = {
            question_id = question.id,
            request_id = request_id,
            status = created and created.status or "queued",
        },
    })
    refresh_surfaces()
    return question
end

function M.answer_question(params, context)
    util.assert_present(params.question_id, "question_id")
    util.assert_present(params.answer, "answer")
    local question = repo.update_question(params.question_id, {
        status = params.status or "answered",
        answer = params.answer,
        answered_by_session_uuid = context and context.session_uuid or params.answered_by_session_uuid,
    })
    if question and not util.is_blank(question.asked_by_session_uuid) then
        local ok, err = pcall(function()
            Hub.get():notify(question.asked_by_session_uuid, {
                source = "project-pipelines",
                title = question.status == "dismissed" and "Question dismissed" or "Question answered",
                body = "A Project Pipelines question was updated for ticket " .. tostring(question.ticket_id) .. ".",
                action = {
                    kind = "mcp_tool",
                    name = "project_pipelines_receive_question_answers",
                    params = { question_id = question.id },
                },
            })
        end)
        if not ok then
            log.warn("[project-pipelines] question answer notification failed: " .. tostring(err))
        end
    end
    refresh_surfaces()
    return question
end

function M.question_answers(params, context)
    params = params or {}
    return repo.question_answers({
        ticket_id = params.ticket_id,
        run_id = params.run_id,
        question_id = params.question_id,
        status = params.status,
        asked_by_session_uuid = params.all == true and nil or (params.asked_by_session_uuid or (context and context.session_uuid)),
    })
end

function M.request_merge(params, context)
    local ticket = repo.get_ticket(util.assert_present(params.ticket_id, "ticket_id"))
    if not ticket then
        error("ticket not found: " .. tostring(params.ticket_id))
    end
    local run = repo.latest_ticket_run(ticket.id)
    if not run or run.status ~= "done" then
        error("ticket must have a completed latest run before merge")
    end
    local merge_policy = run_merge_policy(run)
    for _, event in ipairs(repo.ticket_events(ticket.id, "ticket.merge_requested", 5)) do
        local payload = util.decode(event.payload, {})
        if payload.session_uuid and Agent.get(payload.session_uuid) then
            return {
                ticket = ticket,
                run = run,
                merge_policy = merge_policy,
                agent = Agent.get(payload.session_uuid):info(),
                request_id = payload.request_id,
                status = "already_requested",
            }
        end
    end

    local prompt = table.concat({
        "You are the Project Pipelines merge agent and PR steward.",
        "Ticket: " .. ticket.title,
        "Run: " .. run.id,
        "Merge policy: " .. merge_policy,
        "You are the final acceptance gate and the ongoing steward for the provider PR, not just a Git operator. Re-read the ticket title, ticket description, latest run context, final signoff, review findings, artifacts, branch diff, tests, docs, and merge target before creating, updating, or merging a PR.",
        "Judge the work against the intent and meaning of the ticket, not only the literal checklist. If the ticket asked for a new architecture, replacement, or cleanup, verify the old architecture, dead paths, deprecated code, stale docs, stale tests, compatibility shims, and contradictory examples are removed or explicitly human-waived.",
        "Reject hidden assumptions, speculative scope, broad unrelated refactors, or overcomplicated implementation unless the ticket or a human-approved waiver justifies them.",
        "Reject weak evidence. Success criteria must be explicit and verified; evidence must prove the actual production/user/runtime path uses the change, not merely that new helpers, modules, or tests exist.",
        "For runtime, async, data-plane, control-plane, UI-routing, permission, or architecture migration work, require production path proof: entry point, actor/mailbox or controller/job/component path, fallback behavior, old direct path removed or test-fenced, and logs/tests/smoke evidence exercising the path.",
        "Treat stub wiring as incomplete. A new mailbox, component, policy, or helper that immediately delegates to the old production path is not accepted unless the ticket explicitly says scaffold-only or a human waived it.",
        "For hot attach/connect/snapshot/input paths, require explicit latency or race evidence when relevant. New or retained fixed sleeps on hot paths require measurement-backed justification.",
        "All actionable review findings must be resolved or explicitly human-waived before merge. Do not accept 'acceptable workaround', 'future follow-up', 'not necessary', or similar reasoning from an agent unless you agree it is outside the ticket intent or a human has waived it through Project Pipelines questions.",
        "Be proactive about the affected surface area. Confirm the implementation is wired into the actual runtime paths, not merely added beside the old behavior.",
        "As merge agent and PR steward, you are an orchestrator between the human, the provider PR, source control, and the ticket's existing agent team. Do not implement code changes yourself. Do not answer architectural questions from your own judgment alone when an existing ticket agent has relevant context; route them to the appropriate existing planner, implementer, reviewer, or verifier and synthesize the answer back to the PR/human.",
        "Use the repo's conventions for merge strategy and verification.",
        merge_policy == "pr"
            and "This pipeline requires a PR. Create or update the PR only through the Botster MCP PR tools. Do not merge directly. Do not use gh, hub, direct GitHub API calls, browser automation, or manual web UI actions for PR creation or PR updates."
            or "This pipeline requires a direct merge to main. If the acceptance check passes, merge according to the repo's direct-merge convention and do not open a PR.",
        merge_policy == "pr"
            and "After opening or updating the PR, you remain the PR steward. Submitted PR reviews and PR comments may be delivered back to you. Triage them before delegating: answer informational comments on the PR, send actionable code feedback back to the implementer or another ticket agent, and keep the conversation on the PR when the human asked or said something there. Use Project Pipelines human questions only for decisions that did not originate on the PR, or when you need a durable pipeline-level waiver/decision record in addition to the PR reply."
            or "",
        merge_policy == "pr"
            and "Coordinate with agents that worked on this ticket using Botster MCP. Use project_pipelines_get_ticket or project_pipelines_current_context to find ticket sessions and run steps, list_hubs to inspect live agents, post_message to send structured tasks, and notify_session to wake the target agent. Delegate implementation work to the existing implementer, architectural/product reasoning to the planner or other context-owning agent, verification questions to the verifier, and review interpretation to the reviewer when available. Use project_pipelines_ask_agent only when no existing ticket agent owns the needed context."
            or "",
        merge_policy == "pr"
            and "When delegating PR review feedback, include the linked PR, review URL/body, required changes, expected evidence, and instruction to update the existing PR branch. Do not create a new run or a new PR for ordinary PR review revisions."
            or "",
        merge_policy == "pr" and not util.is_blank(run.base_ref)
            and ("This is stacked pipeline work. Open or update the PR against base_ref `" .. tostring(run.base_ref) .. "`, not main.")
            or "",
        merge_policy == "pr"
            and "If the Botster MCP PR tools are unavailable, ask a human question and wait instead of creating the PR another way."
            or "If direct merge is blocked by conflicts or repo state, add a blocker artifact or ask a human question and wait.",
        "If there are conflicts, incomplete intent coverage, ignored findings, dead/deprecated code, unwired implementation, stale documentation, stale tests, or verification failures, do not merge. Add a project_pipelines_add_artifact blocker summary with exact files, lines when available, and verification attempted; ask a human question only when a waiver or product decision is genuinely needed.",
        "Do not accept pre-existing failures unless you prove with exact evidence they are unrelated to this ticket.",
        "After a successful direct merge or PR creation, add a project_pipelines_add_artifact summary with the merge commit or PR URL.",
        merge_policy == "pr"
            and "Do not close the ticket after opening or updating a PR. Link the PR with project_pipelines_link_pr, then leave the ticket open until the provider pr_merged event closes it."
            or "Close the ticket only when the direct merge is genuinely complete by calling project_pipelines_close_ticket with merge_confirmed=true and include merge_commit or merge_summary when available.",
    }, "\n\n")

    local request_id = string.format("%s:%s:merge:agent", OWNER, ticket.id)
    local ok, created = pcall(function()
        return Hub.get():create_agent{
            request_id = request_id,
            agent_name = params.agent_name or "codex",
            issue_or_branch = ticket_branch_for(ticket, run.id),
            base_ref = run.base_ref,
            base_target_path = run.base_target_path,
            target_id = run.target_id or ticket.target_id,
            workspace_id = run.workspace_id or params.workspace_id,
            workspace_name = params.workspace_name or run.workspace_name or ticket_workspace_name(ticket, run.id),
            label = "Merge - " .. ticket.title,
            prompt = prompt,
            metadata = {
                owner_plugin = OWNER,
                visibility = "plugin",
                surface = SURFACE,
                ticket_id = ticket.id,
                run_id = run.id,
                base_ticket_id = run.base_ticket_id,
                base_run_id = run.base_run_id,
                base_ref = run.base_ref,
                base_target_path = run.base_target_path,
                role = "merge",
            },
        }
    end)
    if not ok then
        error(tostring(created))
    end

    repo.append_event("ticket.merge_requested", {
        run_id = run.id,
        ticket_id = ticket.id,
        payload = {
            request_id = request_id,
            status = created and created.status or "queued",
            strategy = params.strategy or "agent",
            merge_policy = merge_policy,
            base_ref = run.base_ref,
            base_ticket_id = run.base_ticket_id,
            base_run_id = run.base_run_id,
        },
    })
    refresh_surfaces(context)
    return { ticket = ticket, run = run, merge_policy = merge_policy, agent = created, request_id = request_id }
end

local function link_manual_ticket_session(ticket_id, session_uuid, request_id, attrs)
    if util.is_blank(ticket_id) or util.is_blank(session_uuid) or util.is_blank(request_id) then
        return false
    end
    for _, event in ipairs(repo.ticket_events(ticket_id, "ticket.manual_session_linked", 25)) do
        local payload = util.decode(event.payload, {})
        if payload.session_uuid == session_uuid and payload.request_id == request_id then
            return false
        end
    end
    attrs = attrs or {}
    repo.append_event("ticket.manual_session_linked", {
        ticket_id = ticket_id,
        run_id = attrs.run_id,
        payload = {
            session_uuid = session_uuid,
            request_id = request_id,
            session_type = attrs.session_type,
            role = attrs.role,
            agent_name = attrs.agent_name,
            accessory_name = attrs.accessory_name,
        },
    })
    return true
end

function M.spawn_ticket_session(params, context)
    params = params or {}
    local ticket = repo.get_ticket(util.assert_present(params.ticket_id, "ticket_id"))
    if not ticket then
        error("ticket not found: " .. tostring(params.ticket_id))
    end
    if util.is_blank(ticket.target_id) then
        error("ticket has no spawn target")
    end

    local latest_run = repo.latest_ticket_run(ticket.id)
    local branch = ticket_branch_for(ticket, latest_run and latest_run.id or nil)
    local from_worktree, branch_name, workspace_id, workspace_name = live_ticket_worktree(ticket.id)
    local request_id = params.request_id or ("project-pipelines:" .. ticket.id .. ":manual:" .. tostring(os.time()))
    local session_type = params.session_type or "agent"
    local result

    if session_type == "accessory" then
        local accessory_name = params.accessory_name or "terminal"
        result = Hub.get():create_accessory{
            request_id = request_id,
            accessory_name = accessory_name,
            target_id = ticket.target_id,
            workspace_id = params.workspace_id or workspace_id,
            workspace_name = params.workspace_name or workspace_name or ticket_workspace_name(ticket, latest_run and latest_run.id),
            from_worktree = from_worktree,
            branch = branch_name or branch,
            metadata = {
                owner_plugin = OWNER,
                visibility = "plugin",
                surface = SURFACE,
                ticket_id = ticket.id,
                run_id = latest_run and latest_run.id or nil,
                role = "manual-accessory",
            },
        }
        repo.append_event("ticket.manual_accessory_requested", {
            run_id = latest_run and latest_run.id or nil,
            ticket_id = ticket.id,
            payload = { request_id = request_id, accessory_name = accessory_name, session_uuid = result and result.session_uuid },
        })
        link_manual_ticket_session(ticket.id, result and result.session_uuid, request_id, {
            run_id = latest_run and latest_run.id or nil,
            session_type = "accessory",
            role = "manual-accessory",
            accessory_name = accessory_name,
        })
    else
        local prompt = params.prompt
        if util.is_blank(prompt) then
            prompt = table.concat({
                "You are joining a Project Pipelines ticket worktree.",
                "Ticket: " .. ticket.title,
                "Call project_pipelines_get_ticket and project_pipelines_current_context if this ticket has a run.",
                "Stay within the ticket intent and coordinate through Project Pipelines tools.",
            }, "\n\n")
        end
        result = Hub.get():create_agent{
            request_id = request_id,
            agent_name = params.agent_name or "codex",
            issue_or_branch = branch,
            from_worktree = from_worktree,
            target_id = ticket.target_id,
            workspace_id = params.workspace_id or workspace_id,
            workspace_name = params.workspace_name or workspace_name or ticket_workspace_name(ticket, latest_run and latest_run.id),
            label = "Assist - " .. ticket.title,
            prompt = prompt,
            metadata = {
                owner_plugin = OWNER,
                visibility = "plugin",
                surface = SURFACE,
                ticket_id = ticket.id,
                run_id = latest_run and latest_run.id or nil,
                role = "manual-agent",
            },
        }
        repo.append_event("ticket.manual_agent_requested", {
            run_id = latest_run and latest_run.id or nil,
            ticket_id = ticket.id,
            payload = { request_id = request_id, agent_name = params.agent_name or "codex", session_uuid = result and result.session_uuid },
        })
        link_manual_ticket_session(ticket.id, result and result.session_uuid, request_id, {
            run_id = latest_run and latest_run.id or nil,
            session_type = "agent",
            role = "manual-agent",
            agent_name = params.agent_name or "codex",
        })
    end

    refresh_surfaces(context)
    return { ticket = ticket, run = latest_run, session = result, request_id = request_id, session_type = session_type }
end

function M.delete_manual_ticket_session(params, context)
    params = params or {}
    local ticket_id = util.assert_present(params.ticket_id, "ticket_id")
    local session_uuid = util.assert_present(params.session_uuid, "session_uuid")
    local ticket = repo.get_ticket(ticket_id)
    if not ticket then
        error("ticket not found: " .. tostring(ticket_id))
    end

    local linked = false
    for _, event in ipairs(repo.ticket_events(ticket_id, "ticket.manual_session_linked", 200)) do
        local payload = util.decode(event.payload, {})
        if payload.session_uuid == session_uuid then
            linked = true
            break
        end
    end
    if not linked then
        error("session is not a manual ticket session")
    end

    for _, event in ipairs(repo.ticket_events(ticket_id, "ticket.manual_session_removed", 200)) do
        local payload = util.decode(event.payload, {})
        if payload.session_uuid == session_uuid then
            refresh_surfaces(context)
            return { ticket = ticket, session_uuid = session_uuid, removed = true, already_removed = true }
        end
    end

    local session = Agent.get(session_uuid)
    if session then
        Hub.get():delete_agent(session_uuid, false)
    end

    repo.append_event("ticket.manual_session_removed", {
        ticket_id = ticket_id,
        run_id = params.run_id,
        payload = {
            session_uuid = session_uuid,
            reason = params.reason,
            closed = session ~= nil,
        },
    })
    refresh_surfaces(context)
    return { ticket = ticket, session_uuid = session_uuid, removed = true, closed = session ~= nil }
end

local function shell_quote(value)
    return "'" .. tostring(value):gsub("'", "'\\''") .. "'"
end

local function merged_ticket_location(context)
    local merge_target = util.is_blank(context.base_branch) and "main" or tostring(context.base_branch)
    if util.is_blank(context.merge_commit) then
        return merge_target
    end
    return merge_target .. " at " .. tostring(context.merge_commit)
end

local function command_gate_output(data)
    return tostring(data and (data.stdout_tail or data.stdout or data.output) or "")
end

local function notify_merge_delivery(context)
    context = context or {}
    local location = merged_ticket_location(context)
    local title = tostring(context.ticket_title or context.ticket_id or "Ticket")
    local delivered = false
    if not util.is_blank(context.session_uuid) then
        local ok = pcall(function()
            Hub.get():notify(context.session_uuid, {
                source = OWNER,
                title = "Pipeline ticket merged",
                body = "Ticket “" .. title .. "” merged into " .. location .. ".",
                action = {
                    kind = "ticket_merged",
                    ticket_id = context.ticket_id,
                    project_id = context.project_id,
                    merge_commit = context.merge_commit,
                    pr_url = context.pr_url,
                    merge_summary = context.merge_summary,
                    checkout_path = context.checkout_path,
                },
            })
        end)
        delivered = ok
    end
    if not util.is_blank(context.session_uuid) then
        repo.append_event("ticket.merge_orchestrator_notified", {
            run_id = context.run_id,
            ticket_id = context.ticket_id,
            payload = {
                session_uuid = context.session_uuid,
                scope = context.scope,
                delivered = delivered,
            },
        })
    end
    return delivered
end

local function complete_merge_refresh(context, refresh_status, data)
    context = context or {}
    data = data or {}
    local delivered = false
    if (refresh_status == "conflicted" or refresh_status == "failed")
        and not util.is_blank(context.session_uuid) then
        local title = refresh_status == "conflicted"
            and "Merged ticket has checkout conflicts"
            or "Merged ticket checkout refresh failed"
        local body = refresh_status == "conflicted"
            and "Git reports merge conflicts in checkout " .. tostring(context.checkout_path) .. "."
            or "Git could not refresh checkout " .. tostring(context.checkout_path) .. "."
        if not util.is_blank(data.error) then
            body = body .. " Error: " .. tostring(data.error)
        end
        delivered = pcall(function()
            Hub.get():notify(context.session_uuid, {
                source = OWNER,
                title = title,
                body = body,
                action = {
                    kind = "ticket_merge_checkout_refresh",
                    ticket_id = context.ticket_id,
                    project_id = context.project_id,
                    merge_commit = context.merge_commit,
                    checkout_path = context.checkout_path,
                    refresh_status = refresh_status,
                },
            })
        end)
    end

    repo.append_event("ticket.merge_source_pull_completed", {
        run_id = context.run_id,
        ticket_id = context.ticket_id,
        payload = {
            session_uuid = context.session_uuid,
            scope = context.scope,
            checkout_path = context.checkout_path,
            base_branch = context.base_branch,
            pull_status = refresh_status,
            error = data.error,
            success = refresh_status == "updated",
            delivered = delivered,
        },
    })
    return { pull_status = refresh_status, delivered = delivered }
end

local function run_merge_refresh_gate(context, operation, command, timeout_secs, env)
    local next_context = {}
    for key, value in pairs(context or {}) do
        next_context[key] = value
    end
    next_context.operation = operation
    local opts = {
        request_id = tostring(context.request_id) .. ":" .. operation,
        command = command,
        cwd = context.checkout_path,
        timeout_secs = timeout_secs or 30,
        context = next_context,
    }
    if env then
        opts.env = env
    end
    local ok, result = pcall(function()
        return invoke_run_command_gate(opts)
    end)
    if not ok then
        complete_merge_refresh(context, "failed", { error = tostring(result) })
        return nil, tostring(result)
    end
    return result, nil
end

local function merge_base_branch(run, attrs)
    attrs = attrs or {}
    if not util.is_blank(attrs.base_branch) then
        return tostring(attrs.base_branch):gsub("^origin/", "")
    end
    if run and not util.is_blank(run.base_ref) then
        local ref = tostring(run.base_ref):gsub("^origin/", "")
        if not util.is_blank(ref) then
            return ref
        end
    end
    return "main"
end

local function session_checkout_path(session)
    if not session then
        return nil
    end
    local info = nil
    if type(session.info) == "function" then
        local ok, value = pcall(session.info, session)
        if ok then info = value end
    end
    if type(info) ~= "table" then
        return nil
    end
    if not util.is_blank(info.worktree_path) then
        return info.worktree_path
    end
    if not util.is_blank(info.cwd) then
        return info.cwd
    end
    if not util.is_blank(info.target_path) then
        return info.target_path
    end
    return nil
end

--- After a ticket merge, attempt `git pull` in source checkouts so local trees
-- pick up the merged base. Prefer the live question orchestrator checkout, and
-- also try the ticket's resolved source target path when distinct.
local function attempt_merge_upstream_pulls(ticket, run, attrs)
    if not ticket then
        return {}
    end
    attrs = attrs or {}
    local base_branch = merge_base_branch(run, attrs)
    local targets = {}
    local seen = {}

    local function add_target(path, session_uuid, scope)
        if util.is_blank(path) then
            return
        end
        local key = tostring(path)
        if seen[key] then
            return
        end
        seen[key] = true
        targets[#targets + 1] = {
            checkout_path = key,
            session_uuid = session_uuid,
            scope = scope,
        }
    end

    local assignment, session = live_question_orchestrator(ticket.project_id)
    if assignment and session then
        add_target(session_checkout_path(session), assignment.session_uuid, assignment.scope or "orchestrator")
    end
    add_target(repo.resolve_target_path(ticket.target_id), nil, "source_target")

    notify_merge_delivery({
        session_uuid = assignment and assignment.session_uuid or nil,
        scope = assignment and assignment.scope or "orchestrator",
        ticket_id = ticket.id,
        ticket_title = ticket.title,
        project_id = ticket.project_id,
        run_id = run and run.id or nil,
        merge_commit = attrs.merge_commit,
        base_branch = base_branch,
        pr_url = attrs.pr_url,
        merge_summary = attrs.merge_summary or attrs.summary,
    })

    local results = {}
    for _, target in ipairs(targets) do
        local request_id = string.format(
            "%s:%s:merge-source-pull:%s:%s",
            OWNER,
            ticket.id,
            tostring(attrs.merge_commit or base_branch):gsub("[^%w._-]", "-"),
            tostring(target.scope or "path"):gsub("[^%w._-]", "-")
        )
        local context = {
            owner_plugin = OWNER,
            operation = "merge_source_branch",
            request_id = request_id,
            session_uuid = target.session_uuid,
            scope = target.scope,
            checkout_path = target.checkout_path,
            ticket_id = ticket.id,
            ticket_title = ticket.title,
            project_id = ticket.project_id,
            run_id = run and run.id or nil,
            merge_commit = attrs.merge_commit,
            base_branch = base_branch,
            pr_url = attrs.pr_url,
            merge_summary = attrs.merge_summary or attrs.summary,
        }
        local result, err = run_merge_refresh_gate(
            context,
            "merge_source_branch",
            "git branch --show-current",
            30
        )
        if err then
            results[#results + 1] = { checkout_path = target.checkout_path, started = false, error = err }
        else
            repo.append_event("ticket.merge_source_pull_started", {
                run_id = context.run_id,
                ticket_id = context.ticket_id,
                payload = {
                    request_id = request_id,
                    session_uuid = context.session_uuid,
                    scope = context.scope,
                    checkout_path = target.checkout_path,
                    base_branch = base_branch,
                },
            })
            results[#results + 1] = { checkout_path = target.checkout_path, started = true, request_id = request_id }
        end
    end
    return results
end

function M.close_ticket(ticket_id, attrs)
    util.assert_present(ticket_id, "ticket_id")
    attrs = attrs or {}
    local ticket = repo.get_ticket(ticket_id)
    if not ticket then
        error("ticket not found: " .. tostring(ticket_id))
    end
    local latest_run = repo.latest_ticket_run(ticket_id)
    if latest_run and latest_run.status == "done" and attrs.merge_confirmed ~= true then
        error("completed pipeline work must be merged before closing; start merge or close with merge_confirmed=true from the merge agent")
    end
    if latest_run and latest_run.status == "done" and attrs.merge_confirmed == true and run_merge_policy(latest_run) == "pr" then
        local open_links = repo.list_pr_links{ ticket_id = ticket_id, status = "open" }
        if #open_links > 0 then
            error("PR-backed tickets close only after the linked PR is merged; linked PR is still open")
        end
        local merged_links = repo.list_pr_links{ ticket_id = ticket_id, status = "merged" }
        if #merged_links == 0 then
            error("PR-backed tickets close only after Project Pipelines has a linked merged PR")
        end
    end

    local closed_sessions = {}
    local errors = {}
    for _, session_uuid in ipairs(repo.ticket_session_uuids(ticket_id)) do
        local ok, result = pcall(function()
            return Hub.get():delete_agent(session_uuid, false)
        end)
        if ok then
            table.insert(closed_sessions, { session_uuid = session_uuid, result = result })
        else
            table.insert(errors, { session_uuid = session_uuid, error = tostring(result) })
        end
    end

    if latest_run and attrs.merge_confirmed == true then
        local summary = attrs.merge_summary or attrs.summary or "Merge confirmed before ticket closure."
        local payload = attrs.merge_payload or {
            merge_confirmed = true,
            merge_commit = attrs.merge_commit,
            pr_url = attrs.pr_url,
            closed_by_session_uuid = attrs.closed_by_session_uuid,
        }
        repo.add_artifact{
            run_id = latest_run.id,
            kind = attrs.merge_kind or "merge",
            uri = attrs.pr_url,
            summary = summary,
            payload = payload,
        }
    end

    local updated = repo.close_ticket(ticket_id, {
        merge_confirmed = attrs.merge_confirmed == true,
        closed_sessions = closed_sessions,
        errors = errors,
        merge_commit = attrs.merge_commit,
        pr_url = attrs.pr_url,
        merge_summary = attrs.merge_summary or attrs.summary,
    })

    local merge_pulls = nil
    if attrs.merge_confirmed == true then
        local ok_pull, pull_result = pcall(attempt_merge_upstream_pulls, ticket, latest_run, attrs)
        if ok_pull then
            merge_pulls = pull_result
        else
            log.warn("[project-pipelines] merge source pull failed to start: " .. tostring(pull_result))
            merge_pulls = { error = tostring(pull_result) }
        end
    end

    refresh_surfaces()
    return {
        ticket = updated,
        closed_sessions = closed_sessions,
        errors = errors,
        merge_pulls = merge_pulls,
    }
end

local function run_command_step(run, step)
    local command = step.command
    local command_gate = nil
    for _, gate in ipairs(repo.step_gates(step.id)) do
        if gate.kind == "command" then
            command_gate = gate
            command = gate.command or command
            break
        end
    end
    if util.is_blank(command) then
        return nil, "command step has no command"
    end
    local target_path = repo.resolve_target_path(run.target_id)
    if util.is_blank(target_path) then
        return nil, "could not resolve a filesystem path for command step (target_id=" .. tostring(run.target_id) .. ")"
    end

    ensure_worktree_hygiene(target_path, { run_id = run.id, ticket_id = run.ticket_id })

    local env = nil
    local cargo_target = cargo_target_dir_for(target_path)
    if cargo_target then
        env = { CARGO_TARGET_DIR = cargo_target }
    end

    local request_id = string.format("%s:%s:%s:command:%s", OWNER, run.id, step.id, command_gate and command_gate.id or "step")
    local ok, result = pcall(function()
        local opts = {
            request_id = request_id,
            command = command,
            cwd = target_path,
            timeout_secs = 600,
            context = {
                owner_plugin = OWNER,
                run_id = run.id,
                run_step_id = run.current_run_step_id,
                step_id = step.id,
                gate_id = command_gate and command_gate.id or nil,
            },
        }
        if env then
            opts.env = env
        end
        return invoke_run_command_gate(opts)
    end)

    if not ok then
        return nil, tostring(result)
    end
    repo.append_event("step.command_started", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = {
            step_id = step.id,
            command = command,
            request_id = request_id,
            cargo_target_dir = cargo_target,
        },
    })
    return result, nil
end

function M.activate_step(run, step, options)
    if not step then
        repo.update_run(run.id, { status = "done", current_step_id = nil, current_run_step_id = nil })
        repo.append_event("run.completed", { run_id = run.id, ticket_id = run.ticket_id, payload = {} })
        notification_policy.notify_phase_transition({
            run_id = run.id,
            ticket_id = run.ticket_id,
            ticket = repo.get_ticket(run.ticket_id),
        })
        if run_merge_policy(run) == "pr" and #repo.list_pr_links{ ticket_id = run.ticket_id, status = "open" } > 0 then
            repo.append_event("ticket.pr_revision_ready", {
                run_id = run.id,
                ticket_id = run.ticket_id,
                payload = { reason = "open_pr_link_exists" },
            })
            return { ok = true, status = "done", pr_revision_ready = true }
        end
        local merge_ok, merge_result = pcall(function()
            return M.request_merge({ ticket_id = run.ticket_id }, {})
        end)
        if not merge_ok then
            repo.append_event("ticket.merge_request_failed", {
                run_id = run.id,
                ticket_id = run.ticket_id,
                payload = { error = tostring(merge_result) },
            })
            return { ok = true, status = "done", merge_error = tostring(merge_result) }
        end
        return { ok = true, status = "done", merge = merge_result }
    end

    options = options or {}
    local source = options.source or "activate_step"
    local blocked = dependency_block(run, {
        step_id = run.current_step_id,
        run_step_id = run.current_run_step_id,
        target_step_id = step.id,
        source = source,
    })
    if blocked then
        return blocked
    end

    -- Final fail-closed check immediately before any target side effects so a
    -- spawn-time dependency block cannot leave a visit, run pointer change,
    -- step.activated event, or phase notification.
    if step.kind == "agent" or step.kind == "command" then
        blocked = dependency_block(run, {
            step_id = run.current_step_id,
            run_step_id = run.current_run_step_id,
            target_step_id = step.id,
            source = source,
        })
        if blocked then
            return blocked
        end
    end

    local now = util.now()
    local visit = repo.create_run_step_visit(run.id, step.id, { status = "active", started_at = now })
    repo.update_run(run.id, { status = "active", current_step_id = step.id, current_run_step_id = visit.id })
    local activation_payload = { step_id = step.id, run_step_id = visit.id, sequence = visit.sequence, kind = step.kind, name = step.name }
    for key, value in pairs(options.activation_payload or {}) do
        activation_payload[key] = value
    end
    repo.append_event("step.activated", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = activation_payload,
    })
    notification_policy.notify_phase_transition({
        run_id = run.id,
        ticket_id = run.ticket_id,
        ticket = repo.get_ticket(run.ticket_id),
        step = step,
        run_step = visit,
    })

    -- Hygiene on activation when a live ticket worktree already exists.
    -- Never truncate .gitignore; restore from HEAD only when empty/missing.
    local live_path = live_ticket_worktree(run.ticket_id)
    if not util.is_blank(live_path) then
        ensure_worktree_hygiene(live_path, { run_id = run.id, ticket_id = run.ticket_id })
    end

    if step.kind == "agent" then
        local spawn_options = {}
        for key, value in pairs(options.spawn_options or {}) do
            spawn_options[key] = value
        end
        -- Deps already cleared immediately above; do not re-check after side
        -- effects (that path would leave a half-activated target).
        spawn_options.skip_dependency_check = true
        local created, err = spawn_step_agent(repo.get_run(run.id), step, spawn_options)
        if err then
            -- Structured dependency blocks should not reach here after the
            -- pre-side-effect check; preserve them if a caller mis-wires spawn.
            if type(err) == "table" and err.reason == "ticket_dependencies" then
                return err
            end
            repo.update_run(run.id, { status = "blocked" })
            repo.update_run_step_visit(visit.id, { status = "blocked" })
            repo.append_event("step.spawn_failed", {
                run_id = run.id,
                ticket_id = run.ticket_id,
                payload = { step_id = step.id, run_step_id = visit.id, source_event = options.source_event, error = err },
            })
            return { ok = false, status = "blocked", run_step_id = visit.id, error = err }
        end
        return { ok = true, status = "active", agent = created }
    end

    if step.kind == "command" then
        local result, err = run_command_step(repo.get_run(run.id), step)
        if err then
            repo.update_run(run.id, { status = "blocked" })
            repo.update_run_step(run.id, step.id, { status = "blocked" })
            repo.append_event("step.command_failed_to_start", {
                run_id = run.id,
                ticket_id = run.ticket_id,
                payload = { step_id = step.id, error = err },
            })
            return { ok = false, error = err }
        end
        return { ok = true, status = "active", command = result }
    end

    return { ok = true, status = "active" }
end

local function likely_implementation_step(run)
    local fallback = nil
    for _, step in ipairs(repo.pipeline_steps(run.pipeline_id)) do
        if step.kind == "agent" then
            fallback = fallback or step
            local text = string.lower(table.concat({
                tostring(step.id or ""),
                tostring(step.name or ""),
                tostring(step.agent_name or ""),
                tostring(step.prompt or ""),
            }, " "))
            if text:find("implement", 1, true) or text:find("build", 1, true) or text:find("code", 1, true) then
                return step
            end
        end
    end
    return fallback
end

local function pr_review_extra_prompt(ticket, link, event)
    local state = tostring(event.state or "commented")
    local lines = {
        github_forwarded("A GitHub PR review was submitted for this ticket's linked PR."),
        "Review state: " .. state,
        "PR: " .. tostring(event.pr_url or link.pr_url or (tostring(link.repo) .. "#" .. tostring(link.pr_number))),
    }
    if not util.is_blank(event.review_html_url or event.review_url) then
        lines[#lines + 1] = "Review: " .. tostring(event.review_html_url or event.review_url)
    end
    if not util.is_blank(event.reviewer) then
        lines[#lines + 1] = "Reviewer: " .. tostring(event.reviewer)
    end
    if not util.is_blank(event.body) then
        lines[#lines + 1] = "Review body:\n" .. tostring(event.body)
    end
    lines[#lines + 1] = "Update the existing PR branch in this ticket worktree. Answer through the PR when appropriate. Do not create a new run or a new PR."
    lines[#lines + 1] = "When the revision is ready, submit current gate evidence and advance the pipeline. Because the linked PR is already open, completion should update the existing PR instead of spawning a new merge agent."
    if ticket then
        table.insert(lines, 2, "Ticket: " .. tostring(ticket.title or ticket.id))
    end
    return table.concat(lines, "\n\n")
end

local function pr_steward_review_prompt(ticket, link, event)
    local state = tostring(event.state or "commented")
    local lines = {
        github_forwarded("A GitHub PR review was submitted for a PR you steward for this Project Pipelines ticket."),
        "Ticket: " .. tostring(ticket and (ticket.title or ticket.id) or link.ticket_id),
        "Review state: " .. state,
        "PR: " .. tostring(event.pr_url or link.pr_url or (tostring(link.repo) .. "#" .. tostring(link.pr_number))),
    }
    if not util.is_blank(event.review_html_url or event.review_url) then
        lines[#lines + 1] = "Review: " .. tostring(event.review_html_url or event.review_url)
    end
    if not util.is_blank(event.reviewer) then
        lines[#lines + 1] = "Reviewer: " .. tostring(event.reviewer)
    end
    if not util.is_blank(event.body) then
        lines[#lines + 1] = "Review body:\n" .. tostring(event.body)
    end
    lines[#lines + 1] = "You are the PR steward. Triage this through project_pipelines_current_context and project_pipelines_get_ticket before acting."
    lines[#lines + 1] = "You are an orchestrator, not the implementer. Do not implement PR feedback yourself, and do not answer architecture/product questions from your own judgment alone when an existing ticket agent has relevant context."
    lines[#lines + 1] = "If the feedback is informational, answer on the PR using the GitHub MCP tools after checking the appropriate ticket context. If the human asked or said something through the PR, ask clarifying follow-up questions and provide answers on that PR thread instead of switching to project_pipelines_ask_human. If it needs code changes, delegate to the existing implementer with Botster MCP messaging tools such as list_hubs, post_message, and notify_session, and route the pipeline back to implementation when needed. If it is architectural/product reasoning, delegate to the existing planner or other context-owning ticket agent and synthesize their answer back to the PR. Use Project Pipelines human questions only for non-PR-originated decisions or when a durable pipeline-level waiver/decision record is required in addition to the PR reply."
    lines[#lines + 1] = "Do not create a new PR or new ticket run for ordinary PR review revisions. Keep the existing linked PR as the delivery envelope."
    return table.concat(lines, "\n\n")
end

local function pr_steward_comment_prompt(ticket, link, event)
    local action = tostring(event.action or "created")
    local lines = {
        github_forwarded("A GitHub PR conversation comment was " .. action .. " on a PR you steward for this Project Pipelines ticket."),
        "Ticket: " .. tostring(ticket and (ticket.title or ticket.id) or link.ticket_id),
        "PR: " .. tostring(event.pr_url or link.pr_url or (tostring(link.repo) .. "#" .. tostring(link.pr_number))),
    }
    if not util.is_blank(event.comment_url) then
        lines[#lines + 1] = "Comment: " .. tostring(event.comment_url)
    end
    if not util.is_blank(event.comment_author) then
        lines[#lines + 1] = "Author: " .. tostring(event.comment_author)
    end
    if not util.is_blank(event.comment_body) then
        lines[#lines + 1] = "Comment body:\n" .. tostring(event.comment_body)
    end
    lines[#lines + 1] = "You are the PR steward. Triage this through project_pipelines_current_context and project_pipelines_get_ticket before acting."
    lines[#lines + 1] = "If the human asked or said something through the PR, answer on that PR thread using the GitHub MCP tools after checking the appropriate ticket context. If it needs code changes, delegate to the existing implementer with Botster MCP messaging tools such as list_hubs, post_message, and notify_session, and route the pipeline back to implementation when needed."
    lines[#lines + 1] = "Do not create a new PR or new ticket run for ordinary PR conversation comments. Keep the existing linked PR as the delivery envelope."
    return table.concat(lines, "\n\n")
end

local function live_merge_agent(ticket_id)
    for _, event in ipairs(repo.ticket_events(ticket_id, "ticket.merge_agent_linked", 25)) do
        local payload = util.decode(event.payload, {})
        if not util.is_blank(payload.session_uuid) and Agent.get(payload.session_uuid) then
            return payload.session_uuid
        end
    end
    for _, event in ipairs(repo.ticket_events(ticket_id, "ticket.merge_requested", 25)) do
        local payload = util.decode(event.payload, {})
        if not util.is_blank(payload.session_uuid) and Agent.get(payload.session_uuid) then
            return payload.session_uuid
        end
    end
    return nil
end

local function notify_merge_agent_of_pr_review(ticket, run, link, event)
    local session_uuid = live_merge_agent(ticket.id)
    if util.is_blank(session_uuid) then
        return nil
    end
    local state = string.lower(tostring(event.state or ""))
    local instructions = pr_steward_review_prompt(ticket, link, event)
    local ok, err = pcall(function()
        Hub.get():post(session_uuid, {
            type = "task",
            from_agent_id = OWNER,
            from_label = "Project Pipelines",
            payload = {
                run_id = run.id,
                ticket_id = ticket.id,
                pr_link_id = link.id,
                source_event = "pr_review_submitted",
                review_state = state,
                instructions = instructions,
            },
        })
        Hub.get():notify(session_uuid, {
            source = OWNER,
            title = state == "changes_requested" and "PR changes requested" or (state == "approved" and "PR approved" or "PR review submitted"),
            body = github_forwarded("GitHub PR review feedback is ready for PR steward triage on ticket " .. tostring(ticket.title or ticket.id) .. "."),
            action = {
                kind = "mcp_tool",
                name = "project_pipelines_current_context",
                params = { run_id = run.id },
            },
        })
    end)
    repo.append_event("ticket.pr_review_steward_prompted", {
        run_id = run.id,
        ticket_id = ticket.id,
        payload = {
            pr_link_id = link.id,
            session_uuid = session_uuid,
            review_state = state,
            delivered = ok,
            error = ok and nil or tostring(err),
        },
    })
    if ok then
        return { session_uuid = session_uuid, prompted = true }
    end
    return nil
end

local function notify_merge_agent_of_pr_comment(ticket, run, link, event)
    local session_uuid = live_merge_agent(ticket.id)
    if util.is_blank(session_uuid) then
        return nil
    end
    local instructions = pr_steward_comment_prompt(ticket, link, event)
    local ok, err = pcall(function()
        Hub.get():post(session_uuid, {
            type = "task",
            from_agent_id = OWNER,
            from_label = "Project Pipelines",
            payload = {
                run_id = run.id,
                ticket_id = ticket.id,
                pr_link_id = link.id,
                source_event = "pr_comment",
                comment_id = event.comment_id,
                comment_action = event.action,
                instructions = instructions,
            },
        })
        Hub.get():notify(session_uuid, {
            source = OWNER,
            title = "PR comment " .. tostring(event.action or "created"),
            body = github_forwarded("GitHub PR conversation comment is ready for PR steward triage on ticket " .. tostring(ticket.title or ticket.id) .. "."),
            action = {
                kind = "mcp_tool",
                name = "project_pipelines_current_context",
                params = { run_id = run.id },
            },
        })
    end)
    repo.append_event("ticket.pr_comment_steward_prompted", {
        run_id = run.id,
        ticket_id = ticket.id,
        payload = {
            pr_link_id = link.id,
            session_uuid = session_uuid,
            comment_id = event.comment_id,
            comment_action = event.action,
            delivered = ok,
            error = ok and nil or tostring(err),
        },
    })
    if ok then
        return { session_uuid = session_uuid, prompted = true }
    end
    return nil
end

function M.handle_pr_review_submitted(link, event)
    link = link or {}
    event = event or {}
    local ticket = repo.get_ticket(link.ticket_id)
    local run = link.run_id and repo.get_run(link.run_id) or nil
    if not run and ticket then
        run = repo.latest_ticket_run(ticket.id)
    end
    if not ticket or not run then
        return { ok = false, reason = "missing_ticket_or_run" }
    end

    local state = string.lower(tostring(event.state or ""))
    repo.append_event("ticket.pr_review_submitted", {
        run_id = run.id,
        ticket_id = ticket.id,
        payload = {
            pr_link_id = link.id,
            provider = link.provider,
            repo = link.repo,
            pr_number = link.pr_number,
            pr_url = event.pr_url or link.pr_url,
            review_id = event.review_id,
            review_url = event.review_html_url or event.review_url,
            reviewer = event.reviewer,
            state = state,
            body = event.body,
            submitted_at = event.submitted_at,
        },
    })

    local steward = notify_merge_agent_of_pr_review(ticket, run, link, event)
    if steward then
        refresh_surfaces()
        return { ok = true, status = "steward_prompted", ticket = ticket, run = run, pr_steward = steward }
    end

    if state ~= "changes_requested" and state ~= "commented" then
        refresh_surfaces()
        return { ok = true, status = "recorded", ticket = ticket, run = run }
    end

    local step = likely_implementation_step(run)
    if not step then
        refresh_surfaces()
        return { ok = false, reason = "no_implementation_step", ticket = ticket, run = run }
    end

    local activation = M.activate_step(run, step, {
        source = "pr_review_submitted",
        source_event = "pr_review_submitted",
        activation_payload = {
            source_event = "pr_review_submitted",
            pr_review_state = state,
            pr_link_id = link.id,
        },
        spawn_options = {
            source_event = "pr_review_submitted",
            extra_prompt = pr_review_extra_prompt(ticket, link, event),
            notification_title = state == "changes_requested" and "PR changes requested" or "PR review commented",
            notification_body = "GitHub review feedback was sent back to " .. tostring(step.name) .. " for ticket " .. tostring(ticket.title or ticket.id) .. ".",
        },
    })

    refresh_surfaces()
    activation.ticket = ticket
    activation.run = repo.get_run(run.id)
    activation.step = step
    if activation.ok and activation.run and not util.is_blank(activation.run.current_run_step_id) then
        activation.run_step = repo.get_run_step_visit(activation.run.current_run_step_id)
    elseif activation.run_step_id then
        activation.run_step = repo.get_run_step_visit(activation.run_step_id)
    end
    if activation.ok then
        activation.status = "reactivated"
    end
    return activation
end

function M.handle_pr_comment(link, event)
    link = link or {}
    event = event or {}
    local ticket = repo.get_ticket(link.ticket_id)
    local run = link.run_id and repo.get_run(link.run_id) or nil
    if not run and ticket then
        run = repo.latest_ticket_run(ticket.id)
    end
    if not ticket or not run then
        return { ok = false, reason = "missing_ticket_or_run" }
    end

    repo.append_event("ticket.pr_comment", {
        run_id = run.id,
        ticket_id = ticket.id,
        payload = {
            pr_link_id = link.id,
            provider = link.provider,
            repo = link.repo,
            pr_number = link.pr_number,
            pr_url = event.pr_url or link.pr_url,
            comment_id = event.comment_id,
            comment_url = event.comment_url,
            comment_author = event.comment_author,
            comment_body = event.comment_body,
            action = event.action,
            created_at = event.created_at,
            updated_at = event.updated_at,
        },
    })

    local steward = notify_merge_agent_of_pr_comment(ticket, run, link, event)
    refresh_surfaces()
    if steward then
        return { ok = true, status = "steward_prompted", ticket = ticket, run = run, pr_steward = steward }
    end

    return { ok = true, status = "recorded", reason = "no_steward", ticket = ticket, run = run }
end

function M.cancel_run(params, context)
    util.assert_present(params.run_id, "run_id")

    local run = repo.get_run(params.run_id)
    if not run then
        error("run not found: " .. tostring(params.run_id))
    end

    if run.status == "cancelled" then
        return {
            ok = true,
            status = "cancelled",
            already_cancelled = true,
            run = run,
        }
    end

    if run.status == "done" then
        return {
            ok = false,
            status = "done",
            reason = "run_already_done",
            error = "done runs cannot be cancelled",
            run = run,
        }
    end

    if run.status ~= "active" and run.status ~= "blocked" then
        return {
            ok = false,
            status = run.status,
            reason = "run_not_cancellable",
            error = "only active or blocked runs can be cancelled",
            run = run,
        }
    end

    local previous_status = run.status
    local cancelled = repo.update_run(run.id, { status = "cancelled" })
    repo.append_event("run.cancelled", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = {
            previous_status = previous_status,
            cancelled_by_session_uuid = context and context.session_uuid or nil,
        },
    })
    refresh_surfaces(context)

    return {
        ok = true,
        status = "cancelled",
        already_cancelled = false,
        run = cancelled,
    }
end

function M.start_run(params)
    repo.prune_legacy_seed_data()
    local pipeline_id = params.pipeline_id
    if util.is_blank(pipeline_id) then
        local default = repo.get_default_pipeline()
        pipeline_id = default and default.id or nil
    end
    util.assert_present(pipeline_id, "pipeline_id")
    if not repo.get_ticket(params.ticket_id) then
        error("ticket not found: " .. tostring(params.ticket_id))
    end
    local ticket = repo.get_ticket(params.ticket_id)
    local open_run = repo.open_ticket_run(params.ticket_id)
    if open_run and (open_run.status == "active" or open_run.status == "blocked") then
        error("ticket already has an open run: " .. tostring(open_run.id))
    end
    if util.is_blank(ticket.target_id) then
        error("ticket target_id is required before starting a run")
    end
    -- Same authority as activate/advance/retry/spawn: unmet_ticket_dependencies.
    -- Public MCP must receive a typed blocked result that names dependency ticket ids.
    local blockers = unmet_ticket_dependencies(params.ticket_id)
    if #blockers > 0 then
        return {
            ok = false,
            status = "blocked",
            reason = "ticket_dependencies",
            source = "start_run",
            unmet_dependencies = blockers,
        }
    end
    local pipeline = repo.get_pipeline(pipeline_id)
    if not pipeline then
        error("pipeline not found: " .. tostring(pipeline_id))
    end
    local pipeline_archived = repo.pipeline_is_archived
        and repo.pipeline_is_archived(pipeline)
        or (pipeline.archived_at ~= nil and tostring(pipeline.archived_at) ~= "")
    if pipeline_archived then
        error("archived pipeline definitions are unavailable for new runs: " .. tostring(pipeline_id))
    end
    if #repo.pipeline_steps(pipeline_id) == 0 then
        error("pipeline has no steps: " .. tostring(pipeline_id))
    end

    local base_attrs = run_base_attrs(params)
    local run = repo.create_run{
        ticket_id = params.ticket_id,
        pipeline_id = pipeline_id,
        parent_run_id = params.parent_run_id,
        target_id = params.target_id or ticket.target_id,
        workspace_id = params.workspace_id,
        workspace_name = params.workspace_name or ticket_workspace_name(ticket, params.ticket_id),
        base_ticket_id = base_attrs.base_ticket_id,
        base_run_id = base_attrs.base_run_id,
        base_ref = base_attrs.base_ref,
        base_target_path = base_attrs.base_target_path,
    }
    local first_step = repo.next_step(run)
    local activation = M.activate_step(run, first_step)
    return { run = repo.get_run(run.id), activation = activation }
end

function M.request_step_advance(params, context)
    local run_id = params.run_id
    if util.is_blank(run_id) and context and context.session_uuid then
        local assignment = repo.find_active_assignment(context.session_uuid)
        run_id = assignment and assignment.run_id or nil
    end
    util.assert_present(run_id, "run_id")

    local run = repo.get_run(run_id)
    if not run then
        error("run not found: " .. tostring(run_id))
    end
    if util.is_blank(run.current_step_id) then
        return { ok = false, status = run.status, error = "run has no current step" }
    end
    local step = repo.get_step(run.current_step_id)
    local active_visit_id = run.current_run_step_id
    local next_step_override = forced_next_step(run, params)
    local missing = unmet_gates(run, step)
    local review_kickback = has_review_kickback(run, step)
    local override_unmet_gates = next_step_override and params.override_unmet_gates == true
    if #missing > 0 and not review_kickback and not override_unmet_gates then
        repo.append_event("step.advance_blocked", {
            run_id = run.id,
            ticket_id = run.ticket_id,
            payload = {
                step_id = step.id,
                unmet_gates = missing,
                summary = params.summary,
                requested_next_step_id = params.next_step_id,
            },
        })
        return {
            ok = false,
            status = "blocked",
            step = step,
            unmet_gates = missing,
            requested_next_step = next_step_override,
            next_tool = "project_pipelines_request_step_advance",
            next_tool_params = next_step_override and {
                run_id = run.id,
                next_step_id = next_step_override.id,
                override_unmet_gates = true,
                override_reason = "Route around blocked gates to recover pipeline state.",
            } or nil,
        }
    end

    local next_step, transition_error = next_step_override, nil
    if not next_step then
        next_step, transition_error = repo.next_step(run, step, active_visit_id)
    end
    if transition_error then
        repo.update_run(run.id, { status = "blocked" })
        if active_visit_id then
            repo.update_run_step_visit(active_visit_id, { status = "blocked" })
        end
        repo.append_event("step.advance_blocked", {
            run_id = run.id,
            ticket_id = run.ticket_id,
            payload = { step_id = step.id, run_step_id = active_visit_id, transition_error = transition_error },
        })
        return { ok = false, status = "blocked", step = step, transition_error = transition_error }
    end
    if next_step then
        local blocked = dependency_block(run, {
            step_id = step.id,
            run_step_id = active_visit_id,
            target_step_id = next_step.id,
            source = "request_step_advance",
        })
        if blocked then
            blocked.step = step
            blocked.target_step = next_step
            return blocked
        end
    end

    if override_unmet_gates and util.is_blank(params.override_reason) then
        error("override_reason is required when override_unmet_gates=true")
    end
    if override_unmet_gates then
        repo.append_event("step.advance_override", {
            run_id = run.id,
            ticket_id = run.ticket_id,
            payload = {
                step_id = step.id,
                run_step_id = active_visit_id,
                next_step_id = next_step_override.id,
                unmet_gates = missing,
                reason = params.override_reason,
                summary = params.summary,
            },
        })
    end

    local function complete_source_visit()
        if active_visit_id then
            repo.update_run_step_visit(active_visit_id, { status = "done", completed_at = util.now() })
        else
            repo.update_run_step(run.id, step.id, { status = "done", completed_at = util.now() })
        end
        repo.append_event("step.completed", {
            run_id = run.id,
            ticket_id = run.ticket_id,
            payload = {
                step_id = step.id,
                run_step_id = active_visit_id,
                summary = params.summary,
                evidence = params.evidence or {},
            },
        })
    end

    -- Target activation runs before source completion so a dependency block cannot
    -- leave the source visit done while the target is refused.
    if next_step then
        local activation = M.activate_step(run, next_step)
        if activation and activation.ok == false and activation.reason == "ticket_dependencies" then
            return {
                ok = false,
                status = "blocked",
                reason = "ticket_dependencies",
                step = step,
                next_step = next_step,
                target_step = next_step,
                source = activation.source or "activate_step",
                unmet_dependencies = activation.unmet_dependencies,
                activation = activation,
                run = repo.get_run(run.id),
            }
        end
        complete_source_visit()
        if activation and activation.ok == false then
            return {
                ok = false,
                status = activation.status or "blocked",
                completed_step = step,
                next_step = next_step,
                activation = activation,
                run = repo.get_run(run.id),
                error = activation.error,
            }
        end
        return {
            ok = true,
            completed_step = step,
            next_step = next_step,
            activation = activation,
            run = repo.get_run(run.id),
        }
    end

    -- Final advance (no target step): complete the run / merge path.
    complete_source_visit()
    local activation = M.activate_step(repo.get_run(run.id), nil)
    return {
        ok = true,
        completed_step = step,
        next_step = nil,
        activation = activation,
        run = repo.get_run(run.id),
    }
end

function M.retry_step_agent(params, context)
    params = params or {}
    local run_id = params.run_id
    if util.is_blank(run_id) and context and context.session_uuid then
        local assignment = repo.find_active_assignment(context.session_uuid)
        run_id = assignment and assignment.run_id or nil
    end
    util.assert_present(run_id, "run_id")

    local run = repo.get_run(run_id)
    if not run then
        error("run not found: " .. tostring(run_id))
    end
    if util.is_blank(run.current_step_id) then
        error("run has no current step")
    end
    local step = repo.get_step(run.current_step_id)
    if not step then
        error("current step not found: " .. tostring(run.current_step_id))
    end
    if step.kind ~= "agent" then
        error("current step is not an agent step: " .. tostring(step.kind))
    end

    local run_step_id = params.run_step_id or run.current_run_step_id
    local visit = nil
    if not util.is_blank(run_step_id) then
        visit = repo.get_run_step_visit(run_step_id)
        if not visit then
            error("run_step_id not found: " .. tostring(run_step_id))
        end
        if visit.run_id ~= run.id then
            error("run_step_id does not belong to run: " .. tostring(run_step_id))
        end
        if visit.step_id ~= step.id then
            error("run_step_id does not match current step: " .. tostring(run_step_id))
        end
    end
    if not visit then
        visit = repo.get_run_step(run.id, step.id)
    end
    if not visit then
        error("no run step visit found for current step")
    end

    -- Final dependency decision covers the full retry transition before any
    -- durable mutation (status flip, session clear, retry event, spawn).
    local blocked = dependency_block(run, {
        step_id = step.id,
        run_step_id = visit.id,
        target_step_id = step.id,
        source = "retry_step_agent",
    })
    if blocked then
        blocked.step = step
        blocked.run = run
        blocked.run_step = visit
        return blocked
    end
    blocked = dependency_block(run, {
        step_id = step.id,
        run_step_id = visit.id,
        target_step_id = step.id,
        source = "retry_step_agent",
    })
    if blocked then
        blocked.step = step
        blocked.run = run
        blocked.run_step = visit
        return blocked
    end

    local prior_status = run.status
    local prior_visit_status = visit.status
    local prior_session = visit.agent_session_uuid
    local prior_started_at = visit.started_at

    repo.update_run(run.id, { status = "active", current_step_id = step.id, current_run_step_id = visit.id })
    repo.update_run_step_visit(visit.id, {
        status = "active",
        agent_session_uuid = "",
        started_at = util.now(),
    })
    repo.append_event("step.agent_retry_requested", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = {
            step_id = step.id,
            run_step_id = visit.id,
            requested_by_session_uuid = context and context.session_uuid or params.requested_by_session_uuid,
            reason = params.reason,
        },
    })

    -- Deps already decided above; do not re-check after mutation (would leave
    -- half-applied retry state without restoring source fields).
    local created, err = spawn_step_agent(repo.get_run(run.id), step, {
        skip_dependency_check = true,
    })
    if err then
        repo.update_run(run.id, { status = "blocked" })
        repo.update_run_step_visit(visit.id, { status = "blocked" })
        repo.append_event("step.spawn_failed", {
            run_id = run.id,
            ticket_id = run.ticket_id,
            payload = {
                step_id = step.id,
                run_step_id = visit.id,
                retry = true,
                error = err,
                prior_status = prior_status,
                prior_visit_status = prior_visit_status,
                prior_session = prior_session,
                prior_started_at = prior_started_at,
            },
        })
        return { ok = false, status = "blocked", run = repo.get_run(run.id), step = step, run_step = repo.get_run_step_visit(visit.id), error = err }
    end

    refresh_surfaces(context)
    return { ok = true, status = "active", run = repo.get_run(run.id), step = step, run_step = repo.get_run_step_visit(visit.id), agent = created }
end

function M.handle_command_gate_completed(data)
    local context = data and data.context or {}
    if context.owner_plugin ~= OWNER then
        return
    end
    if context.operation == "merge_source_branch" then
        if data.success ~= true then
            complete_merge_refresh(context, "failed", data)
            return
        end
        local branch = command_gate_output(data):gsub("^%s+", ""):gsub("%s+$", "")
        if util.is_blank(branch) then
            complete_merge_refresh(context, "skipped_detached", data)
        elseif branch ~= context.base_branch then
            complete_merge_refresh(context, "skipped_other_branch", data)
        else
            run_merge_refresh_gate(context, "merge_source_status", "git status --porcelain", 30)
        end
        return
    end
    if context.operation == "merge_source_status" then
        if data.success ~= true then
            complete_merge_refresh(context, "failed", data)
        elseif not util.is_blank(command_gate_output(data)) then
            complete_merge_refresh(context, "skipped_local_changes", data)
        else
            run_merge_refresh_gate(
                context,
                "merge_source_pull",
                "git pull --no-rebase --no-autostash --no-edit origin " .. shell_quote(context.base_branch),
                120,
                { GIT_TERMINAL_PROMPT = "0", GIT_MERGE_AUTOEDIT = "no" }
            )
        end
        return
    end
    if context.operation == "merge_source_pull" or context.operation == "merge_orchestrator_pull" then
        if data.success == true then
            complete_merge_refresh(context, "updated", data)
        else
            local next_context = {}
            for key, value in pairs(context) do
                next_context[key] = value
            end
            next_context.pull_error = data.error
            next_context.pull_exit_status = data.exit_status
            run_merge_refresh_gate(
                next_context,
                "merge_source_conflicts",
                "git diff --name-only --diff-filter=U",
                30
            )
        end
        return
    end
    if context.operation == "merge_source_conflicts" then
        if data.success == true and not util.is_blank(command_gate_output(data)) then
            complete_merge_refresh(context, "conflicted", data)
        else
            complete_merge_refresh(context, "failed", {
                error = context.pull_error or data.error,
                exit_status = context.pull_exit_status,
            })
        end
        return
    end
    if context.operation == "gitignore_hygiene" then
        return
    end
    local run_id = context.run_id
    local step_id = context.step_id
    if util.is_blank(run_id) or util.is_blank(step_id) then
        return
    end
    local gate_id = context.gate_id
    if util.is_blank(gate_id) then
        local gates = repo.step_gates(step_id)
        gate_id = gates[1] and gates[1].id or step_id
    end
    local success = data.success == true
    local status = success and "passed" or "failed"
    local summary = success and "Command gate passed" or ("Command gate failed" .. (data.exit_status and (" with exit status " .. tostring(data.exit_status)) or ""))
    if data.error and data.error ~= "" then
        summary = summary .. ": " .. tostring(data.error)
    end
    repo.submit_gate{
        run_id = run_id,
        run_step_id = context.run_step_id,
        step_id = step_id,
        gate_id = gate_id,
        status = status,
        summary = data.summary or summary,
        evidence = data,
    }
    if status == "passed" then
        pcall(function()
            M.request_step_advance({ run_id = run_id, summary = "Command gate passed", evidence = data }, {})
        end)
    else
        repo.update_run(run_id, { status = "blocked" })
        if not util.is_blank(context.run_step_id) then
            repo.update_run_step_visit(context.run_step_id, { status = "blocked" })
        else
            repo.update_run_step(run_id, step_id, { status = "blocked" })
        end
    end
end

function M.handle_agent_created(info)
    local metadata = info and info.metadata or {}
    local request_id = info and (info.request_id or metadata.request_id)
    if util.is_blank(request_id) then
        return
    end

    local ticket_id, question_id = tostring(request_id):match("^" .. lua_pattern_escape(OWNER) .. ":(.-):question:(.-):agent$")
    if not util.is_blank(question_id) then
        local session_uuid = info.session_uuid or info.uuid or info.id
        if not util.is_blank(session_uuid) then
            local question = repo.get_question(question_id)
            if question and question.advisor_session_uuid == session_uuid then
                return
            end
            pcall(repo.update_question, question_id, { advisor_session_uuid = session_uuid })
            repo.append_event("question.agent_linked", {
                ticket_id = ticket_id,
                payload = { question_id = question_id, session_uuid = session_uuid, request_id = request_id },
            })
            refresh_surfaces()
        end
        return
    end

    local merge_ticket_id = tostring(request_id):match("^" .. lua_pattern_escape(OWNER) .. ":(.-):merge:agent$")
    if not util.is_blank(merge_ticket_id) then
        local session_uuid = info.session_uuid or info.uuid or info.id
        if not util.is_blank(session_uuid) then
            for _, event in ipairs(repo.ticket_events(merge_ticket_id, "ticket.merge_agent_linked", 25)) do
                local payload = util.decode(event.payload, {})
                if payload.session_uuid == session_uuid and payload.request_id == request_id then
                    return
                end
            end
            repo.append_event("ticket.merge_agent_linked", {
                ticket_id = merge_ticket_id,
                payload = { session_uuid = session_uuid, request_id = request_id },
            })
            refresh_surfaces()
        end
        return
    end

    local manual_ticket_id = tostring(request_id):match("^" .. lua_pattern_escape(OWNER) .. ":(.-):manual:.*$")
    if not util.is_blank(manual_ticket_id) then
        local session_uuid = info.session_uuid or info.uuid or info.id
        if not util.is_blank(session_uuid) then
            if link_manual_ticket_session(manual_ticket_id, session_uuid, request_id, {
                run_id = metadata.run_id,
                session_type = info.session_type,
                role = metadata.role,
                agent_name = info.agent_name,
                accessory_name = info.session_name,
            }) then
                refresh_surfaces()
            end
        end
        return
    end

    local run_id, step_id = tostring(request_id):match("^" .. lua_pattern_escape(OWNER) .. ":(.-):(.-):agent$")
    if util.is_blank(run_id) or util.is_blank(step_id) then
        return
    end

    local session_uuid = info.session_uuid or info.uuid or info.id
    if util.is_blank(session_uuid) then
        return
    end

    local run = repo.get_run(run_id)
    if not run then
        return
    end

    local run_step = nil
    if run.current_run_step_id then
        run_step = repo.get_run_step_visit(run.current_run_step_id)
    end
    if not run_step or run_step.step_id ~= step_id then
        run_step = repo.get_run_step(run_id, step_id)
    end
    if not run_step then
        return
    end
    if run_step.agent_session_uuid == session_uuid then
        return
    end

    if run_step.id then
        repo.update_run_step_visit(run_step.id, { agent_session_uuid = session_uuid })
    else
        repo.update_run_step(run.id, step_id, { agent_session_uuid = session_uuid })
    end
    repo.append_event("step.agent_linked", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = {
            step_id = step_id,
            run_step_id = run_step.id,
            session_uuid = session_uuid,
            request_id = request_id,
        },
    })
    local worktree_path = info.worktree_path or metadata.worktree_path
    if util.is_blank(worktree_path) then
        local session = Agent.get(session_uuid)
        if session and session.info then
            local ok, session_info = pcall(session.info, session)
            if ok and session_info then
                worktree_path = session_info.worktree_path
            end
        end
    end
    if not util.is_blank(worktree_path) then
        ensure_worktree_hygiene(worktree_path, { run_id = run.id, ticket_id = run.ticket_id })
    end
    refresh_surfaces()
end

function M.handle_agent_lifecycle(info)
    local metadata = info and info.metadata or {}
    local request_id = info and (info.request_id or metadata.request_id)
    if util.is_blank(request_id) then
        return
    end

    local run_id, step_id = tostring(request_id):match("^" .. lua_pattern_escape(OWNER) .. ":(.-):(.-):agent$")
    if util.is_blank(run_id) or util.is_blank(step_id) then
        return
    end

    local run = repo.get_run(run_id)
    if not run then
        return
    end

    repo.append_event("step.agent_lifecycle", {
        run_id = run.id,
        ticket_id = run.ticket_id,
        payload = {
            step_id = step_id,
            request_id = request_id,
            status = info.status,
            error = info.error,
        },
    })

    if info.status == "failed" then
        repo.update_run(run.id, { status = "blocked" })
        if run.current_run_step_id then
            repo.update_run_step_visit(run.current_run_step_id, { status = "blocked" })
        else
            repo.update_run_step(run.id, step_id, { status = "blocked" })
        end
    end
    refresh_surfaces()
end

function M.reconcile_agent_sessions()
    local ok, HubModule = pcall(require, "lib.hub")
    if not ok or not HubModule or not HubModule.get then
        return
    end
    local hub = HubModule.get()
    if not hub or type(hub.list_owned_sessions) ~= "function" then
        return
    end
    local sessions = hub:list_owned_sessions(OWNER) or {}
    for _, session in ipairs(sessions) do
        M.handle_agent_created(session)
    end
end

function M.refresh_surfaces(ctx)
    refresh_surfaces(ctx)
end

return M
