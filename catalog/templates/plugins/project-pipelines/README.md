# @template Project Pipelines
# @description Project and ticket pipeline management with gates, reviews, questions, and agent handoffs
# @category plugins
# @dest plugins/project-pipelines/README.md
# @scope device
# @version 1.1.0

# Project Pipelines

Device-level Botster development plugin for project/ticket workflows.

## Shape

The plugin is intentionally split across modules:

- `init.lua` clears hot-reload module cache, registers tools, surfaces, and events.
- `project_pipelines/db.lua` owns the plugin SQLite schema.
- `project_pipelines/repo.lua` owns persistence and audit events.
- `project_pipelines/entity_contract.lua` owns published entity type names and read-model field contracts.
- `project_pipelines/entities.lua` owns plugin entity read models and publishes dynamic state to clients.
- `project_pipelines/engine.lua` owns run advancement, gates, agent creation, and command gates.
- `project_pipelines/mcp.lua` exposes the agent-facing API.
- `project_pipelines/web/surface.lua` registers routes, sidebar navigation, and the Hub dashboard widget.
- `project_pipelines/web/screens/home.lua` renders the overview.
- `project_pipelines/web/screens/pipelines.lua` renders the pipeline index and edit form.
- `project_pipelines/web/screens/run.lua` renders run details.
- `project_pipelines/web/actions.lua` handles Catalyst UI action envelopes.
- `project_pipelines/web/ui.lua` contains shared UI helpers.
- `project_pipelines/util.lua` contains shared helpers.

## Concepts

- Ticket: durable work item.
- Pipeline: reusable ordered workflow definition.
- Step: one stage in a pipeline. Steps can be `agent`, `command`, or future plugin-owned kinds.
- Gate: requirement that must be satisfied before a step advances.
- Run: one ticket moving through one pipeline.
- Run step: one visit to a pipeline step. A run may visit the same step many
  times when review sends work back for changes.
- Review: structured reviewer output.
- Finding: durable review issue, visible to all agents through context.
- Artifact: durable evidence or external reference.
- Checklist: durable workflow/rubric checkpoint list for a project, ticket, or
  run. Checklists track evidence that the agent followed the workflow; the
  vault remains the source of truth for project conventions.
- Event: append-only audit record of workflow changes.

## Pipeline Definitions

The plugin does not seed default pipelines or sample tickets. Users and agents
create reusable pipeline definitions explicitly through the GUI and MCP tools.
Projects are the durable container for multi-phase or cross-target work; tickets
remain one concrete unit of work in one spawn target.

When a live hub already has `botster_stack_delivery`, plugin load refreshes the
Plan and Plan Review prompts if `pipelines.version_label` differs from the
catalog revision in `project_pipelines/stack_delivery.lua`. Operator-chosen
`agent_name` values are preserved. No operator SQL is required for that refresh.

## Worktree Hygiene

On step activation and agent link, the engine restores a tracked `.gitignore`
from `HEAD` only when the working copy is empty or missing. It never truncates
`.gitignore`. Command gates that run under a path containing `:` set a
colon-free `CARGO_TARGET_DIR` so macOS Cargo/DYLD path joining does not fail.

Pipeline MCP CRUD:

- `project_pipelines_create_pipeline`
- `project_pipelines_list_pipelines`
- `project_pipelines_get_pipeline`
- `project_pipelines_update_pipeline`
- `project_pipelines_delete_pipeline`
- `project_pipelines_create_step`
- `project_pipelines_update_step`
- `project_pipelines_delete_step`
- `project_pipelines_create_gate`
- `project_pipelines_update_gate`
- `project_pipelines_delete_gate`
- `project_pipelines_update_step_agent`

Ticket and project MCP management:

- `project_pipelines_create_ticket`
- `project_pipelines_list_tickets`
- `project_pipelines_get_ticket`
- `project_pipelines_update_ticket`
- `project_pipelines_delete_ticket`
- `project_pipelines_create_project`
- `project_pipelines_list_projects`
- `project_pipelines_get_project`
- `project_pipelines_update_project`
- `project_pipelines_delete_project`
- `project_pipelines_add_project_target`
- `project_pipelines_remove_project_target`
- `project_pipelines_start_run`
- `project_pipelines_cancel_run`

Checklist MCP management:

- `project_pipelines_checklist_instructions`
- `project_pipelines_create_checklist`
- `project_pipelines_create_vault_checklist`
- `project_pipelines_list_checklists`
- `project_pipelines_get_checklist`
- `project_pipelines_update_checklist`
- `project_pipelines_add_checklist_item`
- `project_pipelines_update_checklist_item`

`project_pipelines_get_ticket` is the agent-facing ticket status view. It
returns the ticket, project, runs, latest run, latest run steps, associated
sessions, and open findings so agents can see where the work is in the pipeline.
Tickets and projects can only be deleted while they have no history that would
be orphaned.

Deletes are guarded once run history exists, so past runs keep stable pipeline,
step, and gate references.

`project_pipelines_create_pipeline` requires an explicit stable `id` slug. Step
positions are ordered by `position`, then creation time, then ID, so duplicate
positions remain deterministic but should be avoided in authored definitions.
Attestation gates must declare at least one `required_fields` entry. Command
gates must declare a command. Every gate includes a prompt because unmet gates
surface that prompt back to agents; for `review_clear` gates, use the prompt to
explain that blocker/high review findings must be resolved or waived.

Pipelines have first-class lifecycle metadata. `version_label` is display
metadata, `archived_at` retires a definition without deleting it, and
`replacement_pipeline_id` / `supersedes_pipeline_id` link old and new
definitions. Do not encode lifecycle state in manual title prefixes.

Normal selection paths hide archived definitions: `project_pipelines_list_pipelines`
without `include_archived`, ticket start controls, the home pipeline list, the
pipeline index, and the engine's default pipeline fallback all use active
definitions only. `project_pipelines_list_pipelines{ include_archived = true }`
returns active and archived definitions. `project_pipelines_get_pipeline`
requires `include_archived = true` for an archived pipeline id. The Pipelines UI
keeps its default index active-only and exposes archived definitions from the
explicit archived definitions view.

Runs keep durable references to pipeline, step, and gate IDs. The current UI and
MCP context render live definition names/prompts for those IDs rather than a
full historical snapshot of the definition text. Direct historical lookup paths
inside the repo remain unfiltered so run detail pages and current context can
still render archived pipeline names and steps.

Review loops are first-class. A step can define:

- `next_step_id` for normal progression.
- `on_approved_step_id` for the next step after the latest review for the
  current step visit is `approved`.
- `on_changes_requested_step_id` for sending work back after
  `changes_required`.
- `on_blocked_step_id` for explicit blocked review outcomes.

Each activation appends a new run-step visit and stores it as
`runs.current_run_step_id`, so gates, reviews, artifacts, sessions, and the UI
can distinguish repeated visits to the same pipeline step.

Gate evidence is scoped to the current run-step visit: when a loop returns to a
step, attestation and command gates must be satisfied again for that visit.
Review findings are intentionally run-wide until resolved or waived; a
`review_clear` gate blocks on open blocker/high findings from any prior visit so
carryover review issues cannot be bypassed by looping.

Feature decomposition uses projects plus tickets. Agents can call
`project_pipelines_create_child_run` to create child tickets/runs for parallel
slices when a parent run exists. Child runs inherit target/workspace settings
from the parent unless overridden.

## Agent Contract

Agents should call `project_pipelines_current_context` first. The context includes the ticket, run, current step, current run-step visit, gate prompts, run steps, reviews, findings, artifacts, questions, question answers for the calling session, and recent events.

Agents submit evidence with `project_pipelines_submit_gate`, reviews with `project_pipelines_submit_review`, artifacts with `project_pipelines_add_artifact`, and move the run with `project_pipelines_request_step_advance`. If gates are not satisfied, advancement returns structured unmet gate prompts.

Ticket ordering dependencies are an unconditional activation preflight. The
engine reads the normalized rows returned by
`repo.ticket_dependencies(ticket_id)` every time it advances to a target step,
directly activates a step, returns PR review feedback to implementation, or
retries the current agent step. Every referenced ticket must have
`status = "closed"`; there is no step-level or forced-transition override for
open dependencies.

A blocked attempt returns `ok = false`, `status = "blocked"`,
`reason = "ticket_dependencies"`, and `unmet_dependencies`. It also appends one
`step.advance_blocked` event with the same reason and dependency entries plus
the source/current `step_id`, `run_step_id`, intended `target_step_id`, and
entry-point `source`. Each dependency entry retains its normalized dependency
and ticket ids, title/status when available, an operator prompt, and either:

- `reason = "open_ticket"` when the referenced ticket exists but is not closed.
- `reason = "unavailable_ticket"` when the left-joined ticket or its status is
  unavailable.

The preflight runs before completing the source visit, emitting
`step.completed` or an override event, creating the target visit, changing run
pointers, notifying, resolving command execution, requesting a worktree or
session, or spawning/linking an agent. `start_run` keeps its earlier contract:
it raises `ticket dependencies must close before starting a run: ...` before
creating the run. A final advance with no target step still completes the run
and follows its merge policy; dependency gating applies to target-step
activation, not run completion or merge.

`project_pipelines_cancel_run` changes an active or blocked run to the terminal
`cancelled` status. The operation does not advance a step or request a merge.
It preserves all run steps, plan artifacts, gate results, reviews, and run
history. It appends a durable `run.cancelled` event and publishes the changed
run and ticket entities through the repository mutation path. Repeating the
operation for a cancelled run returns success without adding another event.
A done run returns `reason = "run_already_done"` and remains unchanged. A new
run can start for the same ticket because cancelled runs are not open runs.

Closing or removing a dependency never activates work automatically. The
operator must explicitly advance or retry again. A dependency added after the
current step started also blocks agent retry, which can leave that visit
stranded until the dependency is resolved. On the later explicit retry, the
normal idempotence path remains in force: `repo.latest_step_session` plus
`Agent.get` reuses a live step session by posting the new task instead of
calling `create_agent` again.

Agents should use `project_pipelines_create_vault_checklist` when a ticket or
run needs convention discipline without copying convention text into the
pipeline. The default vault checklist asks for evidence that applicable vault
notes were loaded, the plan was checked against those conventions, repo-approved
verification ran, and new durable knowledge was captured or explicitly deemed
unneeded. Checklist item evidence should name vault notes, commands, conflicts,
waivers, or capture paths; the actual convention content stays in the vault.
Agents can call `project_pipelines_checklist_instructions` to retrieve the
recommended flow, default vault checklist items, statuses, and evidence shape.

Review agents leave findings through `project_pipelines_submit_review`. Blocker and high findings keep `review_clear` gates blocked until each finding is resolved or waived with `project_pipelines_resolve_finding`.

Agents ask for help through project-pipelines tools, not the generic Botster inbox. `project_pipelines_ask_human` creates a durable human question visible in the sidebar and ticket page. `project_pipelines_ask_agent` creates the same durable question and spawns an advisor agent. Answers are read with `project_pipelines_receive_question_answers` or from `project_pipelines_current_context`; question answers wake the asking session with a Project Pipelines notification, not a generic `receive_messages()` inbox doorbell.

Review agents must reject dead code, deprecated code paths, unwired implementation, missing focused tests, and broad "pre-existing failure" excuses. If a failure is not fixed, the reviewer should require exact evidence that it is unrelated to the ticket.

When a run returns to an existing step agent, Project Pipelines sends both a structured task message and a plugin notification pointing the agent back to `project_pipelines_current_context`.

## GUI

The `Pipelines` surface shows tickets, runs, pipeline definitions, selected agent per step, reviews, findings, artifacts, recent events, questions, and plugin-owned sessions. When any question is open, the workspace plugin nav entry for Pipelines shows a notification marker.

The overview is a human workbench, not a full internal state dump. It highlights
questions that need answers, currently running pipeline runs, and PR/merge work
that needs review or follow-up. Autonomous blocked loops stay inside the
pipeline run flow unless they create a human question or merge/PR item.

### Plugin Entity Case Study

Project Pipelines is the reference plugin for Botster's entity-backed UI model.
It ships models in four explicit Lua-owned layers:

- `project_pipelines/db.lua` declares durable `plugin.db` tables, migrations,
  constraints, and persisted workflow facts.
- `project_pipelines/repo.lua` validates and mutates that private persistence
  layer.
- `project_pipelines/entity_contract.lua` names every published entity family
  and documents the screen UI read-model sources, filter fields, and projected
  fields that entity-backed screens bind.
- `project_pipelines/entities.lua` consumes those names, builds normalized
  read-model records from plugin.db persistence rows, and publishes them for
  clients under the `project-pipelines.*` namespace.

New entity-backed plugin templates should keep the same split and follow
Botster's canonical `docs/plugin-entities.md#shipping-a-model` sequence. If a
scaffold publishes plugin entities, include or document an `entity_contract.lua`
module instead of burying entity names and bound field expectations inside
screens or publishers.

The entity layer is the UI/data contract. It can expose derived labels, status
tones, paths, counts, and flattened relationship fields that are convenient for
shared renderers without making the raw SQLite table shape public. Browser and
TUI clients consume those read models through the generic entity store; Project
Pipelines must not add plugin-specific browser stores, custom data channels, or
renderer subscription state as an alternate model path.

`project_pipelines/entities.lua` publishes recovery baselines with
`publish_snapshots()`, exposes targeted `upsert` / `remove` helpers so repo
mutators can update clients after persistence changes, and registers
`query(request, context)` providers for route and overview hydration.
Plugin-owned entity families are not part of the initial browser/TUI hub
baseline; surfaces request the specific plugin data they need. The browser does
this by inspecting the opened surface tree for `ui.bind` / `ui.bind_list`
sources. Unfiltered lists request whole-family snapshots, direct record binds
request id upserts/removes, and filtered `bind_list{ where = { ... } }`
sections request scoped snapshots that replace only matching rows.

The Hub dashboard widget uses the same path: it is a `lib.dashboard`
registration whose body binds to `/project-pipelines.run` instead of querying
plugin storage during dashboard render.

The contract module describes the published read-model shape, not the plugin.db
table schema. Persistence models may have different names, decoded JSON fields,
or private columns; plugin authors should treat `project-pipelines.*` contract
entries as the client-facing API. Its screen-field coverage guards the home
screen plus the non-home entity-backed screen sections. Screens that still
intentionally render from repo-owned overview data are listed in
`repo_rendered_screens` with the repo calls and entity sources they should
migrate toward.

Use singular, plugin-owned entity names for every published family and keep the
shape renderer-ready: flattened relationship ids, labels, status tones, paths,
button ids, counts, and booleans are projection fields, not browser
reconstruction work. `project_pipelines/entity_contract.lua` is the source of
truth for those names and fields; `project_pipelines/entities.lua` projects
private `plugin.db` rows and hub state into that shape. Do not publish raw table
rows, large nested graphs, route-specific view objects, or client-only state as
entity records.

Dynamic state is published as plugin-owned entities:

- `/project-pipelines.ticket`
- `/project-pipelines.project`
- `/project-pipelines.project_target`
- `/project-pipelines.ticket_dependency`
- `/project-pipelines.pipeline`
- `/project-pipelines.pipeline_step`
- `/project-pipelines.pipeline_gate`
- `/project-pipelines.run`
- `/project-pipelines.run_step`
- `/project-pipelines.gate_result`
- `/project-pipelines.review`
- `/project-pipelines.finding`
- `/project-pipelines.artifact`
- `/project-pipelines.question`
- `/project-pipelines.checklist`
- `/project-pipelines.checklist_item`
- `/project-pipelines.pr_link`
- `/project-pipelines.event`

The overview and detail pages render dynamic rows from plugin-owned entities
through shared UI primitives. Tickets, Projects, Pipeline Definitions, Recent
Runs, ticket handoffs, ticket questions, run steps, run reviews, findings,
artifacts, events, pipeline steps, and pipeline gates use `ui.bind_list` /
`ui.bind` against the entity families above. Detail subsections scope child rows
with `ui.bind_list{ where = { ... } }` instead of pre-rendering per-run or
per-step collections into the tree snapshot. Overview sections also use scoped
bindings for active runs, open questions, open projects, standalone tickets,
and merge-ready tickets; those filter fields (`status`, `standalone`,
`latest_run_status`, etc.) are explicit read-model fields and are supported by
the matching `query` providers. `/project-pipelines.ticket` publishes
standalone and project-scoped tickets; standalone lists and project timelines
filter the shared entity family in the view layer. Route scaffolding and form
controls that depend on the current path or available actions still render
structurally.
Pipeline steps and gates are first-class entities, not embedded arrays on the
pipeline entity, because the editor mutates individual step and gate fields and
detail screens need row-level handoff updates. Snapshot publishing is reserved
for registration/recovery paths; mutators publish targeted entity deltas instead
of forcing fresh `ui_tree_snapshot` frames for data-only changes.

UI screens must not perform render-time `repo.*` reads for dynamic rows that
belong to the plugin entity model. If a screen still needs repo-owned
scaffolding before migration, list the exception in
`project_pipelines/entity_contract.lua` under `repo_rendered_screens` with the
repo calls and entity sources it should move toward. When a section becomes
entity-backed, remove the old repo-rendered path, refresh-only snapshot
dependency, docs allowance, and test exception in the same slice.

Project Pipelines migrations are cold-turkey at the section boundary. Do not
leave v1/v2 screen paths, compatibility shims, custom browser stores,
plugin-specific subscriptions, or dual repo/entity reads after an entity-backed
replacement lands.

Stable submitter `id` values are required in repeated rows so the generic
`ui_action` lifecycle can scope pending, success, and error feedback to the
clicked control. `project_pipelines/web/actions.lua` returns `action.HANDLED`
for draft/local updates and `action.result{ message = ..., navigate = ... }` or
`action.result{ ok = false, error = ... }` for submitters that need visible
feedback.

### GUI Implementation Contract

Project Pipelines is a Botster Lua plugin surface rendered through the shared
`ui_contract`, not a Rails ERB, Turbo, Hotwire, or Elements surface. Author UI
in `project_pipelines/web/*.lua` with shared primitives and let the browser
render those nodes through `app/frontend/ui_contract/registry.tsx` and the
existing Catalyst components in `app/frontend/components/catalyst/*`.

For visible controls, use the public Lua primitives instead of plugin-specific
browser components:

- Forms use `ui.form`, `ui.text_input`, `ui.textarea`, `ui.select`, and
  `ui.checkbox`; the web renderer supplies Catalyst inputs and native
  validation.
- Actions use `ui.button` or `ui.icon_button` with semantic `icon` names, so
  `IconGlyph` remains the single icon path. Do not inline SVG or add bespoke
  icon renderers.
- Submitters in repeated rows must carry stable node `id` values so the generic
  `ui_action` lifecycle can scope pending and result feedback to the clicked
  button.
- Dynamic collections publish plugin-owned entity records and bind with
  `ui.bind` or `ui.bind_list` from sources such as `/project-pipelines.ticket`.
  Publish filterable record supersets and explicit read-model fields rather
  than per-view browser stores.
- Detail rows use `ui.bind_list{ where = { ... } }` for filtered children when
  they need dynamic entity-backed rows. Filtered overview rows use the same
  scoped entity hydration path. Keep `ui_tree_snapshot` for route scaffolding,
  current-path controls, and other presentation structure.
- Ephemeral browser-only state uses `ui.local_state(key, default)` with
  `botster.presentation.set`, `botster.presentation.clear`, or
  `botster.presentation.toggle`. Use this for modal open flags and local
  disclosure state; do not encode those states in routes, plugin entities, or
  plugin DB tables.
- Modal field values that are not submitted yet are browser-local presentation
  state too. Keep draft text, selected options, temporary filters, and similar
  local inputs in `ui.local_state` or native form state until submit; persist
  only the accepted workflow fact through `project_pipelines/repo.lua` and the
  resulting entity delta.

Before restyling visible Catalyst primitives, inspect `tmp/tailwind_plus_preview`
if it exists in the worktree. If the directory is absent, use the vendored
Catalyst primitives and existing `ui_contract` registry styles as the design
source. State binding, entity projections, and action lifecycle work should not
invent Tailwind or Elements patterns just because no preview directory exists.

The overview's dynamic Projects, Tickets, Recent Runs, and Pipeline Definitions
sections render from plugin-owned entities through shared UI primitives:
Tickets and Pipeline Definitions use `ui.list` / `ui.list_item`, Projects use
`ui.tree` / `ui.tree_item`, and Recent Runs uses `ui.table` with rows bound from
`/project-pipelines.run`. Keep these collections entity-backed; mutators that
only change collection data should publish entity snapshots or deltas instead
of forcing a fresh `ui_tree_snapshot`. Detail screens render presentation
snapshots for route scaffolding and controls, but dynamic model rows are
entity-backed through `ui.bind` / `ui.bind_list`.

Ticket spawn dialogs are browser-local presentation state. The ticket detail
screen opens them with `botster.presentation.set` and binds dialog `open` to
`ui.local_state`; successful spawn actions return `presentation.clear` for the
dialog keys while leaving navigation on the current ticket route.

### Entity-Only UI Migration Plan

The current Project Pipelines UI is mostly entity-backed. Keep the next
slices small: migrate one screen section at a time from render-time `repo.*`
queries into existing or new plugin entity families, then remove the
corresponding repo-rendered code, refresh-only `ui_tree_snapshot` dependency,
docs allowance, and test exception for that section. Do not add
plugin-specific browser stores or subscription state; browser state remains
generic entity store data plus `ui.local_state` or native form state for
presentation-only modal/disclosure flags and unsubmitted modal field values.
When a migrated section needs data that is not already available as a clean
field, add it to `project_pipelines/entities.lua` as an explicit read-model
projection and document it in `entity_contract.lua` instead of reconstructing it
in the browser.

Entity families already available and expected to remain the primary UI data
source:

- `/project-pipelines.ticket`
- `/project-pipelines.project`
- `/project-pipelines.project_target`
- `/project-pipelines.ticket_dependency`
- `/project-pipelines.pipeline`
- `/project-pipelines.pipeline_step`
- `/project-pipelines.pipeline_gate`
- `/project-pipelines.run`
- `/project-pipelines.run_step`
- `/project-pipelines.gate_result`
- `/project-pipelines.review`
- `/project-pipelines.finding`
- `/project-pipelines.artifact`
- `/project-pipelines.question`
- `/project-pipelines.checklist`
- `/project-pipelines.checklist_item`
- `/project-pipelines.pr_link`
- `/project-pipelines.event`

Add only the smallest new families needed to replace embedded model snapshots:

- `/project-pipelines.ticket_session` for ticket-associated agent, accessory,
  merge, and question-advisor sessions now reconstructed from run steps and
  event payloads.
- `/project-pipelines.session_presence` only if generic core session entities
  cannot supply the needed live/notification labels through shared bindings.
  Prefer binding to core session entities before adding this family.

Screen migration order:

1. Run detail: the header display, header actions, step cards, reviews,
   findings, artifacts, and recent events now use `/project-pipelines.run`
   record bindings plus filtered `ui.bind_list` sources. Use this completed
   screen as the reference pattern for migrating the larger ticket timeline.
2. Overview: questions, active runs, merge-ready tickets, open projects,
   standalone tickets, and pipeline definitions now render through entity binds
   and targeted/scoped hydration. Keep future overview changes in
   `entity_contract.lua` and `entities.lua`; do not reintroduce render-time
   repo queries for dynamic rows.
3. Ticket timeline and terminal sections: replace `handoff_rows`,
   `session_rows`, `run_rows`, `merge_controls`, `merge_result_rows`, and
   dependency option lists in `project_pipelines/web/screens/ticket.lua` with
   entity-bound lists. Publish ticket session rows explicitly rather than
   reconstructing them from `ticket.manual_session_*`, `ticket.merge_*`, and
   `question.agent_linked` events in the screen renderer.
4. Pipeline index/editor: migrate `project_pipelines/web/screens/pipelines.lua`
   from `repo.list_pipelines`, `repo.pipeline_steps`, and `repo.step_gates` to
   `/project-pipelines.pipeline`, `/project-pipelines.pipeline_step`, and
   `/project-pipelines.pipeline_gate`. Keep draft field feedback in the generic
   `ui_action` feedback path; do not create a browser-side pipeline edit store.
5. New ticket/project screens: replace recent ticket/project lists and project
   select options in `project_pipelines/web/screens/new.lua` with bound
   entities. Spawn target options may stay generic core UI data until spawn
   targets have a core entity binding.
6. Sidebar/notification cleanup: keep sidebar rows entity-bound and replace
   `has_open_questions()` with either a generic entity-derived notification
   query or an entity-backed nav badge when the surface registry can bind nav
   notification state. Do not subscribe the browser to a Project Pipelines
   custom channel for this marker.

After each section migration, run static `rg` against the touched screen for
`repo.` and verify that only route scaffolding, action feedback, or generic
presentation helpers remain. Mutators should publish entity deltas for the
affected families; `engine.refresh_surfaces` / `TreeSnapshot.invalidate` should
only remain for structural route changes, not collection data changes.

Routes:

- `/pipelines` is the overview.
- `/pipelines/tickets/:ticket_id` is the ticket detail page for moving a ticket into a pipeline, viewing runs, and opening agent terminals that have touched the ticket.
- `/pipelines/tickets/:ticket_id/sessions/:session_uuid` opens a ticket-associated agent terminal.
- `/pipelines/pipelines` is the pipeline definition index.
- `/pipelines/pipelines/:pipeline_id/edit` edits pipeline name/description, selected agent per agent step, step prompts, command steps, and gate prompts/commands with Catalyst UI primitives.

Pipeline edits mutate the shared pipeline definition. They affect future runs, not already spawned agent sessions.

The plugin sidebar is ticket-first. It intentionally does not show active agents; terminals are reached through the ticket page so the user stays oriented around the work item.

Ticket detail pages can spawn an additional agent or accessory in the ticket's
worktree context. Project Pipelines first reuses a live associated session's
worktree when one exists, then falls back to the ticket pipeline branch so
manual support sessions stay attached to the ticket instead of forcing the user
through the generic new-session flow.

Ticket closure closes every Botster session associated with every run for that ticket without deleting worktrees. Step completion does not close agents; they remain available for inspection and for future prompts if a run returns to their step.

Pipelines declare a merge policy: `direct` merges accepted work directly to
main, while `pr` opens or updates a PR through Botster MCP PR tools. Completed
runs automatically spawn the merge agent; there is no separate merge approval
click. Merge completion should leave a durable merge artifact on the ticket.
Merge agents are final acceptance gates, not just Git operators: they must
verify the ticket intent, review findings, runtime wiring, docs, tests, and
removed/deprecated paths before merging. They should include `merge_commit`,
`pr_url`, or `merge_summary` when calling `project_pipelines_close_ticket` with
`merge_confirmed=true`.

Tickets can be linked to provider pull requests through
`project_pipelines_link_pr`. The link is provider-neutral (`provider`, `repo`,
`pr_number`) so external plugins can emit lifecycle events without coupling to
Project Pipelines internals. The GitHub plugin emits `pr_merged`; Project
Pipelines listens for that event, finds a matching PR link, marks it merged, and
closes the associated ticket with merge evidence. Agents should link the PR when
it is opened or updated so the later merge event can complete the ticket without
another human action.

Every pipeline handoff should bias toward disciplined, verifiable work:
assumptions are explicit, changes are surgical, speculative abstractions are
rejected, and success criteria are proven before advancement. For runtime,
async, permission, UI-routing, data-plane, control-plane, and architecture
migration tickets, gate evidence must prove the actual production path uses the
new behavior; code shape alone is not acceptance. Stub wiring that delegates
back to the old production path is incomplete unless the ticket is explicitly
scaffold-only or a human waiver is recorded.

Tickets can optionally depend on other tickets. A ticket with open dependencies cannot start a pipeline run until each dependency ticket is closed. Project tickets remain visible from the project page; the sidebar ticket list shows standalone tickets only, plus notification badges when their associated sessions need attention.

Stacked pipeline work is modeled explicitly on runs. `base_ref` records the
branch, commit, or PR head that the run should start from; `base_ticket_id` and
`base_run_id` keep the semantic link to upstream pipeline work; and
`base_target_path` can point at an existing source worktree when the base ref is
only available there. Ticket dependencies control run ordering only; they do
not select a branch base. Callers that need stacked work must pass the base
metadata explicitly. Merge agents for PR pipelines must open stacked PRs against
`base_ref` when it is present.

## Persistence And Evolution

State lives in the plugin database at the Botster device data root under `plugin-data/project-pipelines/db.sqlite`. The schema is additive-friendly, but non-additive changes need an explicit `version` bump and migration function in `project_pipelines_db.lua`.

During the first load after the seedless pipeline change, the plugin performs a
one-shot cleanup check for the old `massive_feature` seed pipeline and records a
`pipeline.legacy_prune_checked` event so hot reloads do not repeat the cleanup.

## Starting A Run

Create a ticket:

```json
{
  "title": "Implement saved filters",
  "description": "Users can save and reuse filters.",
  "target_id": "tgt_..."
}
```

Start a run:

```json
{
  "ticket_id": "ticket_...",
  "pipeline_id": "ticket_delivery",
  "target_id": "...",
  "workspace_name": "Pipelines"
}
```

A ticket's `target_id` is the sole stored target handle. The target's filesystem
root is never stored on tickets or runs — it is derived on demand from the
spawn target registry whenever a real path is needed (command step `cwd`,
scanning a repo's `.botster` config for agents and accessories). This keeps the
path from drifting and keeps it out of agent-facing context, where a raw
`target_path` was mistaken for the agent's working directory. Runs need a
resolvable `target_id`; command steps additionally require that the target
still resolves to a filesystem path.
