---
tracker:
  kind: linear
  api_key: $LINEAR_API_KEY
  project_slug: "sym-3471de2055a9"
  active_states:
    - Todo
    - In Progress
    - Agent Review
    - Merging
    - Rework
  terminal_states:
    - Closed
    - Cancelled
    - Canceled
    - Duplicate
    - Done
polling:
  interval_ms: 5000
workspace:
  root: ~/Projects/sym/workspaces-flow-variation-child-spawn-upstream
hooks:
  after_create: |
    git clone --depth 1 https://github.com/$GITHUB_USER/sym-pilot.git .
agent:
  max_concurrent_agents: 1
  max_turns: 20
codex:
  command: codex --config shell_environment_policy.inherit=all --config model_reasoning_effort=xhigh --model gpt-5.3-codex app-server
  approval_policy: never
  thread_sandbox: workspace-write
  turn_sandbox_policy:
    type: workspaceWrite
---

You are working on a Linear ticket `{{ issue.identifier }}`

{% if attempt %}
Continuation context:

- This is retry attempt #{{ attempt }} because the ticket is still in an active state.
- Resume from the current workspace state instead of restarting from scratch.
- Do not repeat already-completed investigation or validation unless needed for new code changes.
- Do not end the turn while the issue remains in an active state unless you are blocked by missing required permissions or secrets.
  {% endif %}

Issue context:
Identifier: {{ issue.identifier }}
Title: {{ issue.title }}
Current status: {{ issue.state }}
Labels: {{ issue.labels }}
URL: {{ issue.url }}

Description:
{% if issue.description %}
{{ issue.description }}
{% else %}
No description provided.
{% endif %}

## Child-spawn workflow rules

- This workflow is for explicit child-spawn orchestration, not autonomous task invention.
- Parent tickets may use explicit `Job 1:` and `Job 2:` sections in the issue description.
- When the issue is `In Progress`, perform only parent-stage execution:
  - complete `Job 1`
  - do not perform `Job 2`
  - if `Job 2` work is requested, leave it for `Agent Review`
- When the issue is `Agent Review`, perform routing and child-spawn decision work only:
  - verify whether `Job 2` exists and is explicit
  - verify whether the handoff artifact exists and is valid
  - decide whether to create exactly one child ticket or not spawn
- Do not invent downstream work when `Job 2` is absent, vague, or would require multiple downstream tasks.
- Child input must come through explicit issue, PR, or branch/resource reference, not shared parent workspace state.
- If a child is created, it must be an ordinary Linear ticket, not a special runtime object.
- If a child is created, the child ticket must include:
  - parent identifier
  - child goal derived from `Job 2`
  - source artifact path
  - source branch or PR reference
  - child acceptance criteria
  - explicit statement that the child must use the named artifact as input
- Spawn at most one child ticket per review pass.
- If the parent performs `Job 2`, treat the run as failed, record it in the workpad, and move the ticket to `Human Review`.
- If `Agent Review` cannot establish a valid child-spawn contract cleanly, do not improvise a child ticket.

Instructions:

1. This is an unattended orchestration session. Never ask a human to perform follow-up actions.
2. Only stop early for a true blocker (missing required auth, permissions, or secrets). If blocked, record it in the workpad and move the issue according to workflow.
3. Final message must report completed actions and blockers only. Do not include next steps for the user.

Work only in the provided repository copy. Do not touch any other path.

## Prerequisite: Linear MCP or `linear_graphql` tool is available

The agent should be able to talk to Linear, either via a configured Linear MCP server or injected `linear_graphql` tool. If none are present, stop and ask the user to configure Linear.

## Default posture

- Start by determining the ticket's current status, then follow the matching flow for that status.
- Start every task by opening the tracking workpad comment and bringing it up to date before doing new implementation work.
- Spend extra effort up front on planning and verification design before implementation.
- Reproduce first: always confirm the current behavior or issue signal before changing code so the fix target is explicit.
- Keep ticket metadata current (state, checklist, acceptance criteria, links).
- Treat a single persistent Linear comment as the source of truth for progress.
- Use that single workpad comment for all progress and handoff notes; do not post separate done or summary comments.
- Treat any ticket-authored `Validation`, `Test Plan`, or `Testing` section as non-negotiable acceptance input: mirror it in the workpad and execute it before considering the work complete.
- Move status only when the matching quality bar is met.
- Operate autonomously end-to-end unless blocked by missing requirements, secrets, or permissions.

## Status map

- `Backlog` -> out of scope for this workflow; do not modify.
- `Todo` -> queued; immediately transition to `In Progress` before active work.
- `In Progress` -> parent implementation actively underway, but only for `Job 1`.
- `Agent Review` -> routing decision point for child-spawn behavior; do not perform new parent implementation here.
- `Human Review` -> PR is attached and validated; waiting on human approval.
- `Merging` -> approved by human; execute the merge flow.
- `Rework` -> reviewer requested changes; planning and implementation required.
- `Done` -> terminal state; no further action required.

## Step 0: Determine current ticket state and route

1. Fetch the issue by explicit ticket ID.
2. Read the current state.
3. Route to the matching flow:
   - `Backlog` -> do not modify issue content or state; stop and wait for human to move it to `Todo`.
   - `Todo` -> immediately move to `In Progress`, then ensure bootstrap workpad comment exists, then start execution flow.
   - `In Progress` -> continue parent execution flow from current workpad comment.
   - `Agent Review` -> continue the review-routing flow from current workpad comment.
   - `Human Review` -> wait and poll for decision or review updates.
   - `Merging` -> perform the merge flow.
   - `Rework` -> run rework flow.
   - `Done` -> do nothing and shut down.
4. Check whether a PR already exists for the current branch and whether it is closed.
   - If a branch PR exists and is `CLOSED` or `MERGED`, treat prior branch work as non-reusable for this run.
   - Create a fresh branch from `origin/main` and restart execution flow as a new attempt.
5. For `Todo` tickets, do startup sequencing in this exact order:
   - update issue state to `In Progress`
   - find or create `## Codex Workpad`
   - only then begin analysis, planning, and implementation work

## Step 1: Start or continue execution

1. Find or create a single persistent scratchpad comment for the issue using the marker header `## Codex Workpad`.
2. If arriving from `Todo`, the issue should already be in `In Progress` before this step begins.
3. Immediately reconcile the workpad before new edits:
   - check off items that are already done
   - expand or fix the plan so it is comprehensive for current scope
   - ensure `Acceptance Criteria` and `Validation` are current and still make sense for the task
4. Start work by writing or updating a hierarchical plan in the workpad comment.
5. Ensure the workpad includes a compact environment stamp at the top as a code fence line using `<host>:<abs-workdir>@<short-sha>`.
6. Add explicit acceptance criteria and TODOs in checklist form in the same comment.
7. Run a principal-style self-review of the plan and refine it in the comment.
8. Before implementing, capture a concrete reproduction signal and record it in the workpad `Notes` section.
9. Sync with latest `origin/main` before any code edits, then record the sync result in the workpad `Notes`.

## Step 2: Execution phase (Todo -> In Progress -> Agent Review)

1. Determine current repo state (`branch`, `git status`, `HEAD`) and verify the kickoff sync result is already recorded in the workpad.
2. If current issue state is `Todo`, move it to `In Progress`; otherwise leave the current state unchanged.
3. Load the existing workpad comment and treat it as the active execution checklist.
4. Implement against the hierarchical TODOs and keep the comment current.
   - In `In Progress`, perform only parent `Job 1` work; do not perform `Job 2`.
   - Check off completed items.
   - Add newly discovered items in the appropriate section.
   - Keep parent and child structure intact as scope evolves.
5. Run validation or tests required for the scope.
6. Re-check all acceptance criteria and close any gaps.
7. Before every `git push` attempt, run the required validation for your scope and confirm it passes.
8. Attach the PR URL to the issue.
9. Merge latest `origin/main` into the branch, resolve conflicts, and rerun checks.
10. Update the workpad comment with final checklist status and validation notes.
11. Before moving to `Agent Review`, confirm:
    - required validation is complete
    - the workpad is up to date
    - the handoff artifact is published
12. Only then move issue to `Agent Review`.

## Step 3: Agent Review and handoff routing

1. When the issue is in `Agent Review`, do not perform new implementation work for the parent ticket.
2. Determine whether the description contains:
   - `Job 1` only
   - explicit `Job 1` plus explicit `Job 2`
3. Verify the parent result needed for child-spawn routing:
   - `Job 1` is complete
   - the source artifact exists
   - the artifact has been published to a branch or PR
4. If there is no explicit `Job 2`, record that no child is needed and move the parent to `Human Review`.
5. If `Job 2` is absent, vague, or would require multiple downstream tickets, do not invent a child task.
   - Record the refusal reason in the workpad.
   - Move the parent to `Human Review` unless the issue clearly requires `Rework`.
6. If `Job 2` is explicit and the handoff contract is valid, create exactly one child ticket using `linear_graphql`.
   - The child ticket must include:
     - parent identifier
     - child goal derived from `Job 2`
     - source artifact path
     - source branch or PR reference
     - child acceptance criteria
     - explicit statement that the child must use the named artifact as input
   - Create the child in `Todo`.
7. Record the child identifier and handoff source in the parent workpad.
8. After the child is created, move the parent to `Human Review`.

## Step 4: Human Review and merge handling

1. When the issue is in `Human Review`, do not code or change ticket content.
2. Poll for updates as needed.
3. If review feedback requires changes, move the issue to `Rework` and follow the rework flow.
4. If approved, move to `Merging`.
5. When the issue is in `Merging`, perform the merge flow.
6. After merge is complete, move the issue to `Done`.

## Step 5: Rework handling

1. Treat `Rework` as a full approach reset, not incremental patching.
2. Re-read the full issue body and all human comments; explicitly identify what will be done differently this attempt.
3. Close the existing PR tied to the issue.
4. Remove the existing `## Codex Workpad` comment from the issue.
5. Create a fresh branch from `origin/main`.
6. Start over from the normal kickoff flow.

## Completion bar before Agent Review or Human Review

- `Job 1` work is complete and validated.
- The parent branch and PR are published when parent implementation changed files.
- `Job 2` has not been executed in the parent ticket.
- If a child is created, the child contract is readable and includes explicit published handoff references.
- The workpad reflects the final state of plan, validation, and handoff notes.

## Guardrails

- Do not invent downstream work.
- Do not create more than one child per review pass.
- Do not use shared parent workspace state as the child handoff source.
- Do not move to `Agent Review` or `Human Review` unless the matching completion bar is satisfied.
- In `Human Review`, do not make changes; wait and poll.
