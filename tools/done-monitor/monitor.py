#!/usr/bin/env python3
"""
Done-Monitor: polls Linear for Done issues, evaluates via Claude API
whether follow-up work is needed, and creates follow-up tickets.

Run via cron every 5 minutes or manually:
    LINEAR_API_KEY=... ANTHROPIC_API_KEY=... python3 monitor.py

State is tracked in processed.json (auto-created).
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LINEAR_API_KEY = os.environ.get("LINEAR_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

PROJECT_SLUG = "3471de2055a9"
DONE_STATE = "Done"
LINEAR_ENDPOINT = "https://api.linear.app/graphql"
ANTHROPIC_ENDPOINT = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-20250514"
PAGE_SIZE = 50

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROCESSED_FILE = os.path.join(SCRIPT_DIR, "processed.json")
LOCK_FILE = os.path.join(SCRIPT_DIR, "monitor.lock")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("done-monitor")

# ---------------------------------------------------------------------------
# Lock file (prevent concurrent cron executions)
# ---------------------------------------------------------------------------

_lock_fd = None


def acquire_lock():
    global _lock_fd
    try:
        _lock_fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(_lock_fd, str(os.getpid()).encode())
        atexit.register(release_lock)
    except FileExistsError:
        # Check if the holding process is still alive
        try:
            with open(LOCK_FILE) as f:
                pid = int(f.read().strip())
            os.kill(pid, 0)  # signal 0 = check existence
            log.info("Another instance (pid %d) is running. Exiting.", pid)
            sys.exit(0)
        except (ValueError, ProcessLookupError, PermissionError):
            log.warning("Stale lock file found. Removing.")
            os.remove(LOCK_FILE)
            _lock_fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(_lock_fd, str(os.getpid()).encode())
            atexit.register(release_lock)


def release_lock():
    global _lock_fd
    if _lock_fd is not None:
        try:
            os.close(_lock_fd)
        except OSError:
            pass
        _lock_fd = None
    try:
        os.remove(LOCK_FILE)
    except FileNotFoundError:
        pass


# ---------------------------------------------------------------------------
# Processed-state persistence
# ---------------------------------------------------------------------------


def load_processed() -> dict:
    if not os.path.exists(PROCESSED_FILE):
        return {}
    try:
        with open(PROCESSED_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Could not read %s: %s — starting fresh", PROCESSED_FILE, exc)
        return {}


def save_processed(data: dict):
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=SCRIPT_DIR, prefix=".processed_", suffix=".tmp"
    )
    try:
        with os.fdopen(tmp_fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, PROCESSED_FILE)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Linear API helpers
# ---------------------------------------------------------------------------

FETCH_DONE_QUERY = """
query DoneMonitorPoll($projectSlug: String!, $stateName: [String!]!, $first: Int!, $after: String) {
  issues(
    filter: {
      project: { slugId: { eq: $projectSlug } }
      state: { name: { in: $stateName } }
    }
    first: $first
    after: $after
  ) {
    nodes {
      id
      identifier
      title
      description
      url
      priority
      assignee { id }
      labels { nodes { id name } }
      project { id }
      team { id }
    }
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"""

CREATE_ISSUE_MUTATION = """
mutation DoneMonitorCreateFollowup($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue {
      id
      identifier
      url
    }
  }
}
"""


def linear_request(query: str, variables: dict) -> dict:
    resp = requests.post(
        LINEAR_ENDPOINT,
        headers={
            "Authorization": LINEAR_API_KEY,
            "Content-Type": "application/json",
        },
        json={"query": query, "variables": variables},
        timeout=30,
    )
    resp.raise_for_status()
    body = resp.json()
    if "errors" in body:
        raise RuntimeError(f"Linear GraphQL errors: {body['errors']}")
    return body


# ---------------------------------------------------------------------------
# Fetch Done issues (paginated)
# ---------------------------------------------------------------------------


def fetch_done_issues() -> list[dict]:
    all_issues: list[dict] = []
    after_cursor = None

    while True:
        body = linear_request(
            FETCH_DONE_QUERY,
            {
                "projectSlug": PROJECT_SLUG,
                "stateName": [DONE_STATE],
                "first": PAGE_SIZE,
                "after": after_cursor,
            },
        )

        issues_data = body["data"]["issues"]
        nodes = issues_data["nodes"]
        all_issues.extend(nodes)

        page_info = issues_data["pageInfo"]
        if page_info["hasNextPage"] and page_info.get("endCursor"):
            after_cursor = page_info["endCursor"]
        else:
            break

    log.info("Fetched %d Done issues from Linear", len(all_issues))
    return all_issues


# ---------------------------------------------------------------------------
# Claude evaluation
# ---------------------------------------------------------------------------

EVALUATION_PROMPT = """\
You are evaluating a completed Linear ticket to decide whether follow-up work is needed.

Ticket identifier: {identifier}
Title: {title}
Description:
{description}

Analyze whether this completed ticket warrants a follow-up ticket. Consider:
- Deferred TODOs or items explicitly postponed
- Natural next steps that logically follow from this work
- Testing gaps (missing tests, edge cases not covered)
- Documentation gaps
- Hardening or production-readiness work
- Monitoring, alerting, or observability improvements

Respond with ONLY a JSON object (no markdown fences, no extra text):
{{
  "needs_followup": true/false,
  "reason": "Brief explanation of your decision",
  "followup_title": "Title for the follow-up ticket (only if needs_followup is true)",
  "followup_description": "Description for the follow-up ticket (only if needs_followup is true)"
}}
"""


def evaluate_issue(issue: dict) -> dict | None:
    """Ask Claude whether a Done issue needs follow-up. Returns parsed JSON or None."""
    identifier = issue.get("identifier", "?")
    title = issue.get("title", "")
    description = issue.get("description") or "(no description)"

    prompt_text = EVALUATION_PROMPT.format(
        identifier=identifier, title=title, description=description
    )

    try:
        resp = requests.post(
            ANTHROPIC_ENDPOINT,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt_text}],
            },
            timeout=60,
        )
        resp.raise_for_status()
        body = resp.json()
    except Exception as exc:
        log.error("Claude API request failed for %s: %s", identifier, exc)
        return None

    try:
        text = body["content"][0]["text"]
        # Strip markdown fences if present
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            if text.endswith("```"):
                text = text[: -len("```")]
            text = text.strip()
        result = json.loads(text)
    except (KeyError, IndexError, json.JSONDecodeError) as exc:
        log.error(
            "Could not parse Claude response for %s: %s — raw: %.500s",
            identifier,
            exc,
            body,
        )
        return None

    log.info(
        "Evaluation for %s: needs_followup=%s reason=%s",
        identifier,
        result.get("needs_followup"),
        result.get("reason", ""),
    )
    return result


# ---------------------------------------------------------------------------
# Create follow-up ticket
# ---------------------------------------------------------------------------


def create_followup(parent: dict, evaluation: dict) -> dict | None:
    """Create a follow-up issue on Linear mirroring the parent's structure."""
    identifier = parent.get("identifier", "?")
    parent_url = parent.get("url", "")

    followup_title = f"Follow-up from {identifier}: {evaluation.get('followup_title', 'Follow-up')}"
    followup_desc = evaluation.get("followup_description", "")
    if parent_url:
        followup_desc += f"\n\n---\nParent ticket: [{identifier}]({parent_url})"

    input_fields: dict = {
        "teamId": parent["team"]["id"],
        "title": followup_title,
        "description": followup_desc,
    }

    # Mirror optional fields from parent
    project = parent.get("project")
    if project and project.get("id"):
        input_fields["projectId"] = project["id"]

    assignee = parent.get("assignee")
    if assignee and assignee.get("id"):
        input_fields["assigneeId"] = assignee["id"]

    labels = parent.get("labels", {}).get("nodes", [])
    if labels:
        input_fields["labelIds"] = [l["id"] for l in labels if l.get("id")]

    priority = parent.get("priority")
    if priority is not None:
        input_fields["priority"] = priority

    try:
        body = linear_request(CREATE_ISSUE_MUTATION, {"input": input_fields})
    except Exception as exc:
        log.error("Failed to create follow-up for %s: %s", identifier, exc)
        return None

    create_data = body.get("data", {}).get("issueCreate", {})
    if not create_data.get("success"):
        log.error("issueCreate returned success=false for %s: %s", identifier, body)
        return None

    created = create_data.get("issue", {})
    log.info(
        "Created follow-up %s for %s: %s",
        created.get("identifier", "?"),
        identifier,
        created.get("url", ""),
    )
    return created


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    if not LINEAR_API_KEY:
        log.error("LINEAR_API_KEY not set")
        sys.exit(1)
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        sys.exit(1)

    acquire_lock()

    processed = load_processed()
    log.info("Loaded %d previously processed issues", len(processed))

    try:
        issues = fetch_done_issues()
    except Exception as exc:
        log.error("Failed to fetch Done issues: %s", exc)
        sys.exit(1)

    unprocessed = [i for i in issues if i["id"] not in processed]
    if not unprocessed:
        log.info("No new Done issues to process")
        return

    log.info("Processing %d new Done issues", len(unprocessed))

    for issue in unprocessed:
        issue_id = issue["id"]
        identifier = issue.get("identifier", "?")

        evaluation = evaluate_issue(issue)
        if evaluation is None:
            log.warning("Skipping %s — evaluation failed (will retry next run)", identifier)
            continue

        now_iso = datetime.now(timezone.utc).isoformat()

        if evaluation.get("needs_followup"):
            created = create_followup(issue, evaluation)
            if created is None:
                log.warning("Skipping %s — follow-up creation failed (will retry next run)", identifier)
                continue
            processed[issue_id] = {
                "at": now_iso,
                "action": "followup_created",
                "followup": created.get("identifier"),
            }
        else:
            processed[issue_id] = {
                "at": now_iso,
                "action": "no_followup_needed",
            }

        # Save after each issue for crash resilience
        save_processed(processed)
        log.info("Marked %s as processed (action=%s)", identifier, processed[issue_id]["action"])

    log.info("Done. Processed %d issues this run.", len(unprocessed))


if __name__ == "__main__":
    main()
