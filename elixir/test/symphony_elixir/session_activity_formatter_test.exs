defmodule SymphonyElixir.SessionActivityFormatterTest do
  use ExUnit.Case, async: true

  alias SymphonyElixirWeb.SessionActivityFormatter

  test "readable_events merges streaming chunks into one entry" do
    now = dt!("2026-03-08T20:22:20Z")

    events = [
      event("2026-03-08T20:22:12Z", "notification", "agent message streaming: Hel"),
      event("2026-03-08T20:22:12Z", "notification", "agent message content streaming: lo"),
      event("2026-03-08T20:22:13Z", "notification", "agent message streaming: !")
    ]

    assert [
             %{
               label: "Agent Message",
               message: "Hello!",
               at_relative: "7s ago"
             }
           ] = SessionActivityFormatter.readable_events(events, now)
  end

  test "readable_events does not merge streaming chunks across turn boundaries" do
    now = dt!("2026-03-08T20:22:20Z")

    events = [
      event("2026-03-08T20:22:12Z", "notification", "agent message streaming: one", turn_count: 1),
      event("2026-03-08T20:22:12Z", "notification", "agent message streaming: two", turn_count: 2)
    ]

    readable = SessionActivityFormatter.readable_events(events, now)
    assert Enum.map(readable, & &1.message) == ["one", "two"]
  end

  test "readable_events splits stream bursts when gap is larger than merge window" do
    now = dt!("2026-03-08T20:22:30Z")

    events = [
      event("2026-03-08T20:22:12Z", "notification", "reasoning streaming: first"),
      event("2026-03-08T20:22:15Z", "notification", "reasoning content streaming: second")
    ]

    readable = SessionActivityFormatter.readable_events(events, now)
    assert Enum.map(readable, & &1.message) == ["first", "second"]
    assert Enum.map(readable, & &1.label) == ["Reasoning", "Reasoning"]
  end

  test "readable_events deduplicates identical non-stream rows in the same second" do
    now = dt!("2026-03-08T20:22:30Z")

    events = [
      event("2026-03-08T20:22:12Z", "session started", "session started"),
      event("2026-03-08T20:22:12Z", "session started", "session started"),
      event("2026-03-08T20:22:13Z", "session started", "session started")
    ]

    assert [
             %{label: "Session Started", count: 2},
             %{label: "Session Started", count: 1}
           ] = SessionActivityFormatter.readable_events(events, now)
  end

  test "raw_events adds compact metadata without changing message semantics" do
    now = dt!("2026-03-08T20:22:20Z")

    [raw] =
      SessionActivityFormatter.raw_events(
        [
          event(
            "2026-03-08T20:22:12Z",
            "notification",
            "agent message content streaming: rendered",
            session_id: "019ccf1a-21d2-73a0-9b09-ef7dcd4034b4-019ccf1a-2251-7212-8664-65d3210f9ca4"
          )
        ],
        now
      )

    assert raw.message == "agent message content streaming: rendered"
    assert raw.at_relative == "8s ago"
    assert raw.session_short == "019ccf1a…0f9ca4"
  end

  test "human_action_events keeps only human action rows with details" do
    now = dt!("2026-03-08T20:22:30Z")

    events = [
      event("2026-03-08T20:22:12Z", "notification", "agent message streaming: noise"),
      event("2026-03-08T20:22:15Z", "notification", "dynamic tool call completed",
        kind: "human_action",
        category: "linear",
        action: "linear_comment_create",
        details: %{"operation" => "commentCreate", "issue_id" => "issue-123"}
      ),
      event("2026-03-08T20:22:18Z", "notification", "git commit", kind: "human_action", category: "git", action: "git_commit", details: %{"command" => "git commit -m \"x\""})
    ]

    human = SessionActivityFormatter.human_action_events(events, now)

    assert length(human) == 2
    assert Enum.map(human, & &1.label) == ["Linear Comment", "Git Commit"]
    assert Enum.at(human, 0).details == %{"issue_id" => "issue-123", "operation" => "commentCreate"}
    assert Enum.at(human, 1).details == %{"command" => "git commit -m \"x\""}
  end

  test "human_action_events infers linear and command actions from readable rows" do
    now = dt!("2026-03-08T20:22:30Z")

    events = [
      event("2026-03-08T20:22:16Z", "notification", "dynamic tool call completed (linear_graphql)"),
      event("2026-03-08T20:22:18Z", "notification", "/bin/zsh -lc gh pr view 9 --comments"),
      event(
        "2026-03-08T20:22:19Z",
        "notification",
        "agent message content streaming: I attached the PR and updated the Linear ticket."
      )
    ]

    human = SessionActivityFormatter.human_action_events(events, now)

    assert Enum.map(human, & &1.label) == ["Linear Tool", "GitHub PR", "Agent Message"]
    assert Enum.at(human, 1).action == "github_pr_command"
    assert Enum.at(human, 2).action == "agent_message"
  end

  test "human_action_events ignores unreadable agent message fragments" do
    now = dt!("2026-03-08T20:22:30Z")

    events = [
      event("2026-03-08T20:22:18Z", "notification", "agent message content streaming: Starting"),
      event("2026-03-08T20:22:19Z", "notification", "agent message content streaming: workflow")
    ]

    human = SessionActivityFormatter.human_action_events(events, now)

    refute Enum.any?(human, &(&1.label == "Agent Message"))
  end

  test "readable_events preserves intra-message spaces in stream chunks" do
    now = dt!("2026-03-08T20:22:30Z")

    events = [
      event("2026-03-08T20:22:18Z", "notification", "agent message content streaming: Hello "),
      event("2026-03-08T20:22:19Z", "notification", "agent message content streaming: world")
    ]

    [entry] = SessionActivityFormatter.readable_events(events, now)
    assert entry.message == "Hello world"
  end

  test "human_action_events prefers explicit human action over inferred duplicate" do
    now = dt!("2026-03-08T20:22:30Z")

    events = [
      event(
        "2026-03-08T20:22:16Z",
        "notification",
        "dynamic tool call completed (linear_graphql)",
        kind: "human_action",
        category: "linear",
        action: "linear_comment_create",
        details: %{"operation" => "commentCreate"}
      )
    ]

    human = SessionActivityFormatter.human_action_events(events, now)

    assert length(human) == 1
    assert Enum.at(human, 0).label == "Linear Comment"
    assert Enum.at(human, 0).action == "linear_comment_create"
  end

  defp event(at, label, message, overrides \\ []) do
    base = %{
      at: at,
      label: label,
      event: "notification",
      message: message,
      session_id: "session-1234567890",
      turn_count: 1
    }

    Enum.into(overrides, base)
  end

  defp dt!(iso8601) do
    {:ok, dt, 0} = DateTime.from_iso8601(iso8601)
    dt
  end
end
