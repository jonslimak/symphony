defmodule SymphonyElixir.ExtensionsTest do
  use SymphonyElixir.TestSupport

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  alias SymphonyElixir.Linear.Adapter
  alias SymphonyElixir.Tracker.Memory

  @endpoint SymphonyElixirWeb.Endpoint

  defmodule FakeLinearClient do
    def fetch_candidate_issues do
      send(self(), :fetch_candidate_issues_called)
      {:ok, [:candidate]}
    end

    def fetch_issues_by_states(states) do
      send(self(), {:fetch_issues_by_states_called, states})
      {:ok, states}
    end

    def fetch_project_issues(limit) do
      send(self(), {:fetch_project_issues_called, limit})
      {:ok, Enum.to_list(1..limit)}
    end

    def fetch_issue_states_by_ids(issue_ids) do
      send(self(), {:fetch_issue_states_by_ids_called, issue_ids})
      {:ok, issue_ids}
    end

    def graphql(query, variables) do
      send(self(), {:graphql_called, query, variables})

      case Process.get({__MODULE__, :graphql_results}) do
        [result | rest] ->
          Process.put({__MODULE__, :graphql_results}, rest)
          result

        _ ->
          Process.get({__MODULE__, :graphql_result})
      end
    end
  end

  defmodule SlowOrchestrator do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, :ok, opts)
    end

    def init(:ok), do: {:ok, :ok}

    def handle_call(:snapshot, _from, state) do
      Process.sleep(25)
      {:reply, %{}, state}
    end

    def handle_call(:request_refresh, _from, state) do
      {:reply, :unavailable, state}
    end
  end

  defmodule StaticOrchestrator do
    use GenServer

    def start_link(opts) do
      name = Keyword.fetch!(opts, :name)
      GenServer.start_link(__MODULE__, opts, name: name)
    end

    def init(opts), do: {:ok, opts}

    def handle_call(:snapshot, _from, state) do
      {:reply, Keyword.fetch!(state, :snapshot), state}
    end

    def handle_call(:request_refresh, _from, state) do
      {:reply, Keyword.get(state, :refresh, :unavailable), state}
    end

    def handle_call({:session_events, event_stream_id, limit}, _from, state) do
      session_events = Keyword.get(state, :session_events, %{})

      reply =
        case Map.get(session_events, event_stream_id) do
          events when is_list(events) -> {:ok, Enum.take(events, -limit)}
          _ -> {:error, :session_not_found}
        end

      {:reply, reply, state}
    end
  end

  setup do
    linear_client_module = Application.get_env(:symphony_elixir, :linear_client_module)

    on_exit(fn ->
      if is_nil(linear_client_module) do
        Application.delete_env(:symphony_elixir, :linear_client_module)
      else
        Application.put_env(:symphony_elixir, :linear_client_module, linear_client_module)
      end
    end)

    :ok
  end

  setup do
    endpoint_config = Application.get_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, [])

    on_exit(fn ->
      Application.put_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, endpoint_config)
    end)

    :ok
  end

  test "workflow store reloads changes, keeps last good workflow, and falls back when stopped" do
    ensure_workflow_store_running()
    assert {:ok, %{prompt: "You are an agent for this repository."}} = Workflow.current()

    write_workflow_file!(Workflow.workflow_file_path(), prompt: "Second prompt")
    send(WorkflowStore, :poll)

    assert_eventually(fn ->
      match?({:ok, %{prompt: "Second prompt"}}, Workflow.current())
    end)

    File.write!(Workflow.workflow_file_path(), "---\ntracker: [\n---\nBroken prompt\n")
    assert {:error, _reason} = WorkflowStore.force_reload()
    assert {:ok, %{prompt: "Second prompt"}} = Workflow.current()

    third_workflow = Path.join(Path.dirname(Workflow.workflow_file_path()), "THIRD_WORKFLOW.md")
    write_workflow_file!(third_workflow, prompt: "Third prompt")
    Workflow.set_workflow_file_path(third_workflow)
    assert {:ok, %{prompt: "Third prompt"}} = Workflow.current()

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)
    assert {:ok, %{prompt: "Third prompt"}} = WorkflowStore.current()
    assert :ok = WorkflowStore.force_reload()
    assert {:ok, _pid} = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)
  end

  test "workflow store init stops on missing workflow file" do
    missing_path = Path.join(Path.dirname(Workflow.workflow_file_path()), "MISSING_WORKFLOW.md")
    Workflow.set_workflow_file_path(missing_path)

    assert {:stop, {:missing_workflow_file, ^missing_path, :enoent}} = WorkflowStore.init([])
  end

  test "workflow store start_link and poll callback cover missing-file error paths" do
    ensure_workflow_store_running()
    existing_path = Workflow.workflow_file_path()
    manual_path = Path.join(Path.dirname(existing_path), "MANUAL_WORKFLOW.md")
    missing_path = Path.join(Path.dirname(existing_path), "MANUAL_MISSING_WORKFLOW.md")

    assert :ok = Supervisor.terminate_child(SymphonyElixir.Supervisor, WorkflowStore)

    Workflow.set_workflow_file_path(missing_path)

    assert {:error, {:missing_workflow_file, ^missing_path, :enoent}} =
             WorkflowStore.force_reload()

    write_workflow_file!(manual_path, prompt: "Manual workflow prompt")
    Workflow.set_workflow_file_path(manual_path)

    assert {:ok, manual_pid} = WorkflowStore.start_link()
    assert Process.alive?(manual_pid)

    state = :sys.get_state(manual_pid)
    File.write!(manual_path, "---\ntracker: [\n---\nBroken prompt\n")
    assert {:noreply, returned_state} = WorkflowStore.handle_info(:poll, state)
    assert returned_state.workflow.prompt == "Manual workflow prompt"
    refute returned_state.stamp == nil
    assert_receive :poll, 1_100

    Workflow.set_workflow_file_path(missing_path)
    assert {:noreply, path_error_state} = WorkflowStore.handle_info(:poll, returned_state)
    assert path_error_state.workflow.prompt == "Manual workflow prompt"
    assert_receive :poll, 1_100

    Workflow.set_workflow_file_path(manual_path)
    File.rm!(manual_path)
    assert {:noreply, removed_state} = WorkflowStore.handle_info(:poll, path_error_state)
    assert removed_state.workflow.prompt == "Manual workflow prompt"
    assert_receive :poll, 1_100

    Process.exit(manual_pid, :normal)
    restart_result = Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore)

    assert match?({:ok, _pid}, restart_result) or
             match?({:error, {:already_started, _pid}}, restart_result)

    Workflow.set_workflow_file_path(existing_path)
    WorkflowStore.force_reload()
  end

  test "tracker delegates to memory and linear adapters" do
    issue = %Issue{id: "issue-1", identifier: "MT-1", state: "In Progress"}
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue, %{id: "ignored"}])
    Application.put_env(:symphony_elixir, :memory_tracker_recipient, self())
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")

    assert Config.tracker_kind() == "memory"
    assert SymphonyElixir.Tracker.adapter() == Memory
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_candidate_issues()
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issues_by_states([" in progress ", 42])
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_project_issues(1)
    assert {:ok, [^issue]} = SymphonyElixir.Tracker.fetch_issue_states_by_ids(["issue-1"])
    assert :ok = SymphonyElixir.Tracker.create_comment("issue-1", "comment")
    assert :ok = SymphonyElixir.Tracker.update_issue_state("issue-1", "Done")
    assert :ok = SymphonyElixir.Tracker.attach_issue_resource("issue-1", "https://example.com", "Example")
    assert_receive {:memory_tracker_comment, "issue-1", "comment"}
    assert_receive {:memory_tracker_state_update, "issue-1", "Done"}
    assert_receive {:memory_tracker_resource_attach, "issue-1", "https://example.com", "Example"}

    Application.delete_env(:symphony_elixir, :memory_tracker_recipient)
    assert :ok = Memory.create_comment("issue-1", "quiet")
    assert :ok = Memory.update_issue_state("issue-1", "Quiet")
    assert :ok = Memory.attach_issue_resource("issue-1", "https://example.com", nil)

    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "linear")
    assert SymphonyElixir.Tracker.adapter() == Adapter
  end

  test "linear adapter delegates reads and validates mutation responses" do
    Application.put_env(:symphony_elixir, :linear_client_module, FakeLinearClient)

    assert {:ok, [:candidate]} = Adapter.fetch_candidate_issues()
    assert_receive :fetch_candidate_issues_called

    assert {:ok, ["Todo"]} = Adapter.fetch_issues_by_states(["Todo"])
    assert_receive {:fetch_issues_by_states_called, ["Todo"]}

    assert {:ok, [1, 2, 3]} = Adapter.fetch_project_issues(3)
    assert_receive {:fetch_project_issues_called, 3}

    assert {:ok, ["issue-1"]} = Adapter.fetch_issue_states_by_ids(["issue-1"])
    assert_receive {:fetch_issue_states_by_ids_called, ["issue-1"]}

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => true}}}}
    )

    assert :ok = Adapter.create_comment("issue-1", "hello")
    assert_receive {:graphql_called, create_comment_query, %{body: "hello", issueId: "issue-1"}}
    assert create_comment_query =~ "commentCreate"

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"commentCreate" => %{"success" => false}}}}
    )

    assert {:error, :comment_create_failed} =
             Adapter.create_comment("issue-1", "broken")

    Process.put({FakeLinearClient, :graphql_result}, {:error, :boom})

    assert {:error, :boom} = Adapter.create_comment("issue-1", "boom")

    Process.put({FakeLinearClient, :graphql_result}, {:ok, %{"data" => %{}}})
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "weird")

    Process.put({FakeLinearClient, :graphql_result}, :unexpected)
    assert {:error, :comment_create_failed} = Adapter.create_comment("issue-1", "odd")

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{"issueUpdate" => %{"success" => true}}}}
      ]
    )

    assert :ok = Adapter.update_issue_state("issue-1", "Done")
    assert_receive {:graphql_called, state_lookup_query, %{issueId: "issue-1", stateName: "Done"}}
    assert state_lookup_query =~ "states"

    assert_receive {:graphql_called, update_issue_query, %{issueId: "issue-1", stateId: "state-1"}}

    assert update_issue_query =~ "issueUpdate"

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{"issueUpdate" => %{"success" => false}}}}
      ]
    )

    assert {:error, :issue_update_failed} =
             Adapter.update_issue_state("issue-1", "Broken")

    Process.put({FakeLinearClient, :graphql_results}, [{:error, :boom}])

    assert {:error, :boom} = Adapter.update_issue_state("issue-1", "Boom")

    Process.put({FakeLinearClient, :graphql_results}, [{:ok, %{"data" => %{}}}])
    assert {:error, :state_not_found} = Adapter.update_issue_state("issue-1", "Missing")

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        {:ok, %{"data" => %{}}}
      ]
    )

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Weird")

    Process.put(
      {FakeLinearClient, :graphql_results},
      [
        {:ok,
         %{
           "data" => %{
             "issue" => %{"team" => %{"states" => %{"nodes" => [%{"id" => "state-1"}]}}}
           }
         }},
        :unexpected
      ]
    )

    assert {:error, :issue_update_failed} = Adapter.update_issue_state("issue-1", "Odd")

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"attachmentLinkGitHubPR" => %{"success" => true}}}}
    )

    assert :ok =
             Adapter.attach_issue_resource(
               "issue-1",
               "https://github.com/openai/symphony/pull/18",
               "PR 18"
             )

    assert_receive {:graphql_called, github_pr_query,
                     %{
                       issueId: "issue-1",
                       title: "PR 18",
                       url: "https://github.com/openai/symphony/pull/18"
                     }}

    assert github_pr_query =~ "attachmentLinkGitHubPR"

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"attachmentLinkURL" => %{"success" => true}}}}
    )

    assert :ok = Adapter.attach_issue_resource("issue-1", "https://example.com/run/1", nil)

    assert_receive {:graphql_called, url_query,
                     %{issueId: "issue-1", title: nil, url: "https://example.com/run/1"}}

    assert url_query =~ "attachmentLinkURL"

    Process.put(
      {FakeLinearClient, :graphql_result},
      {:ok, %{"data" => %{"attachmentLinkURL" => %{"success" => false}}}}
    )

    assert {:error, :attachment_link_failed} =
             Adapter.attach_issue_resource("issue-1", "https://example.com/fail", nil)

    Process.put({FakeLinearClient, :graphql_result}, {:error, :boom})

    assert {:error, :boom} =
             Adapter.attach_issue_resource("issue-1", "https://example.com/boom", nil)
  end

  test "phoenix observability api preserves state, issue, and refresh responses" do
    snapshot = static_snapshot()
    orchestrator_name = Module.concat(__MODULE__, :ObservabilityApiOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        session_events: static_session_events(),
        refresh: %{
          queued: true,
          coalesced: false,
          requested_at: DateTime.utc_now(),
          operations: ["poll", "reconcile"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    conn = get(build_conn(), "/api/v1/state")
    state_payload = json_response(conn, 200)

    assert state_payload == %{
             "generated_at" => state_payload["generated_at"],
             "counts" => %{"running" => 1, "retrying" => 1},
             "running" => [
               %{
                 "issue_id" => "issue-http",
                 "issue_identifier" => "MT-HTTP",
                 "state" => "In Progress",
                 "event_stream_id" => "mt-http-session",
                 "session_id" => "thread-http",
                 "turn_count" => 7,
                 "last_event" => "notification",
                 "last_message" => "rendered",
                 "started_at" => state_payload["running"] |> List.first() |> Map.fetch!("started_at"),
                 "last_event_at" => nil,
                 "tokens" => %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12}
               }
             ],
             "retrying" => [
               %{
                 "issue_id" => "issue-retry",
                 "issue_identifier" => "MT-RETRY",
                 "attempt" => 2,
                 "due_at" => state_payload["retrying"] |> List.first() |> Map.fetch!("due_at"),
                 "error" => "boom"
               }
             ],
             "inactive_sessions" => [
               %{
                 "issue_id" => "issue-old",
                 "issue_identifier" => "MT-OLD",
                 "state" => "In Review",
                 "event_stream_id" => "mt-old-session",
                 "session_id" => "thread-old",
                 "turn_count" => 3,
                 "stop_reason" => "completed",
                 "started_at" => state_payload["inactive_sessions"] |> List.first() |> Map.fetch!("started_at"),
                 "ended_at" => state_payload["inactive_sessions"] |> List.first() |> Map.fetch!("ended_at"),
                 "runtime_seconds" => 90,
                 "last_event" => "turn_completed",
                 "last_message" => "done",
                 "last_event_at" => state_payload["inactive_sessions"] |> List.first() |> Map.fetch!("last_event_at"),
                 "tokens" => %{"input_tokens" => 20, "output_tokens" => 40, "total_tokens" => 60}
               }
             ],
             "codex_totals" => %{
               "input_tokens" => 4,
               "output_tokens" => 8,
               "total_tokens" => 12,
               "seconds_running" => 42.5
             },
             "rate_limits" => %{"primary" => %{"remaining" => 11}}
           }

    conn = get(build_conn(), "/api/v1/MT-HTTP")
    issue_payload = json_response(conn, 200)

    assert issue_payload == %{
             "issue_identifier" => "MT-HTTP",
             "issue_id" => "issue-http",
             "status" => "running",
             "workspace" => %{"path" => Path.join(Config.workspace_root(), "MT-HTTP")},
             "attempts" => %{"restart_count" => 0, "current_retry_attempt" => 0},
             "running" => %{
               "event_stream_id" => "mt-http-session",
               "session_id" => "thread-http",
               "turn_count" => 7,
               "state" => "In Progress",
               "started_at" => issue_payload["running"]["started_at"],
               "last_event" => "notification",
               "last_message" => "rendered",
               "last_event_at" => nil,
               "tokens" => %{"input_tokens" => 4, "output_tokens" => 8, "total_tokens" => 12}
             },
             "retry" => nil,
             "logs" => %{"codex_session_logs" => []},
             "recent_events" => [],
             "last_error" => nil,
             "tracked" => %{}
           }

    conn = get(build_conn(), "/api/v1/MT-RETRY")

    assert %{"status" => "retrying", "retry" => %{"attempt" => 2, "error" => "boom"}} =
             json_response(conn, 200)

    conn = get(build_conn(), "/api/v1/MT-MISSING")

    assert json_response(conn, 404) == %{
             "error" => %{"code" => "issue_not_found", "message" => "Issue not found"}
           }

    conn = get(build_conn(), "/api/v1/session/mt-http-session/events?limit=1")
    session_events_payload = json_response(conn, 200)

    assert session_events_payload == %{
             "event_stream_id" => "mt-http-session",
             "events" => [
               %{
                 "at" => "2026-01-01T10:00:05Z",
                 "issue_identifier" => "MT-HTTP",
                 "session_id" => "thread-http",
                 "turn_count" => 7,
                 "event" => "notification",
                 "label" => "notification",
                 "message" => "thread/status/changed",
                 "kind" => "human_action",
                 "category" => "status",
                 "action" => "thread_status_changed",
                 "details" => %{"from" => "planning", "to" => "in_progress"}
               }
             ]
           }

    conn = get(build_conn(), "/api/v1/session/missing-session/events")

    assert json_response(conn, 404) == %{
             "error" => %{
               "code" => "session_not_found",
               "message" => "Session timeline not found"
             }
           }

    conn = post(build_conn(), "/api/v1/refresh", %{})

    assert %{"queued" => true, "coalesced" => false, "operations" => ["poll", "reconcile"]} =
             json_response(conn, 202)
  end

  test "phoenix observability api preserves 405, 404, and unavailable behavior" do
    unavailable_orchestrator = Module.concat(__MODULE__, :UnavailableOrchestrator)
    start_test_endpoint(orchestrator: unavailable_orchestrator, snapshot_timeout_ms: 5)

    assert json_response(post(build_conn(), "/api/v1/state", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(get(build_conn(), "/api/v1/refresh"), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/api/v1/MT-1", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(post(build_conn(), "/api/v1/session/stream/events", %{}), 405) ==
             %{"error" => %{"code" => "method_not_allowed", "message" => "Method not allowed"}}

    assert json_response(get(build_conn(), "/unknown"), 404) ==
             %{"error" => %{"code" => "not_found", "message" => "Route not found"}}

    state_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert state_payload ==
             %{
               "generated_at" => state_payload["generated_at"],
               "error" => %{"code" => "snapshot_unavailable", "message" => "Snapshot unavailable"}
             }

    assert json_response(post(build_conn(), "/api/v1/refresh", %{}), 503) ==
             %{
               "error" => %{
                 "code" => "orchestrator_unavailable",
                 "message" => "Orchestrator is unavailable"
               }
             }
  end

  test "phoenix observability api preserves snapshot timeout behavior" do
    timeout_orchestrator = Module.concat(__MODULE__, :TimeoutOrchestrator)
    {:ok, _pid} = SlowOrchestrator.start_link(name: timeout_orchestrator)
    start_test_endpoint(orchestrator: timeout_orchestrator, snapshot_timeout_ms: 1)

    timeout_payload = json_response(get(build_conn(), "/api/v1/state"), 200)

    assert timeout_payload ==
             %{
               "generated_at" => timeout_payload["generated_at"],
               "error" => %{"code" => "snapshot_timeout", "message" => "Snapshot timed out"}
             }
  end

  test "dashboard bootstraps liveview from embedded static assets" do
    orchestrator_name = Module.concat(__MODULE__, :AssetOrchestrator)

    {:ok, _pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: static_snapshot(),
        session_events: static_session_events(),
        refresh: %{
          queued: true,
          coalesced: false,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    html = html_response(get(build_conn(), "/"), 200)
    assert html =~ "/dashboard.css"
    assert html =~ "/vendor/phoenix_html/phoenix_html.js"
    assert html =~ "/vendor/phoenix/phoenix.js"
    assert html =~ "/vendor/phoenix_live_view/phoenix_live_view.js"
    refute html =~ "/assets/app.js"
    refute html =~ "<style>"

    dashboard_css = response(get(build_conn(), "/dashboard.css"), 200)
    assert dashboard_css =~ ":root {"
    assert dashboard_css =~ "\"Roboto Mono\""
    assert dashboard_css =~ "/fonts/roboto-mono-400.ttf"
    assert dashboard_css =~ ".status-inline-live"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-inline-live"
    assert dashboard_css =~ "[data-phx-main].phx-connected .status-inline-offline"

    roboto_font = response(get(build_conn(), "/fonts/roboto-mono-400.ttf"), 200)
    assert byte_size(roboto_font) > 10_000

    phoenix_html_js = response(get(build_conn(), "/vendor/phoenix_html/phoenix_html.js"), 200)
    assert phoenix_html_js =~ "phoenix.link.click"

    phoenix_js = response(get(build_conn(), "/vendor/phoenix/phoenix.js"), 200)
    assert phoenix_js =~ "var Phoenix = (() => {"

    live_view_js =
      response(get(build_conn(), "/vendor/phoenix_live_view/phoenix_live_view.js"), 200)

    assert live_view_js =~ "var LiveView = (() => {"
  end

  test "dashboard liveview renders and refreshes over pubsub" do
    orchestrator_name = Module.concat(__MODULE__, :DashboardOrchestrator)
    snapshot = static_snapshot()

    {:ok, orchestrator_pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        session_events: static_session_events(),
        refresh: %{
          queued: true,
          coalesced: true,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(orchestrator: orchestrator_name, snapshot_timeout_ms: 50)

    {:ok, view, html} = live(build_conn(), "/")
    assert html =~ "MT-HTTP"
    assert html =~ "MT-RETRY"
    assert html =~ "rendered"
    assert html =~ "Runtime"
    assert html =~ "Live"
    assert html =~ "Offline"
    assert html =~ "Copy ID"
    assert html =~ "Activity"
    assert html =~ "Codex update"
    assert html =~ "Inactive sessions"
    assert html =~ "MT-OLD"
    refute html =~ "data-runtime-clock="
    refute html =~ "setInterval(refreshRuntimeClocks"
    refute html =~ "Refresh now"
    refute html =~ "Transport"
    assert html =~ "status-inline-live"
    assert html =~ "status-inline-offline"

    view
    |> element("button[phx-click='open_activity'][phx-value-stream_id='mt-http-session']")
    |> render_click()

    assert render(view) =~ "Session activity"
    assert render(view) =~ "mt-http-session"
    assert render(view) =~ "Human actions"
    assert render(view) =~ "Readable"
    assert render(view) =~ "Raw"
    assert render(view) =~ "Status Changed"
    assert render(view) =~ "thread/status/changed"

    refute render(view) =~ "agent message content streaming: rendered"

    view
    |> element("button[phx-click='toggle_activity_detail']")
    |> render_click()

    assert render(view) =~ "planning"
    assert render(view) =~ "in_progress"

    view
    |> element("button[phx-click='set_activity_mode'][phx-value-mode='readable']")
    |> render_click()

    assert render(view) =~ "Agent Message"
    assert render(view) =~ "rendered"

    view
    |> element("button[phx-click='set_activity_mode'][phx-value-mode='raw']")
    |> render_click()

    assert render(view) =~ "agent message content streaming: rendered"

    view
    |> element("button[phx-click='set_activity_mode'][phx-value-mode='human']")
    |> render_click()

    :sys.replace_state(orchestrator_pid, fn state ->
      session_events = Keyword.get(state, :session_events, %{})

      refreshed_events = [
        %{
          at: "2026-01-01T10:00:06Z",
          issue_identifier: "MT-HTTP",
          session_id: "thread-http",
          turn_count: 8,
          event: "notification",
          label: "notification",
          message: "agent message content streaming: refresh only"
        }
      ]

      Keyword.put(state, :session_events, Map.put(session_events, "mt-http-session", refreshed_events))
    end)

    StatusDashboard.notify_update()

    assert_eventually(fn ->
      html = render(view)
      html =~ "Status Changed" and html =~ "thread/status/changed"
    end)

    view
    |> element("button.secondary[phx-click='close_activity']")
    |> render_click()

    refute render(view) =~ "Session activity drawer"

    updated_snapshot =
      put_in(snapshot.running, [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          state: "In Progress",
          event_stream_id: "mt-http-session",
          session_id: "thread-http",
          turn_count: 8,
          last_codex_event: :notification,
          last_codex_message: %{
            event: :notification,
            message: %{
              payload: %{
                "method" => "codex/event/agent_message_content_delta",
                "params" => %{
                  "msg" => %{
                    "content" => "structured update"
                  }
                }
              }
            }
          },
          last_codex_timestamp: DateTime.utc_now(),
          codex_input_tokens: 10,
          codex_output_tokens: 12,
          codex_total_tokens: 22,
          started_at: DateTime.utc_now()
        }
      ])

    :sys.replace_state(orchestrator_pid, fn state ->
      Keyword.put(state, :snapshot, updated_snapshot)
    end)

    StatusDashboard.notify_update()

    assert_eventually(fn ->
      render(view) =~ "structured update"
    end)
  end

  test "dashboard liveview renders project tickets and keeps stale list on fetch error" do
    orchestrator_name = Module.concat(__MODULE__, :ProjectTicketOrchestrator)
    snapshot = static_snapshot()
    parent = self()

    project_tickets = [
      %Issue{
        id: "issue-300",
        identifier: "MT-300",
        title: "Backlog item",
        state: "Backlog",
        url: "https://linear.app/example/issue/MT-300",
        updated_at: ~U[2026-01-01 12:00:00Z]
      },
      %Issue{
        id: "issue-200",
        identifier: "MT-200",
        title: "Older todo item",
        state: "Todo",
        url: "https://linear.app/example/issue/MT-200",
        updated_at: ~U[2026-01-01 10:00:00Z]
      },
      %Issue{
        id: "issue-100",
        identifier: "MT-100",
        title: "Newer todo item",
        state: "Todo",
        url: "https://linear.app/example/issue/MT-100",
        latest_resource_title: "Latest PR",
        latest_resource_url: "https://github.com/jonslimak/sym-pilot/pull/999",
        updated_at: ~U[2026-01-01 11:00:00Z]
      }
    ]

    {:ok, ticket_source} = Agent.start_link(fn -> {:ok, project_tickets} end)

    fetcher = fn limit ->
      send(parent, {:project_tickets_fetch, limit})
      Agent.get(ticket_source, & &1)
    end

    {:ok, _orchestrator_pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        session_events: static_session_events(),
        refresh: %{
          queued: true,
          coalesced: true,
          requested_at: DateTime.utc_now(),
          operations: ["poll"]
        }
      )

    start_test_endpoint(
      orchestrator: orchestrator_name,
      snapshot_timeout_ms: 50,
      project_tickets_fetcher: fetcher
    )

    {:ok, view, html} = live(build_conn(), "/")
    assert_receive {:project_tickets_fetch, 100}

    assert html =~ "Project tickets"
    assert html =~ "https://linear.app/validation/project/project/issues"
    assert html =~ "MT-300"
    assert html =~ "MT-100"
    assert html =~ "MT-200"
    assert html =~ "https://linear.app/example/issue/MT-100"
    assert html =~ "https://github.com/jonslimak/sym-pilot/pull/999"
    assert html_index!(html, "Rate limits") < html_index!(html, "Project tickets")
    assert html_index!(html, "Backlog") < html_index!(html, "Todo")
    assert html_index!(html, "MT-100") < html_index!(html, "MT-200")

    Agent.update(ticket_source, fn _state -> {:error, :boom} end)
    StatusDashboard.notify_update()

    assert_eventually(fn ->
      refreshed = render(view)

      refreshed =~ "Project tickets refresh failed: :boom" and
        refreshed =~ "MT-300" and
        refreshed =~ "MT-100" and
        refreshed =~ "MT-200"
    end)
  end

  test "dashboard liveview updates ticket status and adds ticket comments from project ticket rows" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")
    Application.put_env(:symphony_elixir, :memory_tracker_recipient, self())

    orchestrator_name = Module.concat(__MODULE__, :ProjectTicketActionsOrchestrator)
    snapshot = static_snapshot()

    project_tickets = [
      %Issue{
        id: "issue-100",
        identifier: "MT-100",
        title: "Editable ticket",
        state: "Todo",
        url: "https://linear.app/example/issue/MT-100",
        updated_at: ~U[2026-01-01 11:00:00Z]
      }
    ]

    fetcher = fn _limit -> {:ok, project_tickets} end

    {:ok, _orchestrator_pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        session_events: static_session_events()
      )

    start_test_endpoint(
      orchestrator: orchestrator_name,
      snapshot_timeout_ms: 50,
      project_tickets_fetcher: fetcher
    )

    {:ok, view, html} = live(build_conn(), "/")
    assert html =~ "Project tickets"

    view
    |> element("button[phx-click='open_ticket_status_editor'][phx-value-issue_id='issue-100']")
    |> render_click()

    assert has_element?(view, "form[phx-submit='submit_ticket_status'] select[name='status']")

    view
    |> element("form[phx-submit='submit_ticket_status']")
    |> render_submit(%{"issue_id" => "issue-100", "status" => "Done"})

    assert_receive {:memory_tracker_state_update, "issue-100", "Done"}

    view
    |> element("button[phx-click='open_ticket_comment_editor'][phx-value-issue_id='issue-100']")
    |> render_click()

    assert has_element?(view, "form[phx-submit='submit_ticket_comment'] input[name='comment_body']")

    view
    |> element("form[phx-submit='submit_ticket_comment']")
    |> render_submit(%{"issue_id" => "issue-100", "comment_body" => "Looks good"})

    assert_receive {:memory_tracker_comment, "issue-100", "Looks good"}
  end

  test "dashboard liveview validates ticket actions and surfaces mutation errors inline" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory")
    Application.put_env(:symphony_elixir, :memory_tracker_recipient, self())

    orchestrator_name = Module.concat(__MODULE__, :ProjectTicketActionErrorsOrchestrator)
    snapshot = static_snapshot()

    project_tickets = [
      %Issue{
        id: "issue-100",
        identifier: "MT-100",
        title: "Editable ticket",
        state: "Todo",
        url: "https://linear.app/example/issue/MT-100",
        updated_at: ~U[2026-01-01 11:00:00Z]
      }
    ]

    fetcher = fn _limit -> {:ok, project_tickets} end

    {:ok, _orchestrator_pid} =
      StaticOrchestrator.start_link(
        name: orchestrator_name,
        snapshot: snapshot,
        session_events: static_session_events()
      )

    start_test_endpoint(
      orchestrator: orchestrator_name,
      snapshot_timeout_ms: 50,
      project_tickets_fetcher: fetcher,
      issue_state_updater: fn _issue_id, _status -> {:error, :boom} end,
      issue_comment_creator: fn _issue_id, _body -> {:error, :comment_boom} end
    )

    {:ok, view, _html} = live(build_conn(), "/")

    view
    |> element("button[phx-click='open_ticket_status_editor'][phx-value-issue_id='issue-100']")
    |> render_click()

    view
    |> element("form[phx-submit='submit_ticket_status']")
    |> render_submit(%{"issue_id" => "issue-100", "status" => "NotARealState"})

    assert has_element?(view, "form[phx-submit='submit_ticket_status'] select[name='status']")
    refute_received {:memory_tracker_state_update, "issue-100", _state}

    view
    |> element("form[phx-submit='submit_ticket_status']")
    |> render_submit(%{"issue_id" => "issue-100", "status" => "Done"})

    assert has_element?(view, "form[phx-submit='submit_ticket_status'] select[name='status']")

    view
    |> element("button[phx-click='open_ticket_comment_editor'][phx-value-issue_id='issue-100']")
    |> render_click()

    view
    |> element("form[phx-submit='submit_ticket_comment']")
    |> render_submit(%{"issue_id" => "issue-100", "comment_body" => "   "})

    assert has_element?(view, "form[phx-submit='submit_ticket_comment'] input[name='comment_body']")
    refute_received {:memory_tracker_comment, "issue-100", _body}

    view
    |> element("form[phx-submit='submit_ticket_comment']")
    |> render_submit(%{"issue_id" => "issue-100", "comment_body" => "retry comment"})

    assert has_element?(view, "form[phx-submit='submit_ticket_comment'] input[name='comment_body']")
  end

  test "dashboard liveview renders an unavailable state without crashing" do
    start_test_endpoint(
      orchestrator: Module.concat(__MODULE__, :MissingDashboardOrchestrator),
      snapshot_timeout_ms: 5
    )

    {:ok, _view, html} = live(build_conn(), "/")
    assert html =~ "Snapshot unavailable"
    assert html =~ "snapshot_unavailable"
  end

  test "http server serves embedded assets, accepts form posts, and rejects invalid hosts" do
    spec = HttpServer.child_spec(port: 0)
    assert spec.id == HttpServer
    assert spec.start == {HttpServer, :start_link, [[port: 0]]}

    assert :ignore = HttpServer.start_link(port: nil)
    assert HttpServer.bound_port() == nil

    snapshot = static_snapshot()
    orchestrator_name = Module.concat(__MODULE__, :BoundPortOrchestrator)

    refresh = %{
      queued: true,
      coalesced: false,
      requested_at: DateTime.utc_now(),
      operations: ["poll"]
    }

    server_opts = [
      host: "127.0.0.1",
      port: 0,
      orchestrator: orchestrator_name,
      snapshot_timeout_ms: 50
    ]

    start_supervised!({StaticOrchestrator, name: orchestrator_name, snapshot: snapshot, session_events: static_session_events(), refresh: refresh})

    start_supervised!({HttpServer, server_opts})

    port = wait_for_bound_port()
    assert port == HttpServer.bound_port()

    response = Req.get!("http://127.0.0.1:#{port}/api/v1/state")
    assert response.status == 200
    assert response.body["counts"] == %{"running" => 1, "retrying" => 1}

    dashboard_css = Req.get!("http://127.0.0.1:#{port}/dashboard.css")
    assert dashboard_css.status == 200
    assert dashboard_css.body =~ ":root {"

    phoenix_js = Req.get!("http://127.0.0.1:#{port}/vendor/phoenix/phoenix.js")
    assert phoenix_js.status == 200
    assert phoenix_js.body =~ "var Phoenix = (() => {"

    refresh_response =
      Req.post!("http://127.0.0.1:#{port}/api/v1/refresh",
        headers: [{"content-type", "application/x-www-form-urlencoded"}],
        body: ""
      )

    assert refresh_response.status == 202
    assert refresh_response.body["queued"] == true

    method_not_allowed_response =
      Req.post!("http://127.0.0.1:#{port}/api/v1/state",
        headers: [{"content-type", "application/x-www-form-urlencoded"}],
        body: ""
      )

    assert method_not_allowed_response.status == 405
    assert method_not_allowed_response.body["error"]["code"] == "method_not_allowed"

    assert {:error, _reason} = HttpServer.start_link(host: "bad host", port: 0)
  end

  defp start_test_endpoint(overrides) do
    endpoint_config =
      :symphony_elixir
      |> Application.get_env(SymphonyElixirWeb.Endpoint, [])
      |> Keyword.merge(server: false, secret_key_base: String.duplicate("s", 64))
      |> Keyword.merge(overrides)
      |> Keyword.put_new(:project_tickets_fetcher, fn _limit -> {:ok, []} end)

    Application.put_env(:symphony_elixir, SymphonyElixirWeb.Endpoint, endpoint_config)
    start_supervised!({SymphonyElixirWeb.Endpoint, []})
  end

  defp static_snapshot do
    %{
      running: [
        %{
          issue_id: "issue-http",
          identifier: "MT-HTTP",
          state: "In Progress",
          event_stream_id: "mt-http-session",
          session_id: "thread-http",
          turn_count: 7,
          codex_app_server_pid: nil,
          last_codex_message: "rendered",
          last_codex_timestamp: nil,
          last_codex_event: :notification,
          codex_input_tokens: 4,
          codex_output_tokens: 8,
          codex_total_tokens: 12,
          started_at: DateTime.utc_now()
        }
      ],
      retrying: [
        %{
          issue_id: "issue-retry",
          identifier: "MT-RETRY",
          attempt: 2,
          due_in_ms: 2_000,
          error: "boom"
        }
      ],
      inactive_sessions: [
        %{
          issue_id: "issue-old",
          identifier: "MT-OLD",
          state: "In Review",
          event_stream_id: "mt-old-session",
          session_id: "thread-old",
          turn_count: 3,
          stop_reason: "completed",
          started_at: DateTime.add(DateTime.utc_now(), -90, :second),
          ended_at: DateTime.utc_now(),
          runtime_seconds: 90,
          last_codex_event: :turn_completed,
          last_codex_message: "done",
          last_codex_timestamp: DateTime.utc_now(),
          codex_input_tokens: 20,
          codex_output_tokens: 40,
          codex_total_tokens: 60
        }
      ],
      codex_totals: %{input_tokens: 4, output_tokens: 8, total_tokens: 12, seconds_running: 42.5},
      rate_limits: %{"primary" => %{"remaining" => 11}}
    }
  end

  defp static_session_events do
    %{
      "mt-http-session" => [
        %{
          at: "2026-01-01T10:00:01Z",
          issue_identifier: "MT-HTTP",
          session_id: "thread-http",
          turn_count: 7,
          event: "session_started",
          label: "session started",
          message: "session started"
        },
        %{
          at: "2026-01-01T10:00:04Z",
          issue_identifier: "MT-HTTP",
          session_id: "thread-http",
          turn_count: 7,
          event: "notification",
          label: "notification",
          message: "agent message content streaming: rendered"
        },
        %{
          at: "2026-01-01T10:00:05Z",
          issue_identifier: "MT-HTTP",
          session_id: "thread-http",
          turn_count: 7,
          event: "notification",
          label: "notification",
          message: "thread/status/changed",
          kind: "human_action",
          category: "status",
          action: "thread_status_changed",
          details: %{"from" => "planning", "to" => "in_progress"}
        }
      ],
      "mt-old-session" => [
        %{
          at: "2026-01-01T09:59:00Z",
          issue_identifier: "MT-OLD",
          session_id: "thread-old",
          turn_count: 3,
          event: "turn_completed",
          label: "turn completed",
          message: "done"
        }
      ]
    }
  end

  defp wait_for_bound_port do
    assert_eventually(fn ->
      is_integer(HttpServer.bound_port())
    end)

    HttpServer.bound_port()
  end

  defp assert_eventually(fun, attempts \\ 20)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      true
    else
      Process.sleep(25)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")

  defp html_index!(html, value) do
    case :binary.match(html, value) do
      {index, _length} -> index
      :nomatch -> flunk("expected to find #{inspect(value)} in HTML")
    end
  end

  defp ensure_workflow_store_running do
    if Process.whereis(WorkflowStore) do
      :ok
    else
      case Supervisor.restart_child(SymphonyElixir.Supervisor, WorkflowStore) do
        {:ok, _pid} -> :ok
        {:error, {:already_started, _pid}} -> :ok
      end
    end
  end
end
