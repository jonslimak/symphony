defmodule SymphonyElixir.Orchestrator do
  @moduledoc """
  Polls Linear and dispatches repository copies to Codex-backed workers.
  """

  use GenServer
  require Logger
  import Bitwise, only: [<<<: 2]

  alias SymphonyElixir.{AgentRunner, Config, InactiveSessionStore, RunRecordStore, SessionTimelineStore, StatusDashboard, Tracker, Workflow, Workspace}
  alias SymphonyElixir.Linear.Issue

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  @inactive_sessions_limit 50
  @timeline_events_limit 200
  @timeline_retention_seconds 14 * 24 * 60 * 60
  @timeline_cleanup_interval_ms 60 * 60 * 1_000
  # Slightly above the dashboard render interval so "checking now…" can render.
  @poll_transition_render_delay_ms 20
  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  defmodule State do
    @moduledoc """
    Runtime state for the orchestrator polling loop.
    """

    defstruct [
      :poll_interval_ms,
      :max_concurrent_agents,
      :next_poll_due_at_ms,
      :poll_check_in_progress,
      running: %{},
      timeline_events: %{},
      timeline_last_cleanup_ms: nil,
      inactive_sessions: [],
      completed: MapSet.new(),
      claimed: MapSet.new(),
      suppressed_dispatch: %{},
      retry_attempts: %{},
      codex_totals: nil,
      codex_rate_limits: nil
    ]
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(_opts) do
    now_ms = System.monotonic_time(:millisecond)

    state = %State{
      poll_interval_ms: Config.poll_interval_ms(),
      max_concurrent_agents: Config.max_concurrent_agents(),
      next_poll_due_at_ms: now_ms,
      poll_check_in_progress: false,
      codex_totals: @empty_codex_totals,
      codex_rate_limits: nil,
      timeline_events: %{},
      timeline_last_cleanup_ms: nil
    }

    run_terminal_workspace_cleanup()
    state = maybe_cleanup_timeline_files(state, true)
    state = load_persisted_inactive_sessions(state)
    :ok = schedule_tick(0)

    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = refresh_runtime_config(state)
    state = maybe_cleanup_timeline_files(state)
    state = %{state | poll_check_in_progress: true, next_poll_due_at_ms: nil}

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info(:run_poll_cycle, state) do
    state = refresh_runtime_config(state)
    state = maybe_dispatch(state)
    now_ms = System.monotonic_time(:millisecond)
    next_poll_due_at_ms = now_ms + state.poll_interval_ms
    :ok = schedule_tick(state.poll_interval_ms)

    state = %{state | poll_check_in_progress: false, next_poll_due_at_ms: next_poll_due_at_ms}

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{running: running} = state
      ) do
    case find_issue_id_for_ref(running, ref) do
      nil ->
        {:noreply, state}

      issue_id ->
        {running_entry, state} = pop_running_entry(state, issue_id)

        state =
          state
          |> record_session_completion_totals(running_entry)
          |> record_inactive_session(running_entry, down_stop_reason(reason))

        session_id = running_entry_session_id(running_entry)

        state =
          case reason do
            :normal ->
              Logger.info("Agent task completed for issue_id=#{issue_id} session_id=#{session_id}; scheduling active-state continuation check")

              state
              |> persist_run_record(running_entry, %{
                final_status: "completed",
                next_action: "continuation",
                stop_reason: down_stop_reason(reason)
              })
              |> complete_issue(issue_id)
              |> schedule_issue_retry(issue_id, 1, %{
                identifier: running_entry.identifier,
                delay_type: :continuation
              })

            _ ->
              Logger.warning("Agent task exited for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}; scheduling retry")

              next_attempt = next_retry_attempt_from_running(running_entry)

              state
              |> persist_run_record(running_entry, %{
                final_status: "failed",
                next_action: "retry",
                stop_reason: down_stop_reason(reason),
                failure_summary: "agent exited: #{inspect(reason)}"
              })
              |> schedule_issue_retry(issue_id, next_attempt, %{
                identifier: running_entry.identifier,
                error: "agent exited: #{inspect(reason)}"
              })
          end

        Logger.info("Agent task finished for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}")

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info(
        {:codex_worker_update, issue_id, %{event: _, timestamp: _} = update},
        %{running: running} = state
      ) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        {updated_running_entry, token_delta} = integrate_codex_update(running_entry, update)

        state =
          state
          |> apply_codex_token_delta(token_delta)
          |> apply_codex_rate_limits(update)
          |> maybe_suppress_spawned_child_dispatch(issue_id, update)
          |> append_timeline_event(updated_running_entry, update)

        notify_dashboard()
        {:noreply, %{state | running: Map.put(running, issue_id, updated_running_entry)}}
    end
  end

  def handle_info({:codex_worker_update, _issue_id, _update}, state), do: {:noreply, state}

  def handle_info({:retry_issue, issue_id}, state) do
    result =
      case pop_retry_attempt_state(state, issue_id) do
        {:ok, attempt, metadata, state} -> handle_retry_issue(state, issue_id, attempt, metadata)
        :missing -> {:noreply, state}
      end

    notify_dashboard()
    result
  end

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp maybe_dispatch(%State{} = state) do
    state = reconcile_running_issues(state)
    state = reconcile_suppressed_dispatch(state)

    with :ok <- Config.validate!(),
         {:ok, issues} <- Tracker.fetch_candidate_issues(),
         true <- available_slots(state) > 0 do
      choose_issues(issues, state)
    else
      {:error, :missing_linear_api_token} ->
        Logger.error("Linear API token missing in WORKFLOW.md")
        state

      {:error, :missing_linear_project_slug} ->
        Logger.error("Linear project slug missing in WORKFLOW.md")
        state

      {:error, :missing_tracker_kind} ->
        Logger.error("Tracker kind missing in WORKFLOW.md")

        state

      {:error, {:unsupported_tracker_kind, kind}} ->
        Logger.error("Unsupported tracker kind in WORKFLOW.md: #{inspect(kind)}")

        state

      {:error, :missing_codex_command} ->
        Logger.error("Codex command missing in WORKFLOW.md")
        state

      {:error, {:invalid_codex_approval_policy, value}} ->
        Logger.error("Invalid codex.approval_policy in WORKFLOW.md: #{inspect(value)}")
        state

      {:error, {:invalid_codex_thread_sandbox, value}} ->
        Logger.error("Invalid codex.thread_sandbox in WORKFLOW.md: #{inspect(value)}")
        state

      {:error, {:invalid_codex_turn_sandbox_policy, reason}} ->
        Logger.error("Invalid codex.turn_sandbox_policy in WORKFLOW.md: #{inspect(reason)}")
        state

      {:error, {:missing_workflow_file, path, reason}} ->
        Logger.error("Missing WORKFLOW.md at #{path}: #{inspect(reason)}")
        state

      {:error, :workflow_front_matter_not_a_map} ->
        Logger.error("Failed to parse WORKFLOW.md: workflow front matter must decode to a map")
        state

      {:error, {:workflow_parse_error, reason}} ->
        Logger.error("Failed to parse WORKFLOW.md: #{inspect(reason)}")
        state

      {:error, reason} ->
        Logger.error("Failed to fetch from Linear: #{inspect(reason)}")
        state

      false ->
        state
    end
  end

  defp reconcile_running_issues(%State{} = state) do
    state = reconcile_stalled_running_issues(state)
    running_ids = Map.keys(state.running)

    if running_ids == [] do
      state
    else
      case Tracker.fetch_issue_states_by_ids(running_ids) do
        {:ok, issues} ->
          reconcile_running_issue_states(
            issues,
            state,
            active_state_set(),
            terminal_state_set()
          )

        {:error, reason} ->
          Logger.debug("Failed to refresh running issue states: #{inspect(reason)}; keeping active workers")

          state
      end
    end
  end

  @doc false
  @spec reconcile_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  def reconcile_issue_states_for_test(issues, state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec reconcile_suppressed_dispatch_for_test(term()) :: term()
  def reconcile_suppressed_dispatch_for_test(%State{} = state) do
    reconcile_suppressed_dispatch(state)
  end

  def reconcile_suppressed_dispatch_for_test(state), do: state

  @doc false
  @spec should_dispatch_issue_for_test(Issue.t(), term()) :: boolean()
  def should_dispatch_issue_for_test(%Issue{} = issue, %State{} = state) do
    should_dispatch_issue?(issue, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec retry_candidate_issue_for_test(Issue.t(), term()) :: boolean()
  def retry_candidate_issue_for_test(%Issue{} = issue, %State{} = state) do
    retry_candidate_issue?(issue, state, terminal_state_set())
  end

  @doc false
  @spec revalidate_issue_for_dispatch_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:ok, Issue.t()} | {:skip, Issue.t() | :missing} | {:error, term()}
  def revalidate_issue_for_dispatch_for_test(%Issue{} = issue, issue_fetcher)
      when is_function(issue_fetcher, 1) do
    revalidate_issue_for_dispatch(issue, issue_fetcher, terminal_state_set())
  end

  @doc false
  @spec sort_issues_for_dispatch_for_test([Issue.t()]) :: [Issue.t()]
  def sort_issues_for_dispatch_for_test(issues) when is_list(issues) do
    sort_issues_for_dispatch(issues)
  end

  defp reconcile_suppressed_dispatch(%State{suppressed_dispatch: suppressed_dispatch} = state)
       when map_size(suppressed_dispatch) == 0,
       do: state

  defp reconcile_suppressed_dispatch(%State{suppressed_dispatch: suppressed_dispatch} = state) do
    issue_ids = Map.keys(suppressed_dispatch)

    case Tracker.fetch_issue_states_by_ids(issue_ids) do
      {:ok, issues} ->
        states_by_id =
          Enum.reduce(issues, %{}, fn
            %Issue{id: id} = issue, acc when is_binary(id) -> Map.put(acc, id, issue)
            _issue, acc -> acc
          end)

        next_suppressed_dispatch =
          Enum.reduce(suppressed_dispatch, %{}, fn {issue_id, metadata}, acc ->
            case Map.get(states_by_id, issue_id) do
              %Issue{state: state_name} when is_binary(state_name) ->
                if normalize_issue_state(state_name) == "todo" do
                  Map.put(acc, issue_id, metadata)
                else
                  acc
                end

              _ ->
                acc
            end
          end)

        %{state | suppressed_dispatch: next_suppressed_dispatch}

      {:error, reason} ->
        Logger.debug("Failed to refresh suppressed issue states: #{inspect(reason)}; keeping suppressed dispatch entries")
        state
    end
  end

  defp reconcile_running_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_running_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_running_issue_states(
      rest,
      reconcile_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, true, "terminal_state")

      !issue_routable_to_worker?(issue) ->
        Logger.info("Issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; stopping active agent")

        terminate_running_issue(state, issue.id, false, "no_longer_routed")

      active_issue_state?(issue.state, active_states) ->
        refresh_running_issue_state(state, issue)

      true ->
        Logger.info("Issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        terminate_running_issue(state, issue.id, false, "non_active_state")
    end
  end

  defp reconcile_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp refresh_running_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.running, issue.id) do
      %{issue: _} = running_entry ->
        %{state | running: Map.put(state.running, issue.id, %{running_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp terminate_running_issue(%State{} = state, issue_id, cleanup_workspace, stop_reason, opts \\ []) do
    case Map.get(state.running, issue_id) do
      nil ->
        release_issue_claim(state, issue_id)

      %{pid: pid, ref: ref, identifier: identifier} = running_entry ->
        state =
          state
          |> record_session_completion_totals(running_entry)
          |> record_inactive_session(running_entry, stop_reason)
          |> persist_run_record(running_entry, %{
            final_status: Keyword.get(opts, :final_status, "stopped"),
            next_action: Keyword.get(opts, :next_action, "none"),
            stop_reason: stop_reason,
            failure_summary: Keyword.get(opts, :failure_summary)
          })

        if cleanup_workspace do
          cleanup_issue_workspace(identifier)
        end

        if is_pid(pid) do
          terminate_task(pid)
        end

        if is_reference(ref) do
          Process.demonitor(ref, [:flush])
        end

        %{
          state
          | running: Map.delete(state.running, issue_id),
            claimed: MapSet.delete(state.claimed, issue_id),
            retry_attempts: Map.delete(state.retry_attempts, issue_id)
        }
        |> prune_timeline_events()

      _ ->
        release_issue_claim(state, issue_id)
    end
  end

  defp reconcile_stalled_running_issues(%State{} = state) do
    timeout_ms = Config.codex_stall_timeout_ms()

    cond do
      timeout_ms <= 0 ->
        state

      map_size(state.running) == 0 ->
        state

      true ->
        now = DateTime.utc_now()

        Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
          restart_stalled_issue(state_acc, issue_id, running_entry, now, timeout_ms)
        end)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, timeout_ms) do
    elapsed_ms = stall_elapsed_ms(running_entry, now)

    if is_integer(elapsed_ms) and elapsed_ms > timeout_ms do
      identifier = Map.get(running_entry, :identifier, issue_id)
      session_id = running_entry_session_id(running_entry)

      Logger.warning("Issue stalled: issue_id=#{issue_id} issue_identifier=#{identifier} session_id=#{session_id} elapsed_ms=#{elapsed_ms}; restarting with backoff")

      next_attempt = next_retry_attempt_from_running(running_entry)

      state
      |> terminate_running_issue(issue_id, false, "stalled_restart",
        final_status: "failed",
        next_action: "retry",
        failure_summary: "stalled for #{elapsed_ms}ms without codex activity"
      )
      |> schedule_issue_retry(issue_id, next_attempt, %{
        identifier: identifier,
        error: "stalled for #{elapsed_ms}ms without codex activity"
      })
    else
      state
    end
  end

  defp stall_elapsed_ms(running_entry, now) do
    running_entry
    |> last_activity_timestamp()
    |> case do
      %DateTime{} = timestamp ->
        max(0, DateTime.diff(now, timestamp, :millisecond))

      _ ->
        nil
    end
  end

  defp last_activity_timestamp(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_timestamp) || Map.get(running_entry, :started_at)
  end

  defp last_activity_timestamp(_running_entry), do: nil

  defp terminate_task(pid) when is_pid(pid) do
    case Task.Supervisor.terminate_child(SymphonyElixir.TaskSupervisor, pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Process.exit(pid, :shutdown)
    end
  end

  defp terminate_task(_pid), do: :ok

  defp choose_issues(issues, state) do
    active_states = active_state_set()
    terminal_states = terminal_state_set()

    issues
    |> sort_issues_for_dispatch()
    |> Enum.reduce(state, fn issue, state_acc ->
      if should_dispatch_issue?(issue, state_acc, active_states, terminal_states) do
        dispatch_issue(state_acc, issue)
      else
        state_acc
      end
    end)
  end

  defp sort_issues_for_dispatch(issues) when is_list(issues) do
    Enum.sort_by(issues, fn
      %Issue{} = issue ->
        {priority_rank(issue.priority), issue_created_at_sort_key(issue), issue.identifier || issue.id || ""}

      _ ->
        {priority_rank(nil), issue_created_at_sort_key(nil), ""}
    end)
  end

  defp priority_rank(priority) when is_integer(priority) and priority in 1..4, do: priority
  defp priority_rank(_priority), do: 5

  defp issue_created_at_sort_key(%Issue{created_at: %DateTime{} = created_at}) do
    DateTime.to_unix(created_at, :microsecond)
  end

  defp issue_created_at_sort_key(%Issue{}), do: 9_223_372_036_854_775_807
  defp issue_created_at_sort_key(_issue), do: 9_223_372_036_854_775_807

  defp should_dispatch_issue?(
         %Issue{} = issue,
         %State{running: running, claimed: claimed, suppressed_dispatch: suppressed_dispatch} = state,
         active_states,
         terminal_states
       ) do
    candidate_issue?(issue, active_states, terminal_states) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states) and
      !MapSet.member?(claimed, issue.id) and
      !Map.has_key?(suppressed_dispatch, issue.id) and
      !Map.has_key?(running, issue.id) and
      available_slots(state) > 0 and
      state_slots_available?(issue, running)
  end

  defp should_dispatch_issue?(_issue, _state, _active_states, _terminal_states), do: false

  defp state_slots_available?(%Issue{state: issue_state}, running) when is_map(running) do
    limit = Config.max_concurrent_agents_for_state(issue_state)
    used = running_issue_count_for_state(running, issue_state)
    limit > used
  end

  defp state_slots_available?(_issue, _running), do: false

  defp running_issue_count_for_state(running, issue_state) when is_map(running) do
    normalized_state = normalize_issue_state(issue_state)

    Enum.count(running, fn
      {_id, %{issue: %Issue{state: state_name}}} ->
        normalize_issue_state(state_name) == normalized_state

      _ ->
        false
    end)
  end

  defp candidate_issue?(
         %Issue{
           id: id,
           identifier: identifier,
           title: title,
           state: state_name
         } = issue,
         active_states,
         terminal_states
       )
       when is_binary(id) and is_binary(identifier) and is_binary(title) and is_binary(state_name) do
    issue_routable_to_worker?(issue) and
      active_issue_state?(state_name, active_states) and
      !terminal_issue_state?(state_name, terminal_states)
  end

  defp candidate_issue?(_issue, _active_states, _terminal_states), do: false

  defp issue_routable_to_worker?(%Issue{assigned_to_worker: assigned_to_worker})
       when is_boolean(assigned_to_worker),
       do: assigned_to_worker

  defp issue_routable_to_worker?(_issue), do: true

  defp todo_issue_blocked_by_non_terminal?(
         %Issue{state: issue_state, blocked_by: blockers},
         terminal_states
       )
       when is_binary(issue_state) and is_list(blockers) do
    normalize_issue_state(issue_state) == "todo" and
      Enum.any?(blockers, fn
        %{state: blocker_state} when is_binary(blocker_state) ->
          !terminal_issue_state?(blocker_state, terminal_states)

        _ ->
          true
      end)
  end

  defp todo_issue_blocked_by_non_terminal?(_issue, _terminal_states), do: false

  defp terminal_issue_state?(state_name, terminal_states) when is_binary(state_name) do
    MapSet.member?(terminal_states, normalize_issue_state(state_name))
  end

  defp terminal_issue_state?(_state_name, _terminal_states), do: false

  defp active_issue_state?(state_name, active_states) when is_binary(state_name) do
    MapSet.member?(active_states, normalize_issue_state(state_name))
  end

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    String.downcase(String.trim(state_name))
  end

  defp terminal_state_set do
    Config.linear_terminal_states()
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp active_state_set do
    Config.linear_active_states()
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp dispatch_issue(%State{} = state, issue, attempt \\ nil) do
    case revalidate_issue_for_dispatch(issue, &Tracker.fetch_issue_states_by_ids/1, terminal_state_set()) do
      {:ok, %Issue{} = refreshed_issue} ->
        do_dispatch_issue(state, refreshed_issue, attempt)

      {:skip, :missing} ->
        Logger.info("Skipping dispatch; issue no longer active or visible: #{issue_context(issue)}")
        state

      {:skip, %Issue{} = refreshed_issue} ->
        Logger.info("Skipping stale dispatch after issue refresh: #{issue_context(refreshed_issue)} state=#{inspect(refreshed_issue.state)} blocked_by=#{length(refreshed_issue.blocked_by)}")

        state

      {:error, reason} ->
        Logger.warning("Skipping dispatch; issue refresh failed for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp do_dispatch_issue(%State{} = state, issue, attempt) do
    recipient = self()
    event_stream_id = build_event_stream_id(issue)

    case Task.Supervisor.start_child(SymphonyElixir.TaskSupervisor, fn ->
           AgentRunner.run(issue, recipient, attempt: attempt)
         end) do
      {:ok, pid} ->
        ref = Process.monitor(pid)

        Logger.info("Dispatching issue to agent: #{issue_context(issue)} pid=#{inspect(pid)} attempt=#{inspect(attempt)}")

        running =
          Map.put(state.running, issue.id, %{
            pid: pid,
            ref: ref,
            identifier: issue.identifier,
            issue: issue,
            initial_tracker_state: issue.state,
            workflow_path: Workflow.workflow_file_path(),
            event_stream_id: event_stream_id,
            session_id: nil,
            last_codex_message: nil,
            last_codex_timestamp: nil,
            last_codex_event: nil,
            codex_app_server_pid: nil,
            codex_input_tokens: 0,
            codex_output_tokens: 0,
            codex_total_tokens: 0,
            codex_last_reported_input_tokens: 0,
            codex_last_reported_output_tokens: 0,
            codex_last_reported_total_tokens: 0,
            turn_count: 0,
            retry_attempt: normalize_retry_attempt(attempt),
            started_at: DateTime.utc_now()
          })

        %{
          state
          | running: running,
            timeline_events: Map.put_new(state.timeline_events, event_stream_id, []),
            claimed: MapSet.put(state.claimed, issue.id),
            retry_attempts: Map.delete(state.retry_attempts, issue.id)
        }

      {:error, reason} ->
        Logger.error("Unable to spawn agent for #{issue_context(issue)}: #{inspect(reason)}")
        next_attempt = if is_integer(attempt), do: attempt + 1, else: nil

        schedule_issue_retry(state, issue.id, next_attempt, %{
          identifier: issue.identifier,
          error: "failed to spawn agent: #{inspect(reason)}"
        })
    end
  end

  defp revalidate_issue_for_dispatch(%Issue{id: issue_id}, issue_fetcher, terminal_states)
       when is_binary(issue_id) and is_function(issue_fetcher, 1) do
    case issue_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if retry_candidate_issue?(refreshed_issue, terminal_states) do
          {:ok, refreshed_issue}
        else
          {:skip, refreshed_issue}
        end

      {:ok, []} ->
        {:skip, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp revalidate_issue_for_dispatch(issue, _issue_fetcher, _terminal_states), do: {:ok, issue}

  defp complete_issue(%State{} = state, issue_id) do
    %{
      state
      | completed: MapSet.put(state.completed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp schedule_issue_retry(%State{} = state, issue_id, attempt, metadata)
       when is_binary(issue_id) and is_map(metadata) do
    previous_retry = Map.get(state.retry_attempts, issue_id, %{attempt: 0})
    next_attempt = if is_integer(attempt), do: attempt, else: previous_retry.attempt + 1
    delay_ms = retry_delay(next_attempt, metadata)
    old_timer = Map.get(previous_retry, :timer_ref)
    due_at_ms = System.monotonic_time(:millisecond) + delay_ms
    identifier = pick_retry_identifier(issue_id, previous_retry, metadata)
    error = pick_retry_error(previous_retry, metadata)

    if is_reference(old_timer) do
      Process.cancel_timer(old_timer)
    end

    timer_ref = Process.send_after(self(), {:retry_issue, issue_id}, delay_ms)

    error_suffix = if is_binary(error), do: " error=#{error}", else: ""

    Logger.warning("Retrying issue_id=#{issue_id} issue_identifier=#{identifier} in #{delay_ms}ms (attempt #{next_attempt})#{error_suffix}")

    %{
      state
      | retry_attempts:
          Map.put(state.retry_attempts, issue_id, %{
            attempt: next_attempt,
            timer_ref: timer_ref,
            due_at_ms: due_at_ms,
            identifier: identifier,
            error: error
          })
    }
  end

  defp pop_retry_attempt_state(%State{} = state, issue_id) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          error: Map.get(retry_entry, :error)
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}}

      _ ->
        :missing
    end
  end

  defp handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    case Tracker.fetch_candidate_issues() do
      {:ok, issues} ->
        issues
        |> find_issue_by_id(issue_id)
        |> handle_retry_issue_lookup(state, issue_id, attempt, metadata)

      {:error, reason} ->
        Logger.warning("Retry poll failed for issue_id=#{issue_id} issue_identifier=#{metadata[:identifier] || issue_id}: #{inspect(reason)}")

        {:noreply,
         schedule_issue_retry(
           state,
           issue_id,
           attempt + 1,
           Map.merge(metadata, %{error: "retry poll failed: #{inspect(reason)}"})
         )}
    end
  end

  defp handle_retry_issue_lookup(%Issue{} = issue, state, issue_id, attempt, metadata) do
    terminal_states = terminal_state_set()

    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue state is terminal: issue_id=#{issue_id} issue_identifier=#{issue.identifier} state=#{issue.state}; removing associated workspace")

        cleanup_issue_workspace(issue.identifier)
        {:noreply, release_issue_claim(state, issue_id)}

      retry_candidate_issue?(issue, state, terminal_states) ->
        handle_active_retry(state, issue, attempt, metadata)

      true ->
        Logger.debug("Issue left active states, removing claim issue_id=#{issue_id} issue_identifier=#{issue.identifier}")

        {:noreply, release_issue_claim(state, issue_id)}
    end
  end

  defp handle_retry_issue_lookup(nil, state, issue_id, _attempt, _metadata) do
    Logger.debug("Issue no longer visible, removing claim issue_id=#{issue_id}")
    {:noreply, release_issue_claim(state, issue_id)}
  end

  defp cleanup_issue_workspace(identifier) when is_binary(identifier) do
    Workspace.remove_issue_workspaces(identifier)
  end

  defp cleanup_issue_workspace(_identifier), do: :ok

  defp run_terminal_workspace_cleanup do
    case Tracker.fetch_issues_by_states(Config.linear_terminal_states()) do
      {:ok, issues} ->
        issues
        |> Enum.each(fn
          %Issue{identifier: identifier} when is_binary(identifier) ->
            cleanup_issue_workspace(identifier)

          _ ->
            :ok
        end)

      {:error, reason} ->
        Logger.warning("Skipping startup terminal workspace cleanup; failed to fetch terminal issues: #{inspect(reason)}")
    end
  end

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(state, issue, attempt, metadata) do
    if retry_candidate_issue?(issue, state, terminal_state_set()) and
         dispatch_slots_available?(issue, state) do
      {:noreply, dispatch_issue(state, issue, attempt)}
    else
      Logger.debug("No available slots for retrying #{issue_context(issue)}; retrying again")

      {:noreply,
       schedule_issue_retry(
         state,
         issue.id,
         attempt + 1,
         Map.merge(metadata, %{
           identifier: issue.identifier,
           error: "no available orchestrator slots"
         })
       )}
    end
  end

  defp release_issue_claim(%State{} = state, issue_id) do
    %{state | claimed: MapSet.delete(state.claimed, issue_id)}
  end

  defp retry_delay(attempt, metadata) when is_integer(attempt) and attempt > 0 and is_map(metadata) do
    if metadata[:delay_type] == :continuation and attempt == 1 do
      @continuation_retry_delay_ms
    else
      failure_retry_delay(attempt)
    end
  end

  defp failure_retry_delay(attempt) do
    max_delay_power = min(attempt - 1, 10)
    min(@failure_retry_base_ms * (1 <<< max_delay_power), Config.max_retry_backoff_ms())
  end

  defp normalize_retry_attempt(attempt) when is_integer(attempt) and attempt > 0, do: attempt
  defp normalize_retry_attempt(_attempt), do: 0

  defp next_retry_attempt_from_running(running_entry) do
    case Map.get(running_entry, :retry_attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt + 1
      _ -> nil
    end
  end

  defp pick_retry_identifier(issue_id, previous_retry, metadata) do
    metadata[:identifier] || Map.get(previous_retry, :identifier) || issue_id
  end

  defp pick_retry_error(previous_retry, metadata) do
    metadata[:error] || Map.get(previous_retry, :error)
  end

  defp find_issue_by_id(issues, issue_id) when is_binary(issue_id) do
    Enum.find(issues, fn
      %Issue{id: ^issue_id} ->
        true

      _ ->
        false
    end)
  end

  defp find_issue_id_for_ref(running, ref) do
    running
    |> Enum.find_value(fn {issue_id, %{ref: running_ref}} ->
      if running_ref == ref, do: issue_id
    end)
  end

  defp running_entry_session_id(%{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp running_entry_session_id(_running_entry), do: "n/a"

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp available_slots(%State{} = state) do
    max(
      (state.max_concurrent_agents || Config.max_concurrent_agents()) - map_size(state.running),
      0
    )
  end

  @spec request_refresh() :: map() | :unavailable
  def request_refresh do
    request_refresh(__MODULE__)
  end

  @spec request_refresh(GenServer.server()) :: map() | :unavailable
  def request_refresh(server) do
    if Process.whereis(server) do
      GenServer.call(server, :request_refresh)
    else
      :unavailable
    end
  end

  @spec snapshot() :: map() | :timeout | :unavailable
  def snapshot, do: snapshot(__MODULE__, 15_000)

  @spec snapshot(GenServer.server(), timeout()) :: map() | :timeout | :unavailable
  def snapshot(server, timeout) do
    if Process.whereis(server) do
      try do
        GenServer.call(server, :snapshot, timeout)
      catch
        :exit, {:timeout, _} -> :timeout
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @spec session_events(String.t(), pos_integer()) ::
          {:ok, [map()]} | {:error, :session_not_found} | :timeout | :unavailable
  def session_events(event_stream_id, limit \\ 200) when is_binary(event_stream_id) do
    session_events(__MODULE__, event_stream_id, limit, 15_000)
  end

  @spec session_events(GenServer.server(), String.t(), pos_integer(), timeout()) ::
          {:ok, [map()]} | {:error, :session_not_found} | :timeout | :unavailable
  def session_events(server, event_stream_id, limit, timeout)
      when is_binary(event_stream_id) and is_integer(limit) and limit > 0 do
    if Process.whereis(server) do
      try do
        GenServer.call(server, {:session_events, event_stream_id, normalize_session_event_limit(limit)}, timeout)
      catch
        :exit, {:timeout, _} -> :timeout
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    state = refresh_runtime_config(state)
    now = DateTime.utc_now()
    now_ms = System.monotonic_time(:millisecond)

    running =
      state.running
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: metadata.identifier,
          state: metadata.issue.state,
          event_stream_id: Map.get(metadata, :event_stream_id),
          session_id: metadata.session_id,
          codex_app_server_pid: metadata.codex_app_server_pid,
          codex_input_tokens: metadata.codex_input_tokens,
          codex_output_tokens: metadata.codex_output_tokens,
          codex_total_tokens: metadata.codex_total_tokens,
          turn_count: Map.get(metadata, :turn_count, 0),
          started_at: metadata.started_at,
          last_codex_timestamp: metadata.last_codex_timestamp,
          last_codex_message: metadata.last_codex_message,
          last_codex_event: metadata.last_codex_event,
          runtime_seconds: running_seconds(metadata.started_at, now)
        }
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt, due_at_ms: due_at_ms} = retry} ->
        %{
          issue_id: issue_id,
          attempt: attempt,
          due_in_ms: max(0, due_at_ms - now_ms),
          identifier: Map.get(retry, :identifier),
          error: Map.get(retry, :error)
        }
      end)

    {:reply,
     %{
       running: running,
       inactive_sessions: state.inactive_sessions,
       retrying: retrying,
       codex_totals: state.codex_totals,
       rate_limits: Map.get(state, :codex_rate_limits),
       polling: %{
         checking?: state.poll_check_in_progress == true,
         next_poll_in_ms: next_poll_in_ms(state.next_poll_due_at_ms, now_ms),
         poll_interval_ms: state.poll_interval_ms
       }
     }, state}
  end

  def handle_call(:request_refresh, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    already_due? = is_integer(state.next_poll_due_at_ms) and state.next_poll_due_at_ms <= now_ms
    coalesced = state.poll_check_in_progress == true or already_due?

    unless coalesced do
      :ok = schedule_tick(0)
    end

    {:reply,
     %{
       queued: true,
       coalesced: coalesced,
       requested_at: DateTime.utc_now(),
       operations: ["poll", "reconcile"]
     }, state}
  end

  def handle_call({:session_events, event_stream_id, limit}, _from, state) do
    case session_events_from_state(state, event_stream_id, limit) do
      {:ok, events} -> {:reply, {:ok, events}, state}
      {:error, :session_not_found} -> {:reply, {:error, :session_not_found}, state}
    end
  end

  defp integrate_codex_update(running_entry, %{event: event, timestamp: timestamp} = update) do
    token_delta = extract_token_delta(running_entry, update)
    codex_input_tokens = Map.get(running_entry, :codex_input_tokens, 0)
    codex_output_tokens = Map.get(running_entry, :codex_output_tokens, 0)
    codex_total_tokens = Map.get(running_entry, :codex_total_tokens, 0)
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
    last_reported_input = Map.get(running_entry, :codex_last_reported_input_tokens, 0)
    last_reported_output = Map.get(running_entry, :codex_last_reported_output_tokens, 0)
    last_reported_total = Map.get(running_entry, :codex_last_reported_total_tokens, 0)
    turn_count = Map.get(running_entry, :turn_count, 0)

    {
      Map.merge(running_entry, %{
        last_codex_timestamp: timestamp,
        last_codex_message: summarize_codex_update(update),
        session_id: session_id_for_update(running_entry.session_id, update),
        last_codex_event: event,
        codex_app_server_pid: codex_app_server_pid_for_update(codex_app_server_pid, update),
        codex_input_tokens: codex_input_tokens + token_delta.input_tokens,
        codex_output_tokens: codex_output_tokens + token_delta.output_tokens,
        codex_total_tokens: codex_total_tokens + token_delta.total_tokens,
        codex_last_reported_input_tokens: max(last_reported_input, token_delta.input_reported),
        codex_last_reported_output_tokens: max(last_reported_output, token_delta.output_reported),
        codex_last_reported_total_tokens: max(last_reported_total, token_delta.total_reported),
        turn_count: turn_count_for_update(turn_count, running_entry.session_id, update)
      }),
      token_delta
    }
  end

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_binary(pid),
       do: pid

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_integer(pid),
       do: Integer.to_string(pid)

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid}) when is_list(pid),
    do: to_string(pid)

  defp codex_app_server_pid_for_update(existing, _update), do: existing

  defp session_id_for_update(_existing, %{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp session_id_for_update(existing, _update), do: existing

  defp turn_count_for_update(existing_count, existing_session_id, %{
         event: :session_started,
         session_id: session_id
       })
       when is_integer(existing_count) and is_binary(session_id) do
    if session_id == existing_session_id do
      existing_count
    else
      existing_count + 1
    end
  end

  defp turn_count_for_update(existing_count, _existing_session_id, _update)
       when is_integer(existing_count),
       do: existing_count

  defp turn_count_for_update(_existing_count, _existing_session_id, _update), do: 0

  defp summarize_codex_update(update) do
    %{
      event: update[:event],
      message: update[:payload] || update[:raw],
      timestamp: update[:timestamp]
    }
  end

  defp append_timeline_event(%State{} = state, running_entry, %{event: _, timestamp: _} = update)
       when is_map(running_entry) do
    case Map.get(running_entry, :event_stream_id) do
      stream_id when is_binary(stream_id) ->
        event = timeline_event_payload(running_entry, update)

        state =
          Map.update(state, :timeline_events, %{stream_id => [event]}, fn existing ->
            Map.update(existing, stream_id, [event], fn events ->
              (events ++ [event]) |> Enum.take(-@timeline_events_limit)
            end)
          end)

        case SessionTimelineStore.append(stream_id, event) do
          :ok ->
            state

          {:error, reason} ->
            Logger.warning("Failed to append session timeline event stream=#{stream_id} issue_identifier=#{Map.get(running_entry, :identifier)}: #{inspect(reason)}")

            state
        end

      _ ->
        state
    end
  end

  defp append_timeline_event(state, _running_entry, _update), do: state

  defp timeline_event_payload(running_entry, update) do
    event_name = update[:event] |> to_string()
    classification = classify_human_timeline_event(update)

    message =
      update
      |> summarize_codex_update()
      |> StatusDashboard.humanize_codex_message()
      |> sanitize_timeline_message()

    %{
      at: timeline_timestamp(update[:timestamp]),
      issue_identifier: Map.get(running_entry, :identifier),
      session_id: Map.get(running_entry, :session_id),
      turn_count: Map.get(running_entry, :turn_count, 0),
      event: event_name,
      label: String.replace(event_name, "_", " "),
      message: message
    }
    |> maybe_put_timeline_field(:kind, Map.get(classification, :kind))
    |> maybe_put_timeline_field(:category, Map.get(classification, :category))
    |> maybe_put_timeline_field(:action, Map.get(classification, :action))
    |> maybe_put_timeline_field(:details, sanitize_timeline_details(Map.get(classification, :details)))
  end

  defp timeline_timestamp(%DateTime{} = timestamp),
    do: timestamp |> DateTime.truncate(:second) |> DateTime.to_iso8601()

  defp timeline_timestamp(_timestamp), do: DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

  defp sanitize_timeline_message(message) when is_binary(message) do
    message
    |> String.trim()
    |> redact_sensitive_tokens()
    |> String.slice(0, 300)
  end

  defp sanitize_timeline_message(message), do: message |> to_string() |> sanitize_timeline_message()

  defp redact_sensitive_tokens(message) when is_binary(message) do
    [
      ~r/lin_api_[A-Za-z0-9]+/,
      ~r/sk-[A-Za-z0-9_-]{16,}/,
      ~r/(ghp_[A-Za-z0-9]{12,}|github_pat_[A-Za-z0-9_]{20,})/,
      ~r/(xox[baprs]-[A-Za-z0-9-]{10,})/,
      ~r/((api[_-]?key|authorization|token|password|secret)\s*[:=]\s*)[^\s,;]+/i
    ]
    |> Enum.reduce(message, fn pattern, acc ->
      Regex.replace(pattern, acc, "[REDACTED]")
    end)
  end

  defp classify_human_timeline_event(update) when is_map(update) do
    payload = codex_update_payload(update)
    event = Map.get(update, :event)
    method = codex_method(payload)
    linear_tool_event = classify_linear_tool_call(payload)

    cond do
      event == :malformed ->
        %{
          kind: "stream_error",
          category: "codex",
          action: "malformed_event",
          details: Map.get(update, :details)
        }

      event == :tool_call_completed and map_size(linear_tool_event) > 0 ->
        linear_tool_event

      method in ["codex/event/exec_command_begin", "item/commandExecution/requestApproval"] ->
        classify_command_event(payload)

      method == "thread/status/changed" ->
        classify_thread_status_change(payload)

      true ->
        %{}
    end
  end

  defp classify_human_timeline_event(_update), do: %{}

  defp codex_update_payload(%{} = update) do
    case Map.get(update, :payload) do
      %{} = payload ->
        payload

      _ ->
        case Map.get(update, :raw) do
          raw when is_binary(raw) ->
            case Jason.decode(raw) do
              {:ok, %{} = payload} -> payload
              _ -> %{}
            end

          _ ->
            %{}
        end
    end
  end

  defp codex_method(payload) when is_map(payload) do
    map_lookup_any(payload, [["method"], [:method]])
  end

  defp codex_method(_payload), do: nil

  defp codex_update_result(%{} = update), do: Map.get(update, :result)
  defp codex_update_result(_update), do: nil

  defp classify_linear_tool_call(payload) when is_map(payload) do
    with {:ok, "linear_graphql", normalized_args} <- extract_linear_tool_call(payload),
         query when is_binary(query) <- map_lookup_any(normalized_args, [["query"], [:query]]) do
      case linear_operation_from_query(query) do
        :issue_create ->
          %{
            kind: "human_action",
            category: "linear",
            action: "linear_issue_create",
            details:
              %{
                operation: "issueCreate",
                team_id: extract_linear_team_id(normalized_args)
              }
              |> compact_timeline_details()
          }

        :comment_create ->
          %{
            kind: "human_action",
            category: "linear",
            action: "linear_comment_create",
            details:
              %{
                operation: "commentCreate",
                issue_id: extract_linear_issue_id(normalized_args)
              }
              |> compact_timeline_details()
          }

        :issue_update ->
          %{
            kind: "human_action",
            category: "linear",
            action: "linear_status_change",
            details:
              %{
                operation: "issueUpdate",
                issue_id: extract_linear_issue_id(normalized_args),
                state_id: extract_linear_state_id(normalized_args)
              }
              |> compact_timeline_details()
          }

        :unknown ->
          %{}
      end
    else
      _ -> %{}
    end
  end

  defp classify_linear_tool_call(_payload), do: %{}

  defp extract_linear_tool_call(payload) when is_map(payload) do
    params = map_lookup_any(payload, [["params"], [:params]])

    tool_name =
      first_present([
        map_lookup_any(params || %{}, [["tool"], [:tool], ["name"], [:name]]),
        map_lookup_any(payload, [["tool"], [:tool], ["name"], [:name]])
      ])

    arguments =
      first_present([
        map_lookup_any(params || %{}, [["arguments"], [:arguments]]),
        map_lookup_any(payload, [["arguments"], [:arguments]])
      ])

    with tool_name when is_binary(tool_name) <- normalize_string(tool_name),
         %{} = normalized_args <- normalize_linear_tool_arguments(arguments) do
      {:ok, tool_name, normalized_args}
    else
      _ -> :error
    end
  end

  defp extract_linear_tool_call(_payload), do: :error

  defp normalize_linear_tool_arguments(arguments) when is_map(arguments), do: arguments

  defp normalize_linear_tool_arguments(arguments) when is_binary(arguments) do
    case Jason.decode(arguments) do
      {:ok, %{} = decoded} -> decoded
      _ -> %{}
    end
  end

  defp normalize_linear_tool_arguments(_arguments), do: %{}

  defp linear_operation_from_query(query) when is_binary(query) do
    cond do
      Regex.match?(~r/\bissueCreate\b/, query) -> :issue_create
      Regex.match?(~r/\bcommentCreate\b/, query) -> :comment_create
      Regex.match?(~r/\bissueUpdate\b/, query) -> :issue_update
      true -> :unknown
    end
  end

  defp linear_operation_from_query(_query), do: :unknown

  defp extract_linear_issue_id(arguments) when is_map(arguments) do
    variables =
      map_lookup_any(arguments, [["variables"], [:variables]])

    input =
      map_lookup_any(variables || %{}, [["input"], [:input]])

    first_present([
      map_lookup_any(variables || %{}, [["issueId"], [:issueId]]),
      map_lookup_any(variables || %{}, [["issue_id"], [:issue_id]]),
      map_lookup_any(variables || %{}, [["id"], [:id]]),
      map_lookup_any(input || %{}, [["issueId"], [:issueId]]),
      map_lookup_any(input || %{}, [["id"], [:id]]),
      map_lookup_any(arguments, [["issueId"], [:issueId]]),
      map_lookup_any(arguments, [["id"], [:id]])
    ])
  end

  defp extract_linear_issue_id(_arguments), do: nil

  defp extract_linear_team_id(arguments) when is_map(arguments) do
    variables =
      map_lookup_any(arguments, [["variables"], [:variables]])

    input =
      map_lookup_any(variables || %{}, [["input"], [:input]])

    first_present([
      map_lookup_any(variables || %{}, [["teamId"], [:teamId]]),
      map_lookup_any(input || %{}, [["teamId"], [:teamId]]),
      map_lookup_any(arguments, [["teamId"], [:teamId]])
    ])
  end

  defp extract_linear_team_id(_arguments), do: nil

  defp extract_linear_state_id(arguments) when is_map(arguments) do
    variables =
      map_lookup_any(arguments, [["variables"], [:variables]])

    input =
      map_lookup_any(variables || %{}, [["input"], [:input]])

    first_present([
      map_lookup_any(variables || %{}, [["stateId"], [:stateId]]),
      map_lookup_any(variables || %{}, [["state_id"], [:state_id]]),
      map_lookup_any(input || %{}, [["stateId"], [:stateId]]),
      map_lookup_any(input || %{}, [["state_id"], [:state_id]]),
      map_lookup_any(arguments, [["stateId"], [:stateId]])
    ])
  end

  defp extract_linear_state_id(_arguments), do: nil

  defp maybe_suppress_spawned_child_dispatch(%State{} = state, parent_issue_id, update)
       when is_binary(parent_issue_id) and is_map(update) do
    payload = codex_update_payload(update)
    result = codex_update_result(update)

    with :tool_call_completed <- Map.get(update, :event),
         {:ok, "linear_graphql", normalized_args} <- extract_linear_tool_call(payload),
         query when is_binary(query) <- map_lookup_any(normalized_args, [["query"], [:query]]),
         :issue_create <- linear_operation_from_query(query),
         child_issue_id when is_binary(child_issue_id) <- extract_created_issue_id(result) do
      put_in(state.suppressed_dispatch[child_issue_id], %{parent_issue_id: parent_issue_id})
    else
      _ -> state
    end
  end

  defp maybe_suppress_spawned_child_dispatch(state, _parent_issue_id, _update), do: state

  defp extract_created_issue_id(%{} = result) do
    result
    |> extract_graphql_result_payload()
    |> case do
      %{} = payload ->
        first_present([
          map_lookup_any(payload, [["data", "issueCreate", "issue", "id"], [:data, :issueCreate, :issue, :id]]),
          map_lookup_any(payload, [["issueCreate", "issue", "id"], [:issueCreate, :issue, :id]])
        ])

      _ ->
        nil
    end
  end

  defp extract_created_issue_id(_result), do: nil

  defp extract_graphql_result_payload(%{"contentItems" => items}) when is_list(items) do
    decode_graphql_result_items(items)
  end

  defp extract_graphql_result_payload(%{contentItems: items}) when is_list(items) do
    decode_graphql_result_items(items)
  end

  defp extract_graphql_result_payload(%{"output" => items}) when is_list(items) do
    decode_graphql_result_items(items)
  end

  defp extract_graphql_result_payload(%{output: items}) when is_list(items) do
    decode_graphql_result_items(items)
  end

  defp extract_graphql_result_payload(items) when is_list(items) do
    decode_graphql_result_items(items)
  end

  defp extract_graphql_result_payload(_result), do: %{}

  defp decode_graphql_result_items(items) when is_list(items) do
    Enum.find_value(items, %{}, fn
      %{"type" => type, "text" => text} when type in ["inputText", "input_text"] and is_binary(text) ->
        decode_graphql_result_text(text)

      %{type: type, text: text} when type in ["inputText", "input_text"] and is_binary(text) ->
        decode_graphql_result_text(text)

      _ ->
        nil
    end)
  end

  defp decode_graphql_result_items(_items), do: %{}

  defp decode_graphql_result_text(text) when is_binary(text) do
    case Jason.decode(text) do
      {:ok, %{} = payload} -> payload
      _ -> nil
    end
  end

  defp classify_command_event(payload) when is_map(payload) do
    command = extract_timeline_command(payload)
    command_target = normalize_command_target(command)
    exit_code = extract_timeline_exit_code(payload)

    cond do
      command_target == nil ->
        %{}

      String.starts_with?(command_target, "git commit") ->
        %{
          kind: "human_action",
          category: "git",
          action: "git_commit",
          details:
            %{
              command: command,
              exit_code: exit_code
            }
            |> compact_timeline_details()
        }

      String.starts_with?(command_target, "gh pr create") ->
        %{
          kind: "human_action",
          category: "git",
          action: "github_pr_create",
          details:
            %{
              command: command,
              exit_code: exit_code
            }
            |> compact_timeline_details()
        }

      true ->
        %{}
    end
  end

  defp classify_command_event(_payload), do: %{}

  defp classify_thread_status_change(payload) when is_map(payload) do
    %{
      kind: "human_action",
      category: "status",
      action: "thread_status_changed",
      details:
        %{
          from: extract_status_value(payload, :from),
          to: extract_status_value(payload, :to)
        }
        |> compact_timeline_details()
    }
  end

  defp classify_thread_status_change(_payload), do: %{}

  defp extract_status_value(payload, :from) do
    first_present([
      map_lookup(payload, ["params", "from"]),
      map_lookup(payload, ["params", :from]),
      map_lookup(payload, ["params", "oldStatus"]),
      map_lookup(payload, ["params", :oldStatus]),
      map_lookup(payload, ["params", "previousStatus"]),
      map_lookup(payload, ["params", :previousStatus]),
      map_lookup(payload, ["params", "status", "from"])
    ])
  end

  defp extract_status_value(payload, :to) do
    first_present([
      map_lookup(payload, ["params", "to"]),
      map_lookup(payload, ["params", :to]),
      map_lookup(payload, ["params", "newStatus"]),
      map_lookup(payload, ["params", :newStatus]),
      map_lookup(payload, ["params", "currentStatus"]),
      map_lookup(payload, ["params", :currentStatus]),
      map_lookup(payload, ["params", "status", "to"])
    ])
  end

  defp extract_timeline_command(payload) do
    first_present([
      map_lookup(payload, ["params", "msg", "command"]),
      map_lookup(payload, ["params", :msg, :command]),
      map_lookup(payload, ["params", "msg", "parsed_cmd"]),
      map_lookup(payload, ["params", :msg, :parsed_cmd]),
      map_lookup(payload, ["params", "parsedCmd"]),
      map_lookup(payload, ["params", :parsedCmd]),
      map_lookup(payload, ["params", "command"]),
      map_lookup(payload, ["params", :command])
    ])
    |> normalize_string()
  end

  defp extract_timeline_exit_code(payload) do
    first_present([
      map_lookup(payload, ["params", "msg", "exit_code"]),
      map_lookup(payload, ["params", :msg, :exit_code]),
      map_lookup(payload, ["params", "msg", "exitCode"]),
      map_lookup(payload, ["params", :msg, :exitCode]),
      map_lookup(payload, ["params", "exit_code"]),
      map_lookup(payload, ["params", :exit_code])
    ])
  end

  defp normalize_command_target(command) when is_binary(command) do
    target =
      case Regex.run(~r/\s-lc\s+(.+)$/, command, capture: :all_but_first) do
        [shell_command] -> shell_command
        _ -> command
      end
      |> String.trim()
      |> strip_matching_quotes()
      |> String.trim()
      |> String.downcase()

    if target == "", do: nil, else: target
  end

  defp normalize_command_target(_command), do: nil

  defp strip_matching_quotes(<<"\"", rest::binary>>) do
    if String.ends_with?(rest, "\""), do: String.trim_trailing(rest, "\""), else: "\"" <> rest
  end

  defp strip_matching_quotes(<<"'", rest::binary>>) do
    if String.ends_with?(rest, "'"), do: String.trim_trailing(rest, "'"), else: "'" <> rest
  end

  defp strip_matching_quotes(value), do: value

  defp compact_timeline_details(details) when is_map(details) do
    details
    |> Enum.reduce(%{}, fn
      {_key, nil}, acc -> acc
      {key, value}, acc -> Map.put(acc, key, value)
    end)
    |> case do
      map when map_size(map) == 0 -> nil
      map -> map
    end
  end

  defp compact_timeline_details(_details), do: nil

  defp sanitize_timeline_details(nil), do: nil

  defp sanitize_timeline_details(details) when is_map(details) do
    sanitized =
      details
      |> Enum.reduce(%{}, fn {key, value}, acc ->
        case sanitize_timeline_detail_value(value) do
          nil -> acc
          sanitized_value -> Map.put(acc, to_string(key), sanitized_value)
        end
      end)

    if map_size(sanitized) == 0, do: nil, else: sanitized
  end

  defp sanitize_timeline_details(_details), do: nil

  defp sanitize_timeline_detail_value(nil), do: nil

  defp sanitize_timeline_detail_value(value) when is_binary(value) do
    value
    |> String.trim()
    |> redact_sensitive_tokens()
    |> String.slice(0, 240)
  end

  defp sanitize_timeline_detail_value(value) when is_map(value), do: sanitize_timeline_details(value)

  defp sanitize_timeline_detail_value(value) when is_list(value) do
    sanitized =
      value
      |> Enum.map(&sanitize_timeline_detail_value/1)
      |> Enum.reject(&is_nil/1)

    if sanitized == [], do: nil, else: sanitized
  end

  defp sanitize_timeline_detail_value(value)
       when is_integer(value) or is_float(value) or is_boolean(value),
       do: value

  defp sanitize_timeline_detail_value(value) do
    value
    |> to_string()
    |> sanitize_timeline_detail_value()
  end

  defp maybe_put_timeline_field(map, _key, nil), do: map
  defp maybe_put_timeline_field(map, key, value), do: Map.put(map, key, value)

  defp map_lookup(map, path) when is_map(map) and is_list(path) do
    Enum.reduce_while(path, map, fn segment, acc ->
      case acc do
        current when is_map(current) ->
          case Map.fetch(current, segment) do
            {:ok, next} -> {:cont, next}
            :error -> {:halt, nil}
          end

        _ ->
          {:halt, nil}
      end
    end)
  end

  defp map_lookup(_map, _path), do: nil

  defp map_lookup_any(map, paths) when is_map(map) and is_list(paths) do
    Enum.find_value(paths, fn path -> map_lookup(map, path) end)
  end

  defp map_lookup_any(_map, _paths), do: nil

  defp first_present(values) when is_list(values) do
    Enum.find(values, fn value ->
      case value do
        nil -> false
        "" -> false
        _ -> true
      end
    end)
  end

  defp normalize_string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_string(value), do: value

  defp session_events_from_state(%State{} = state, event_stream_id, limit)
       when is_binary(event_stream_id) and is_integer(limit) and limit > 0 do
    in_memory = state.timeline_events |> Map.get(event_stream_id, []) |> Enum.take(-limit)
    persisted = read_session_events_from_store(event_stream_id, limit)

    cond do
      persisted != nil and persisted != [] ->
        {:ok, merge_session_events(persisted, in_memory, limit)}

      in_memory != [] ->
        {:ok, in_memory}

      known_event_stream_id?(state, event_stream_id) ->
        {:ok, []}

      true ->
        {:error, :session_not_found}
    end
  end

  defp session_events_from_state(_state, _event_stream_id, _limit), do: {:error, :session_not_found}

  defp read_session_events_from_store(event_stream_id, limit) do
    case SessionTimelineStore.read(event_stream_id, limit) do
      {:ok, events} when is_list(events) -> events
      _ -> nil
    end
  end

  defp merge_session_events(persisted, in_memory, limit)
       when is_list(persisted) and is_list(in_memory) and is_integer(limit) and limit > 0 do
    (persisted ++ in_memory)
    |> Enum.reduce({MapSet.new(), []}, fn event, {seen, acc} ->
      key = session_event_dedupe_key(event)

      if MapSet.member?(seen, key) do
        {seen, acc}
      else
        {MapSet.put(seen, key), [event | acc]}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
    |> Enum.take(-limit)
  end

  defp merge_session_events(_persisted, in_memory, _limit) when is_list(in_memory), do: in_memory

  defp session_event_dedupe_key(%{} = event) do
    [
      Map.get(event, :at) || Map.get(event, "at"),
      Map.get(event, :event) || Map.get(event, "event"),
      Map.get(event, :session_id) || Map.get(event, "session_id"),
      Map.get(event, :turn_count) || Map.get(event, "turn_count"),
      Map.get(event, :message) || Map.get(event, "message")
    ]
  end

  defp session_event_dedupe_key(event), do: event

  defp known_event_stream_id?(%State{} = state, event_stream_id) when is_binary(event_stream_id) do
    running_ids =
      state.running
      |> Map.values()
      |> Enum.map(&Map.get(&1, :event_stream_id))
      |> Enum.reject(&is_nil/1)

    inactive_ids =
      state.inactive_sessions
      |> Enum.map(&Map.get(&1, :event_stream_id))
      |> Enum.reject(&is_nil/1)

    Enum.any?(running_ids ++ inactive_ids, &(&1 == event_stream_id)) or
      SessionTimelineStore.exists?(event_stream_id)
  end

  defp known_event_stream_id?(_state, _event_stream_id), do: false

  defp prune_timeline_events(%State{} = state) do
    keep_ids =
      state.running
      |> Map.values()
      |> Enum.map(&Map.get(&1, :event_stream_id))
      |> Kernel.++(Enum.map(state.inactive_sessions, &Map.get(&1, :event_stream_id)))
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    timeline_events =
      state.timeline_events
      |> Enum.filter(fn {stream_id, _events} -> MapSet.member?(keep_ids, stream_id) end)
      |> Map.new()

    %{state | timeline_events: timeline_events}
  end

  defp maybe_cleanup_timeline_files(%State{} = state, force \\ false) do
    now_ms = System.monotonic_time(:millisecond)
    last_cleanup_ms = Map.get(state, :timeline_last_cleanup_ms)

    should_cleanup? =
      force or is_nil(last_cleanup_ms) or now_ms - last_cleanup_ms >= @timeline_cleanup_interval_ms

    if should_cleanup? do
      case SessionTimelineStore.cleanup_expired(@timeline_retention_seconds) do
        {:ok, _removed_count} ->
          %{state | timeline_last_cleanup_ms: now_ms}

        {:error, reason} ->
          Logger.warning("Failed session timeline retention cleanup: #{inspect(reason)}")
          %{state | timeline_last_cleanup_ms: now_ms}
      end
    else
      state
    end
  end

  defp build_event_stream_id(%Issue{} = issue) do
    base =
      issue.identifier
      |> to_string()
      |> String.trim()
      |> String.replace(~r/[^A-Za-z0-9_-]/, "-")
      |> case do
        "" -> "issue"
        value -> String.downcase(value)
      end

    unique_suffix = System.unique_integer([:positive, :monotonic]) |> Integer.to_string(36)
    "#{base}-#{unique_suffix}"
  end

  defp normalize_session_event_limit(limit) when is_integer(limit) and limit > 0, do: min(limit, 10_000)
  defp normalize_session_event_limit(_limit), do: 200

  defp schedule_tick(delay_ms) do
    :timer.send_after(delay_ms, self(), :tick)
    :ok
  end

  defp schedule_poll_cycle_start do
    :timer.send_after(@poll_transition_render_delay_ms, self(), :run_poll_cycle)
    :ok
  end

  defp next_poll_in_ms(nil, _now_ms), do: nil

  defp next_poll_in_ms(next_poll_due_at_ms, now_ms) when is_integer(next_poll_due_at_ms) do
    max(0, next_poll_due_at_ms - now_ms)
  end

  defp pop_running_entry(state, issue_id) do
    {Map.get(state.running, issue_id), %{state | running: Map.delete(state.running, issue_id)}}
  end

  defp down_stop_reason(:normal), do: "completed"
  defp down_stop_reason(reason), do: "exited: #{inspect(reason)}"

  defp record_inactive_session(%State{} = state, running_entry, stop_reason)
       when is_map(running_entry) and is_binary(stop_reason) do
    ended_at = DateTime.utc_now()
    issue = Map.get(running_entry, :issue, %{})

    entry = %{
      issue_id: map_value(issue, :id),
      identifier: Map.get(running_entry, :identifier),
      state: map_value(issue, :state),
      event_stream_id: Map.get(running_entry, :event_stream_id),
      session_id: Map.get(running_entry, :session_id),
      turn_count: Map.get(running_entry, :turn_count, 0),
      started_at: Map.get(running_entry, :started_at),
      ended_at: ended_at,
      stop_reason: stop_reason,
      runtime_seconds: running_seconds(Map.get(running_entry, :started_at), ended_at),
      last_codex_timestamp: Map.get(running_entry, :last_codex_timestamp),
      last_codex_message: Map.get(running_entry, :last_codex_message),
      last_codex_event: Map.get(running_entry, :last_codex_event),
      codex_input_tokens: Map.get(running_entry, :codex_input_tokens, 0),
      codex_output_tokens: Map.get(running_entry, :codex_output_tokens, 0),
      codex_total_tokens: Map.get(running_entry, :codex_total_tokens, 0)
    }

    inactive_sessions =
      [entry | state.inactive_sessions]
      |> Enum.take(@inactive_sessions_limit)

    state =
      case InactiveSessionStore.append(entry) do
        :ok ->
          state

        {:error, reason} ->
          Logger.warning("Failed to persist inactive session issue_identifier=#{Map.get(running_entry, :identifier)}: #{inspect(reason)}")
          state
      end

    %{state | inactive_sessions: inactive_sessions}
    |> prune_timeline_events()
  end

  defp record_inactive_session(state, _running_entry, _stop_reason), do: state

  defp persist_run_record(%State{} = state, running_entry, attrs)
       when is_map(running_entry) and is_map(attrs) do
    case build_run_record(state, running_entry, attrs) do
      %{event_stream_id: event_stream_id} = record when is_binary(event_stream_id) and event_stream_id != "" ->
        case RunRecordStore.write(event_stream_id, record) do
          :ok ->
            state

          {:error, reason} ->
            Logger.warning(
              "Failed to persist run record issue_identifier=#{Map.get(running_entry, :identifier)} event_stream_id=#{event_stream_id}: #{inspect(reason)}"
            )

            state
        end

      _ ->
        state
    end
  end

  defp persist_run_record(state, _running_entry, _attrs), do: state

  defp build_run_record(%State{} = state, running_entry, attrs) when is_map(running_entry) and is_map(attrs) do
    ended_at = DateTime.utc_now() |> DateTime.truncate(:second)
    issue = Map.get(running_entry, :issue, %{})
    event_stream_id = Map.get(running_entry, :event_stream_id)
    timeline_events = timeline_events_for_run_record(state, event_stream_id)
    initial_tracker_state = Map.get(running_entry, :initial_tracker_state)
    final_tracker_state = map_value(issue, :state)
    failure_summary = Map.get(attrs, :failure_summary)
    stop_reason = Map.get(attrs, :stop_reason)
    final_status = Map.get(attrs, :final_status, "stopped")

    %{
      issue_identifier: Map.get(running_entry, :identifier),
      issue_id: map_value(issue, :id),
      session_id: Map.get(running_entry, :session_id),
      event_stream_id: event_stream_id,
      workflow_path: Map.get(running_entry, :workflow_path) || Workflow.workflow_file_path(),
      started_at: datetime_to_iso8601(Map.get(running_entry, :started_at)),
      ended_at: DateTime.to_iso8601(ended_at),
      final_status: final_status,
      next_action: Map.get(attrs, :next_action, "none"),
      final_tracker_state: final_tracker_state,
      run_kind: derive_run_kind(running_entry, timeline_events),
      path_summary: derive_path_summary(initial_tracker_state, final_tracker_state),
      key_evidence: derive_key_evidence(timeline_events),
      failure_class: derive_failure_class(stop_reason, failure_summary, timeline_events, final_status),
      failure_summary: derive_failure_summary(stop_reason, failure_summary)
    }
  end

  defp timeline_events_for_run_record(%State{} = state, event_stream_id) when is_binary(event_stream_id) do
    Map.get(state.timeline_events, event_stream_id, [])
  end

  defp timeline_events_for_run_record(_state, _event_stream_id), do: []

  defp derive_run_kind(running_entry, timeline_events) when is_map(running_entry) and is_list(timeline_events) do
    initial_tracker_state =
      case Map.get(running_entry, :initial_tracker_state) do
        value when is_binary(value) -> normalize_issue_state(value)
        _ -> nil
      end

    cond do
      initial_tracker_state == "rework" ->
        "rework"

      Map.get(running_entry, :retry_attempt, 1) > 1 ->
        "retry"

      Enum.any?(timeline_events, &timeline_event_action?(&1, "linear_issue_create")) ->
        "child_spawn_parent"

      true ->
        "normal"
    end
  end

  defp derive_path_summary(initial_tracker_state, final_tracker_state) do
    [initial_tracker_state, final_tracker_state]
    |> Enum.filter(&(is_binary(&1) and String.trim(&1) != ""))
    |> Enum.uniq()
  end

  defp derive_key_evidence(timeline_events) when is_list(timeline_events) do
    timeline_events
    |> Enum.reduce([], fn event, acc ->
      evidence =
        cond do
          timeline_event_action?(event, "linear_issue_create") -> "linear_issue_create"
          timeline_event_action?(event, "linear_status_change") -> "linear_status_change"
          timeline_event_action?(event, "github_pr_create") -> "github_pr_create"
          timeline_event_action?(event, "git_commit") -> "git_commit"
          timeline_event_action?(event, "malformed_event") -> "malformed_event_seen"
          true -> nil
        end

      if is_binary(evidence), do: [evidence | acc], else: acc
    end)
    |> Enum.uniq()
    |> Enum.reverse()
  end

  defp derive_failure_class(stop_reason, failure_summary, timeline_events, final_status) do
    cond do
      final_status == "completed" or final_status == "stopped" ->
        "none"

      is_binary(stop_reason) and String.contains?(stop_reason, "stalled") ->
        "runtime_stall"

      is_binary(failure_summary) and String.contains?(failure_summary, "agent exited") ->
        "tool_failure"

      Enum.any?(timeline_events, &timeline_event_action?(&1, "malformed_event")) ->
        "session_malformed"

      true ->
        "none"
    end
  end

  defp derive_failure_summary(_stop_reason, failure_summary) when is_binary(failure_summary), do: failure_summary
  defp derive_failure_summary(stop_reason, _failure_summary) when is_binary(stop_reason), do: stop_reason
  defp derive_failure_summary(_stop_reason, _failure_summary), do: nil

  defp timeline_event_action?(event, action) when is_map(event) and is_binary(action) do
    map_value(event, :action) == action
  end

  defp timeline_event_action?(_event, _action), do: false

  defp datetime_to_iso8601(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp datetime_to_iso8601(_value), do: nil

  defp load_persisted_inactive_sessions(%State{} = state) do
    case InactiveSessionStore.read_recent(@inactive_sessions_limit) do
      {:ok, inactive_sessions} when is_list(inactive_sessions) ->
        %{state | inactive_sessions: Enum.take(inactive_sessions, @inactive_sessions_limit)}

      {:error, reason} ->
        Logger.warning("Failed to load persisted inactive sessions: #{inspect(reason)}")
        state
    end
  end

  defp map_value(map, key) when is_map(map), do: Map.get(map, key)
  defp map_value(_map, _key), do: nil

  defp record_session_completion_totals(state, running_entry) when is_map(running_entry) do
    runtime_seconds = running_seconds(running_entry.started_at, DateTime.utc_now())

    codex_totals =
      apply_token_delta(
        state.codex_totals,
        %{
          input_tokens: 0,
          output_tokens: 0,
          total_tokens: 0,
          seconds_running: runtime_seconds
        }
      )

    %{state | codex_totals: codex_totals}
  end

  defp record_session_completion_totals(state, _running_entry), do: state

  defp refresh_runtime_config(%State{} = state) do
    %{
      state
      | poll_interval_ms: Config.poll_interval_ms(),
        max_concurrent_agents: Config.max_concurrent_agents()
    }
  end

  defp retry_candidate_issue?(%Issue{} = issue, %State{} = state, terminal_states) do
    retry_candidate_issue?(issue, terminal_states) and !Map.has_key?(state.suppressed_dispatch, issue.id)
  end

  defp retry_candidate_issue?(%Issue{} = issue, terminal_states) do
    candidate_issue?(issue, active_state_set(), terminal_states) and
      !todo_issue_blocked_by_non_terminal?(issue, terminal_states)
  end

  defp dispatch_slots_available?(%Issue{} = issue, %State{} = state) do
    available_slots(state) > 0 and state_slots_available?(issue, state.running)
  end

  defp apply_codex_token_delta(
         %{codex_totals: codex_totals} = state,
         %{input_tokens: input, output_tokens: output, total_tokens: total} = token_delta
       )
       when is_integer(input) and is_integer(output) and is_integer(total) do
    %{state | codex_totals: apply_token_delta(codex_totals, token_delta)}
  end

  defp apply_codex_token_delta(state, _token_delta), do: state

  defp apply_codex_rate_limits(%State{} = state, update) when is_map(update) do
    case extract_rate_limits(update) do
      %{} = rate_limits ->
        %{state | codex_rate_limits: rate_limits}

      _ ->
        state
    end
  end

  defp apply_codex_rate_limits(state, _update), do: state

  defp apply_token_delta(codex_totals, token_delta) do
    input_tokens = Map.get(codex_totals, :input_tokens, 0) + token_delta.input_tokens
    output_tokens = Map.get(codex_totals, :output_tokens, 0) + token_delta.output_tokens
    total_tokens = Map.get(codex_totals, :total_tokens, 0) + token_delta.total_tokens

    seconds_running =
      Map.get(codex_totals, :seconds_running, 0) + Map.get(token_delta, :seconds_running, 0)

    %{
      input_tokens: max(0, input_tokens),
      output_tokens: max(0, output_tokens),
      total_tokens: max(0, total_tokens),
      seconds_running: max(0, seconds_running)
    }
  end

  defp extract_token_delta(running_entry, %{event: _, timestamp: _} = update) do
    running_entry = running_entry || %{}
    usage = extract_token_usage(update)

    {
      compute_token_delta(
        running_entry,
        :input,
        usage,
        :codex_last_reported_input_tokens
      ),
      compute_token_delta(
        running_entry,
        :output,
        usage,
        :codex_last_reported_output_tokens
      ),
      compute_token_delta(
        running_entry,
        :total,
        usage,
        :codex_last_reported_total_tokens
      )
    }
    |> Tuple.to_list()
    |> then(fn [input, output, total] ->
      %{
        input_tokens: input.delta,
        output_tokens: output.delta,
        total_tokens: total.delta,
        input_reported: input.reported,
        output_reported: output.reported,
        total_reported: total.reported
      }
    end)
  end

  defp compute_token_delta(running_entry, token_key, usage, reported_key) do
    next_total = get_token_usage(usage, token_key)
    prev_reported = Map.get(running_entry, reported_key, 0)

    delta =
      if is_integer(next_total) and next_total >= prev_reported do
        next_total - prev_reported
      else
        0
      end

    %{
      delta: max(delta, 0),
      reported: if(is_integer(next_total), do: next_total, else: prev_reported)
    }
  end

  defp extract_token_usage(update) do
    payloads = [
      update[:usage],
      Map.get(update, "usage"),
      Map.get(update, :usage),
      update[:payload],
      Map.get(update, "payload"),
      update
    ]

    Enum.find_value(payloads, &absolute_token_usage_from_payload/1) ||
      Enum.find_value(payloads, &turn_completed_usage_from_payload/1) ||
      %{}
  end

  defp extract_rate_limits(update) do
    rate_limits_from_payload(update[:rate_limits]) ||
      rate_limits_from_payload(Map.get(update, "rate_limits")) ||
      rate_limits_from_payload(Map.get(update, :rate_limits)) ||
      rate_limits_from_payload(update[:payload]) ||
      rate_limits_from_payload(Map.get(update, "payload")) ||
      rate_limits_from_payload(update)
  end

  defp absolute_token_usage_from_payload(payload) when is_map(payload) do
    absolute_paths = [
      ["params", "msg", "payload", "info", "total_token_usage"],
      [:params, :msg, :payload, :info, :total_token_usage],
      ["params", "msg", "info", "total_token_usage"],
      [:params, :msg, :info, :total_token_usage],
      ["params", "tokenUsage", "total"],
      [:params, :tokenUsage, :total],
      ["tokenUsage", "total"],
      [:tokenUsage, :total]
    ]

    explicit_map_at_paths(payload, absolute_paths)
  end

  defp absolute_token_usage_from_payload(_payload), do: nil

  defp turn_completed_usage_from_payload(payload) when is_map(payload) do
    method = Map.get(payload, "method") || Map.get(payload, :method)

    if method in ["turn/completed", :turn_completed] do
      direct =
        Map.get(payload, "usage") ||
          Map.get(payload, :usage) ||
          map_at_path(payload, ["params", "usage"]) ||
          map_at_path(payload, [:params, :usage])

      if is_map(direct) and integer_token_map?(direct), do: direct
    end
  end

  defp turn_completed_usage_from_payload(_payload), do: nil

  defp rate_limits_from_payload(payload) when is_map(payload) do
    direct = Map.get(payload, "rate_limits") || Map.get(payload, :rate_limits)

    cond do
      rate_limits_map?(direct) ->
        direct

      rate_limits_map?(payload) ->
        payload

      true ->
        rate_limit_payloads(payload)
    end
  end

  defp rate_limits_from_payload(payload) when is_list(payload) do
    rate_limit_payloads(payload)
  end

  defp rate_limits_from_payload(_payload), do: nil

  defp rate_limit_payloads(payload) when is_map(payload) do
    Map.values(payload)
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limit_payloads(payload) when is_list(payload) do
    payload
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limits_map?(payload) when is_map(payload) do
    limit_id =
      Map.get(payload, "limit_id") ||
        Map.get(payload, :limit_id) ||
        Map.get(payload, "limit_name") ||
        Map.get(payload, :limit_name)

    has_buckets =
      Enum.any?(
        ["primary", :primary, "secondary", :secondary, "credits", :credits],
        &Map.has_key?(payload, &1)
      )

    !is_nil(limit_id) and has_buckets
  end

  defp rate_limits_map?(_payload), do: false

  defp explicit_map_at_paths(payload, paths) when is_map(payload) and is_list(paths) do
    Enum.find_value(paths, fn path ->
      value = map_at_path(payload, path)

      if is_map(value) and integer_token_map?(value), do: value
    end)
  end

  defp explicit_map_at_paths(_payload, _paths), do: nil

  defp map_at_path(payload, path) when is_map(payload) and is_list(path) do
    Enum.reduce_while(path, payload, fn key, acc ->
      if is_map(acc) and Map.has_key?(acc, key) do
        {:cont, Map.get(acc, key)}
      else
        {:halt, nil}
      end
    end)
  end

  defp map_at_path(_payload, _path), do: nil

  defp integer_token_map?(payload) do
    token_fields = [
      :input_tokens,
      :output_tokens,
      :total_tokens,
      :prompt_tokens,
      :completion_tokens,
      :inputTokens,
      :outputTokens,
      :totalTokens,
      :promptTokens,
      :completionTokens,
      "input_tokens",
      "output_tokens",
      "total_tokens",
      "prompt_tokens",
      "completion_tokens",
      "inputTokens",
      "outputTokens",
      "totalTokens",
      "promptTokens",
      "completionTokens"
    ]

    token_fields
    |> Enum.any?(fn field ->
      value = payload_get(payload, field)
      !is_nil(integer_like(value))
    end)
  end

  defp get_token_usage(usage, :input),
    do:
      payload_get(usage, [
        "input_tokens",
        "prompt_tokens",
        :input_tokens,
        :prompt_tokens,
        :input,
        "promptTokens",
        :promptTokens,
        "inputTokens",
        :inputTokens
      ])

  defp get_token_usage(usage, :output),
    do:
      payload_get(usage, [
        "output_tokens",
        "completion_tokens",
        :output_tokens,
        :completion_tokens,
        :output,
        :completion,
        "outputTokens",
        :outputTokens,
        "completionTokens",
        :completionTokens
      ])

  defp get_token_usage(usage, :total),
    do:
      payload_get(usage, [
        "total_tokens",
        "total",
        :total_tokens,
        :total,
        "totalTokens",
        :totalTokens
      ])

  defp payload_get(payload, fields) when is_list(fields) do
    Enum.find_value(fields, fn field -> map_integer_value(payload, field) end)
  end

  defp payload_get(payload, field), do: map_integer_value(payload, field)

  defp map_integer_value(payload, field) do
    if is_map(payload) do
      value = Map.get(payload, field)
      integer_like(value)
    else
      nil
    end
  end

  defp running_seconds(%DateTime{} = started_at, %DateTime{} = now) do
    max(0, DateTime.diff(now, started_at, :second))
  end

  defp running_seconds(_started_at, _now), do: 0

  defp integer_like(value) when is_integer(value) and value >= 0, do: value

  defp integer_like(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {num, _} when num >= 0 -> num
      _ -> nil
    end
  end

  defp integer_like(_value), do: nil
end
