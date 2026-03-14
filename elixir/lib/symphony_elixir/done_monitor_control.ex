defmodule SymphonyElixir.DoneMonitorControl do
  @moduledoc """
  Controls done-monitor scheduling and on-demand runs for dashboard/API toggles.
  """

  use GenServer

  require Logger

  @default_interval_seconds 300
  @min_interval_seconds 60
  @max_interval_seconds 86_400
  @default_mode "new_only"

  @type mode :: String.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec snapshot() :: {:ok, map()} | {:error, :unavailable}
  def snapshot do
    call(:snapshot)
  end

  @spec set_enabled(boolean(), mode() | nil) :: {:ok, map()} | {:error, term()}
  def set_enabled(enabled, mode \\ nil) when is_boolean(enabled) do
    call({:set_enabled, enabled, mode})
  end

  @spec set_interval(pos_integer()) :: {:ok, map()} | {:error, term()}
  def set_interval(interval_seconds) when is_integer(interval_seconds) do
    call({:set_interval, interval_seconds})
  end

  @spec run_once(pos_integer() | nil) :: {:ok, map()} | {:error, term()}
  def run_once(max_items \\ 1)

  def run_once(nil), do: call({:run_once, nil})

  def run_once(max_items) when is_integer(max_items) and max_items > 0 do
    call({:run_once, max_items})
  end

  def run_once(_max_items), do: {:error, :invalid_max_items}

  @impl true
  def init(_opts) do
    state =
      defaults()
      |> Map.merge(load_control_file())
      |> normalize_state()
      |> schedule_next_tick()

    {:ok, state}
  end

  @impl true
  def handle_call(:snapshot, _from, state) do
    {:reply, {:ok, public_state(state)}, state}
  end

  def handle_call({:set_enabled, enabled, mode}, _from, state) do
    with {:ok, normalized_mode} <- normalize_mode(mode || state.mode) do
      next_state =
        state
        |> Map.put(:enabled, enabled)
        |> Map.put(:mode, normalized_mode)
        |> maybe_set_enabled_at(enabled, normalized_mode)
        |> Map.put(:last_error, nil)
        |> schedule_next_tick()

      :ok = persist_control_file(next_state)
      {:reply, {:ok, public_state(next_state)}, next_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:set_interval, interval_seconds}, _from, state) do
    if interval_seconds < @min_interval_seconds or interval_seconds > @max_interval_seconds do
      {:reply, {:error, :invalid_interval}, state}
    else
      next_state =
        state
        |> Map.put(:interval_seconds, interval_seconds)
        |> schedule_next_tick()

      :ok = persist_control_file(next_state)
      {:reply, {:ok, public_state(next_state)}, next_state}
    end
  end

  def handle_call({:run_once, max_items}, _from, state) do
    if state.running do
      {:reply, {:error, :already_running}, state}
    else
      next_state = start_run(state, :manual, max_items)
      {:reply, {:ok, public_state(next_state)}, next_state}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    next_state =
      state
      |> schedule_next_tick()
      |> maybe_start_scheduled_run()

    {:noreply, next_state}
  end

  def handle_info({:run_complete, result}, state) when is_map(result) do
    now_iso = DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    {last_status, last_error} =
      if result.ok do
        {"ok", nil}
      else
        {"error", result.error || "run_failed"}
      end

    next_state =
      state
      |> Map.put(:running, false)
      |> Map.put(:last_run_at, now_iso)
      |> Map.put(:last_status, last_status)
      |> Map.put(:last_error, last_error)

    :ok = persist_control_file(next_state)
    {:noreply, next_state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp maybe_start_scheduled_run(state) do
    if state.enabled and not state.running do
      start_run(state, :scheduled, nil)
    else
      state
    end
  end

  defp start_run(state, source, max_items) do
    mode = state.mode
    enabled_at = state.enabled_at
    script_path = monitor_script_path()
    python_exec = python_executable()

    Task.start(fn ->
      args =
        [script_path, "--mode", mode, "--summary-json"]
        |> maybe_add_enabled_at(mode, enabled_at)
        |> maybe_add_max_items(max_items)

      {output, exit_code} =
        System.cmd(python_exec, args,
          stderr_to_stdout: true,
          env: [],
          cd: File.cwd!()
        )

      summary = parse_summary_json(output)

      if exit_code == 0 do
        Logger.info("done-monitor run (#{source}) succeeded")
        send(__MODULE__, {:run_complete, %{ok: true, summary: summary}})
      else
        Logger.warning("done-monitor run (#{source}) failed: #{String.trim(output)}")

        send(
          __MODULE__,
          {:run_complete,
           %{ok: false, error: "exit_#{exit_code}", output: String.slice(output || "", 0, 600), summary: summary}}
        )
      end
    end)

    state
    |> Map.put(:running, true)
    |> Map.put(:last_status, "running")
    |> Map.put(:last_error, nil)
  end

  defp parse_summary_json(output) when is_binary(output) do
    output
    |> String.split("\n", trim: true)
    |> Enum.reverse()
    |> Enum.find_value(fn line ->
      case Jason.decode(line) do
        {:ok, value} when is_map(value) -> value
        _ -> nil
      end
    end)
  end

  defp parse_summary_json(_output), do: nil

  defp maybe_add_enabled_at(args, "new_only", enabled_at) when is_binary(enabled_at) and enabled_at != "" do
    args ++ ["--enabled-at", enabled_at]
  end

  defp maybe_add_enabled_at(args, _mode, _enabled_at), do: args

  defp maybe_add_max_items(args, max_items) when is_integer(max_items) and max_items > 0 do
    args ++ ["--max-items", Integer.to_string(max_items)]
  end

  defp maybe_add_max_items(args, _max_items), do: args

  defp call(message) do
    case GenServer.whereis(__MODULE__) do
      nil -> {:error, :unavailable}
      _pid -> GenServer.call(__MODULE__, message)
    end
  end

  defp maybe_set_enabled_at(state, false, _mode), do: Map.put(state, :enabled_at, state.enabled_at)

  defp maybe_set_enabled_at(state, true, "new_only") do
    enabled_at =
      case state.enabled_at do
        value when is_binary(value) and value != "" and state.enabled -> value
        _ -> DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()
      end

    Map.put(state, :enabled_at, enabled_at)
  end

  defp maybe_set_enabled_at(state, true, _mode), do: Map.put(state, :enabled_at, nil)

  defp schedule_next_tick(state) do
    if is_reference(state.timer_ref) do
      Process.cancel_timer(state.timer_ref)
    end

    if state.enabled do
      ref = Process.send_after(self(), :tick, max(state.interval_seconds, @min_interval_seconds) * 1_000)
      %{state | timer_ref: ref}
    else
      %{state | timer_ref: nil}
    end
  end

  defp defaults do
    %{
      enabled: false,
      interval_seconds: @default_interval_seconds,
      mode: @default_mode,
      enabled_at: nil,
      last_run_at: nil,
      last_status: "idle",
      last_error: nil,
      running: false,
      timer_ref: nil
    }
  end

  defp normalize_state(state) do
    interval =
      state[:interval_seconds]
      |> case do
        value when is_integer(value) -> value
        value when is_binary(value) ->
          case Integer.parse(String.trim(value)) do
            {parsed, _rest} -> parsed
            _ -> @default_interval_seconds
          end
        _ -> @default_interval_seconds
      end
      |> max(@min_interval_seconds)
      |> min(@max_interval_seconds)

    mode =
      case normalize_mode(state[:mode]) do
        {:ok, normalized} -> normalized
        _ -> @default_mode
      end

    %{
      enabled: !!state[:enabled],
      interval_seconds: interval,
      mode: mode,
      enabled_at: normalize_optional_string(state[:enabled_at]),
      last_run_at: normalize_optional_string(state[:last_run_at]),
      last_status: normalize_optional_string(state[:last_status]) || "idle",
      last_error: normalize_optional_string(state[:last_error]),
      running: false,
      timer_ref: nil
    }
  end

  defp normalize_mode("new_only"), do: {:ok, "new_only"}
  defp normalize_mode("backfill"), do: {:ok, "backfill"}
  defp normalize_mode(_), do: {:error, :invalid_mode}

  defp normalize_optional_string(value) when is_binary(value) do
    trimmed = String.trim(value)
    if trimmed == "", do: nil, else: trimmed
  end

  defp normalize_optional_string(_value), do: nil

  defp persist_control_file(state) do
    payload = %{
      enabled: state.enabled,
      interval_seconds: state.interval_seconds,
      mode: state.mode,
      enabled_at: state.enabled_at,
      last_run_at: state.last_run_at,
      last_status: state.last_status,
      last_error: state.last_error
    }

    path = control_file_path()
    _ = File.mkdir_p(Path.dirname(path))

    with {:ok, json} <- Jason.encode(payload, pretty: true),
         :ok <- File.write(path, json <> "\n") do
      :ok
    else
      {:error, reason} ->
        Logger.warning("done-monitor: failed to persist control file: #{inspect(reason)}")
        :ok
    end
  end

  defp load_control_file do
    path = control_file_path()

    with {:ok, contents} <- File.read(path),
         {:ok, decoded} <- Jason.decode(contents) do
      decoded
      |> Enum.reduce(%{}, fn
        {"enabled", value}, acc -> Map.put(acc, :enabled, value)
        {"interval_seconds", value}, acc -> Map.put(acc, :interval_seconds, value)
        {"mode", value}, acc -> Map.put(acc, :mode, value)
        {"enabled_at", value}, acc -> Map.put(acc, :enabled_at, value)
        {"last_run_at", value}, acc -> Map.put(acc, :last_run_at, value)
        {"last_status", value}, acc -> Map.put(acc, :last_status, value)
        {"last_error", value}, acc -> Map.put(acc, :last_error, value)
        {_other, _value}, acc -> acc
      end)
    else
      {:error, :enoent} -> %{}
      {:error, reason} ->
        Logger.warning("done-monitor: could not load control file #{path}: #{inspect(reason)}")
        %{}
    end
  end

  defp public_state(state) do
    %{
      enabled: state.enabled,
      interval_seconds: state.interval_seconds,
      mode: state.mode,
      enabled_at: state.enabled_at,
      last_run_at: state.last_run_at,
      last_status: state.last_status,
      last_error: state.last_error,
      running: state.running
    }
  end

  defp monitor_script_path do
    System.get_env("DONE_MONITOR_SCRIPT") ||
      Path.expand("../../../tools/done-monitor/monitor.py", __DIR__)
  end

  defp control_file_path do
    System.get_env("DONE_MONITOR_CONTROL") ||
      Path.expand("../../../tools/done-monitor/control.json", __DIR__)
  end

  defp python_executable do
    System.get_env("DONE_MONITOR_PYTHON") || "python3"
  end
end
