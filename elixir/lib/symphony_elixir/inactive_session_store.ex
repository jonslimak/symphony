defmodule SymphonyElixir.InactiveSessionStore do
  @moduledoc """
  Persists inactive session summaries for observability across restarts.
  """

  alias SymphonyElixir.Config

  @observability_dir ".symphony_observability"
  @inactive_sessions_file "inactive_sessions.ndjson"

  @spec append(map()) :: :ok | {:error, term()}
  def append(%{} = entry) do
    with :ok <- ensure_observability_dir(),
         encoded <- Jason.encode!(entry),
         :ok <- File.write(path(), encoded <> "\n", [:append]) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    error in [Jason.EncodeError] ->
      {:error, {:encode_failed, error}}
  end

  @spec read_recent(pos_integer()) :: {:ok, [map()]} | {:error, term()}
  def read_recent(limit) when is_integer(limit) and limit > 0 do
    case File.read(path()) do
      {:ok, content} ->
        entries =
          content
          |> String.split("\n", trim: true)
          |> Enum.reduce([], fn line, acc ->
            case Jason.decode(line) do
              {:ok, %{} = decoded} ->
                case normalize_entry(decoded) do
                  {:ok, entry} -> [entry | acc]
                  :error -> acc
                end

              _ ->
                acc
            end
          end)
          |> Enum.take(limit)

        {:ok, entries}

      {:error, :enoent} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def read_recent(_limit), do: {:ok, []}

  @spec path() :: Path.t()
  def path do
    Path.join(observability_dir_path(), @inactive_sessions_file)
  end

  defp normalize_entry(%{} = entry) do
    {:ok,
     %{
       issue_id: map_value(entry, :issue_id),
       identifier: map_value(entry, :identifier),
       state: map_value(entry, :state),
       event_stream_id: map_value(entry, :event_stream_id),
       session_id: map_value(entry, :session_id),
       turn_count: normalize_integer(map_value(entry, :turn_count)),
       started_at: parse_datetime(map_value(entry, :started_at)),
       ended_at: parse_datetime(map_value(entry, :ended_at)),
       stop_reason: map_value(entry, :stop_reason),
       runtime_seconds: normalize_integer(map_value(entry, :runtime_seconds)),
       last_codex_timestamp: parse_datetime(map_value(entry, :last_codex_timestamp)),
       last_codex_message: map_value(entry, :last_codex_message),
       last_codex_event: map_value(entry, :last_codex_event),
       codex_input_tokens: normalize_integer(map_value(entry, :codex_input_tokens)),
       codex_output_tokens: normalize_integer(map_value(entry, :codex_output_tokens)),
       codex_total_tokens: normalize_integer(map_value(entry, :codex_total_tokens))
     }}
  end

  defp normalize_entry(_entry), do: :error

  defp parse_datetime(%DateTime{} = value), do: value

  defp parse_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> datetime
      _ -> nil
    end
  end

  defp parse_datetime(_value), do: nil

  defp normalize_integer(value) when is_integer(value), do: value
  defp normalize_integer(value) when is_float(value), do: trunc(value)
  defp normalize_integer(_value), do: 0

  defp map_value(map, key) when is_map(map) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  defp map_value(_map, _key), do: nil

  defp ensure_observability_dir do
    File.mkdir_p(observability_dir_path())
  end

  defp observability_dir_path do
    Path.join(Config.workspace_root(), @observability_dir)
  end
end
