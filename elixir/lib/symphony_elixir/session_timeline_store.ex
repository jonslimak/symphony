defmodule SymphonyElixir.SessionTimelineStore do
  @moduledoc """
  Persists per-session timeline entries for observability.
  """

  require Logger

  alias SymphonyElixir.Config

  @timeline_dir ".symphony_observability/timelines"
  @stream_id_pattern ~r/^[A-Za-z0-9._-]+$/

  @spec append(String.t(), map()) :: :ok | {:error, term()}
  def append(event_stream_id, %{} = entry) when is_binary(event_stream_id) do
    with {:ok, stream_id} <- normalize_stream_id(event_stream_id),
         :ok <- ensure_timeline_dir(),
         encoded <- Jason.encode!(entry),
         :ok <- File.write(stream_file_path(stream_id), encoded <> "\n", [:append]) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    error in [Jason.EncodeError] ->
      {:error, {:encode_failed, error}}
  end

  @spec read(String.t(), pos_integer()) :: {:ok, [map()]} | {:error, :session_not_found | term()}
  def read(event_stream_id, limit) when is_binary(event_stream_id) do
    with {:ok, stream_id} <- normalize_stream_id(event_stream_id),
         {:ok, content} <- File.read(stream_file_path(stream_id)) do
      entries =
        content
        |> String.split("\n", trim: true)
        |> Enum.reduce([], fn line, acc ->
          case Jason.decode(line) do
            {:ok, %{} = entry} -> [entry | acc]
            _ -> acc
          end
        end)
        |> Enum.reverse()
        |> Enum.take(-normalize_limit(limit))

      {:ok, entries}
    else
      {:error, :enoent} -> {:error, :session_not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec cleanup_expired(non_neg_integer()) :: {:ok, non_neg_integer()} | {:error, term()}
  def cleanup_expired(retention_seconds) when is_integer(retention_seconds) and retention_seconds >= 0 do
    dir = timeline_dir_path()

    case File.mkdir_p(dir) do
      :ok ->
        now_seconds = DateTime.utc_now() |> DateTime.to_unix()

        removed_count =
          dir
          |> Path.join("*.ndjson")
          |> Path.wildcard()
          |> Enum.reduce(0, fn path, removed ->
            if stale_file?(path, now_seconds, retention_seconds) do
              case File.rm(path) do
                :ok ->
                  removed + 1

                {:error, reason} ->
                  Logger.warning("Failed to remove stale session timeline #{path}: #{inspect(reason)}")
                  removed
              end
            else
              removed
            end
          end)

        {:ok, removed_count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec exists?(String.t()) :: boolean()
  def exists?(event_stream_id) when is_binary(event_stream_id) do
    with {:ok, stream_id} <- normalize_stream_id(event_stream_id) do
      File.exists?(stream_file_path(stream_id))
    else
      _ -> false
    end
  end

  defp stale_file?(path, now_seconds, retention_seconds) do
    case File.stat(path, time: :posix) do
      {:ok, stat} when is_integer(stat.mtime) ->
        now_seconds - stat.mtime > retention_seconds

      _ ->
        false
    end
  end

  defp normalize_stream_id(event_stream_id) do
    trimmed = String.trim(event_stream_id)

    cond do
      trimmed == "" ->
        {:error, :session_not_found}

      String.match?(trimmed, @stream_id_pattern) ->
        {:ok, trimmed}

      true ->
        {:error, :session_not_found}
    end
  end

  defp normalize_limit(limit) when is_integer(limit) and limit > 0, do: min(limit, 10_000)
  defp normalize_limit(_limit), do: 200

  defp ensure_timeline_dir do
    File.mkdir_p(timeline_dir_path())
  end

  defp timeline_dir_path do
    Path.join(Config.workspace_root(), @timeline_dir)
  end

  defp stream_file_path(stream_id) do
    Path.join(timeline_dir_path(), "#{stream_id}.ndjson")
  end
end
