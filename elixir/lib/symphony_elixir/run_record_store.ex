defmodule SymphonyElixir.RunRecordStore do
  @moduledoc """
  Persists per-session run records for observability.
  """

  alias SymphonyElixir.Config

  @run_record_dir ".symphony_observability/run_records"
  @stream_id_pattern ~r/^[A-Za-z0-9._-]+$/

  @spec write(String.t(), map()) :: :ok | {:error, term()}
  def write(event_stream_id, %{} = record) when is_binary(event_stream_id) do
    with {:ok, stream_id} <- normalize_stream_id(event_stream_id),
         :ok <- ensure_run_record_dir(),
         encoded <- Jason.encode!(record),
         :ok <- File.write(run_record_path(stream_id), encoded) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    error in [Jason.EncodeError, Protocol.UndefinedError] ->
      {:error, {:encode_failed, error}}
  end

  @spec read(String.t()) :: {:ok, map()} | {:error, :not_found | term()}
  def read(event_stream_id) when is_binary(event_stream_id) do
    with {:ok, stream_id} <- normalize_stream_id(event_stream_id),
         {:ok, content} <- File.read(run_record_path(stream_id)),
         {:ok, %{} = decoded} <- Jason.decode(content) do
      {:ok, decoded}
    else
      {:error, :enoent} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :invalid_record}
    end
  end

  @spec path(String.t()) :: Path.t()
  def path(event_stream_id) when is_binary(event_stream_id) do
    case normalize_stream_id(event_stream_id) do
      {:ok, stream_id} -> run_record_path(stream_id)
      {:error, _reason} -> run_record_path(event_stream_id)
    end
  end

  defp normalize_stream_id(event_stream_id) do
    trimmed = String.trim(event_stream_id)

    cond do
      trimmed == "" ->
        {:error, :not_found}

      String.match?(trimmed, @stream_id_pattern) ->
        {:ok, trimmed}

      true ->
        {:error, :not_found}
    end
  end

  defp ensure_run_record_dir do
    File.mkdir_p(run_record_dir_path())
  end

  defp run_record_dir_path do
    Path.join(Config.workspace_root(), @run_record_dir)
  end

  defp run_record_path(stream_id) do
    Path.join(run_record_dir_path(), "#{stream_id}.json")
  end
end
