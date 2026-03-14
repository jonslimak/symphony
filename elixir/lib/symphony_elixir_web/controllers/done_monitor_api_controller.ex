defmodule SymphonyElixirWeb.DoneMonitorApiController do
  @moduledoc """
  JSON API for done-monitor control and status.
  """

  use Phoenix.Controller, formats: [:json]

  alias Plug.Conn
  alias SymphonyElixir.DoneMonitorControl

  @spec state(Conn.t(), map()) :: Conn.t()
  def state(conn, _params) do
    case DoneMonitorControl.snapshot() do
      {:ok, payload} -> json(conn, payload)
      {:error, :unavailable} -> error_response(conn, 503, "monitor_unavailable", "Done monitor is unavailable")
    end
  end

  @spec toggle(Conn.t(), map()) :: Conn.t()
  def toggle(conn, params) do
    with {:ok, enabled} <- parse_enabled(Map.get(params, "enabled")),
         {:ok, mode} <- parse_mode(Map.get(params, "mode")),
         {:ok, payload} <- DoneMonitorControl.set_enabled(enabled, mode) do
      json(conn, payload)
    else
      {:error, :invalid_enabled} ->
        error_response(conn, 422, "invalid_enabled", "enabled must be true or false")

      {:error, :invalid_mode} ->
        error_response(conn, 422, "invalid_mode", "mode must be new_only or backfill")

      {:error, :unavailable} ->
        error_response(conn, 503, "monitor_unavailable", "Done monitor is unavailable")
    end
  end

  @spec interval(Conn.t(), map()) :: Conn.t()
  def interval(conn, params) do
    with {:ok, interval_seconds} <- parse_interval(Map.get(params, "interval_seconds")),
         {:ok, payload} <- DoneMonitorControl.set_interval(interval_seconds) do
      json(conn, payload)
    else
      {:error, :invalid_interval} ->
        error_response(conn, 422, "invalid_interval", "interval_seconds must be between 60 and 86400")

      {:error, :unavailable} ->
        error_response(conn, 503, "monitor_unavailable", "Done monitor is unavailable")
    end
  end

  @spec run_once(Conn.t(), map()) :: Conn.t()
  def run_once(conn, params) do
    max_items = parse_max_items(Map.get(params, "max_items"))

    case DoneMonitorControl.run_once(max_items) do
      {:ok, payload} ->
        conn
        |> put_status(202)
        |> json(payload)

      {:error, :already_running} ->
        error_response(conn, 409, "already_running", "Done monitor is already running")

      {:error, :invalid_max_items} ->
        error_response(conn, 422, "invalid_max_items", "max_items must be a positive integer")

      {:error, :unavailable} ->
        error_response(conn, 503, "monitor_unavailable", "Done monitor is unavailable")
    end
  end

  @spec method_not_allowed(Conn.t(), map()) :: Conn.t()
  def method_not_allowed(conn, _params) do
    error_response(conn, 405, "method_not_allowed", "Method not allowed")
  end

  defp error_response(conn, status, code, message) do
    conn
    |> put_status(status)
    |> json(%{error: %{code: code, message: message}})
  end

  defp parse_enabled(true), do: {:ok, true}
  defp parse_enabled(false), do: {:ok, false}

  defp parse_enabled(value) when is_binary(value) do
    case String.downcase(String.trim(value)) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, :invalid_enabled}
    end
  end

  defp parse_enabled(_value), do: {:error, :invalid_enabled}

  defp parse_mode(nil), do: {:ok, nil}
  defp parse_mode("new_only"), do: {:ok, "new_only"}
  defp parse_mode("backfill"), do: {:ok, "backfill"}
  defp parse_mode(_value), do: {:error, :invalid_mode}

  defp parse_interval(value) when is_integer(value), do: validate_interval(value)

  defp parse_interval(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, _rest} -> validate_interval(parsed)
      _ -> {:error, :invalid_interval}
    end
  end

  defp parse_interval(_value), do: {:error, :invalid_interval}

  defp validate_interval(value) when value >= 60 and value <= 86_400, do: {:ok, value}
  defp validate_interval(_value), do: {:error, :invalid_interval}

  defp parse_max_items(nil), do: 1
  defp parse_max_items(value) when is_integer(value) and value > 0, do: value

  defp parse_max_items(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, _rest} when parsed > 0 -> parsed
      _ -> 1
    end
  end

  defp parse_max_items(_value), do: 1
end
