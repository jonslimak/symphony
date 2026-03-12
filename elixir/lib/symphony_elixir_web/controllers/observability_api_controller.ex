defmodule SymphonyElixirWeb.ObservabilityApiController do
  @moduledoc """
  JSON API for Symphony observability data.
  """

  use Phoenix.Controller, formats: [:json]

  alias Plug.Conn
  alias SymphonyElixirWeb.{Endpoint, Presenter}

  @spec state(Conn.t(), map()) :: Conn.t()
  def state(conn, _params) do
    json(conn, Presenter.state_payload(orchestrator(), snapshot_timeout_ms()))
  end

  @spec issue(Conn.t(), map()) :: Conn.t()
  def issue(conn, %{"issue_identifier" => issue_identifier}) do
    case Presenter.issue_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, :issue_not_found} ->
        error_response(conn, 404, "issue_not_found", "Issue not found")
    end
  end

  @spec session_events(Conn.t(), map()) :: Conn.t()
  def session_events(conn, %{"event_stream_id" => event_stream_id} = params) do
    limit = parse_event_limit(Map.get(params, "limit"))

    case Presenter.session_events_payload(
           event_stream_id,
           limit,
           orchestrator(),
           snapshot_timeout_ms()
         ) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, :session_not_found} ->
        error_response(conn, 404, "session_not_found", "Session timeline not found")
    end
  end

  @spec debug_running_issue(Conn.t(), map()) :: Conn.t()
  def debug_running_issue(conn, %{"issue_identifier" => issue_identifier}) do
    case Presenter.debug_running_issue_payload(issue_identifier, orchestrator(), snapshot_timeout_ms()) do
      {:ok, payload} ->
        json(conn, payload)

      {:error, :not_running} ->
        error_response(conn, 404, "not_running", "Issue is not currently running")
    end
  end

  @spec refresh(Conn.t(), map()) :: Conn.t()
  def refresh(conn, _params) do
    case Presenter.refresh_payload(orchestrator()) do
      {:ok, payload} ->
        conn
        |> put_status(202)
        |> json(payload)

      {:error, :unavailable} ->
        error_response(conn, 503, "orchestrator_unavailable", "Orchestrator is unavailable")
    end
  end

  @spec method_not_allowed(Conn.t(), map()) :: Conn.t()
  def method_not_allowed(conn, _params) do
    error_response(conn, 405, "method_not_allowed", "Method not allowed")
  end

  @spec not_found(Conn.t(), map()) :: Conn.t()
  def not_found(conn, _params) do
    error_response(conn, 404, "not_found", "Route not found")
  end

  defp error_response(conn, status, code, message) do
    conn
    |> put_status(status)
    |> json(%{error: %{code: code, message: message}})
  end

  defp orchestrator do
    Endpoint.config(:orchestrator) || SymphonyElixir.Orchestrator
  end

  defp snapshot_timeout_ms do
    Endpoint.config(:snapshot_timeout_ms) || 15_000
  end

  defp parse_event_limit(limit) when is_integer(limit) and limit > 0, do: min(limit, 10_000)

  defp parse_event_limit(limit) when is_binary(limit) do
    case Integer.parse(String.trim(limit)) do
      {value, _rest} when value > 0 -> min(value, 10_000)
      _ -> 200
    end
  end

  defp parse_event_limit(_limit), do: 200
end
