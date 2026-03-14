defmodule SymphonyElixirWeb.Router do
  @moduledoc """
  Router for Symphony's observability dashboard and API.
  """

  use Phoenix.Router
  import Phoenix.LiveView.Router

  pipeline :browser do
    plug(:fetch_session)
    plug(:fetch_live_flash)
    plug(:put_root_layout, html: {SymphonyElixirWeb.Layouts, :root})
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  scope "/", SymphonyElixirWeb do
    get("/dashboard.css", StaticAssetController, :dashboard_css)
    get("/fonts/:name", StaticAssetController, :font)
    get("/vendor/phoenix_html/phoenix_html.js", StaticAssetController, :phoenix_html_js)
    get("/vendor/phoenix/phoenix.js", StaticAssetController, :phoenix_js)
    get("/vendor/phoenix_live_view/phoenix_live_view.js", StaticAssetController, :phoenix_live_view_js)
  end

  scope "/", SymphonyElixirWeb do
    pipe_through(:browser)

    live("/", DashboardLive, :index)
  end

  scope "/", SymphonyElixirWeb do
    get("/api/v1/state", ObservabilityApiController, :state)
    get("/api/v1/done-monitor/state", DoneMonitorApiController, :state)
    post("/api/v1/done-monitor/toggle", DoneMonitorApiController, :toggle)
    post("/api/v1/done-monitor/interval", DoneMonitorApiController, :interval)
    post("/api/v1/done-monitor/run_once", DoneMonitorApiController, :run_once)
    get("/api/v1/session/:event_stream_id/events", ObservabilityApiController, :session_events)
    get("/api/v1/debug/running/:issue_identifier", ObservabilityApiController, :debug_running_issue)

    match(:*, "/", ObservabilityApiController, :method_not_allowed)
    match(:*, "/api/v1/state", ObservabilityApiController, :method_not_allowed)
    match(:*, "/api/v1/done-monitor/state", DoneMonitorApiController, :method_not_allowed)
    match(:*, "/api/v1/done-monitor/toggle", DoneMonitorApiController, :method_not_allowed)
    match(:*, "/api/v1/done-monitor/interval", DoneMonitorApiController, :method_not_allowed)
    match(:*, "/api/v1/done-monitor/run_once", DoneMonitorApiController, :method_not_allowed)
    match(:*, "/api/v1/session/:event_stream_id/events", ObservabilityApiController, :method_not_allowed)
    match(:*, "/api/v1/debug/running/:issue_identifier", ObservabilityApiController, :method_not_allowed)
    post("/api/v1/refresh", ObservabilityApiController, :refresh)
    match(:*, "/api/v1/refresh", ObservabilityApiController, :method_not_allowed)
    get("/api/v1/:issue_identifier", ObservabilityApiController, :issue)
    match(:*, "/api/v1/:issue_identifier", ObservabilityApiController, :method_not_allowed)
    match(:*, "/*path", ObservabilityApiController, :not_found)
  end
end
