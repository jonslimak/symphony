defmodule SymphonyElixirWeb.DashboardLive do
  @moduledoc """
  Live observability dashboard for Symphony.
  """

  use Phoenix.LiveView, layout: {SymphonyElixirWeb.Layouts, :app}

  alias SymphonyElixir.Config
  alias SymphonyElixir.Linear.Issue
  alias SymphonyElixir.Tracker
  alias SymphonyElixirWeb.{Endpoint, ObservabilityPubSub, Presenter, SessionActivityFormatter}
  @runtime_tick_ms 1_000
  @session_events_limit 10_000
  @session_human_ledger_limit 5_000
  @project_tickets_limit 100

  @impl true
  def mount(_params, _session, socket) do
    socket =
      socket
      |> assign(:payload, load_payload())
      |> assign(:now, DateTime.utc_now())
      |> assign(:activity_drawer_open, false)
      |> assign(:activity_mode, "human")
      |> assign(:activity_stream_id, nil)
      |> assign(:activity_issue_identifier, nil)
      |> assign(:activity_events_raw, [])
      |> assign(:activity_events_readable, [])
      |> assign(:activity_events_human, [])
      |> assign(:activity_human_ledger, %{})
      |> assign(:activity_expanded_keys, MapSet.new())
      |> assign(:activity_error, nil)
      |> assign(:project_tickets_limit, @project_tickets_limit)
      |> assign(:project_tickets, [])
      |> assign(:project_tickets_error, nil)
      |> assign(:project_tickets_fetched_at, nil)
      |> assign(:ticket_editor, nil)
      |> assign(:ticket_status_drafts, %{})
      |> assign(:ticket_comment_drafts, %{})
      |> assign(:ticket_status_options, ticket_status_options())
      |> refresh_project_tickets()

    if connected?(socket) do
      :ok = ObservabilityPubSub.subscribe()
      schedule_runtime_tick()
    end

    {:ok, socket}
  end

  @impl true
  def handle_info(:runtime_tick, socket) do
    schedule_runtime_tick()
    {:noreply, assign(socket, :now, DateTime.utc_now())}
  end

  @impl true
  def handle_info(:observability_updated, socket) do
    payload = load_payload()

    {:noreply,
     socket
     |> assign(:payload, payload)
     |> assign(:now, DateTime.utc_now())
     |> maybe_refresh_activity_drawer()
     |> refresh_project_tickets()}
  end

  @impl true
  def handle_event("open_activity", %{"stream_id" => stream_id} = params, socket)
      when is_binary(stream_id) do
    issue_identifier =
      params["issue_identifier"]
      |> case do
        value when is_binary(value) and value != "" -> value
        _ -> "unknown issue"
      end

    now = socket.assigns.now || DateTime.utc_now()
    {raw_events, readable_events, human_events, error} = load_session_events(stream_id, now)
    {socket, merged_human_events} = update_human_ledger(socket, stream_id, human_events)

    {:noreply,
     socket
     |> assign(:activity_drawer_open, true)
     |> assign(:activity_mode, "human")
     |> assign(:activity_stream_id, stream_id)
     |> assign(:activity_issue_identifier, issue_identifier)
     |> assign(:activity_events_raw, raw_events)
     |> assign(:activity_events_readable, readable_events)
     |> assign(:activity_events_human, merged_human_events)
     |> assign(:activity_expanded_keys, MapSet.new())
     |> assign(:activity_error, error)}
  end

  def handle_event("open_activity", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("set_activity_mode", %{"mode" => mode}, socket)
      when mode in ["human", "readable", "raw"] do
    {:noreply, assign(socket, :activity_mode, mode)}
  end

  def handle_event("set_activity_mode", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("toggle_activity_detail", %{"key" => key}, socket) when is_binary(key) do
    expanded_keys = socket.assigns.activity_expanded_keys || MapSet.new()

    next_expanded =
      if MapSet.member?(expanded_keys, key) do
        MapSet.delete(expanded_keys, key)
      else
        MapSet.put(expanded_keys, key)
      end

    {:noreply, assign(socket, :activity_expanded_keys, next_expanded)}
  end

  def handle_event("toggle_activity_detail", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("close_activity", _params, socket) do
    {:noreply, close_activity_drawer(socket)}
  end

  @impl true
  def handle_event("open_ticket_status_editor", %{"issue_id" => issue_id} = params, socket)
      when is_binary(issue_id) and issue_id != "" do
    default_status = normalize_status_draft(params["current_state"])

    next_status =
      socket.assigns.ticket_status_drafts
      |> ticket_status_draft(issue_id, default_status)

    {:noreply,
     socket
     |> assign(:ticket_editor, %{issue_id: issue_id, mode: "status"})
     |> put_ticket_status_draft(issue_id, next_status)}
  end

  def handle_event("open_ticket_status_editor", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("open_ticket_comment_editor", %{"issue_id" => issue_id}, socket)
      when is_binary(issue_id) and issue_id != "" do
    {:noreply,
     socket
     |> assign(:ticket_editor, %{issue_id: issue_id, mode: "comment"})
     |> put_ticket_comment_draft(issue_id, ticket_comment_draft(socket.assigns.ticket_comment_drafts, issue_id, ""))}
  end

  def handle_event("open_ticket_comment_editor", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("close_ticket_editor", _params, socket) do
    {:noreply, assign(socket, :ticket_editor, nil)}
  end

  @impl true
  def handle_event("ticket_status_change", %{"issue_id" => issue_id, "status" => status}, socket)
      when is_binary(issue_id) and issue_id != "" do
    {:noreply, put_ticket_status_draft(socket, issue_id, normalize_status_draft(status))}
  end

  def handle_event("ticket_status_change", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("ticket_comment_change", %{"issue_id" => issue_id, "comment_body" => body}, socket)
      when is_binary(issue_id) and issue_id != "" do
    {:noreply, put_ticket_comment_draft(socket, issue_id, to_string(body))}
  end

  def handle_event("ticket_comment_change", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("submit_ticket_status", %{"issue_id" => issue_id, "status" => raw_status}, socket)
      when is_binary(issue_id) and issue_id != "" do
    status = normalize_status_draft(raw_status)
    socket = put_ticket_status_draft(socket, issue_id, status)

    case find_project_ticket(socket.assigns.project_tickets, issue_id) do
      %Issue{} = issue ->
        cond do
          status == "" ->
            {:noreply, put_flash(socket, :error, "Choose a status before saving.")}

          !valid_ticket_status?(socket.assigns.ticket_status_options, status) ->
            {:noreply, put_flash(socket, :error, "Choose a valid status from the list.")}

          true ->
            submit_ticket_status_update(socket, issue, status)
        end

      _ ->
        {:noreply, put_flash(socket, :error, "Ticket not found in the current dashboard snapshot.")}
    end
  end

  def handle_event("submit_ticket_status", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("submit_ticket_comment", %{"issue_id" => issue_id, "comment_body" => raw_body}, socket)
      when is_binary(issue_id) and issue_id != "" do
    body = to_string(raw_body)
    socket = put_ticket_comment_draft(socket, issue_id, body)

    case find_project_ticket(socket.assigns.project_tickets, issue_id) do
      %Issue{} = issue ->
        trimmed = String.trim(body)

        if trimmed == "" do
          {:noreply, put_flash(socket, :error, "Comment cannot be empty.")}
        else
          submit_ticket_comment(socket, issue, trimmed)
        end

      _ ->
        {:noreply, put_flash(socket, :error, "Ticket not found in the current dashboard snapshot.")}
    end
  end

  def handle_event("submit_ticket_comment", _params, socket), do: {:noreply, socket}

  @impl true
  def render(assigns) do
    ~H"""
    <section class="dashboard-shell">
      <%= if @payload[:error] do %>
        <section class="error-card">
          <h2 class="error-title">
            Snapshot unavailable
          </h2>
          <p class="error-copy">
            <strong><%= @payload.error.code %>:</strong> <%= @payload.error.message %>
          </p>
        </section>
      <% else %>
        <section class="metric-inline-bar" aria-label="Runtime summary">
          <p class="metric-inline-item">
            <span class="metric-inline-label">Running</span>
            <span class="metric-inline-value numeric"><%= @payload.counts.running %></span>
          </p>

          <p class="metric-inline-item">
            <span class="metric-inline-label">Retrying</span>
            <span class="metric-inline-value numeric"><%= @payload.counts.retrying %></span>
          </p>

          <p class="metric-inline-item">
            <span class="metric-inline-label">Total tokens</span>
            <span class="metric-inline-value numeric"><%= format_int(@payload.codex_totals.total_tokens) %></span>
          </p>

          <p class="metric-inline-item">
            <span class="metric-inline-label">Runtime</span>
            <span class="metric-inline-value numeric"><%= format_runtime_seconds(total_runtime_seconds(@payload, @now)) %></span>
          </p>

          <p class="metric-inline-item metric-inline-item-live">
            <span class="metric-inline-label">Live</span>
            <span class="metric-inline-value">
              <span class="status-inline status-inline-live">
                <span class="status-inline-dot"></span>
                Live
              </span>
              <span class="status-inline status-inline-offline">
                <span class="status-inline-dot"></span>
                Offline
              </span>
            </span>
          </p>
        </section>

        <section class="section-card section-running">
          <div class="section-header">
            <div>
              <h2 class="section-title">Running sessions</h2>
            </div>
          </div>

          <%= if @payload.running == [] do %>
            <p class="empty-state">No active sessions.</p>
          <% else %>
            <div class="table-wrap">
              <table class="data-table data-table-running data-table-running-main">
                <colgroup>
                  <col style="width: 12rem;" />
                  <col style="width: 8rem;" />
                  <col style="width: 7.5rem;" />
                  <col style="width: 8.5rem;" />
                  <col />
                  <col style="width: 10rem;" />
                </colgroup>
                <thead>
                  <tr>
                    <th>Issue</th>
                    <th>State</th>
                    <th>Session</th>
                    <th>Runtime / turns</th>
                    <th>Codex update</th>
                    <th>Tokens</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @payload.running}>
                    <td>
                      <div class="issue-stack">
                        <span class="issue-id"><%= entry.issue_identifier %></span>
                        <a class="issue-link" href={"/api/v1/#{entry.issue_identifier}"}>JSON details</a>
                      </div>
                    </td>
                    <td>
                      <span class={state_badge_class(entry.state)}>
                        <%= entry.state %>
                      </span>
                    </td>
                    <td>
                      <div class="session-stack">
                        <%= if entry.session_id do %>
                          <button
                            type="button"
                            class="subtle-button session-action-button"
                            data-label="Copy ID"
                            data-copy={entry.session_id}
                            onclick="navigator.clipboard.writeText(this.dataset.copy); this.textContent = 'Copied'; clearTimeout(this._copyTimer); this._copyTimer = setTimeout(() => { this.textContent = this.dataset.label }, 1200);"
                          >
                            Copy ID
                          </button>
                        <% else %>
                          <span class="muted">n/a</span>
                        <% end %>

                        <button
                          :if={entry.event_stream_id}
                          type="button"
                          class="subtle-button session-action-button"
                          phx-click="open_activity"
                          phx-value-stream_id={entry.event_stream_id}
                          phx-value-issue_identifier={entry.issue_identifier}
                        >
                          Activity
                        </button>
                      </div>
                    </td>
                    <td class="numeric table-cell-small"><%= format_runtime_and_turns(entry.started_at, entry.turn_count, @now) %></td>
                    <td class="table-cell-small">
                      <div class="detail-stack">
                        <span
                          class="event-text"
                          title={entry.last_message || to_string(entry.last_event || "n/a")}
                        ><%= entry.last_message || to_string(entry.last_event || "n/a") %></span>
                        <span class="muted event-meta">
                          <%= entry.last_event || "n/a" %>
                          <%= if entry.last_event_at do %>
                            · <span class="mono numeric"><%= entry.last_event_at %></span>
                          <% end %>
                        </span>
                      </div>
                    </td>
                    <td class="table-cell-small">
                      <div class="token-stack numeric">
                        <span>Total: <%= format_int(entry.tokens.total_tokens) %></span>
                        <span class="muted">In <%= format_int(entry.tokens.input_tokens) %> / Out <%= format_int(entry.tokens.output_tokens) %></span>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>

        <section class="section-card section-retry">
          <div class="section-header">
            <div>
              <h2 class="section-title">Retry queue</h2>
            </div>
          </div>

          <%= if @payload.retrying == [] do %>
            <p class="empty-state">No issues are currently backing off.</p>
          <% else %>
            <div class="table-wrap">
              <table class="data-table" style="min-width: 680px;">
                <thead>
                  <tr>
                    <th>Issue</th>
                    <th>Attempt</th>
                    <th>Due at</th>
                    <th>Error</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @payload.retrying}>
                    <td>
                      <div class="issue-stack">
                        <span class="issue-id"><%= entry.issue_identifier %></span>
                        <a class="issue-link" href={"/api/v1/#{entry.issue_identifier}"}>JSON details</a>
                      </div>
                    </td>
                    <td><%= entry.attempt %></td>
                    <td class="mono"><%= entry.due_at || "n/a" %></td>
                    <td><%= entry.error || "n/a" %></td>
                  </tr>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>

        <section class="section-card section-rate-limits">
          <div class="section-header">
            <div>
              <h2 class="section-title">Rate limits</h2>
            </div>
          </div>

          <pre class="code-panel code-panel-plain"><%= pretty_value(@payload.rate_limits) %></pre>
        </section>

        <section class="section-card section-project-tickets">
          <div class="section-header">
            <div>
              <h2 class="section-title">
                <a
                  :if={linear_project_issues_url()}
                  class="issue-link"
                  href={linear_project_issues_url()}
                  target="_blank"
                  rel="noreferrer"
                >
                  Project tickets
                </a>
                <%= if !linear_project_issues_url() do %>
                  Project tickets
                <% end %>
              </h2>
            </div>
          </div>

          <p :if={@project_tickets_error} class="empty-state"><%= @project_tickets_error %></p>

          <%= if @project_tickets == [] do %>
            <p class="empty-state">No project tickets found.</p>
          <% else %>
            <div class="table-wrap">
              <table class="data-table" style="min-width: 1060px;">
                <thead>
                  <tr>
                    <th>Issue</th>
                    <th>State</th>
                    <th>Updated</th>
                    <th>Linear</th>
                    <th>Resource</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  <%= for {state_name, entries} <- grouped_project_tickets(@project_tickets) do %>
                    <tr>
                      <td colspan="6">
                        <span class={state_badge_class(state_name)}><%= state_name %></span>
                      </td>
                    </tr>

                    <%= for entry <- entries do %>
                      <tr>
                        <td>
                          <div class="detail-stack">
                            <span class="issue-id"><%= project_ticket_identifier(entry) %></span>
                            <a
                              :if={project_ticket_url(entry)}
                              class="issue-link"
                              href={project_ticket_url(entry)}
                              target="_blank"
                              rel="noreferrer"
                            >
                              <%= project_ticket_title(entry) %>
                            </a>
                            <span :if={!project_ticket_url(entry)} class="muted"><%= project_ticket_title(entry) %></span>
                          </div>
                        </td>
                        <td>
                          <%= if project_ticket_editable?(entry) do %>
                            <button
                              type="button"
                              class={"ticket-status-button " <> state_badge_class(project_ticket_state(entry))}
                              phx-click="open_ticket_status_editor"
                              phx-value-issue_id={project_ticket_id(entry)}
                              phx-value-current_state={project_ticket_state(entry)}
                            >
                              <%= project_ticket_state(entry) %>
                            </button>
                          <% else %>
                            <span class={state_badge_class(project_ticket_state(entry))}>
                              <%= project_ticket_state(entry) %>
                            </span>
                          <% end %>
                        </td>
                        <td class="mono numeric project-updated-small"><%= format_project_ticket_updated_at(project_ticket_updated_at(entry)) %></td>
                        <td>
                          <a
                            :if={project_ticket_url(entry)}
                            class="issue-link project-link-action"
                            href={project_ticket_url(entry)}
                            target="_blank"
                            rel="noreferrer"
                          >
                            Open
                          </a>
                          <span :if={!project_ticket_url(entry)} class="muted project-link-action">n/a</span>
                        </td>
                        <td>
                          <a
                            :if={project_ticket_latest_resource_url(entry)}
                            class="issue-link project-link-action"
                            href={project_ticket_latest_resource_url(entry)}
                            title={project_ticket_latest_resource_title(entry)}
                            target="_blank"
                            rel="noreferrer"
                          >
                            Resource
                          </a>
                          <span :if={!project_ticket_latest_resource_url(entry)} class="muted project-link-action">n/a</span>
                        </td>
                        <td>
                          <%= if project_ticket_editable?(entry) do %>
                            <button
                              type="button"
                              class="subtle-button"
                              phx-click="open_ticket_comment_editor"
                              phx-value-issue_id={project_ticket_id(entry)}
                            >
                              Comment
                            </button>
                          <% else %>
                            <span class="muted">n/a</span>
                          <% end %>
                        </td>
                      </tr>
                      <tr
                        :if={ticket_editor_open?(@ticket_editor, project_ticket_id(entry), "status")}
                        class="ticket-editor-row"
                      >
                        <td colspan="6">
                          <form
                            class="ticket-editor-popover"
                            phx-change="ticket_status_change"
                            phx-submit="submit_ticket_status"
                          >
                            <input type="hidden" name="issue_id" value={project_ticket_id(entry)} />
                            <div class="ticket-editor-body ticket-editor-body-inline">
                              <label class="ticket-editor-label" for={"ticket-status-" <> project_ticket_id(entry)}>
                                Status
                              </label>
                              <select
                                id={"ticket-status-" <> project_ticket_id(entry)}
                                class="ticket-editor-input ticket-editor-select"
                                name="status"
                              >
                                <option value="">Select status</option>
                                <option
                                  :for={status <- @ticket_status_options}
                                  value={status}
                                  selected={status == ticket_status_draft(@ticket_status_drafts, project_ticket_id(entry), project_ticket_state(entry))}
                                >
                                  <%= status %>
                                </option>
                              </select>
                              <div class="ticket-editor-actions">
                                <button type="submit">Save</button>
                                <button type="button" class="secondary" phx-click="close_ticket_editor">
                                  Cancel
                                </button>
                              </div>
                            </div>
                          </form>
                        </td>
                      </tr>
                      <tr
                        :if={ticket_editor_open?(@ticket_editor, project_ticket_id(entry), "comment")}
                        class="ticket-editor-row"
                      >
                        <td colspan="6">
                          <form
                            class="ticket-editor-popover"
                            phx-change="ticket_comment_change"
                            phx-submit="submit_ticket_comment"
                          >
                            <input type="hidden" name="issue_id" value={project_ticket_id(entry)} />
                            <div class="ticket-editor-body ticket-editor-body-inline">
                              <label class="ticket-editor-label" for={"ticket-comment-" <> project_ticket_id(entry)}>
                                Comment
                              </label>
                              <input
                                type="text"
                                id={"ticket-comment-" <> project_ticket_id(entry)}
                                class="ticket-editor-input ticket-editor-comment-input"
                                name="comment_body"
                                placeholder="Write a short update..."
                                value={ticket_comment_draft(@ticket_comment_drafts, project_ticket_id(entry), "")}
                              />
                              <div class="ticket-editor-actions">
                                <button type="submit">Post</button>
                                <button type="button" class="secondary" phx-click="close_ticket_editor">
                                  Cancel
                                </button>
                              </div>
                            </div>
                          </form>
                        </td>
                      </tr>
                    <% end %>
                  <% end %>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>

        <section class="section-card section-inactive">
          <div class="section-header">
            <div>
              <h2 class="section-title">Inactive sessions</h2>
            </div>
          </div>

          <%= if @payload.inactive_sessions == [] do %>
            <p class="empty-state">No inactive sessions yet.</p>
          <% else %>
            <div class="table-wrap">
              <table class="data-table data-table-running data-table-running-main">
                <colgroup>
                  <col style="width: 12rem;" />
                  <col style="width: 8rem;" />
                  <col style="width: 7.5rem;" />
                  <col style="width: 8.5rem;" />
                  <col style="width: 10rem;" />
                  <col />
                  <col style="width: 10rem;" />
                </colgroup>
                <thead>
                  <tr>
                    <th>Issue</th>
                    <th>State</th>
                    <th>Session</th>
                    <th>Runtime / turns</th>
                    <th>Ended</th>
                    <th>Result</th>
                    <th>Tokens</th>
                  </tr>
                </thead>
                <tbody>
                  <tr :for={entry <- @payload.inactive_sessions}>
                    <td>
                      <div class="issue-stack">
                        <span class="issue-id"><%= entry.issue_identifier || entry.issue_id || "n/a" %></span>
                        <a :if={entry.issue_identifier} class="issue-link" href={"/api/v1/#{entry.issue_identifier}"}>
                          JSON details
                        </a>
                      </div>
                    </td>
                    <td>
                      <span class={state_badge_class(entry.state || "inactive")}>
                        <%= entry.state || "n/a" %>
                      </span>
                    </td>
                    <td>
                      <div class="session-stack">
                        <%= if entry.session_id do %>
                          <button
                            type="button"
                            class="subtle-button session-action-button"
                            data-label="Copy ID"
                            data-copy={entry.session_id}
                            onclick="navigator.clipboard.writeText(this.dataset.copy); this.textContent = 'Copied'; clearTimeout(this._copyTimer); this._copyTimer = setTimeout(() => { this.textContent = this.dataset.label }, 1200);"
                          >
                            Copy ID
                          </button>
                        <% else %>
                          <span class="muted">n/a</span>
                        <% end %>

                        <button
                          :if={entry.event_stream_id}
                          type="button"
                          class="subtle-button session-action-button"
                          phx-click="open_activity"
                          phx-value-stream_id={entry.event_stream_id}
                          phx-value-issue_identifier={entry.issue_identifier || entry.issue_id || "unknown issue"}
                        >
                          Activity
                        </button>
                      </div>
                    </td>
                    <td class="numeric table-cell-small">
                      <%= format_inactive_runtime_and_turns(entry.runtime_seconds, entry.turn_count) %>
                    </td>
                    <td class="mono numeric table-cell-small"><%= entry.ended_at || "n/a" %></td>
                    <td class="table-cell-small">
                      <div class="detail-stack">
                        <span class="event-text" title={format_stop_reason(entry.stop_reason)}>
                          <%= format_stop_reason(entry.stop_reason) %>
                        </span>
                        <span class="muted event-meta">
                          <%= entry.last_message || to_string(entry.last_event || "n/a") %>
                        </span>
                      </div>
                    </td>
                    <td class="table-cell-small">
                      <div class="token-stack numeric">
                        <span>Total: <%= format_int(entry.tokens.total_tokens) %></span>
                        <span class="muted">In <%= format_int(entry.tokens.input_tokens) %> / Out <%= format_int(entry.tokens.output_tokens) %></span>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          <% end %>
        </section>
      <% end %>

      <%= if @activity_drawer_open do %>
        <button
          type="button"
          class="activity-drawer-backdrop"
          style="position: fixed; inset: 0; z-index: 70; border: 0; border-radius: 0; padding: 0; margin: 0; background: rgba(22, 24, 35, 0.36);"
          phx-click="close_activity"
          aria-label="Close session activity drawer"
        >
        </button>

        <aside
          class="activity-drawer"
          style="position: fixed; top: 0; right: 0; bottom: 0; z-index: 80; width: min(38rem, 100vw); height: 100vh; display: flex; flex-direction: column; background: rgba(255, 255, 255, 0.97); border-left: 1px solid var(--line-strong); box-shadow: -18px 0 40px rgba(15, 23, 42, 0.18);"
          role="dialog"
          aria-modal="true"
          aria-label="Session activity drawer"
        >
          <header class="activity-drawer-header">
            <div>
              <p class="eyebrow">Session activity</p>
              <h2 class="section-title"><%= @activity_issue_identifier %></h2>
              <p class="section-copy mono"><%= @activity_stream_id %></p>
            </div>
            <div class="activity-drawer-controls">
              <div class="activity-mode-toggle" role="tablist" aria-label="Activity feed mode">
                <button
                  type="button"
                  class={activity_mode_button_class(@activity_mode == "human")}
                  phx-click="set_activity_mode"
                  phx-value-mode="human"
                >
                  Human actions
                </button>
                <button
                  type="button"
                  class={activity_mode_button_class(@activity_mode == "readable")}
                  phx-click="set_activity_mode"
                  phx-value-mode="readable"
                >
                  Readable
                </button>
                <button
                  type="button"
                  class={activity_mode_button_class(@activity_mode == "raw")}
                  phx-click="set_activity_mode"
                  phx-value-mode="raw"
                >
                  Raw
                </button>
              </div>
              <button type="button" class="secondary" phx-click="close_activity">Close</button>
            </div>
          </header>

          <section class="activity-drawer-body">
            <%= if @activity_error do %>
              <p class="empty-state"><%= @activity_error %></p>
            <% else %>
              <%= if visible_activity_events(@activity_mode, @activity_events_human, @activity_events_readable, @activity_events_raw) == [] do %>
                <%= if @activity_mode == "human" do %>
                  <p class="empty-state">No human actions yet. Switch to Readable or Raw to inspect all session events.</p>
                <% else %>
                  <p class="empty-state">No session events yet.</p>
                <% end %>
              <% else %>
                <ol class="activity-list">
                  <%= if @activity_mode == "human" do %>
                    <%= for {event, index} <- Enum.with_index(@activity_events_human) do %>
                      <% event_key = event.key || "human-event-#{index}" %>
                      <% expanded = MapSet.member?(@activity_expanded_keys, event_key) %>

                      <li class={if expanded, do: "activity-item activity-item-expanded", else: "activity-item"}>
                        <button
                          type="button"
                          class="activity-item-button"
                          phx-click="toggle_activity_detail"
                          phx-value-key={event_key}
                          aria-expanded={to_string(expanded)}
                        >
                          <div class="activity-item-head">
                            <div class="activity-item-label-wrap">
                              <span class="state-badge state-badge-active"><%= event.label || "Human Action" %></span>
                            </div>
                            <span class="muted mono" title={event.at_iso || "n/a"}>
                              <%= event.at_relative || "n/a" %>
                            </span>
                          </div>
                          <p class="activity-item-message"><%= event.message || "n/a" %></p>
                          <p
                            class="activity-item-meta muted mono"
                            title={"session " <> (event.session_id || "n/a")}
                          >
                            session <%= event.session_short || "n/a" %> · turn <%= event.turn_count || 0 %>
                          </p>
                        </button>

                        <div :if={expanded and is_map(event.details)} class="activity-item-details">
                          <pre class="code-panel"><%= pretty_value(event.details) %></pre>
                        </div>
                      </li>
                    <% end %>
                  <% else %>
                    <%= if @activity_mode == "readable" do %>
                    <li :for={event <- @activity_events_readable} class="activity-item">
                      <div class="activity-item-head">
                        <div class="activity-item-label-wrap">
                          <span class="state-badge"><%= event.label || "event" %></span>
                          <span :if={event.count && event.count > 1} class="activity-count">x<%= event.count %></span>
                        </div>
                        <span class="muted mono" title={event.at_iso || "n/a"}>
                          <%= event.at_relative || "n/a" %>
                        </span>
                      </div>
                      <p class="activity-item-message"><%= event.message || "n/a" %></p>
                      <p
                        class="activity-item-meta muted mono"
                        title={"session " <> (event.session_id || "n/a")}
                      >
                        session <%= event.session_short || "n/a" %> · turn <%= event.turn_count || 0 %>
                      </p>
                    </li>
                    <% else %>
                    <li :for={event <- @activity_events_raw} class="activity-item">
                      <div class="activity-item-head">
                        <span class="state-badge"><%= event.label || event.event || "event" %></span>
                        <span class="muted mono" title={event.at_iso || "n/a"}>
                          <%= event.at_relative || "n/a" %>
                        </span>
                      </div>
                      <p class="activity-item-message"><%= event.message || "n/a" %></p>
                      <p
                        class="activity-item-meta muted mono"
                        title={"session " <> (event.session_id || "n/a")}
                      >
                        session <%= event.session_short || "n/a" %> · turn <%= event.turn_count || 0 %>
                      </p>
                    </li>
                    <% end %>
                  <% end %>
                </ol>
              <% end %>
            <% end %>
          </section>
        </aside>
      <% end %>
    </section>
    """
  end

  defp load_payload do
    Presenter.state_payload(orchestrator(), snapshot_timeout_ms())
  end

  defp orchestrator do
    Endpoint.config(:orchestrator) || SymphonyElixir.Orchestrator
  end

  defp snapshot_timeout_ms do
    Endpoint.config(:snapshot_timeout_ms) || 15_000
  end

  defp completed_runtime_seconds(payload) do
    payload.codex_totals.seconds_running || 0
  end

  defp total_runtime_seconds(payload, now) do
    completed_runtime_seconds(payload) +
      Enum.reduce(payload.running, 0, fn entry, total ->
        total + runtime_seconds_from_started_at(entry.started_at, now)
      end)
  end

  defp format_runtime_and_turns(started_at, turn_count, now) when is_integer(turn_count) and turn_count > 0 do
    "#{format_runtime_seconds(runtime_seconds_from_started_at(started_at, now))} / #{turn_count}"
  end

  defp format_runtime_and_turns(started_at, _turn_count, now),
    do: format_runtime_seconds(runtime_seconds_from_started_at(started_at, now))

  defp format_inactive_runtime_and_turns(runtime_seconds, turn_count)
       when is_integer(turn_count) and turn_count > 0 do
    "#{format_runtime_seconds(runtime_seconds)} / #{turn_count}"
  end

  defp format_inactive_runtime_and_turns(runtime_seconds, _turn_count),
    do: format_runtime_seconds(runtime_seconds)

  defp format_runtime_seconds(seconds) when is_number(seconds) do
    whole_seconds = max(trunc(seconds), 0)
    mins = div(whole_seconds, 60)
    secs = rem(whole_seconds, 60)
    "#{mins}m #{secs}s"
  end

  defp runtime_seconds_from_started_at(%DateTime{} = started_at, %DateTime{} = now) do
    DateTime.diff(now, started_at, :second)
  end

  defp runtime_seconds_from_started_at(started_at, %DateTime{} = now) when is_binary(started_at) do
    case DateTime.from_iso8601(started_at) do
      {:ok, parsed, _offset} -> runtime_seconds_from_started_at(parsed, now)
      _ -> 0
    end
  end

  defp runtime_seconds_from_started_at(_started_at, _now), do: 0

  defp format_int(value) when is_integer(value) do
    value
    |> Integer.to_string()
    |> String.reverse()
    |> String.replace(~r/.{3}(?=.)/, "\\0,")
    |> String.reverse()
  end

  defp format_int(_value), do: "n/a"

  defp state_badge_class(state) do
    base = "state-badge"
    normalized = state |> to_string() |> String.downcase()

    cond do
      String.contains?(normalized, "human review") -> "#{base} state-badge-active"
      String.contains?(normalized, ["progress", "running", "active"]) -> "#{base} state-badge-active"
      String.contains?(normalized, ["blocked", "error", "failed"]) -> "#{base} state-badge-danger"
      String.contains?(normalized, ["todo", "queued", "pending", "retry"]) -> "#{base} state-badge-warning"
      true -> base
    end
  end

  defp format_stop_reason(nil), do: "n/a"

  defp format_stop_reason(reason) when is_binary(reason) do
    reason
    |> String.replace("_", " ")
    |> String.trim()
  end

  defp format_stop_reason(reason), do: to_string(reason)

  defp refresh_project_tickets(socket) do
    case load_project_tickets() do
      {:ok, tickets} ->
        socket
        |> assign(:project_tickets, tickets)
        |> assign(:project_tickets_error, nil)
        |> assign(:project_tickets_fetched_at, DateTime.utc_now())

      {:error, reason} ->
        socket
        |> assign(:project_tickets_error, format_project_tickets_error(reason))
        |> assign(:project_tickets_fetched_at, DateTime.utc_now())
    end
  end

  defp load_project_tickets do
    case project_tickets_fetcher().(@project_tickets_limit) do
      {:ok, issues} when is_list(issues) ->
        {:ok,
         issues
         |> Enum.filter(&match?(%Issue{}, &1))
         |> Enum.take(@project_tickets_limit)
         |> Enum.sort(&project_ticket_before?/2)}

      {:error, reason} ->
        {:error, reason}

      other ->
        {:error, {:unexpected_project_tickets_payload, other}}
    end
  end

  defp grouped_project_tickets(tickets) when is_list(tickets) do
    tickets
    |> Enum.chunk_by(&project_ticket_state_key/1)
    |> Enum.map(fn
      [first | _] = entries -> {project_ticket_state(first), entries}
      [] -> {"Unknown", []}
    end)
  end

  defp grouped_project_tickets(_tickets), do: []

  defp project_ticket_before?(left, right) do
    left_state = project_ticket_state_key(left)
    right_state = project_ticket_state_key(right)

    cond do
      left_state < right_state ->
        true

      left_state > right_state ->
        false

      project_ticket_updated_sort_value(left) > project_ticket_updated_sort_value(right) ->
        true

      project_ticket_updated_sort_value(left) < project_ticket_updated_sort_value(right) ->
        false

      true ->
        project_ticket_identifier_sort_value(left) <= project_ticket_identifier_sort_value(right)
    end
  end

  defp project_ticket_updated_sort_value(%Issue{updated_at: %DateTime{} = updated_at}) do
    DateTime.to_unix(updated_at, :microsecond)
  end

  defp project_ticket_updated_sort_value(_issue), do: -1

  defp project_ticket_state_key(%Issue{state: state}) when is_binary(state) do
    state
    |> String.trim()
    |> case do
      "" -> "unknown"
      value -> String.downcase(value)
    end
  end

  defp project_ticket_state_key(_issue), do: "unknown"

  defp project_ticket_identifier_sort_value(%Issue{} = issue) do
    issue
    |> project_ticket_identifier()
    |> String.downcase()
  end

  defp project_ticket_identifier_sort_value(_issue), do: "zzzzzzzz"

  defp project_ticket_identifier(%Issue{identifier: identifier}) when is_binary(identifier) and identifier != "",
    do: identifier

  defp project_ticket_identifier(%Issue{id: id}) when is_binary(id) and id != "", do: id
  defp project_ticket_identifier(_issue), do: "n/a"

  defp project_ticket_title(%Issue{title: title}) when is_binary(title) and title != "", do: title
  defp project_ticket_title(_issue), do: "No title"

  defp project_ticket_state(%Issue{state: state}) when is_binary(state) do
    case String.trim(state) do
      "" -> "Unknown"
      normalized -> normalized
    end
  end

  defp project_ticket_state(_issue), do: "Unknown"

  defp project_ticket_id(%Issue{id: id}) when is_binary(id) and id != "", do: id
  defp project_ticket_id(_issue), do: nil

  defp project_ticket_editable?(%Issue{} = issue), do: is_binary(project_ticket_id(issue))
  defp project_ticket_editable?(_issue), do: false

  defp project_ticket_url(%Issue{url: url}) when is_binary(url) and url != "", do: url
  defp project_ticket_url(_issue), do: nil

  defp project_ticket_latest_resource_url(%Issue{latest_resource_url: url})
       when is_binary(url) and url != "",
       do: url

  defp project_ticket_latest_resource_url(_issue), do: nil

  defp project_ticket_latest_resource_title(%Issue{latest_resource_title: title})
       when is_binary(title) and title != "",
       do: title

  defp project_ticket_latest_resource_title(_issue), do: "Latest resource"

  defp project_ticket_updated_at(%Issue{updated_at: %DateTime{} = updated_at}), do: updated_at
  defp project_ticket_updated_at(_issue), do: nil

  defp format_project_ticket_updated_at(%DateTime{} = updated_at),
    do: DateTime.truncate(updated_at, :second) |> DateTime.to_iso8601()

  defp format_project_ticket_updated_at(_updated_at), do: "n/a"

  defp format_project_tickets_error(reason) do
    "Project tickets refresh failed: #{inspect(reason)}"
  end

  defp find_project_ticket(tickets, issue_id) when is_list(tickets) and is_binary(issue_id) do
    Enum.find(tickets, fn
      %Issue{id: ^issue_id} -> true
      _ -> false
    end)
  end

  defp find_project_ticket(_tickets, _issue_id), do: nil

  defp submit_ticket_status_update(socket, %Issue{} = issue, status) do
    case issue_state_updater().(issue.id, status) do
      :ok ->
        {:noreply,
         socket
         |> assign(:ticket_editor, nil)
         |> refresh_project_tickets()
         |> put_flash(:info, "Updated #{project_ticket_identifier(issue)} to #{status}.")}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed to update status: #{inspect(reason)}")}
    end
  end

  defp submit_ticket_comment(socket, %Issue{} = issue, body) do
    case issue_comment_creator().(issue.id, body) do
      :ok ->
        {:noreply,
         socket
         |> assign(:ticket_editor, nil)
         |> put_ticket_comment_draft(issue.id, "")
         |> refresh_project_tickets()
         |> put_flash(:info, "Added comment to #{project_ticket_identifier(issue)}.")}

      {:error, reason} ->
        {:noreply, put_flash(socket, :error, "Failed to add comment: #{inspect(reason)}")}
    end
  end

  defp ticket_editor_open?(%{issue_id: issue_id, mode: mode}, issue_id, mode), do: true
  defp ticket_editor_open?(_editor, _issue_id, _mode), do: false

  defp ticket_status_draft(drafts, issue_id, fallback)
       when is_map(drafts) and is_binary(issue_id) and is_binary(fallback) do
    Map.get(drafts, issue_id, fallback)
  end

  defp ticket_status_draft(_drafts, _issue_id, fallback) when is_binary(fallback), do: fallback
  defp ticket_status_draft(_drafts, _issue_id, _fallback), do: ""

  defp ticket_comment_draft(drafts, issue_id, fallback)
       when is_map(drafts) and is_binary(issue_id) and is_binary(fallback) do
    Map.get(drafts, issue_id, fallback)
  end

  defp ticket_comment_draft(_drafts, _issue_id, fallback) when is_binary(fallback), do: fallback
  defp ticket_comment_draft(_drafts, _issue_id, _fallback), do: ""

  defp put_ticket_status_draft(socket, issue_id, status)
       when is_binary(issue_id) and is_binary(status) do
    current = socket.assigns.ticket_status_drafts || %{}
    assign(socket, :ticket_status_drafts, Map.put(current, issue_id, status))
  end

  defp put_ticket_comment_draft(socket, issue_id, body)
       when is_binary(issue_id) and is_binary(body) do
    current = socket.assigns.ticket_comment_drafts || %{}
    assign(socket, :ticket_comment_drafts, Map.put(current, issue_id, body))
  end

  defp valid_ticket_status?(allowed_statuses, status)
       when is_list(allowed_statuses) and is_binary(status) do
    normalized = String.downcase(status)
    Enum.any?(allowed_statuses, &(String.downcase(&1) == normalized))
  end

  defp valid_ticket_status?(_allowed_statuses, _status), do: false

  defp normalize_status_draft(value) when is_binary(value), do: String.trim(value)
  defp normalize_status_draft(_value), do: ""

  defp ticket_status_options do
    ["Backlog"] ++
      Config.linear_active_states() ++ ["Human Review", "Merging", "Rework"] ++ Config.linear_terminal_states()
    |> Enum.map(&normalize_status_draft/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.reduce({MapSet.new(), []}, fn status, {seen, acc} ->
      key = String.downcase(status)

      if MapSet.member?(seen, key) do
        {seen, acc}
      else
        {MapSet.put(seen, key), acc ++ [status]}
      end
    end)
    |> elem(1)
  end

  defp linear_project_issues_url do
    case Config.linear_project_slug() do
      project_slug when is_binary(project_slug) and project_slug != "" ->
        "https://linear.app/validation/project/#{project_slug}/issues"

      _ ->
        nil
    end
  end

  defp project_tickets_fetcher do
    Endpoint.config(:project_tickets_fetcher) || &Tracker.fetch_project_issues/1
  end

  defp issue_state_updater do
    Endpoint.config(:issue_state_updater) || &Tracker.update_issue_state/2
  end

  defp issue_comment_creator do
    Endpoint.config(:issue_comment_creator) || &Tracker.create_comment/2
  end

  defp maybe_refresh_activity_drawer(socket) do
    if socket.assigns.activity_drawer_open and is_binary(socket.assigns.activity_stream_id) do
      now = socket.assigns.now || DateTime.utc_now()

      {raw_events, readable_events, human_events, error} =
        load_session_events(socket.assigns.activity_stream_id, now)

      {socket, merged_human_events} =
        update_human_ledger(socket, socket.assigns.activity_stream_id, human_events)

      socket
      |> assign(:activity_events_raw, raw_events)
      |> assign(:activity_events_readable, readable_events)
      |> assign(:activity_events_human, merged_human_events)
      |> assign(:activity_error, error)
    else
      socket
    end
  end

  defp load_session_events(stream_id, %DateTime{} = now)
       when is_binary(stream_id) and stream_id != "" do
    case Presenter.session_events_payload(
           stream_id,
           @session_events_limit,
           orchestrator(),
           snapshot_timeout_ms()
         ) do
      {:ok, %{events: events}} when is_list(events) ->
        {
          SessionActivityFormatter.raw_events(events, now),
          SessionActivityFormatter.readable_events(events, now),
          SessionActivityFormatter.human_action_events(events, now),
          nil
        }

      {:error, :session_not_found} ->
        {[], [], [], "Session timeline not found."}

      _ ->
        {[], [], [], "Session timeline unavailable."}
    end
  end

  defp load_session_events(_stream_id, _now), do: {[], [], [], "Session timeline unavailable."}

  defp update_human_ledger(socket, stream_id, new_events)
       when is_binary(stream_id) and is_list(new_events) do
    ledger = socket.assigns.activity_human_ledger || %{}
    existing = Map.get(ledger, stream_id, [])

    merged =
      merge_human_ledger_events(existing, new_events)
      |> Enum.take(-@session_human_ledger_limit)

    {
      assign(socket, :activity_human_ledger, Map.put(ledger, stream_id, merged)),
      merged
    }
  end

  defp update_human_ledger(socket, _stream_id, _new_events), do: {socket, []}

  defp merge_human_ledger_events(existing, incoming) when is_list(existing) and is_list(incoming) do
    (existing ++ incoming)
    |> Enum.reduce(%{}, fn event, acc ->
      Map.put(acc, human_event_identity(event), event)
    end)
    |> Map.values()
    |> Enum.sort_by(&human_event_sort_key/1)
  end

  defp merge_human_ledger_events(_existing, incoming) when is_list(incoming), do: incoming
  defp merge_human_ledger_events(_existing, _incoming), do: []

  defp human_event_identity(%{} = event) do
    Map.get(event, :key) || [Map.get(event, :at_iso), Map.get(event, :session_id), Map.get(event, :turn_count), Map.get(event, :message)]
  end

  defp human_event_identity(event), do: event

  defp human_event_sort_key(%{} = event) do
    {
      Map.get(event, :at_iso) || "",
      Map.get(event, :turn_count) || 0,
      Map.get(event, :key) || ""
    }
  end

  defp human_event_sort_key(_event), do: {"", 0, ""}

  defp visible_activity_events("human", human_events, _readable_events, _raw_events), do: human_events
  defp visible_activity_events("raw", _human_events, _readable_events, raw_events), do: raw_events
  defp visible_activity_events(_mode, _human_events, readable_events, _raw_events), do: readable_events

  defp activity_mode_button_class(true), do: "activity-mode-button activity-mode-button-active"
  defp activity_mode_button_class(false), do: "activity-mode-button"

  defp close_activity_drawer(socket) do
    socket
    |> assign(:activity_drawer_open, false)
    |> assign(:activity_mode, "human")
    |> assign(:activity_stream_id, nil)
    |> assign(:activity_issue_identifier, nil)
    |> assign(:activity_events_raw, [])
    |> assign(:activity_events_readable, [])
    |> assign(:activity_events_human, [])
    |> assign(:activity_expanded_keys, MapSet.new())
    |> assign(:activity_error, nil)
  end

  defp schedule_runtime_tick do
    Process.send_after(self(), :runtime_tick, @runtime_tick_ms)
  end

  defp pretty_value(nil), do: "n/a"
  defp pretty_value(value), do: inspect(value, pretty: true, limit: :infinity)
end
