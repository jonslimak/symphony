defmodule SymphonyElixirWeb.SessionActivityFormatter do
  @moduledoc """
  Formats session activity rows for dashboard display.
  """

  @stream_window_seconds 2
  @max_stream_message_chars 800
  @min_agent_message_chars 24

  @stream_label_map %{
    "agent message content streaming" => {"agent_message", "Agent Message"},
    "agent message streaming" => {"agent_message", "Agent Message"},
    "reasoning content streaming" => {"reasoning", "Reasoning"},
    "reasoning streaming" => {"reasoning", "Reasoning"},
    "command output streaming" => {"command_output", "Command Output"},
    "plan streaming" => {"plan", "Plan"}
  }

  @stream_prefixes Map.keys(@stream_label_map)

  @spec readable_events([map()], DateTime.t()) :: [map()]
  def readable_events(events, %DateTime{} = now) when is_list(events) do
    events
    |> Enum.reduce([], fn raw_event, acc ->
      event = normalize_event(raw_event, now)

      case stream_descriptor(event.message) do
        {:stream, stream_key, stream_label, chunk} ->
          append_stream_event(acc, event, stream_key, stream_label, chunk, now)

        :non_stream ->
          append_non_stream_event(acc, event, now)
      end
    end)
    |> Enum.reverse()
    |> Enum.map(&strip_internal_fields/1)
  end

  @spec raw_events([map()], DateTime.t()) :: [map()]
  def raw_events(events, %DateTime{} = now) when is_list(events) do
    Enum.map(events, fn raw_event ->
      event = normalize_event(raw_event, now)

      %{
        at_iso: event.at_iso,
        at_relative: event.at_relative,
        label: event.label,
        event: event.event,
        message: event.message,
        session_id: event.session_id,
        session_short: short_session_id(event.session_id),
        turn_count: event.turn_count
      }
      |> maybe_put(:kind, event.kind)
      |> maybe_put(:category, event.category)
      |> maybe_put(:action, event.action)
      |> maybe_put(:details, event.details)
    end)
  end

  @spec human_action_events([map()], DateTime.t()) :: [map()]
  def human_action_events(events, %DateTime{} = now) when is_list(events) do
    explicit =
      events
      |> Enum.map(&normalize_event(&1, now))
      |> Enum.filter(&(&1.kind == "human_action"))
      |> Enum.map(&build_explicit_human_action/1)

    inferred =
      events
      |> readable_events(now)
      |> Enum.map(&infer_human_action_from_readable/1)
      |> Enum.reject(&is_nil/1)

    (explicit ++ inferred)
    |> dedupe_human_actions()
  end

  defp append_stream_event(acc, _event, _stream_key, _stream_label, "", _now), do: acc

  defp append_stream_event([last | rest], event, stream_key, stream_label, chunk, %DateTime{} = now) do
    cond do
      stream_mergeable?(last, event, stream_key) and last[:last_chunk] == chunk ->
        merged = update_event_time(last, event, now)
        [merged | rest]

      stream_mergeable?(last, event, stream_key) ->
        merged =
          last
          |> update_event_time(event, now)
          |> Map.put(:last_chunk, chunk)
          |> Map.put(:message, merge_stream_message(last.message, chunk))

        [merged | rest]

      true ->
        [build_stream_entry(event, stream_key, stream_label, chunk) | [last | rest]]
    end
  end

  defp append_stream_event([], event, stream_key, stream_label, chunk, _now) do
    [build_stream_entry(event, stream_key, stream_label, chunk)]
  end

  defp append_non_stream_event([last | rest], event, %DateTime{} = now) do
    if duplicate_non_stream?(last, event) do
      deduped =
        last
        |> update_event_time(event, now)
        |> Map.update(:count, 2, &(&1 + 1))

      [deduped | rest]
    else
      [build_non_stream_entry(event) | [last | rest]]
    end
  end

  defp append_non_stream_event([], event, _now), do: [build_non_stream_entry(event)]

  defp build_stream_entry(event, stream_key, stream_label, chunk) do
    %{
      kind: :stream,
      stream_key: stream_key,
      label: stream_label,
      message: chunk,
      at_iso: event.at_iso,
      at_dt: event.at_dt,
      at_relative: event.at_relative,
      session_id: event.session_id,
      session_short: short_session_id(event.session_id),
      turn_count: event.turn_count,
      count: 1,
      last_chunk: chunk
    }
  end

  defp build_non_stream_entry(event) do
    %{
      kind: :event,
      label: event.label,
      message: event.message,
      at_iso: event.at_iso,
      at_dt: event.at_dt,
      at_relative: event.at_relative,
      session_id: event.session_id,
      session_short: short_session_id(event.session_id),
      turn_count: event.turn_count,
      count: 1
    }
  end

  defp normalize_event(event, %DateTime{} = now) when is_map(event) do
    at_iso = event_value(event, :at)
    at_dt = parse_iso8601(at_iso)

    label =
      event_value(event, :label) ||
        event_value(event, :event) ||
        "event"

    message = event_value(event, :message) || "n/a"

    %{
      at_iso: at_iso,
      at_dt: at_dt,
      at_relative: relative_time(at_dt, now),
      event: event_value(event, :event),
      label: humanize_label(label),
      message: to_string(message),
      session_id: event_value(event, :session_id),
      turn_count: normalize_turn_count(event_value(event, :turn_count)),
      kind: normalize_string(event_value(event, :kind)),
      category: normalize_string(event_value(event, :category)),
      action: normalize_string(event_value(event, :action)),
      details: normalize_details(event_value(event, :details))
    }
  end

  defp normalize_turn_count(value) when is_integer(value), do: max(value, 0)

  defp normalize_turn_count(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {parsed, _rest} -> max(parsed, 0)
      _ -> 0
    end
  end

  defp normalize_turn_count(_value), do: 0

  defp stream_mergeable?(last, event, stream_key) do
    last[:kind] == :stream and
      last[:stream_key] == stream_key and
      last[:session_id] == event.session_id and
      last[:turn_count] == event.turn_count and
      within_stream_window?(last[:at_dt], event.at_dt)
  end

  defp duplicate_non_stream?(last, event) do
    last[:kind] == :event and
      last[:label] == event.label and
      last[:message] == event.message and
      last[:session_id] == event.session_id and
      last[:turn_count] == event.turn_count and
      same_second?(last[:at_dt], event.at_dt)
  end

  defp within_stream_window?(%DateTime{} = left, %DateTime{} = right) do
    delta = DateTime.diff(right, left, :second)
    delta >= 0 and delta <= @stream_window_seconds
  end

  defp within_stream_window?(_left, _right), do: false

  defp same_second?(%DateTime{} = left, %DateTime{} = right) do
    DateTime.to_unix(left, :second) == DateTime.to_unix(right, :second)
  end

  defp same_second?(_left, _right), do: false

  defp merge_stream_message(left, right) do
    (left <> right)
    |> String.slice(0, @max_stream_message_chars)
  end

  defp update_event_time(last, event, %DateTime{} = now) do
    at_dt = event.at_dt || last[:at_dt]
    at_iso = event.at_iso || last[:at_iso]

    last
    |> Map.put(:at_dt, at_dt)
    |> Map.put(:at_iso, at_iso)
    |> Map.put(:at_relative, relative_time(at_dt, now))
  end

  defp stream_descriptor(message) when is_binary(message) do
    downcased = String.downcase(message)

    Enum.find_value(@stream_prefixes, :non_stream, fn prefix ->
      if String.starts_with?(downcased, prefix <> ":") do
        {stream_key, stream_label} = Map.fetch!(@stream_label_map, prefix)
        chunk = message |> String.slice((byte_size(prefix) + 1)..-1//1) |> strip_single_leading_space()
        {:stream, stream_key, stream_label, chunk}
      else
        false
      end
    end)
  end

  defp stream_descriptor(_message), do: :non_stream

  defp strip_single_leading_space(" " <> rest), do: rest
  defp strip_single_leading_space(value), do: value

  defp short_session_id(session_id) when is_binary(session_id) do
    trimmed = String.trim(session_id)

    if String.length(trimmed) > 16 do
      "#{String.slice(trimmed, 0, 8)}…#{String.slice(trimmed, -6, 6)}"
    else
      trimmed
    end
  end

  defp short_session_id(_session_id), do: "n/a"

  defp relative_time(nil, _now), do: "n/a"

  defp relative_time(%DateTime{} = at, %DateTime{} = now) do
    seconds = max(DateTime.diff(now, at, :second), 0)

    cond do
      seconds < 5 -> "just now"
      seconds < 60 -> "#{seconds}s ago"
      seconds < 3600 -> "#{div(seconds, 60)}m ago"
      seconds < 86_400 -> "#{div(seconds, 3600)}h ago"
      true -> "#{div(seconds, 86_400)}d ago"
    end
  end

  defp humanize_label(value) when is_binary(value) do
    value
    |> String.replace("_", " ")
    |> String.split(" ", trim: true)
    |> Enum.map(&String.capitalize/1)
    |> Enum.join(" ")
  end

  defp humanize_label(value), do: value |> to_string() |> humanize_label()

  defp parse_iso8601(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp parse_iso8601(_value), do: nil

  defp normalize_string(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_string(_value), do: nil

  defp normalize_details(%{} = details), do: details
  defp normalize_details(_details), do: nil

  defp build_explicit_human_action(event) do
    %{
      key: human_event_key(event),
      label: human_action_label(event.action, event.category),
      message: event.message,
      at_iso: event.at_iso,
      at_relative: event.at_relative,
      session_id: event.session_id,
      session_short: short_session_id(event.session_id),
      turn_count: event.turn_count,
      category: event.category,
      action: event.action,
      details: event.details
    }
  end

  defp infer_human_action_from_readable(%{} = entry) do
    label = normalize_string(Map.get(entry, :label))
    message = normalize_string(Map.get(entry, :message))

    cond do
      is_nil(message) ->
        nil

      true ->
        command = extract_shell_command(message)

        cond do
          is_binary(command) ->
            inferred_command_human_action(entry, message, command)

          linear_tool_lifecycle_message?(message) ->
            inferred_linear_tool_human_action(entry, message)

          label == "Agent Message" and human_readable_agent_message?(message) ->
            inferred_agent_message_human_action(entry, message)

          true ->
            nil
        end
    end
  end

  defp infer_human_action_from_readable(_entry), do: nil

  defp inferred_linear_tool_human_action(entry, message) do
    action =
      if String.contains?(String.downcase(message), "completed"),
        do: "linear_tool_call_completed",
        else: "linear_tool_call_requested"

    build_inferred_human_action(entry, "Linear Tool", "linear", action, message, %{
      "source" => "inferred",
      "tool" => "linear_graphql"
    })
  end

  defp inferred_agent_message_human_action(entry, message) do
    build_inferred_human_action(entry, "Agent Message", "agent", "agent_message", message, %{
      "source" => "inferred"
    })
  end

  defp inferred_command_human_action(entry, message, command) do
    case classify_human_command(command) do
      nil ->
        nil

      %{label: label, category: category, action: action} ->
        build_inferred_human_action(entry, label, category, action, message, %{
          "source" => "inferred",
          "command" => command
        })
    end
  end

  defp build_inferred_human_action(entry, label, category, action, message, details) do
    event = %{
      at_iso: Map.get(entry, :at_iso),
      at_relative: Map.get(entry, :at_relative),
      session_id: Map.get(entry, :session_id),
      session_short: Map.get(entry, :session_short),
      turn_count: normalize_turn_count(Map.get(entry, :turn_count)),
      message: message,
      category: category,
      action: action
    }

    %{
      key: human_event_key(event),
      label: label,
      message: message,
      at_iso: event.at_iso,
      at_relative: event.at_relative,
      session_id: event.session_id,
      session_short: event.session_short || short_session_id(event.session_id),
      turn_count: event.turn_count,
      category: category,
      action: action,
      details: details
    }
  end

  defp extract_shell_command(message) when is_binary(message) do
    trimmed = String.trim(message)

    cond do
      String.starts_with?(trimmed, "/bin/zsh -lc ") ->
        trimmed
        |> String.replace_prefix("/bin/zsh -lc ", "")
        |> String.trim()
        |> normalize_string()

      String.starts_with?(trimmed, "gh ") or String.starts_with?(trimmed, "git ") ->
        trimmed

      true ->
        nil
    end
  end

  defp extract_shell_command(_message), do: nil

  defp classify_human_command(command) when is_binary(command) do
    normalized = String.downcase(command)

    cond do
      String.contains?(normalized, "gh pr ") ->
        %{label: "GitHub PR", category: "github", action: "github_pr_command"}

      String.contains?(normalized, "gh issue ") ->
        %{label: "GitHub Issue", category: "github", action: "github_issue_command"}

      String.contains?(normalized, "gh api ") and String.contains?(normalized, "/pulls/") ->
        %{label: "GitHub PR", category: "github", action: "github_pr_command"}

      String.contains?(normalized, "gh api ") and String.contains?(normalized, "/issues/") ->
        %{label: "GitHub Issue", category: "github", action: "github_issue_command"}

      String.contains?(normalized, "git commit") ->
        %{label: "Git Commit", category: "git", action: "git_commit"}

      String.contains?(normalized, "git push") ->
        %{label: "Git Push", category: "git", action: "git_push"}

      String.contains?(normalized, "git fetch") ->
        %{label: "Git Fetch", category: "git", action: "git_fetch"}

      String.contains?(normalized, "git status") ->
        %{label: "Git Status", category: "git", action: "git_status"}

      true ->
        nil
    end
  end

  defp classify_human_command(_command), do: nil

  defp linear_tool_lifecycle_message?(message) when is_binary(message) do
    downcased = String.downcase(message)

    String.contains?(downcased, "dynamic tool call completed (linear_graphql)") or
      String.contains?(downcased, "dynamic tool call requested (linear_graphql)")
  end

  defp linear_tool_lifecycle_message?(_message), do: false

  defp human_readable_agent_message?(message) when is_binary(message) do
    trimmed = String.trim(message)

    cond do
      String.length(trimmed) < @min_agent_message_chars ->
        false

      true ->
        words = String.split(trimmed, ~r/\s+/, trim: true)
        word_count = length(words)
        longest_word_len = words |> Enum.map(&String.length/1) |> Enum.max(fn -> 0 end)

        word_count >= 4 and longest_word_len <= 40
    end
  end

  defp human_readable_agent_message?(_message), do: false

  defp dedupe_human_actions(events) when is_list(events) do
    events
    |> Enum.reduce({MapSet.new(), []}, fn event, {seen, acc} ->
      key = human_event_dedupe_key(event)

      if MapSet.member?(seen, key) do
        {seen, acc}
      else
        {MapSet.put(seen, key), [event | acc]}
      end
    end)
    |> elem(1)
    |> Enum.reverse()
  end

  defp human_event_dedupe_key(%{} = event) do
    [
      Map.get(event, :at_iso),
      Map.get(event, :session_id),
      Map.get(event, :turn_count),
      Map.get(event, :message)
    ]
  end

  defp human_event_dedupe_key(event), do: event

  defp human_action_label("linear_comment_create", _category), do: "Linear Comment"
  defp human_action_label("linear_status_change", _category), do: "Linear Status"
  defp human_action_label("git_commit", _category), do: "Git Commit"
  defp human_action_label("git_push", _category), do: "Git Push"
  defp human_action_label("git_fetch", _category), do: "Git Fetch"
  defp human_action_label("git_status", _category), do: "Git Status"
  defp human_action_label("github_pr_create", _category), do: "GitHub PR"
  defp human_action_label("github_pr_command", _category), do: "GitHub PR"
  defp human_action_label("github_issue_command", _category), do: "GitHub Issue"
  defp human_action_label("linear_tool_call_completed", _category), do: "Linear Tool"
  defp human_action_label("linear_tool_call_requested", _category), do: "Linear Tool"
  defp human_action_label("agent_message", _category), do: "Agent Message"
  defp human_action_label("thread_status_changed", _category), do: "Status Changed"

  defp human_action_label(action, category) do
    case normalize_string(action) || normalize_string(category) do
      nil -> "Human Action"
      value -> humanize_label(value)
    end
  end

  defp human_event_key(event) do
    raw =
      [
        event.at_iso,
        event.session_id,
        event.turn_count,
        event.action,
        event.message
      ]
      |> Enum.map(&to_string_or_empty/1)
      |> Enum.join("|")

    "ha-" <> Integer.to_string(:erlang.phash2(raw))
  end

  defp to_string_or_empty(nil), do: ""
  defp to_string_or_empty(value), do: to_string(value)

  defp event_value(event, key) when is_map(event) do
    Map.get(event, key) || Map.get(event, Atom.to_string(key))
  end

  defp strip_internal_fields(entry) when is_map(entry) do
    entry
    |> Map.delete(:at_dt)
    |> Map.delete(:stream_key)
    |> Map.delete(:kind)
    |> Map.delete(:last_chunk)
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
