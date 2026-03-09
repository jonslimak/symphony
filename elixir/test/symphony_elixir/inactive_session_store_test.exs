defmodule SymphonyElixir.InactiveSessionStoreTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.InactiveSessionStore

  setup do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-inactive-session-store-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(workspace_root)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

    on_exit(fn ->
      File.rm_rf(workspace_root)
    end)

    {:ok, workspace_root: workspace_root, store_path: Path.join(workspace_root, ".symphony_observability/inactive_sessions.ndjson")}
  end

  test "append and read_recent returns latest entries first", %{store_path: store_path} do
    now = DateTime.utc_now() |> DateTime.truncate(:second)

    first_entry = %{
      issue_id: "issue-1",
      identifier: "MT-401",
      state: "Done",
      event_stream_id: "mt-401-stream",
      session_id: "thread-401",
      turn_count: 1,
      started_at: DateTime.add(now, -30, :second),
      ended_at: DateTime.add(now, -20, :second),
      stop_reason: "completed",
      runtime_seconds: 10,
      last_codex_timestamp: DateTime.add(now, -20, :second),
      last_codex_message: "first done",
      last_codex_event: :turn_completed,
      codex_input_tokens: 10,
      codex_output_tokens: 5,
      codex_total_tokens: 15
    }

    second_entry = %{
      issue_id: "issue-2",
      identifier: "MT-402",
      state: "Done",
      event_stream_id: "mt-402-stream",
      session_id: "thread-402",
      turn_count: 2,
      started_at: DateTime.add(now, -10, :second),
      ended_at: now,
      stop_reason: "completed",
      runtime_seconds: 10,
      last_codex_timestamp: now,
      last_codex_message: "second done",
      last_codex_event: :turn_completed,
      codex_input_tokens: 20,
      codex_output_tokens: 10,
      codex_total_tokens: 30
    }

    assert :ok = InactiveSessionStore.append(first_entry)
    assert :ok = InactiveSessionStore.append(second_entry)
    assert File.exists?(store_path)

    assert {:ok, [latest, older]} = InactiveSessionStore.read_recent(10)
    assert latest.identifier == "MT-402"
    assert older.identifier == "MT-401"
    assert %DateTime{} = latest.started_at
    assert %DateTime{} = older.ended_at
  end

  test "read_recent returns empty list when file is missing", %{store_path: store_path} do
    File.rm(store_path)
    assert {:ok, []} = InactiveSessionStore.read_recent(10)
  end

  test "read_recent skips malformed rows", %{store_path: store_path} do
    File.mkdir_p!(Path.dirname(store_path))

    File.write!(
      store_path,
      [
        "{not-json}\n",
        "{\"identifier\":\"MT-403\",\"turn_count\":3,\"runtime_seconds\":42,\"codex_input_tokens\":1,\"codex_output_tokens\":2,\"codex_total_tokens\":3}\n"
      ]
    )

    assert {:ok, [entry]} = InactiveSessionStore.read_recent(10)
    assert entry.identifier == "MT-403"
    assert entry.turn_count == 3
  end
end
