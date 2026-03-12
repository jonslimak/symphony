defmodule SymphonyElixir.RunRecordStoreTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.RunRecordStore

  setup do
    workspace_root =
      Path.join(
        System.tmp_dir!(),
        "symphony-run-record-store-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(workspace_root)
    write_workflow_file!(Workflow.workflow_file_path(), workspace_root: workspace_root)

    on_exit(fn ->
      File.rm_rf(workspace_root)
    end)

    {:ok,
     workspace_root: workspace_root,
     store_path: Path.join(workspace_root, ".symphony_observability/run_records/mt-501-stream.json")}
  end

  test "write and read round-trip a run record", %{store_path: store_path} do
    record = %{
      issue_identifier: "MT-501",
      event_stream_id: "mt-501-stream",
      final_status: "completed",
      next_action: "continuation",
      path_summary: ["Todo", "In Progress"],
      key_evidence: ["git_commit"]
    }

    assert :ok = RunRecordStore.write("mt-501-stream", record)
    assert File.exists?(store_path)
    assert {:ok, decoded} = RunRecordStore.read("mt-501-stream")
    assert decoded["issue_identifier"] == "MT-501"
    assert decoded["final_status"] == "completed"
    assert decoded["key_evidence"] == ["git_commit"]
  end

  test "read returns not_found when record file is missing" do
    assert {:error, :not_found} = RunRecordStore.read("missing-stream")
  end

  test "write returns an encode error for non-json-safe payload values" do
    assert {:error, {:encode_failed, _error}} =
             RunRecordStore.write("bad-stream", %{
               issue_identifier: "MT-502",
               event_stream_id: "bad-stream",
               bad_value: self()
             })
  end
end
