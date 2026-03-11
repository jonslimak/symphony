defmodule SymphonyElixir.Linear.Adapter do
  @moduledoc """
  Linear-backed tracker adapter.
  """

  @behaviour SymphonyElixir.Tracker

  alias SymphonyElixir.Linear.Client

  @create_comment_mutation """
  mutation SymphonyCreateComment($issueId: String!, $body: String!) {
    commentCreate(input: {issueId: $issueId, body: $body}) {
      success
    }
  }
  """

  @update_state_mutation """
  mutation SymphonyUpdateIssueState($issueId: String!, $stateId: String!) {
    issueUpdate(id: $issueId, input: {stateId: $stateId}) {
      success
    }
  }
  """

  @attach_github_pr_mutation """
  mutation SymphonyAttachGitHubPR($issueId: String!, $url: String!, $title: String) {
    attachmentLinkGitHubPR(issueId: $issueId, url: $url, title: $title) {
      success
    }
  }
  """

  @attach_url_mutation """
  mutation SymphonyAttachURL($issueId: String!, $url: String!, $title: String) {
    attachmentLinkURL(issueId: $issueId, url: $url, title: $title) {
      success
    }
  }
  """

  @state_lookup_query """
  query SymphonyResolveStateId($issueId: String!, $stateName: String!) {
    issue(id: $issueId) {
      team {
        states(filter: {name: {eq: $stateName}}, first: 1) {
          nodes {
            id
          }
        }
      }
    }
  }
  """

  @spec fetch_candidate_issues() :: {:ok, [term()]} | {:error, term()}
  def fetch_candidate_issues, do: client_module().fetch_candidate_issues()

  @spec fetch_issues_by_states([String.t()]) :: {:ok, [term()]} | {:error, term()}
  def fetch_issues_by_states(states), do: client_module().fetch_issues_by_states(states)

  @spec fetch_project_issues(pos_integer()) :: {:ok, [term()]} | {:error, term()}
  def fetch_project_issues(limit) when is_integer(limit) and limit > 0 do
    client_module().fetch_project_issues(limit)
  end

  @spec fetch_issue_states_by_ids([String.t()]) :: {:ok, [term()]} | {:error, term()}
  def fetch_issue_states_by_ids(issue_ids), do: client_module().fetch_issue_states_by_ids(issue_ids)

  @spec create_comment(String.t(), String.t()) :: :ok | {:error, term()}
  def create_comment(issue_id, body) when is_binary(issue_id) and is_binary(body) do
    with {:ok, response} <- client_module().graphql(@create_comment_mutation, %{issueId: issue_id, body: body}),
         true <- get_in(response, ["data", "commentCreate", "success"]) == true do
      :ok
    else
      false -> {:error, :comment_create_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :comment_create_failed}
    end
  end

  @spec update_issue_state(String.t(), String.t()) :: :ok | {:error, term()}
  def update_issue_state(issue_id, state_name)
      when is_binary(issue_id) and is_binary(state_name) do
    with {:ok, state_id} <- resolve_state_id(issue_id, state_name),
         {:ok, response} <-
           client_module().graphql(@update_state_mutation, %{issueId: issue_id, stateId: state_id}),
         true <- get_in(response, ["data", "issueUpdate", "success"]) == true do
      :ok
    else
      false -> {:error, :issue_update_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :issue_update_failed}
    end
  end

  @spec attach_issue_resource(String.t(), String.t(), String.t() | nil) :: :ok | {:error, term()}
  def attach_issue_resource(issue_id, url, title \\ nil)
      when is_binary(issue_id) and is_binary(url) do
    {mutation, payload_key} = attachment_mutation(url)
    variables = %{issueId: issue_id, url: url, title: normalize_optional_title(title)}

    with {:ok, response} <- client_module().graphql(mutation, variables),
         true <- get_in(response, ["data", payload_key, "success"]) == true do
      :ok
    else
      false -> {:error, :attachment_link_failed}
      {:error, reason} -> {:error, reason}
      _ -> {:error, :attachment_link_failed}
    end
  end

  defp client_module do
    Application.get_env(:symphony_elixir, :linear_client_module, Client)
  end

  defp attachment_mutation(url) when is_binary(url) do
    if github_pr_url?(url) do
      {@attach_github_pr_mutation, "attachmentLinkGitHubPR"}
    else
      {@attach_url_mutation, "attachmentLinkURL"}
    end
  end

  defp github_pr_url?(url) when is_binary(url) do
    String.match?(url, ~r/^https:\/\/github\.com\/[^\/]+\/[^\/]+\/pull\/\d+(?:\/.*)?$/)
  end

  defp normalize_optional_title(title) when is_binary(title) do
    case String.trim(title) do
      "" -> nil
      trimmed -> trimmed
    end
  end

  defp normalize_optional_title(_title), do: nil

  defp resolve_state_id(issue_id, state_name) do
    with {:ok, response} <-
           client_module().graphql(@state_lookup_query, %{issueId: issue_id, stateName: state_name}),
         state_id when is_binary(state_id) <-
           get_in(response, ["data", "issue", "team", "states", "nodes", Access.at(0), "id"]) do
      {:ok, state_id}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :state_not_found}
    end
  end
end
