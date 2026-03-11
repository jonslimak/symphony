defmodule SymphonyElixir.Codex.DynamicTool do
  @moduledoc """
  Executes client-side tool calls requested by Codex app-server turns.
  """

  alias SymphonyElixir.Linear.Client
  alias SymphonyElixir.Tracker

  @linear_graphql_tool "linear_graphql"
  @linear_attach_resource_tool "linear_attach_issue_resource"
  @linear_graphql_description """
  Execute a raw GraphQL query or mutation against Linear using Symphony's configured auth.
  """
  @linear_attach_resource_description """
  Attach a review resource URL to a Linear issue using Symphony's configured tracker integration.
  """
  @linear_graphql_input_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["query"],
    "properties" => %{
      "query" => %{
        "type" => "string",
        "description" => "GraphQL query or mutation document to execute against Linear."
      },
      "variables" => %{
        "type" => ["object", "null"],
        "description" => "Optional GraphQL variables object.",
        "additionalProperties" => true
      }
    }
  }
  @linear_attach_resource_input_schema %{
    "type" => "object",
    "additionalProperties" => false,
    "required" => ["issueId", "url"],
    "properties" => %{
      "issueId" => %{
        "type" => "string",
        "description" => "Linear issue id to attach the resource to."
      },
      "url" => %{
        "type" => "string",
        "description" => "Resource URL to attach, such as a GitHub pull request URL."
      },
      "title" => %{
        "type" => ["string", "null"],
        "description" => "Optional attachment title override."
      }
    }
  }

  @spec execute(String.t() | nil, term(), keyword()) :: map()
  def execute(tool, arguments, opts \\ []) do
    case tool do
      @linear_graphql_tool ->
        execute_linear_graphql(arguments, opts)

      @linear_attach_resource_tool ->
        execute_linear_attach_resource(arguments, opts)

      other ->
        failure_response(%{
          "error" => %{
            "message" => "Unsupported dynamic tool: #{inspect(other)}.",
            "supportedTools" => supported_tool_names()
          }
        })
    end
  end

  @spec tool_specs() :: [map()]
  def tool_specs do
    [
      %{
        "name" => @linear_graphql_tool,
        "description" => @linear_graphql_description,
        "inputSchema" => @linear_graphql_input_schema
      },
      %{
        "name" => @linear_attach_resource_tool,
        "description" => @linear_attach_resource_description,
        "inputSchema" => @linear_attach_resource_input_schema
      }
    ]
  end

  defp execute_linear_graphql(arguments, opts) do
    linear_client = Keyword.get(opts, :linear_client, &Client.graphql/3)

    with {:ok, query, variables} <- normalize_linear_graphql_arguments(arguments),
         {:ok, response} <- linear_client.(query, variables, []) do
      graphql_response(response)
    else
      {:error, reason} ->
        failure_response(tool_error_payload(reason))
    end
  end

  defp execute_linear_attach_resource(arguments, opts) do
    attach_resource = Keyword.get(opts, :attach_resource, &Tracker.attach_issue_resource/3)

    with {:ok, issue_id, url, title} <- normalize_linear_attach_resource_arguments(arguments),
         :ok <- attach_resource.(issue_id, url, title) do
      %{
        "success" => true,
        "contentItems" => [
          %{
            "type" => "inputText",
            "text" =>
              encode_payload(%{
                "issueId" => issue_id,
                "url" => url,
                "title" => title,
                "attached" => true
              })
          }
        ]
      }
    else
      {:error, reason} ->
        failure_response(tool_error_payload(reason))
    end
  end

  defp normalize_linear_graphql_arguments(arguments) when is_binary(arguments) do
    case String.trim(arguments) do
      "" -> {:error, :missing_query}
      query -> {:ok, query, %{}}
    end
  end

  defp normalize_linear_graphql_arguments(arguments) when is_map(arguments) do
    case normalize_query(arguments) do
      {:ok, query} ->
        case normalize_variables(arguments) do
          {:ok, variables} ->
            {:ok, query, variables}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp normalize_linear_graphql_arguments(_arguments), do: {:error, :invalid_arguments}

  defp normalize_linear_attach_resource_arguments(arguments) when is_map(arguments) do
    issue_id = Map.get(arguments, "issueId") || Map.get(arguments, :issueId)
    url = Map.get(arguments, "url") || Map.get(arguments, :url)
    title = Map.get(arguments, "title") || Map.get(arguments, :title)

    cond do
      not (is_binary(issue_id) and String.trim(issue_id) != "") ->
        {:error, :missing_issue_id}

      not (is_binary(url) and String.trim(url) != "") ->
        {:error, :missing_url}

      not (is_nil(title) or is_binary(title)) ->
        {:error, :invalid_title}

      true ->
        {:ok, String.trim(issue_id), String.trim(url), title}
    end
  end

  defp normalize_linear_attach_resource_arguments(_arguments), do: {:error, :invalid_arguments}

  defp normalize_query(arguments) do
    case Map.get(arguments, "query") || Map.get(arguments, :query) do
      query when is_binary(query) ->
        case String.trim(query) do
          "" -> {:error, :missing_query}
          trimmed -> {:ok, trimmed}
        end

      _ ->
        {:error, :missing_query}
    end
  end

  defp normalize_variables(arguments) do
    case Map.get(arguments, "variables") || Map.get(arguments, :variables) || %{} do
      variables when is_map(variables) -> {:ok, variables}
      _ -> {:error, :invalid_variables}
    end
  end

  defp graphql_response(response) do
    success =
      case response do
        %{"errors" => errors} when is_list(errors) and errors != [] -> false
        %{errors: errors} when is_list(errors) and errors != [] -> false
        _ -> true
      end

    %{
      "success" => success,
      "contentItems" => [
        %{
          "type" => "inputText",
          "text" => encode_payload(response)
        }
      ]
    }
  end

  defp failure_response(payload) do
    %{
      "success" => false,
      "contentItems" => [
        %{
          "type" => "inputText",
          "text" => encode_payload(payload)
        }
      ]
    }
  end

  defp encode_payload(payload) when is_map(payload) or is_list(payload) do
    Jason.encode!(payload, pretty: true)
  end

  defp encode_payload(payload), do: inspect(payload)

  defp tool_error_payload(:missing_query) do
    %{
      "error" => %{
        "message" => "`linear_graphql` requires a non-empty `query` string."
      }
    }
  end

  defp tool_error_payload(:missing_issue_id) do
    %{
      "error" => %{
        "message" => "`linear_attach_issue_resource` requires a non-empty `issueId` string."
      }
    }
  end

  defp tool_error_payload(:missing_url) do
    %{
      "error" => %{
        "message" => "`linear_attach_issue_resource` requires a non-empty `url` string."
      }
    }
  end

  defp tool_error_payload(:invalid_title) do
    %{
      "error" => %{
        "message" => "`linear_attach_issue_resource.title` must be a string when provided."
      }
    }
  end

  defp tool_error_payload(:invalid_arguments) do
    %{
      "error" => %{
        "message" =>
          "Tool arguments are invalid. `linear_graphql` expects a query payload; `linear_attach_issue_resource` expects `issueId`, `url`, and optional `title`."
      }
    }
  end

  defp tool_error_payload(:invalid_variables) do
    %{
      "error" => %{
        "message" => "`linear_graphql.variables` must be a JSON object when provided."
      }
    }
  end

  defp tool_error_payload(:missing_linear_api_token) do
    %{
      "error" => %{
        "message" => "Symphony is missing Linear auth. Set `linear.api_key` in `WORKFLOW.md` or export `LINEAR_API_KEY`."
      }
    }
  end

  defp tool_error_payload({:linear_api_status, status}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed with HTTP #{status}.",
        "status" => status
      }
    }
  end

  defp tool_error_payload({:linear_api_request, reason}) do
    %{
      "error" => %{
        "message" => "Linear GraphQL request failed before receiving a successful response.",
        "reason" => inspect(reason)
      }
    }
  end

  defp tool_error_payload(:attachment_link_failed) do
    %{
      "error" => %{
        "message" => "Linear issue attachment failed."
      }
    }
  end

  defp tool_error_payload(reason) do
    %{
      "error" => %{
        "message" => "Linear GraphQL tool execution failed.",
        "reason" => inspect(reason)
      }
    }
  end

  defp supported_tool_names do
    Enum.map(tool_specs(), & &1["name"])
  end
end
