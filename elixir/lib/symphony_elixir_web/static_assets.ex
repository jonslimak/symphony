defmodule SymphonyElixirWeb.StaticAssets do
  @moduledoc false

  @dashboard_css_path Path.expand("../../priv/static/dashboard.css", __DIR__)
  @roboto_mono_400_path Path.expand("../../priv/static/fonts/roboto-mono-400.ttf", __DIR__)
  @roboto_mono_500_path Path.expand("../../priv/static/fonts/roboto-mono-500.ttf", __DIR__)
  @roboto_mono_600_path Path.expand("../../priv/static/fonts/roboto-mono-600.ttf", __DIR__)
  @roboto_mono_700_path Path.expand("../../priv/static/fonts/roboto-mono-700.ttf", __DIR__)
  @phoenix_html_js_path Application.app_dir(:phoenix_html, "priv/static/phoenix_html.js")
  @phoenix_js_path Application.app_dir(:phoenix, "priv/static/phoenix.js")
  @phoenix_live_view_js_path Application.app_dir(:phoenix_live_view, "priv/static/phoenix_live_view.js")

  @external_resource @dashboard_css_path
  @external_resource @roboto_mono_400_path
  @external_resource @roboto_mono_500_path
  @external_resource @roboto_mono_600_path
  @external_resource @roboto_mono_700_path
  @external_resource @phoenix_html_js_path
  @external_resource @phoenix_js_path
  @external_resource @phoenix_live_view_js_path

  @dashboard_css File.read!(@dashboard_css_path)
  @roboto_mono_400 File.read!(@roboto_mono_400_path)
  @roboto_mono_500 File.read!(@roboto_mono_500_path)
  @roboto_mono_600 File.read!(@roboto_mono_600_path)
  @roboto_mono_700 File.read!(@roboto_mono_700_path)
  @phoenix_html_js File.read!(@phoenix_html_js_path)
  @phoenix_js File.read!(@phoenix_js_path)
  @phoenix_live_view_js File.read!(@phoenix_live_view_js_path)

  @assets %{
    "/dashboard.css" => {"text/css", @dashboard_css},
    "/fonts/roboto-mono-400.ttf" => {"font/ttf", @roboto_mono_400},
    "/fonts/roboto-mono-500.ttf" => {"font/ttf", @roboto_mono_500},
    "/fonts/roboto-mono-600.ttf" => {"font/ttf", @roboto_mono_600},
    "/fonts/roboto-mono-700.ttf" => {"font/ttf", @roboto_mono_700},
    "/vendor/phoenix_html/phoenix_html.js" => {"application/javascript", @phoenix_html_js},
    "/vendor/phoenix/phoenix.js" => {"application/javascript", @phoenix_js},
    "/vendor/phoenix_live_view/phoenix_live_view.js" => {"application/javascript", @phoenix_live_view_js}
  }

  @spec fetch(String.t()) :: {:ok, String.t(), binary()} | :error
  def fetch(path) when is_binary(path) do
    case Map.fetch(@assets, path) do
      {:ok, {content_type, body}} -> {:ok, content_type, body}
      :error -> :error
    end
  end
end
