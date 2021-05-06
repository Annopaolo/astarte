defmodule Astarte.TriggerEngine.AMQPConsumer.Queries do
  require Logger

  def list_policies(realm_name) do
    with {:ok, policy_list} <-
           Xandra.Cluster.run(:xandra, fn conn ->
             do_list_policies(conn, realm_name)
           end) do
      {:ok, policy_list}
    else
      {:error, reason} ->
        _ =
          Logger.warn(
            "Cannot retrieve policy list for realm #{realm_name}, reason: #{inspect(reason)}.",
            tag: "policy_list_failed"
          )

        {:error, reason}
    end
  end

  defp do_list_policies(conn, realm_name) do
    # warning
    list_policies_statement = "SELECT * FROM #{realm_name}.kv_store WHERE group='trigger_policy';"

    with {:ok, prepared} <-
           Xandra.prepare(conn, list_policies_statement),
         {:ok, %Xandra.Page{} = page} <-
           Xandra.execute(conn, prepared, %{}),
         policy_list <- Enum.map(page, &extract_name_and_data/1) do
      {:ok, policy_list}
    else
      {:error, %Xandra.Error{} = err} ->
        _ = Logger.warn("Database error: #{inspect(err)}.", tag: "database_error")
        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        _ =
          Logger.warn("Database connection error: #{inspect(err)}.",
            tag: "database_connection_error"
          )

        {:error, :database_connection_error}
    end
  end

  # TODO use conn
  def list_realms do
    query = """
    SELECT realm_name
    FROM astarte.realms;
    """

    case Xandra.Cluster.execute(:xandra, query, %{}, consistency: :quorum) do
      {:ok, %Xandra.Page{} = page} ->
        {:ok, Enum.map(page, fn %{"realm_name" => realm_name} -> realm_name end)}

      {:error, %Xandra.Error{} = err} ->
        _ =
          Logger.warn("Database error while listing realms: #{inspect(err)}.",
            tag: "database_error"
          )

        {:error, :database_error}

      {:error, %Xandra.ConnectionError{} = err} ->
        _ =
          Logger.warn("Database connection error while listing realms: #{inspect(err)}.",
            tag: "database_connection_error"
          )

        {:error, :database_connection_error}
    end
  end

  defp extract_name_and_data(%{"key" => name, "value" => data}) do
    {name, data}
  end
end
