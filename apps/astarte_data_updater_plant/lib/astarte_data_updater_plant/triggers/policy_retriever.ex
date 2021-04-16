defmodule Astarte.DataUpdaterPlant.Triggers.PolicyRetriever do
  use GenServer

  require Logger
  # alias Astarte.DataAccess.Database
  # alias CQEx.Query, as: DatabaseQuery
  # alias CQEx.Result, as: DatabaseResult

  # API
  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def get_policy_name(realm_id, trigger_id) do
    GenServer.call(__MODULE__, {:get_policy_name, realm_id, trigger_id})
  end

  # Callbacks
  @impl true
  def init(_state) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:get_policy_name, realm_id, trigger_id}, _from, state) do
    case Map.get(state, {realm_id, trigger_id}) do
      nil ->
        with {:ok, policy_name} <- retrieve_policy_name(realm_id, trigger_id) do
          {:reply, policy_name, Map.put(state, {realm_id, trigger_id}, policy_name)}
        else
          {:error, _error} ->
            Logger.warn(
              "Policy name for trigger #{trigger_id} not found, reverting to default policy"
            )

            {:reply, "@default", Map.put(state, {realm_id, trigger_id}, "@default")}
        end

      policy_name ->
        {:reply, policy_name, state}
    end
  end

  defp retrieve_policy_name(realm_id, trigger_id) do
    retrieve_statement =
      "SELECT value FROM #{realm_id}.kv_store WHERE group='trigger_to_policy' AND key=:trigger_id;"

    with {:ok, prepared} <-
           Xandra.Cluster.prepare(:xandra, retrieve_statement),
         {:ok, %Xandra.Page{} = page} <-
           Xandra.Cluster.execute(:xandra, prepared, %{"trigger_id" => trigger_id}),
         [%{"value" => policy_name}] <- Enum.to_list(page) do
      {:ok, policy_name}
    else
      {:error, :database_connection_error} ->
        Logger.warn("Database connection error.")
        {:error, :database_connection_error}

      error ->
        Logger.warn("Error while fetching policy for trigger #{trigger_id}: #{inspect(error)}")
        {:error, :event_processing_error}
    end
  end

  # defp retrieve_policy_name(realm_id, trigger_id) do
  #   {:ok, client} = Database.connect(realm: realm_id)

  #   retrieve_statement =
  #     "SELECT value FROM kv_store WHERE group='trigger_to_policy' AND key=:trigger_id;"

  #   query =
  #     DatabaseQuery.new()
  #     |> DatabaseQuery.statement(retrieve_statement)
  #     |> DatabaseQuery.put(:trigger_id, trigger_id)
  #     |> DatabaseQuery.consistency(:quorum)

  #   with {:ok, res} <- DatabaseQuery.call(client, query),
  #        [value: policy_name] <- DatabaseResult.head(res) do
  #     {:ok, policy_name}
  #   else
  #     :empty_dataset ->
  #       {:ok, nil}

  #     %{acc: _, msg: error_message} ->
  #       Logger.warn("Database error: #{error_message}.")
  #       {:error, :database_error}

  #     {:error, reason} ->
  #       Logger.warn("Failed with reason: #{inspect(reason)}.")
  #       {:error, :database_error}
  #   end
  # end
end
