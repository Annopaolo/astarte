#
# This file is part of Astarte.
#
# Copyright 2022 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

defmodule Astarte.TriggerEngine.AMQPConsumer.AMQPConsumerTracker do
  require Logger

  use GenServer
  alias Astarte.TriggerEngine.AMQPConsumer.Queries
  alias Astarte.TriggerEngine.AMQPConsumer.AMQPConsumerSupervisor
  alias Astarte.TriggerEngine.AMQPConsumer.AMQPMessageConsumer.Policy

  # 30 seconds
  @update_timeout 30 * 1000

  def start_link(default) when is_list(default) do
    GenServer.start_link(__MODULE__, default, name: __MODULE__)
  end

  # Server callbacks

  @impl true
  def init(args \\ []) do
    schedule_update()
    {:ok, args}
  end

  # TODO integration
  @impl true
  def handle_info(:update_consumers, state) do
    registered_consumers =
      Registry.select(Registry.AMQPConsumerRegistry, [{{:"$1", :_, :_}, [], [:"$1"]}])

    _ = Logger.debug("registered_consumers: #{inspect(registered_consumers)}")

    all_policies = fetch_all_policies_with_realms()

    %{create: new_consumers, remove: outdated_consumers} =
      Policy.update_consumers(all_policies, registered_consumers)

    _ = Logger.debug("new_consumers: #{inspect(new_consumers)}")

    Enum.each(new_consumers, &start_new_consumer/1)

    _ = Logger.debug("outdated_consumers: #{inspect(outdated_consumers)}")

    Enum.each(outdated_consumers, &remove_outdated_consumer/1)

    schedule_update()

    {:noreply, state}
  end

  defp schedule_update() do
    Process.send_after(__MODULE__, :update_consumers, @update_timeout)
  end

  defp start_new_consumer(%{realm: realm, policy: policy}) do
    _ =
      Logger.debug("Found new policy queue for #{realm}, #{policy.name}, starting consumer")

    child = Policy.to_consumer(realm, policy)

    {:ok, _pid} = AMQPConsumerSupervisor.start_child(child)
  end

  defp remove_outdated_consumer({realm_name, policy_name}) do
    _ = Logger.debug("Removing old consumer for policy #{realm_name}, #{policy_name}")

    case Registry.lookup(Registry.AMQPConsumerRegistry, {realm_name, policy_name}) do
      [{pid, nil}] -> AMQPConsumerSupervisor.terminate_child(pid)
      # already ded, we don't care
      [] -> :ok
    end
  end

  defp fetch_all_policies_with_realms() do
    with {:ok, realm_names} <- Queries.list_realms() do
      Enum.flat_map(realm_names, fn realm ->
        realm |> fetch_policies_for_realm() |> Policy.to_policy_list()
      end)
    end
  end

  defp fetch_policies_for_realm(realm_name) do
    with {:ok, policies_list} <- Queries.list_policies(realm_name) do
      policies_list
    end
  end
end
