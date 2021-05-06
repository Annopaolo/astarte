#
# This file is part of Astarte.
#
# Copyright 2017-2018 Ispirata Srl
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

defmodule Astarte.TriggerEngine.AMQPConsumer.Tracker do
  # Spawns (and kills?) amqp consumers, based on what trigger policies
  # are available.
  # Once in a little while (10 min?) checks what policies are available
  # in the database.
  # Then, much like Santa, checks the list twice with PolicyRegistry,
  # finding nice policies (to be kept) or naughty ones (to be started).
  # Finally, starts a new consumer for each policy to be started.
  require Logger

  use GenServer
  alias Astarte.TriggerEngine.AMQPConsumer.Queries
  alias Astarte.TriggerEngine.AMQPConsumer.Supervisor, as: AMQPConsumerSupervisor
  alias Astarte.TriggerEngine.AMQPEventsConsumer
  alias Astarte.Core.Triggers.Policy
  alias Astarte.Core.Triggers.PolicyProtobuf.Policy, as: PolicyProto

  # 10 minutes
  @update_timeout 60 * 10 * 1000

  # Client

  def start_link(default) when is_list(default) do
    GenServer.start_link(__MODULE__, default, name: __MODULE__)
  end

  # Server callbacks

  @impl true
  def init(args \\ []) do
    schedule_update()
    {:ok, args}
  end

  @impl true
  def handle_cast(:update_consumers, state) do
    registered_consumers =
      Registry.select(Registry.AMQPConsumerRegistry, [{{:"$1", :_, :_}, [], [:"$1"]}])

    policies = fetch_all_policies_with_realms_list()

    new_consumers =
      Enum.filter(policies, fn {realm, {policy_name, _policy_data}} ->
        {realm, policy_name} in registered_consumers
      end)

    _ =
      Logger.info(
        "Found new policy queues in need of consumers: #{inspect(new_consumers)}, starting consumers..."
      )

    Enum.each(new_consumers, &start_new_consumer/1)

    schedule_update()

    {:noreply, state}
  end

  defp schedule_update() do
    Process.send_after(__MODULE__, :update_consumers, @update_timeout)
  end

  defp start_new_consumer({realm_name, {_policy_name, policy_data}}) do
    policy = policy_data |> PolicyProto.decode() |> Policy.from_policy_proto!()
    child = {AMQPEventsConsumer, [realm_name: realm_name, policy: policy]}

    {:ok, _pid} = AMQPConsumerSupervisor.start_child(child)
    # add pid to registry: this is done in start_link of amqpeventsconsumer
  end

  defp fetch_all_policies_with_realms_list() do
    with {:ok, realm_names} <- Queries.list_realms() do
      Enum.reduce(realm_names, [], fn realm_name, acc ->
        fetch_realm_policies_list(realm_name) ++ acc
      end)
    end
  end

  defp fetch_realm_policies_list(realm_name) do
    policies = do_fetch_realm_policies_list(realm_name)
    real_policies = Enum.map(policies, fn x -> {realm_name, x} end)
    [{realm_name, generate_default_policy()} | real_policies]
  end

  defp do_fetch_realm_policies_list(realm_name) do
    with {:ok, policies_list} <- Queries.list_policies(realm_name) do
      policies_list
    end
  end

  # we need this because default policies
  # cannot be installed
  defp generate_default_policy() do
    name = "@default"

    {name,
     %Policy{
       name: name,
       # revise
       maximum_capacity: 100,
       error_handlers: [
         %{on: "any_error", strategy: "discard"}
       ]
     }
     |> Policy.to_policy_proto()
     |> PolicyProto.encode()}
  end
end
