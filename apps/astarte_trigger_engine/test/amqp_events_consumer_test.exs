#
# This file is part of Astarte.
#
# Copyright 2018 Ispirata Srl
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

defmodule Astarte.TriggerEngine.AMQPEventsConsumerTest do
  use ExUnit.Case
  require Logger
  import Mox
  alias Astarte.TriggerEngine.AMQPEventsConsumer
  alias Astarte.TriggerEngine.Config
  alias AMQP.{Basic, Channel, Connection, Queue}
  alias Astarte.TriggerEngine.DatabaseTestHelper
  alias Astarte.Core.Triggers.Policy, as: PolicyStruct

  @realm_name DatabaseTestHelper.test_realm()

  @payload "some_payload"
  @message_id "message_id"
  @headers [some: "headers"]

  @a_policy %{
    name: "a_policy",
    retry_times: 1,
    maximum_capacity: 100,
    error_handlers: [
      %{on: "any_error", strategy: "retry"}
    ]
  }

  setup_all [:setup_amqp_consumer_process, :get_amqp_channel]
  setup [:set_mox_global]

  test "Consumes an event with a policy process", %{channel: chan} do
    MockEventsConsumer
    |> expect(:consume, 1, fn _, _ ->
      :ok
    end)

    routing_key = generate_routing_key(@realm_name, @a_policy.name)

    assert :ok == produce_event(chan, routing_key, @payload, @headers, @message_id)

    # Leave time for the consumer to spawn the policy
    :timer.sleep(1000)

    registered_policies =
      Registry.select(Registry.PolicyRegistry, [{{:"$1", :_, :_}, [], [:"$1"]}])

    assert {@realm_name, @a_policy.name} in registered_policies

    # Leave time for the policy to ack
    :timer.sleep(1000)
  end

  test "AMQP queue has only one consumer", %{channel: chan} do
    routing_key = generate_routing_key(@realm_name, @a_policy.name)
    queue_name = generate_queue_name(@realm_name, @a_policy.name)

    assert {:ok, %{queue: ^queue_name, message_count: _message_count, consumer_count: 1}} =
             Queue.declare(chan, queue_name, passive: true)
  end

  defp setup_amqp_consumer_process(_context) do
    policy =
      PolicyStruct.changeset(%PolicyStruct{}, @a_policy)
      |> Ecto.Changeset.apply_action!(:insert)

    {:ok, pid} = start_supervised({AMQPEventsConsumer, [realm_name: @realm_name, policy: policy]})

    %{process_id: pid}
  end

  defp get_amqp_channel(%{process_id: pid}) do
    {:ok, chan} = wait_for_connection(pid)
    %{process_id: pid, channel: chan}
  end

  defp wait_for_connection(_pid, retry_count \\ 0)

  # Avoid endless waiting (retry_count > 50 ~= 5 seconds)
  defp wait_for_connection(_pid, retry_count) when retry_count > 50 do
    {:error, :not_connected}
  end

  defp wait_for_connection(pid, retry_count) do
    %{channel: chan} = :sys.get_state(pid)

    if chan do
      {:ok, chan}
    else
      :timer.sleep(100)
      wait_for_connection(pid, retry_count + 1)
    end
  end

  defp produce_event(chan, routing_key, payload, headers, message_id) do
    exchange = Config.events_exchange_name!()

    Basic.publish(chan, exchange, routing_key, payload, headers: headers, message_id: message_id)
  end

  defp generate_queue_name(realm, policy) do
    "#{realm}_#{policy}_queue"
  end

  defp generate_routing_key(realm, policy) do
    "#{realm}_#{policy}"
  end
end
