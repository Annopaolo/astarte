#
# This file is part of Astarte.
#
# Copyright 2021 Ispirata Srl
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

defmodule Astarte.TriggerEngine.PolicyTest do
  use ExUnit.Case
  require Logger
  import Mox
  alias Astarte.TriggerEngine.Config
  alias AMQP.{Basic, Channel, Connection}
  alias Astarte.TriggerEngine.DatabaseTestHelper
  alias Astarte.TriggerEngine.PolicyTestAMQPHelper
  alias Astarte.TriggerEngine.Policy.PolicySupervisor
  alias Astarte.TriggerEngine.Policy
  alias Astarte.Core.Triggers.Policy, as: PolicyStruct

  @realm_name DatabaseTestHelper.test_realm()

  @payload "some_payload"
  @max_retry_times 10
  @message_id "message_id"
  @headers [
    one: "header",
    another: "different header",
    number: 42,
    x_astarte_realm: @realm_name
  ]

  @retry_all_policy %{
    name: "retry_all",
    retry_times: @max_retry_times,
    maximum_capacity: 100,
    error_handlers: [
      %{on: "any_error", strategy: "retry"}
    ]
  }

  @retry_all_policy_2 %{
    name: "retry_all_2",
    retry_times: @max_retry_times,
    maximum_capacity: 100,
    error_handlers: [
      %{on: "any_error", strategy: "retry"}
    ]
  }

  @discard_all_policy %{
    name: "discard_all",
    maximum_capacity: 100,
    error_handlers: [
      %{on: "any_error", strategy: "discard"}
    ]
  }

  @default_policy %{
    name: "default",
    maximum_capacity: 100,
    error_handlers: [
      %{on: "any_error", strategy: "discard"}
    ]
  }

  @mixed_policy %{
    name: "mixed",
    maximum_capacity: 100,
    retry_times: @max_retry_times,
    error_handlers: [
      %{on: "server_error", strategy: "discard"},
      %{on: [401, 402, 403, 404], strategy: "retry"}
    ]
  }

  setup_all _context do
    %{chan1: chan1, chan2: chan2, chan3: chan3, chan4: chan4, chan5: chan5, conn: conn} =
      setup_policy_and_consumer_processes()

    on_exit(fn ->
      Channel.close(chan1)
      Channel.close(chan2)
      Channel.close(chan3)
      Channel.close(chan4)
      Channel.close(chan5)
      Connection.close(conn)
    end)

    %{chan1: chan1, chan2: chan2, chan3: chan3, chan4: chan4, chan5: chan5}
  end

  setup [:set_mox_global]

  test "successfully retry message with retry strategy", %{chan1: chan} do
    routing_key = generate_routing_key(@realm_name, @retry_all_policy.name)

    headers = [x_astarte_trigger_policy: @retry_all_policy.name] ++ @headers

    map_headers =
      Enum.reduce(headers, %{}, fn {k, v}, acc ->
        Map.put(acc, to_string(k), v)
      end)

    MockEventsConsumer
    |> expect(:consume, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      {:http_error, 404}
    end)
    |> expect(:consume, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      :ok
    end)

    assert :ok == produce_event(chan, routing_key, @payload, headers, @message_id)
    :timer.sleep(1000)
  end

  test "discard (retry strategy) when delivery fails >= retry_times", %{chan2: chan} do
    routing_key = generate_routing_key(@realm_name, @retry_all_policy_2.name)

    headers = [x_astarte_trigger_policy: @retry_all_policy_2.name] ++ @headers

    map_headers =
      Enum.reduce(@headers, %{}, fn {k, v}, acc ->
        Map.put(acc, to_string(k), v)
      end)

    MockEventsConsumer
    |> expect(:consume, @max_retry_times, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      {:http_error, 404}
    end)

    assert :ok == produce_event(chan, routing_key, @payload, headers, @message_id)
    :timer.sleep(1000)
  end

  test "discard message with discard strategy when delivery fails", %{chan3: chan} do
    routing_key = generate_routing_key(@realm_name, @discard_all_policy.name)

    headers = [x_astarte_trigger_policy: @discard_all_policy.name] ++ @headers

    map_headers =
      Enum.reduce(headers, %{}, fn {k, v}, acc ->
        Map.put(acc, to_string(k), v)
      end)

    MockEventsConsumer
    |> expect(:consume, 1, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      {:http_error, 500}
    end)

    assert :ok == produce_event(chan, routing_key, @payload, headers, @message_id)
    :timer.sleep(1000)
  end

  test "@default policy never retries", %{chan4: chan} do
    routing_key = generate_routing_key(@realm_name, @default_policy.name)

    headers = [x_astarte_trigger_policy: @default_policy.name] ++ @headers

    map_headers =
      Enum.reduce(headers, %{}, fn {k, v}, acc ->
        Map.put(acc, to_string(k), v)
      end)

    MockEventsConsumer
    |> expect(:consume, 1, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      {:http_error, 404}
    end)

    assert :ok == produce_event(chan, routing_key, @payload, headers, @message_id)
    :timer.sleep(1000)
  end

  test "mixed policy ok", %{chan5: chan} do
    routing_key = generate_routing_key(@realm_name, @mixed_policy.name)

    headers = [x_astarte_trigger_policy: @mixed_policy.name] ++ @headers

    map_headers =
      Enum.reduce(headers, %{}, fn {k, v}, acc ->
        Map.put(acc, to_string(k), v)
      end)

    MockEventsConsumer
    |> expect(:consume, 10, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      {:http_error, 404}
    end)
    |> expect(:consume, 1, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      {:http_error, 500}
    end)
    |> expect(:consume, 1, fn payload, headers ->
      assert payload == @payload
      assert is_map(headers)
      assert headers == map_headers
      :ok
    end)

    assert :ok == produce_event(chan, routing_key, @payload, headers, "message1")
    assert :ok == produce_event(chan, routing_key, @payload, headers, "message2")
    assert :ok == produce_event(chan, routing_key, @payload, headers, "message3")
    :timer.sleep(1000)
  end

  defp setup_policy_and_consumer_processes() do
    {:ok, {conn, [chan1, chan2, chan3, chan4, chan5]}} = init_conn_and_chans()

    policy1 =
      PolicyStruct.changeset(%PolicyStruct{}, @retry_all_policy)
      |> Ecto.Changeset.apply_action!(:insert)

    policy1_pid = get_policy_process(@realm_name, policy1)

    _consumer1 =
      PolicyTestAMQPHelper.start_link(%{
        realm_name: @realm_name,
        policy_name: policy1.name,
        policy_pid: policy1_pid,
        channel: chan1
      })

    policy2 =
      PolicyStruct.changeset(%PolicyStruct{}, @retry_all_policy_2)
      |> Ecto.Changeset.apply_action!(:insert)

    policy2_pid = get_policy_process(@realm_name, policy2)

    _consumer2 =
      PolicyTestAMQPHelper.start_link(%{
        realm_name: @realm_name,
        policy_name: policy2.name,
        policy_pid: policy2_pid,
        channel: chan2
      })

    policy3 =
      PolicyStruct.changeset(%PolicyStruct{}, @discard_all_policy)
      |> Ecto.Changeset.apply_action!(:insert)

    policy3_pid = get_policy_process(@realm_name, policy3)

    _consumer3 =
      PolicyTestAMQPHelper.start_link(%{
        realm_name: @realm_name,
        policy_name: policy3.name,
        policy_pid: policy3_pid,
        channel: chan3
      })

    policy4_valid_name =
      PolicyStruct.changeset(%PolicyStruct{}, @default_policy)
      |> Ecto.Changeset.apply_action!(:insert)

    policy4 = %PolicyStruct{policy4_valid_name | name: "@default"}

    policy4_pid = get_policy_process(@realm_name, policy4)

    _consumer4 =
      PolicyTestAMQPHelper.start_link(%{
        realm_name: @realm_name,
        policy_name: policy4.name,
        policy_pid: policy4_pid,
        channel: chan4
      })

    policy5 =
      PolicyStruct.changeset(%PolicyStruct{}, @mixed_policy)
      |> Ecto.Changeset.apply_action!(:insert)

    policy5_pid = get_policy_process(@realm_name, policy5)

    _consumer5 =
      PolicyTestAMQPHelper.start_link(%{
        realm_name: @realm_name,
        policy_name: policy5.name,
        policy_pid: policy5_pid,
        channel: chan5
      })

    %{chan1: chan1, chan2: chan2, chan3: chan3, chan4: chan4, chan5: chan5, conn: conn}
  end

  defp init_conn_and_chans() do
    with amqp_consumer_options = Config.amqp_consumer_options!(),
         {:ok, conn} <- Connection.open(amqp_consumer_options),
         {:ok, chan1} <- Channel.open(conn),
         {:ok, chan2} <- Channel.open(conn),
         {:ok, chan3} <- Channel.open(conn),
         {:ok, chan4} <- Channel.open(conn),
         {:ok, chan5} <- Channel.open(conn) do
      {:ok, {conn, [chan1, chan2, chan3, chan4, chan5]}}
    else
      {:error, reason} ->
        Logger.warn("RabbitMQ Connection error: #{inspect(reason)}")
        {:ok, %{channel: nil, connection: nil}}

      _ ->
        Logger.warn("Unknown RabbitMQ connection error")
        {:ok, %{channel: nil, connection: nil}}
    end
  end

  defp produce_event(chan, routing_key, payload, headers, message_id) do
    exchange = Config.events_exchange_name!()

    Basic.publish(chan, exchange, routing_key, payload, headers: headers, message_id: message_id)
  end

  defp get_policy_process(realm_name, policy) do
    case Registry.lookup(Registry.PolicyRegistry, {realm_name, policy.name}) do
      [] ->
        child = {Policy, [realm_name: realm_name, policy: policy]}
        {:ok, pid} = PolicySupervisor.start_child(child)
        pid

      [{pid, nil}] ->
        pid
    end
  end

  defp generate_routing_key(realm, policy) do
    "#{realm}_#{policy}"
  end
end
