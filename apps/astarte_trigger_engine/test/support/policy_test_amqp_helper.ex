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

defmodule Astarte.TriggerEngine.PolicyTestAMQPHelper do
  require Logger
  use GenServer

  alias AMQP.Basic
  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Exchange
  alias AMQP.Queue
  alias Astarte.TriggerEngine.Policy
  alias Astarte.TriggerEngine.Config

  @connection_backoff 10000

  # API

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def ack(delivery_tag) do
    GenServer.call(__MODULE__, {:ack, delivery_tag})
  end

  # Server callbacks

  def init(%{realm_name: realm_name, policy_name: policy_name, policy_pid: pid, channel: chan}) do
    rabbitmq_connect(realm_name, policy_name, pid, chan, false)
  end

  def terminate(_reason, %{channel: chan} = _state) do
    %Channel{conn: conn} = chan
    Channel.close(chan)
    Connection.close(conn)
  end

  def handle_call({:ack, delivery_tag}, _from, %{channel: chan} = state) do
    res = Basic.ack(chan, delivery_tag)
    {:reply, res, state}
  end

  # Confirmation sent by the broker after registering this process as a consumer
  def handle_info({:basic_consume_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Sent by the broker when the consumer is unexpectedly cancelled (such as after a queue deletion)
  def handle_info({:basic_cancel, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Confirmation sent by the broker to the consumer process after a Basic.cancel
  def handle_info({:basic_cancel_ok, %{consumer_tag: _consumer_tag}}, state) do
    {:noreply, state}
  end

  # Message consumed
  def handle_info({:basic_deliver, payload, meta}, %{channel: chan, policy_pid: pid} = state) do
    Policy.handle_event(pid, payload, meta, chan)
    {:noreply, state}
  end

  def handle_info(
        {:try_to_connect},
        %{realm_name: realm_name, policy_name: policy_name, policy_pid: pid, channel: chan} =
          _state
      ) do
    {:ok, new_state} = rabbitmq_connect(realm_name, policy_name, pid, chan, false)
    {:noreply, new_state}
  end

  def handle_info(
        {:DOWN, _, :process, _pid, reason},
        %{realm_name: realm_name, policy_name: policy_name, policy_pid: pid, channel: chan} =
          _state
      ) do
    Logger.warn("RabbitMQ connection lost: #{inspect(reason)}. Trying to reconnect...")
    {:ok, new_state} = rabbitmq_connect(realm_name, policy_name, pid, chan, false)
    {:noreply, new_state}
  end

  defp rabbitmq_connect(
         realm_name,
         policy_name,
         policy_pid,
         chan,
         retry \\ true
       ) do
    with events_exchange_name = Config.events_exchange_name!(),
         events_queue_name = generate_queue_name(realm_name, policy_name),
         events_routing_key = generate_routing_key(realm_name, policy_name),
         :ok <-
           Exchange.declare(chan, events_exchange_name, :direct, durable: true),
         {:ok, _queue} <- Queue.declare(chan, events_queue_name, durable: true),
         :ok <-
           Queue.bind(
             chan,
             events_queue_name,
             events_exchange_name,
             routing_key: events_routing_key,
             arguments: [{"x-queue-mode", :longstr, "lazy"}]
           ),
         {:ok, _consumer_tag} <- Basic.consume(chan, events_queue_name),
         # Get notifications when the chan or conn go down
         Process.monitor(chan.pid) do
      {:ok,
       %{channel: chan, policy_pid: policy_pid, realm_name: realm_name, policy_name: policy_name}}
    else
      {:error, reason} ->
        Logger.warn("RabbitMQ Connection error: #{inspect(reason)}")
        maybe_retry(retry)

      :error ->
        Logger.warn("Unknown RabbitMQ connection error")
        maybe_retry(retry)
    end
  end

  defp generate_queue_name(realm, policy) do
    "#{realm}_#{policy}_queue"
  end

  defp generate_routing_key(realm, policy) do
    "#{realm}_#{policy}"
  end

  defp maybe_retry(retry) do
    if retry do
      Logger.warn("Retrying connection in #{@connection_backoff} ms")
      :erlang.send_after(@connection_backoff, :erlang.self(), {:try_to_connect})
      {:ok, :not_connected}
    else
      {:stop, :connection_failed}
    end
  end
end
