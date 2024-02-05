#
# This file is part of Astarte.
#
# Copyright 2017-2020 Ispirata Srl
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

defmodule Astarte.DataUpdaterPlant.AMQPEventsProducer do
  require Logger
  use GenServer

  alias Astarte.DataUpdaterPlant.Config

  @connection_backoff 10000
  @adapter Config.amqp_adapter!()

  # API

  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def publish(exchange, routing_key, payload, opts) do
    # Use a longer timeout to allow RabbitMQ to process requests even if loaded
    GenServer.call(__MODULE__, {:publish, exchange, routing_key, payload, opts}, 60_000)
  end

  def declare_exchange(exchange) do
    # Use a longer timeout to allow RabbitMQ to process requests even if loaded
    GenServer.call(__MODULE__, {:declare_exchange, exchange}, 60_000)
  end

  # Get a connection worker out of the connection pool, if there is one available.
  # Then take a channel out of its channel pool, if there is one available, and try
  # to declare an exchange and monitor the channel.
  # If anything fails, try again in @connection_backoff ms.
  defp do_connect(state) do
    conn = ExRabbitPool.get_connection_worker(:events_producer_pool)

    case ExRabbitPool.checkout_channel(conn) do
      {:ok, channel} ->
        _ = Logger.debug("Successfully checked out channel for events producer")
        try_to_setup_exchange(channel, conn, state)

      {:error, _reason} ->
        _ =
          Logger.warn("Failed to check out channel for events producer",
            tag: "events_producer_checkout_channel_fail"
          )

        schedule_connect()
        {:noreply, state}
    end
  end

  defp try_to_setup_exchange(channel, conn, _state) do
    %{pid: channel_pid} = channel

    case @adapter.declare_exchange(channel, Config.events_exchange_name!(),
           type: :direct,
           durable: true
         ) do
      :ok ->
        _ = Logger.debug("Successfully declared events exchange")
        Process.monitor(channel_pid)

        {:noreply, channel}

      {:error, reason} ->
        Logger.warn("RabbitMQ Connection error: #{inspect(reason)}",
          tag: "events_producer_conn_err"
        )

        _ = ExRabbitPool.checkin_channel(conn, channel)

        schedule_connect()
    end
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    {:ok, [], {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state), do: do_connect(state)

  @impl true
  def handle_call({:publish, exchange, routing_key, payload, opts}, _from, chan) do
    reply = @adapter.publish(chan, exchange, routing_key, payload, opts)

    {:reply, reply, chan}
  end

  def handle_call({:declare_exchange, exchange}, _from, chan) do
    # TODO: we need to decide who is responsible of deleting the exchange once it is
    # no longer needed
    reply = @adapter.declare_exchange(chan, exchange, type: :direct, durable: true)

    {:reply, reply, chan}
  end

  @impl true
  def handle_info({:DOWN, _, :process, _pid, reason}, state) do
    Logger.warn("RabbitMQ connection lost: #{inspect(reason)}. Trying to reconnect...",
      tag: "events_producer_conn_lost"
    )

    do_connect(state)
  end

  def handle_info(:try_to_connect, state) do
    do_connect(state)
  end

  defp schedule_connect() do
    _ = Logger.warn("Retrying connection in #{@connection_backoff} ms")
    Process.send_after(@connection_backoff, self(), :try_to_connect)
    {:noreply, :not_connected}
  end
end
