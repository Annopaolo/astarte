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

defmodule Astarte.TriggerEngine.Policy do
  use GenServer
  require Logger

  alias Astarte.Core.Triggers.Policy.Handler
  alias Astarte.Core.Triggers.Policy
  alias Astarte.TriggerEngine.Config
  alias AMQP.Basic

  @consumer Config.events_consumer!()

  # API
  def start_link(args \\ []) do
    with {:ok, realm_name} <- Keyword.fetch(args, :realm_name),
         {:ok, policy} <- Keyword.fetch(args, :policy),
         {:ok, pid} <-
           GenServer.start_link(__MODULE__, args, name: via_tuple(realm_name, policy.name)) do
      {:ok, pid}
    else
      :error ->
        # Missing realm or policy in args
        {:error, :no_realm_or_policy_name}

      {:error, {:already_started, pid}} ->
        # Already started, we don't care
        {:ok, pid}

      other ->
        # Relay everything else
        other
    end
  end

  def handle_event(pid, payload, meta, amqp_channel) do
    Logger.debug(
      "policy process #{inspect(pid)} got event, payload: #{inspect(payload)},  meta: #{
        inspect(meta)
      }"
    )

    GenServer.cast(pid, {:handle_event, payload, meta, amqp_channel})
  end

  def get_event_retry_map(pid) do
    Logger.debug("Required event retry map for policy process #{inspect(pid)}")
    GenServer.call(pid, {:get_event_retry_map})
  end

  # Server callbacks

  def init(realm_name: _realm_name, policy: policy) do
    state = %{policy: policy, retry_map: %{}}
    {:ok, state}
  end

  def handle_cast(
        {:handle_event, payload, meta, amqp_channel},
        %{policy: policy, retry_map: retry_map} = state
      ) do
    {headers, _other_meta} = Map.pop(meta, :headers, [])
    headers_map = amqp_headers_to_map(headers)

    event_consumed? = @consumer.consume(payload, headers_map)
    retry_map = Map.update(retry_map, meta.message_id, 1, fn value -> value + 1 end)

    Logger.debug(
      "Message #{meta.message_id} consumed, this is the #{Map.get(retry_map, meta.message_id)}-th time"
    )

    case event_consumed? do
      # All was ok
      :ok ->
        Basic.ack(amqp_channel, meta.delivery_tag)
        retry_map = Map.delete(retry_map, meta.message_id)
        {:noreply, %{policy: policy, retry_map: retry_map}}

      {:http_error, status_code} ->
        with :ok <- retry_sending?(meta.message_id, status_code, policy, retry_map) do
          Basic.nack(amqp_channel, meta.delivery_tag, requeue: true)
          {:noreply, %{policy: policy, retry_map: retry_map}}
        else
          :no ->
            Basic.nack(amqp_channel, meta.delivery_tag, requeue: false)
            retry_map = Map.delete(retry_map, meta.message_id)
            {:noreply, %{policy: policy, retry_map: retry_map}}
        end

      {:error, :trigger_not_found} ->
        Basic.nack(amqp_channel, meta.delivery_tag, requeue: false)
        retry_map = Map.delete(retry_map, meta.message_id)
        {:noreply, %{policy: policy, retry_map: retry_map}}

      {:error, reason} ->
        Logger.warn("Error #{reason} while processing event #{meta.message_id}",
          tag: "event_consume"
        )

        with :ok <- retry_sending?(meta.message_id, nil, policy, retry_map) do
          Basic.nack(amqp_channel, meta.delivery_tag, requeue: true)
          {:noreply, %{policy: policy, retry_map: retry_map}}
        else
          :no ->
            Basic.nack(amqp_channel, meta.delivery_tag, requeue: false)
            retry_map = Map.delete(retry_map, meta.message_id)
            {:noreply, %{policy: policy, retry_map: retry_map}}
        end
    end
  end

  def handle_call({:get_event_retry_map}, %{retry_map: retry_map}) do
    {:ok, retry_map}
  end

  defp retry_sending?(event_id, error_number, policy, retry_map) do
    %Policy{error_handlers: handlers} = policy
    handler = Enum.find(handlers, fn handler -> Handler.includes?(handler, error_number) end)

    res =
      cond do
        handler == nil -> :no
        Handler.discards?(handler) -> :no
        policy.retry_times == nil -> :no
        Map.get(retry_map, event_id) < policy.retry_times -> :ok
        true -> :no
      end

    Logger.debug("Message #{event_id} was processed; scheduled for retry? #{res}")
    res
  end

  defp amqp_headers_to_map(headers) do
    Enum.reduce(headers, %{}, fn {key, _type, value}, acc ->
      Map.put(acc, key, value)
    end)
  end

  defp via_tuple(realm_name, policy_name) do
    {:via, Registry, {Registry.PolicyRegistry, {realm_name, policy_name}}}
  end
end
