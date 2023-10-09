#
# This file is part of Astarte.
#
# Copyright 2023 SECO Mind Srl
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

defmodule Astarte.DataUpdaterPlant.DataUpdater.Core.TimeBasedActions do
  alias Astarte.DataUpdaterPlant.TriggersHandler
  alias Astarte.DataUpdaterPlant.DataUpdater.Queries
  alias Astarte.DataUpdaterPlant.DataUpdater.Impl.Core.Triggers
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.AMQPTriggerTarget
  alias Astarte.Core.Triggers.DataTrigger
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.Utils, as: SimpleTriggersProtobufUtils
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.AMQPTriggerTarget
  alias Astarte.DataUpdaterPlant.DataUpdater.EventTypeUtils
  alias Astarte.DataUpdaterPlant.DataUpdater.Queries
  alias Astarte.DataUpdaterPlant.TriggersHandler
  alias Astarte.DataUpdaterPlant.TriggerPolicy.Queries, as: PolicyQueries
  alias Astarte.DataUpdaterPlant.DataUpdater.Impl.Core.Triggers

  require Logger

  @paths_cache_size 32
  @interface_lifespan_decimicroseconds 60 * 10 * 1000 * 10000
  @device_triggers_lifespan_decimicroseconds 60 * 10 * 1000 * 10000
  @groups_lifespan_decimicroseconds 60 * 10 * 1000 * 10000

  defstruct [
    :realm,
    :device_id,
    :encoded_device_id,
    :introspection,
    :interfaces,
    :trigger_id_to_policy_name,
    :db_client,
    :timestamp_ms
  ]

  @spec reload_groups_on_expiry(number, any, any, any) ::
          :up_to_date | {:reload, %{groups: list, last_groups_refresh: any}}
  def reload_groups_on_expiry(last_groups_refresh, timestamp, device_id, db_client) do
    if last_groups_refresh + @groups_lifespan_decimicroseconds <= timestamp do
      {:ok, groups} = Queries.get_device_groups(db_client, device_id)

      {:reload, %{last_groups_refresh: timestamp, groups: groups}}
    else
      :up_to_date
    end
  end

  @spec purge_expired_interfaces(map, map, map, map, map, any) :: %{
          data_triggers: map,
          interface_ids_to_name: map,
          interfaces: map,
          interfaces_by_expiry: list,
          mappings: map
        }
  def purge_expired_interfaces(
        interfaces,
        data_triggers,
        mappings,
        interface_ids_to_name,
        interfaces_by_expiry,
        timestamp
      ) do
    expired =
      Enum.take_while(interfaces_by_expiry, fn {expiry, _interface} ->
        expiry <= timestamp
      end)

    new_interfaces_by_expiry = Enum.drop(interfaces_by_expiry, length(expired))

    interfaces_to_drop =
      for {_exp, iface} <- expired do
        iface
      end

    drop_interfaces(
      interfaces,
      data_triggers,
      mappings,
      interface_ids_to_name,
      interfaces_to_drop
    )
    |> Map.put(:interfaces_by_expiry, new_interfaces_by_expiry)
  end

  @spec drop_interfaces(map(), map(), map(), map(), list()) :: %{
          data_triggers: map(),
          interface_ids_to_name: map(),
          interfaces: map(),
          mappings: map()
        }
  def drop_interfaces(
        interfaces,
        data_triggers,
        mappings,
        interface_ids_to_name,
        interfaces_to_drop
      ) do
    iface_ids_to_drop =
      Enum.filter(interfaces_to_drop, &Map.has_key?(interfaces, &1))
      |> Enum.map(fn iface ->
        Map.fetch!(interfaces, iface).interface_id
      end)

    updated_triggers =
      Enum.reduce(iface_ids_to_drop, data_triggers, fn interface_id, data_triggers ->
        Enum.reject(data_triggers, fn {{_event_type, iface_id, _endpoint}, _val} ->
          iface_id == interface_id
        end)
        |> Enum.into(%{})
      end)

    updated_mappings =
      Enum.reduce(iface_ids_to_drop, mappings, fn interface_id, mappings ->
        Enum.reject(mappings, fn {_endpoint_id, mapping} ->
          mapping.interface_id == interface_id
        end)
        |> Enum.into(%{})
      end)

    updated_ids =
      Enum.reduce(iface_ids_to_drop, interface_ids_to_name, fn interface_id, ids ->
        Map.delete(ids, interface_id)
      end)

    updated_interfaces =
      Enum.reduce(interfaces_to_drop, interfaces, fn iface, ifaces ->
        Map.delete(ifaces, iface)
      end)

    %{
      interfaces: updated_interfaces,
      interface_ids_to_name: updated_ids,
      mappings: updated_mappings,
      data_triggers: updated_triggers
    }
  end

  # TODO test
  @spec forget_any_interface_data_triggers(map) :: map
  def forget_any_interface_data_triggers(data_triggers) do
    for {{_type, iface_id, _endpoint} = key, value} <- data_triggers,
        iface_id != :any_interface,
        into: %{} do
      {key, value}
    end
  end

  def load_trigger(
        realm_name,
        data_triggers,
        trigger_id_to_policy_name,
        interfaces,
        interface_ids_to_name,
        {:data_trigger, proto_buf_data_trigger},
        trigger_target
      ) do
    new_data_trigger =
      SimpleTriggersProtobufUtils.simple_trigger_to_data_trigger(proto_buf_data_trigger)

    event_type = EventTypeUtils.pretty_data_trigger_type(proto_buf_data_trigger.data_trigger_type)

    data_trigger_key =
      Triggers.data_trigger_to_key(
        interfaces,
        interface_ids_to_name,
        new_data_trigger,
        event_type
      )

    existing_triggers_for_key = Map.get(data_triggers, data_trigger_key, [])

    # Extract all the targets belonging to the (eventual) existing congruent trigger
    congruent_targets =
      existing_triggers_for_key
      |> Enum.filter(&DataTrigger.are_congruent?(&1, new_data_trigger))
      |> Enum.flat_map(fn congruent_trigger -> congruent_trigger.trigger_targets end)

    new_targets = [trigger_target | congruent_targets]
    new_data_trigger_with_targets = %{new_data_trigger | trigger_targets: new_targets}

    # Register the new target
    :ok = TriggersHandler.register_target(trigger_target)

    # Replace the (eventual) congruent existing trigger with the new one
    new_data_triggers_for_key = [
      new_data_trigger_with_targets
      | Enum.reject(
          existing_triggers_for_key,
          &DataTrigger.are_congruent?(&1, new_data_trigger_with_targets)
        )
    ]

    next_data_triggers = Map.put(data_triggers, data_trigger_key, new_data_triggers_for_key)

    %{
      data_triggers: Map.put(data_triggers, :data_triggers, next_data_triggers),
      trigger_id_to_policy_name:
        maybe_cache_trigger_policy(realm_name, trigger_id_to_policy_name, trigger_target)
    }
  end

  # TODO: implement on_empty_cache_received
  def load_trigger(
        realm_name,
        device_triggers,
        trigger_id_to_policy_name,
        {:device_trigger, proto_buf_device_trigger},
        trigger_target
      ) do
    # device event type is one of
    # :on_device_connected, :on_device_disconnected, :on_device_empty_cache_received, :on_device_error,
    # :on_incoming_introspection, :on_interface_added, :on_interface_removed, :on_interface_minor_updated
    event_type =
      EventTypeUtils.pretty_device_event_type(proto_buf_device_trigger.device_event_type)

    # introspection triggers have a pair as key, standard device ones do not
    trigger_key = Triggers.device_trigger_to_key(proto_buf_device_trigger, event_type)

    existing_trigger_targets = Map.get(device_triggers, trigger_key, [])

    new_targets = [trigger_target | existing_trigger_targets]

    # Register the new target
    :ok = TriggersHandler.register_target(trigger_target)

    next_device_triggers = Map.put(device_triggers, trigger_key, new_targets)

    %{
      device_triggers: Map.put(device_triggers, :device_triggers, next_device_triggers),
      trigger_id_to_policy_name:
        maybe_cache_trigger_policy(realm_name, trigger_id_to_policy_name, trigger_target)
    }
  end

  defp maybe_cache_trigger_policy(realm_name, trigger_id_to_policy_name, %AMQPTriggerTarget{
         parent_trigger_id: parent_trigger_id
       }) do
    case PolicyQueries.retrieve_policy_name(
           realm_name,
           parent_trigger_id
         ) do
      {:ok, policy_name} ->
        next_trigger_id_to_policy_name =
          Map.put(trigger_id_to_policy_name, parent_trigger_id, policy_name)

        %{trigger_id_to_policy_name: next_trigger_id_to_policy_name}

        # @default policy is not installed, so here are triggers without policy
        %{trigger_id_to_policy_name: trigger_id_to_policy_name}
    end
  end
end
