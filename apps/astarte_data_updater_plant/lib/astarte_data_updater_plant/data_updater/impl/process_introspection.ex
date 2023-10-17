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

defmodule Astarte.DataUpdaterPlant.DataUpdater.Core.ProcessIntrospection do
  alias Astarte.Core.Device
  alias Astarte.Core.CQLUtils
  alias Astarte.DataUpdaterPlant.TriggersHandler
  alias Astarte.DataUpdaterPlant.DataUpdater.Core.ProcessIntrospection
  alias Astarte.DataUpdaterPlant.DataUpdater.Queries

  require Logger

  use TypedStruct

  typedstruct do
    field :realm, String.t()
    field :device_id, Device.device_id()
    field :encoded_device_id, Device.encoded_device_id()
    field :introspection, map()
    field :interfaces, map()
    field :trigger_id_to_policy_name, map()
    field :db_client, term()
    field :timestamp_ms, DateTime.t()
  end

  @spec handle_incoming_introspection(
          __MODULE__.t(),
          any,
          map()
        ) :: :error | :ok
  def handle_incoming_introspection(
        %ProcessIntrospection{
          realm: realm,
          encoded_device_id: encoded_device_id,
          trigger_id_to_policy_name: trigger_id_to_policy_name,
          timestamp_ms: timestamp_ms
        },
        payload,
        device_triggers
      ) do
    on_introspection_target_with_policy_list =
      Map.get(device_triggers, :on_incoming_introspection, [])
      |> Enum.map(&policy_and_target_from_target(trigger_id_to_policy_name, &1))

    TriggersHandler.incoming_introspection(
      on_introspection_target_with_policy_list,
      realm,
      encoded_device_id,
      payload,
      timestamp_ms
    )
  end

  @spec handle_added_interfaces(__MODULE__.t(), term(), map(), map(), map()) :: :ok
  def handle_added_interfaces(
        process_introspection,
        db_client,
        added_interfaces,
        introspection_minor_map,
        device_triggers
      ) do
    Logger.debug("Adding interfaces to introspection: #{inspect(added_interfaces)}.")

    Enum.each(added_interfaces, fn {interface_name, interface_major} ->
      handle_added_interface(
        process_introspection,
        db_client,
        interface_name,
        interface_major,
        introspection_minor_map,
        device_triggers
      )
    end)
  end

  defp handle_added_interface(
         %ProcessIntrospection{
           realm: realm,
           encoded_device_id: encoded_device_id,
           trigger_id_to_policy_name: trigger_id_to_policy_name,
           timestamp_ms: timestamp_ms
         },
         db_client,
         interface_name,
         interface_major,
         introspection_minor_map,
         device_triggers
       ) do
    :ok =
      maybe_register_device_with_interface(
        db_client,
        encoded_device_id,
        interface_name,
        interface_major
      )

    minor = Map.get(introspection_minor_map, interface_name)

    interface_id = CQLUtils.interface_id(interface_name, interface_major)

    interface_added_target =
      Map.get(device_triggers, {:on_interface_added, interface_id}, []) ++
        Map.get(device_triggers, {:on_interface_added, :any_interface}, [])

    interface_added_target_with_policy_list =
      Enum.map(
        interface_added_target,
        &policy_and_target_from_target(trigger_id_to_policy_name, &1)
      )

    TriggersHandler.interface_added(
      interface_added_target_with_policy_list,
      realm,
      encoded_device_id,
      interface_name,
      interface_major,
      minor,
      timestamp_ms
    )
  end

  @spec handle_removed_interfaces(__MODULE__.t(), term(), map(), map()) :: :ok
  def handle_removed_interfaces(
        process_introspection,
        db_client,
        removed_interfaces,
        device_triggers
      ) do
    Logger.debug("Adding interfaces to introspection: #{inspect(removed_interfaces)}.")

    Enum.each(removed_interfaces, fn {interface_name, interface_major} ->
      handle_removed_interface(
        process_introspection,
        db_client,
        interface_name,
        interface_major,
        device_triggers
      )
    end)
  end

  defp handle_removed_interface(
         %ProcessIntrospection{
           realm: realm,
           encoded_device_id: encoded_device_id,
           timestamp_ms: timestamp_ms,
           trigger_id_to_policy_name: trigger_id_to_policy_name
         },
         db_client,
         interface_name,
         interface_major,
         device_triggers
       ) do
    :ok =
      maybe_unregister_device_with_interface(
        db_client,
        encoded_device_id,
        interface_name,
        interface_major
      )

    interface_id = CQLUtils.interface_id(interface_name, interface_major)

    interface_removed_target =
      Map.get(device_triggers, {:on_interface_removed, interface_id}, []) ++
        Map.get(device_triggers, {:on_interface_removed, :any_interface}, [])

    interface_removed_target_with_policy_list =
      Enum.map(
        interface_removed_target,
        &policy_and_target_from_target(trigger_id_to_policy_name, &1)
      )

    TriggersHandler.interface_removed(
      interface_removed_target_with_policy_list,
      realm,
      encoded_device_id,
      interface_name,
      interface_major,
      timestamp_ms
    )
  end

  def update_device_old_introspection(
        %ProcessIntrospection{
          introspection: introspection,
          device_id: device_id
        },
        db_client,
        added_interfaces,
        removed_interfaces,
        old_minors
      ) do
    readded_introspection = Enum.to_list(added_interfaces)

    old_introspection =
      Enum.reduce(removed_interfaces, %{}, fn {iface, _major}, acc ->
        prev_major = Map.fetch!(introspection, iface)
        prev_minor = Map.get(old_minors, iface, 0)
        Map.put(acc, {iface, prev_major}, prev_minor)
      end)

    :ok = Queries.add_old_interfaces(db_client, device_id, old_introspection)
    :ok = Queries.remove_old_interfaces(db_client, device_id, readded_introspection)
  end

  @spec handle_interface_minor_updated(
          Astarte.DataUpdaterPlant.DataUpdater.Core.ProcessIntrospection.t(),
          map(),
          map(),
          map(),
          map()
        ) :: list
  def handle_interface_minor_updated(
        %ProcessIntrospection{
          timestamp_ms: timestamp_ms,
          realm: realm,
          encoded_device_id: encoded_device_id,
          introspection: introspection,
          trigger_id_to_policy_name: trigger_id_to_policy_name
        },
        introspection_major_map,
        introspection_minor_map,
        old_minors,
        device_triggers
      ) do
    for {interface_name, old_minor} <- old_minors,
        interface_major = Map.fetch!(introspection, interface_name),
        Map.get(introspection_major_map, interface_name) == interface_major,
        new_minor = Map.get(introspection_minor_map, interface_name),
        new_minor != old_minor do
      interface_id = CQLUtils.interface_id(interface_name, interface_major)

      interface_minor_updated_target_with_policy_list =
        Map.get(device_triggers, {:on_interface_minor_updated, interface_id}, [])
        |> Enum.map(&policy_and_target_from_target(trigger_id_to_policy_name, &1))

      TriggersHandler.interface_minor_updated(
        interface_minor_updated_target_with_policy_list,
        realm,
        encoded_device_id,
        interface_name,
        interface_major,
        old_minor,
        new_minor,
        timestamp_ms
      )
    end
  end

  defp maybe_register_device_with_interface(db_client, encoded_device_id, interface_name, 0) do
    Queries.register_device_with_interface(
      db_client,
      encoded_device_id,
      interface_name,
      0
    )
  end

  defp maybe_register_device_with_interface(
         _db_client,
         _device_id,
         _interface_name,
         _interface_major
       ) do
    :ok
  end

  defp maybe_unregister_device_with_interface(db_client, encoded_device_id, interface_name, 0) do
    Queries.register_device_with_interface(
      db_client,
      encoded_device_id,
      interface_name,
      0
    )
  end

  defp maybe_unregister_device_with_interface(
         _db_client,
         _device_id,
         _interface_name,
         _interface_major
       ) do
    :ok
  end

  defp policy_and_target_from_target(
         trigger_id_to_policy_name_map,
         trigger_target
       ) do
    policy_name = trigger_id_to_policy_name_map[trigger_target.parent_trigger_id]
    %{target: trigger_target, policy_name: policy_name}
  end

  def introspection_list_to_major_and_minor_maps(introspection_list) do
    # TODO use `for reduce` when it will be available: https://elixirforum.com/t/introducing-for-let-and-for-reduce/44773
    Enum.reduce(introspection_list, {%{}, %{}}, fn {interface, major, minor},
                                                   {introspection_major_map,
                                                    introspection_minor_map} ->
      introspection_major_map = Map.put(introspection_major_map, interface, major)
      introspection_minor_map = Map.put(introspection_minor_map, interface, minor)

      {introspection_major_map, introspection_minor_map}
    end)
  end

  def compute_introspection_changes(old_introspection, new_introspection) do
    old_sorted_introspection_list = Enum.sort(old_introspection)

    new_sorted_introspection_list = Enum.sort(new_introspection)

    List.myers_difference(old_sorted_introspection_list, new_sorted_introspection_list)
  end

  def updated_interfaces_from_changes(changes_list) do
    Enum.reduce(changes_list, {%{}, %{}}, fn {change_type, changed_interfaces},
                                             {add_acc, rm_acc} ->
      case change_type do
        :ins ->
          changed_map = Enum.into(changed_interfaces, %{})
          {Map.merge(add_acc, changed_map), rm_acc}

        :del ->
          changed_map = Enum.into(changed_interfaces, %{})
          {add_acc, Map.merge(rm_acc, changed_map)}

        :eq ->
          {add_acc, rm_acc}
      end
    end)
  end

  def compute_interfaces_to_drop(old_interfaces, removed_interfaces) do
    remove_interfaces_list = Map.keys(removed_interfaces)

    {interfaces_to_drop_map, _} = Map.split(old_interfaces, remove_interfaces_list)
    Map.keys(interfaces_to_drop_map)
  end
end
