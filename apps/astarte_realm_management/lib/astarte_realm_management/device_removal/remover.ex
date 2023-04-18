#
# This file is part of Astarte.
#
# Copyright 20230 SECO Mind Srl
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

defmodule Astarte.RealmManagement.DeviceRemoval.Remover do
  use Task
  require Logger
  alias Astarte.Core.InterfaceDescriptor
  alias Astarte.Core.CQLUtils
  alias Astarte.RealmManagement.Queries
  alias Astarte.Core.Device

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :run, [opts]},
      type: :worker,
      restart: :transient,
      shutdown: 5000
    }
  end

  def run(%{realm_name: realm_name, device_id: device_id}) do
    {:ok, decoded_device_id} = Device.decode_device_id(device_id)

    datastream_keys = Queries.retrieve_individual_datastreams_keys!(realm_name, decoded_device_id)
    :ok = delete_individual_datastreams!(realm_name, datastream_keys)
    property_keys = Queries.retrieve_individual_properties_keys!(realm_name, decoded_device_id)
    :ok = delete_individual_properties!(realm_name, property_keys)
    introspection = Queries.retrieve_device_introspection!(realm_name, decoded_device_id)
    object_interfaces = filter_object_interfaces!(realm_name, introspection)
    object_tables = Enum.map(object_interfaces, &object_interface_to_table_name/1)

    object_keys_with_table_name =
      retrieve_object_datastream_keys!(
        realm_name,
        decoded_device_id,
        object_tables
      )

    :ok = delete_object_datastreams!(realm_name, object_keys_with_table_name)
    aliases = Queries.retrieve_aliases!(realm_name, decoded_device_id)
    :ok = Enum.each(aliases, &Queries.delete_alias_values!(realm_name, &1))
    group_keys = Queries.retrieve_groups_keys!(realm_name, decoded_device_id)
    :ok = delete_groups!(realm_name, group_keys)
    kv_store_keys = Queries.retrieve_kv_store_keys!(realm_name, device_id)
    :ok = delete_kv_store_values!(realm_name, kv_store_keys)
    %Xandra.Void{} = Queries.delete_device!(realm_name, decoded_device_id)
    %Xandra.Void{} = Queries.set_device_deleted!(realm_name, decoded_device_id)
    :ok
  end

  defp delete_individual_datastreams!(realm_name, keys) do
    Enum.each(keys, fn key ->
      %{
        device_id: device_id,
        interface_id: interface_id,
        endpoint_id: endpoint_id,
        path: path
      } = key

      Queries.delete_individual_datastream_values!(
        realm_name,
        device_id,
        interface_id,
        endpoint_id,
        path
      )
    end)
  end

  defp delete_individual_properties!(realm_name, keys) do
    Enum.each(keys, fn key ->
      %{
        device_id: device_id,
        interface_id: interface_id
      } = key

      Queries.delete_individual_properties_values!(realm_name, device_id, interface_id)
    end)
  end

  defp filter_object_interfaces!(realm_name, introspection) do
    Enum.filter(introspection, fn {interface_name, interface_major} ->
      case Queries.retrieve_interface_descriptor!(realm_name, interface_name, interface_major) do
        # TODO check
        %InterfaceDescriptor{type: :object} -> true
        _ -> false
      end
    end)
  end

  defp object_interface_to_table_name({interface_name, interface_major}) do
    CQLUtils.interface_name_to_table_name(interface_name, interface_major)
  end

  defp retrieve_object_datastream_keys!(realm_name, device_id, object_tables) do
    Enum.map(object_tables, fn table_name ->
      # TODO check
      keys =
        Queries.retrieve_object_datastream_keys!(
          realm_name,
          device_id,
          table_name
        )

      Map.put(keys, :table_name, table_name)
    end)
  end

  defp delete_object_datastreams!(realm_name, keys) do
    Enum.each(keys, fn key ->
      %{
        device_id: device_id,
        path: path,
        table_name: table_name
      } = key

      Queries.delete_object_datastream_values!(realm_name, device_id, path, table_name)
    end)
  end

  defp delete_groups!(realm_name, keys) do
    Enum.each(keys, fn key ->
      %{
        device_id: device_id,
        group_name: group_name,
        insertion_uuid: insertion_uuid
      } = key

      Queries.delete_group_values!(realm_name, device_id, group_name, insertion_uuid)
    end)
  end

  defp delete_kv_store_values!(realm_name, keys) do
    Enum.each(keys, fn element ->
      %{
        group: group_name,
        key: key
      } = element

      Queries.delete_kv_store_values!(realm_name, group_name, key)
    end)
  end

  defp via_tuple(realm_name, device_id) do
    {:via, Registry, {Registry.DeviceRemover, {realm_name, device_id}}}
  end
end
