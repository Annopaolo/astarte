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
defmodule Astarte.RealmManagement.DeviceRemover do
  alias Astarte.RealmManagement.Queries

  use Task
  require Logger
  alias Astarte.Core.InterfaceDescriptor

  @spec start_link(list) :: {:ok, pid}
  def start_link(args) do
    Task.start_link(__MODULE__, :run, args)
  end

  def run(args) do
    realm_name = Keyword.fetch(args, :realm_name)
    device_id = Keyword.fetch(args, :device_id)

    with {:ok, datastream_keys} <-
           retrieve_individual_datastreams_keys(realm_name, device_id),
         {:ok, _res} <- delete_individual_datastreams(realm_name, datastream_keys),
         {:ok, property_keys} <-
           retrieve_individual_properties_keys(realm_name, device_id),
         {:ok, _res} <- delete_individual_properties(realm_name, property_keys),
         {:ok, introspection} <- retrieve_device_introspection(realm_name, device_id),
         {:ok, object_interfaces} <- filter_object_interfaces(realm_name, introspection),
         object_tables =
           Enum.map(object_interfaces, fn {name, major} ->
             CQLUtils.interface_name_to_table_name(name, major)
           end),
         object_keys = retrieve_object_datastream_keys(realm_name, device_id, object_tables) do
      :ok
    end
  end

  defp retrieve_individual_datastreams_keys(realm_name, device_id) do
    with {:error, reason} <-
           Queries.retrieve_individual_datastreams_keys(realm_name, device_id) do
      _ =
        Logger.warn(
          "Cannot retrieve individual datasream keys for #{device_id}",
          tag: "get_individual_datastream_keys_fail"
        )

      {:error, reason}
    end
  end

  defp delete_individual_datastreams(realm_name, keys) do
    Enum.reduce_while(keys, :ok, fn key, acc ->
      %{
        device_id: device_id,
        interface_id: interface_id,
        endpoint_id: endpoint_id,
        path: path
      } = key

      case do_delete_individual_datastream(
             realm_name,
             device_id,
             interface_id,
             endpoint_id,
             path
           ) do
        :ok ->
          {:cont, acc}

        {:error, reason} ->
          _ =
            Logger.warn(
              "Cannot delete individual datastream for #{device_id}",
              tag: "delete_individual_datastream_fail"
            )

          {:halt, {:error, reason}}
      end
    end)
  end

  defp do_delete_individual_datastream(
         realm_name,
         device_id,
         interface_id,
         endpoint_id,
         path
       ) do
    with {:error, reason} <-
           Queries.delete_individual_datastream(
             realm_name,
             device_id,
             interface_id,
             endpoint_id,
             path
           ) do
      _ =
        Logger.warn(
          "Cannot delete individual datastream for #{device_id} on #{interface_id}, path: #{path}",
          tag: "delete_individual_datastream_fail"
        )

      {:error, reason}
    end
  end

  defp retrieve_individual_properties_keys(realm_name, device_id) do
    with {:error, reason} <-
           Queries.retrieve_individual_properties_keys(realm_name, device_id) do
      _ =
        Logger.warn(
          "Cannot retrieve individual properties keys for #{device_id}",
          tag: "get_individual_properties_keys_fail"
        )

      {:error, reason}
    end
  end

  defp delete_individual_properties(realm_name, keys) do
    Enum.reduce_while(keys, :ok, fn key, acc ->
      %{
        device_id: device_id,
        interface_id: interface_id
      } = key

      case do_delete_individual_property(realm_name, device_id, interface_id) do
        :ok ->
          {:cont, acc}

        {:error, reason} ->
          _ =
            Logger.warn(
              "Cannot delete individual property for #{device_id}",
              tag: "delete_individual_property_fail"
            )

          {:halt, {:error, reason}}
      end
    end)
  end

  defp do_delete_individual_property(realm_name, device_id, interface_id) do
    with {:error, reason} <-
           Queries.delete_individual_property(realm_name, device_id, interface_id) do
      _ =
        Logger.warn(
          "Cannot delete individual property for #{device_id} on #{interface_id}",
          tag: "delete_individual_property_fail"
        )

      {:error, reason}
    end
  end

  defp retrieve_device_introspection(realm_name, device_id) do
    with {:error, reason} <- Queries.retrieve_device_introspection(realm_name, device_id) do
      _ =
        Logger.warn(
          "Cannot retrieve introspection for #{device_id}",
          tag: "get_introspection_fail"
        )

      {:error, reason}
    end
  end

  defp filter_object_interfaces(realm_name, introspection) do
    Enum.reduce_while(introspection, {:ok, []}, fn {interface_name, interface_major}, acc ->
      case Queries.retrieve_interface_descriptor(realm_name, interface_name, interface_major) do
        # TODO check
        {:ok, %InterfaceDescriptor{type: :object}} ->
          {:cont, [{interface_name, interface_major} | acc]}

        {:ok, _} ->
          {:cont, acc}

        {:error, _} ->
          {:halt, :error}
      end
    end)
  end

  defp retrieve_object_datastream_keys(realm_name, device_id, object_tables) do
    Enum.flat_map(object_tables, fn table_name ->
      # TODO check errors
      Queries.retrieve_object_datastream_keys(realm_name, device_id, table_name)
    end)
  end

  defp via_tuple(realm_name, device_id) do
    {:via, Registry, {Registry.DeviceRemover, {realm_name, device_id}}}
  end
end
