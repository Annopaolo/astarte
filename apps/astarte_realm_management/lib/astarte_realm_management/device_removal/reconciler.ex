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
defmodule Astarte.RealmManagement.DeviceRemoval.Reconciler do
  use GenServer

  alias Astarte.RealmManagement.DeviceRemoval.Remover
  alias Astarte.RealmManagement.Queries
  alias Astarte.RealmManagement.DeviceRemoval.DeviceRemoverSupervisor

  require Logger

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init(_init_arg) do
    {:ok, {:continue, []}}
  end

  def delete_device(realm_name, device_id) do
    GenServer.call(__MODULE__, {:delete_device, {realm_name, device_id}})
  end

  def handle_continue(_args, state) do
    devices_to_delete = retrieve_devices_to_delete()

    Enum.each(devices_to_delete, fn {realm_name, device_id} ->
      start_device_deletion(realm_name, device_id)
    end)

    {:noreply, state}
  end

  def handle_call({:delete_device, {realm_name, device_id}}, _from, state) do
    start_device_deletion(realm_name, device_id)

    {:reply, :ok, state}
  end

  defp start_device_deletion(realm_name, device_id) do
    DeviceRemoverSupervisor.start_child(
      Remover.child_spec(%{realm_name: realm_name, device_id: device_id})
    )
  end

  defp retrieve_devices_to_delete() do
    realms = Queries.retrieve_realms!()

    Enum.flat_map(realms, fn %{realm_name: realm_name} ->
      devices = Queries.retrieve_devices_to_delete!(realm_name)
      Enum.map(devices, fn %{device_id: device_id} -> {realm_name, device_id} end)
    end)
  end
end
