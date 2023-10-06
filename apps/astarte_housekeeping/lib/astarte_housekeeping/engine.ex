#
# This file is part of Astarte.
#
# Copyright 2017-2018 Ispirata Srl
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

defmodule Astarte.Housekeeping.Engine do
  require Logger

  alias Astarte.Housekeeping.Config
  alias Astarte.Housekeeping.Queries

  def create_realm(realm, public_key_pem, replication_factor, opts \\ []) do
    _ =
      Logger.info(
        "Creating new realm.",
        tag: "create_realm",
        realm: realm,
        replication_factor: replication_factor
      )

    Queries.create_realm(realm, public_key_pem, replication_factor, opts)
  end

  def get_health do
    case Queries.check_astarte_health(:quorum) do
      :ok ->
        {:ok, %{status: :ready}}

      {:error, :health_check_bad} ->
        case Queries.check_astarte_health(:one) do
          :ok ->
            {:ok, %{status: :degraded}}

          {:error, :health_check_bad} ->
            {:ok, %{status: :bad}}

          {:error, :database_connection_error} ->
            {:ok, %{status: :error}}
        end

      {:error, :database_connection_error} ->
        {:ok, %{status: :error}}
    end
  end

  def get_realm(realm) do
    Queries.get_realm(realm)
  end

  def delete_realm(realm, opts \\ []) do
    if Config.enable_realm_deletion!() do
      _ = Logger.info("Deleting realm", tag: "delete_realm", realm: realm)

      Queries.delete_realm(realm, opts)
    else
      _ =
        Logger.info("HOUSEKEEPING_ENABLE_REALM_DELETION is disabled, realm will not be deleted.",
          tag: "realm_deletion_disabled",
          realm: realm
        )

      {:error, :realm_deletion_disabled}
    end
  end

  def is_realm_existing(realm) do
    Queries.is_realm_existing(realm)
  end

  def list_realms do
    Queries.list_realms()
  end

  def update_realm(realm_name, update_status) do
    # TODO: what should we do with replication and replication factors?
    # Peraphs fail if they're missing, saying that updating them is
    # not supported?

    %{
      jwt_public_key_pem: new_jwt_public_key_pem,
      device_registration_limit: new_device_registration_limit
    } = update_status

    with {:ok, realm} <- Queries.get_realm(realm_name),
         :ok <- update_jwt_public_key_pem(realm_name, new_jwt_public_key_pem),
         :ok <- update_device_registration_limit(realm_name, new_device_registration_limit) do
      updated_realm =
        realm
        |> Map.put(:jwt_public_key_pem, new_jwt_public_key_pem)
        |> Map.put(:device_registration_limit, new_device_registration_limit)

      {:ok, updated_realm}
    end
  end

  defp update_jwt_public_key_pem(realm_name, new_jwt_public_key_pem) do
    case Queries.update_jwt_public_key_pem(realm_name, new_jwt_public_key_pem) do
      {:ok, %Xandra.Void{}} ->
        :ok

      {:error, reason} ->
        _ =
          Logger.warn(
            "Cannot update JWT public key for realm #{realm_name}, error #{inspect(reason)}",
            tag: "update_public_key_fail"
          )

        {:error, :update_public_key_fail}
    end
  end

  defp update_device_registration_limit(realm_name, new_device_registration_limit) do
    case Queries.update_device_registration_limit(realm_name, new_device_registration_limit) do
      {:ok, %Xandra.Void{}} ->
        :ok

      {:error, reason} ->
        _ =
          Logger.warn(
            "Cannot update device registration limit for realm #{realm_name}, error #{inspect(reason)}",
            tag: "update_device_registration_limit_fail"
          )

        {:error, :update_device_registration_limit_fail}
    end
  end
end
