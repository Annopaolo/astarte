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

defmodule Astarte.RealmManagement.API.Triggers.Policies do
  alias Astarte.Core.Triggers.Policy
  alias Astarte.RealmManagement.API.RPC.RealmManagement

  @doc """
  Returns the list of trigger policies.
  """
  def list_trigger_policies(realm_name) do
    RealmManagement.get_trigger_policies_list(realm_name)
  end

  # TODO doc
  def get_trigger_policy_source(realm_name, policy_name) do
    RealmManagement.get_trigger_policy_source(realm_name, policy_name)
  end

  @doc """
  Creates a trigger policy.

  ## Examples

      iex> create_trigger_policy(%{field: value})
      {:ok, %Policy{}}

      iex> create_trigger_policy(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_trigger_policy(realm_name, params) do
    changeset = Policy.changeset(%Policy{}, params)

    with {:ok, %Policy{} = policy} <- Ecto.Changeset.apply_action(changeset, :insert),
         {:ok, policy_source} <- Jason.encode(policy),
         {:ok, :started} <- RealmManagement.install_trigger_policy(realm_name, policy_source) do
      {:ok, policy}
    end
  end

  @doc """
  Deletes a Trigger policy.

  ## Examples

      iex> delete_trigger_policy(trigger_policy)
      {:ok, %Policy{}}

      iex> delete_trigger_policy(trigger_policy)
      {:error, trigger_policy_not_found}

  """
  def delete_trigger_policy(realm_name, policy_name, _attrs \\ %{}) do
    RealmManagement.delete_trigger_policy(realm_name, policy_name)
  end
end
