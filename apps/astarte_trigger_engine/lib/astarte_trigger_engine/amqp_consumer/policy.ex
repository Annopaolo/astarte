#
# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
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

defmodule Astarte.TriggerEngine.AMQPConsumer.Policy do
  alias Astarte.TriggerEngine.AMQPConsumer.AMQPMessageConsumer
  alias Astarte.Core.Triggers.Policy, as: CorePolicy
  alias Astarte.Core.Triggers.PolicyProtobuf.Policy, as: PolicyProto

  alias Astarte.Core.Triggers.Policy.Handler
  alias Astarte.Core.Triggers.Policy.ErrorKeyword
  alias Astarte.Core.Triggers.PolicyProtobuf.Policy, as: PolicyProto

  # TODO unit test this
  # TODO typespec
  def consumers_difference(all_policies, existing_consumers) do
    all_policies_map =
      Enum.into(%{}, all_policies, fn %{realm: realm, policy: policy} ->
        {{realm, policy.name}, policy}
      end)

    policies_to_create =
      Map.drop(all_policies_map, existing_consumers)
      |> Enum.into([], fn {{realm, _name}, policy} -> %{realm: realm, policy: policy} end)

    consumers_to_remove =
      Enum.reject(existing_consumers, fn policy_key ->
        policy_key not in Map.keys(all_policies_map)
      end)

    %{create: policies_to_create, remove: consumers_to_remove}
  end

  # TODO typespec
  def to_consumer(realm, policy) do
    {AMQPMessageConsumer, [realm_name: realm, policy: policy, pool_id: :events_consumer_pool]}
  end

  # TODO unit test this
  # TODO typespec
  def to_policy_list(realm_name, raw_list) do
    raw_list =
      raw_list
      |> Enum.map(fn policy_proto ->
        policy = policy_proto |> PolicyProto.decode() |> CorePolicy.from_policy_proto!()
        %{realm: realm_name, policy: policy}
      end)

    [%{realm: realm_name, policy: default_policy()} | raw_list]
  end

  # we need this because the default policy cannot be installed
  defp default_policy() do
    %CorePolicy{
      name: "@default",
      # Do not limit default queue size so that we don't break Astarte < 1.1 behaviour
      maximum_capacity: nil,
      error_handlers: [
        %Handler{on: %ErrorKeyword{keyword: "any_error"}, strategy: "discard"}
      ]
    }
  end
end
