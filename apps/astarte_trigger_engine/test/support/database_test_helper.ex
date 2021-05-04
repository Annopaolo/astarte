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

defmodule Astarte.TriggerEngine.DatabaseTestHelper do
  require Logger

  alias Astarte.Core.Triggers.Policy
  alias Astarte.Core.Triggers.Policy.{KeywordError, RangeError, Handler}
  alias Astarte.Core.Triggers.PolicyProtobuf.Policy, as: PolicyProto

  @test_realm "autotestrealm"

  def create_db do
    create_autotestrealm_statement = """
    CREATE KEYSPACE autotestrealm
      WITH
      replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND
      durable_writes = true;
    """

    create_kv_store_table_statement = """
    CREATE TABLE autotestrealm.kv_store (
      group varchar,
      key varchar,
      value blob,

      PRIMARY KEY ((group), key)
    );
    """

    with {:ok, _} <- Xandra.Cluster.execute(:xandra, create_autotestrealm_statement),
         {:ok, _} <- Xandra.Cluster.execute(:xandra, create_kv_store_table_statement) do
      :ok
    else
      {:error, error} ->
        Logger.error("Database initialization error: #{inspect(error)}", tag: :db_init_error)
        {:error, error}
    end
  end

  def populate_policies_db do
    {name1, policy1} =
      %Policy{
        name: "apolicy",
        maximum_capacity: 100,
        error_handlers: [
          %Handler{on: %KeywordError{keyword: "client_error"}, strategy: "retry"},
          %Handler{on: %RangeError{error_codes: [500, 501, 503]}, strategy: "discard"}
        ],
        retry_times: 10
      }
      |> Policy.to_policy_proto()
      |> PolicyProto.encode()
      |> (fn x -> {"apolicy", x} end).()

    {name2, policy2} =
      %Policy{
        name: "anotherpolicy",
        maximum_capacity: 100,
        error_handlers: [
          %Handler{on: %KeywordError{keyword: "any_error"}, strategy: "retry"}
        ],
        retry_times: 1
      }
      |> Policy.to_policy_proto()
      |> PolicyProto.encode()
      |> (fn x -> {"anotherpolicy", x} end).()

    insert_policy_statement =
      "INSERT INTO #{@test_realm}.kv_store (group, key, value) VALUES ('trigger_policy', :name, :proto)"

    with {:ok, prepared} <- Xandra.Cluster.prepare(:xandra, insert_policy_statement),
         {:ok, _res} <-
           Xandra.Cluster.execute(:xandra, prepared, %{"name" => name1, "proto" => policy1}),
         {:ok, _res} <-
           Xandra.Cluster.execute(:xandra, prepared, %{"name" => name2, "proto" => policy2}) do
      :ok
    end
  end

  def test_realm, do: @test_realm

  def drop_db do
    drop_statement = "DROP KEYSPACE #{@test_realm}"
    Xandra.Cluster.execute!(:xandra, drop_statement)
    :ok
  end
end
