#
# This file is part of Astarte.
#
# Copyright 2017 Ispirata Srl
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

defmodule Astarte.RealmManagement.Queries do
  require CQEx
  require Logger
  alias Astarte.Core.AstarteReference
  alias Astarte.Core.CQLUtils
  alias Astarte.Core.Device
  alias Astarte.Core.Interface, as: InterfaceDocument
  alias Astarte.Core.InterfaceDescriptor
  alias Astarte.Core.Interface.Aggregation
  alias Astarte.Core.Interface.Ownership
  alias Astarte.Core.Interface.Type, as: InterfaceType
  alias Astarte.Core.Mapping
  alias Astarte.Core.Mapping.DatabaseRetentionPolicy
  alias Astarte.Core.Mapping.Reliability
  alias Astarte.Core.Mapping.Retention
  alias Astarte.Core.Mapping.ValueType
  alias Astarte.Core.StorageType
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.SimpleTriggerContainer
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.TaggedSimpleTrigger
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.TriggerTargetContainer
  alias Astarte.Core.Triggers.Trigger
  alias Astarte.Core.Triggers.PolicyProtobuf.PolicyProto

  @max_batch_queries 32

  @insert_into_interfaces """
    INSERT INTO interfaces
      (name, major_version, minor_version, interface_id, storage_type, storage, type, ownership, aggregation, automaton_transitions, automaton_accepting_states, description, doc)
      VALUES (:name, :major_version, :minor_version, :interface_id, :storage_type, :storage, :type, :ownership, :aggregation, :automaton_transitions, :automaton_accepting_states, :description, :doc)
  """

  @create_datastream_individual_multiinterface_table """
    CREATE TABLE IF NOT EXISTS individual_datastreams (
      device_id uuid,
      interface_id uuid,
      endpoint_id uuid,
      path varchar,
      value_timestamp timestamp,
      reception_timestamp timestamp,
      reception_timestamp_submillis smallint,

      double_value double,
      integer_value int,
      boolean_value boolean,
      longinteger_value bigint,
      string_value varchar,
      binaryblob_value blob,
      datetime_value timestamp,
      doublearray_value list<double>,
      integerarray_value list<int>,
      booleanarray_value list<boolean>,
      longintegerarray_value list<bigint>,
      stringarray_value list<varchar>,
      binaryblobarray_value list<blob>,
      datetimearray_value list<timestamp>,

      PRIMARY KEY((device_id, interface_id, endpoint_id, path), value_timestamp, reception_timestamp, reception_timestamp_submillis)
    )
  """

  @create_interface_table_with_object_aggregation """
    CREATE TABLE :interface_name (
      device_id uuid,
      path varchar,

      :value_timestamp,
      reception_timestamp timestamp,
      reception_timestamp_submillis smallint,
      :columns,

      PRIMARY KEY((device_id, path), :key_timestamp reception_timestamp, reception_timestamp_submillis)
    )
  """

  @query_jwt_public_key_pem """
    SELECT blobAsVarchar(value)
    FROM kv_store
    WHERE group='auth' AND key='jwt_public_key_pem';
  """

  @query_insert_jwt_public_key_pem """
  INSERT INTO kv_store (group, key, value)
  VALUES ('auth', 'jwt_public_key_pem', varcharAsBlob(:pem));
  """

  defp create_one_object_columns_for_mappings(mappings) do
    for %Mapping{endpoint: endpoint, value_type: value_type} <- mappings do
      column_name = CQLUtils.endpoint_to_db_column_name(endpoint)
      cql_type = CQLUtils.mapping_value_type_to_db_type(value_type)
      "#{column_name} #{cql_type}"
    end
    |> Enum.join(~s(,\n))
  end

  defp create_interface_table(
         :individual,
         :multi,
         %InterfaceDescriptor{type: :properties},
         _mappings
       ) do
    {:multi_interface_individual_properties_dbtable, "individual_properties", ""}
  end

  defp create_interface_table(
         :individual,
         :multi,
         %InterfaceDescriptor{type: :datastream},
         _mappings
       ) do
    {:multi_interface_individual_datastream_dbtable, "individual_datastreams",
     @create_datastream_individual_multiinterface_table}
  end

  defp create_interface_table(:object, :one, interface_descriptor, mappings) do
    table_name =
      CQLUtils.interface_name_to_table_name(
        interface_descriptor.name,
        interface_descriptor.major_version
      )

    columns = create_one_object_columns_for_mappings(mappings)

    [%Mapping{explicit_timestamp: explicit_timestamp} | _tail] = mappings

    {value_timestamp, key_timestamp} =
      if explicit_timestamp do
        {"value_timestamp timestamp,", "value_timestamp,"}
      else
        {"", ""}
      end

    create_table_statement =
      @create_interface_table_with_object_aggregation
      |> String.replace(":interface_name", table_name)
      |> String.replace(":value_timestamp", value_timestamp)
      |> String.replace(":columns", columns)
      |> String.replace(":key_timestamp", key_timestamp)

    {:one_object_datastream_dbtable, table_name, create_table_statement}
  end

  defp execute_batch(queries) when length(queries) < @max_batch_queries do
    Xandra.Cluster.run(:xandra, fn conn ->
      do_execute_batch(conn, queries)
    end)
  end

  defp do_execute_batch(conn, queries) do
    batch =
      Enum.reduce(queries, Xandra.Batch.new(:logged), fn {statement, params}, batch ->
        Xandra.Batch.add(batch, statement, params)
      end)

    with {:ok, _result} <- Xandra.execute(conn, batch, consistency: :each_quorum) do
      :ok
    else
      {:error, reason} ->
        _ =
          Logger.warn("Failed batch due to database error: #{inspect(reason)}.", tag: "db_error")

        {:error, :database_error}
    end
  end

  defp execute_batch(queries) do
    _ =
      Logger.debug(
        "Trying to run #{inspect(length(queries))} queries, not running in batched mode."
      )

    Enum.reduce_while(queries, :ok, fn {statement, params}, _acc ->
      Xandra.Cluster.run(:xandra, fn conn ->
        with {:ok, _result} <- Xandra.execute(conn, statement, params) do
          {:cont, :ok}
        else
          {:error, err} ->
            _ =
              Logger.warn(
                "Failed due to database error: #{inspect(err)}. Changes will not be undone!",
                tag: "db_error"
              )

            {:halt, {:error, :database_error}}
        end
      end)
    end)
  end

  def check_astarte_health(consistency) do
    schema_statement = """
      SELECT count(value)
      FROM astarte.kv_store
      WHERE group='astarte' AND key='schema_version'
    """

    # no-op, just to check if nodes respond
    # no realm name can contain '_', '^'
    realms_statement = """
    SELECT *
    FROM astarte.realms
    WHERE realm_name='_invalid^name_'
    """

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, _result} <- Xandra.execute(conn, schema_statement, consistency: consistency),
           {:ok, _result} <- Xandra.execute(conn, realms_statement, consistency: consistency) do
        :ok
      else
        {:error, err} ->
          _ = Logger.warn("Health is not good, reason: #{inspect(err)}.", tag: "health_check_bad")

          {:error, :health_check_bad}
      end
    end)
  end

  def install_new_interface(interface_document, automaton) do
    interface_descriptor = InterfaceDescriptor.from_interface(interface_document)

    %InterfaceDescriptor{
      interface_id: interface_id,
      name: interface_name,
      major_version: major,
      minor_version: minor,
      type: interface_type,
      ownership: interface_ownership,
      aggregation: aggregation
    } = interface_descriptor

    %InterfaceDocument{
      description: description,
      doc: doc
    } = interface_document

    table_type =
      if aggregation == :individual do
        :multi
      else
        :one
      end

    {storage_type, table_name, create_table_statement} =
      create_interface_table(
        aggregation,
        table_type,
        interface_descriptor,
        interface_document.mappings
      )

    {:ok, _} =
      if create_table_statement != "" do
        _ = Logger.info("Creating new interface table.", tag: "create_interface_table")

        {:ok, _res} =
          Xandra.Cluster.run(:xandra, fn conn ->
            CSystem.run_with_schema_agreement(conn, fn ->
              Xandra.execute(conn, create_table_statement)
            end)
          end)
      else
        {:ok, nil}
      end

    {transitions, accepting_states_no_ids} = automaton

    transitions_bin = :erlang.term_to_binary(transitions)

    accepting_states_bin =
      accepting_states_no_ids
      |> replace_automaton_acceptings_with_ids(interface_name, major)
      |> :erlang.term_to_binary()

    insert_interface_params = %{
      name: interface_name,
      major_version: major,
      minor_version: minor,
      interface_id: interface_id,
      storage_type: StorageType.to_int(storage_type),
      storage: table_name,
      type: InterfaceType.to_int(interface_type),
      ownership: Ownership.to_int(interface_ownership),
      aggregation: Aggregation.to_int(aggregation),
      automaton_transitions: transitions_bin,
      automaton_accepting_states: accepting_states_bin,
      description: description,
      doc: doc
    }

    # TODO |> DatabaseQuery.consistency(:each_quorum)

    insert_endpoints =
      Enum.map(
        interface_document.mappings,
        &insert_mapping_query(interface_id, interface_name, major, minor, interface_type, &1)
      )

    execute_batch([
      {@insert_into_interfaces, insert_interface_params} | insert_endpoints
    ])
  end

  defp insert_mapping_query(interface_id, interface_name, major, minor, interface_type, mapping) do
    insert_mapping_statement = """
    INSERT INTO endpoints
    (
      interface_id, endpoint_id, interface_name, interface_major_version, interface_minor_version,
      interface_type, endpoint, value_type, reliability, retention, database_retention_policy,
      database_retention_ttl, expiry, allow_unset, explicit_timestamp, description, doc
    )
    VALUES (
      :interface_id, :endpoint_id, :interface_name, :interface_major_version, :interface_minor_version,
      :interface_type, :endpoint, :value_type, :reliability, :retention, :database_retention_policy,
      :database_retention_ttl, :expiry, :allow_unset, :explicit_timestamp, :description, :doc
    )
    """

    insert_mapping_params = %{
      interface_id: interface_id,
      endpoint_id: mapping.endpoint_id,
      interface_name: interface_name,
      interface_major_version: major,
      interface_minor_version: minor,
      interface_type: InterfaceType.to_int(interface_type),
      endpoint: mapping.endpoint,
      value_type: ValueType.to_int(mapping.value_type),
      reliability: Reliability.to_int(mapping.reliability),
      retention: Retention.to_int(mapping.retention),
      database_retention_policy:
        DatabaseRetentionPolicy.to_int(mapping.database_retention_policy),
      database_retention_ttl: mapping.database_retention_ttl,
      expiry: mapping.expiry,
      allow_unset: mapping.allow_unset,
      explicit_timestamp: mapping.explicit_timestamp,
      description: mapping.description,
      doc: mapping.doc
    }

    # TODO |> DatabaseQuery.consistency(:each_quorum)
    {insert_mapping_statement, insert_mapping_params}
  end

  # TODO: this was needed when Cassandra used to generate endpoint IDs
  # it might be a good idea to drop this and generate those IDs in A.C.Mapping.EndpointsAutomaton
  defp replace_automaton_acceptings_with_ids(accepting_states, interface_name, major) do
    Enum.reduce(accepting_states, %{}, fn state, new_states ->
      {state_index, endpoint} = state

      Map.put(new_states, state_index, CQLUtils.endpoint_id(interface_name, major, endpoint))
    end)
  end

  def update_interface(interface_descriptor, new_mappings, automaton, description, doc) do
    %InterfaceDescriptor{
      name: interface_name,
      major_version: major,
      minor_version: minor,
      type: interface_type,
      interface_id: interface_id
    } = interface_descriptor

    {automaton_transitions, automaton_accepting_states_no_ids} = automaton

    automaton_accepting_states_bin =
      automaton_accepting_states_no_ids
      |> replace_automaton_acceptings_with_ids(interface_name, major)
      |> :erlang.term_to_binary()

    automaton_transitions_bin = :erlang.term_to_binary(automaton_transitions)

    update_interface_statement = """
    UPDATE interfaces
    SET minor_version=:minor_version, automaton_accepting_states=:automaton_accepting_states,
      automaton_transitions = :automaton_transitions, description = :description, doc = :doc
    WHERE name=:name AND major_version=:major_version
    """

    update_interface_params = %{
      name: interface_name,
      major_version: major,
      minor_version: minor,
      automaton_accepting_states: automaton_accepting_states_bin,
      automaton_transitions: automaton_transitions_bin,
      description: description,
      doc: doc
    }

    # TODO |> DatabaseQuery.consistency(:each_quorum)

    insert_mapping_queries =
      Enum.map(
        new_mappings,
        &insert_mapping_query(interface_id, interface_name, major, minor, interface_type, &1)
      )

    execute_batch([{update_interface_statement, update_interface_params} | insert_mapping_queries])
  end

  def update_interface_storage(_interface_descriptor, []) do
    # No new mappings, nothing to do
    :ok
  end

  def update_interface_storage(
        %InterfaceDescriptor{storage_type: :one_object_datastream_dbtable, storage: table_name} =
          _interface_descriptor,
        new_mappings
      ) do
    add_cols = create_one_object_columns_for_mappings(new_mappings)

    _ =
      Logger.debug("Interface update: going to add #{inspect(add_cols)} to #{table_name}.",
        tag: "db_interface_add_table_cols"
      )

    update_storage_statement = """
    ALTER TABLE #{table_name}
    ADD (#{add_cols})
    """

    Xandra.Cluster.run(:xandra, fn conn ->
      # See https://hexdocs.pm/xandra/Xandra.SchemaChange.html for more
      with {:ok, %Xandra.SchemaChange{effect: "UPDATED", target: "TABLE"} = _result} <-
             Xandra.execute(conn, update_storage_statement) do
        :ok
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def update_interface_storage(_interface_descriptor, _new_mappings) do
    :ok
  end

  def delete_interface(interface_name, interface_major_version) do
    _ =
      Logger.info("Delete interface.",
        interface: interface_name,
        interface_major: interface_major_version,
        tag: "db_delete_interface"
      )

    delete_endpoints_statement = "DELETE FROM endpoints WHERE interface_id=:interface_id"

    interface_id = CQLUtils.interface_id(interface_name, interface_major_version)

    delete_endpoints_params = %{interface_id: interface_id}
    # TODO |> DatabaseQuery.consistency(:each_quorum)

    delete_interface_statement =
      "DELETE FROM interfaces WHERE name=:name AND major_version=:major"

    delete_interface_params = %{
      name: interface_name,
      major: interface_major_version
    }

    # TODO |> DatabaseQuery.consistency(:each_quorum)

    execute_batch([
      {delete_endpoints_statement, delete_endpoints_params},
      {delete_interface_statement, delete_interface_params}
    ])
  end

  def delete_interface_storage(
        %InterfaceDescriptor{
          storage_type: :one_object_datastream_dbtable,
          storage: table_name
        } = _interface_descriptor
      ) do
    delete_statement = "DROP TABLE IF EXISTS #{table_name}"

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, _res} <- Xandra.execute(conn, delete_statement) do
        _ = Logger.info("Deleted #{table_name} table.", tag: "db_delete_interface_table")
        :ok
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def delete_interface_storage(%InterfaceDescriptor{} = interface_descriptor) do
    with {:ok, result} <- devices_with_data_on_interface(interface_descriptor.name) do
      Enum.reduce_while(result, :ok, fn %{key: encoded_device_id}, _acc ->
        with {:ok, device_id} <- Device.decode_device_id(encoded_device_id),
             :ok <- delete_values(device_id, interface_descriptor) do
          {:cont, :ok}
        else
          {:error, reason} ->
            {:halt, {:error, reason}}
        end
      end)
    end
  end

  def is_any_device_using_interface?(interface_name) do
    devices_query_statement = "SELECT key FROM kv_store WHERE group=:group_name LIMIT 1"

    # TODO: validate interface name?
    devices_query_params = %{
      group_name: "devices-by-interface-#{interface_name}-v0"
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, devices_query_statement, devices_query_params,
               consistency: :quorum
             ),
           [%{key: _device_id}] <- Enum.to_list(page) do
        {:ok, true}
      else
        [] ->
          {:ok, false}

        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def devices_with_data_on_interface(interface_name) do
    devices_query_statement = "SELECT key FROM kv_store WHERE group=:group_name"

    # TODO: validate interface name?
    devices_query_params = %{
      group_name: "devices-with-data-on-interface-#{interface_name}-v0"
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, devices_query_statement, devices_query_params,
               consistency: :quorum
             ) do
        # TODO: check returned value: is is now a list and not a CQEx.Result
        Enum.to_list(page)
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def delete_devices_with_data_on_interface(interface_name) do
    devices_query_statement = "DELETE FROM kv_store WHERE group=:group_name"

    # TODO: validate interface name?
    devices_query_params = %{
      group_name: "devices-with-data-on-interface-#{interface_name}-v0"
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, devices_query_statement, devices_query_params,
               consistency: :each_quorum
             ) do
        :ok
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def delete_values(
        device_id,
        %InterfaceDescriptor{
          interface_id: interface_id,
          storage_type: :multi_interface_individual_properties_dbtable,
          storage: table_name
        }
      ) do
    delete_values_statement = """
    DELETE
    FROM #{table_name}
    WHERE device_id=:device_id AND interface_id=:interface_id
    """

    delete_values_params = %{
      device_id: device_id,
      interface_id: interface_id
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, delete_values_statement, delete_values_params,
               consistency: :each_quorum
             ) do
        :ok
      else
        {:error, reason} ->
          _ =
            Logger.warn("Database error: cannot delete values. Reason: #{inspect(reason)}.",
              tag: "db_error"
            )

          {:error, :database_error}
      end
    end)
  end

  def delete_values(
        device_id,
        %InterfaceDescriptor{
          storage_type: :multi_interface_individual_datastream_dbtable
        } = interface_descriptor
      ) do
    with {:ok, result} <-
           fetch_all_paths_and_endpoint_ids(device_id, interface_descriptor),
         :ok <- delete_all_paths_values(device_id, interface_descriptor, result) do
      delete_all_paths(device_id, interface_descriptor)
    end
  end

  defp delete_all_paths_values(device_id, interface_descriptor, all_paths) do
    Enum.reduce_while(all_paths, :ok, fn %{endpoint_id: endpoint_id, path: path}, _acc ->
      with :ok <- delete_path_values(device_id, interface_descriptor, endpoint_id, path) do
        {:cont, :ok}
      else
        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  def delete_path_values(
        device_id,
        %InterfaceDescriptor{
          interface_id: interface_id,
          storage_type: :multi_interface_individual_datastream_dbtable,
          storage: table_name
        },
        endpoint_id,
        path
      ) do
    delete_path_values_statement = """
    DELETE
    FROM #{table_name}
    WHERE device_id=:device_id AND interface_id=:interface_id
      AND endpoint_id=:endpoint_id AND path=:path
    """

    delete_path_values_params = %{
      device_id: device_id,
      interface_id: interface_id,
      endpoint_id: endpoint_id,
      path: path
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      # TODO is :quorum what we want?
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, delete_path_values_statement, delete_path_values_params,
               consistency: :quorum
             ) do
        :ok
      else
        {:error, reason} ->
          _ =
            Logger.warn("Database error: cannot delete path values. Reason: #{inspect(reason)}.",
              tag: "db_error"
            )

          {:error, :database_error}
      end
    end)
  end

  defp fetch_all_paths_and_endpoint_ids(
         device_id,
         %InterfaceDescriptor{
           interface_id: interface_id,
           storage_type: :multi_interface_individual_datastream_dbtable
         }
       ) do
    all_paths_statement = """
    SELECT endpoint_id, path
    FROM individual_properties
    WHERE device_id=:device_id AND interface_id=:interface_id
    """

    all_paths_params = %{
      device_id: device_id,
      interface_id: interface_id
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, all_paths_statement, all_paths_params, consistency: :quorum) do
        # TODO check return type: now it's a list and not a CQEx.result
        Enum.to_list(page)
      else
        {:error, reason} ->
          _ =
            Logger.warn("Database error: cannot delete path values. Reason: #{inspect(reason)}.",
              tag: "db_error"
            )

          {:error, :database_error}
      end
    end)
  end

  defp delete_all_paths(
         device_id,
         %InterfaceDescriptor{
           interface_id: interface_id,
           storage_type: :multi_interface_individual_datastream_dbtable
         }
       ) do
    delete_paths_statement = """
    DELETE
    FROM individual_properties
    WHERE device_id=:device_id AND interface_id=:interface_id
    """

    delete_paths_params = %{
      device_id: device_id,
      interface_id: interface_id
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, delete_paths_statement, delete_paths_params,
               consistency: :each_quorum
             ) do
        :ok
      else
        {:error, reason} ->
          _ =
            Logger.warn("Database error: cannot delete path values. Reason: #{inspect(reason)}.",
              tag: "db_error"
            )

          {:error, :database_error}
      end
    end)
  end

  def interface_available_versions(interface_name) do
    interface_versions_statement = """
    SELECT major_version, minor_version
    FROM interfaces
    WHERE name = :interface_name
    """

    interface_versions_params = %{
      interface_name: interface_name
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, interface_versions_statement, interface_versions_params,
               consistency: :quorum
             ) do
        case Enum.to_list(page) do
          [] ->
            {:error, :interface_not_found}

          result ->
            {:ok, result}
        end
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def is_interface_major_available?(interface_name, interface_major) do
    interface_available_major_statement = """
    SELECT COUNT(*)
    FROM interfaces
    WHERE name = :interface_name AND major_version = :interface_major
    """

    interface_available_major_params = %{
      interface_name: interface_name,
      interface_major: interface_major
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(
               conn,
               interface_available_major_statement,
               interface_available_major_params,
               consistency: :quorum
             ),
           [%{count: count}] <- Enum.to_list(page) do
        {:ok, count != 0}
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  defp normalize_interface_name(interface_name) do
    String.replace(interface_name, "-", "")
    |> String.downcase()
  end

  def check_interface_name_collision(interface_name) do
    normalized_interface = normalize_interface_name(interface_name)

    all_names_statement = """
    SELECT DISTINCT name
    FROM interfaces
    """

    # TODO consider whether it is feasible to move this check in the query rather than after (name normalization issues)

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, all_names_statement, %{}, consistency: :quorum) do
        # TODO check type of element here
        Enum.reduce_while(Enum.to_list(page), :ok, fn %{name: name}, _acc ->
          if normalize_interface_name(name) == normalized_interface do
            if name == interface_name do
              # If there is already an interface with the same name, we know it's possible to install it.
              # Version conflicts will be checked in another function.
              {:halt, :ok}
            else
              {:halt, {:error, :interface_name_collision}}
            end
          else
            {:cont, :ok}
          end
        end)
      else
        {:error, reason} ->
          Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def fetch_interface(interface_name, interface_major) do
    with {:ok, interface_row} <-
           Astarte.DataAccess.Interface.retrieve_interface_row(
             "TODO REALM NAME",
             interface_name,
             interface_major
           ),
         {:ok, interface_id} <- Keyword.fetch(interface_row, :interface_id),
         {:ok, mappings} <-
           Astarte.DataAccess.Mappings.fetch_interface_mappings("TODO REALM NAME", interface_id,
             include_docs: true
           ) do
      interface = %InterfaceDocument{
        name: Keyword.fetch!(interface_row, :name),
        major_version: Keyword.fetch!(interface_row, :major_version),
        minor_version: Keyword.fetch!(interface_row, :minor_version),
        interface_id: interface_id,
        type: Keyword.fetch!(interface_row, :type) |> InterfaceType.from_int(),
        ownership: Keyword.fetch!(interface_row, :ownership) |> Ownership.from_int(),
        aggregation: Keyword.fetch!(interface_row, :aggregation) |> Aggregation.from_int(),
        mappings: mappings,
        description: Keyword.fetch!(interface_row, :description),
        doc: Keyword.fetch!(interface_row, :doc)
      }

      {:ok, interface}
    end
  end

  def get_interfaces_list() do
    all_names_statement = """
    SELECT DISTINCT name
    FROM interfaces
    """

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, all_names_statement, %{}, consistency: :quorum) do
        # TODO check type of element here
        page
        |> Enum.to_list()
        |> Enum.map(fn %{name: name} -> name end)
      else
        {:error, reason} ->
          Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def has_interface_simple_triggers?(object_id) do
    # FIXME: hardcoded object type here
    simple_triggers_statement = """
    SELECT COUNT(*)
    FROM simple_triggers
    WHERE object_id=:object_id AND object_type=2
    """

    simple_triggers_params = %{object_id: object_id}

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, simple_triggers_statement, simple_triggers_params,
               consistency: :quorum
             ) do
        case Enum.to_list(page) do
          [%{count: 0}] -> {:ok, false}
          [%{count: _n}] -> {:ok, true}
        end
      else
        {:error, reason} ->
          _ = Logger.warn("Failed with reason: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def get_jwt_public_key_pem() do
    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <- Xandra.execute(conn, @query_jwt_public_key_pem),
           [%{"system.blobasvarchar(value)": pem}] <- Enum.to_list(page) do
        {:ok, pem}
      else
        _ ->
          {:error, :public_key_not_found}
      end
    end)
  end

  def update_jwt_public_key_pem(jwt_public_key_pem) do
    update_params = %{pem: jwt_public_key_pem}

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, @query_insert_jwt_public_key_pem, update_params) do
        :ok
      else
        _ ->
          {:error, :cant_update_public_key}
      end
    end)
  end

  def install_trigger(trigger) do
    # TODO: use IF NOT EXISTS
    insert_by_name_query_statement =
      "INSERT INTO kv_store (group, key, value) VALUES ('triggers-by-name', :trigger_name, uuidAsBlob(:trigger_uuid));"

    insert_by_name_params = %{
      trigger_name: trigger.name,
      trigger_uuid: trigger.trigger_uuid
    }

    # TODO: use IF NOT EXISTS
    insert_query_statement =
      "INSERT INTO kv_store (group, key, value) VALUES ('triggers', :trigger_uuid, :trigger_data);"

    insert_params = %{
      trigger_uuid: :uuid.uuid_to_string(trigger.trigger_uuid),
      trigger_data: Trigger.encode(trigger)
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      # TODO: Batch queries
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, insert_by_name_query_statement, insert_by_name_params),
           {:ok, %Xandra.Void{}} <- Xandra.execute(conn, insert_query_statement, insert_params) do
        :ok
      else
        {:error, err} ->
          _ = Logger.warn("Database error: #{inspect(err)}.", tag: "db_error")
          {:error, :cannot_install_trigger}
      end
    end)
  end

  def install_simple_trigger(
        object_id,
        object_type,
        parent_trigger_id,
        simple_trigger_id,
        simple_trigger,
        trigger_target
      ) do
    insert_simple_trigger_statement = """
    INSERT INTO simple_triggers
    (object_id, object_type, parent_trigger_id, simple_trigger_id, trigger_data, trigger_target)
    VALUES (:object_id, :object_type, :parent_trigger_id, :simple_trigger_id, :simple_trigger_data, :trigger_target_data);
    """

    insert_simple_trigger_params = %{
      object_id: object_id,
      object_type: object_type,
      parent_trigger_id: parent_trigger_id,
      simple_trigger_id: simple_trigger_id,
      simple_trigger_data: SimpleTriggerContainer.encode(simple_trigger),
      trigger_target_data: TriggerTargetContainer.encode(trigger_target)
    }

    astarte_ref = %AstarteReference{
      object_type: object_type,
      object_uuid: object_id
    }

    insert_simple_trigger_by_uuid_statement =
      "INSERT INTO kv_store (group, key, value) VALUES ('simple-triggers-by-uuid', :simple_trigger_id, :astarte_ref);"

    insert_simple_trigger_by_uuid_params = %{
      simple_trigger_id: :uuid.uuid_to_string(simple_trigger_id),
      astarte_ref: AstarteReference.encode(astarte_ref)
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, insert_simple_trigger_statement, insert_simple_trigger_params),
           {:ok, %Xandra.Void{}} <-
             Xandra.execute(
               conn,
               insert_simple_trigger_by_uuid_statement,
               insert_simple_trigger_by_uuid_params
             ) do
        :ok
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :cannot_install_simple_trigger}
      end
    end)
  end

  def install_trigger_policy_link(_client, _trigger_uuid, nil) do
    :ok
  end

  def install_trigger_policy_link(trigger_uuid, trigger_policy) do
    insert_trigger_with_policy_statement =
      "INSERT INTO kv_store (group, key, value) VALUES (:policy_group, :trigger_uuid, uuidAsBlob(:trigger_uuid))"

    insert_trigger_with_policy_params = %{
      policy_group: "triggers-with-policy-#{trigger_policy}",
      trigger_uuid: :uuid.uuid_to_string(trigger_uuid)
    }

    insert_trigger_to_policy_statement =
      "INSERT INTO kv_store (group, key, value) VALUES ('trigger_to_policy',  :trigger_uuid, :trigger_policy);"

    insert_trigger_to_policy_params = %{
      trigger_uuid: :uuid.uuid_to_string(trigger_uuid),
      trigger_policy: trigger_policy
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(
               conn,
               insert_trigger_with_policy_statement,
               insert_trigger_with_policy_params
             ),
           {:ok, %Xandra.Void{}} <-
             Xandra.execute(
               conn,
               insert_trigger_to_policy_statement,
               insert_trigger_to_policy_params
             ) do
        :ok
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :cannot_install_trigger_policy_link}
      end
    end)
  end

  def retrieve_trigger_uuid(trigger_name, format \\ :string) do
    trigger_uuid_statement =
      "SELECT value FROM kv_store WHERE group='triggers-by-name' AND key=:trigger_name;"

    trigger_uuid_params = %{trigger_name: trigger_name}

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, trigger_uuid_statement, trigger_uuid_params),
           [%{value: trigger_uuid}] <- Enum.to_list(page) do
        case format do
          :string ->
            {:ok, :uuid.uuid_to_string(trigger_uuid)}

          :bytes ->
            {:ok, trigger_uuid}
        end
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :cannot_retrieve_trigger_uuid}
      end
    end)
  end

  def delete_trigger_policy_link(_client, _trigger_uuid, nil) do
    :ok
  end

  def delete_trigger_policy_link(trigger_uuid, trigger_policy) do
    delete_trigger_with_policy_statement =
      "DELETE FROM kv_store WHERE group=:policy_group AND key=:trigger_uuid;"

    delete_trigger_with_policy_params = %{
      policy_group: "triggers-with-policy-#{trigger_policy}",
      trigger_uuid: :uuid.uuid_to_string(trigger_uuid)
    }

    delete_trigger_to_policy_statement =
      "DELETE FROM kv_store WHERE group='trigger_to_policy' AND key=:trigger_uuid;"

    delete_trigger_to_policy_params = %{trigger_uuid: :uuid.uuid_to_string(trigger_uuid)}

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(
               conn,
               delete_trigger_with_policy_statement,
               delete_trigger_with_policy_params
             ),
           {:ok, %Xandra.Void{}} <-
             Xandra.execute(
               conn,
               delete_trigger_to_policy_statement,
               delete_trigger_to_policy_params
             ) do
        :ok
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :cannot_delete_trigger_policy_link}
      end
    end)
  end

  def delete_trigger(client, trigger_name) do
    with {:ok, trigger_uuid} <- retrieve_trigger_uuid(client, trigger_name) do
      delete_trigger_by_name_statement =
        "DELETE FROM kv_store WHERE group='triggers-by-name' AND key=:trigger_name;"

      delete_trigger_by_name_params = %{trigger_name: trigger_name}

      delete_trigger_statement =
        "DELETE FROM kv_store WHERE group='triggers' AND key=:trigger_uuid;"

      delete_trigger_params = %{trigger_uuid: trigger_uuid}

      Xandra.Cluster.run(:xandra, fn conn ->
        with {:ok, %Xandra.Void{}} <-
               Xandra.execute(conn, delete_trigger_statement, delete_trigger_params),
             {:ok, %Xandra.Void{}} <-
               Xandra.execute(
                 conn,
                 delete_trigger_by_name_statement,
                 delete_trigger_by_name_params
               ) do
          :ok
        else
          {:error, reason} ->
            _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
            {:error, :cannot_delete_trigger}
        end
      end)
    end
  end

  def get_triggers_list() do
    triggers_list_statement = "SELECT key FROM kv_store WHERE group = 'triggers-by-name';"

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <- Xandra.execute(conn, triggers_list_statement) do
        page
        |> Enum.to_list()
        |> Enum.map(fn {:key, value} -> value end)
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :cannot_list_triggers}
      end
    end)
  end

  def retrieve_trigger(trigger_name) do
    with {:ok, trigger_uuid} <- retrieve_trigger_uuid(trigger_name) do
      retrieve_trigger_statement =
        "SELECT value FROM kv_store WHERE group='triggers' AND key=:trigger_uuid;"

      retrieve_trigger_params = %{trigger_uuid: trigger_uuid}

      Xandra.Cluster.run(:xandra, fn conn ->
        with {:ok, %Xandra.Page{} = page} <-
               Xandra.execute(conn, retrieve_trigger_statement, retrieve_trigger_params) do
          case Enum.to_list(page) do
            [] ->
              {:error, :trigger_not_found}

            [%{value: trigger_data}] ->
              {:ok, Trigger.decode(trigger_data)}
          end
        else
          {:error, reason} ->
            _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
            {:error, :cannot_retrieve_trigger}
        end
      end)
    end
  end

  # TODO: simple_trigger_uuid is required due how we made the compound key
  # should we move simple_trigger_uuid to the first part of the key?
  def retrieve_tagged_simple_trigger(parent_trigger_uuid, simple_trigger_uuid) do
    with %{object_uuid: object_id, object_type: object_type} <-
           retrieve_simple_trigger_astarte_ref(simple_trigger_uuid) do
      retrieve_simple_trigger_statement = """
      SELECT trigger_data
      FROM simple_triggers
      WHERE object_id=:object_id AND object_type=:object_type AND
            parent_trigger_id=:parent_trigger_id AND simple_trigger_id=:simple_trigger_id
      """

      retrieve_simple_trigger_params = %{
        object_id: object_id,
        object_type: object_type,
        parent_trigger_id: parent_trigger_uuid,
        simple_trigger_id: simple_trigger_uuid
      }

      Xandra.Cluster.run(:xandra, fn conn ->
        with {:ok, %Xandra.Page{} = page} <-
               Xandra.execute(
                 conn,
                 retrieve_simple_trigger_statement,
                 retrieve_simple_trigger_params
               ),
             [%{trigger_data: trigger_data}] <- Enum.to_list(page) do
          {
            :ok,
            %TaggedSimpleTrigger{
              object_id: object_id,
              object_type: object_type,
              simple_trigger_container: SimpleTriggerContainer.decode(trigger_data)
            }
          }
        else
          {:error, reason} ->
            _ =
              Logger.warn("Possible inconsistency found: database error: #{inspect(reason)}.",
                tag: "db_error"
              )

            {:error, :cannot_retrieve_simple_trigger}
        end
      end)
    end
  end

  def delete_simple_trigger(parent_trigger_uuid, simple_trigger_uuid) do
    with %{object_uuid: object_id, object_type: object_type} <-
           retrieve_simple_trigger_astarte_ref(simple_trigger_uuid) do
      delete_simple_trigger_statement = """
      DELETE FROM simple_triggers
      WHERE object_id=:object_id AND object_type=:object_type AND
            parent_trigger_id=:parent_trigger_id AND simple_trigger_id=:simple_trigger_id
      """

      delete_simple_trigger_params = %{
        object_id: object_id,
        object_type: object_type,
        parent_trigger_id: parent_trigger_uuid,
        simple_trigger_id: simple_trigger_uuid
      }

      delete_astarte_ref_statement =
        "DELETE FROM kv_store WHERE group='simple-triggers-by-uuid' AND key=:simple_trigger_uuid;"

      delete_astarte_ref_params = %{
        simple_trigger_uuid: :uuid.uuid_to_string(simple_trigger_uuid)
      }

      Xandra.Cluster.run(:xandra, fn conn ->
        with {:ok, %Xandra.Void{}} <-
               Xandra.execute(conn, delete_simple_trigger_statement, delete_simple_trigger_params),
             {:ok, %Xandra.Void{}} <-
               Xandra.execute(conn, delete_astarte_ref_statement, delete_astarte_ref_params) do
          :ok
        else
          {:error, reason} ->
            _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
            {:error, :cannot_delete_simple_trigger}
        end
      end)
    end
  end

  defp retrieve_simple_trigger_astarte_ref(simple_trigger_uuid) do
    retrieve_astarte_ref_statement =
      "SELECT value FROM kv_store WHERE group='simple-triggers-by-uuid' AND key=:simple_trigger_uuid;"

    retrieve_astarte_ref_params = %{
      simple_trigger_uuid: :uuid.uuid_to_string(simple_trigger_uuid)
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, retrieve_astarte_ref_statement, retrieve_astarte_ref_params) do
        case Enum.to_list(page) do
          [] -> {:error, :trigger_not_found}
          [%{value: astarte_ref_blob}] -> AstarteReference.decode(astarte_ref_blob)
        end
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :cannot_retrieve_simple_trigger}
      end
    end)
  end

  def install_new_trigger_policy(policy_name, policy_proto) do
    insert_query_statement =
      "INSERT INTO kv_store (group, key, value) VALUES ('trigger_policy', :policy_name, :policy_container);"

    insert_query_params = %{
      policy_name: policy_name,
      policy_container: policy_proto
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, insert_query_statement, insert_query_params) do
        :ok
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :cannot_install_trigger_policy}
      end
    end)
  end

  def get_trigger_policies_list() do
    trigger_policies_list_statement = """
    SELECT key FROM kv_store WHERE group=:group_name
    """

    trigger_policies_list_params = %{group_name: "trigger_policy"}

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(
               conn,
               trigger_policies_list_statement,
               trigger_policies_list_params,
               consistency: :quorum
             ) do
        list =
          page
          |> Enum.to_list()
          |> Enum.map(fn {:key, value} -> value end)

        {:ok, list}
      else
        {:error, reason} ->
          _ =
            Logger.warn("Database error: failed with reason: #{inspect(reason)}.", tag: "db_error")

          {:error, :database_error}
      end
    end)
  end

  def fetch_trigger_policy(policy_name) do
    policy_cols_statement = """
    SELECT value
    FROM kv_store
    WHERE group=:group_name and key=:policy_name
    """

    policy_cols_params = %{
      group_name: "trigger_policy",
      policy_name: policy_name
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, policy_cols_statement, policy_cols_params, consistency: :quorum) do
        case Enum.to_list(page) do
          [] -> {:error, :policy_not_found}
          [%{value: value}] -> {:ok, value}
        end
      else
        {:error, reason} ->
          _ = Logger.warn("Failed, reason: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end

  def check_policy_has_triggers(policy_name) do
    devices_statement = "SELECT key FROM kv_store WHERE group=:group_name LIMIT 1"

    devices_params = %{
      group_name: "triggers-with-policy-#{policy_name}"
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, devices_statement, devices_params, consistency: :quorum) do
        case Enum.to_list(page) do
          [] ->
            {:ok, false}

          [%{key: _device_id}] ->
            {:ok, true}
        end
      else
        {:error, reason} ->
          _ =
            Logger.error(
              "Database error while checking #{policy_name}, reason: #{inspect(reason)}.",
              tag: "db_error"
            )

          {:error, :database_error}
      end
    end)
  end

  def delete_trigger_policy(policy_name) do
    _ =
      Logger.info("Delete trigger policy.",
        policy_name: policy_name,
        tag: "db_delete_trigger_policy"
      )

    delete_policy_statement =
      "DELETE FROM kv_store WHERE group= :group_name AND key= :policy_name"

    delete_policy_params = %{
      group_name: "trigger_policy",
      policy_name: policy_name
    }

    # TODO check warning
    delete_triggers_with_policy_group_statement = "DELETE FROM kv_store WHERE group=:group_name"

    delete_triggers_with_policy_group_params = %{
      group_name: "triggers-with-policy-#{policy_name}"
    }

    delete_trigger_to_policy_statement = "DELETE FROM kv_store WHERE group=:group_name;"

    delete_trigger_to_policy_params = %{
      group_name: "trigger_to_policy"
    }

    # TODO batch together
    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Void{}} <-
             Xandra.execute(conn, delete_policy_statement, delete_policy_params,
               consistency: :each_quorum
             ),
           {:ok, %Xandra.Void{}} <-
             Xandra.execute(
               conn,
               delete_triggers_with_policy_group_statement,
               delete_triggers_with_policy_group_params,
               consistency: :each_quorum
             ),
           {:ok, %Xandra.Void{}} <-
             Xandra.execute(
               conn,
               delete_trigger_to_policy_statement,
               delete_trigger_to_policy_params,
               consistency: :each_quorum
             ) do
        :ok
      else
        {:error, reason} ->
          _ =
            Logger.error(
              "Database error while deleting #{policy_name}, reason: #{inspect(reason)}.",
              tag: "db_error"
            )

          {:error, :database_error}
      end
    end)
  end

  def check_trigger_policy_already_present(policy_name) do
    policy_cols_statement = """
    SELECT COUNT(*)
    FROM kv_store
    WHERE group= :group_name and key= :policy_name
    """

    policy_cols_params = %{
      group_name: "trigger_policy",
      policy_name: policy_name
    }

    Xandra.Cluster.run(:xandra, fn conn ->
      with {:ok, %Xandra.Page{} = page} <-
             Xandra.execute(conn, policy_cols_statement, policy_cols_params, consistency: :quorum) do
        case Enum.to_list(page) do
          [%{count: 0}] -> {:ok, false}
          [%{count: _n}] -> {:ok, true}
        end
      else
        {:error, reason} ->
          _ = Logger.warn("Database error: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    end)
  end
end
