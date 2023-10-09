#
# This file is part of Astarte.
#
# Copyright 2017-2023 SECO Mind Srl
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

defmodule Astarte.DataUpdaterPlant.DataUpdater.Impl do
  alias Astarte.Core.Device
  alias Astarte.Core.InterfaceDescriptor
  alias Astarte.Core.Mapping.EndpointsAutomaton
  alias Astarte.DataUpdaterPlant.DataUpdater.State
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.DataTrigger, as: ProtobufDataTrigger
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.Utils, as: SimpleTriggersProtobufUtils
  alias Astarte.DataAccess.Data
  alias Astarte.DataAccess.Database
  alias Astarte.DataAccess.Interface, as: InterfaceQueries
  alias Astarte.DataUpdaterPlant.DataUpdater.Cache
  alias Astarte.DataUpdaterPlant.DataUpdater.CachedPath
  alias Astarte.DataUpdaterPlant.DataUpdater.PayloadsDecoder
  alias Astarte.DataUpdaterPlant.DataUpdater.Queries
  alias Astarte.DataUpdaterPlant.MessageTracker
  alias Astarte.DataUpdaterPlant.TriggersHandler
  alias Astarte.DataUpdaterPlant.DataUpdater.Impl.Core
  alias Astarte.DataUpdaterPlant.DataUpdater.Core.ProcessIntrospection
  alias Astarte.DataUpdaterPlant.DataUpdater.Core.Error

  require Logger

  @paths_cache_size 32
  @interface_lifespan_decimicroseconds 60 * 10 * 1000 * 10000
  @device_triggers_lifespan_decimicroseconds 60 * 10 * 1000 * 10000
  @groups_lifespan_decimicroseconds 60 * 10 * 1000 * 10000

  def init_state(realm, device_id, message_tracker) do
    MessageTracker.register_data_updater(message_tracker)
    Process.monitor(message_tracker)

    new_state = %State{
      realm: realm,
      device_id: device_id,
      message_tracker: message_tracker,
      connected: true,
      groups: [],
      interfaces: %{},
      interface_ids_to_name: %{},
      interfaces_by_expiry: [],
      mappings: %{},
      paths_cache: Cache.new(@paths_cache_size),
      device_triggers: %{},
      data_triggers: %{},
      volatile_triggers: [],
      interface_exchanged_bytes: %{},
      interface_exchanged_msgs: %{},
      last_seen_message: 0,
      last_device_triggers_refresh: 0,
      last_groups_refresh: 0,
      trigger_id_to_policy_name: %{}
    }

    encoded_device_id = Device.encode_device_id(device_id)
    Logger.metadata(realm: realm, device_id: encoded_device_id)
    Logger.info("Created device process.", tag: "device_process_created")

    {:ok, db_client} = Database.connect(realm: new_state.realm)

    stats_and_introspection =
      Queries.retrieve_device_stats_and_introspection!(db_client, device_id)

    {:ok, ttl} = Queries.fetch_datastream_maximum_storage_retention(db_client)

    Map.merge(new_state, stats_and_introspection)
    |> Map.put(:datastream_maximum_storage_retention, ttl)
  end

  def handle_deactivation(_state) do
    Logger.info("Deactivated device process.", tag: "device_process_deactivated")

    :ok
  end

  def handle_connection(state, ip_address_string, message_id, timestamp) do
    {:ok, db_client} = Database.connect(realm: state.realm)

    new_state = Core.execute_time_based_actions(state, timestamp, db_client)

    timestamp_ms = div(timestamp, 10_000)

    ip_address_result =
      ip_address_string
      |> to_charlist()
      |> :inet.parse_address()

    ip_address =
      case ip_address_result do
        {:ok, ip_address} ->
          ip_address

        _ ->
          Logger.warn("Received invalid IP address #{ip_address_string}.")
          {0, 0, 0, 0}
      end

    Queries.set_device_connected!(
      db_client,
      new_state.device_id,
      timestamp_ms,
      ip_address
    )

    trigger_target_with_policy_list =
      Map.get(new_state.device_triggers, :on_device_connection, [])
      |> Enum.map(fn target ->
        {target, Map.get(state.trigger_id_to_policy_name, target.parent_trigger_id)}
      end)

    device_id_string = Device.encode_device_id(new_state.device_id)

    TriggersHandler.device_connected(
      trigger_target_with_policy_list,
      new_state.realm,
      device_id_string,
      ip_address_string,
      timestamp_ms
    )

    MessageTracker.ack_delivery(new_state.message_tracker, message_id)
    Logger.info("Device connected.", ip_address: ip_address_string, tag: "device_connected")

    :telemetry.execute([:astarte, :data_updater_plant, :data_updater, :device_connection], %{}, %{
      realm: new_state.realm
    })

    %{new_state | connected: true, last_seen_message: timestamp}
  end

  # TODO make this private when all heartbeats will be moved to internal
  def handle_heartbeat(state, message_id, timestamp) do
    {:ok, db_client} = Database.connect(realm: state.realm)

    new_state = Core.execute_time_based_actions(state, timestamp, db_client)

    Queries.maybe_refresh_device_connected!(db_client, new_state.device_id)

    MessageTracker.ack_delivery(new_state.message_tracker, message_id)
    Logger.info("Device heartbeat.", tag: "device_heartbeat")

    %{new_state | connected: true, last_seen_message: timestamp}
  end

  def handle_internal(state, "/heartbeat", _payload, message_id, timestamp) do
    handle_heartbeat(state, message_id, timestamp)
  end

  def handle_internal(state, path, payload, message_id, timestamp) do
    Logger.warn("Unexpected internal message on #{path}, payload: #{inspect(payload)}",
      tag: "unexpected_internal_message"
    )

    {:ok, new_state} = Core.ask_clean_session(state, timestamp)
    MessageTracker.discard(new_state.message_tracker, message_id)

    :telemetry.execute(
      [:astarte, :data_updater_plant, :data_updater, :discarded_internal_message],
      %{},
      %{realm: new_state.realm}
    )

    base64_payload = Base.encode64(payload)

    error_metadata = %{
      "path" => inspect(path),
      "base64_payload" => base64_payload
    }

    # TODO maybe we don't want triggers on unexpected internal messages?
    Core.execute_device_error_triggers(
      new_state,
      "unexpected_internal_message",
      error_metadata,
      timestamp
    )

    Core.update_stats(new_state, "", nil, path, payload)
  end

  def handle_disconnection(state, message_id, timestamp) do
    {:ok, db_client} = Database.connect(realm: state.realm)

    new_state =
      state
      |> Core.execute_time_based_actions(timestamp, db_client)
      |> Core.set_device_disconnected(db_client, timestamp)

    MessageTracker.ack_delivery(new_state.message_tracker, message_id)
    Logger.info("Device disconnected.", tag: "device_disconnected")

    %{new_state | last_seen_message: timestamp}
  end

  def handle_data(state, interface, path, payload, message_id, timestamp) do
    {:ok, db_client} = Database.connect(realm: state.realm)

    new_state = Core.execute_time_based_actions(state, timestamp, db_client)

    with :ok <- Core.validate_interface(interface),
         :ok <- Core.validate_path(path),
         maybe_descriptor <- Map.get(new_state.interfaces, interface),
         {:ok, interface_descriptor, new_state} <-
           Core.maybe_handle_cache_miss(maybe_descriptor, interface, new_state, db_client),
         :ok <- Core.can_write_on_interface?(interface_descriptor),
         interface_id <- interface_descriptor.interface_id,
         {:ok, endpoint} <- Core.resolve_path(path, interface_descriptor, new_state.mappings),
         endpoint_id <- endpoint.endpoint_id,
         db_retention_policy = endpoint.database_retention_policy,
         db_ttl = endpoint.database_retention_ttl,
         {value, value_timestamp, _metadata} <-
           PayloadsDecoder.decode_bson_payload(payload, timestamp),
         expected_types <-
           Core.extract_expected_types(path, interface_descriptor, endpoint, new_state.mappings),
         :ok <- Core.validate_value_type(expected_types, value) do
      device_id_string = Device.encode_device_id(new_state.device_id)

      maybe_explicit_value_timestamp =
        if endpoint.explicit_timestamp do
          value_timestamp
        else
          div(timestamp, 10000)
        end

      Core.execute_incoming_data_triggers(
        new_state,
        device_id_string,
        interface_descriptor.name,
        interface_id,
        path,
        endpoint_id,
        payload,
        value,
        maybe_explicit_value_timestamp
      )

      {has_change_triggers, change_triggers} =
        Core.get_value_change_triggers(new_state, interface_id, endpoint_id, path, value)

      previous_value =
        with {:has_change_triggers, :ok} <- {:has_change_triggers, has_change_triggers},
             {:ok, property_value} <-
               Data.fetch_property(
                 new_state.realm,
                 new_state.device_id,
                 interface_descriptor,
                 endpoint,
                 path
               ) do
          property_value
        else
          {:has_change_triggers, _not_ok} ->
            nil

          {:error, :property_not_set} ->
            nil
        end

      if has_change_triggers == :ok do
        :ok =
          Core.execute_pre_change_triggers(
            change_triggers,
            new_state.realm,
            device_id_string,
            interface_descriptor.name,
            path,
            previous_value,
            value,
            maybe_explicit_value_timestamp,
            state.trigger_id_to_policy_name
          )
      end

      realm_max_ttl = state.datastream_maximum_storage_retention

      db_max_ttl =
        cond do
          db_retention_policy == :use_ttl and is_integer(realm_max_ttl) ->
            min(db_ttl, realm_max_ttl)

          db_retention_policy == :use_ttl ->
            db_ttl

          is_integer(realm_max_ttl) ->
            realm_max_ttl

          true ->
            nil
        end

      cond do
        interface_descriptor.type == :datastream and value != nil ->
          :ok =
            cond do
              Cache.has_key?(new_state.paths_cache, {interface, path}) ->
                :ok

              Core.is_still_valid?(
                Queries.fetch_path_expiry(
                  db_client,
                  new_state.device_id,
                  interface_descriptor,
                  endpoint,
                  path
                ),
                db_max_ttl
              ) ->
                :ok

              true ->
                Queries.insert_path_into_db(
                  db_client,
                  new_state.device_id,
                  interface_descriptor,
                  endpoint,
                  path,
                  maybe_explicit_value_timestamp,
                  timestamp,
                  ttl: Core.path_ttl(db_max_ttl)
                )
            end

        interface_descriptor.type == :datastream ->
          Logger.warn("Tried to unset a datastream.", tag: "unset_on_datastream")
          MessageTracker.discard(new_state.message_tracker, message_id)

          :telemetry.execute(
            [:astarte, :data_updater_plant, :data_updater, :discarded_message],
            %{},
            %{realm: new_state.realm}
          )

          base64_payload = Base.encode64(payload)

          error_metadata = %{
            "interface" => inspect(interface),
            "path" => inspect(path),
            "base64_payload" => base64_payload
          }

          Core.execute_device_error_triggers(
            new_state,
            "unset_on_datastream",
            error_metadata,
            timestamp
          )

          raise "Unsupported"

        true ->
          :ok
      end

      # TODO: handle insert failures here
      insert_result =
        Queries.insert_value_into_db(
          db_client,
          new_state.device_id,
          interface_descriptor,
          endpoint,
          path,
          value,
          maybe_explicit_value_timestamp,
          timestamp,
          ttl: db_max_ttl
        )

      :ok = insert_result

      if has_change_triggers == :ok do
        :ok =
          Core.execute_post_change_triggers(
            change_triggers,
            new_state.realm,
            device_id_string,
            interface_descriptor.name,
            path,
            previous_value,
            value,
            maybe_explicit_value_timestamp,
            state.trigger_id_to_policy_name
          )
      end

      ttl = db_max_ttl
      paths_cache = Cache.put(new_state.paths_cache, {interface, path}, %CachedPath{}, ttl)
      new_state = %{new_state | paths_cache: paths_cache}

      MessageTracker.ack_delivery(new_state.message_tracker, message_id)

      :telemetry.execute(
        [:astarte, :data_updater_plant, :data_updater, :processed_message],
        %{},
        %{
          realm: new_state.realm,
          interface_type: interface_descriptor.type
        }
      )

      Core.update_stats(new_state, interface, interface_descriptor.major_version, path, payload)
    else
      {:error, :cannot_write_on_server_owned_interface} ->
        Logger.warn(
          "Tried to write on server owned interface: #{interface} on " <>
            "path: #{path}, payload: #{inspect(payload)}, timestamp: #{inspect(timestamp)}.",
          tag: "write_on_server_owned_interface"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "write_on_server_owned_interface",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)

      {:error, :invalid_interface} ->
        Logger.warn("Received invalid interface: #{inspect(interface)}.",
          tag: "invalid_interface"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "invalid_interface",
          error_metadata,
          timestamp
        )

        # We dont't update stats on an invalid interface
        new_state

      {:error, :invalid_path} ->
        Logger.warn("Received invalid path: #{inspect(path)}.", tag: "invalid_path")
        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(new_state, "invalid_path", error_metadata, timestamp)

        Core.update_stats(new_state, interface, nil, path, payload)

      {:error, :mapping_not_found} ->
        Logger.warn("Mapping not found for #{interface}#{path}. Maybe outdated introspection?",
          tag: "mapping_not_found"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "mapping_not_found",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)

      {:error, :interface_loading_failed} ->
        Logger.warn("Cannot load interface: #{interface}.", tag: "interface_loading_failed")
        # TODO: think about additional actions since the problem
        # could be a missing interface in the DB
        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "interface_loading_failed",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)

      {:guessed, _guessed_endpoints} ->
        Logger.warn("Mapping guessed for #{interface}#{path}. Maybe outdated introspection?",
          tag: "ambiguous_path"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "ambiguous_path",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)

      {:error, :undecodable_bson_payload} ->
        Logger.warn("Invalid BSON payload: #{inspect(payload)} sent to #{interface}#{path}.",
          tag: "undecodable_bson_payload"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "undecodable_bson_payload",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)

      {:error, :unexpected_value_type} ->
        Logger.warn("Received invalid value: #{inspect(payload)} sent to #{interface}#{path}.",
          tag: "unexpected_value_type"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "unexpected_value_type",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)

      {:error, :value_size_exceeded} ->
        Logger.warn("Received huge payload: #{inspect(payload)} sent to #{interface}#{path}.",
          tag: "value_size_exceeded"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        base64_payload = Base.encode64(payload)

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "value_size_exceeded",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)

      {:error, :unexpected_object_key} ->
        base64_payload = Base.encode64(payload)

        Logger.warn(
          "Received object with unexpected key, object base64 is: #{base64_payload} sent to #{interface}#{path}.",
          tag: "unexpected_object_key"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        error_metadata = %{
          "interface" => inspect(interface),
          "path" => inspect(path),
          "base64_payload" => base64_payload
        }

        Core.execute_device_error_triggers(
          new_state,
          "unexpected_object_key",
          error_metadata,
          timestamp
        )

        Core.update_stats(new_state, interface, nil, path, payload)
    end
  end

  def handle_introspection(state, payload, message_id, timestamp) do
    with {:ok, new_introspection_list} <- PayloadsDecoder.parse_introspection(payload) do
      {:ok, db_client} = Database.connect(realm: state.realm)

      state = Core.execute_time_based_actions(state, timestamp, db_client)

      timestamp_ms = div(timestamp, 10_000)

      process_introspection = %ProcessIntrospection{
        realm: state.realm,
        device_id: state.device_id,
        encoded_device_id: Device.encode_device_id(state.device_id),
        introspection: state.introspection,
        interfaces: state.interfaces,
        trigger_id_to_policy_name: state.trigger_id_to_policy_name,
        timestamp_ms: timestamp_ms
      }

      {new_introspection_map, new_introspection_minor_map} =
        ProcessIntrospection.introspection_list_to_major_and_minor_maps(new_introspection_list)

      any_interface_id = SimpleTriggersProtobufUtils.any_interface_object_id()

      %{device_triggers: device_triggers} =
        Core.populate_triggers_for_object!(state, db_client, any_interface_id, :any_interface)

      ProcessIntrospection.handle_incoming_introspection(
        process_introspection,
        payload,
        device_triggers
      )

      # TODO: implement here object_id handling for a certain interface name. idea: introduce interface_family_id

      introspection_changes =
        ProcessIntrospection.compute_introspection_changes(
          state.introspection,
          new_introspection_map
        )

      Enum.each(introspection_changes, fn {change_type, changed_interfaces} ->
        case change_type do
          :ins ->
            ProcessIntrospection.handle_added_interfaces(
              process_introspection,
              db_client,
              changed_interfaces,
              new_introspection_minor_map,
              device_triggers
            )

          :del ->
            Logger.debug(
              "Removing interfaces from introspection: #{inspect(changed_interfaces)}."
            )

            ProcessIntrospection.handle_removed_interfaces(
              process_introspection,
              db_client,
              changed_interfaces,
              device_triggers
            )

          :eq ->
            Logger.debug("#{inspect(changed_interfaces)} are already on device introspection.")
        end
      end)

      {:ok, old_minors} = Queries.fetch_device_introspection_minors(db_client, state.device_id)

      {added_interfaces, removed_interfaces} =
        ProcessIntrospection.updated_interfaces_from_changes(introspection_changes)

      ProcessIntrospection.update_device_old_introspection(
        process_introspection,
        db_client,
        added_interfaces,
        removed_interfaces,
        old_minors
      )

      # Deliver interface_minor_updated triggers if needed
      ProcessIntrospection.handle_interface_minor_updated(
        process_introspection,
        new_introspection_map,
        new_introspection_minor_map,
        old_minors,
        device_triggers
      )

      # Removed/updated interfaces must be purged away, otherwise data will be written using old
      # interface_id.
      interfaces_to_drop_list =
        ProcessIntrospection.compute_interfaces_to_drop(state.interfaces, removed_interfaces)

      # drop_interfaces wants a list of already loaded interfaces, otherwise it will crash
      new_state = Core.drop_interfaces(state, interfaces_to_drop_list)

      Queries.update_device_introspection!(
        db_client,
        new_state.device_id,
        new_introspection_map,
        new_introspection_minor_map
      )

      MessageTracker.ack_delivery(new_state.message_tracker, message_id)

      :telemetry.execute(
        [:astarte, :data_updater_plant, :data_updater, :processed_introspection],
        %{},
        %{realm: new_state.realm}
      )

      new_state
      |> Core.update_introspection(new_introspection_map)
      |> Core.init_paths_cache()
      |> Core.add_received_message(payload)
    else
      {:error, :invalid_introspection} ->
        base64_payload = Base.encode64(payload)

        error = %Error{
          state: state,
          message_id: message_id,
          timestamp: timestamp,
          log_line: "Discarding invalid introspection base64: #{base64_payload}.",
          tag: "invalid_introspection",
          telemetry_tag: :discarded_introspection,
          metadata: %{"base64_payload" => base64_payload},
          state_update_fun: &Core.update_stats(&1, "", nil, "", payload)
        }

        Error.ask_clean_session_and_error(error)
    end
  end

  def handle_control(state, "/producer/properties", <<0, 0, 0, 0>>, message_id, timestamp) do
    {:ok, db_client} = Database.connect(realm: state.realm)

    new_state = Core.execute_time_based_actions(state, timestamp, db_client)

    timestamp_ms = div(timestamp, 10_000)

    operation_result = Core.prune_device_properties(new_state, "", timestamp_ms)

    if operation_result != :ok do
      Logger.debug("Result is #{inspect(operation_result)} further actions should be required.")
    end

    MessageTracker.ack_delivery(new_state.message_tracker, message_id)

    %{
      new_state
      | total_received_msgs: new_state.total_received_msgs + 1,
        total_received_bytes:
          new_state.total_received_bytes + byte_size(<<0, 0, 0, 0>>) +
            byte_size("/producer/properties")
    }
  end

  def handle_control(state, "/producer/properties", payload, message_id, timestamp) do
    {:ok, db_client} = Database.connect(realm: state.realm)

    new_state = Core.execute_time_based_actions(state, timestamp, db_client)

    timestamp_ms = div(timestamp, 10_000)

    # TODO: check payload size, to avoid anoying crashes

    <<_size_header::size(32), zlib_payload::binary>> = payload

    decoded_payload = PayloadsDecoder.safe_inflate(zlib_payload)

    if decoded_payload != :error do
      operation_result = Core.prune_device_properties(new_state, decoded_payload, timestamp_ms)

      if operation_result != :ok do
        Logger.debug("Result is #{inspect(operation_result)} further actions should be required.")
      end
    end

    MessageTracker.ack_delivery(new_state.message_tracker, message_id)

    %{
      new_state
      | total_received_msgs: new_state.total_received_msgs + 1,
        total_received_bytes:
          new_state.total_received_bytes + byte_size(payload) + byte_size("/producer/properties")
    }
  end

  def handle_control(state, "/emptyCache", _payload, message_id, timestamp) do
    Logger.debug("Received /emptyCache")

    {:ok, db_client} = Database.connect(realm: state.realm)

    new_state = Core.execute_time_based_actions(state, timestamp, db_client)

    with :ok <- Core.send_control_consumer_properties(state, db_client),
         {:ok, new_state} <- Core.resend_all_properties(state, db_client),
         :ok <- Queries.set_pending_empty_cache(db_client, new_state.device_id, false) do
      MessageTracker.ack_delivery(state.message_tracker, message_id)

      :telemetry.execute(
        [:astarte, :data_updater_plant, :data_updater, :processed_empty_cache],
        %{},
        %{realm: new_state.realm}
      )

      new_state
    else
      {:error, :session_not_found} ->
        Logger.warn("Cannot push data to device.", tag: "device_session_not_found")

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        Core.execute_device_error_triggers(new_state, "device_session_not_found", timestamp)

        new_state

      {:error, :sending_properties_to_interface_failed} ->
        Logger.warn("Cannot resend properties to interface",
          tag: "resend_interface_properties_failed"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        Core.execute_device_error_triggers(
          new_state,
          "resend_interface_properties_failed",
          timestamp
        )

        new_state

      {:error, reason} ->
        Logger.warn("Unhandled error during emptyCache: #{inspect(reason)}",
          tag: "empty_cache_error"
        )

        {:ok, new_state} = Core.ask_clean_session(new_state, timestamp)
        MessageTracker.discard(new_state.message_tracker, message_id)

        :telemetry.execute(
          [:astarte, :data_updater_plant, :data_updater, :discarded_message],
          %{},
          %{realm: new_state.realm}
        )

        error_metadata = %{"reason" => inspect(reason)}

        Core.execute_device_error_triggers(
          new_state,
          "empty_cache_error",
          error_metadata,
          timestamp
        )

        new_state
    end
  end

  def handle_control(state, path, payload, message_id, timestamp) do
    Logger.warn("Unexpected control on #{path}, payload: #{inspect(payload)}",
      tag: "unexpected_control_message"
    )

    {:ok, new_state} = Core.ask_clean_session(state, timestamp)
    MessageTracker.discard(new_state.message_tracker, message_id)

    :telemetry.execute(
      [:astarte, :data_updater_plant, :data_updater, :discarded_control_message],
      %{},
      %{realm: new_state.realm}
    )

    base64_payload = Base.encode64(payload)

    error_metadata = %{
      "path" => inspect(path),
      "base64_payload" => base64_payload
    }

    Core.execute_device_error_triggers(
      new_state,
      "unexpected_control_message",
      error_metadata,
      timestamp
    )

    Core.update_stats(new_state, "", nil, path, payload)
  end

  def handle_install_volatile_trigger(
        state,
        object_id,
        object_type,
        parent_id,
        trigger_id,
        simple_trigger,
        trigger_target
      ) do
    trigger = SimpleTriggersProtobufUtils.deserialize_simple_trigger(simple_trigger)

    target =
      SimpleTriggersProtobufUtils.deserialize_trigger_target(trigger_target)
      |> Map.put(:simple_trigger_id, trigger_id)
      |> Map.put(:parent_trigger_id, parent_id)

    volatile_triggers_list = [
      {{object_id, object_type}, {trigger, target}} | state.volatile_triggers
    ]

    new_state = Map.put(state, :volatile_triggers, volatile_triggers_list)

    if Map.has_key?(new_state.interface_ids_to_name, object_id) do
      interface_name = Map.get(new_state.interface_ids_to_name, object_id)
      %InterfaceDescriptor{automaton: automaton} = new_state.interfaces[interface_name]

      case trigger do
        {:data_trigger, %ProtobufDataTrigger{match_path: "/*"}} ->
          {:ok, Core.load_trigger(new_state, trigger, target)}

        {:data_trigger, %ProtobufDataTrigger{match_path: match_path}} ->
          with {:ok, _endpoint_id} <- EndpointsAutomaton.resolve_path(match_path, automaton) do
            {:ok, Core.load_trigger(new_state, trigger, target)}
          else
            {:guessed, _} ->
              # State rollback here
              {{:error, :invalid_match_path}, state}

            {:error, :not_found} ->
              # State rollback here
              {{:error, :invalid_match_path}, state}
          end
      end
    else
      case trigger do
        {:data_trigger, %ProtobufDataTrigger{interface_name: "*"}} ->
          {:ok, Core.load_trigger(new_state, trigger, target)}

        {:data_trigger,
         %ProtobufDataTrigger{
           interface_name: interface_name,
           interface_major: major,
           match_path: "/*"
         }} ->
          with {:ok, db_client} <- Database.connect(realm: state.realm),
               :ok <-
                 InterfaceQueries.check_if_interface_exists(state.realm, interface_name, major) do
            {:ok, new_state}
          else
            {:error, reason} ->
              # State rollback here
              {{:error, reason}, state}
          end

        {:data_trigger,
         %ProtobufDataTrigger{
           interface_name: interface_name,
           interface_major: major,
           match_path: match_path
         }} ->
          with {:ok, db_client} <- Database.connect(realm: state.realm),
               {:ok, %InterfaceDescriptor{automaton: automaton}} <-
                 InterfaceQueries.fetch_interface_descriptor(state.realm, interface_name, major),
               {:ok, _endpoint_id} <- EndpointsAutomaton.resolve_path(match_path, automaton) do
            {:ok, new_state}
          else
            {:error, :not_found} ->
              {{:error, :invalid_match_path}, state}

            {:guessed, _} ->
              {{:error, :invalid_match_path}, state}

            {:error, reason} ->
              # State rollback here
              {{:error, reason}, state}
          end

        {:device_trigger, _} ->
          {:ok, Core.load_trigger(new_state, trigger, target)}
      end
    end
  end

  def handle_delete_volatile_trigger(state, trigger_id) do
    {new_volatile, maybe_trigger} =
      Enum.reduce(state.volatile_triggers, {[], nil}, fn item, {acc, found} ->
        {_, {_simple_trigger, trigger_target}} = item

        if trigger_target.simple_trigger_id == trigger_id do
          {acc, item}
        else
          {[item | acc], found}
        end
      end)

    case maybe_trigger do
      {{obj_id, obj_type}, {simple_trigger, trigger_target}} ->
        %{state | volatile_triggers: new_volatile}
        |> Core.delete_volatile_trigger({obj_id, obj_type}, {simple_trigger, trigger_target})

      nil ->
        {:ok, state}
    end
  end
end
