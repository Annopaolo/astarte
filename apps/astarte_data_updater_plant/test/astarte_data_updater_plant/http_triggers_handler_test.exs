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

defmodule Astarte.DataUpdaterPlant.HTTPTriggersHandlerTest do
  use ExUnit.Case

  use Astarte.Core.Triggers.SimpleEvents

  alias AMQP.Channel
  alias AMQP.Connection
  alias AMQP.Queue
  alias Astarte.Core.Triggers.SimpleTriggersProtobuf.AMQPTriggerTarget
  alias Astarte.DataUpdaterPlant.Config
  alias Astarte.DataUpdaterPlant.TriggersHandler
  alias Astarte.DataUpdaterPlant.DatabaseTestHelper

  @introspection "com.My.Interface:1:0;com.Another.Interface:1:2"
  @realm "autotestrealm"
  @policy1 "@default"
  @queue_name1 "#{@realm}_#{@policy1}_queue"
  @routing_key1 "#{@realm}_#{@policy1}"
  @device_id :crypto.strong_rand_bytes(16) |> Base.url_encode64(padding: false)
  @interface "com.Test.Interface"
  @major_version 1
  @minor_version 1
  @path "/some/path"
  @bson_value %{v: "testvalue"} |> Cyanide.encode!()
  @ip_address "2.3.4.5"

  setup_all do
    {:ok, conn} = Connection.open(Config.amqp_producer_options!())
    {:ok, chan} = Channel.open(conn)
    {:ok, _queue} = Queue.declare(chan, @queue_name1)

    :ok =
      Queue.bind(chan, @queue_name1, Config.events_exchange_name!(), routing_key: @routing_key1)

    on_exit(fn ->
      Channel.close(chan)
      Connection.close(conn)
    end)

    [chan: chan]
  end

  test "HTTP event with no trigger policy handling", %{chan: chan} do
    test_pid = self()

    {:ok, consumer_tag} =
      AMQP.Queue.subscribe(chan, @queue_name1, fn payload, meta ->
        send(test_pid, {:event, payload, meta})
      end)

    simple_trigger_id = :uuid.get_v4()
    parent_trigger_id = :uuid.get_v4()
    static_header_key = "important_metadata_value_change_applied"
    static_header_value = "test_meta_value_change_applied"
    static_headers = [{static_header_key, static_header_value}]
    old_bson_value = %{v: 41} |> Cyanide.encode!()
    new_bson_value = %{v: 42} |> Cyanide.encode!()
    timestamp = get_timestamp()

    target = %AMQPTriggerTarget{
      simple_trigger_id: simple_trigger_id,
      parent_trigger_id: parent_trigger_id,
      static_headers: static_headers,
      routing_key: "trigger_engine"
    }

    TriggersHandler.value_change_applied(
      target,
      @realm,
      @device_id,
      @interface,
      @path,
      old_bson_value,
      new_bson_value,
      timestamp
    )

    assert_receive {:event, payload, meta}

    assert %SimpleEvent{
             device_id: @device_id,
             parent_trigger_id: ^parent_trigger_id,
             simple_trigger_id: ^simple_trigger_id,
             realm: @realm,
             timestamp: ^timestamp,
             event: {:value_change_applied_event, value_change_applied_event}
           } = SimpleEvent.decode(payload)

    assert %ValueChangeAppliedEvent{
             interface: @interface,
             path: @path,
             old_bson_value: ^old_bson_value,
             new_bson_value: ^new_bson_value
           } = value_change_applied_event

    headers_map = amqp_headers_to_map(meta.headers)

    assert Map.get(headers_map, "x_astarte_realm") == @realm
    assert Map.get(headers_map, "x_astarte_trigger_policy") == @policy1
    assert Map.get(headers_map, "x_astarte_device_id") == @device_id
    assert Map.get(headers_map, "x_astarte_event_type") == "value_change_applied_event"

    assert Map.get(headers_map, "x_astarte_simple_trigger_id") |> :uuid.string_to_uuid() ==
             simple_trigger_id

    assert Map.get(headers_map, "x_astarte_parent_trigger_id") |> :uuid.string_to_uuid() ==
             parent_trigger_id

    assert Map.get(headers_map, static_header_key) == static_header_value

    AMQP.Queue.unsubscribe(chan, consumer_tag)
  end

  defp amqp_headers_to_map(headers) do
    Enum.reduce(headers, %{}, fn {key, _type, value}, acc ->
      Map.put(acc, key, value)
    end)
  end

  defp get_timestamp do
    DateTime.utc_now()
    |> DateTime.to_unix(:microsecond)
  end
end
