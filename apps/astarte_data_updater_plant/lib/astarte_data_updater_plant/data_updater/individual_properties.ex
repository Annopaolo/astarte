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

defmodule Astarte.DataUpdaterPlant.IndividualProperty do
  use TypedEctoSchema
  alias Astarte.DataUpdaterPlant.UUID

  @primary_key false
  typed_schema "individual_properties" do
    field :device_id, UUID
    field :interface_id, UUID
    field :endpoint_id, UUID
    field :path, :string
    field :reception_timestamp, :datetime
    # TODO Exandra does not have :smallint type
    field :reception_timestamp_submillis, :smallint
    field :binaryblob_value, :binary
    field :binaryblobarray_value, {:array, :binary}
    field :boolean_value, :boolean
    field :booleanarray_value, {:array, :boolean}
    field :datetime_value, :timestamp
    field :datetimearray_value, {:array, :datetime}
    field :double_value, :double
    field :doublearray_value, {:array, :double}
    field :integer_value, :integer
    field :integerarray_value, {:array, :integer}
    # TODO Exandra does not have :bigint type
    field :longinteger_value, :bigint
    field :longintegerarray_value, {:array, :bigint}
    field :string_value, :string
    field :stringarray_value, {:array, :string}
  end
end
