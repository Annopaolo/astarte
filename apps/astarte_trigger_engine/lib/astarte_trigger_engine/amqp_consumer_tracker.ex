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

defmodule Astarte.TriggerEngine.AMQPConsumerTracker do
  TODO
  # Spawns (and kills?) amqp consumers, based on what trigger policies
  # are available.
  # Once in a little while (10 min?) checks what policies are available
  # in the database.
  # Then, much like Santa, checks the list twice with PolicyRegistry,
  # finding nice policies (to be kept) or naughty ones (to be started).
  # Finally, starts a new consumer for each policy to be started.
end
