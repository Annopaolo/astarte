defmodule Astarte.DataUpdaterPlant.DataUpdater.Core.Error do
  alias Astarte.DataUpdaterPlant.DataUpdater.Core.Error
  alias Astarte.DataUpdaterPlant.DataUpdater.Impl.Core
  alias Astarte.DataUpdaterPlant.MessageTracker
  require Logger

  @keys [
    :state,
    :message_id,
    :timestamp,
    :log_line,
    :tag,
    :telemetry_tag,
    :metadata,
    :state_update_fun
  ]

  @enforce_keys @keys
  defstruct @keys

  @type t :: %__MODULE__{
          state: Astarte.DataUpdaterPlant.DataUpdater.State.t(),
          message_id: binary(),
          log_line: String.t(),
          tag: String.t(),
          telemetry_tag: atom(),
          metadata: map(),
          state_update_fun: fun() | nil
        }

  def ask_clean_session_and_error(%Error{} = error) do
    Logger.warn(error.log_line, tag: error.tag)
    {:ok, new_state} = Core.ask_clean_session(error.state, error.timestamp)
    MessageTracker.discard(new_state.message_tracker, error.message_id)

    :telemetry.execute(
      [:astarte, :data_updater_plant, :data_updater, error.telemetry_tag],
      %{},
      %{realm: new_state.realm}
    )

    Core.execute_device_error_triggers(
      new_state,
      error.tag,
      error.metadata,
      error.timestamp
    )

    if error.state_update_fun, do: error.state_update_fun.(new_state)
  end
end
