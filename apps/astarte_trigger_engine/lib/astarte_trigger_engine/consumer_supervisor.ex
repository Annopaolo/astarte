defmodule Astarte.TriggerEngine.ConsumerSupervisor do
  # Automatically defines child_spec/1
  use Supervisor
  require Logger

  alias Astarte.TriggerEngine.AMQPConsumerSupervisor
  alias Astarte.TriggerEngine.AMQPConsumerTracker

  def start_link(init_arg) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def init(_init_arg) do
    # make amqp supervisors logs less verbose
    :logger.add_primary_filter(
      :ignore_rabbitmq_progress_reports,
      {&:logger_filters.domain/2, {:stop, :equal, [:progress]}}
    )

    Logger.info("Starting consumer supervisor", tag: "consumer_supervisor_start")

    children = [
      AMQPConsumerSupervisor,
      {Registry, [keys: :unique, name: Registry.AMQPConsumerRegistry]},
      AMQPConsumerTracker
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
