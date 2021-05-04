defmodule Astarte.TriggerEngine.AMQPConsumerSupervisor do
  require Logger
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    Logger.info("Starting AMQP consumer supervisor", tag: "amqp_consumer_supervisor_start")
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_child(child) do
    Logger.info("Adding new policy to AMQP consumer supervisor",
      tag: "amqp_consumer_supervisor_add"
    )

    DynamicSupervisor.start_child(__MODULE__, child)
  end
end
