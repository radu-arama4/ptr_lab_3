defmodule MessageBroker.TopicsProvider do
  use GenServer
  require Logger

  def start_link(_args) do
    GenServer.start_link(__MODULE__, ["user", "tweet"], name: __MODULE__)
  end

  @impl true
  def handle_call({:get_topics}, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def init(args) do
    Logger.info("topics_provider initializing")
    {:ok, args}
  end
end
