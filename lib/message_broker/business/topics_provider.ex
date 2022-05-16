defmodule MessageBroker.TopicsProvider do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, topics: ['user', 'tweet'], name: __MODULE__)
  end

  @impl true
  def handle_call({:get_topics}, _from, state) do
    {:reply, state.topics, state}
  end

  @impl true
  def init(args) do
    {:ok, args}
  end
end
