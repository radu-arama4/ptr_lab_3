defmodule MessageBroker.TopicsProvider do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, subscribers: [], name: __MODULE__)
  end

  def handle_call({:get_topics}, from, state) do
  end

  @impl true
  def init(args) do
    {:ok, args}
  end
end
