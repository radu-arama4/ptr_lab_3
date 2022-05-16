defmodule MessageBroker.QueueManager do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_call({:new_sub, sub}, _from, state) do
    # {:ok, pid} =
    #   DynamicSupervisor.start_child(
    #     MessageBroker.QueueSupervisor,
    #     subt: sub
    #   )

    {:reply, state, state}
  end

  @impl true
  def init(_args) do
    # will fetch all the topics
    topics = GenServer.call(MessageBroker.TopicsProvider, {:get_topics})

    {:ok,
     topics
     |> Enum.chunk_every(1)
     |> Map.new(fn [k] -> {k, []} end)}
  end
end
