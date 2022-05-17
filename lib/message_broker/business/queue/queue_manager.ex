defmodule MessageBroker.QueueManager do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:new_sub, sub, topic}, state) do
    {:ok, pid} =
      DynamicSupervisor.start_child(
        MessageBroker.QueueSupervisor,
        sub
      )

    queues = Map.get(state, topic)

    {:reply, Map.put(state, topic, Enum.concat(queues, [pid]))}
  end

  @impl true
  def handle_cast({:new_mess, mess, topic}, state) do
    queues = Map.get(state, topic)

    Enum.each(queues, fn queue -> GenServer.cast(queue, {:new_msg, mess}) end)

    {:noreply, state}
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
