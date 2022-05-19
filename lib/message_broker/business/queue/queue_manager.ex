defmodule MessageBroker.QueueManager do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:new_sub, sub, topic}, state) do
    IO.puts("NEW SUB")

    {:ok, pid} =
      DynamicSupervisor.start_child(
        MessageBroker.QueueSupervisor,
        {MessageBroker.Queue, [sub: sub]}
      )

    queues = Map.get(state, topic)

    {:noreply, Map.put(state, topic, Enum.concat(queues, [pid]))}
  end

  @impl true
  def handle_cast({:delete_sub, sub, topic}, state) do
    queues = Map.get(state, topic)

    # also send back ACK

    Enum.each(queues, fn queue ->
      received_sub = GenServer.call(queue, {:get_sub})

      if sub == received_sub do
        DynamicSupervisor.terminate_child(MessageBroker.QueueSupervisor, queue)

        GenServer.cast(
          MessageBroker.QueueManager,
          {:delete_queue_from_internal_state, queue, topic}
        )
      end
    end)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:delete_queue_from_internal_state, queue, topic}, state) do
    queues = Map.get(state, topic)

    {:noreply,
     Map.get_and_update(state, topic, fn current_list ->
       {current_list, List.delete(queues, queue)}
     end)}
  end

  @impl true
  def handle_cast({:new_mess, mess, topic}, state) do
    queues = Map.get(state, topic)

    Enum.each(queues, fn queue -> GenServer.cast(queue, {:new_msg, mess}) end)

    {:noreply, state}
  end

  @impl true
  def handle_cast({:ack, sub, topic}, state) do
    queues = Map.get(state, topic)

    # finds the subscriber to be acknowledged
    Enum.each(queues, fn queue ->
      received_sub = GenServer.call(queue, {:get_sub})

      if sub == received_sub do
        GenServer.cast(queue, {:ack})
      end
    end)

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
