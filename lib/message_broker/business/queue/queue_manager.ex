defmodule MessageBroker.QueueManager do
  use GenServer
  require Logger

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:new_sub, sub, topic}, state) do
    Logger.info("New subscriber #{inspect(sub)} to topic #{inspect(topic)}")

    {:ok, pid} =
      DynamicSupervisor.start_child(
        MessageBroker.QueueSupervisor,
        {MessageBroker.Queue, [sub: sub, topic: topic]}
      )

    # also add new one in message_handler
    GenServer.cast(MessageBroker.MessageHandler, {:new_sub, sub, topic})

    queues = Map.get(state, topic)

    {:noreply, Map.put(state, topic, Enum.concat(queues, [pid]))}
  end

  @impl true
  def handle_cast({:delete_sub, sub, topic}, state) do
    queues = Map.get(state, topic)

    Enum.each(queues, fn queue ->
      received_sub = GenServer.call(queue, {:get_sub})

      if sub == received_sub do
        DynamicSupervisor.terminate_child(MessageBroker.QueueSupervisor, queue)

        Logger.info("Consumer #{inspect(sub)} unsubscribed from #{inspect(topic)}")
        AckUtil.send_back_ack("User unsubscribed!", topic, sub)

        # remove also from message_handler
        GenServer.cast(MessageBroker.MessageHandler, {:delete_sub, sub, topic})

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

    Enum.each(queues, fn queue ->
      received_sub = GenServer.call(queue, {:get_sub})

      Logger.info(
        "Acknowledge received. Removing last message for #{inspect(sub)} from topic #{inspect(topic)}"
      )

      if sub == received_sub do
        GenServer.cast(queue, {:ack})
      end
    end)

    {:noreply, state}
  end

  @impl true
  def init(_args) do
    topics = GenServer.call(MessageBroker.TopicsProvider, {:get_topics})

    Logger.info("queue_manager initializing with topics: #{inspect(topics)}")

    {:ok,
     topics
     |> Enum.chunk_every(1)
     |> Map.new(fn [k] -> {k, []} end)}
  end
end
