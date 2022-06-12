defmodule MessageBroker.MessageHandler do
  use GenServer
  require Logger

  def start_link(_args) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @impl true
  def handle_cast({:new_message, message, topic, socket}, state) do
    case validate_topic(topic) do
      true ->
        Logger.info("New message - #{inspect(message)}")

        AckUtil.send_back_ack("Message received and validated!", "", socket)

        topics_map = state[:messages]

        case Map.fetch(topics_map, topic) do
          {:ok, sub_map} ->
            list =
              Enum.map(sub_map, fn {sub, queue_and_state_map} -> {sub, queue_and_state_map} end)

            new_sub_map =
              Enum.reduce(list, %{}, fn {sub, queue_and_state_map}, acc ->
                new_queue = :queue.in(message, queue_and_state_map[:queue])
                new_queue_and_state_map = Map.put(queue_and_state_map, :queue, new_queue)

                Map.put(acc, sub, new_queue_and_state_map)
              end)

            {:noreply,
             %{
               :messages => Map.replace(topics_map, topic, new_sub_map),
               :messages_state => state[:messages_state]
             }}

          :error ->
            {:noreply, state}
        end

      false ->
        AckUtil.send_back_ack("Message received but not validated!", "", socket)
        Logger.warn("Message not validated")
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:new_sub, sub, topic}, state) do
    topics_map = state[:messages]

    case Map.fetch(topics_map, topic) do
      {:ok, sub_map} ->
        new_sub_map = Map.put(sub_map, sub, %{:queue => :queue.new(), :ack_state => true})
        new_topics_map = Map.put(topics_map, topic, new_sub_map)

        {:noreply, %{:messages => new_topics_map}}

      :error ->
        Logger.error("No such topic")
    end
  end

  @impl true
  def handle_cast({:delete_sub, sub, topic}, state) do
    topics_map = state[:messages]

    case Map.fetch(topics_map, topic) do
      {:ok, sub_map} ->
        new_sub_map = Map.delete(sub_map, sub)
        new_topics_map = Map.put(topics_map, topic, new_sub_map)

        {:noreply, %{:messages => new_topics_map}}

      :error ->
        Logger.error("No such topic")
        {:noreply, state}
    end
  end

  def handle_cast({:delete_message, sub, topic}, state) do
    topics_map = state[:messages]

    case Map.fetch(topics_map, topic) do
      {:ok, sub_map} ->
        case Map.fetch(sub_map, sub) do
          {:ok, sub_value_map} ->
            case Map.fetch(sub_value_map, :queue) do
              {:ok, queue} ->
                {_peek, new_queue} = :queue.out(queue)

                new_sub_value_map = Map.put(sub_value_map, :queue, new_queue)
                new_sub_value_map = Map.put(new_sub_value_map, :ack_state, true)

                new_sub_map = Map.put(sub_map, sub, new_sub_value_map)
                new_topics_map = Map.put(topics_map, topic, new_sub_map)
                {:noreply, %{:messages => new_topics_map}}

              :error ->
                nil
            end

          :error ->
            nil
        end

      :error ->
        nil
    end
  end

  defp validate_topic(topic) do
    topics = GenServer.call(MessageBroker.TopicsProvider, {:get_topics})

    case Enum.member?(topics, topic) do
      true -> true
      false -> false
    end
  end

  def extract_messages() do
    Process.send_after(self(), {:extract_and_send}, 200)
  end

  def backup_messages() do
    Process.send_after(self(), {:store_messages}, 2000)
  end

  @impl true
  def handle_info({:extract_and_send}, state) do
    send_multiple_messages(state)
    extract_messages()
    {:noreply, state}
  end

  # change the queues state to true and store messages
  @impl true
  def handle_info({:store_messages}, state) do
    {:ok, table} = :dets.open_file(:messages, type: :set)

    messages = state[:messages]
    list_of_topics = Enum.map(messages, fn {topic, sub_map} -> {topic, sub_map} end)

    messages_to_store =
      Enum.reduce(list_of_topics, %{}, fn {topic, sub_map}, acc ->
        list_of_sub = Enum.map(sub_map, fn {sub, map} -> {sub, map} end)

        sub_map_to_store =
          Enum.reduce(list_of_sub, %{}, fn {sub, map_of_queue_and_state}, acc ->
            new_map_of_queue_and_state = Map.put(map_of_queue_and_state, :ack_state, true)
            Map.put(acc, sub, new_map_of_queue_and_state)
          end)

        Map.put(acc, topic, sub_map_to_store)
      end)

    :dets.insert(table, {:messages, messages_to_store})
    :dets.close(table)

    backup_messages()

    {:noreply, state}
  end

  # updates the state of a specific queue
  @impl true
  def handle_cast({:change_queue_state, topic, sub, new_queue_state}, state) do
    topics_map = state[:messages]

    case Map.fetch(topics_map, topic) do
      {:ok, sub_map} ->
        case Map.fetch(sub_map, sub) do
          {:ok, sub_value_map} ->
            new_sub_value_map = Map.put(sub_value_map, :ack_state, new_queue_state)
            new_sub_map = Map.put(sub_map, sub, new_sub_value_map)
            new_topics_map = Map.put(topics_map, topic, new_sub_map)
            {:noreply, %{:messages => new_topics_map}}

          :error ->
            nil
        end

      :error ->
        nil
    end
  end

  defp send_multiple_messages(state) do
    topics_map = state[:messages]
    topics_list = Enum.map(topics_map, fn {key, value} -> {key, value} end)

    Enum.each(topics_list, fn {topic, sub_map} ->
      sub_list =
        Enum.map(sub_map, fn {key, queue_and_state_map} -> {key, queue_and_state_map} end)

      Enum.each(sub_list, fn {sub, queue_and_state_map} ->
        # check queues state and if queue is not empty
        if queue_and_state_map[:ack_state] == true &&
             !:queue.is_empty(queue_and_state_map[:queue]) do
          message = :queue.get(queue_and_state_map[:queue])

          Logger.info("Sending the message to QueueManager !!!")

          # send the message
          GenServer.cast(MessageBroker.QueueManager, {:new_mess, message, topic})

          # change queue state
          GenServer.cast(MessageBroker.MessageHandler, {:change_queue_state, topic, sub, false})
        end
      end)
    end)
  end

  defp init_table(table) do
    topics = GenServer.call(MessageBroker.TopicsProvider, {:get_topics})

    topics_map =
      topics
      |> Enum.chunk_every(1)
      |> Map.new(fn [k] -> {k, Map.new()} end)

    :dets.insert(table, {:messages, topics_map})
    :dets.close(table)

    topics_map
  end

  @impl true
  def init(args) do
    {:ok, table} = :dets.open_file(:messages, type: :set)

    case :dets.lookup(table, :messages) do
      [] ->
        Logger.info("Creating a new file for storing messages")
        map = init_table(table)
        extract_messages()
        backup_messages()
        {:ok, %{:messages => map}}

      [_map] ->
        Logger.info("The messages file is not empty")
        map = :dets.lookup(table, :messages)
        extract_messages()
        backup_messages()
        {:ok, %{:messages => map[:messages]}}

      _ ->
        Logger.error("Cannot open the file!")
        {:stop, args}
    end
  end
end
