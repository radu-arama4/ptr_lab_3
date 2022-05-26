defmodule MessageBroker.MessageHandler do
  use GenServer
  require Logger

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:new_message, message, topic, socket}, state) do
    case validate_topic(topic) do
      true ->
        # store_message(message, topic, socket)
        Logger.info("New message - #{message}")

        # store_message(message, topic)

        AckUtil.send_back_ack("Message received and validated!", "", socket)

        {:noreply, Enum.concat(state, [{message, topic}])}

      false ->
        AckUtil.send_back_ack("Message received but not validated!", "", socket)
        Logger.warn("Message not validated")
        {:noreply, state}
    end
  end

  defp store_message(message, topic) do
    {:ok, table} = :dets.open_file(:messages, type: :set)
    ack = false
    # :dets.insert(table, {:messages, [{message, topic, ack}})
    :dets.close(table)
  end

  defp validate_topic(topic) do
    topics = GenServer.call(MessageBroker.TopicsProvider, {:get_topics})

    case Enum.member?(topics, topic) do
      true -> true
      false -> false
    end
  end

  def extract_messages() do
    Process.send_after(self(), {:extract_and_send}, 100)
  end

  @impl true
  def handle_info({:extract_and_send}, state) do
    # will extract from storage normally
    if !Enum.empty?(state) do
      {message, topic} = Enum.at(state, 0)
      GenServer.cast(MessageBroker.QueueManager, {:new_mess, message, topic})
      extract_messages()
      # extract_message_from_storage()
      {:noreply, List.delete_at(state, 0)}
    else
      extract_messages()
      {:noreply, state}
    end
  end

  defp extract_message_from_storage() do
    {:ok, table} = :dets.open_file(:messages, type: :set)

    list = :dets.lookup(table, :messages)

    Enum.each([1..5], fn _nr ->
      random_number = :rand.uniform(list.length)
      {message, topic, ack} = Enum.at(list, random_number, nil)
      Logger.info("#{message} and #{topic} and #{ack}")
    end)
  end

  defp init_table(table) do
    :dets.insert(table, {:messages, []})
  end

  @impl true
  def init(args) do
    {:ok, table} = :dets.open_file(:messages, type: :set)

    case :dets.lookup(table, :map) do
      [] ->
        Logger.info("Creating a new file for storing messages")
        init_table(table)

      [_map] ->
        Logger.info("The messages file is not empty")

      _ ->
        Logger.error("Cannot open the file!")
    end

    :dets.close(table)

    extract_messages()
    {:ok, args}
  end
end
