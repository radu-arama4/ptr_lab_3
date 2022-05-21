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
        Logger.info("New message validated")
        # also will store the messages to some storage

        AckUtil.send_back_ack("Message received and validated!", "", socket)
        {:noreply, Enum.concat(state, [{message, topic}])}

      false ->
        AckUtil.send_back_ack("Message received but not validated!", "", socket)
        Logger.info("Message not validated")
        {:noreply, state}
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
    Process.send_after(self(), {:extract_and_send}, 100)
  end

  @impl true
  def handle_info({:extract_and_send}, state) do
    # will extract from storage normally
    if !Enum.empty?(state) do
      {message, topic} = Enum.at(state, 0)
      GenServer.cast(MessageBroker.QueueManager, {:new_mess, message, topic})
      extract_messages()
      {:noreply, List.delete_at(state, 0)}
    else
      extract_messages()
      {:noreply, state}
    end
  end

  @impl true
  def init(args) do
    extract_messages()
    {:ok, args}
  end
end
