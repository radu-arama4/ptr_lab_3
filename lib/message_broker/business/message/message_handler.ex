defmodule MessageBroker.MessageHandler do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:new_message, message, topic}, state) do
    # will also distribute the message based on topic

    case validate_topic(topic) do
      true ->
        # also will store the messages to some storage
        {:noreply, Enum.concat(state, [message])}

      false ->
        # later send message back
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

  @impl true
  def init(args) do
    IO.puts("MESSAGE HANDLER")
    extract_messages()
    {:ok, args}
  end

  def extract_messages() do
  end
end
