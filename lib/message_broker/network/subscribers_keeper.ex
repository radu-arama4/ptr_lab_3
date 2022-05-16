defmodule MessageBroker.SubscribersKeeper do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, subscribers: [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:subscribe, socket, topic}, state) do
    # will check if the requested topic is present and based on that will store it in the internal state
    case validate_topic(topic) do
      true ->
        # TODO check if the subscriber already exists
        # TODO serialize map

        # line = %Message{:action => "ACKNOWLEDGE", :topic => "", :message => ""}
        # mess_to_send = Poison.encode(Enum.to_list(line))
        # MessageBroker.Controller.write_line(mess_to_send, socket)
        MessageBroker.Controller.write_line("ACK", socket)

        {:noreply,
         subscribers: Enum.concat(state.subscribers, [%{:socket => socket, :topic => topic}])}

      false ->
        # later send message back
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:unsubscribe, socket, topic}, state) do
    {:noreply, List.delete(state.subscribers, %{:socket => socket, :topic => topic})}
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
    {:ok, args}
  end
end
