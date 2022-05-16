defmodule MessageBroker.SubscribersKeeper do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:subscribe, socket, topic}, state) do
    # will check if the requested topic is present and based on that will store it in the internal state
    case validate_topic(topic) do
      true ->
        IO.puts("FOUND TOPIC")
        # TODO check if the subscriber already exists
        # TODO serialize map

        # send to queue manager

        MessageBroker.Controller.write_line(
          "{\"action\": \"ACKNOWLEDGE\", \"topic\": \"\", \"message\": {}}\r\n",
          socket
        )

        {:noreply, subscribers: Enum.concat(state, [%{:socket => socket, :topic => topic}])}

      false ->
        # later send message back
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:unsubscribe, socket, topic}, state) do
    {:noreply, List.delete(state, %{:socket => socket, :topic => topic})}
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
    IO.puts("SUBSCRIBERS KEEPER")
    {:ok, args}
  end
end
