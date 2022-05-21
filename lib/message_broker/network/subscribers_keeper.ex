defmodule MessageBroker.SubscribersKeeper do
  use GenServer
  require Logger

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:subscribe, socket, topic}, state) do
    # will check if the requested topic is present and based on that will store it in the internal state
    case validate_topic(topic) do
      true ->
        GenServer.cast(MessageBroker.QueueManager, {:new_sub, socket, topic})
        AckUtil.send_back_ack("User subscribed!", topic, socket)

        {:noreply, subscribers: Enum.concat(state, [%{:socket => socket, :topic => topic}])}

      false ->
        AckUtil.send_back_ack("Not existing topic!", topic, socket)
        {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:unsubscribe, socket, topic}, state) do
    GenServer.cast(MessageBroker.QueueManager, {:delete_sub, socket, topic})
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
    Logger.info("subscribers_keeper initializing")
    {:ok, args}
  end
end
