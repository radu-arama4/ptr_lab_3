defmodule MessageBroker.SubscribersKeeper do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, subscribers: [], name: __MODULE__)
  end

  def handle_cast({:new_subscriber, socket}, state) do
    # will check if the requested topic is present and based on that will store it in the internal state
  end

  @impl true
  def init(args) do
    {:ok, args}
  end
end
