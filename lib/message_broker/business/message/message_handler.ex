defmodule MessageBroker.MessageHandler do
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def handle_cast({:new_message, message}, state) do
    {:noreply, Enum.concat(state, [message])}
  end

  @impl true
  def init(args) do
    IO.puts("MESSAGE HANDLER")
    {:ok, args}
  end
end
