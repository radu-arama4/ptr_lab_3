defmodule MessageBroker.Queue do
  use GenServer

  def start_link(args) do
    queue = :queue.new()

    GenServer.start_link(__MODULE__, %{:sub => args[:sub], :ack => true, :queue => queue},
      name: __MODULE__
    )
  end

  @impl true
  def init(args) do
    work()
    {:ok, args}
  end

  @impl true
  def handle_info({:send}, state) do
    if state[:ack] == true do
      message = state[:queue].out()
      # send message
      IO.inspect(message)
      MessageBroker.Controller.write_line(message, state[:sub])
      {:noreply, %{:sub => state[:sub], :ack => false, :queue => state[:queue]}}
    else
      {:noreply, state}
    end
  end

  def work() do
    Process.send_after(self(), {:send}, 100)
  end
end
