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
  def handle_cast({:new_msg, message}, state) do
    {
      :noreply,
      %{:sub => state[:sub], :ack => state[:ack], :queue => :queue.in(state[:queue], message)}
    }
  end

  @impl true
  def handle_cast({:ack}, state) do
    {
      :noreply,
      %{:sub => state[:sub], :ack => true, :queue => state[:queue]}
    }
  end

  @impl true
  def handle_info({:send}, state) do
    if state[:ack] == true do
      message = state[:queue].out()
      # send message
      IO.inspect(message)
      MessageBroker.Controller.write_line(message, state[:sub])
      {:noreply, %{:sub => state[:sub], :ack => false, :queue => state[:queue]}}
      work()
    else
      work()
      {:noreply, state}
    end
  end

  def work() do
    Process.send_after(self(), {:send}, 100)
  end
end
