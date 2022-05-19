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
    IO.puts("NEW QUEUE")
    work()
    {:ok, args}
  end

  @impl true
  def handle_call({:get_sub}, _from, state) do
    {:reply, state[:sub], state}
  end

  @impl true
  def handle_cast({:new_msg, message}, state) do
    IO.puts("QUEUE RECEIVED MESSAGE")

    {
      :noreply,
      %{:sub => state[:sub], :ack => state[:ack], :queue => :queue.in(message, state[:queue])}
    }
  end

  @impl true
  def handle_cast({:ack}, state) do
    IO.puts("ACK!! REMOVING MESSAGE")

    {
      :noreply,
      %{:sub => state[:sub], :ack => true, :queue => :queue.peek(state[:queue])}
    }
  end

  @impl true
  def handle_info({:send}, state) do
    if state[:ack] == true && !:queue.is_empty(state[:queue]) do
      queue = state[:queue]
      message = :queue.head(queue)
      # send message
      IO.inspect(message)

      {:ok, message_to_send} = Poison.encode(message)

      MessageBroker.Controller.write_line(message_to_send, state[:sub])
      work()
      {:noreply, %{:sub => state[:sub], :ack => false, :queue => state[:queue]}}
    else
      work()
      {:noreply, state}
    end
  end

  def work() do
    Process.send_after(self(), {:send}, 100)
  end
end
