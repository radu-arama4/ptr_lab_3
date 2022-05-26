defmodule MessageBroker.Queue do
  use GenServer
  require Logger

  def start_link(args) do
    queue = :queue.new()

    GenServer.start_link(
      __MODULE__,
      %{:sub => args[:sub], :topic => args[:topic], :ack => true, :queue => queue},
      args
    )
  end

  @impl true
  def init(args) do
    Logger.info("New Queue defined")
    work()
    {:ok, args}
  end

  @impl true
  def handle_call({:get_sub}, _from, state) do
    {:reply, state[:sub], state}
  end

  @impl true
  def handle_cast({:new_msg, message}, state) do
    Logger.info("New message in queue for subscriber #{inspect(state[:sub])}")

    {
      :noreply,
      %{
        :sub => state[:sub],
        :topic => state[:topic],
        :ack => state[:ack],
        :queue => :queue.in(message, state[:queue])
      }
    }
  end

  @impl true
  def handle_cast({:ack}, state) do
    {_peek, new_queue} = :queue.out(state[:queue])

    Logger.info("State - #{inspect(state)}")

    {
      :noreply,
      %{:sub => state[:sub], :topic => state[:topic], :ack => true, :queue => new_queue}
    }
  end

  @impl true
  def handle_info({:send}, state) do
    if state[:ack] == true && !:queue.is_empty(state[:queue]) do
      queue = state[:queue]
      message = :queue.head(queue)

      message_to_send = %Message{
        :action => "PUBLISH",
        :topic => state[:topic],
        :message => message
      }

      {:ok, encoded_message} = Poison.encode(message_to_send)

      MessageBroker.Controller.write_line(encoded_message, state[:sub])
      work()

      {:noreply,
       %{:sub => state[:sub], :topic => state[:topic], :ack => false, :queue => state[:queue]}}
    else
      work()
      {:noreply, state}
    end
  end

  def work() do
    Process.send_after(self(), {:send}, 100)
  end
end
