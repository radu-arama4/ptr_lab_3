defmodule MessageBroker.Controller do
  use GenServer
  require Logger

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def accept(port) do
    {:ok, socket} =
      :gen_tcp.listen(
        port,
        [:binary, packet: :line, active: false, reuseaddr: true]
      )

    Logger.info("Accepting connections on port #{port}")
    loop_acceptor(socket)
  end

  def subscribe(socket, topic) do
    IO.puts("SUBSCRIBING")
    IO.inspect(socket)
    GenServer.cast(MessageBroker.SubscribersKeeper, {:subscribe, socket, topic})
  end

  def unsubscribe(socket, topic) do
    IO.puts("UNSUBSCRIBING")
    GenServer.cast(MessageBroker.SubscribersKeeper, {:unsubscribe, socket, topic})
  end

  def acknowledge(_socket, _topic) do
    # will remove message from queue
    IO.puts("NEW ACKNOWLEDGMENT")
  end

  def publish(_socket, _topic, _message) do
    # will add message to queue
    IO.puts("NEW PUBLISHMENT")
  end

  defp loop_acceptor(socket) do
    {:ok, client} = :gen_tcp.accept(socket)

    # :queue.new()

    {:ok, pid} =
      Task.Supervisor.start_child(MessageBroker.ConnectionsSupervisor, fn ->
        serve(client)
      end)

    :ok = :gen_tcp.controlling_process(client, pid)
    loop_acceptor(socket)
  end

  def parse_message(message) do
    case Poison.decode(message, as: %Message{}) do
      {:ok, %Message{:action => "SUBSCRIBE", :topic => topic}} ->
        {:ok, {:subscribe, topic}}

      {:ok, %Message{:action => "UNSUBSCRIBE", :topic => topic}} ->
        {:ok, {:unsubscribe, topic}}

      {:ok, %Message{:action => "ACKNOWLEDGE", :topic => topic}} ->
        {:ok, {:acknowledge, topic}}

      {:ok, %Message{:action => "PUBLISH", :topic => topic, :message => message}} ->
        {:ok, {:publish_message, topic, message}}

      {:error, _error} ->
        IO.puts("WAAAAI")
        %Message{action: "ERROR", message: "ERROR\r\n"}

      _ ->
        IO.puts("WAAAAI")
        %Message{action: "ERROR", message: "ERROR\r\n"}
    end
  end

  def validate_message(message, socket) do
    case parse_message(message) do
      {:ok, {:subscribe, topic}} ->
        subscribe(socket, topic)

      {:ok, {:unsubscribe, topic}} ->
        unsubscribe(socket, topic)

      {:ok, {:acknowledge, topic}} ->
        acknowledge(socket, topic)

      {:ok, {:publish, topic, message}} ->
        publish(socket, topic, message)

      {:error, _error} ->
        IO.puts("ERROR")

      _ ->
        IO.puts("ERROR")
    end
  end

  defp serve(socket) do
    message = read_line(socket)
    IO.inspect(message)

    validate_message(message, socket)

    serve(socket)
  end

  defp read_line(socket) do
    {:ok, data} = :gen_tcp.recv(socket, 0)
    data
  end

  def write_line(line, socket) do
    :gen_tcp.send(socket, line)
  end

  @impl true
  def init(args) do
    accept(8080)
    {:ok, args}
  end
end
