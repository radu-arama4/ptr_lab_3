defmodule MessageBroker.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {MessageBroker.TopicsProvider, []},
      {MessageBroker.QueueManager, []},
      {MessageBroker.SubscribersKeeper, []},
      {Task.Supervisor, name: MessageBroker.ConnectionsSupervisor},
      {MessageBroker.Controller, []}
    ]

    opts = [strategy: :one_for_one, name: MessageBroker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
