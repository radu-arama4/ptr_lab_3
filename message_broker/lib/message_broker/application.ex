defmodule MessageBroker.Application do
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {MessageBroker.Controller, []},
      {MessageBroker.SubscribersKeeper, []}
    ]

    opts = [strategy: :one_for_one, name: MessageBroker.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
