defmodule Message do
  @derive [Poison.Encoder]
  defstruct [:action, :topic, :message]
end
