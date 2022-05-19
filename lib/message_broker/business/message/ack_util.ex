defmodule AckUtil do
  def send_back_ack(message, topic, socket) do
    ack_message = %Message{
      :action => "ACKNOWLEDGE",
      :topic => topic,
      :message => message
    }

    {:ok, message_to_send} = Poison.encode(ack_message)
    MessageBroker.Controller.write_line(message_to_send, socket)
  end
end
