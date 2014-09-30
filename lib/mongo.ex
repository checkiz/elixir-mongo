defmodule Mongo do
  use Mongo.Helpers
  @moduledoc """
  [MongoDB](http://www.mongodb.org) driver in Elixir

  See [elixir-mongo source repo](https://github.com/checkiz/elixir-mongo)
  """

  @doc """
  Connects to a Mongo Database Server, see `Mongo.Server.connect/0`
  """
  defdelegate connect,              to: Mongo.Server

  @doc """
  Connects to a Mongo Database Server, see `Mongo.Server.connect/2`
  """
  defdelegate connect(host, port),  to: Mongo.Server

  @doc """
  Connects to a Mongo Database Server, see `Mongo.Server.connect/1`
  """
  defdelegate connect(opts),        to: Mongo.Server
  defbang connect
  defbang connect(opts)
  defbang connect(host, port)

  @doc """
  Returns a db struct `%Mongo.Db{}, see `Mongo.Server.new/2``
  """
  defdelegate db(mongo, name),        to: Mongo.Db, as: :new

  @doc """
  Helper function that assigns radom ids to a list of documents when `:_id` is missing

  see `Mongo.Server.assign_id/2`
  """
  defdelegate assign_id(docs), to: Mongo.Server

  @doc """
  Helper function that assigns radom ids (with prefix) to a list of documents when `:_id` is missing

  see `Mongo.Server.assign_id/2`
  """
  defdelegate assign_id(docs, mongo), to: Mongo.Server

  defmodule Error, do: defstruct([msg: nil, acc: []])

  defmodule Bang do
    defexception [:message, :stack]
    def exception(message) when is_bitstring(message), do: %Bang{message: message}
    def exception(msg: msg, acc: acc), do: %Bang{message: inspect(msg), stack: acc}
  end

end
