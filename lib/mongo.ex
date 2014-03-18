defmodule Mongo do
  use Mongo.Helpers
  @moduledoc """
  [MongoDB](http://www.mongodb.org) driver in Elixir

  See [elixir-mongo source repo](https://github.com/checkiz/elixir-mongo)
  """

  @doc """
  Connects to a Mongo Database Server, see module `Mongo.Server`for details
  """
  defdelegate connect,              to: Mongo.Server
  @doc """
  Connects to a Mongo Database Server, see module `Mongo.Server`for details
  """
  defdelegate connect(host, port),  to: Mongo.Server
  @doc """
  Connects to a Mongo Database Server, see module `Mongo.Server`for details
  """
  defdelegate connect(opts),        to: Mongo.Server
  defbang connect
  defbang connect(opts)
  defbang connect(host, port)

  @doc """
  Assigns radom ids to a list of documents when `:_id` is missing

      [%{a: 1}] |> Mongo.assign_id
      [%{_id: ObjectId(...), a: 1}]
  """
  defdelegate assign_id(docs), to: Mongo.Server

  @doc """
  Assigns sequential ids to a list of documents when `:_id` is missing

      [%{a: 1}] |> Mongo.assign_id(mongo)
      [%{_id: ObjectId(...), a: 1}]
  """
  defdelegate assign_id(docs, mongo), to: Mongo.Server

  defexception Error, reason: nil, context: nil do

    def message(Error[reason: reason, context: nil]) do
      inspect(reason)
    end
    def message(Error[reason: reason, context: context]) do
      inspect(reason) <> "\n" <> inspect(context)
    end
  end
end
