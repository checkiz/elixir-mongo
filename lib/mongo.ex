defmodule Mongo do
  use Mongo.Helpers
  @moduledoc """
  [MongoDB](http://www.mongodb.org) driver in Elixir
  """

  @doc """
  Connects to a Mongo Database Server, see module `Mongo.Server`for details
  """
  defdelegate connect,                      to: Mongo.Server
  @doc """
  Connects to a Mongo Database Server, see module `Mongo.Server`for details
  """
  defdelegate connect(host, port),  to: Mongo.Server
  @doc """
  Connects to a Mongo Database Server, see module `Mongo.Server`for details
  """
  defdelegate connect(opts),    to: Mongo.Server
  defbang connect
  defbang connect(host, port)
  defbang connect(opts)
  @doc """
  Assigns id to a document or a list of document when `:_id` is missing

      [a: 1] |> Mongo.assign_id
      [_id: ObjectId(...), a: 1]
  """
  def assign_id([{a,_}]=doc) when is_atom(a) do
    Keyword.put(doc, :'_id', Bson.ObjectId.new(oid: :crypto.rand_bytes(12)))
  end
  def assign_id(docs) do
    Enum.map_reduce(
      docs,
      case length(docs) do
        l when l < 256     -> <<0::8>>  <> :crypto.rand_bytes(11)
        l when l < 256*256 -> <<0::16>> <> :crypto.rand_bytes(10)
        _ ->                  <<0::24>> <> :crypto.rand_bytes(9)
      end,
      fn(doc, id) -> { Keyword.put(doc, :'_id', Bson.ObjectId.new(oid: to_oid(id))), next_id(id) } end)
      |> elem(0)
  end

  # from random ID to ObjectID
  defp to_oid(id), do: id |> bitstring_to_list |> Enum.reverse |> list_to_bitstring
  # Selects next ID
  defp next_id(<<255, r::binary>>), do: <<0::8, (next_id(r))::binary>>
  defp next_id(<<n::8, r::binary>>), do: <<(n+1)::8, r::binary>>

  defexception Error, reason: nil, context: nil do

    def message(Error[reason: reason, context: nil]) do
      inspect(reason)
    end
    def message(Error[reason: reason, context: context]) do
      inspect(reason) <> "\n" <> inspect(context)
    end
  end
end
