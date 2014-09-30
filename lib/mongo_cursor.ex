defmodule Mongo.Cursor do
  require Record
  @moduledoc """
  Manages MongoDB [cursors](http://docs.mongodb.org/manual/core/cursors/).

  Cursors are returned by a find see `Mongo.Collection.find/3`.
  """
  use Mongo.Helpers

  defstruct [
    collection: nil,
    response: nil,
    batchSize: 0,
    exhausted: nil]

  def next_batch(%Mongo.Cursor{exhausted: true}), do: nil
  def next_batch(cursor) do
    mongo = cursor.collection.db.mongo
    case Mongo.Server.send(mongo, Mongo.Request.get_more(cursor.collection, cursor.batchSize, cursor.response.cursorID)) do
        {:ok, _reqid} ->
          case Mongo.Server.response(mongo) do
            {:ok, response} -> %Mongo.Cursor{cursor| response: response, exhausted: response.cursorID == 0}
            error -> error
          end
        error -> error
    end
  end

  def exec(collection, query, batchSize \\ 0) do
    mongo = collection.db.mongo
    case Mongo.Server.send(mongo, query) do
      {:ok, _reqid} ->
        case Mongo.Server.response(mongo) do
          {:ok, initialResponse} ->
            %Mongo.Cursor{ collection: collection,
                           response:   initialResponse,
                           exhausted:  initialResponse.cursorID == 0,
                           batchSize:  batchSize}
          %Mongo.Error{}=error -> error
          # {:error, msg} -> %Mongo.Error{msg: msg}
        end
      error -> error
    end
  end

  defimpl Enumerable, for: Mongo.Cursor do

    @doc """
    Reduce documents in the buffer into a value
    """
    def reduce(cursor, acc, reducer)
    def reduce(cursor, {:cont, acc}, reducer) do
      case reducer.(cursor.response, acc) do
        {:cont, acc} ->
          if cursor.exhausted do
            {:done, acc}
          else
            case Mongo.Cursor.next_batch(cursor) do
              %Mongo.Cursor{exhausted: true} -> {:done, acc}
              %Mongo.Cursor{}=cursor -> reduce(cursor, {:cont, acc}, reducer)
              error -> {:halted, %Mongo.Error{error| acc: [cursor | error.acc]}}
            end
          end
        reduced -> reduce(cursor, reduced, reducer)
      end
    end
    def reduce(_, {:halt, acc}, _reducer),   do: {:halted, acc}
    def reduce(cursor, {:suspend, acc}, reducer), do: {:suspended, acc, &reduce(cursor, &1, reducer)}


    @doc false
    #Not implemented use `Mongo.Collection.count/1`
    def count(_cursor), do: {:ok, -1}

    @doc false
    #Not implemented
    def member?(_, _cursor), do: {:ok, false}
  end

end
