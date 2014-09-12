defmodule Mongo.Cursor do
  require Record
  @moduledoc """
  Manages MongoDB [cursors](http://docs.mongodb.org/manual/core/cursors/).

  Cursors are returned by a find see `Mongo.Collection.find/3`.
  """
  use Mongo.Helpers

  Record.defrecordp :cursor, __MODULE__ ,
    collection: nil,
    batchSize: 0,
    response: nil,
    cursorID: nil,
    cursorExhausted: nil

  @doc """
  Creates a cursor record
  """
  def new(collection, initialResponse, batchSize) do
    cursor(
      collection: collection,
      response: initialResponse,
      cursorID: initialResponse.cursorID,
      cursorExhausted: initialResponse.exhausted?,
      batchSize: batchSize)
  end

  @doc false
  def batchStream(cursor()=c) do
    Stream.resource(
      fn() -> c end,
      &next_batch!/1,
      &kill/1)
  end
  def batchStream(find) do
    Stream.resource(
      fn() ->
        case find.exec do
          {:ok, _, c} -> c
          {:error, reason} -> raise Mongo.Error, reason: reason
        end
      end,
      fn(c) ->
        case next_batch!(c) do
          nil -> {:halt, c}
          {resp, c} -> {[resp], c}
        end
      end,
      &kill/1)
  end

  @doc """
  Returns response for the next batch of document of a given cursor
  """
  def next_batch(cursor(cursorExhausted: true)), do: nil
  def next_batch(cursor(collection: collection, batchSize: batchSize, cursorID: cursorID)=c) do
    (mongo = collection.db.mongo)
      |> Mongo.Request.get_more(collection, batchSize, cursorID).send
    case mongo.response do
      {:ok, resp} -> {:ok, {resp, cursor(c, response: resp, cursorExhausted: resp.exhausted?)}}
      error -> error
    end
  end
  defbang next_batch(c)

  @doc """
  Creates a stream of documents for the given the cursor

  `stream`, exectutes the query if not done yet, then, streams documents one by one (retreived from DB by batch).
  When exhausted, it kills the cursor.
  """
  def stream(cursor(response: r)=c) do
    Stream.resource(
      fn() ->
        {r, c}
      end,
      &next/1,
      fn({_, c}) -> c.kill end)
  end
  def stream(find) do
    Stream.resource(
      fn() ->
        case find.exec do
          {:ok, r, c} -> {r, c}
          {:error, reason} -> raise Mongo.Error, reason: reason
        end
      end,
      &next/1,
      fn({_, c}) -> c.kill end)
  end

  @doc """
  Kills the cursor
  """
  def kill(cursor(cursorID: 0)), do: :ok
  def kill(cursor(cursorID: nil)), do: :ok
  def kill(cursor(collection: collection, cursorID: cursorID)) do
    collection.db.kill_cursor(cursorID)
  end

  # @doc """
  # Gets next doc, and, when nedded, requests next document batch.
  # Returns `nil` after last document when cursor is exhausted otherwise returns `{:ok, next_doc, cursor}`
  # """
  defp next({r, c}) do
    case next?({r, c}) do
      false -> {:halt, {r, c}}
      {r, c} ->
        {d, r} = r.next
        {[d], {r, c}}
    end
  end
  # @doc """
  # Tests if there is one or more documents to be retreived

  # Return `false` when the cursor is exhausted otherwise returns the cursor,
  # or a new cusrsor with next document batch when needed.
  # """
  defp next?({r, c}) do
    if r.next? do
      {r, c}
    else
      case c.next_batch! do
        nil -> false
        {r, c} -> if r.next?, do: {r, c}, else: false
      end
    end
  end

  @doc """
  Creates a list of document retreive by a find query

  `toArray`, exectutes the query then accumulate in a list all documents one by one.
  It calls `Mongo.Cursor/1` then pipes the resulting stream to `Enum.to_list/1`
  """
  def toArray(find), do: find.stream |> Enum.to_list

  @doc false
  def batchArray(find), do: find.batchStream |> Enum.to_list

  @doc """
  Specifies the number of documents to return in next batch(es)
  """
  def batchSize(n, c), do: cursor(c, batchSize: n)

end
