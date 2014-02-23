defmodule Mongo.Find do
  @moduledoc """
  Find operation on MongoDB
  """
  defrecordp :find, __MODULE__ ,
    mongo: nil,
    collection: nil,
    selector: nil,
    projector: nil,
    batchSize: 0,
    skip: 0,
    opts: []

  @doc """
  Creates a new find operation.

  Not to be used directly, prefer `Mongo.Collection.find/3` that returns a `Mongo.Cursor`
  """
  def new(collection, selector, projector) do
    find(collection: collection, selector: selector, projector: projector)
  end

  @doc """
  Sets where MongoDB begins returning results

  Must be run before executing the query

      iex> Mongo.connect.db("test").collection("anycoll").find.skip(1).toArray |> Enum.count
      5
      iex> Mongo.connect.db("test").collection("anycoll").find.skip(2).toArray |> Enum.count
      4

  """
  def skip(n, f), do: find(f, skip: n)

  @doc """
  Specifies the number of documents to return in each batch

  Must be run before executing the query
  """
  def batchSize(n, f), do: find(f, batchSize: n)

  @doc """
  Stream documents retreived from a find query

  The following 2 statements are equivalent:

      Mongo.connect.db("test").collection("anycoll").find.stream |> Enum.to_list
      Mongo.connect.db("test").collection("anycoll").find.toArray

  see `Mongo.Cursor.stream/2`
  """
  defdelegate stream(find), to: Mongo.Cursor

  @doc false
  defdelegate batchStream(find), to: Mongo.Cursor

  @doc """
  Returns a list of documents retreived from a find query

  see `Mongo.Cursor.toArray/2`
  """
  defdelegate toArray(find), to: Mongo.Cursor

  @doc false
  defdelegate batchArray(find), to: Mongo.Cursor

  # Builds a query request
  defp query(find(collection: collection, selector: selector, projector: projector, skip: skip, batchSize: batchSize, opts: opts)) do
    Mongo.Request.query(collection, selector, projector, skip, batchSize, opts)
  end

  @doc """
  Executes the query and returns a `Mongo.Cursor`
  """
  def exec(find(collection: collection, batchSize: batchSize)=f) do
    query(f).send
    mongo = collection.db.mongo
    unless mongo.active? do 
      case mongo.response do
        {:ok, initialResponse} ->
          {:ok, initialResponse, Mongo.Cursor.new(collection, initialResponse, batchSize)}
        error -> error
      end
    end
  end

  @doc """
  Sets query options

  Defaults option set is equivalent of calling:

      Find.opts(
        awaitdata: false
        nocursortimeout: false
        slaveok: true
        tailablecursor: false)
  """
  def opts(options, f), do: find(f, opts: options)

end