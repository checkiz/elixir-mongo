defmodule Mongo.Find do
  @moduledoc """
  Find operation on MongoDB
  """
  use Mongo.Helpers
  defrecordp :find, __MODULE__ ,
    mongo: nil,
    collection: nil,
    selector: %{},
    projector: %{},
    batchSize: 0,
    skip: 0,
    opts: %{},
    mods: %{}

  @doc """
  Creates a new find operation.

  Not to be used directly, prefer `Mongo.Collection.find/3` that returns a `Mongo.Cursor`
  """
  def new(collection, jsString, projector) when is_binary(jsString), do: new(collection, %{'$where': jsString}, projector)
  def new(collection, selector, projector) do
    find(collection: collection, selector: selector, projector: projector, opts: collection.read_opts)
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

  @doc """
  Executes the query and returns a `Mongo.Cursor`
  """
  def exec(find(collection: collection, batchSize: batchSize)=f) do
    mongo = collection.db.mongo
    mongo |> Mongo.Request.query(f).send
    case mongo.response do
      {:ok, initialResponse} ->
        {:ok, initialResponse, Mongo.Cursor.new(collection, initialResponse, batchSize)}
      error -> error
    end
  end

  @doc """
  Runs the explain operator that provides information on the query plan
  """
  def explain(find(collection: collection)=f) do
    mongo = collection.db.mongo
    mongo |> Mongo.Request.query(f.addSpecial(:'$explain', 1)).send
    case mongo.response do
      {:ok, response} -> response.cmd
      error -> error
    end
  end
  defbang explain(find)

  @doc """
  Add hint opperator that forces the query optimizer to use a specific index to fulfill the query 
  """
  def hint(indexName, f) when is_atom(indexName), do: f.addSpecial(:'$hint', indexName)
  def hint(hints, f) when is_map(hints), do: f.addSpecial(:'$hint', hints)

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

  def addSpecial(k, v, find(mods: mods)=f) do
    find(f, mods: Map.put(mods, k, v))
  end

  @query       <<0xd4, 0x07, 0, 0>> # 2004  query a collection
  @query_opts  <<0b00000100::8>>    # default query options, equvalent to `cursor.set_opts(slaveok: true)`

  @doc """
    Builds a query message

    * collection: collection
    * selector: selection criteria (Map or nil)
    * projector: fields (Map or nil)
  """
  def query(find(collection: collection, selector: selector, projector: projector, skip: skip, batchSize: batchSize, opts: opts, mods: mods)) do
    selector = if mods == %{}, do: selector, else: Map.put(mods, :'$query', selector)
    @query <> (Enum.reduce(opts, @query_opts, &queryopt_red/2)) <> <<0::24>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>> <>
      Bson.int32(skip) <>
      Bson.int32(batchSize) <>
      Bson.encode(selector) <>
      Bson.encode(projector)
  end
  
  use Bitwise
  # Operates one option
  defp queryopt_red({opt, true},  bits), do: bits ||| queryopt(opt)
  defp queryopt_red({opt, false}, bits), do: bits &&& ~~~queryopt(opt)
  defp queryopt_red(_, bits),            do: bits
  # Identifies the bit that is switched by an option when it is set to `true`
  defp queryopt(:awaitdata),       do: 0b00100000
  defp queryopt(:nocursortimeout), do: 0b00010000
  defp queryopt(:slaveok),         do: 0b00000100
  defp queryopt(:tailablecursor),  do: 0b00000010
  defp queryopt(_),                do: 0b00000000

end