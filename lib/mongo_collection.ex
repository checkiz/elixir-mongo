defmodule Mongo.Collection do
  @moduledoc """
  Module holding operations that can be performed on a collection (find, count...)

  Usage:

      iex> Mongo.Helpers.test_collection("anycoll") |> Mongo.Collection.count
      {:ok, 6}

  `count()` or `count!()`

  The first returns `{:ok, value}`, the second returns simply `value` when the call is sucessful.
  In case of error, the first returns `%Mongo.Error{}` the second raises a `Mongo.Bang` exception.

      iex> collection = Mongo.Helpers.test_collection("anycoll")
      ...> {:ok, 6} = collection |> Mongo.Collection.count
      ...> 6 === collection |> Mongo.Collection.count!
      true

      iex> collection = Mongo.Helpers.test_collection("anycoll")
      ...> {:ok, 2} = collection |> Mongo.Collection.count(a: ['$in': [1,3]])
      ...> %Mongo.Error{} = collection |> Mongo.Collection.count(a: ['$in': 1]) # $in should take a list, so this triggers an error
      ...> collection |> Mongo.Collection.count!(a: ['$in': 1])
      ** (Mongo.Bang) :"cmd error"

  """
  use Mongo.Helpers
  alias Mongo.Server
  alias Mongo.Db
  alias Mongo.Request

  defstruct [
    name: nil,
    db: nil,
    opts: %{} ]

  @def_reduce "function(k, vs){return Array.sum(vs)}"

  @doc """
  New collection
  """
  def new(db, name), do: %__MODULE__{db: db, name: name, opts: Db.coll_opts(db)}

  @doc """
  Creates a `%Mongo.Find{}` for a given collection, query and projection

  See `Mongo.Find` for details.

  """
  def find(collection, criteria \\ %{}, projection \\ %{}) do
    Mongo.Find.new(collection, criteria, projection)
  end

  @doc """
  Insert one document into the collection returns the document it received.

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> %{a: 23} |> Mongo.Collection.insert_one(collection) |> elem(1)
      %{a: 23}

  """
  def insert_one(doc, collection) when is_map(doc) do
    case insert([doc], collection) do
      {:ok, docs} -> {:ok, docs |> hd}
      error -> error
    end
  end
  defbang insert_one(doc, collection)

  @doc """
  Insert a list of documents into the collection

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> [%{a: 23}, %{a: 24, b: 1}] |> Mongo.Collection.insert(collection) |> elem(1)
      [%{a: 23}, %{a: 24, b: 1}]

  You can chain it with `Mongo.assign_id/1` when you need ids for further processing. If you don't Mongodb will assign ids automatically.

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> [%{a: 23}, %{a: 24, b: 1}] |> Mongo.assign_id |> Mongo.Collection.insert(collection) |> elem(1) |> Enum.at(0) |> Map.has_key?(:"_id")
      true

  `Mongo.Collection.insert` returns the list of documents it received.
  """
  def insert(docs, collection) do
    Server.send(
      collection.db.mongo,
      Request.insert(collection, docs))
    case collection.opts[:wc] do
      nil -> {:ok, docs}
      :safe -> case collection.db.getLastError do
        :ok -> {:ok, docs}
        error -> error
      end
    end
  end
  defbang insert(docs, collection)

  @doc """
  Modifies an existing document or documents in the collection

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> collection |> Mongo.Collection.update(%{a: 456}, %{a: 123, b: 789})
      :ok

  """
  def update(collection, query, update, upsert \\ false, multi \\ false)
  def update(collection, query, update, upsert, multi) do
    Server.send(
      collection.db.mongo,
      Request.update(collection, query, update, upsert, multi))
    case collection.opts[:wc] do
      nil -> :ok
      :safe -> collection.db.getLastError
    end
  end

  @doc """
  Removes an existing document or documents in the collection (see db.collection.remove)

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> collection |> Mongo.Collection.delete(%{b: 789})
      :ok

  """
  def delete(collection, query, justOne \\ false)
  def delete(collection, query, justOne) do
    Server.send(
      collection.db.mongo,
     Request.delete(collection, query, justOne))
    case collection.opts[:wc] do
      nil -> :ok
      :safe -> collection.db.getLastError
    end
  end

  @doc """
  Count documents in the collection

  If `query` is not specify, it counts all document collection.
  `skip_limit` is a map that specify Mongodb otions skip and limit

  """
  def count(collection, query \\ %{}, skip_limit \\ %{})
  def count(collection, query, skip_limit) do
    skip_limit = Map.take(skip_limit, [:skip, :limit])
    case Mongo.Db.cmd_sync(collection.db, %{count: collection.name},
        Map.merge(skip_limit, %{query: query})) do
      {:ok, resp} ->
        case resp |> Mongo.Response.count do
          {:ok, n} -> {:ok, n |> trunc}
          # _error -> {:ok, -1}
          error -> error
        end
      error -> error
    end
  end
  defbang count(collection)
  defbang count(collection, query)
  defbang count(collection, query, skip_limit)

  @doc """
  Finds the distinct values for a specified field across a single collection (see db.collection.distinct)


      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> collection |> Mongo.Collection.distinct!("value", %{value: %{"$lt": 3}})
      [0, 1]

  """
  def distinct(collection, key, query \\ %{})
  def distinct(collection, key, query) do
    case Mongo.Db.cmd_sync(collection.db, %{distinct: collection.name}, %{key: key, query: query}) do
      {:ok, resp} -> Mongo.Response.distinct(resp)
      error -> error
    end
  end
  defbang distinct(key, collection)
  defbang distinct(key, query, collection)

  @doc """
  Provides a wrapper around the mapReduce command

  Returns  `:ok` or an array of documents (with option `:inline` active - set by default).

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> Mongo.Collection.mr!(collection, "function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}") |> is_list
      true

      %{_id: Bson.ObjectId.from_string("542aa3fab9742bc0d5eaa12d"), value: 0.0}

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> Mongo.Collection.mr!(collection, "function(d){emit('z', 3*this.value)}", "function(k, vs){return Array.sum(vs)}", "mrcoll")
      :ok

  """
  def mr(collection, map, reduce \\ @def_reduce, out \\ %{inline: true}, params \\ %{})
  def mr(collection, map, reduce, out, params) do
    params = Map.take(params, [:limit, :finalize, :scope, :jsMode, :verbose])
    case Mongo.Db.cmd_sync(collection.db, %{mapReduce: collection.name}, Map.merge(params, %{map: map, reduce: reduce, out: out})) do
      {:ok, resp} -> Mongo.Response.mr resp
      error -> error
    end
  end
  defbang mr(map, collection)
  defbang mr(map, reduce, collection)
  defbang mr(map, reduce, out, collection)
  defbang mr(map, reduce, out, more, collection)

  @doc """
  Groups documents in the collection by the specified key

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> collection |> Mongo.Collection.group!(%{a: true}) |> is_list
      true

      [%{a: 0.0}, %{a: 1.0}, %{a: 2.0}, ...]

  """
  def group(collection, key, reduce \\ @def_reduce, initial \\ %{}, params \\ %{})
  def group(collection, key, reduce, initial, params) do
    params = Map.take(params, [:'$keyf', :cond, :finalize])
    if params[:keyf], do: params = Map.put_new(:'$keyf', params[:keyf])
    case Mongo.Db.cmd_sync(collection.db, %{group: Map.merge(params, %{ns: collection.name, key: key, '$reduce': reduce, initial: initial})}) do
      {:ok, resp} -> Mongo.Response.group resp
      error -> error
    end
  end
  defbang group(key, collection)
  defbang group(key, reduce, collection)
  defbang group(key, reduce, initial, collection)
  defbang group(key, reduce, initial, params, collection)

  @doc """
  Drops the collection

  returns `:ok` or a string containing the error message
  """
  def drop(collection) do
    case Db.cmd_sync(collection.db, %{drop: collection.name}) do
      {:ok, resp} -> Mongo.Response.success resp
      error -> error
    end
  end
  defbang drop(collection)

  @doc """
  Calculates aggregate values for the data in the collection (see db.collection.aggregate)

      iex> collection = Mongo.connect! |> Mongo.db("test") |> Mongo.Db.collection("anycoll")
      ...> collection |> Mongo.Collection.aggregate([
      ...>    %{'$skip': 1},
      ...>    %{'$limit': 5},
      ...>    %{'$project': %{'_id': false, value: true}} ])
      [%{value: 1}, %{value: 1}, %{value: 1}, %{value: 1}, %{value: 3}]

  """
  def aggregate(collection, pipeline) do
    case Mongo.Db.cmd_sync(collection.db, %{aggregate: collection.name}, %{pipeline: pipeline} ) do
      {:ok, resp} -> Mongo.Response.aggregate resp
      error -> error
    end
  end
  defbang aggregate(pipeline, collection)

  @doc """
  Adds options to the collection overwriting database options

  new_opts must be a map with zero or more pairs represeting one of these options:

  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(collection, new_opts) do
    %__MODULE__{collection| opts: Map.merge(collection.opts, new_opts)}
  end

  @doc """
  Gets read default options
  """
  def read_opts(collection) do
    Map.take(collection.opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :mode, :timeout])
  end

  @doc """
  Gets write default options
  """
  def write_opts(collection) do
    Map.take(collection.opts, [:wc, :mode, :timeout])
  end

  @doc """
  Creates an index for the collection
  """
  def createIndex(collection, name, key, unique \\ false) do
    system_indexes = new(collection.db, "system.indexes")
    %{name: name, ns: collection.db.name <> "." <> collection.name, key: key, unique: unique} |> insert_one(system_indexes)
  end

end
