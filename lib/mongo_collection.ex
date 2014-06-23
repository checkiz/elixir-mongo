defmodule Mongo.Collection do
  @moduledoc """
  Module holding operations that can be performed on a collection (find, count...)
  
  Example:


      db = Mongo.connect.db("test")
      anycoll = db.collection("anycoll")
      anycoll.count()
  
  `count()` or `count!()`

  The first returns `{:ok, value}`, the second returns simply `value`.
  In case of error, the first returns `{:error, reason}` the second raises an exception.

      iex> anycoll.count()
      {:ok, 6.0}
      iex> anycoll.count!()
      6.0
      iex> anycoll.count(a: ['$in': [1,3]])
      {:ok, 2.0}
      iex> anycoll.count(a: ['$in': 1]) # $in should take a list
      {:error, [errmsg: "exception: invalid query", code: 12580, ok: 0.0]}
      iex> anycoll.count!(a: ['$in': 1])
      ** (Mongo.Error) [errmsg: "exception: invalid query", code: 12580, ok: 0.0]

  """
  use Mongo.Helpers

  require Record
  Record.defrecordp :coll, __MODULE__ ,
    collname: nil,
    db: nil,
    opts: %{}

  @def_reduce "function(k, vs){return Array.sum(vs)}"
  
  @doc """
  New collection
  """
  def new(db, collname), do: coll(db: db, collname: collname, opts: db.coll_opts)

  @doc """
  Gets the collection name

      coll = mongo.connect.db("test").collection("anycoll")
      "anycoll" = coll.name
  """
  def name(coll(collname: collname)), do: collname

  @doc """
  Runs db.find() for a given query and returns a `Mongo.Cursor`

      mongo.connect.db("test").collection("anycoll").find().toArray()

  For a limited number of documents, `Mongo.Cursor.toArray` returns the complete list of documents.
  See `Mongo.Cursor` for more detail on other options to retreive documents.

  """
  def find(criteria \\ %{}, projection \\ %{}, collection) do
    Mongo.Find.new(collection, criteria, projection)
  end

  @doc """
  Insert one document into the collection

  
      db = mongo.connect.db("test")
      %{a: 23} |> 
      db.collection("anycoll").insert_one

  `Mongo.Collection.insert_one` returns the document it received.
  """
  def insert_one(doc, collection) when is_map(doc) do
    case insert([doc], collection) do
      {:ok, docs} -> {:ok, docs |> hd}
      error -> error
    end
  end
  defbang insert_one(docs, collection)

  @doc """
  Insert a list of documents into the collection

  
      db = mongo.connect.db("test")
      [%{a: 23}, %{a: 24, b: 1}] |> 
      db.collection("anycoll").insert

  If you need object ID to be added to your documents, you can run

      db = mongo.connect.db("test")
      [%{a: 23}, %{a: 24, b: 1}] |> Mongo.assign_id
      db.collection("anycoll").insert

  `Mongo.Collection.insert` returns the list of documents it received.
  """
  def insert(docs, coll(opts: opts, db: db)=collection) do
    db.mongo |> Mongo.Request.insert(collection, docs).send
    case opts[:wc] do
      nil -> {:ok, docs}
      :safe -> case db.getLastError do
        :ok -> {:ok, docs}
        error -> error
      end
    end
  end
  defbang insert(docs, collection)

  @doc """
  Modifies an existing document or documents in the collection

      db = mongo.connect.db("test")
      db.collection("anycoll").update(%{a: 456}, %{a: 123, b: 789})
  """
  def update(query, update, upsert \\ false, multi \\ false, coll(opts: opts, db: db)=collection) do
    db.mongo |> Mongo.Request.update(collection, query, update, upsert, multi).send
    case opts[:wc] do
      nil -> :ok
      :safe -> db.getLastError
    end
  end

  @doc """
  Removes an existing document or documents in the collection (see db.collection.remove)

      db = mongo.connect.db("test")
      db.collection("anycoll").remove(%{b: 789})
  """
  def delete(query, justOne \\ false, coll(opts: opts, db: db)=collection) do
    db.mongo |> Mongo.Request.delete(collection, query, justOne).send
    case opts[:wc] do
      nil -> :ok
      :safe -> db.getLastError
    end
  end

  @doc """
  Count documents in the collection (see db.collection.count)

  Returns `{:ok, n}`, the result of count, or `{:error, reason}`

      db = mongo.connect.db("test")
      {:ok, n} = db.collection.count(%{value: %{'$gt': 0}})
  """
  def count(query \\ %{}, skip_limit \\ %{}, coll(collname: collname, db: db)) do
    skip_limit = Map.take(skip_limit, [:skip, :limit])
    db.mongo |> Mongo.Request.cmd(db, %{count: collname}, Map.merge(skip_limit, %{query: query})).send
    case db.mongo.response do
      {:ok, resp} -> resp.count
      error -> error
    end
  end
  defbang count(collection)
  defbang count(query, collection)
  defbang count(query, skip_limit, collection)

  @doc """
  Finds the distinct values for a specified field across a single collection (see db.collection.distinct)

      db = mongo.connect.db("test")
      db.collection.distinct("value", %{value: %{"$gt": 3}})
  """
  def distinct(key, query \\ %{}, coll(collname: collname, db: db)) do
    db.mongo |> Mongo.Request.cmd(db, %{distinct: collname}, %{key: key, query: query}).send
    case db.mongo.response do
      {:ok, resp} -> resp.distinct
      error -> error
    end
  end
  defbang distinct(key, collection)
  defbang distinct(key, query, collection)
  
  @doc """
  Provides a wrapper around the mapReduce command (db.collection.mapReduce)

  Returns  `:ok` or an array of documents (inline). `:out` option is set to inline by default.

      db = mongo.connect.db("test")
      db.collection.mr("function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}")
  """
  def mr(map, reduce \\ @def_reduce, out \\ %{inline: true}, params \\ %{}, coll(collname: collname, db: db)) do
    params = Map.take(params, [:limit, :finalize, :scope, :jsMode, :verbose])
    db.mongo |> Mongo.Request.cmd(db, %{mapReduce: collname}, Map.merge(params, %{map: map, reduce: reduce, out: out})).send
    case db.mongo.response do
      {:ok, resp} -> resp.mr
      error -> error
    end
  end
  defbang mr(map, collection)
  defbang mr(map, reduce, collection)
  defbang mr(map, reduce, out, collection)
  defbang mr(map, reduce, out, more, collection)

  @doc """
  Groups documents in the collection by the specified key (see db.collection.group)

      db = mongo.connect.db("test")
      db.collection.group(%{a: true})
  """
  def group(key, reduce \\ @def_reduce, initial \\ %{}, params \\ %{}, coll(collname: collname, db: db)) do
    params = Map.take(params, [:'$keyf', :cond, :finalize])
    if params[:keyf], do: params = Map.put_new(:'$keyf', params[:keyf])
    db.mongo |> Mongo.Request.cmd(db, %{group: Map.merge(params, %{ns: collname, key: key, '$reduce': reduce, initial: initial})}).send
    case db.mongo.response do
      {:ok, resp} -> resp.group
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
  def drop(coll(collname: collname, db: db)) do
    db.mongo |> Mongo.Request.cmd(db, %{drop: collname}).send
    #db.mongo.response |> IO.inspect
    case db.mongo.response do
      {:ok, resp} -> resp.success
      error -> error
    end
  end
  defbang drop(collection)

  @doc """
  Calculates aggregate values for the data in the collection (see db.collection.aggregate)

      db = mongo.connect.db("test")
      db.collection.aggregate([
        %{'$skip': 1}, 
        %{'$limit': 5}, 
        %{'$project': %{'_id': false, value: true}}
      ])
  """
  def aggregate(pipeline, coll(collname: collname, db: db)) do
    db.mongo |> Mongo.Request.cmd(db, %{aggregate: collname}, %{pipeline: pipeline} ).send
    case db.mongo.response do
      {:ok, resp} -> resp.aggregate
      error -> error
    end
  end
  defbang aggregate(pipeline, collection)

  @doc """
  Adds options to the collection overwriting database options

  new_opts must be a keyword with zero or more pairs represeting one of these options:
  
  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write: concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(new_opts, coll(opts: opts)=c) do
    coll(c, opts: Map.merge(opts, new_opts))
  end

  @doc """
  Gets read default options
  """
  def read_opts(coll(opts: opts)) do
    Map.take(opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :mode, :timeout])
  end

  @doc """
  Gets write default options
  """
  def write_opts(coll(opts: opts)) do
    Map.take(opts, [:wc, :mode, :timeout])
  end

  @doc """
  Returns the db of the collection
  """
  def db(coll(db: db)), do: db

  @doc """
  Creates an index for the collection
  """
  def createIndex(name, key, unique \\ false, coll(collname: collname, db: db)) do
    db.collection("system.indexes").insert_one(%{name: name, ns: db.name <> "." <> collname, key: key, unique: unique})
  end
  
end