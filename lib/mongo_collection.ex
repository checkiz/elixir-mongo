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

  defrecordp :coll, __MODULE__ ,
    collname: nil,
    db: nil

  @def_reduce "function(k, vs){return Array.sum(vs)}"
  
  @doc """
  New collection
  """
  def new(db, collname), do: coll(db: db, collname: collname)

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
  def find(criteria \\ [], projection \\ [], collection) do
    Mongo.Find.new(collection, criteria, projection)
  end

  @doc """
  Insert one document or an array of documents into the collection

  
      db = mongo.connect.db("test")
      [[a: 23], [a: 24, b: 1]] |> 
      db.collection("anycoll").insert

  If you need object ID to be added to your documents, you can run

      db = mongo.connect.db("test")
      [[a: 23], [a: 24, b: 1]] |> Mongo.assign_id
      db.collection("anycoll").insert

  `Mongo.Collection.insert` returns the list of documents it received.
  """
  def insert([{a,_}|_]=doc, collection) when is_atom(a), do: insert([doc], collection) |> hd
  def insert(docs, collection) do
    Mongo.Request.insert(collection, docs).send
    docs
  end

  @doc """
  Modifies an existing document or documents in the collection

      db = mongo.connect.db("test")
      db.collection("anycoll").update([a: 456], [a: 123, b: 789])
  """
  def update(query, update, upsert \\ false, multi \\ false, collection) do
    Mongo.Request.update(collection, query, update, upsert, multi).send
  end

  @doc """
  Removes an existing document or documents in the collection (see db.collection.remove)

      db = mongo.connect.db("test")
      db.collection("anycoll").remove([b: 789])
  """
  def delete(query, justOne \\ false, collection) do
    Mongo.Request.delete(collection, query, justOne).send
  end

  @doc """
  Count documents in the collection (see db.collection.count)

  Returns `{:ok, n}`, the result of count, or `{:error, reason}`

      db = mongo.connect.db("test")
      {:ok, n} = db.collection.count(value: ['$gt': 0])
  """
  def count(query \\ {}, opts \\ [], coll(collname: collname, db: db)) do
    Mongo.Request.cmd(db, Keyword.merge(opts, count: collname, query: query)).send
    case db.mongo.response do
      {:ok, resp} -> resp.count
      error -> error
    end
  end
  @doc """
  Count documents in the collection (see db.collection.count)
  
  Returns `n`, the result of count, or raise an error

      db = mongo.connect.db("test")
      n = db.collection.count!(value: ['$gt': 0])
  """
  defbang count(collection)
  defbang count(query, collection)
  defbang count(query, opts, collection)

  @doc """
  Finds the distinct values for a specified field across a single collection (see db.collection.distinct)

      db = mongo.connect.db("test")
      db.collection.distinct("value", value: ["$gt": 3])
  """
  def distinct(key, query \\ {}, coll(collname: collname, db: db)) do
    Mongo.Request.cmd(db, distinct: collname, key: key, query: query).send
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
  def mr(map, reduce \\ @def_reduce, out \\ [inline: true], opts \\ [], coll(collname: collname, db: db)) do
    Mongo.Request.cmd(db, Keyword.merge(opts, mapReduce: collname, map: map, reduce: reduce, out: out)).send
    case db.mongo.response do
      {:ok, resp} -> resp.mr
      error -> error
    end
  end
  defbang mr(map, collection)
  defbang mr(map, reduce, collection)
  defbang mr(map, reduce, out, collection)
  defbang mr(map, reduce, out, opts, collection)

  @doc """
  Groups documents in the collection by the specified key (see db.collection.group)

      db = mongo.connect.db("test")
      db.collection.group(a: true)
  """
  def group(key, reduce \\ @def_reduce, initial \\ {}, opts \\ [], coll(collname: collname, db: db)) do
    Mongo.Request.cmd(db, group: Keyword.merge(opts, ns: collname, key: key, '$reduce': reduce, initial: initial)).send
    case db.mongo.response do
      {:ok, resp} -> resp.group
      error -> error
    end
  end
  defbang group(key, collection)
  defbang group(key, reduce, collection)
  defbang group(key, reduce, initial, collection)
  defbang group(key, reduce, initial, opts, collection)

  @doc """
  Drops the collection 

  returns `:ok` or a string containing the error message
  """
  def drop(coll(collname: collname, db: db)) do
    Mongo.Request.cmd(db, drop: collname).send
    case db.mongo.response do
      {:ok, resp} -> resp.success
      error -> error
    end
  end
  defbang drop(collection)

  @doc """
  Calculates aggregate values for the data in the collection (see db.collection.aggregate)

      db = mongo.connect.db("test")
      db.collection.aggregate(skip: 1, limit: 5, project: ['_id': false, value: true])
  """
  def aggregate(pipeline, coll(collname: collname, db: db)) do
    Mongo.Request.cmd(db, aggregate: collname, pipeline: (lc line inlist pipeline, do: pipe(line)) ).send
    case db.mongo.response do
      {:ok, resp} -> resp.aggregate
      error -> error
    end
  end
  defbang aggregate(pipeline, collection)

  # Reshapes a document stream. $project can rename, add, or remove fields as well as create computed values and sub-documents
  defp pipe({:project, kw}), do: ['$project': kw]
  # Filters the document stream, and only allows matching documents to pass into the next pipeline stage. $match uses standard MongoDB queries
  defp pipe({:match, kw})  , do: ['$match': kw]
  # Restricts the number of documents in an aggregation pipeline
  defp pipe({:limit, kw})  , do: ['$limit': kw]
  # Skips over a specified number of documents from the pipeline and returns the rest
  defp pipe({:skip, n})    , do: ['$skip': n]
  # Takes an array of documents and returns them as a stream of documents
  defp pipe({:unwind, kw}) , do: ['$unwind': kw]
  # Groups documents together for the purpose of calculating aggregate values based on the collection of documents
  defp pipe({:group, kw})  , do: ['$group': kw]
  # Takes all input documents and returns them in a stream of sorted documents
  defp pipe({:sort, kw})   , do: ['$sort': kw]
  # Returns an ordered stream of documents based on proximity to a geospatial point
  defp pipe({:geoNear, kw}), do: ['$geoNear': kw]

  @doc """
  Returns the db of the collection
  """
  def db(coll(db: db)), do: db
  
end