defmodule Mongo do

  defrecord DB,
    socket: nil,
    db: nil

  defrecord Opts,
    tailablecursor: false, 
    slaveok: true, 
    nocursortimeout: false, 
    awaitdata: false

  defrecord Reply,
    id: nil,
    cursornotfound: nil,
    queryerror: nil,
    awaitcapable: nil,
    cursorid: nil,
    startingfrom: nil,
    nbdocs: 0,
    bsonbuffer: nil

  @doc """
  connects to a mongodb server by defaults to {"127.0.0.1", 27017}
  """
  def mongo(host // "127.0.0.1", port // 27017), do: Socket.TCP.connect!(host, port)

  @doc """
  connects to a database on a given mongodb server
  """
  def connect(socket, db), do: DB.new(socket: socket, db: db)

  @doc """
  Runs db.find() for a given query and returns a stream of document in the form of Keyword

  To retreive documents and decode them, you can do:

  ```elixir
  mongo
    |> connect("test")
    |> Mongo.find("anycoll", ['$maxScan': 2, '$skip': 0])
  ```
  """
  def find(db, collection, criteria // [], projection // [], opts // Opts[]) do
    find_bsondocs(db, collection, criteria, projection, opts)
      |> Stream.map(fn {bsonbuffer, part} -> Bson.decode(part, bsonbuffer) end) 
  end

  # executes the find command returning a list of bson docs
  defp find_bsondocs(db, collection, criteria, projection, opts) do
    {skip, criteria} = Keyword.pop_first criteria, :'$skip', 0
    {batchsize, criteria} = Keyword.pop_first criteria, :'$limit', 0
    {batchsize, criteria} = Keyword.pop_first criteria, :'$maxScan', batchsize
    criteria = if criteria == [], do: {}, else: criteria
    case query_command(db, collection, criteria, projection, opts, skip, batchsize)
      |> exec(db.socket) do
      Reply[nbdocs: nbdocs, bsonbuffer: bsonbuffer, cursorid: 0] ->
        Stream.unfold({nbdocs, bsonbuffer, 0}, &(next_bson/1))
      Reply[nbdocs: nbdocs, bsonbuffer: bsonbuffer, cursorid: cursorid] ->
        Stream.resource(
          fn() -> {nbdocs, bsonbuffer, 0, fn() -> getmore_command(db, collection, batchsize, cursorid) |> exec(db.socket) end}
          end,
          &(next_bson/1),
          fn(_) ->
            killcursor_command(cursorid) |> exec!(db.socket)
          end)
    end
  end

  @doc """
  Insert one document or an array of documents into a collection

  ```elixir
  mongo
    |> connect("test")
    |> [[a: 23], [a: 24, b: 1]] |> Mongo.insert(mongo, "anycoll")
  ```
  """
  def insert([{a,_}|_]=doc, db, collection) when is_atom(a), do: insert([doc], db, collection) |> hd
  def insert(docs, db, collection) do
    insert_command(db, collection, docs) |> exec!(db.socket)
    docs
  end

  @doc """
  Modifies an existing document or documents in a collection (see db.collection.update)

  ```elixir
  mongo
    |> connect("test")
    |> Mongo.update("anycoll", [a: 456], [a: 123, b: 789])
  ```
  """
  def update(db, collection, query, update, upsert // false, multi // false) do
    update_command(db, collection, query, update, upsert, multi) |> exec!(db.socket)
  end

  @doc """
  Removes an existing document or documents in a collection (see db.collection.remove)

  ```elixir
  mongo
    |> connect("test")
    |> Mongo.remove("anycoll", [b: 789])
  ```
  """
  def remove(db, collection, query, justOne // false) do
    remove_command(db, collection, query, justOne) |> exec!(db.socket)
  end

  @doc """
  Returns the error status of the preceding operation.
  """
  def getlasterror(db, w // 0) do
    resp = cmd(db, getlasterror: 1, w: w)
    case resp |> Keyword.get :err do
      :nil -> :ok
      err -> err
    end
  end

  @doc """
  Count documents in a collection (see db.collection.count)

  ```elixir
  mongo
    |> connect("test")
    |> Mongo.count("anycoll", [value: ['$gt': 0]])
  ```
  """
  def count(db, collection, query // {}, opts // []) do
    resp =  cmd(db, Keyword.merge(opts, count: collection, query: query))
    case resp |> Keyword.fetch! :ok do
      ok when ok>0 -> resp |> Keyword.fetch! :n
      _ -> resp |> Keyword.fetch! :errmsg
    end
  end
  
  @doc """
  Finds the distinct values for a specified field across a single collection (see db.collection.distinct)

  ```elixir
  mongo
    |> connect("test")
    |> Mongo.distinct("anycoll", "value", [value: ["$gt": 3]])
  ```
  """
  def distinct(db, collection, key, query // {}) do
    resp =  cmd(db, distinct: collection, key: key, query: query)
    case resp |> Keyword.fetch! :ok do
      ok when ok>0 -> resp |> Keyword.fetch! :values
      _ -> resp |> Keyword.fetch! :errmsg
    end
  end
  
  @doc """
  Provides a wrapper around the mapReduce command (db.collection.mapReduce)

  Returns  `:ok` or an array of documents (inline). `:out` option is set to inline by default.
  ```elixir
  mongo
    |> connect("test")
    |> Mongo.mr("anycoll", "function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}")
  ```
  """
  def mr(db, collection, map, reduce // "function(k, vs){return Array.sum(vs)}", out // [inline: true], opts // []) do
    resp = cmd(db, Keyword.merge(opts, mapReduce: collection, map: map, reduce: reduce, out: out))
    case resp |> Keyword.fetch! :ok do
      ok when ok>0 -> case resp |> Keyword.fetch :results do
        {:ok, results} -> results
        _ -> :ok
      end
      _ -> resp |> Keyword.fetch! :errmsg
    end
  end

  @doc """
  Groups documents in a collection by the specified key (see db.collection.group)

  ```elixir
  mongo
    |> connect("test")
    |> Mongo.group("anycoll", a: true)
  ```
  """
  def group(db, collection, key, reduce // "function(k, vs){return Array.sum(vs)}", initial // {}, opts // []) do
    resp = cmd(db, group: Keyword.merge(opts, ns: collection, key: key, '$reduce': reduce, initial: initial))
    case resp |> Keyword.fetch! :ok do
      ok when ok>0 -> resp |> Keyword.fetch! :retval
      _ -> resp |> Keyword.fetch! :errmsg
    end
  end

  @doc """
  Drops a collection
  """
  def drop(db, collection), do: cmd_command(db, drop: collection) |> exec!(db.socket)

  @doc """
  Calculates aggregate values for the data in a collection (see db.collection.aggregate)

  ```elixir
  mongo
    |> connect("test")
    |> Mongo.aggregate("anycoll", skip: 1, limit: 5, project: ['_id': false, value: true])
  ```
  """
  def aggregate(db, collection, pipeline) do
    resp = cmd(db,
      aggregate: collection,
      pipeline: (lc line inlist pipeline, do: pipe(line))
      )
    case resp |> Keyword.fetch! :ok do
      ok when ok>0 -> resp |> Keyword.fetch! :result
      _ -> resp |> Keyword.fetch! :errmsg
    end
  end

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
  # Groups documents together for the purpose of calculating aggregate values based on a collection of documents
  defp pipe({:group, kw})  , do: ['$group': kw]
  # Takes all input documents and returns them in a stream of sorted documents
  defp pipe({:sort, kw})   , do: ['$sort': kw]
  # Returns an ordered stream of documents based on proximity to a geospatial point
  defp pipe({:geoNear, kw}), do: ['$geoNear': kw]

  defp cmd(db, command) do
    case cmd_command(db, command)
      |> exec(db.socket) do
        Reply[nbdocs: 1, bsonbuffer: doc] ->
          Bson.decode doc
        _ -> raise :error
    end
  end

  defp exec!(payload, socket) do
    Socket.Stream.send(socket, payload |> message)
  end
  defp exec(payload, socket) do
    Socket.Stream.send(socket, payload |> message)
    { bsonbuffer, tot_bson_len, reply } = Socket.Stream.recv!(socket, timeout: 600) |> parse_payload
    {bsonbuffer, _, _} = check_bsonbuffer(tot_bson_len, bsonbuffer, 0, fn() ->
      Socket.Stream.recv!(socket, timeout: 600)
    end)
    reply.bsonbuffer bsonbuffer
  end

  defp message(payload) do
    <<(byte_size(payload) + 12)::[size(32),little]>> <> reqid() <> <<0::32>> <> <<payload::binary>>
  end

  defp parse_payload(payload) do
    <<len::[little, signed, size(32)],
      _::[size(32),signed,little], requestId::[size(32),signed,little], 1::[size(32),signed,little],
      _::4, awaitCapable::1, _::1, queryError::1, cursorNotFound::1, _::24,
      cursorId::[size(64),signed,little],
      startingFrom::[size(32),signed,little],
      numDocs::[size(32),signed,little],
      bsonbuffer::bitstring>> = payload
    { bsonbuffer, len-36,
      Reply.new(
        id: requestId,
        cursornotfound: bool(cursorNotFound),
        queryerror: bool(queryError),
        awaitcapable: bool(awaitCapable),
        cursorid: cursorId,
        startingfrom: startingFrom,
        nbdocs: numDocs ) }
  end

  defp check_bsonbuffer(minlen, buffer, off, morefn) do
    case size(buffer) do
      buffer_size when buffer_size-off >= minlen ->
        {binary_part(buffer, off, buffer_size-off), 0, morefn}
      _ ->
        check_bsonbuffer(minlen, buffer <> morefn.(), off, morefn)
    end
  end

  defp query_command(db, collection, selector, projector, opts, skip, batchsize) do
    Bson.int32(opcode(:query)) <>
    queryopts(opts) <>
    db.db <> "." <>  collection <> <<0::8>> <>
    Bson.int32(skip) <>
    Bson.int32(batchsize) <>
    ( document(selector) ) <>
    document(projector)
  end

  defp cmd_command(db, command), do: query_command(db, "$cmd", command, {}, Opts[slaveok: true], 0, -1)

  defp getmore_command(db, collection, batchsize, cursorid) do
    Bson.int32(opcode(:getmore)) <> <<0::32>> <>
    db.db <> "." <>  collection <> <<0::8>> <>
    Bson.int32(batchsize) <>
    Bson.int64(cursorid)
  end

  defp insert_command(db, collection, docs) do
    docs |> Enum.reduce(
      Bson.int32(opcode(:insert)) <> <<0::32>> <>
      db.db <> "." <>  collection <> <<0::8>>,
      fn(doc, acc) -> acc <> Bson.encode(doc) end)
  end

  defp update_command(db, collection, selector, update, upsert, multi) do
    (Bson.int32(opcode(:update)) <> <<0::32>> <>
    db.db <> "." <>  collection <> <<0::8>> <>
    <<0::6, (bit(multi))::1, (bit(upsert))::1, 0::24>> <>
    (document(selector) ) <>
    (document(update)) )
  end

  defp remove_command(db, collection, selector, justOne) do
    Bson.int32(opcode(:delete)) <> <<0::32>> <>
    db.db <> "." <>  collection <> <<0::8>> <>
    <<0::7, (bit(justOne))::1, 0::24>> <>
    document(selector)
  end

  defp killcursor_command(cursorid) do
    Bson.int32(opcode(:killcursor)) <> <<0::32>> <>
    Bson.int32(1) <>
    Bson.int64(cursorid)
  end

  defp queryopts(opts) do
    <<0::2, (bit(opts.awaitdata))::1, (bit(opts.nocursortimeout))::1,  0::1,
            (bit(opts.slaveok))::1,   (bit(opts.tailablecursor))::1, 0::25>>
  end

  defp opcode(:reply),       do: 1
  defp opcode(:msg),         do: 1000
  defp opcode(:update),      do: 2001
  defp opcode(:insert),      do: 2002
  defp opcode(:query),       do: 2004
  defp opcode(:getmore),     do: 2005
  defp opcode(:delete),      do: 2006
  defp opcode(:killcursor),  do: 2007
  defp bit(false), do: 0
  defp bit(true), do: 1
  defp bool(0), do: false
  defp bool(1), do: true
  defp document(nil), do: <<>>
  defp document(doc) when is_binary(doc), do: doc
  defp document(doc), do: Bson.encode(doc)

  defp reqid() do
    <<tail::24, _::1, head::7>> = :crypto.rand_bytes(4)
    <<tail::24, 0::1, head::7>>
  end

  @doc """
  Assigns id to a document or a list of document when `:_id` is missing
  """
  def assign_id([{a,_}]=doc) when is_atom(a) do
    Keyword.put(doc, :'_id', Bson.ObjectId.new(oid: :crypto.rand_bytes(12)))
  end
  def assign_id(docs) do
    case length(docs) do
      l when l < 256 ->
        Enum.map_reduce(
          docs,
          <<0::16>> <> :crypto.rand_bytes(10),
          fn(doc, id) -> { Keyword.put(doc, :'_id', Bson.ObjectId.new(oid: to_oid(id))), next_id(id) } end)
        |> elem(0)
    end
  end

  defp to_oid(id), do: id |> bitstring_to_list |> Enum.reverse |> list_to_bitstring
  defp next_id(<<255, r::binary>>), do: <<0::8, (next_id(r))::binary>>
  defp next_id(<<n::8, r::binary>>), do: <<(n+1)::8, r::binary>>

  defp next_bson({0, _bsonbuffer, _off, nextbatchfn}) do
    case nextbatchfn.() do
      Reply[nbdocs: 0] -> nil
      # last batch
      Reply[nbdocs: nbdocs, bsonbuffer: bsonbuffer, cursorid: 0] ->
        next_bson({nbdocs, bsonbuffer, 0})
      # next bacth
      Reply[nbdocs: nbdocs, bsonbuffer: bsonbuffer] ->
        next_bson({nbdocs, bsonbuffer, 0, nextbatchfn})
    end
  end
  defp next_bson({nbdocs, bsonbuffer, off, nextbatchfn}) do
    partlen = Bson.int32(bsonbuffer, off)
    {{bsonbuffer, {off, partlen}}, {nbdocs-1, bsonbuffer, off+partlen, nextbatchfn}}
  end


  defp next_bson({0, _bsonbuffer, _off}), do: nil
  defp next_bson({nbdocs, bsonbuffer, off}) do
    partlen = Bson.int32(bsonbuffer, off)
    {{bsonbuffer, {off, partlen}}, {nbdocs-1, bsonbuffer, off+partlen}}
  end


end