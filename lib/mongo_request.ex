defmodule Mongo.Request do
  @moduledoc """
  Defines, encodes and sends MongoDB operations to the server
  """
  use Bitwise

  defrecordp :request, __MODULE__ ,
    requestID: nil,
    mongo: nil,
    payload: nil

  @update      <<0xd1, 0x07, 0, 0>> # 2001  update document
  @insert      <<0xd2, 0x07, 0, 0>> # 2002  insert new document
  @query       <<0xd4, 0x07, 0, 0>> # 2004  query a collection
  @get_more    <<0xd5, 0x07, 0, 0>> # 2005  Get more data from a query. See Cursors
  @delete      <<0xd6, 0x07, 0, 0>> # 2006  Delete documents
  @kill_cursor <<0xd7, 0x07, 0, 0>> # 2007  Tell database client is done with a cursor

  @query_opts  <<0b00000100::8>>    # default query options, equvalent to `cursor.set_opts(slaveok: true)`
  @doc """
    Builds a query message

    * collection: collection
    * selector: selection criteria (Keyword or nil)
    * projector: fields (Keyword or nil)
  """
  def query(collection, selector, projector, skip, batchSize, opts) do
    request mongo: collection.db.mongo, payload:
      @query <> (Enum.reduce(opts, @query_opts, &queryopt_red/2)) <> <<0::24>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>> <>
      Bson.int32(skip) <>
      Bson.int32(batchSize) <>
      document(selector) <>
      document(projector)
  end

  @doc """
    Builds a database command message
  """
  def cmd(db, command) do
    request mongo: db.mongo, payload:
      @query <> @query_opts <> <<0::24>> <> # [slaveok: true]
      db.name <> ".$cmd" <>
      <<0::40, 255, 255, 255, 255>> <> # skip(0), batchSize(-1)
      document(command)
  end

  @doc """
    Builds an admin command message
  """
  def adminCmd(mongo, command) do
    cmd(mongo.db("admin"), command)
  end

  @doc """
    Builds an insert command message
  """
  def insert(collection, docs) do
    request mongo: collection.db.mongo, payload:
      docs |> Enum.reduce(
      @insert <> <<0::32>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>>,
      fn(doc, acc) -> acc <> Bson.encode(doc) end)
  end

  @doc """
    Builds an update command message
  """
  def update(collection, selector, update, upsert, multi) do
    request mongo: collection.db.mongo, payload:
      @update <> <<0::32>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>> <>
      <<0::6, (bit(multi))::1, (bit(upsert))::1, 0::24>> <>
      (document(selector) ) <>
      (document(update))
  end
  # transforms `true` and `false` to bits
  defp bit(false), do: 0
  defp bit(true), do: 1

  @doc """
    Builds a delete command message
  """
  def delete(collection, selector, justOne) do
    request mongo: collection.db.mongo, payload:
      @delete <> <<0::32>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>> <>
      <<0::7, (bit(justOne))::1, 0::24>> <>
      document(selector)
  end

  @doc """
    Builds a kill_cursor command message
  """
  def kill_cursor(db, cursorid) do
    request mongo: db.mongo, payload:
      @kill_cursor <> <<0::32>> <>
      Bson.int32(1) <>
      Bson.int64(cursorid)
  end

  @doc """
    Builds a get_more command message
  """
  def get_more(collection, batchsize, cursorid) do
    request mongo: collection.db.mongo, payload:
      @get_more <> <<0::32>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>> <>
      Bson.int32(batchsize) <>
      Bson.int64(cursorid)
  end

  @doc """
  Sends request to mongodb
  """
  def send(request(payload: payload, requestID: requestID, mongo: mongo)) do
    requestID = if requestID==nil, do: gen_reqid, else: requestID
    case message(payload, requestID) |> mongo.send do
      :ok -> requestID
      error -> error
    end
  end

  @doc """
  Sets the request ID

  By default, request ID is generated, but it can be set using this function.
  This is usefull when the connection to MongoDB is active (by default, it is passive)
  """
  def id(requestID, r), do: request(r, requestID: requestID)

  # transform a document into bson
  defp document(nil), do: document({})
  defp document(doc) when is_binary(doc), do: doc
  defp document(doc), do: Bson.encode(doc)

  defp message(payload, reqid) do
    <<(byte_size(payload) + 12)::[size(32),little]>> <> reqid <> <<0::32>> <> <<payload::binary>>
  end
  # generates a request Id when not provided (makes sure it is a positive integer)
  defp gen_reqid() do
    <<tail::24, _::1, head::7>> = :crypto.rand_bytes(4)
    <<tail::24, 0::1, head::7>>
  end
  
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