defmodule Mongo.Request do
  @moduledoc """
  Defines, encodes and sends MongoDB operations to the server
  """

  defstruct [
    requestID: nil,
    payload: nil]


  @update      <<0xd1, 0x07, 0, 0>> # 2001  update document
  @insert      <<0xd2, 0x07, 0, 0>> # 2002  insert new document
  @get_more    <<0xd5, 0x07, 0, 0>> # 2005  Get more data from a query. See Cursors
  @delete      <<0xd6, 0x07, 0, 0>> # 2006  Delete documents
  @kill_cursor <<0xd7, 0x07, 0, 0>> # 2007  Tell database client is done with a cursor

  @query       <<0xd4, 0x07, 0, 0>> # 2004  query a collection
  @query_opts  <<0b00000100::8>>    # default query options, equvalent to `cursor.set_opts(slaveok: true)`

  @doc """
    Builds a query message

    * collection: collection
    * selector: selection criteria (Map or nil)
    * projector: fields (Map or nil)
  """
  def query(find) do
    selector = if find.mods == %{}, do: find.selector, else: Map.put(find.mods, :'$query', find.selector)
    @query <> (Enum.reduce(find.opts, @query_opts, &queryopt_red/2)) <> <<0::24>> <>
      find.collection.db.name <> "." <>  find.collection.name <> <<0::8>> <>
      <<find.skip::32-little-signed>> <>
      <<find.batchSize::32-little-signed>> <>
      Bson.encode(selector) <>
      Bson.encode(find.projector)
  end

  @doc """
    Builds a database command message composed of the command tag and its arguments.
  """
  def cmd(dbname, cmd, cmd_args \\ %{}) do
    @query <> @query_opts <> <<0::24>> <> # [slaveok: true]
    dbname <> ".$cmd" <>
    <<0::40, 255, 255, 255, 255>> <> # skip(0), batchSize(-1)
    document(cmd, cmd_args)
  end

  @doc """
    Builds an insert command message
  """
  def insert(collection, docs) do
    docs |> Enum.reduce(
      @insert <> <<0::32>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>>,
      fn(doc, acc) -> acc <> Bson.encode(doc) end)
  end

  @doc """
    Builds an update command message
  """
  def update(collection, selector, update, upsert, multi) do
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
      @delete <> <<0::32>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>> <>
      <<0::7, (bit(justOne))::1, 0::24>> <>
      document(selector)
  end

  @doc """
    Builds a kill_cursor command message
  """
  def kill_cursor(cursorid) do
      @kill_cursor <> <<0::32>> <>
      <<1::32-little-signed>> <>
      <<cursorid::64-little-signed>>
  end

  @doc """
    Builds a get_more command message
  """
  def get_more(collection, batchsize, cursorid) do
      @get_more <> <<0::32>> <>
      collection.db.name <> "." <>  collection.name <> <<0::8>> <>
      <<batchsize::32-little-signed>> <>
      <<cursorid::64-little-signed>>
  end

  # transform a document into bson
  defp document(command), do: Bson.encode(command)
  defp document(command, command_args), do: Bson.Encoder.document(Enum.to_list(command) ++ Enum.to_list(command_args))

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
