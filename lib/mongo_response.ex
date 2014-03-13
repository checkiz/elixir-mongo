defmodule Mongo.Response do
  @moduledoc """
  Receives, decode and parse MongoDB response from the server
  """
  use Mongo.Helpers
  defrecordp :response, __MODULE__ ,
    cursorID: nil,
    startingFrom: nil,
    nbDoc: nil,
    docBuffer: nil,
    requestID: nil,
    bufferOffset: 0
  @msg         <<1, 0, 0, 0>>    #    1  Opcode OP_REPLY : Reply to a client request

  @doc """
  Parses a response message

  If the message is partial, this method makes shure the response is complete by fetching additional messages
  """
  def new(
    <<_::32,                                            # total message size, including this
      _::32,                                            # identifier for this message
      requestID::[size(32),signed,little],              # requestID from the original request
      @msg::binary,                                     # Opcode OP_REPLY
      _::6, queryFailure::1, cursorNotFound::1, _::24,  # bit vector representing response flags
      cursorID::[size(64),signed,little],               # cursor id if client needs to do get more's
      startingFrom::[size(32),signed,little],           # where in the cursor this reply is starting
      numberReturned::[size(32),signed,little],         # number of documents in the reply
      docBuffer::bitstring>>) do                        # buffer of Bson documents
    cond do
      cursorNotFound>0 ->
        {:error, "Cursor not found"}
      queryFailure>0 ->
        if numberReturned>0 do
          {:error, Keyword.take(Bson.decode(docBuffer), [:'$err', :err, :errmsg, :code, :connectionId])}
        else
          {:error, "Query error"}
        end
      true -> {:ok, response( cursorID: cursorID,
                  startingFrom: startingFrom,
                  nbDoc: numberReturned,
                  docBuffer: docBuffer,
                  requestID: requestID )}
    end
  end
  defbang new(message)

  @doc """
  Returns `true` if there are more documents to fetch regardless whether the cursor is exhausted or not
  """
  def next?(response(nbDoc: 0)), do: false
  def next?(_),                  do: true

  @doc """
  Gets next doc within the current batch return `nil` after last doc of the bacth 
  
  When this function returns `nil`, it does not mean the cursor is exhausted, see `Mongo.Response.hasNext/1`
  """
  def next(response(nbDoc: nbDoc, bufferOffset: off, docBuffer: docBuffer)=r) when nbDoc>0 do
    # get document part of documents from this offset
    partlen = Bson.int32(docBuffer, off)
    {Bson.decode({off, partlen}, docBuffer), response(r, nbDoc: nbDoc-1, bufferOffset: off+partlen)}
  end
  def next(_, response(nbDoc: 0)),    do: nil

  @doc """
  Returns the cursor ID
  """
  def cursorID(response(cursorID: cursorID)), do: cursorID

  @doc """
  Returns `true` if the cursor is exhausted
  """
  def exhausted?(response(cursorID: 0)), do: true
  def exhausted?(_), do: false

  @doc """
  Returns `true` if there are more documents to fetch from this batch or if the cursor is not exhausted
  """
  def hasNext(r), do: r.next? or not r.exhausted

  @doc """
  Parse a command respsonse

  Returns `{:ok, doc}` or `{:error, reason}`
  """
  def cmd(response(nbDoc: 1)=r) do
    case r.next do
        nil -> {:error, "No document received"}
        {doc, _} ->
          if doc[:ok] > 0 do
            {:ok, doc}
          else
            {:error, Keyword.take(doc, [:err, :errmsg, :code, :connectionId])}
          end
      end
  end

  @doc """
  Parse a count respsonse

  Returns `{:ok, n}` or `{:error, reason}`
  """
  def count(r) do
    case r.cmd do
      {:ok, doc} -> {:ok, doc[:n]}
      error -> error
    end
  end
  
  @doc """
  Parse a success respsonse

  Returns `:ok` or `{:error, reason}`
  """
  def success(r) do
    case r.cmd do
      {:ok, _} -> :ok
      error -> error
    end
  end
  
  @doc """
  Parse a distinct respsonse

  Returns `{:ok, values}` or `{:error, reason}`
  """
  def distinct(r) do
    case r.cmd do
      {:ok, doc} -> {:ok, doc[:values]}
      error -> error
    end
  end

  @doc """
  Parse a map-reduce respsonse

  Returns `{:ok, results}` (inline) or `:ok` or `{:error, reason}`
  """
  def mr(r) do
    case r.cmd do
      {:ok, doc} ->
        case doc[:results] do
          nil -> :ok
          results -> {:ok, results}
        end
      error -> error
    end
  end

  @doc """
  Parse a group respsonse

  Returns `{:ok, retval}` or `{:error, reason}`
  """
  def group(r) do
    case r.cmd do
      {:ok, doc} -> {:ok, doc[:retval]}
      error -> error
    end
  end

  @doc """
  Parse a aggregate respsonse

  Returns `{:ok, result}` or `{:error, reason}`
  """
  def aggregate(r) do
    case r.cmd do
      {:ok, doc} -> doc[:result]
      error -> error
    end
  end
  @doc """
  Parse a getnonce respsonse

  Returns `{:ok, nonce}` or `{:error, reason}`
  """
  def getnonce(r) do
    case r.cmd do
      {:ok, doc} -> doc[:nonce]
      error -> error
    end
  end
  @doc """
  Parse a error respsonse

  Returns `{:ok, nonce}` or `{:error, reason}`
  """
  def error(r) do
    case r.cmd do
      {:ok, doc} ->
        case doc[:err] do
          nil -> :ok
          _ -> {:error, Keyword.take(doc, [:err, :errmsg, :code, :connectionId])}
        end
      error -> error
    end
  end
end