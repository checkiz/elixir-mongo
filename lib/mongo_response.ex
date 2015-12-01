defmodule Mongo.Response do
  @moduledoc """
  Receives, decode and parse MongoDB response from the server
  """
  defstruct [
    cursorID: nil,
    startingFrom: nil,
    nbdoc: nil,
    buffer: nil,
    requestID: nil,
    decoder: nil]

  @msg         <<1, 0, 0, 0>>    #    1  Opcode OP_REPLY : Reply to a client request

  defimpl Enumerable, for: Mongo.Response do

    @doc """
    Reduce documents in the buffer into a value
    """
    def reduce(resp, acc, reducer)
    def reduce(%Mongo.Response{buffer: buffer, nbdoc: nbdoc, decoder: decoder}, acc, reducer), do: do_reduce(buffer, nbdoc, decoder, acc, reducer)

    defp do_reduce(_, _, _, {:halt, acc}, _fun),   do: {:halted, acc}
    defp do_reduce(buffer, nbdoc, decoder, {:suspend, acc}, reducer), do: {:suspended, acc, &do_reduce(buffer, nbdoc, decoder, &1, reducer)}
    defp do_reduce(_, 0, _, {:cont, acc}, _reducer),   do: {:done, acc}
    defp do_reduce(buffer, nbdoc, decoder, {:cont, acc}, reducer) do
      case decoder.(buffer) do
        {:cont, doc, rest} -> do_reduce(rest, nbdoc-1, decoder, reducer.(doc, acc), reducer)
        other -> other
      end
    end

    @doc """
    Retreives number of documents in the buffer
    """
    def count(%Mongo.Response{nbdoc: nbdoc}), do: {:ok, nbdoc}

    @doc """
    Checks whether a document is part of the buffer
    """
    def member?(resp, doc)
    def member?(%Mongo.Response{buffer: buffer}, doc), do: is_member(buffer, doc)

    defp is_member(buffer, doc), do: is_member(buffer, doc, byte_size(doc))
    # size are identical, check content
    defp is_member(buffer, _doc, docsize) when byte_size(buffer) < docsize, do: false
    defp is_member(<<docsize::32-little-signed, _::binary>>=buffer, doc, docsize) do
      case :erlang.split_binary(buffer, docsize) do
        {^doc, _} -> {:ok, true}
        {_, tail} -> is_member(tail, doc)
      end
    end
    # size different, skip to next doc
    defp is_member(<<size::32-little-signed, _::binary>>=buffer, doc, docsize) do
      is_member(:erlang.split_binary(buffer, size)|>elem(1), doc, docsize)
    end
  end

  @doc """
  Parses a response message

  If the message is partial, this method makes shure the response is complete by fetching additional messages
  """
  def new(
    <<_::32,                                           # total message size, including this
      _::32,                                           # identifier for this message
      requestID::size(32)-signed-little,               # requestID from the original request
      @msg::binary,                                    # Opcode OP_REPLY
      _::6, queryFailure::1, cursorNotFound::1, _::24, # bit vector representing response flags
      cursorID::size(64)-signed-little,                # cursor id if client needs to do get more's
      startingFrom::size(32)-signed-little,            # where in the cursor this reply is starting
      numberReturned::size(32)-signed-little,          # number of documents in the reply
      buffer::bitstring>>,                             # buffer of Bson documents
      decoder \\ &(Mongo.Response.bson_decode(&1))) do
    cond do
      cursorNotFound>0 ->
        %Mongo.Error{msg: :"cursor not found"}
      queryFailure>0 ->
        if numberReturned>0 do
          %Mongo.Error{
            msg: :"query failure",
            acc: %Mongo.Response{buffer: buffer, nbdoc: numberReturned, decoder: decoder}|>Enum.to_list}
        else
          %Mongo.Error{msg: :"query failure"}
        end
      true -> {:ok, %Mongo.Response{
                cursorID: cursorID,
                startingFrom: startingFrom,
                nbdoc: numberReturned,
                buffer: buffer,
                requestID: requestID,
                decoder: decoder }}
    end
  end

  @doc """
  Decodes a command response

  Returns `{:ok, doc}` or transfers the error message
  """
  def cmd(%Mongo.Response{nbdoc: 1, buffer: buffer}) do
    case buffer |> Bson.decode do
      nil -> %Mongo.Error{msg: :"no document received"}
      %{ok: ok}=doc when ok>0 -> {:ok, doc}
      errdoc -> %Mongo.Error{msg: :"cmd error", acc: errdoc}
    end
  end

  @doc """
  Decodes a count respsonse

  Returns `{:ok, n}` or transfers the error message
  """
  def count(response) do
    case cmd(response) do
      {:ok, doc} -> {:ok, doc[:n]}
      error -> error
    end
  end

  @doc """
  Decodes a success respsonse

  Returns `:ok` or transfers the error message
  """
  def success(response) do
    case cmd(response) do
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Decodes a distinct respsonse

  Returns `{:ok, values}` or transfers the error message
  """
  def distinct(response) do
    case cmd(response) do
      {:ok, doc} -> {:ok, doc[:values]}
      error -> error
    end
  end

  @doc """
  Decodes a map-reduce respsonse

  Returns `{:ok, results}` (inline) or `:ok` or transfers the error message
  """
  def mr(response) do
    case cmd(response) do
      {:ok, doc} ->
        case doc[:results] do
          nil -> :ok
          results -> {:ok, results}
        end
      error -> error
    end
  end

  @doc """
  Decodes a group respsonse

  Returns `{:ok, retval}` or transfers the error message
  """
  def group(response) do
    case cmd(response) do
      {:ok, doc} -> {:ok, doc[:retval]}
      error -> error
    end
  end

  @doc """
  Decodes an aggregate respsonse

  Returns `{:ok, result}` or transfers the error message
  """
  def aggregate(response) do
    case cmd(response) do
      {:ok, doc} -> doc[:result]
      error -> error
    end
  end
  @doc """
  Decodes a getnonce respsonse

  Returns `{:ok, nonce}` or transfers the error message
  """
  def getnonce(response) do
    case cmd(response) do
      {:ok, doc} -> doc[:nonce]
      error -> error
    end
  end
  @doc """
  Decodes an error respsonse

  Returns `{:ok, nonce}` or transfers the error message
  """
  def error(response) do
    case cmd(response) do
      {:ok, doc} ->
        case doc[:err] do
          nil -> :ok
          _ -> {:error, doc}
        end
      error -> error
    end
  end

  @doc """
  Helper fuction to decode the first document of a bson buffer
  """
  def bson_decode(buffer, opts \\ %Bson.Decoder{}) do
    case Bson.Decoder.document(buffer, opts) do
      %Bson.Decoder.Error{}=error -> {:halt, %Mongo.Error{msg: :bson, acc: [error]}}
      {doc, rest} -> {:cont, doc, rest}
    end
  end

  @doc """
  Helper fuction to split buffer into documents in binary format
  """
  def bson_no_decoding(<<size::32-little-signed, _rest>>=buffer) do
    {doc, rest} = :erlang.split_binary(buffer, size)
    {:cont, doc, rest}
  end
end
