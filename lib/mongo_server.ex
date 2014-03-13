defmodule Mongo.Server do
  @moduledoc """
  Manage the connection to a mongodb server
  """
  use Mongo.Helpers
  defrecordp :mongo, __MODULE__ ,
    host: nil,
    port: nil,
    mode: false,
    timeout: nil,
    opts: [],
    id_prefix: nil,
    socket: nil

  @port    27017
  @mode    :passive
  @host    "127.0.0.1"
  @timeout 6000

  @doc """
  connects to local mongodb server by defaults to {"127.0.0.1", 27017}

  This can be overwritten by the environment variable `:host`, ie:

  ```erlang
  [
    {mongo, 
      [
        {host, {"127.0.0.1", 27017}}
      ]}
  ].
  ```
  """
  def connect() do
    connect []
  end

  @doc """
  connects to a mongodb server
  """
  def connect(host, port) when is_binary(host) and port |> is_integer do
    connect(host: host, port: port)
  end

  @doc """
  connects to a mongodb server specifying options

  Opts must be a Keyword
  """
  def connect(opts) when opts |> is_list do
    opts    = default_env(opts)
    host    = Keyword.get(opts, :host,    @host)
    port    = Keyword.get(opts, :port,    @port)
    timeout = Keyword.get(opts, :timeout, @timeout)
    mode    = Keyword.get(opts, :mode,    @mode)
    if is_binary(host) do
      host = String.to_char_list!(host)
    end
    mongo(host: host, port: port, mode: mode, timeout: timeout, id_prefix: mongo_prefix).tcp_connect
  end
  def connect(mongo()=m) do
    case m.tcp_connect do
      { :ok, m } ->
        m
      error -> error
    end    
  end

  @doc false
  def tcp_connect(mongo(host: host, port: port, timeout: timeout)=m) do
    case :gen_tcp.connect(host, port, tcp_options(m), timeout) do
      {:ok, socket} ->
        {:ok, mongo(m, socket: socket)}
      error -> error
    end
  end

  @doc false
  defp tcp_recv(mongo(socket: socket, timeout: timeout)) do
    :gen_tcp.recv(socket, 0, timeout)
  end

  @doc """
  Retreives a repsonce from the MongoDB server (only for passive mode)
  """
  def response(mongo) do
    case tcp_recv(mongo) do
      {:ok, <<messageLength::[little, signed, size(32)], _::binary>> = message} ->
        complete(messageLength, message, mongo) |> Mongo.Response.new
      error -> error
    end
  end

  @doc """
  Completes a possibly partial repsonce from the MongoDB server
  """
  def response(
    <<messageLength::[little, signed, size(32)], _::binary>> = message,
    mongo) do
    complete(messageLength, message, mongo) |> Mongo.Response.new
  end
  defbang response(message, mongo)

  @doc """
  Sends a message to MongoDB  
  """
  def send(message, mongo(socket: socket, mode: :passive)) do
     :gen_tcp.send(socket, message)
  end
  def send(message, mongo(socket: socket, mode: :active)) do
    :inet.setopts(socket, active: :once)
    :gen_tcp.send(socket, message)
  end
  @doc false
  # preprares for a one-time async request
  def async(mongo(socket: socket, mode: :passive)) do
    :inet.setopts(socket, active: :once)
  end

  @doc """
  Executes an admin command to the server
  """
  def adminCmd(command, mongo) do
    mongo.db("admin").cmd(command)
  end

  @doc """
  Pings the server
  """
  def ping(mongo) do
    mongo |> Mongo.Request.adminCmd(mongo, ping: true).send
    case mongo.response do
      {:ok, resp} -> resp.success
      error -> error
    end
  end

  @doc """
  Returns true if connection mode is active
  """
  def active?(mongo(mode: mode)), do: mode == :active

  @doc """
  Connects to a specific database
  """
  def db(name, mongo) do
    Mongo.Db.new(mongo, name)
  end

  @doc """
  Closes the connection
  """
  def close(mongo(socket: socket)) do
    :gen_tcp.close(socket)
  end

  defp default_env(opts) do
    case :application.get_env(:mongo, :host) do
        {:ok, {host, port}} ->
          opts |> Keyword.put_new(:host, host) |> Keyword.put_new(:port, port)
        _ -> opts
    end
  end

  # makes shure response is complete
  defp complete(expected_length, buffer, _mongo) when size(buffer) == expected_length, do: buffer
  defp complete(expected_length, buffer, _mongo) when size(buffer) >  expected_length, do: binary_part(buffer, 0, expected_length)
  defp complete(expected_length, buffer, mongo) do
    case tcp_recv(mongo) do
      {:ok, mess} -> complete(expected_length, buffer <> mess, mongo)
    end
  end

  # Convert TCP options to `:inet.setopts` compatible arguments.
  defp tcp_options(m) do
    args = options(m)

    # default to binary
    args = [:binary | args]

    args
  end
  # default server options
  defp options(mongo(timeout: timeout)) do
    [ active: false,
      send_timeout: timeout,
      send_timeout_close: true ]
  end

  defp mongo_prefix do
    case :inet.gethostname do
      {:ok, hostname} ->
        <<prefix::16, _::binary>> = :crypto.hash(:md5, (hostname ++ :os.getpid) |> to_string)
        prefix
      _ -> :crypto.rand_uniform(0, 65535)
    end
  end
  @doc false
  def prefix(mongo(id_prefix: prefix)) do
    bc <<b::4>> inbits <<prefix::16>> do
        <<integer_to_binary(b,16)::binary>>
    end |> String.downcase
  end

  @doc """
  Adds options to the mongo server connection

  new_opts must be a keyword with zero or more pairs represeting one of these options:
  
  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write: concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(new_opts, mongo(opts: opts)=mongo) do
    mongo(mongo, opts: Keyword.merge(opts, new_opts))
  end

  @doc """
  Gets the mongo connection default options
  """
  def db_opts(mongo(opts: opts)) do
    Keyword.take(opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :wc, :mode, :timeout])
  end

  @doc false
  use Bitwise, only_operators: true
  def assign_id(docs, client_prefix \\ gen_client_prefix) do
    client_prefix = check_client_prefix(client_prefix)
    Enum.map_reduce(
      docs,
      {client_prefix, gen_trans_prefix, :crypto.rand_uniform(0, 4294967295)},
      fn(doc, id) -> { Keyword.put(doc, :'_id', Bson.ObjectId.new(oid: to_oid(id))), next_id(id) } end)
      |> elem(0)
  end

  # returns a 2 bites prefix integer
  defp check_client_prefix(mongo(id_prefix: prefix)) when is_integer(prefix), do: prefix
  defp check_client_prefix(prefix) when is_integer(prefix), do: prefix
  defp check_client_prefix(_), do: gen_client_prefix
  # generates a 2 bites prefix integer
  defp gen_client_prefix, do: :crypto.rand_uniform(0, 65535)
  # returns a 6 bites prefix integer
  defp gen_trans_prefix do
    {gs, s, ms} = :erlang.now
    (gs * 1000000000000 + s * 1000000 + ms) &&& 281474976710655
  end

  # from a 3 integer tuple to ObjectID
  defp to_oid({client_prefix, trans_prefix, suffix}), do: <<client_prefix::16, trans_prefix::48, suffix::32>>
  # Selects next ID
  defp next_id({client_prefix, trans_prefix, suffix}), do: {client_prefix, trans_prefix, suffix+1}

end
