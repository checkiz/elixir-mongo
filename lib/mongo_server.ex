defmodule Mongo.Server do
  import Kernel, except: [send: 2]
  @moduledoc """
  Manage the connection to a mongodb server
  """
  defstruct [
    host: nil,
    port: nil,
    mode: false,
    timeout: nil,
    opts: %{},
    id_prefix: nil,
    socket: nil ]

  @port    27017
  @mode    :passive
  @host    "127.0.0.1"
  @timeout 6000

  use Mongo.Helpers

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
  def connect do
    connect %{}
  end

  @doc """
  connects to a mongodb server
  """
  def connect(host, port) when is_binary(host) and is_integer(port) do
    connect %{host: host, port: port}
  end

  @doc """
  connects to a mongodb server specifying options

  Opts must be a Map
  """
  def connect(opts) when is_map(opts) do
    opts = default_env(opts)
    host = Map.get(opts, :host,    @host)
    tcp_connect %Mongo.Server{
      host: case host do
              host when is_binary(host) -> String.to_char_list(host)
              host -> host
            end,
      port: Map.get(opts, :port,    @port),
      mode: Map.get(opts, :mode,    @mode),
      timeout: Map.get(opts, :timeout,    @timeout),
      id_prefix: mongo_prefix}
  end

  @doc false
  def tcp_connect(mongo) do
    case :gen_tcp.connect(mongo.host, mongo.port, tcp_options(mongo), mongo.timeout) do
      {:ok, socket} ->
        {:ok, %Mongo.Server{mongo| socket: socket}}
      error -> error
    end
  end

  defp tcp_recv(mongo) do
    :gen_tcp.recv(mongo.socket, 0, mongo.timeout)
  end

  @doc """
  Retreives a repsonce from the MongoDB server (only for passive mode)
  """
  def response(mongo, decoder \\ &(Mongo.Response.bson_decode(&1))) do
    case tcp_recv(mongo) do
      {:ok, <<messageLength::32-signed-little, _::binary>> = message} ->
        complete(mongo, messageLength, message) |> Mongo.Response.new(decoder)
      {:error, msg} -> %Mongo.Error{msg: msg}
    end
  end

  @doc """
  Sends a message to MongoDB
  """
  def send(mongo, payload, reqid \\ gen_reqid)
  def send(%Mongo.Server{socket: socket, mode: :passive}, payload, reqid) do
     do_send(socket, payload, reqid)
  end
  def send(%Mongo.Server{socket: socket, mode: :active}, payload, reqid) do
    :inet.setopts(socket, active: :once)
    do_send(socket, payload, reqid)
  end
  # sends the message to the socket, returns request {:ok, reqid}
  defp do_send(socket, payload, reqid) do
    case :gen_tcp.send(socket, payload |> message(reqid)) do
      :ok -> {:ok, reqid}
      error -> error
    end
  end

  @doc false
  # preprares for a one-time async request
  def async(%Mongo.Server{mode: :passive}=mongo) do
    :inet.setopts(mongo.socket, active: :once)
  end

  @doc """
  Sends a command message requesting imediate response
  """
  def cmd_sync(mongo, command) do
    case cmd(mongo, command) do
      {:ok, _reqid} -> response(mongo)
      error -> error
    end
  end

  @doc """
  Executes an admin command to the server

    iex> mongo = Mongo.connect!  # Returns a exception when connection fails
    iex> case Mongo.connect do
    ...>    {:ok, mongo } -> :ok
    ...>    error -> error
    ...> end
    :ok

  """
  def cmd(mongo, cmd) do
    send(mongo, Mongo.Request.cmd("admin", cmd))
  end

  @doc """
  Pings the server

    iex> Mongo.connect! |> Mongo.Server.ping
    :ok

  """
  def ping(mongo) do
    case cmd_sync(mongo, %{ping: true}) do
      {:ok, resp} -> Mongo.Response.success(resp)
      error -> error
    end
  end

  @doc """
  Returns true if connection mode is active
  """
  def active?(mongo), do: mongo.mode == :active

  @doc """
  Closes the connection
  """
  def close(mongo) do
    :gen_tcp.close(mongo.socket)
  end

  defp default_env(opts) do
    case :application.get_env(:mongo, :host) do
        {:ok, {host, port}} ->
          opts |> Map.put_new(:host, host) |> Map.put_new(:port, port)
        _ -> opts
    end
  end

  # makes sure response is complete
  defp complete(_mongo, expected_length, buffer) when byte_size(buffer) == expected_length, do: buffer
  defp complete(_mongo, expected_length, buffer) when byte_size(buffer) >  expected_length, do: binary_part(buffer, 0, expected_length)
  defp complete(mongo, expected_length, buffer) do
    case tcp_recv(mongo) do
      {:ok, mess} -> complete(mongo, expected_length, buffer <> mess)
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
  defp options(mongo) do
    [ active: false,
      send_timeout: mongo.timeout,
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
  def prefix(%Mongo.Server{id_prefix: prefix}) do
    for << <<b::4>> <- <<prefix::16>> >>, into: <<>> do
        <<Integer.to_string(b,16)::binary>>
    end |> String.downcase
  end

  @doc """
  Adds options to an existing mongo server connection

  new_opts must be a map with zero or more of the following keys:

  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(mongo, new_opts) do
    %Mongo.Server{mongo| opts: Map.merge(mongo.opts, new_opts)}
  end

  @doc """
  Gets mongo connection default options
  """
  def db_opts(mongo) do
    Map.take(mongo.opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :wc]) #, :mode, :timeout])
    |> Map.put(:mode, mongo.mode) |> Map.put(:timeout, mongo.timeout)
  end

  use Bitwise, only_operators: true
  @doc """
  Assigns radom ids to a list of documents when `:_id` is missing

      iex> [%{a: 1}] |> Mongo.Server.assign_id |> Enum.at(0) |> Map.keys
      [:"_id", :a]

      #a prefix to ids can be set manually like this
      iex> prefix = case [%{a: 1}] |> Mongo.Server.assign_id(256*256-1) |> Enum.at(0) |> Map.get(:"_id") do
      ...>   %Bson.ObjectId{oid: <<prefix::16, _::binary>>} -> prefix
      ...>   error -> error
      ...> end
      ...> prefix
      256*256-1

      #by default prefix are set at connection time and remains identical for the entire connection
      iex> mongo = Mongo.connect!
      ...> prefix = case [%{a: 1}] |> Mongo.Server.assign_id(mongo) |> Enum.at(0) |> Map.get(:"_id") do
      ...>   %Bson.ObjectId{oid: <<prefix::16, _::binary>>} -> prefix
      ...>   error -> error
      ...> end
      ...> prefix == mongo.id_prefix
      true

  """
  def assign_id(docs, client_prefix \\ gen_client_prefix)
  def assign_id(docs, client_prefix) do
    client_prefix = check_client_prefix(client_prefix)
    Enum.map_reduce(
      docs,
      {client_prefix, gen_trans_prefix, :crypto.rand_uniform(0, 4294967295)},
      fn(doc, id) -> { Map.put(doc, :'_id', %Bson.ObjectId{oid: to_oid(id)}), next_id(id) } end)
      |> elem(0)
  end

  # returns a 2 bites prefix integer
  defp check_client_prefix(%Mongo.Server{id_prefix: prefix}) when is_integer(prefix), do: prefix
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

  # add request ID to a payload message
  defp message(payload, reqid)
  defp message(payload, reqid) do
    <<(byte_size(payload) + 12)::size(32)-little>> <> reqid <> <<0::32>> <> <<payload::binary>>
  end
  # generates a request Id when not provided (makes sure it is a positive integer)
  defp gen_reqid() do
    <<tail::24, _::1, head::7>> = :crypto.rand_bytes(4)
    <<tail::24, 0::1, head::7>>
  end

end
