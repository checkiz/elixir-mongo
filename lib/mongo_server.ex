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
  connects to a mongodb server specifying mode (default port)
  """
  def connect(host, mode) when is_binary(host) and (mode == :active or mode == :passive) do
    connect(host: host, mode: mode)
  end

  @doc """
  connects to a mongodb server specifying mode
  """
  def connect(host, port, mode) when mode == :active or mode == :passive do
    connect(host: host, port: port, mode: mode)
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
    case mongo(host: host, port: port, mode: mode, timeout: timeout).tcp_connect do
      { :ok, m } ->
        m
      {:error, reason} ->
        raise Mongo.error, reason: reason
    end
  end
  def connect(mongo()=m) do
    case m.tcp_connect do
      { :ok, m } ->
        m
      {:error, reason} ->
        raise Mongo.error, reason: reason
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
  defp options(mongo(mode: mode)) do
    args = []

    # mode active or passive
    args = [{ :active, false }]

    args
  end

end
