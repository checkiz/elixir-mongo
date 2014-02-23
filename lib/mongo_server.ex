defmodule Mongo.Server do
  @moduledoc """
  Manage the connection to a mongodb server
  """
  use Mongo.Helpers
  defrecordp :mongo, __MODULE__ ,
    host: nil,
    port: nil,
    active: false,
    timeout: nil,
    socket: nil,
    opts: nil

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
    case :application.get_env(:mongo, :host) do
        {:ok, {host, port}} -> connect host, port, mode: @mode
        _                   -> connect @host, @port, mode: @mode
    end
  end

  @doc """
  connects to a mongodb server with mode `:active` or `:passive`

  `:passive` is the default mode
  """
  def connect(mode) when mode == :active or mode == :passive do
    {host, port} = default_host
    connect(host, port, mode)
  end

  @doc """
  connects to a mongodb server
  """
  def connect(host, port \\ @port) when port |> is_integer do
    connect(host, port, mode: @mode)
  end

  @doc """
  connects to a mongodb server specifying mode (default port)
  """
  def connect(host, mode) when mode == :active or mode == :passive do
    connect(host, @port, mode)
  end

  @doc """
  connects to a mongodb server specifying mode
  """
  def connect(host, port, mode) when mode == :active or mode == :passive do
    connect(host, port, mode: mode, timeout: @timeout)
  end

  @doc """
  connects to a mongodb server specifying options
  """
  def connect(host, port, opts) when opts |> is_list do
    mongo(
      host: host, port: port, opts: opts,
      active: opts[:mode]!=:passive,
      timeout: opts[:timeout],
      socket: Socket.TCP.connect!(host, port, opts))
  end

  @doc """
  Retreives a repsonce from the MongoDB server (only for passive mode)
  """
  def response(mongo(socket: socket, timeout: timeout, active: false)) do
    case recv({socket, timeout}) do
      {:ok, <<messageLength::[little, signed, size(32)], _::binary>> = message} ->
        complete(messageLength, message, {socket, timeout}) |> Mongo.Response.new
      error -> error
    end
  end

  @doc """
  Completes a possibly partial repsonce from the MongoDB server
  """
  def response(
    <<messageLength::[little, signed, size(32)], _::binary>> = message,
    mongo(socket: socket, timeout: timeout, active: true)) do
    complete(messageLength, message, {socket, timeout}) |> Mongo.Response.new
  end
  defbang response(message, mongo)

  #Receives message from the MongoDB server
  defp recv({socket, timeout}) do
    Socket.Stream.recv(socket, timeout: timeout)
  end
  def recv(_, _, mongo(active: true)) do
    raise Mongo.Error, reason: "Cannot reveive message from an active connection"
  end
  defbang recv(mongo)

  @doc """
  Receives message from MongoDB  
  """
  def send(message, mongo(socket: socket)) do
    Socket.Stream.send(socket, message)
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
    Mongo.Request.adminCmd(mongo, ping: true).send
    case mongo.response do
      {:ok, resp} -> resp.success
      error -> error
    end
  end

  @doc """
  Returns true if connection mode is active
  """
  def active?(mongo(active: mode)), do: mode

  @doc """
  Connects to a specific database
  """
  def db(name, mongo) do
    Mongo.Db.new(mongo, name)
  end

  defp default_host() do
    case :application.get_env(:mongo, :host) do
        {:ok, {host, port}} -> {host, port}
        _                   -> {@host, @port}
    end
  end

  # makes shure response is complete
  defp complete(expected_length, buffer, _host) when size(buffer) == expected_length, do: buffer
  defp complete(expected_length, buffer, _host) when size(buffer) >  expected_length, do: binary_part(buffer, 0, expected_length)
  defp complete(expected_length, buffer, host), do: complete(expected_length, buffer <> Mongo.recv(host), mongo)
end