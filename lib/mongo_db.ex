defmodule Mongo.Db do
  @moduledoc """
 Module holding operations that can be performed on MongoDB databases
  """
  defstruct [
    name: nil,
    mongo: nil,
    auth: nil,
    opts: %{} ]

  use Mongo.Helpers

  alias Mongo.Server

  @doc """
  Creates `%Mongo.Db{}` with default options
  """
  def new(mongo, name), do: %Mongo.Db{mongo: mongo, name: name, opts: Server.db_opts(mongo)}

  @doc """
  Authenticates a user to a database

  Expects a DB struct, a user and a password returns `{:ok, db}` or `%Mongo.Error{}`
  """
  def auth(db, username, password) do
    %Mongo.Db{db| auth: {username, hash(username <> ":mongo:" <> password)}} |> auth
  end
  defbang auth(username, password, db)

  @doc """
  Check authentication

  returns true if authentication was performed and succesful
  """
  def auth?(db)
  def auth?(%Mongo.Db{auth: nil}), do: false
  def auth?(_), do: true

  @doc false
  # Authenticates a user to a database (or do it again after failure)
  def auth(%Mongo.Db{auth: nil}=db), do: {:ok, db}
  def auth(%Mongo.Db{auth: {username, hash_password}}=db) do
    nonce = getnonce(db)
    case Mongo.Request.cmd(db, %{authenticate: 1}, %{nonce: nonce, user: username, key: hash(nonce <> username <> hash_password)})
      |> Server.call do
      {:ok, resp} ->
        case resp.success do
          :ok ->{:ok, db}
          error -> error
        end
      error -> error
    end
  end

  @doc """
  Returns a collection struct
  """
  defdelegate collection(db, name), to: Mongo.Collection, as: :new

  @doc """
  Executes a db command requesting imediate response
  """
  def cmd_sync(db, command, cmd_args \\ %{}) do
    case cmd(db, command, cmd_args) do
      {:ok, _reqid} -> Server.response(db.mongo)
      error -> error
    end
  end

  @doc """
  Executes a db command

  Before using this check `Mongo.Collection`, `Mongo.Db` or `Mongo.Server`
  for commands already implemented by these modules
  """
  def cmd(db, cmd, cmd_args \\ %{}) do
    Server.send(db.mongo, Mongo.Request.cmd(db.name, cmd, cmd_args))
  end
  defbang cmd(db, command)

  # creates a md5 hash in hex with loawercase
  defp hash(data) do
    :crypto.hash(:md5, data) |> binary_to_hex
  end

  # creates an hex string from binary
  defp binary_to_hex(bin) do
    for << <<b::4>> <- bin >>, into: <<>> do
        <<Integer.to_string(b,16)::binary>>
    end |> String.downcase
  end

  # get `nonce` token from server
  defp getnonce(db) do
    case cmd_sync(db, %{getnonce: true}) do
      {:ok, resp} -> resp |> Mongo.Response.getnonce
      error -> error
    end
  end

  @doc """
  Returns the error status of the preceding operation.
  """
  def getLastError(db) do
    case cmd_sync(db, %{getlasterror: true}) do
      {:ok, resp} -> resp |> Mongo.Response.error
      error -> error
    end
  end
  defbang getLastError(db)

  @doc """
  Returns the previous error status of the preceding operation(s).
  """
  def getPrevError(db) do
    case cmd_sync(db, %{getPrevError: true}) do
      {:ok, resp} -> resp |> Mongo.Response.error
      error -> error
    end
  end
  defbang getPrevError(db)

  @doc """
  Resets error
  """
  def resetError(db) do
    case cmd(db, %{resetError: true}) do
      {:ok, _} -> :ok
      error -> error
    end
  end
  defbang resetError(db)

  @doc """
  Kill a cursor of the db
  """
  def kill_cursor(db, cursorID) do
    Mongo.Request.kill_cursor(cursorID) |> Server.send(db.mongo)
  end

  @doc """
  Adds options to the database overwriting mongo server connection options

  new_opts must be a map with zero or more of the following keys:

  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(db, new_opts) do
    %Mongo.Db{db| opts: Map.merge(db.opts, new_opts)}
  end

  @doc """
  Gets collection default options
  """
  def coll_opts(db) do
    Map.take(db.opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :wc])
  end

end
