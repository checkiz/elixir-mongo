defmodule Mongo.Db do
  @moduledoc """
 Module holding operations that can be performed on MongoDB databases
  """
  require Record
  Record.defrecordp :db, __MODULE__ ,
    dbname: nil,
    mongo: nil,
    auth: nil,
    opts: %{}
  use Mongo.Helpers

  @doc """
  Gets mongo server hosting the database
  """
  def mongo(db(mongo: mongo)), do: mongo
  @doc """
  Gets dbname
  """
  def name(db(dbname: dbname)), do: dbname

  @doc """
  Creates a db record
  """
  def new(mongo, dbname), do: db(mongo: mongo, dbname: dbname, opts: mongo.db_opts)

  @doc """
  Authenticates a user to a database

  Expects a DB record, a user and a password returns `{:ok, db}` or `{:error, reason}`
  """
  def auth(username, password, db) do
    db(db, auth: {username, hash(username <> ":mongo:" <> password)}).auth
  end
  defbang auth(username, password, db)

  def auth?(db(auth: nil)), do: false
  def auth?(_), do: true

  @doc false
  # Authenticates a user to a database (or do it again after failure)
  def auth(db(auth: nil)=db), do: {:ok, db}
  def auth(db(mongo: mongo, auth: {username, hash_password})=db) do
    nonce = getnonce(db)
    digest = hash nonce <> username <> hash_password
    mongo |> Mongo.Request.cmd(db, %{authenticate: 1}, %{nonce: nonce, user: username, key: digest}).send
    case mongo.response do
      {:ok, resp} ->
        case resp.success do
          :ok ->{:ok, db}
          error -> error
        end
      error -> error
    end
  end

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
  defp getnonce(db(mongo: mongo)=db) do
    mongo |> Mongo.Request.cmd(db, %{getnonce: 1}).send
    case mongo.response do
      {:ok, resp} -> resp.getnonce
      error -> error
    end
  end

  @doc """
  Returns a collection of the database
  """
  def collection(collname, db), do: Mongo.Collection.new(db, collname)

  @doc """
  Returns the error status of the preceding operation.
  """
  def getLastError(db(mongo: mongo)=db) do
    mongo |> Mongo.Request.cmd(db, %{getlasterror: true}).send
    case mongo.response do
      {:ok, resp} -> resp.error
      error -> error
    end
  end
  defbang getLastError(db)

  @doc """
  Returns the previous error status of the preceding operation(s).
  """
  def getPrevError(db(mongo: mongo)=db) do
    mongo |> Mongo.Request.cmd(db, %{getPrevError: 1}).send
    case mongo.response do
      {:ok, resp} -> resp.error
      error -> error
    end
  end
  defbang getPrevError(db)

  @doc """
  Resets error
  """
  def resetError(db) do
    case db.cmd(%{resetError: 1}) do
      {:ok, _} -> :ok
      error -> error
    end
  end
  defbang resetError(db)

  @doc """
  Runs a database command

  Before using this check `Mongo.Collection`, `Mongo.Db` or `Mongo.Server`
  for commands already implemented by these modules
  """
  def cmd(command, db(mongo: mongo)=db) do
    mongo |> Mongo.Request.cmd(db, command).send
    case mongo.response do
      {:ok, resp} -> resp.cmd
      error -> error
    end
  end
  defbang cmd(command, db)

  @doc """
  Kill a cursor of the db
  """
  def kill_cursor(cursorID, db(mongo: mongo)) do
    mongo |> Mongo.Request.kill_cursor(cursorID).send
  end

  @doc """
  Adds options to the database overwriting mongo server connection options

  new_opts must be a keyword with zero or more pairs represeting one of these options:
  
  * read: `:awaitdata`, `:nocursortimeout`, `:slaveok`, `:tailablecursor`
  * write: concern: `:wc`
  * socket: `:mode`, `:timeout`
  """
  def opts(new_opts, db(opts: opts)=db) do
    db(db, opts: Map.merge(opts, new_opts))
  end

  @doc """
  Gets collection default options
  """
  def coll_opts(db(opts: opts)) do
    Map.take(opts, [:awaitdata, :nocursortimeout, :slaveok, :tailablecursor, :wc])
  end

end
