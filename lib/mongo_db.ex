defmodule Mongo.Db do
  @moduledoc """
 Module holding operations that can be performed on MongoDB databases
  """
  defrecordp :db, __MODULE__ ,
    dbname: nil,
    mongo: nil
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
  def new(mongo, dbname), do: db(mongo: mongo, dbname: dbname)

  @doc """
  Authenticates a user to a database

  Expects a DB record, a user and a password returns `:ok` or a string containing the error message
  """
  def auth(username, password, db(mongo: mongo)=db) do
    # sysDb = DB.new(mongo: mongo, db: "")
    nonce = getnonce(db)
    hash_password = hash username <> ":mongo:" <> password
    digest = hash nonce <> username <> hash_password
    mongo |> Mongo.Request.cmd(db, authenticate: 1, nonce: nonce, user: username, key: digest).send
    case mongo.response do
      {:ok, resp} -> resp.success
      error -> error
    end
  end

  # creates a md5 hash in hex with loawercase
  defp hash(data) do
    :crypto.hash(:md5, data) |> binary_to_hex
  end

  # creates an hex string from binary
  defp binary_to_hex(bin) do
    bc <<b::4>> inbits bin do
        <<integer_to_binary(b,16)::binary>>
    end |> String.downcase
  end

  # get `nonce` token from server
  defp getnonce(db(mongo: mongo)=db) do
    mongo |> Mongo.Request.cmd(db, getnonce: 1).send
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
  def getLastError(w \\ 0, db(mongo: mongo)=db) do
    mongo |> Mongo.Request.cmd(db, getlasterror: 1, w: w).send
    case mongo.response do
      {:ok, resp} -> resp.error
      error -> error
    end
  end
  defbang getLastError(db)
  defbang getLastError(w, db)

  @doc """
  Returns the previous error status of the preceding operation(s).
  """
  def getPrevError(db(mongo: mongo)=db) do
    mongo |> Mongo.Request.cmd(db, getPrevError: 1).send
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
    case db.cmd(resetError: 1) do
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

end
