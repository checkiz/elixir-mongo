Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Server.Test do
  use ExUnit.Case, async: false

  test "ping" do
    assert :ok == Mongo.connect! |> Mongo.Server.ping
    case Mongo.connect %{port: 27017, timeout: 0} do
      {:ok, localhost} ->
        assert %Mongo.Error{msg: :timeout} == localhost |> Mongo.Server.ping
      _ ->
        {:error, :no_localhost_80}
    end
  end

  test "active mode" do
    mongo = Mongo.connect! %{mode: :active}
    ping_cmd = Mongo.Request.cmd("admin", %{ping: true})
    Mongo.Server.send(mongo, ping_cmd)
    receive do
      {:tcp, _, m} ->
        assert :ok == (case Mongo.Response.new(m) do
          {:ok, resp } -> Mongo.Response.success(resp)
          error -> error
        end)
    end
  end

  test "async request" do
    mongo = Mongo.connect!
    ping_cmd = Mongo.Request.cmd("admin", %{ping: true})
    Mongo.Server.async(mongo)
    Mongo.Server.send(mongo, ping_cmd)
    receive do
      {:tcp, _, m} ->
        assert :ok == (case Mongo.Response.new(m) do
          {:ok, resp } -> Mongo.Response.success(resp)
          error -> error
        end)
    end
  end

  test "async ping" do
    me = self()
    spawn_link(
      fn() ->
        mongo = Mongo.connect! %{mode: :active}
        ping_cmd = Mongo.Request.cmd("admin", %{ping: true})
        Mongo.Server.send(mongo, ping_cmd)
        receive do
          {:tcp, _, m} ->
            send(me, case Mongo.Response.new(m) do
              {:ok, resp } -> Mongo.Response.success(resp)
              error -> error
            end)
        end
      end)
    assert_receive :ok
  end

  test "def connection" do
    assert {:ok, {_, _}} = :application.get_env(:mongo, :host)
  end

  test "chunked messages" do
    db = Mongo.connect! |> Mongo.db("test")
    chunked_test = Mongo.Db.collection(db, "chunked_test")
    chunked_test |> Mongo.Collection.drop
    1..5000 |> Enum.map(&(%{a: &1, value: "this should be long enough"})) |> Mongo.Collection.insert(chunked_test)
    assert 5000 == chunked_test |> Mongo.Collection.find() |> Enum.count
  end

  # # test "write concern" do
  # #   db = Mongo.connect!.db("test")
  # #   db2 = Mongo.connect!.db("test")
  # #   chunked_test = db.collection("wc_test")
  # #   chunked_test.drop
  # #   1..50000 |> Enum.map(&(%{a: &1, value: "this should be long enough"})) |> chunked_test.insert
  # #   assert 50000 > db2.collection("wc_test").find().toArray |> Enum.count
  # #   # assert :ok = db.getLastError
  # #   # assert 5000 == Mongo.connect!.db("test").collection("chunked_test").find().toArray |> Enum.count
  # # end

  test "timout recv" do
    db = Mongo.connect! |> Mongo.db("test")
    timout_test = Mongo.Db.collection(db, "timout_test")
    timout_test |> Mongo.Collection.drop
    1..5000 |>
      Enum.map(&(%{a: &1, value: "this should be long enough"}))
       |> Mongo.Collection.insert(timout_test)
    assert :ok = Mongo.Db.getLastError(db)
    db = Mongo.connect!(%{timeout: 1}) |> Mongo.db("test")
    timout_test = Mongo.Db.collection(db, "timout_test")
    assert %Mongo.Error{msg: :timeout} == timout_test |> Mongo.Collection.find("obj.a == 1 || obj.a == 49000") |> Mongo.Find.exec
  end

end
