Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Server.Test do
  use ExUnit.Case, async: false

  test "ping" do
    assert :ok == Mongo.connect!.ping
    ping_timeout = 
      case Mongo.connect(port: 80, timeout: 1) do
        {:ok, localhost} ->
          # a Mongo ping on 80 should timout!
          localhost.ping
        _ ->
          #can't test this without localhost:80
          {:error, :no_localhost_80}
      end
    assert ping_timeout == {:error, :timeout}
  end
 
  test "active mode" do
    mongo = Mongo.connect!(mode: :active)
    mongo |> Mongo.Request.adminCmd(mongo, ping: true).send
    receive do
      {:tcp, _, m} ->
        assert :ok == mongo.response!(m).success
    end
  end
 
  test "async request" do
    mongo = Mongo.connect!
    mongo |> Mongo.Request.adminCmd(mongo, ping: true).send true
    receive do
      {:tcp, _, m} ->
        assert :ok == mongo.response!(m).success
    end
  end
 
  test "async ping" do
    me = self()
    Process.spawn_link(
      fn() ->
        mongo = Mongo.connect!(mode: :active)
        mongo |> Mongo.Request.adminCmd(mongo, ping: true).send
        receive do
          {:tcp, _, m} ->
            Process.send me, mongo.response!(m).success
        end
      end)
    assert_receive :ok
  end

  test "def connection" do
    assert {:ok, {_, _}} = :application.get_env(:mongo, :host)
  end

  test "chunked messages" do
    db = Mongo.connect!.db("test")
    chunked_test = db.collection("chunked_test")
    chunked_test.drop
    1..5000 |> Enum.map(&([a: &1, value: "this should be long enough"])) |> chunked_test.insert
    # db = Mongo.connect!.db("test")
    # chunked_test = db.collection("chunked_test")
    assert 5000 == chunked_test.find().toArray |> Enum.count
  end

  test "write concern" do
    db = Mongo.connect!.db("test")
    chunked_test = db.collection("chunked_test")
    chunked_test.drop
    1..5000 |> Enum.map(&([a: &1, value: "this should be long enough"])) |> chunked_test.insert
    assert 5000 > Mongo.connect!.db("test").collection("chunked_test").find().toArray |> Enum.count
    assert :ok = db.getLastError
    assert 5000 == Mongo.connect!.db("test").collection("chunked_test").find().toArray |> Enum.count
  end

  test "timout recv" do
    db = Mongo.connect!.db("test")
    timout_test = db.collection("timout_test")
    timout_test.drop
    1..5000 |>
      Enum.map(&([a: &1, value: "this should be long enough"]))
      |> timout_test.insert
    assert :ok = db.getLastError
    db = Mongo.connect!(timeout: 1).db("test")
    timout_test = db.collection("timout_test")
    assert_raise Mongo.Error, fn -> Enum.count(timout_test.find("obj.a == 1 || obj.a == 49000").toArray) end
  end
end