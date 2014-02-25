Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Server.Test do
  use ExUnit.Case, async: true

  test "ping" do
    assert :ok == Mongo.connect.ping
  end
 
  test "active mode" do
    mongo = Mongo.connect(mode: :active)
    mongo |> Mongo.Request.adminCmd(mongo, ping: true).send
    receive do
      {:tcp, _, m} ->
        assert :ok == mongo.response!(m).success
    end
  end
 
  test "async ping" do
    me = self()
    Process.spawn_link(
      fn() ->
        mongo = Mongo.connect(mode: :active)
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
    db = Mongo.connect.db("test")
    anycoll = db.collection("coll_large")
    anycoll.drop
    1..5000 |>
      Enum.map(&([a: &1, value: "this should be long enough"]))
      |> anycoll.insert
    assert 5000 == anycoll.find().toArray |> Enum.count

  end
end