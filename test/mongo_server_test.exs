Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Server.Test do
  use ExUnit.Case, async: true

  test "ping" do
    assert :ok == Mongo.connect.ping
  end
 
  test "active mode" do
    mongo = Mongo.connect(:active)
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
        mongo = Mongo.connect(:active)
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

end