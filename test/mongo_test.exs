Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Test do
  use ExUnit.Case, async: true

  test "ping", do: assert :ok == Mongo.connect.ping
  # # In order to run the tests a mongodb server must be listening locally on the default port
  setup do
    db = Mongo.connect.db("test")
    anycoll = db.collection("anycoll")
    anycoll.drop
    [
      [a: 0, value: 0],
      [a: 1, value: 1],
      [a: 2, value: 1],
      [a: 3, value: 1],
      [a: 4, value: 1],
      [a: 5, value: 3]
    ]
      |> anycoll.insert
    { :ok, db: db, anycoll: anycoll }
  end

  test "count", ctx do
    if true do
      assert ctx[:anycoll].count!([value: ['$gt': 0]]) == 5
    end
  end

  test "distinct", ctx do
    if true do
      assert ctx[:anycoll].distinct!("value", [value: ["$lt": 3]])
        |> is_list
    end
  end

  test "mapreduce", ctx do
    if true do
      anycoll = ctx[:anycoll]
      assert anycoll.mr!("function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}") |> is_list
      assert :ok == anycoll.mr!("function(d){emit('z', 3*this.value)}", "function(k, vs){return Array.sum(vs)}", "anycoll2")
    end
  end

  test "group", ctx do
    if true do
      assert ctx[:anycoll].group!(a: true) |> is_list
    end
  end

  test "find", ctx do
    if true do
      # assert ctx[:anycoll].find().stream
      #   |> Enum.count == 6
      # assert ctx[:anycoll].find().skip(1).stream
      #   |> Enum.count == 5
      assert ctx[:anycoll].find().stream
      # assert ctx[:anycoll].find().batchSize(2).stream
        |> Enum.count == 6
    end
  end

  test "insert", ctx do
    anycoll = ctx[:anycoll]
    if true do
      assert [a: 23] |> anycoll.insert == [a: 23]
      assert [[a: 23], [a: 24, b: 1]] |> anycoll.insert |> is_list
    end
    if true do
      assert ['_id': 2, a: 456] |> anycoll.insert |> Keyword.keyword?
      assert ctx[:db].getLastError == :ok
    end
  end

  test "update", ctx do
    if true do
      ctx[:anycoll].update([a: 456], [a: 123, b: 789])
      assert ctx[:db].getLastError == :ok
    end
  end

  test "delete", ctx do
    if true do
      ctx[:anycoll].delete([b: 789])
      assert ctx[:db].getLastError == :ok
    end
  end

  test "aggregate", ctx do
    if true do
      assert [[value: 1]|_] = ctx[:anycoll].aggregate(skip: 1, limit: 5, project: ['_id': false, value: true])
    end
  end

  test "drop", ctx do
    if true do
      anycoll = ctx[:anycoll]
      assert anycoll.count! > 0
      assert anycoll.drop! == :ok
      assert anycoll.count! == 0
    end
  end

  test "objid", ctx do
    if true do
      anycoll = ctx[:anycoll]
      assert [[a: -23], [a: -24, b: 1]] |> Mongo.assign_id |> anycoll.insert |> is_list
    end
  end


  test "def connection" do
    assert {:ok, {_, _}} = :application.get_env(:mongo, :host)
  end

  test "bang", ctx do
    if true do
      assert_raise Mongo.Error, fn -> ctx[:anycoll].find([value: ['$in': 0]]).toArray end
      assert_raise Mongo.Error, fn -> ctx[:anycoll].count!([value: ['$in': 0]]) end
    end
  end

  test "error", ctx do
    if true do
      assert {:error, _} = ctx[:anycoll].count([value: ['$in': 0]])
    end
  end

  test "mongohq" do
    # prior to runing this test, add user `testuser` with pwd `123` to db `test`
    if false do
      # assert Mongo.connect("test") |> Mongo.auth("testuser", "123")  == :ok
      # assert Mongo.connect("test") |> Mongo.auth("testuser", "124")  != :ok
      # assert Mongo.connect("test") |> Mongo.auth("uabc", "123")      != :ok
      # assert Mongo.connect("testZ") |> Mongo.auth("testuser", "123") != :ok
    end
  end

  test "insert error", ctx do
    anycoll = ctx[:anycoll]
    if true do
      [_id: 1, a: 31] |> anycoll.insert
      [_id: 1, a: 32] |> anycoll.insert
      assert {:error, _} = ctx[:db].getLastError
    end
  end

  test "batchSize", ctx do
    assert ctx[:anycoll].find.batchSize(2).toArray |> Enum.count == 6
  end  

  test "batchArray", ctx do
    assert ctx[:anycoll].find.batchSize(2).batchArray |> Enum.count == 3
  end  

  test "explain", ctx do
    assert ctx[:anycoll].find.explain! |> Keyword.has_key?(:cursor)
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
end