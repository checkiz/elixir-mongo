Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Test do
  use ExUnit.Case, async: true

  # In order to run the test a mongodb server must be listening locally on the default port
  setup_all do
    mongo = Mongo.mongo |> Mongo.connect("test")
    Mongo.remove(mongo, "anycoll", {})
    [
      [a: 0, value: 0],
      [a: 1, value: 1],
      [a: 2, value: 1],
      [a: 3, value: 1],
      [a: 4, value: 1],
      [a: 5, value: 3]
    ]
      |> Mongo.insert(mongo, "anycoll")
    { :ok, mongo: mongo }
  end

  test "count", ctx do
    assert ctx[:mongo]
      |> Mongo.count("anycoll", [value: ['$gt': 0]]) == 5
  end

  test "distinct", ctx do
    assert ctx[:mongo]
      |> Mongo.distinct("anycoll", "value", [value: ["$lt": 3]])
      |> is_list
  end

  test "mapreduce", ctx do
    mongo = ctx[:mongo]
    assert Mongo.mr(mongo, "anycoll", "function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}") |> is_list
    assert :ok == Mongo.mr(mongo, "anycoll", "function(d){emit('z', 3*this.value)}", "function(k, vs){return Array.sum(vs)}", "anycoll2")
  end

  test "group", ctx do
    assert ctx[:mongo]
      |> Mongo.group("anycoll", a: true) |> is_list
  end

  test "find", ctx do
    assert ctx[:mongo]
      |> Mongo.find("anycoll", ['$maxScan': 2, '$skip': 0])
      |> Stream.map(fn {bsonbuffer, part} -> Bson.decode(part, bsonbuffer) end) |> Enum.count >= 6
  end

  test "insert", ctx do
    mongo = ctx[:mongo]
    if false do
      assert [a: 23] |> Mongo.insert(mongo, "anycoll") == [a: 23]
      assert [[a: 23], [a: 24, b: 1]] |> Mongo.insert(mongo, "anycoll") |> is_list
    end
    if false do
      assert ['_id': 2, a: 456] |> Mongo.insert(mongo, "anycoll") |> Keyword.keyword?
      assert Mongo.getlasterror(mongo) == :ok
    end
  end

  test "update", ctx do
    mongo = ctx[:mongo]
    if false do
      Mongo.update(mongo, "anycoll", [a: 456], [a: 123, b: 789])
      assert Mongo.getlasterror(mongo) == :ok
    end
  end

  test "delete", ctx do
    mongo = ctx[:mongo]
    if false do
      Mongo.remove(mongo, "anycoll", [b: 789])
      assert Mongo.getlasterror(mongo) == :ok
    end
  end

  test "aggregate", ctx do
    mongo = ctx[:mongo]
    if true do
      assert [[value: 1]|_] = Mongo.aggregate(mongo, "anycoll", skip: 1, limit: 5, project: ['_id': false, value: true])
    end
  end

  test "objid", ctx do
    mongo = ctx[:mongo]
    if true do
      assert [[a: -23], [a: -24, b: 1]] |> Mongo.assign_id |> Mongo.insert(mongo, "anycoll") |> is_list
    end
  end

end