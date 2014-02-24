Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Crud.Test do
  use ExUnit.Case, async: true

  # # In order to run the tests a mongodb server must be listening locally on the default port
  setup do
    db = Mongo.connect.db("test")
    anycoll = db.collection("coll_crud")
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

  test "find", ctx do
    if true do
      # assert ctx[:anycoll].find().stream
      #   |> Enum.count == 6
      assert ctx[:anycoll].find().skip(1).stream
        |> Enum.count == 5
    end
  end

  test "find where", ctx do
    if true do
      assert ctx[:anycoll].find("obj.value == 0").stream
        |> Enum.count == 1
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

  test "objid", ctx do
    if true do
      anycoll = ctx[:anycoll]
      assert [[a: -23], [a: -24, b: 1]] |> Mongo.assign_id |> anycoll.insert |> is_list
    end
  end

  test "bang find", ctx do
    if true do
      assert_raise Mongo.Error, fn -> ctx[:anycoll].find([value: ['$in': 0]]).toArray end
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

end