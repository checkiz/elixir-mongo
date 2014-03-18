Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Cursor.Test do
  use ExUnit.Case, async: true

  # In order to run the tests a mongodb server must be listening locally on the default port
  setup do
    db = Mongo.connect!.db("test")
    anycoll = db.collection("coll_cursor")
    anycoll.drop
    [
      %{a: 0, value: 0},
      %{a: 1, value: 1},
      %{a: 2, value: 1},
      %{a: 3, value: 1},
      %{a: 4, value: 1},
      %{a: 5, value: 3}
    ]
      |> anycoll.insert
    { :ok, db: db, anycoll: anycoll }
  end

  test "batchSize", ctx do
    assert ctx[:anycoll].find.batchSize(2).toArray |> Enum.count == 6
  end  

  test "batchArray", ctx do
    assert ctx[:anycoll].find.batchSize(2).batchArray |> Enum.count == 3
  end  

  test "explain", ctx do
    assert ctx[:anycoll].find.explain! |> Map.has_key?(:cursor)
  end  

  test "find hint", ctx do
    ctx[:anycoll].createIndex("tst_value", %{value: 1})
    assert ctx[:anycoll].find.hint(%{value: 1}).explain! |> Map.has_key?(:cursor)
  end  

end