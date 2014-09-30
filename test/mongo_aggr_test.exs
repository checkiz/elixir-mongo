Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Aggr.Test do
  use ExUnit.Case, async: false

  # In order to run the tests a mongodb server must be listening locally on the default port
  setup do
    mongo = Mongo.connect!
    db = Mongo.db(mongo, "test")
    anycoll = Mongo.Db.collection(db, "coll_aggr")
    Mongo.Collection.drop anycoll
    [
        %{a: 0, value: 0},
        %{a: 1, value: 1},
        %{a: 2, value: 1},
        %{a: 3, value: 1},
        %{a: 4, value: 1},
        %{a: 5, value: 3} ] |> Mongo.Collection.insert(anycoll)
    { :ok, mongo: mongo, db: db, anycoll: anycoll }
  end

  test "count", ctx do
    if true do
      assert ctx[:anycoll] |> Mongo.Collection.count!(%{value: %{'$gt': 0}}) == 5
    end
  end

  test "distinct", ctx do
    if true do
      assert ctx[:anycoll] |> Mongo.Collection.distinct!("value", %{value: %{"$lt": 3}})
        |> is_list
    end
  end

  test "mapreduce", ctx do
    if true do
      anycoll = ctx[:anycoll]
      assert Mongo.Collection.mr!(anycoll, "function(d){emit(this._id, this.value*2)}", "function(k, vs){return Array.sum(vs)}") |> is_list
      assert :ok == Mongo.Collection.mr!(anycoll, "function(d){emit('z', 3*this.value)}", "function(k, vs){return Array.sum(vs)}", "anycoll2")
    end
  end

  test "group", ctx do
    if true do
      assert ctx[:anycoll] |> Mongo.Collection.group!(%{a: true}) |> is_list
    end
  end

  test "aggregate", ctx do
    if true do
      assert [%{value: 1}|_] = ctx[:anycoll] |> Mongo.Collection.aggregate([
        %{'$skip': 1},
        %{'$limit': 5},
        %{'$project': %{'_id': false, value: true}}
      ])
    end
  end

  test "error count", ctx do
    if true do
      assert %Mongo.Error{} = ctx[:anycoll] |> Mongo.Collection.count(%{value: %{'$in': 0}})
    end
  end

end
