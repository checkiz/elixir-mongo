defmodule Mongo.Helpers do
  @moduledoc "Helper macros"
  defmacro __using__(_opts) do
    quote do
      import Mongo.Helpers
    end
  end

  @doc """
  Helps defining a function like `count!` calling function `count`

  When `count` returns `{:ok, value}`, `count!` returns `value`

  When `count` returns `{:error, reason}`, `count!` raises an exception
  """
  defmacro defbang({ name, _, args }) do
    unless args |> is_list do
      args = []
    end

    {:__block__, [], quoted} =
    quote bind_quoted: [name: Macro.escape(name), args: Macro.escape(args)] do
      def unquote(to_string(name) <> "!" |> String.to_atom)(unquote_splicing(args)) do
        case unquote(name)(unquote_splicing(args)) do
          :ok -> :ok
          nil -> nil
          { :ok, result } -> result
          { :error, reason } -> raise Mongo.Bang, msg: reason, acc: unquote(args)
          %{msg: msg, acc: acc}=err -> raise Mongo.Bang, msg: msg, acc: acc
        end
      end
    end
    {:__block__, [], [{:@, [context: Mongo.Helpers, import: Kernel], [{:doc, [], ["See "<>to_string(name)<>"/"<>to_string(args |> length)]}]}|quoted]}
  end

  @doc """
  Feeds sample data into a collection of database `test`
  """
  def test_collection(collname) do
    mongo = Mongo.connect!
    db = Mongo.db(mongo, "test")
    collection = Mongo.Db.collection(db, collname)
    Mongo.Collection.drop collection
    [
        %{a: 0, value: 0},
        %{a: 1, value: 1},
        %{a: 2, value: 1},
        %{a: 3, value: 1},
        %{a: 4, value: 1},
        %{a: 5, value: 3} ] |> Mongo.Collection.insert(collection)
    collection
  end
end
