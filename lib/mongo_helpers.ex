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
      def unquote(to_string(name) <> "!" |> binary_to_atom)(unquote_splicing(args)) do
        case unquote(name)(unquote_splicing(args)) do
          :ok -> :ok
          nil -> nil
          { :ok, result } -> result
          { :error, reason } ->
            raise Mongo.Error, reason: reason, context: unquote(args)
        end
      end
    end
    {:__block__, [], [{:@, [context: Mongo.Helpers, import: Kernel], [{:doc, [], ["See "<>to_string(name)<>"/"<>to_string(args |> length)]}]}|quoted]}
  end
end