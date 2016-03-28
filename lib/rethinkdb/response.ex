defmodule RethinkDB.Record do
  @moduledoc false
  defstruct data: "", profile: nil
end

defmodule RethinkDB.Collection do
  @moduledoc false
  defstruct data: [], profile: nil

  defimpl Enumerable, for: __MODULE__ do
    def reduce(%{data: data}, acc, fun) do
      Enumerable.reduce(data, acc, fun)
    end

    def count(%{data: data}), do: Enumerable.count(data)
    def member?(%{data: data}, el), do: Enumerable.member?(data, el)
  end
end

defmodule RethinkDB.Feed do
  @moduledoc false
  defstruct token: nil, data: nil, pid: nil, note: nil, profile: nil

  defimpl Enumerable, for: __MODULE__ do
    def reduce(changes, acc, fun) do
      stream = Stream.unfold(changes, fn
        x = %RethinkDB.Feed{data: []} ->
          r = RethinkDB.next(x)
          {r, struct(r, data: [])}
        x = %RethinkDB.Feed{} ->
          {x, struct(x, data: [])}
        x = %RethinkDB.Collection{} -> {x, nil}
        nil -> nil
      end) |> Stream.flat_map(fn (el) ->
        el.data
      end)
      stream.(acc, fun)
    end
    def count(_changes), do: raise "count/1 not supported for changes"
    def member?(_changes, _values), do: raise "member/2 not supported for changes"
  end
end

defmodule RethinkDB.Response do
  @moduledoc false
  defstruct token: nil, data: "", profile: nil

  def parse(raw_data, token, pid) do
    d = Poison.decode!(raw_data)
    r = RethinkDB.Pseudotypes.convert_reql_pseudotypes(d["r"])
    profile = d["p"]
    type = d["t"]

    resp = case type do
      1  -> {:ok, hd(r)}
      2  -> {:ok, r}
      3  -> case d["n"] do
          [2] -> {:ok, %RethinkDB.Feed{token: token, data: hd(r), pid: pid, note: d["n"]}}
           _  -> {:ok, %RethinkDB.Feed{token: token, data: r, pid: pid, note: d["n"]}}
        end
      4 -> {:ok, :noreply}
      5 -> {:ok, hd(r)}
      16 -> {:error, r}
      17 -> {:error, r}
      18 -> {:error, r}
    end

    maybe_add_profile(profile, resp)
  end

  defp maybe_add_profile(resp, nil), do: resp
  defp maybe_add_profile({status, resp}, profile) do
    resp = case resp do
      %RethinkDB.Feed{} = struct ->
        %{feed | :profile => profile}
      %RethinkDB.Response{} = struct ->
        %{struct | :profile => profile}
      _ ->
        %{data: resp, profile: profile}
    end

    {status, resp}
  end
end
