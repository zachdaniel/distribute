defmodule DistributeTest do
  use ExUnit.Case
  doctest Distribute

  test "simple test" do
    processed =
      [{:a, 1}, {:a, 2}, {:b, 1}, {:b, 2}]
      |> Distribute.distribute(&elem(&1, 0), %{
        a: fn stream -> Stream.map(stream, &{:a, elem(&1, 1), self()}) end,
        b: fn stream -> Stream.map(stream, &{:b, elem(&1, 1), self()}) end
      })
      |> Enum.to_list()

    {as, bs} = Enum.split_with(processed, &(elem(&1, 0) == :a))

    a_pids = as |> Enum.map(&elem(&1, 2)) |> Enum.uniq()
    b_pids = bs |> Enum.map(&elem(&1, 2)) |> Enum.uniq()

    assert length(a_pids) == 1
    assert length(b_pids) == 1

    assert a_pids != b_pids
  end
end
