defmodule Distribute do
  @moduledoc """
  Documentation for `Distribute`.
  """

  def distribute(enum, key_fun, stream_funs, opts \\ []) do
    stream_ref = make_ref()

    collect? = Keyword.get(opts, :collect?, true)

    Stream.transform(
      enum,
      fn -> %{} end,
      fn i, pids ->
        stream_id = key_fun.(i)

        case Map.fetch(pids, stream_id) do
          {:ok, {pid, _ref}} ->
            send(pid, {:stream_distribute_push, stream_ref, i})
            {[], pids}

          :error ->
            worker_ref = make_ref()

            pid =
              distributed_worker(
                self(),
                worker_ref,
                stream_ref,
                stream_funs[stream_id] || (& &1),
                collect?
              )

            send(pid, {:stream_distribute_push, stream_ref, i})

            {[], Map.put(pids, stream_id, {pid, worker_ref})}
        end
      end,
      fn pids ->
        if collect? do
          {pids
           |> Stream.flat_map(fn {_, {pid, worker_ref}} ->
             send(pid, {:stream_distribute_stop, stream_ref})

             Stream.resource(
               fn -> nil end,
               fn
                 :stop ->
                   {:halt, nil}

                 nil ->
                   receive do
                     {:stream_distribute_pull, ^worker_ref, ^stream_ref, :item, item} ->
                       {[item], nil}

                     {:stream_distribute_pull, ^worker_ref, ^stream_ref, :done} ->
                       {receive_all_pulls(worker_ref, stream_ref), :stop}
                   after
                     0 ->
                       {[], nil}
                   end
               end,
               fn nil -> :ok end
             )
           end), nil}
        else
          {[], nil}
        end
      end,
      fn _ -> :ok end
    )
  end

  defp distributed_worker(parent, worker_ref, stream_ref, stream_fun, collect?) do
    spawn_link(fn ->
      stream =
        Stream.resource(
          fn -> nil end,
          fn
            :stop ->
              {:halt, nil}

            nil ->
              receive do
                {:stream_distribute_push, ^stream_ref, item} ->
                  {[item], nil}

                {:stream_distribute_stop, ^stream_ref} ->
                  {receive_all_pushes(stream_ref), :stop}
              after
                0 ->
                  {[], nil}
              end
          end,
          fn nil -> :ok end
        )

      stream = stream_fun.(stream)

      if collect? do
        Enum.each(
          stream,
          &send(parent, {:stream_distribute_pull, worker_ref, stream_ref, :item, &1})
        )

        send(parent, {:stream_distribute_pull, worker_ref, stream_ref, :done})
        []
      else
        Stream.run(stream)

        send(parent, {:stream_distribute_pull, worker_ref, stream_ref, :done})
      end
    end)
  end

  defp receive_all_pulls(worker_ref, stream_ref, acc \\ []) do
    receive do
      {:stream_distribute_pull, ^worker_ref, ^stream_ref, :item, item} ->
        [item | acc]
    after
      0 -> acc
    end
  end

  defp receive_all_pushes(stream_ref, acc \\ []) do
    receive do
      {:stream_distribute_push, ^stream_ref, item} ->
        [item | acc]
    after
      0 -> acc
    end
  end
end
