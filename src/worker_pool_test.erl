%% Copyright (c) 2020 Nicolas Martyanoff <khaelin@gmail.com>.
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
%% REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
%% AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
%% INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
%% LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
%% OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
%% PERFORMANCE OF THIS SOFTWARE.

-module(worker_pool_test).

-include_lib("eunit/include/eunit.hrl").

pool_test_() ->
  {foreach,
   fun () ->
       error_logger:tty(false)
   end,
   fun (_) ->
       try
         worker_pool:stop(worker_pool_test)
       catch
         exit:{normal, _} ->
           ok;
         exit:noproc ->
           ok
       end,
       error_logger:tty(true)
   end,
   [fun start_stop/0,
    fun stop/0,
    fun worker_init_error/0,
    fun with_worker/0,
    fun with_worker_kill/0,
    fun with_worker_crash/0,
    fun timeout/0,
    fun stats/0,
    fun crash_while_busy/0,
    fun crash_while_free/0,
    fun stress/0]}.

start_stop() ->
  Pool = test_pool([], #{}),
  worker_pool:stop(Pool).

stop() ->
  Pool = test_pool([], #{max_nb_workers => 2}),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  {ok, W2} = worker_pool:acquire(worker_pool_test),
  worker_pool:release(worker_pool_test, W2),
  ?assert(is_process_alive(W1)),
  ?assert(is_process_alive(W2)),
  worker_pool:stop(Pool),
  ?assertNot(is_process_alive(W1)),
  ?assertNot(is_process_alive(W2)).

worker_init_error() ->
  Pool = test_pool([{error, test}], #{}),
  ?assertEqual({error, test}, worker_pool:acquire(worker_pool_test)),
  worker_pool:stop(Pool).

with_worker() ->
  Pool = test_pool([], #{max_nb_workers => 10}),
  ?assertEqual(ok, worker_pool:with_worker(worker_pool_test,
                                           fun (_W) -> ok end)),
  ?assertThrow(crash, worker_pool:with_worker(worker_pool_test,
                                              fun (_W) -> throw(crash) end)),
  ?assertExit(crash, worker_pool:with_worker(worker_pool_test,
                                             fun (_W) -> exit(crash) end)),
  ?assertError(crash, worker_pool:with_worker(worker_pool_test,
                                              fun (_W) -> error(crash) end)),
  ?assertMatch(#{nb_workers := 1,
                 nb_free_workers := 1,
                 nb_busy_workers := 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

with_worker_kill() ->
  Pool = test_pool([], #{max_nb_workers => 10}),
  ?assertExit({noproc, {gen_server, call, _}},
              worker_pool:with_worker(worker_pool_test,
                                      fun (W) ->
                                          kill_worker(W),
                                          gen_server:call(W, {echo, foo})
                                      end)),
  ?assertMatch(#{nb_workers := 0,
                 nb_free_workers := 0,
                 nb_busy_workers := 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

with_worker_crash() ->
  Pool = test_pool([], #{max_nb_workers => 10}),
  ?assertExit({{bad_return_value, foo}, {gen_server, call, _}},
              worker_pool:with_worker(worker_pool_test,
                                      fun (W) ->
                                          gen_server:call(W, {throw, foo})
                                      end)),
  ?assertExit({foo, {gen_server, call, _}},
               worker_pool:with_worker(worker_pool_test,
                                       fun (W) ->
                                           gen_server:call(W, {exit, foo})
                                       end)),
  ?assertExit({{foo, _WorkerStack}, _Stack},
               worker_pool:with_worker(worker_pool_test,
                                       fun (W) ->
                                           gen_server:call(W, {error, foo})
                                       end)),
  ?assertMatch(#{nb_workers := 0,
                 nb_free_workers := 0,
                 nb_busy_workers := 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

timeout() ->
  Pool = test_pool([], #{max_nb_workers => 2, request_timeout => 100}),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  {ok, W2} = worker_pool:acquire(worker_pool_test),
  {error, timeout} = worker_pool:acquire(worker_pool_test),
  worker_pool:release(worker_pool_test, W1),
  {ok, W3} = worker_pool:acquire(worker_pool_test),
  worker_pool:release(worker_pool_test, W2),
  worker_pool:release(worker_pool_test, W3),
  worker_pool:stop(Pool).

stats() ->
  Pool = test_pool([], #{max_nb_workers => 10, request_timeout => 1000}),
  ?assertEqual(#{nb_workers => 0,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 0,
                 nb_requests => 0},
               worker_pool:stats(worker_pool_test)),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  ?assertEqual(#{nb_workers => 1,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 1,
                 nb_requests => 0},
               worker_pool:stats(worker_pool_test)),
  {ok, W2} = worker_pool:acquire(worker_pool_test),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 2,
                 nb_requests => 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:release(worker_pool_test, W1),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 1,
                 nb_busy_workers => 1,
                 nb_requests => 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:release(worker_pool_test, W2),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 2,
                 nb_busy_workers => 0,
                 nb_requests => 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

crash_while_busy() ->
  Pool = test_pool([], #{max_nb_workers => 10}),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  ?assertMatch(#{nb_workers := 1,
                 nb_free_workers := 0,
                 nb_busy_workers := 1},
               worker_pool:stats(worker_pool_test)),
  kill_worker(W1),
  worker_pool:release(worker_pool_test, W1),
  ?assertMatch(#{nb_workers := 0,
                 nb_free_workers := 0,
                 nb_busy_workers := 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

crash_while_free() ->
  Pool = test_pool([], #{}),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  worker_pool:release(worker_pool_test, W1),
  ?assertMatch(#{nb_workers := 1,
                 nb_free_workers := 1,
                 nb_busy_workers := 0},
               worker_pool:stats(worker_pool_test)),
  kill_worker(W1),
  ?assertMatch(#{nb_workers := 0,
                 nb_free_workers := 0,
                 nb_busy_workers := 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

stress() ->
  NbWorkers = 10,
  NbClients = 1000,
  Duration = 1000,
  AcquisitionDuration = 50,
  Pool = test_pool([], #{max_nb_workers => NbWorkers}),
  ClientFun = fun F() ->
                  case worker_pool:acquire(worker_pool_test) of
                    {ok, Worker} ->
                      gen_server:call(Worker, {echo, hello}),
                      timer:sleep(AcquisitionDuration),
                      worker_pool:release(worker_pool_test, Worker),
                      F();
                    {error, timeout} ->
                      F()
                  end
              end,
  Clients = [spawn(ClientFun) || _ <- lists:seq(1, NbClients)],
  timer:sleep(Duration),
  lists:foreach(fun (Client) ->
                    monitor(process, Client),
                    exit(Client, stop),
                    receive
                      {'DOWN', _, process, Client, _} ->
                        ok
                    end
                end, Clients),
  worker_pool:stop(Pool).

test_pool(Args, ExtraOptions) ->
  WorkerSpec = {worker_pool_worker_test, Args},
  Options = maps:merge(#{max_nb_workers => 10,
                         request_timeout => 1000},
                       ExtraOptions),
  {ok, Pool} = worker_pool:start_link({local, worker_pool_test},
                                      WorkerSpec, Options),
  Pool.

kill_worker(Worker) ->
    monitor(process, Worker),
    gen_server:call(Worker, die),
    receive
      {'DOWN', _, process, Worker, _} ->
        ok
    end.
