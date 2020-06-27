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

test_pool(Args, ExtraOpts) ->
  WorkerSpec = {worker_pool_worker_test, Args},
  Opts = maps:merge(#{max_nb_workers => 10,
                      request_timeout => 1000},
                    ExtraOpts),
  {ok, Pool} = worker_pool:start_link({local, worker_pool_test},
                                      WorkerSpec, Opts),
  Pool.

pool_test_() ->
  {foreach,
   fun () ->
       error_logger:tty(false)
   end,
   fun (_) ->
       case whereis(worker_pool_test) of
         undefined ->
           ok;
         Pid ->
           gen_server:stop(Pid)
       end,
       error_logger:tty(true)
   end,
   [fun start_stop/0,
    fun timeout/0,
    fun stats/0,
    fun crash_while_busy/0,
    fun crash_while_free/0]}.

start_stop() ->
  Pool = test_pool([], #{}),
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
                 nb_busy_workers => 0},
               worker_pool:stats(worker_pool_test)),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  ?assertEqual(#{nb_workers => 1,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 1},
               worker_pool:stats(worker_pool_test)),
  {ok, W2} = worker_pool:acquire(worker_pool_test),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 2},
               worker_pool:stats(worker_pool_test)),
  worker_pool:release(worker_pool_test, W1),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 1,
                 nb_busy_workers => 1},
               worker_pool:stats(worker_pool_test)),
  worker_pool:release(worker_pool_test, W2),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 2,
                 nb_busy_workers => 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

crash_while_busy() ->
  Pool = test_pool([], #{max_nb_workers => 10}),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  ?assertEqual(#{nb_workers => 1,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 1},
               worker_pool:stats(worker_pool_test)),
  gen_server:call(W1, die),
  worker_pool:release(worker_pool_test, W1),
  ?assertEqual(#{nb_workers => 0,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).

crash_while_free() ->
  Pool = test_pool([], #{}),
  {ok, W1} = worker_pool:acquire(worker_pool_test),
  worker_pool:release(worker_pool_test, W1),
  ?assertEqual(#{nb_workers => 1,
                 max_nb_workers => 10,
                 nb_free_workers => 1,
                 nb_busy_workers => 0},
               worker_pool:stats(worker_pool_test)),
  gen_server:call(W1, die),
  ?assertEqual(#{nb_workers => 0,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 0},
               worker_pool:stats(worker_pool_test)),
  worker_pool:stop(Pool).
