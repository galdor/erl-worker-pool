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

start_stop_test() ->
  WorkerSpec = {worker_pool_worker_test, start_link, []},
  {ok, Pool} = worker_pool:start_link({local, test}, WorkerSpec),
  worker_pool:stop(Pool).

timeout_test() ->
  WorkerSpec = {worker_pool_worker_test, start_link, []},
  Opts = #{max_nb_workers => 2,
           request_timeout => 100},
  {ok, Pool} = worker_pool:start_link({local, test}, WorkerSpec, Opts),
  {ok, W1} = worker_pool:acquire(test),
  {ok, W2} = worker_pool:acquire(test),
  {error, timeout} = worker_pool:acquire(test),
  worker_pool:release(test, W1),
  {ok, W3} = worker_pool:acquire(test),
  worker_pool:release(test, W2),
  worker_pool:release(test, W3),
  worker_pool:stop(Pool).

stats_test() ->
  WorkerSpec = {worker_pool_worker_test, start_link, []},
  Opts = #{max_nb_workers => 10,
           request_timeout => 1000},
  {ok, Pool} = worker_pool:start_link({local, test}, WorkerSpec, Opts),
  ?assertEqual(#{nb_workers => 0,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 0},
               worker_pool:stats(test)),
  {ok, W1} = worker_pool:acquire(test),
  ?assertEqual(#{nb_workers => 1,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 1},
               worker_pool:stats(test)),
  {ok, W2} = worker_pool:acquire(test),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 0,
                 nb_busy_workers => 2},
               worker_pool:stats(test)),
  worker_pool:release(test, W1),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 1,
                 nb_busy_workers => 1},
               worker_pool:stats(test)),
  worker_pool:release(test, W2),
  ?assertEqual(#{nb_workers => 2,
                 max_nb_workers => 10,
                 nb_free_workers => 2,
                 nb_busy_workers => 0},
               worker_pool:stats(test)),
  worker_pool:stop(Pool).
