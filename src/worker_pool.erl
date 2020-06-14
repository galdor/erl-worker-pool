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

-module(worker_pool).

-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

-export([default_options/0, start_link/2, start_link/3, stop/1,
         stats/1, acquire/1, release/2,
         init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([options/0, pool_name/0, pool_ref/0,
              worker/0, worker_spec/0, stats/0]).

-type options() :: #{max_nb_workers => pos_integer(),
                     request_timeout => pos_integer()}.

-type pool_name() :: {local, term()} | {global, term()} | {via, atom(), term()}.
-type pool_ref() :: term() | {term(), atom()} | {global, term()}
                  | {via, atom(), term()} | pid().

-type worker() :: pid().
-type worker_spec() :: {module(), Fun :: atom(), Args :: list()}.

-type stats() :: #{nb_workers := non_neg_integer(),
                   max_nb_workers := pos_integer(),
                   nb_free_workers := non_neg_integer(),
                   nb_busy_workers := non_neg_integer()}.

-type request() :: {From :: {pid(), term()}, timer:tref()}.

-record(state, {worker_spec :: worker_spec(),
                options = #{} :: options(),
                free_workers = [] :: [pid()],
                busy_workers = [] :: [pid()],
                requests :: queue:queue(request())}).

-spec default_options() -> options().
default_options() ->
  #{max_nb_workers => 10,
    request_timeout => 1000}.

-spec start_link(pool_name(), worker_spec()) -> Result when
    Result :: {ok, pid()} | ignore | {error, term()}.
start_link(Name, WorkerSpec) ->
  start_link(Name, WorkerSpec, default_options()).

-spec start_link(pool_name(), worker_spec(), options()) -> Result when
    Result :: {ok, pid()} | ignore | {error, term()}.
start_link(Name, WorkerSpec, Opts) ->
  gen_server:start_link(Name, ?MODULE, [WorkerSpec, Opts], []).

-spec stop(pool_ref()) -> ok.
stop(PoolRef) ->
  gen_server:stop(PoolRef).

-spec stats(pool_ref()) -> stats().
stats(PoolRef) ->
  gen_server:call(PoolRef, stats).

-spec acquire(pool_ref()) -> {ok, worker()} | {error, term()}.
acquire(PoolRef) ->
  gen_server:call(PoolRef, acquire).

-spec release(pool_ref(), worker()) -> ok.
release(PoolRef, Worker) ->
  gen_server:call(PoolRef, {release, Worker}).

init([WorkerSpec, Opts]) ->
  State = #state{worker_spec = WorkerSpec,
                 options = Opts,
                 requests = queue:new()},
  {ok, State}.

handle_call(acquire, _From,
            State = #state{free_workers = [Worker | FreeWorkers],
                           busy_workers = BusyWorkers}) ->
  State2 = State#state{free_workers = FreeWorkers,
                       busy_workers = [Worker | BusyWorkers]},
  {reply, {ok, Worker}, State2};
handle_call(acquire, _From,
            State = #state{worker_spec = {M, F, A},
                           options = #{max_nb_workers := MaxNbWorkers},
                           free_workers = [],
                           busy_workers = BusyWorkers}) when
    length(BusyWorkers) < MaxNbWorkers ->
  Worker = spawn_link(M, F, [A]),
  State2 = State#state{busy_workers = [Worker | BusyWorkers]},
  {reply, {ok, Worker}, State2};
handle_call(acquire, From, State = #state{free_workers = [],
                                          requests = Requests}) ->
  #{request_timeout := Delay} = State#state.options,
  {ok, Timer} = timer:send_after(Delay, {expire_request, From}),
  State2 = State#state{requests = queue:in({From, Timer}, Requests)},
  {noreply, State2};

handle_call({release, Worker}, _From, State) ->
  Pred = fun (W) -> W == Worker end,
  {[_], BusyWorkers2} = lists:partition(Pred, State#state.busy_workers),
  State2 = State#state{free_workers = [Worker | State#state.free_workers],
                       busy_workers = BusyWorkers2},
  State3 = case queue:out(State#state.requests) of
    {{value, {AFrom, Timer}}, Requests2} ->
      timer:cancel(Timer),
      gen_server:reply(AFrom, {ok, Worker}),
      State2#state{requests = Requests2};
    {empty, _} ->
      State2
  end,
  {reply, ok, State3};

handle_call(stats, _From, State) ->
  #{max_nb_workers := MaxNbWorkers} = State#state.options,
  NbFreeWorkers = length(State#state.free_workers),
  NbBusyWorkers = length(State#state.busy_workers),
  Stats = #{nb_workers => NbFreeWorkers + NbBusyWorkers,
            max_nb_workers => MaxNbWorkers,
            nb_free_workers => NbFreeWorkers,
            nb_busy_workers => NbBusyWorkers},
  {reply, Stats, State};

handle_call(Request, From, State) ->
  ?LOG_WARNING("unhandled call ~p from ~p~n", [Request, From]),
  {noreply, State}.

handle_cast(Request, State) ->
  ?LOG_WARNING("unhandled cast ~p~n", [Request]),
  {noreply, State}.

handle_info({expire_request, From}, State) ->
  Fun = fun ({AFrom, Timer}) ->
            case AFrom == From of
              true ->
                timer:cancel(Timer),
                gen_server:reply(AFrom, {error, timeout}),
                false;
              false ->
                true
            end
        end,
  Requests2 = queue:filter(Fun, State#state.requests),
  State2 = State#state{requests = Requests2},
  {noreply, State2};

handle_info(Info, _State) ->
  ?LOG_WARNING("unhandled info ~p~n", [Info]).
