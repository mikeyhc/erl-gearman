-module(gearman_worker).

-behaviour(gen_fsm).

%% Public API
-export([start_link/2, start/2, stop/1]).

%% gen_fsm callbacks
-export([init/1, terminate/3, code_change/4]).
-export([handle_event/3, handle_sync_event/4, handle_info/3]).

%% fsm events
-export([working/2, sleeping/2, dead/2]).

%% TODO: type this
-record(state, {connection,
                modules,
                functions
               }).

%%%-------------------------------------------------------------------
%% Public API
%%%-------------------------------------------------------------------

start_link(Server, WorkerModules) ->
    gen_fsm:start_link(?MODULE, {self(), Server, WorkerModules}, []).

start(Server, WorkerModules) ->
    gen_fsm:start(?MODULE, {self(), Server, WorkerModules}, []).

stop(Server) -> Server ! stop.

%%%-------------------------------------------------------------------
%% gen_fsm callbacks
%%%-------------------------------------------------------------------

init({_PidMaster, Server, WorkerModules}) ->
    Functions = get_functions(WorkerModules),
    {ok, Connection} = gearman_connection:start_link(),
    gearman_connection:connect(Connection, Server),
    {ok, dead, #state{connection=Connection, modules=WorkerModules,
                      functions=Functions}}.

terminate(Reason, StateName, _State) ->
    io:format("Worker terminated: ~p [~p]~n", [Reason, StateName]),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

handle_event(Event, StateName, State) ->
    io:format("UNHANDLED event ~p ~p ~p~n", [Event, StateName, State]),
    {stop, {StateName, undefined_event, Event}, State}.

handle_sync_event(Event, From, StateName, State) ->
    io:format("UNHANDLED sync_event ~p ~p ~p ~p~n",
              [Event, From, StateName, State]),
    {stop, {StateName, undefined_event, Event}, State}.

handle_info({Connection, connected}, _StateName,
            State=#state{connection=Connection}) ->
    register_functions(Connection, State#state.functions),
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State};
handle_info({Connection, disconnected}, _StateName,
            State=#state{connection=Connection}) ->
    {next_state, dead, State};
handle_info(stop, _StateName, State) ->
    {stop, normal, State};
handle_info(Other, StateName, State) -> ?MODULE:StateName(Other, State).

%%%-------------------------------------------------------------------
%% fsm events
%%%-------------------------------------------------------------------

working({Conn, command, noop}, State=#state{connection=Conn}) ->
    {next_state, working, State};
working({Conn, command, no_job}, State=#state{connection=Conn}) ->
    gearman_connection:send_request(Conn, pre_sleep, {}),
    {next_state, sleeping, State};
working({Conn, command, {job_assign, Handle, Func, Arg}},
        State=#state{connection=Conn, functions=Functions}) ->
    try dispatch_function(Functions, Func, Arg, Handle) of
        {ok, Result} ->
            gearman_connection:send_request(Conn, work_complete,
                                            {Handle, Result});
        {error, _Reason} ->
            io:format("Unknown function ~p~n", [Func]),
            gearman_connection:send_request(Conn, work_fail, {Handle})
    catch
        Exc1:Exc2 ->
            io:format("Work failed for function ~p: ~p:~p~n~p~n",
                      [Func, Exc1, Exc2, erlang:get_stacktrace()]),
            gearman_connection:send_request(Conn, work_fail, {Handle})
    end,
    gearman_connection:send_request(Conn, grab_job, {}),
    {next_state, working, State}.

sleeping(timeout, State=#state{connection=Conn}) ->
    gearman_connection:send_request(Conn, grab_job, {}),
    {next_state, working, State};
sleeping({Conn, command, noop}, State=#state{connection=Conn}) ->
    gearman_connection:send_request(Conn, grab_job, {}),
    {next_state, working, State}.

dead(Event, State) ->
    io:format("Received unexpected event for state 'dead': ~p ~p~n",
              [Event, State]),
    {next_state, dead, State}.

%%%-------------------------------------------------------------------
%% Helper functions
%%%-------------------------------------------------------------------

get_functions(Modules) -> get_functions(Modules, []).

get_functions([], Functions) -> lists:flatten(Functions);
get_functions([Module|Modules], Functions) ->
    get_functions(Modules, lists:merge(Functions, Module:functions())).


dispatch_function([], _Func, _Arg, _Handle) ->
    {error, invalid_function};
dispatch_function([{Func, Function}|_], Func, Arg, Handle) ->
    {ok, Function(Handle, Func, Arg)};
dispatch_function([_|Functions], Func, Arg, Handle) ->
    dispatch_function(Functions, Func, Arg, Handle).

register_functions(_Connection, []) -> ok;
register_functions(Connection, [{Name, _Function}|Functions]) ->
    gearman_connection:send_request(Connection, can_do, {Name}),
    register_functions(Connection, Functions).
