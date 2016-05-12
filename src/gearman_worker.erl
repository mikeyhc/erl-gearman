%%%-------------------------------------------------------------------
%% @doc
%% A gearman_worker gets a list of functions from the given modules,
%% sets up callbacks to the gearman server, then calls the correct
%% functions whenever the server sends it work
%% @author mikeyhc <mikeyhc@atmosia.net>
%% @version 0.1.0
%% @end
%%%-------------------------------------------------------------------
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

%% @doc
%% starts a linked gearman_worker. Needs a Server to connect to and a
%% list of WorkerModules which have the function functions/0 defined
%% (as this will be called to get a list of worker functions).
%%
%% Every module in WorkerModules needs the following callback defined
%% ?MODULE:function() -> [
%%   {Job :: string(), 
%%    Function :: fun(string(), string(), string()) -> string()
%%    }].
%% @end
start_link(Server, WorkerModules) ->
    Functions = get_functions(WorkerModules),
    gen_fsm:start_link(?MODULE, {self(), Server, WorkerModules, Functions},
                       []).

%% @doc
%% starts an gearman_worker. see start_link/2 for more details on parameters.
%% @end
start(Server, WorkerModules) ->
    Functions = get_functions(WorkerModules),
    gen_fsm:start(?MODULE, {self(), Server, WorkerModules, Functions}, []).

%% @doc
%% stops a running gearman_worker
%% @end
stop(Server) -> Server ! stop.

%%%-------------------------------------------------------------------
%% gen_fsm callbacks
%%%-------------------------------------------------------------------

init({_PidMaster, Server, WorkerModules, Functions}) ->
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

%% handle connection from gearman_connection
handle_info({Connection, connected}, _StateName,
            State=#state{connection=Connection}) ->
    register_functions(Connection, State#state.functions),
    gearman_connection:send_request(Connection, grab_job, {}),
    {next_state, working, State};
%% handle disconnection from gearman_connection
handle_info({Connection, disconnected}, _StateName,
            State=#state{connection=Connection}) ->
    {next_state, dead, State};
%% handle stop from stop/1
handle_info(stop, _StateName, State) ->
    {stop, normal, State};
handle_info(Other, StateName, State) -> ?MODULE:StateName(Other, State).

%%%-------------------------------------------------------------------
%% fsm events
%%%-------------------------------------------------------------------

%% handle job commands
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

%% request a job after sleeping
sleeping(timeout, State=#state{connection=Conn}) ->
    gearman_connection:send_request(Conn, grab_job, {}),
    {next_state, working, State};
%% request a job of noop wakeup
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

%% get all the functions the module provides for callbacks
get_functions(Modules) -> get_functions(Modules, []).

get_functions([], Functions) -> lists:flatten(Functions);
get_functions([Module|Modules], Functions) ->
    get_functions(Modules, lists:merge(Functions, Module:functions())).


%% dispatch to correct callback in list
dispatch_function([], _Func, _Arg, _Handle) ->
    {error, invalid_function};
dispatch_function([{Func, Function}|_], Func, Arg, Handle) ->
    {ok, Function(Handle, Func, Arg)};
dispatch_function([_|Functions], Func, Arg, Handle) ->
    dispatch_function(Functions, Func, Arg, Handle).

%% register functions with the gearman server
register_functions(_Connection, []) -> ok;
register_functions(Connection, [{Name, _Function}|Functions]) ->
    gearman_connection:send_request(Connection, can_do, {Name}),
    register_functions(Connection, Functions).
