-module(gearman_connection).

-behaviour(gen_server).

%% Public API
-export([start/0, start_link/0, stop/1, connect/2, send_request/3]).
-export([send_response/3]).

%% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-define(DEFAULT_PORT, 4730).
-define(RECONNECT_DELAY, 10000).    %% milliseconds
-define(CONNECT_TIMEOUT, 30000). %% milliseconds

-record(state, {pidparent   :: pid(),
                host        :: string() | null,
                port        :: 1..65535 | null,
                socket      :: any(),       % TODO: tighten this up
                buffer      :: [binary()]
               }).

%%%-------------------------------------------------------------------
%% Public API
%%%-------------------------------------------------------------------

-spec start() -> pid().
start() -> gen_server:start(?MODULE, self(), []).

-spec start_link() -> pid().
start_link() -> gen_server:start_link(?MODULE, self(), []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:cast(Pid, stop).

-spec connect(pid(), string() | {string(), 1..65535}) -> ok.
connect(Pid, {Host, Port}) ->
    gen_server:call(Pid, {connect, Host, Port});
connect(Pid, Host) ->
    connect(Pid, {Host, ?DEFAULT_PORT}).

% TODO: tighten this up
-spec send_request(pid(), any(), any()) -> ok | {error, any()}.
send_request(Pid, Command, Args) ->
    Packet = gearman_protocol:pack_request(Command, Args),
    gen_server:call(Pid, {send_command, Packet}).

% TODO: tighten this up
-spec send_response(pid(), any(), any()) -> ok | {error, any()}.
send_response(Pid, Command, Args) ->
    Packet = gearman_protocol:pack_response(Command, Args),
    gen_server:call(Pid, {send_command, Packet}).

%%%-------------------------------------------------------------------
%% gen_server callbacks
%%%-------------------------------------------------------------------

init(PidParent) -> {ok, #state{pidparent=PidParent}}.

terminate(Reason, #state{socket=Socket}) ->
    io:format("~p stopping: ~p~n", [?MODULE, Reason]),
    case Socket of
        not_connected -> void;
        _             -> gen_tcp:close(Socket)
    end,
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

handle_call({connect, Host, Port}, _From, State) ->
    NewState = State#state{host=Host, port=Port, socket=not_connected},
    {reply, ok, NewState, 0};
handle_call({send_command, Packet}, _From, State) ->
    try gen_tcp:send(State#state.socket, Packet) of
        ok  -> {reply, ok, State};
        Any ->
            io:format("gen_tcp:send returned unhandled value ~p~n", [Any]),
            NewState = disconnect_state(State),
            {reply, {error, Any}, NewState, ?RECONNECT_DELAY}
    catch
        Exc1:Exc2 ->
            io:format("gen_tcp:send raised an exception ~p:~p~n",
                      [Exc1, Exc2]),
            NewState = disconnect_state(State),
            {reply, {error, {Exc1, Exc2}}, NewState, ?RECONNECT_DELAY}
    end.

handle_cast(stop, State) -> {stop, normal, State}.

handle_info(timeout, State=#state{host=Host, port=Port, socket=OldSocket}) ->
    io:format("connecting..."),
    case OldSocket of
        not_connected ->
            case gen_tcp:connect(Host, Port, [binary, {packet, 0}],
                                 ?CONNECT_TIMEOUT) of
                {ok, Socket} ->
                    State#state.pidparent ! {self(), connected},
                    NewState = State#state{socket=Socket},
                    io:format("connected~n"),
                    {noreply, NewState};
                {error, econnrefused} ->
                    io:format("refused~n"),
                    {noreply, State, ?RECONNECT_DELAY};
                {error, timeout} ->
                    io:format("timed out~n"),
                    {noreply, State, ?RECONNECT_DELAY}
            end;
        _ ->
            io:format("Timeout while socket not disconnected: ~p~n", [State]),
            {noreply, State}
    end;
handle_info({tcp, _Socket, NewData}, State) ->
    {ok, NewState} = handle_command(State, NewData),
    {noreply, NewState};
handle_info({tcp_closed, _Socket}, State) ->
    NewState = disconnect_state(State),
    {noreply, NewState, ?RECONNECT_DELAY};
handle_info(Info, State) ->
    io:format("UNHANDLED handle_info ~p ~p~n", [Info, State]),
    {noreply, State}.

%%%-------------------------------------------------------------------
%% Helper Functions
%%%-------------------------------------------------------------------

disconnect_state(State) ->
    State#state.pidparent ! {self(), disconnected},
    gen_tcp:close(State#state.socket),
    State#state{socket=not_connected, buffer=[]}.

handle_command(State, NewData) ->
    Packet = list_to_binary([State#state.buffer, NewData]),
    case gearman_protocol:parse_command(Packet) of
        {error, not_enough_data} -> {ok, State#state{buffer=Packet}};
        {ok, NewPacket, response, Command} ->
            State#state.pidparent ! {self(), command, Command},
            handle_command(State#state{buffer=[]}, NewPacket)
    end.
