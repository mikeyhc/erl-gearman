%%%-------------------------------------------------------------------
%% @doc
%% A module for creating a parsing the binary gearman protocol
%% @author mikeyhc <mikeyhc@atmosia.net>
%% @version 0.1.0
%% @end
%%%-------------------------------------------------------------------
-module(gearman_protocol).

-export([parse_command/1, pack_request/2, pack_response/2]).

-type type() :: request | response.

%% @doc
%% parses a command, returning the remaining data, the type of
%% the command (request or reply) and the command with its
%% arguments
%% @end
-spec parse_command(binary()) -> {error, not_enough_data}
                               | {ok, binary(), type(), atom() }
                               | {ok, binary(), type(), tuple() }.
parse_command(<<"\000RE", TypeChar:8, CmdID:32/big, Length:32/big,
                Rest/binary>>) ->
    if  size(Rest) >= Length ->
           Type = case TypeChar of
                      $Q -> request;
                      $S -> response
                  end,
           {Args, Rem} = split_binary(Rest, Length),
           Cmd = parse_command(CmdID, binary:bin_to_list(Args)),
           {ok, Rem, Type, Cmd};
        true -> {error, not_enough_data}
    end;
parse_command(_) -> {error, not_enough_data}.

%% @doc
%% @private
%% parses a command ID and the data to extract the arguments
%% from the data
%% @end
parse_command(CmdID, Data) ->
    case CmdID of
        1   -> {can_do, Data};          % function
        2   -> {cant_do, Data};         % function
        3   -> reset_abilities;
        4   -> pre_sleep;
        % 5 unused
        6   -> noop;
        7   ->
            [Function, Unique, Arg] = split(Data, 0, 2),
            {job_assign, Function, Unique, Arg};
        8   -> {job_created, Data};
        9   -> grab_job;
        10  -> no_job;
        11  ->
            [Handle, Function, Arg] = split(Data, 0, 2),
            {job_assign, Handle, Function, Arg};
        12  ->
            [Handle, Num, Den] = split(Data, 0, 2),
            {work_status, Handle, list_to_integer(Num), list_to_integer(Den)};
        13  ->
            [Handle, Result] = split(Data, 0, 1),
            {work_complete, Handle, Result};
        14  -> {work_fail, Data};       % handle
        15  -> {get_status, Data};      % handle
        16  -> {echo_req, Data};        % text
        17  -> {echo_res, Data};        % text
        18  ->
            [Function, Unique, Arg] = split(Data, 0, 2),
            {submit_job_bg, Function, Unique, Arg};
        19  ->
            [Code, Text] = split(Data, 0, 1),
            {error, list_to_integer(Code), Text};
        20  ->
            [Handle, Known, Running, Num, Den] = split(Data, 0, 4),
            {status_res, Handle, Known, Running, list_to_integer(Num),
             list_to_integer(Den)};
        21  ->
            [Function, Unique, Arg] = split(Data, 0, 2),
            {submit_job_high, Function, Unique, Arg};
        22  -> {set_client_id, Data};   % client id
        23  ->
            [Function, Timeout] = split(Data, 0, 1),
            {can_do_timeout, Function, list_to_integer(Timeout)};
        24  -> all_yours
        % TODO: implement 25-36
    end.

%% @doc
%% given a command and its arguments it packs it into the gearman
%% request binary format
%% @end
-spec pack_request(atom(), tuple()) -> tuple().
pack_request(Cmd, Args) ->
    {CmdID, ArgList} = pack_command(Cmd, Args),
    pack_command(CmdID, ArgList, "\000REQ").

%% @doc
%% given a command and its arguments it packs it into the gearman
%% response binary format
%% @end
-spec pack_response(atom(), tuple()) -> tuple().
pack_response(Cmd, Args) ->
    {CmdID, ArgList} = pack_command(Cmd, Args),
    pack_command(CmdID, ArgList, "\000RES").

%% @doc
%% @private
%% given a command ID, arguments a type (Magic) it will pack it into
%% the gearman binary format
%% @end
pack_command(CmdID, Args, Magic) ->
    Data = list_to_binary(string:join(Args, [0])),
    Length = size(Data),
    list_to_binary([Magic, <<CmdID:32/big, Length:32/big>>, Data]).

%% @doc
%% @private
%% given the command ID and args it returns the ID and arguments in a
%% list format
%% @end
pack_command(can_do, {Fun}) -> {1, [Fun]};
pack_command(cant_do, {Fun}) -> {2, [Fun]};
pack_command(reset_abilities, {}) -> {3, []};
pack_command(pre_sleep, {}) -> {4, []};
% 5 unused
pack_command(noop, {}) -> {6, []};
pack_command(submit_job, {Func, Uniq, Arg}) -> {7, [Func, Uniq, Arg]};
pack_command(job_created, {Handle}) -> {8, [Handle]};
pack_command(grab_job, {}) -> {9, []};
pack_command(no_job, {}) -> {10, []};
pack_command(job_assign, {Handle, Func, Arg}) -> {11, [Handle, Func, Arg]};
pack_command(work_status, {Handle, Num, Den}) -> {12, [Handle, Num, Den]};
pack_command(work_complete, {Handle, Result}) -> {13, [Handle, Result]};
pack_command(work_fail, {Handle}) -> {14, [Handle]};
pack_command(get_status, {Handle}) -> {15, [Handle]};
pack_command(echo_req, {Text}) -> {16, [Text]};
pack_command(echo_res, {Text}) -> {17, [Text]};
pack_command(submit_job_bg, {Func, Uniq, Arg}) -> {18, [Func, Uniq, Arg]};
pack_command(error, {Code, Text}) -> {19, [integer_to_list(Code), Text]};
pack_command(status_res, {Handle, Known, Running, Num, Den}) ->
    {20, [Handle, Known, Running, list_to_integer(Num), integer_to_list(Den)]};
pack_command(submit_job_high, {Func, Uniq, Arg}) -> {21, [Func, Uniq, Arg]};
pack_command(set_client_id, {ClientID}) -> {22, [ClientID]};
pack_command(can_do_timeout, {Func, Timeout}) ->
    {23, [Func, integer_to_list(Timeout)]};
pack_command(all_yours, {}) -> {24, []}.
% TODO: implement 25-36

%% splits a List into at most Count parts, using Seperator as a delimiter.
split(List, Seperator, Count) ->
    split(List, Seperator, [], [], Count).

split([], _, [], [], _) -> [];
split([], _, Lists, Current, _) -> lists:reverse([Current|Lists]);
split(List, _, Lists, [], 0) -> lists:reverse([List|Lists]);
split([Sep|Rest], Sep, Lists, Current, Count) ->
    split(Rest, Sep, [lists:reverse(Current)|Lists], [], Count - 1);
split([Other|Rest], Sep, Lists, Current, Count) ->
    split(Rest, Sep, Lists, [Other|Current], Count).
