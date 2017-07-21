%%
%% domain for resolve requests queue
%%

-module(ecldb_domain).

-behaviour(gen_server).

-include("../include/ecldb.hrl").

-export([start_link/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

% MISC
-export([
    start/2, stop/2,
    resolve/3,
    reg/1, unreg/1,
    is_node_in_domains/2,
    gen_key/1,
    ping/1,
    state/1,
    workers_list/1, workers_num/1
  ]).

-define(TIMEOUT, 3000).
-define(ttl(Now, Until), case Until - Now of Ttl when Ttl > 0 -> Ttl; _ -> 0 end).  



%%
-spec start(C::atom(), D::atom()) -> ok|err().
start(C, D) when is_atom(D) ->
  case erlang:whereis(D) of
    undefined -> ecldb_domain_sup:start_domain(C,D);
    _ -> ?e(already_exists)
  end;
start(C, #{node := Node, name := Name})  ->
  case Node == node() of
    true ->
      case erlang:whereis(Name) of
        undefined -> ecldb_domain_sup:start_domain(C,Name);
        _ -> ?e(already_exists)
      end;
    false -> ?e(wrong_node)
  end.

%%
-spec stop(C::atom(), D::atom()) -> ok|err().
stop(C, D) when is_atom(C), is_atom(D) ->
  ecldb_domain_sup:stop_domain(C, D).





start_link(ClusterName, DomainName) ->
  Args = #{name => DomainName, cluster => ClusterName},
  gen_server:start_link({local, DomainName}, ?MODULE, Args, []).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen Server api
handle_info({'EXIT', Pid, Reason}, S)  -> exit_signal(S, Pid, Reason); 
handle_info(timeout, S)                -> timeout(S);
handle_info(Msg, S = #{out := O})      -> io:format("Unk msg ~p~n", [Msg]), {noreply, S, out_ttl(O, ?mnow)}.
code_change(_Old,S = #{out := O}, _Ex) -> {ok, S, out_ttl(O, ?mnow)}.

%% casts
handle_cast({queue_manage_, Key}, S)   -> queue_manage_(S, Key);
handle_cast({req, Key, Opts, F, T}, S) -> req_(S, F, Key, Opts, T);
handle_cast(Req, S = #{out := O})      -> ?INF("unknown_msg", Req), {noreply, S, out_ttl(O, ?mnow)}.

%% calls
handle_call(ping,  _F,S = #{out := O}) -> {reply, pong, S, out_ttl(O, ?mnow)};
handle_call(state, _F,S = #{out := O}) -> {reply, S, S, out_ttl(O, ?mnow)};
handle_call(workers_list, _F,S)        -> workers_list_(S);
handle_call(workers_num, _F,S)         -> workers_num_(S);
handle_call({req, Key, Opts, T}, F, S) -> req_(S, F, Key, Opts, T);
handle_call({reg, Key, From, R},_F, S) -> reg_(S, Key, From, R);
handle_call({unreg, Key, Pid},_F, S)   -> unreg_(S, Key, Pid);
handle_call(Req, _F, S = #{out := O})  -> {reply, {err, unknown_command, ?p(Req)}, S, out_ttl(O, ?mnow)}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%
init(#{name := Name, cluster := ClusterName}) ->
  {ok, ClusterSrvPid} = ecldb:get_srv_pid(ClusterName),
  {ok, Ets}           = gen_server:call(ClusterSrvPid, ets),
  S = #{
    name    => Name,
    cluster => ClusterName,
    ets     => Ets,
    q       => #{},           %% Queues
    out     => ordsets:new(), %% Timeout ordered set
    c       => orddict:new()     %% Childs {Key, Pid}
  }, 
  process_flag(trap_exit, true),
  {ok, S}.

%
terminate(_Reason, _S) -> 
  ok.

state(DomainName) -> 
  gen_server:call(DomainName, state).

workers_list(#{name := Name, node := Node}) ->
  gen_server:call({Name, Node}, workers_list).
workers_list_(S = #{out := O, c := Childs}) ->
  {reply, Childs, S, out_ttl(O, ?mnow)}.
  
%%
workers_num(#{name := Name, node := Node}) ->
  gen_server:call({Name, Node}, workers_num).
workers_num_(S = #{out := O, c := Childs}) ->
  {reply, length(Childs), S, out_ttl(O, ?mnow)}.


% If child stops
exit_signal(S = #{c := Childs, out := Out}, Pid, _Reason) ->
  ?INF("Exit", {Pid, _Reason}),
  {noreply, S#{c := lists:keydelete(Pid, 2, Childs)}, out_ttl(Out, ?mnow)}.


timeout(S = #{out := Out}) ->
  Now = ?mnow,
  OutFun = fun
    (Fu, [{Until, From}|RestOut]) when Now >= Until ->
      gen_server:reply(From, ?e(timeout)),
      Fu(Fu, RestOut);
    (_F, RestOut) -> {RestOut, out_ttl(RestOut, Now)}
  end,
  {NewOut, Ttl} = OutFun(OutFun, Out), 

  {noreply, S#{out := NewOut}, Ttl}.






%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Resolve pid by route
%-spec resolve(Key::binary, 
%              Route::{norma, D::domain()}|
%                     {proxy, DNew::domain(), DOld::domain()}|
%                     {proxy, DOld::domain()}|
%                     {merge, DNew::domain(), DOld::domain()}|
%                     {merge, DOld::domain()},
%              Opts::map()) -> {ok, Pid::pid()}|not_started|not_exists|err().
%
%% Norma resolve by main domain
resolve(Key, {norma, D}, Opts = #{mode := _}) -> 
  #{name := Name, node := Node} = D,
  gen_server:call({Name, Node}, {req, Key, Opts, norma});

%%
%% Proxy resolve by new domain (DNew)
resolve(Key, {proxy, DOld, DNew}, Opts = #{mode := _}) ->
  #{name := Name, node := Node} = DNew,
  gen_server:call({Name, Node}, {req, Key, Opts, {proxy, DOld}});

%%
%% Merge resolve by new domain (DNew)
resolve(Key, {merge, DOld, DNew}, Opts = #{mode := _}) ->
  #{name := Name, node := Node} = DNew,
  gen_server:call({Name, Node}, {req, Key, Opts, {merge, DOld}});

%%
%% Wrong args
resolve(Key, Args, Opts) -> 
  ?INF("resolve", {Key, Args, Opts}),
  ?e(wrong_args, {Key, Args, Opts}).



%%
req_(S = #{q := Qs}, From, Key, Opts, Type) ->
  QItem = #{from => From, type => Type, opts => Opts},
  NewQs = Qs#{Key => queue:in(QItem, maps:get(Key, Qs, queue:new()))},
  gen_server:cast(self(), {queue_manage_, Key}),
  {noreply, S#{q := NewQs}, 0}.


%%
reg(#{domain := Domain, key := Key, from := From, reply := ok}) ->
  process_flag(trap_exit, true),
  gen_server:call(Domain, {reg, Key, From, {ok, self()}});
reg(#{domain := Domain, key := Key, from := From, reply := Reply}) ->
  gen_server:call(Domain, {reg, Key, From, Reply}).
reg_(S = #{out := Out, c := Childs}, Key, From, Reply) ->
  case lists:keytake(From, 2, Out) of
    {value, {_Until, From}, NewOut} -> 
      gen_server:reply(From, Reply),
      gen_server:cast(self(), {queue_manage_, Key}),
      case Reply of
        {ok, Pid} -> 
          try 
            link(Pid),
            {reply, ok, S#{out := NewOut, c := orddict:store(Key, Pid, Childs)}, 0}
          catch
            _:_ -> {reply, ?e(link_worker_exeption), S, 0}
          end;
        _Else -> {reply, ok, S#{out := NewOut}, 0}
      end;
    _ ->
      gen_server:cast(self(), {queue_manage_, Key}),
      {reply, ?e(empty_queue_or_start_process_timeout), S, 0}
  end.


%% TODO unreg by Key only
unreg(#{domain := DomainPid, key := Key, pid := Pid}) ->
  gen_server:call(DomainPid, {unreg, Key, Pid}).
unreg_(S = #{c := Childs}, Key, Pid) ->
  try unlink(Pid) catch _:_ -> do_nothing end,
  {reply, ok, S#{c := orddict:erase(Key, Childs)}, 0}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% QUEUE MANAGE {{{
queue_manage_(S = #{ets := Ets, c := Childs, q := Qs, out := Out}, Key) ->
  Now = ?mnow,
  OutFun = 
    fun(Opts, From) -> 
      O = ordsets:add_element({Now + maps:get(start_timeout, Opts, ?TIMEOUT), From}, Out),
      T = out_ttl(O, Now),
      {O, T}
    end,

  case Qs of
    #{Key := Q} ->
      case queue:out(Q) of

        %% NORMA MANAGE
        {{value, #{from := From, type := norma, opts := #{mode := info} }}, NewQ} -> 
          case orddict:find(Key, Childs) of
            {ok, Pid} -> 
              gen_server:reply(From, {ok, Pid}), 
              queue_manage_(S#{q := Qs#{Key := NewQ}}, Key);
            error -> 
              gen_server:reply(From, not_started),
              queue_manage_(S#{q := Qs#{Key := NewQ}}, Key)
          end;
        {{value, #{from := From, type := norma, opts := Opts}}, NewQ} -> 
          case orddict:find(Key, Childs) of
            {ok, Pid} -> 
              gen_server:reply(From, {ok, Pid}), 
              queue_manage_(S#{q := Qs#{Key := NewQ}}, Key);
            error     ->
              RegArgs = #{domain => self(), ets => Ets, from => From, key => Key},
              start_spawn(Key, Opts, RegArgs),
              {NewOut, Ttl} = OutFun(Opts, From),
              {noreply, S#{q := Qs#{Key := NewQ}, out := NewOut}, Ttl}
          end;

        %% PROXY MANAGE
        {{value, #{from := From, type := {proxy, D}, opts := Opts}}, NewQ} -> 
          case orddict:find(Key, Childs) of
            {ok, Pid} -> 
              gen_server:reply(From, {ok, Pid}), 
              queue_manage_(S#{q := Qs#{Key := NewQ}}, Key);
            error     ->
              #{name := Name, node := Node} = D,
              gen_server:cast({Name, Node}, {req, Key, Opts, From, norma}),
              {noreply, S#{q := Qs#{Key := NewQ}}, out_ttl(Out, Now)}
          end;

        %% MERGE MANAGE
        {{value, #{from := From, type := {merge, D}, opts := Opts}}, NewQ} -> 
          case orddict:find(Key, Childs) of
            {ok, Pid} -> 
              gen_server:reply(From, {ok, Pid}), 
              queue_manage_(S#{q := Qs#{Key := NewQ}}, Key);
            error     ->
              RegArgs = #{domain => self(), ets => Ets, from => From, key => Key},
              merge_spawn(Key, Opts, D, RegArgs),
              {NewOut, Ttl} = OutFun(Opts, From),
              {noreply, S#{q := Qs#{Key := NewQ}, out := NewOut}, Ttl}
          end;

        %% EMPTY QUEUE
        {empty, Q} -> {noreply, S#{q := maps:remove(Key, Qs)}, out_ttl(Out, Now)}
      end;
    _Else -> {noreply, S#{q := maps:remove(Key, Qs)}, out_ttl(Out, Now)}
  end.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% START SPAWN {{{
%% Try start process
start_spawn(Key, Opts, RegArgs = #{ets := Ets}) ->
  erlang:spawn(
    fun() ->
      try 
        case ets:lookup(Ets, ma) of
          [{ma, {M,A}}] -> 
            gen_server:start(M, A#{key => Key, opts => Opts, reg => RegArgs}, []);
          _ -> 
            reg(RegArgs#{reply => ?e(ma_not_found)})
        end
      catch
        Err:Reason -> reg(RegArgs#{reply => ?e(crash, {Err, Reason})})
      end
    end).
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MERGE SPAWN {{{
merge_spawn(Key, Opts, #{name := Domain, node := Node}, RegArgs = #{ets := Ets}) ->
  erlang:spawn(
    fun() ->
      try 
        case ets:lookup(Ets, ma) of
          [{ma, {M,A}}] -> 
            Args = A#{key => Key, opts => Opts, reg => RegArgs},
            case gen_server:call({Domain, Node}, {req, Key, #{mode => info}, norma}) of
              {ok, Pid} -> gen_server:start(M, Args#{merge_pid => Pid}, []);
              _Else     -> gen_server:start(M, Args, [])
            end;
          _ -> 
            reg(RegArgs#{reply => ?e(ma_not_found)})
        end
      catch
        Err:Reason -> reg(RegArgs#{reply => ?e(crash, {Err, Reason})})
      end
    end).
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%






%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%55
% MISC
is_node_in_domains(Node, Domains) when is_map(Domains) ->
  CheckFun = fun
    (_F, [{_Key, #{node :=  N}}|_]) when Node == N -> true;
    (Fu, [{_Key, #{node := _N}}|Rest]) -> Fu(Fu, Rest);
    (_F, []) -> false
  end,
  CheckFun(CheckFun, maps:to_list(Domains)).



gen_key(Domain = #{node := _, name := _}) ->
  ecldb_misc:md5_hex(term_to_binary(Domain)).


ping(Name) when is_atom(Name) ->
  ping(#{name => Name, node => node()});
ping(#{name := Name, node := Node}) ->
  try gen_server:call({Name, Node}, ping) catch _E:_R -> pang end.


%%
out_ttl([{Until, _}|_], Now) when Until > Now -> Until - Now;
out_ttl([], _)        -> 600000;
out_ttl([_|_], _Now)  -> 0.
