%%
%% Cluster manage gen_server
%%


%
% 1. Config Ets namage
% 2. Rings manage
% 3. Hold nodes list (for add node to ring node should be in list)
% 3. Sync cluster state tru nodes
% 4. Hold thrid ring in state
% 5. Monitorig cluster
% 

-module(ecldb_cluster).

-behaviour(gen_server).

-include("../include/ecldb.hrl").

-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([
    state/1,
    ping/2,
    commit/1,
    add_domain_to_third_ring/3, del_domain_from_third_ring/3,
    show_first/1, show_second/1, show_third/1,
    sync_to/2, sync_to_all/1,
    switch/2,
    copy_third_from_first/1, copy_third_from_second/1
  ]).

start_link(Args = #{name := Name}) ->
  gen_server:start_link({local, Name}, ?MODULE, Args, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Gen Server api
handle_info({'EXIT',_F,_R}, _S) -> stop; %exit_signal(S, F, R);
handle_info(Msg, S)             -> io:format("Unk msg ~p~n", [Msg]), {noreply, S}.
code_change(_Old, S, _Extra)    -> {ok, S}.

%% casts
handle_cast(Req, S)             -> ?INF("unknown_msg", Req), {noreply, S}.

%% calls
handle_call(ping,_F,S)               -> {reply, pong, S};
% Dict calls
%[show_nodes/1,   add_node/2,   del_node/2,   flush_node/2,   flush_nodes/1]).
%[show_domains/1, add_domain/2, del_domain/2, flush_domain/2, flush_domains/1]).
%[flush_all/1]).
handle_call(state, _F,S)             -> {reply, S, S};
handle_call(show_nodes, _F,S)        -> ecldb_dicts:show_nodes    ({call, S});
handle_call(show_domains, _F,S)      -> ecldb_dicts:show_domains  ({call, S});
handle_call(show_third, _F,S)        -> show_third_               (S);
handle_call(norma_proxy, _F,S)       -> norma_proxy_              (S);
handle_call(proxy_merge, _F,S)       -> proxy_merge_              (S);
handle_call(merge_norma, _F,S)       -> merge_norma_              (S);
handle_call(copy_third_from_first_, _F,S)  -> copy_third_from_first_  (S);
handle_call(copy_third_from_second_, _F,S) -> copy_third_from_second_ (S);
handle_call({sync_to, Node}, _F,S)   -> sync_to_                  (S,          Node);
handle_call({add_node, N}, _F,S)     -> ecldb_dicts:add_node      ({call, S},  N);
handle_call({add_domain, D}, _F,S)   -> ecldb_dicts:add_domain    ({call, S},  D);
handle_call({del_node, N}, _F,S)     -> ecldb_dicts:del_node      ({call, S},  N);
handle_call({del_domain, D}, _F,S)   -> ecldb_dicts:del_domain    ({call, S},  D);
handle_call({flush_node, N}, _F,S)   -> ecldb_dicts:flush_node    ({call, S},  N);
handle_call({flush_domain, D}, _F,S) -> ecldb_dicts:flush_domain  ({call, S},  D);
handle_call(flush_nodes,_F,S)        -> ecldb_dicts:flush_nodes   ({call, S});
handle_call(flush_domains,_F,S)      -> ecldb_dicts:flush_domains ({call, S});
handle_call(flush_all,_F,S)          -> ecldb_dicts:flush_all     ({call, S});

% ring manage
handle_call({reg_domain,   D, N}, _F, S)  -> reg_domain_                 (S, D, N);
handle_call({unreg_domain, D, N}, _F, S)  -> unreg_domain_               (S, D, N);
% Commit                                 
handle_call(commit,_F,S)                  -> commit_                     (S);

handle_call(ets, _F,S = #{ets := Ets})    -> {reply, {ok, Ets}, S};

handle_call(Req, _From, S)                -> {reply, {err, unknown_command, ?p(Req)}, S}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init(_Args = #{
    new  := New,  %% true|false|do_try Start or not new cluster. 
                  %% if do_try, look cluster file existance, if not exists start new cluster
    name := Name, %% Cluster name
    ma   := MFA,
    opt  := Opt,
    path := Path  %% Path for dinamic compile files with rings and monitoring
  }) ->
 
  EtsName = list_to_atom(atom_to_list(Name) ++ "_ets"),
  EtsName = ets:new(EtsName, [named_table]),

  Confs   = [
      {ma,  MFA},
      {opt, Opt}
    ],

  true = ets:insert(EtsName, Confs),
  File = Path ++ atom_to_list(Name),

  FileLoadRes = case New of 
    false ->
      case code:load_abs(File) of
        {module, Name} -> ok;
        Else -> ?e(start_cluster_error, Else)
      end;
    do_try ->
      case code:load_abs(File) of
        {module, Name} -> ok;
        Else -> 
          ?INF("Try start result", Else),
          new_cluster_file(Name, File)
      end;
    true -> new_cluster_file(Name, File)
  end,

  case FileLoadRes of
    ok ->
      S = #{
        name    => Name,
        ets     => EtsName,  %% ets table name
        ring    => ecldb_ring:new(), %% thrid ring
        domains => {[], []}, %% domains {AddDomain, DelDomains}.
        nodes   => {[], []}, %% Cluster nodes [{add, Node}, {del, Node}].
        log     => []        %% messages from other nodes
      },
      process_flag(trap_exit, true),
      {ok, S};
    _ ->
      {stop, FileLoadRes}
  end.

%
new_cluster_file(Name, File) ->
  FunList = [
    {mode,    norma},
    {first,   ecldb_ring:new()},
    {second,  ecldb_ring:new()},
    {domains, #{}},
    {nodes,   []},
    {rev,     ecldb_misc:rev()}
  ],
  case ecldb_compile:c(Name, File, FunList) of
    {ok, Name} -> ok;
    Else -> Else
  end.



%
terminate(Reason, #{name := Name}) ->
  UnloadRes = case code:soft_purge(Name) of
    true -> code:delete(Name);
    Else -> Else
  end,
  ?INF("Terminate", {Reason, UnloadRes}),
  ok.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Cluster functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
ping(Name, Node) ->
  case node() == Node of
    true  -> case is_pid(erlang:whereis(Name)) of true -> pong; false -> pang end;
    false -> try gen_server:call({Name, Node}, ping) catch _E:_R -> pang end
  end.

state(Name) ->
  gen_server:call(Name, state).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Commit domains and nodes changes {{{
commit(C) ->
  gen_server:call(C, commit, 20000).
commit_(S = #{name := Cluster}) ->
  % 1. Check mode
  % 2. Check nodes and domains
  % 3. Merge nodes
  % 4. merge domains
  % 5. Compile new module

  CS = #{ %% CommitState
    status  => ok,
    name    => Cluster,
    nodes   => maps:get(nodes,   S, {[],[]}),
    domains => maps:get(domains, S, {[],[]}),
    ring    => maps:get(ring,    S),
    log     => []
  },

  FunList = [
    %% Checks
    fun commit_check_mode/1,
    fun commit_check_add_nodes/1,
    fun commit_check_del_nodes/1,
    fun commit_check_add_domains/1,
    fun commit_check_del_domains/1,
    %% Del & Add 
    fun commit_merge_nodes/1,
    fun commit_merge_domains/1,
    %% Compile
    fun commit_compile/1
  ],

  {NewS, Reply} = 
    case ecldb_misc:c_r(FunList, CS) of
      #{status := ok} -> {S#{nodes := {[],[]}, domains := {[],[]}}, ok};
      #{status := Else} -> 
        %undo_commit(S, Log),
        {S, Else}
    end,

  {reply, Reply, NewS}. 


%% Mode should be norma
commit_check_mode(CS = #{name := Name}) ->
  case Name:mode() of
    norma -> CS;
    Else  -> ?e(wrong_cluster_mode, Else)
  end.
 
%% {ANs, DNs} = {AddNodes, DelNodes}
commit_check_add_nodes(CS = #{name := Name, nodes := {ANs, _DNs}}) ->
  %% 1. Check node exists and cluster started
  %% 2. Check node not already added
  CheckFun = fun
    (Fu, [N|RestANs]) ->  
      case net_adm:ping(N) of
        pong -> 
          case ping(Name, N) of
            pong -> 
              case lists:member(N, Name:nodes()) of
                false -> Fu(Fu, RestANs);
                true  -> CS#{status := ?e(node_already_added, N)}
              end;
            pang -> CS#{status := ?e(node_ping_failed, N)}
          end;
        pang -> CS#{status := ?e(node_ping_failed, N)}
      end;
    (_F, []) -> CS
  end,
  CheckFun(CheckFun, ANs).


commit_check_del_nodes(CS = #{name := Name, nodes := {_ANs, DNs}, domains := {ADs, _DDs}}) -> 
  CheckFun = fun
    (Fu, [N|RestDNs]) ->
      Cond1 = ecldb_domain:is_node_in_domains(N, Name:domains()),
      Cond2 = [N1 || #{node := N1} <- ADs, N1 == N] /= [],
      case Cond1 orelse Cond2 of
        true  -> CS#{status := ?e(node_used_in_domains)};
        false ->
          case lists:member(N, Name:nodes()) of
            true  -> Fu(Fu, RestDNs);
            false -> CS#{status := ?e(node_already_not_exist, N)}
          end
      end;
    (_F, []) -> CS
  end,
  CheckFun(CheckFun, DNs).


%
commit_check_add_domains(CS = #{name := Name, domains := {ADs, _DDs}} ) -> 
  CheckFun = fun
    (Fu, [D|RestADs]) ->
      case ecldb_domain:ping(D) of
        pong ->
          DomainKey = ecldb_domain:gen_key(D),
          case maps:is_key(DomainKey, Name:domains()) of
            false -> Fu(Fu, RestADs);
            true  -> CS#{status := ?e(domain_already_added, D)}
          end;
        pang ->
          CS#{status := ?e(domain_ping_failed, D)}
      end;
    (_F, []) -> CS
  end,
  CheckFun(CheckFun, ADs).


%
commit_check_del_domains(CS = #{name := Name, domains := {_ADs, DDs}, ring := Ring}) -> 

  IsDomainFun = fun(D, R) -> lists:member(D, ecldb_ring:list_domains(R)) end, 

  CheckFun = fun
    (Fu, [D|RestDDs]) ->
      DKey = ecldb_domain:gen_key(D),
      case IsDomainFun(DKey, Ring) orelse
           IsDomainFun(DKey, Name:first()) orelse
           IsDomainFun(DKey, Name:second()) of
             true  -> CS#{status := ?e(domain_used_in_rings, D)};
             false ->
               case maps:is_key(DKey, Name:domains()) of
                 true  -> Fu(Fu, RestDDs);
                 false -> CS#{status := ?e(domain_already_not_exist, D)}
               end
      end;
    (_F, []) -> CS
  end,
  CheckFun(CheckFun, DDs).

%
commit_merge_nodes(CS = #{nodes := {ANs, DNs}, name := Name}) ->
  DelFun = fun(N, Acc) -> lists:delete(N, Acc) end,
  NewNodes0 = lists:foldr(DelFun, Name:nodes(), DNs),
  NewNodes  = lists:append(ANs, NewNodes0),
  CS#{nodes := NewNodes}.

%
commit_merge_domains(CS = #{domains := Domains, name := Name}) ->
  %% Name:domains()) = #{Key1 => Domain = #{node := Node, domain := DomainName}, Key2 => ...},
  %% Key = 20byte hash for ring value (ecldb_domain:gen_key(Domain))
  %% Domains = {AddDomains,DelDomains}
  %% AddDomains = [#{node := Node, domain := Domain}]
  MergeFun = fun
    (Fu, {[D|Rest], DDs}, Acc) -> 
      Key = ecldb_domain:gen_key(D),
      Fu(Fu, {Rest, DDs}, Acc#{Key => D});
    (Fu, {[], [D|Rest]}, Acc) ->
      Key = ecldb_domain:gen_key(D),
      Fu(Fu, {[], Rest}, maps:remove(Key, Acc));
    (_F, {[], []}, Acc) -> Acc
  end,
  NewDomains = MergeFun(MergeFun, Domains, Name:domains()),
  CS#{domains := NewDomains}.

%
commit_compile(CS = #{name := Name, domains := Domains, nodes := Nodes, ring := Ring}) ->
  FunList = [
      {mode,    Name:mode()},
      {first,   Name:first()},
      {second,  Ring},
      {domains, Domains},
      {nodes,   Nodes},
      {rev,     ecldb_misc:rev(Name:rev())}
    ],
  ecldb_compile:c(Name, none, FunList),
  CS.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% add_domain_to_third_ring(Cluster, DomainName, N) {{{
add_domain_to_third_ring(Cluster, DomainName, N) when is_atom(DomainName) ->
  Domain = #{name => DomainName, node => node()},
  add_domain_to_third_ring(Cluster, Domain, N);
add_domain_to_third_ring(Cluster, Domain, N) when is_map(Domain) ->
  DomainKey = ecldb_domain:gen_key(Domain),
  case maps:is_key(DomainKey, Cluster:domains()) of
    true  -> gen_server:call(Cluster, {reg_domain, DomainKey, N});
    false -> ?e(domain_not_in_cluster_dicts)
  end.

reg_domain_(S = #{ring := Ring}, DomainKey, N) ->
  case ecldb_ring:add_domain(DomainKey, Ring, N) of
    {ok, NewRing, NewCount} -> {reply, {ok, NewCount}, S#{ring := NewRing}};
    Else -> {reply, Else, S}
  end.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% del_domain_to_third_ring {{{
del_domain_from_third_ring(Cluster, DomainName, N) when is_atom(DomainName) ->
  Domain = #{name => DomainName, node => node()},
  del_domain_from_third_ring(Cluster, Domain, N);
del_domain_from_third_ring(Cluster, Domain, N) when is_map(Domain) ->
  DomainKey = ecldb_domain:gen_key(Domain),
  case maps:is_key(DomainKey, Cluster:domains()) of
    true  -> gen_server:call(Cluster, {unreg_domain, DomainKey, N});
    false -> ?e(domain_not_in_cluster_dicts)
  end.

unreg_domain_(S = #{ring := Ring}, DomainKey, N) ->
  case ecldb_ring:del_domain(DomainKey, Ring, N) of
    {ok, NewRing, NewCount} -> {reply, {ok, NewCount}, S#{ring := NewRing}};
    Else -> {reply, Else, S}
  end.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% SHOW
show_first(ClusterName)  -> map_domains(ClusterName, ClusterName:first()).
show_second(ClusterName) -> map_domains(ClusterName, ClusterName:second()).
show_third(ClusterName)  -> gen_server:call(ClusterName, show_third).
show_third_(S = #{name := N, ring := R}) -> {reply, map_domains(N, R), S}.
%
map_domains(Name, Ring) ->
  [{maps:get(K, Name:domains(), u), N} || {K, N} <- ecldb_ring:list_domains(Ring)].



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SYNC {{{
sync_to(Cluster, Node) when is_atom(Cluster), is_atom(Node) ->
  case lists:member(Node, Cluster:nodes()) of
    true  -> gen_server:call(Cluster, {sync_to, Node});
    false -> ?e(node_not_in_cluster)
  end;
sync_to(_, _) ->
  ?e(wrong_args).
 
sync_to_(S = #{name := Name}, Node) ->
  FunList = [
      {mode,    Name:mode()},
      {first,   Name:first()},
      {second,  Name:second()},
      {domains, Name:domains()},
      {nodes,   Name:nodes()},
      {rev,     Name:rev()}
    ],
  Reply = rpc:call(Node, ecldb_compile,c, [Name, none, FunList]),
  {reply, Reply, S}.


sync_to_all(C) ->
  Nodes = C:nodes() -- [node()],
  [ok = sync_to(C, N) || N <- Nodes],
  ok.

%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SWITCH {{{
switch(Cluster, Mode) when Mode == norma; Mode == proxy; Mode == merge ->
  case {Cluster:mode(), Mode} of
    {norma, proxy} -> gen_server:call(Cluster, norma_proxy);
    {proxy, merge} -> gen_server:call(Cluster, proxy_merge);
    {merge, norma} -> gen_server:call(Cluster, merge_norma);
    _              -> ?e(wrong_switch)
  end;
switch(_Cluster, _Mode) ->
  ?e(wrong_mode).

%
norma_proxy_(S = #{name := Name}) ->
  FunList = [
      {mode,    proxy},
      {first,   Name:first()},
      {second,  Name:second()},
      {domains, Name:domains()},
      {nodes,   Name:nodes()},
      {rev,     ecldb_misc:rev(Name:rev())}
    ],
  Reply = ecldb_compile:c(Name, none, FunList),
  {reply, Reply, S}.
  
%
proxy_merge_(S = #{name := Name}) ->
  FunList = [
      {mode,    merge},
      {first,   Name:first()},
      {second,  Name:second()},
      {domains, Name:domains()},
      {nodes,   Name:nodes()},
      {rev,     ecldb_misc:rev(Name:rev())}
    ],
  Reply = ecldb_compile:c(Name, none, FunList),
  {reply, Reply, S}.

%
merge_norma_(S = #{name := Name}) ->
  FunList = [
      {mode,    norma},
      {first,   Name:second()},
      {second,  Name:second()},
      {domains, Name:domains()},
      {nodes,   Name:nodes()},
      {rev,     ecldb_misc:rev(Name:rev())}
    ],
  Reply = ecldb_compile:c(Name, none, FunList),
  {reply, Reply, S}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
copy_third_from_first(Cluster) when is_atom(Cluster) ->
  gen_server:call(Cluster, copy_third_from_first_).
copy_third_from_first_(S = #{name := Name}) ->
  {reply, ok, S#{ring := Name:first()}}.
copy_third_from_second(Cluster) when is_atom(Cluster) ->
  gen_server:call(Cluster, copy_third_from_second_).
copy_third_from_second_(S = #{name := Name}) ->
  {reply, ok, S#{ring := Name:second()}}.
  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
