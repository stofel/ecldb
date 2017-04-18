%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Erlang cluster process value database
%% 
%% Support multinode sharding and resharding by hash rings
%%
%% Rings and domains. Rings for sharding, domains for sync queues 
%% of requests to the processes for prevent races on start new process.
%%
%% Three ring schema:
%%  1. First working ring
%%  2. Second working ring
%%  3. Third devel ring
%%
%% Use order
%% 1. Changes do to the third devel ring. 
%% 2. Commit third devel ring to the second working ring (only in norma mode), 
%% 3. Switch switch mode norma -> proxy -> merge -> norma
%%    3.1 norma -> proxy - resolving thru second working ring and first working ring
%%                         return result from first working ring
%%    3.2 proxy -> merge - resolving thru second working ring and first working ring
%%                         return result from both ring for migrating operations
%%    3.3 merge -> norma - copy second working ring to first working ring
%%
%%
%% MODES
%%
%%    Norma
%%  Shard requests to domains by first working ring
%%
%%    Proxy
%%  Mode for garatee that all requests on all nodes go tru domains from second working ring
%%  Shard requests to domains by second working ring, domains do nothing and route request
%%  to domains from shard from first working ring.
%%
%%    Merge
%%  Mode same as Proxy but domains by second working ring check if process exists on
%%  domains from shard from first working ring, send to process migrate Msg with new domain Pid.
%%  If not, start id thru domain from second working ring
%%  Mode for take time to migrate all processes from firts ring to second
%%

-module(ecldb).


-export([

    %% Manage
    start_cluster/2, start_cluster/0, stop_cluster/1, list_clusters/0,
    add_node/2,   del_node/2,
    add_domain/2, unreg_domain/2, stop_domain/2,
    flush_changes/1,
    merge/1, norma/1,

    % Show
    cluster_stat/1,
    show_first/1, show_second/1, show_third/1,

    % Main resolve function
    route/0, route/2,
    call/4,
    cursor/3,

    % Misc 
    get_srv_pid/1, get_sup_pid/1
  ]).


-include("../include/ecldb.hrl").

-export_type([err/0]).


%%%%%%%%%%%%%%%%%%%
%% Abriviatures
%% C - ClasterName
%% N - Node
%% D - Domain
%%%%%%%%%%%%%%%%%%%


%
start_cluster() -> 
  %% test starting
  <<"Usage: start_cluster(ClusterName, Args). see README.md">>.

%
start_cluster(C, Args) -> 
  ecldb_sup:start_cluster(C, Args).

%
stop_cluster(C) ->
  ecldb_sup:stop_cluster(C).

%
list_clusters() ->
  [N || {N,_,_,_} <- supervisor:which_children(ecldb_sup)].

%
show_first(ClusterName)  -> ecldb_cluster:show_first(ClusterName).
show_second(ClusterName) -> ecldb_cluster:show_second(ClusterName).
show_third(ClusterName)  -> ecldb_cluster:show_third(ClusterName).

%
cluster_stat(ClusterName) ->
  #{domains => maps:size(ClusterName:domains()),
    nodes   => length(ClusterName:nodes()),
    workers => lists:sum([ecldb_domain:workers_num(D) || D <- maps:values(ClusterName:domains())])
  }.


%%
add_node(ClusterName, Node) ->
  ok = ecldb_dicts:add_node(ClusterName, Node),
  ok = ecldb_cluster:commit(ClusterName),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  ok.

del_node(ClusterName, Node) ->
  ok = ecldb_dicts:del_node(ClusterName, Node),
  ok = ecldb_cluster:commit(ClusterName),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  ok.


add_domain(ClusterName, DomainName) -> 
  add_domain(ClusterName, DomainName, 32).
add_domain(ClusterName, DomainName, N) ->
  ok = ecldb_domain:start(ClusterName, DomainName),
  ok = ecldb_dicts:add_domain(ClusterName, DomainName),
  ok = ecldb_cluster:copy_third_from_second(ClusterName),
  ok = ecldb_cluster:commit(ClusterName),
  {ok, NewN} = ecldb_cluster:add_domain_to_third_ring(ClusterName, DomainName, N),
  ok = ecldb_cluster:commit(ClusterName),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  {ok, NewN}.


unreg_domain(ClusterName, DomainName) ->
  unreg_domain(ClusterName, DomainName, 32).
unreg_domain(ClusterName, DomainName, N) ->
  ok = ecldb_cluster:copy_third_from_second(ClusterName),
  {ok, NewN} = ecldb_cluster:del_domain_from_third_ring(ClusterName, DomainName, N),
  ok = ecldb_cluster:commit(ClusterName),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  {ok, NewN}.
  

stop_domain(ClusterName, DomainName) ->
  ok = ecldb_dicts:del_domain(ClusterName, DomainName),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  ecldb_domain:stop(ClusterName, DomainName),
  ok.


flush_changes(ClusterName) ->
  ok = ecldb_cluster:copy_third_from_second(ClusterName),
  ok = ecldb_dicts:flush_domains(ClusterName),
  ok = ecldb_dicts:flush_nodes(ClusterName),
  ok.


merge(ClusterName) -> 
  ok = ecldb_cluster:switch(ClusterName, proxy),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  timer:sleep(1000),
  ok = ecldb_cluster:switch(ClusterName, merge),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  ok.
  
norma(ClusterName) ->
  ok = ecldb_cluster:switch(ClusterName, norma),
  ok = ecldb_cluster:sync_to_all(ClusterName),
  ok.


route() -> <<"Usage: ecldb:route(ClusterName, Key), ecldb:list_clusters()">>.
route(_C, Key) when not is_binary(Key) ->
  ?e(key_not_binary);
route(C, Key) ->
  case lists:member(C, ecldb:list_clusters()) of
    true  -> ecldb_ring:route(C, Key);
    false -> ?e(no_such_cluster)
  end.




%% Opts = #{mode => Mode}, 
%% Mode = info|start|init|temp
%%  temp - for temporary process manage
%%
call(C, Key, Msg, Opts) ->
  Route = ecldb_ring:route(C, Key),
  %?INF("Route", Route),
  case ecldb_domain:resolve(Key, Route, Opts) of
    {ok, Pid} -> gen_server:call(Pid, Msg);
    Else -> Else
  end.


%%
-spec cursor(ClusterName::atom(), Cursor::init|pid(), Opts::map()) -> 
        {ok, Cursor::stop|pid(), Result::list()}|err().
cursor(ClusterName, init, Opts) -> 
  Timeout = maps:get(timeout, Opts, 60000),
  Cursor =
    spawn(fun() ->
      CursorFun = fun
          %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
          (Fu, init, u, u, [], u) -> Fu(Fu, maps:values(ClusterName:domains()), u, u, [], u);
          %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
          (_F, [], Pid, Ref, [], _N) when is_pid(Pid) -> 
            Pid ! {Ref, stop}, 
            stop;
          %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
          (Fu, Ds, u, u, Acc, u) ->
            receive 
              {next, Pid, Ref, Count} -> Fu(Fu, Ds, Pid, Ref, Acc, Count);
              stop -> stop
            after
              Timeout -> timeout
            end;
          %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
          (_F, [], Pid, Ref, [],  _N) when is_pid(Pid) -> 
            Pid ! {Ref, stop}, 
            stop;
          %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
          (Fu, [], Pid, Ref, Acc, N) when is_pid(Pid) -> 
            case length(Acc) < N of
              true  -> 
                Pid ! {Ref, {ok, self(), Acc}},
                Fu(Fu, [], u, u, [], u);
              false -> 
                {Answer, Rest} = lists:split(N, Acc),
                Pid ! {Ref, {ok, self(), Answer}},
                Fu(Fu, [], u, u, Rest, u)
            end;
          %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
          (Fu, [D|Ds], Pid, Ref, Acc, N) ->
            NewAcc = Acc ++ ecldb_domain:workers_list(D),
            case length(NewAcc) < N of
              true  -> Fu(Fu, Ds, Pid, Ref, NewAcc, N);
              false -> 
                {Answer, Rest} = lists:split(N, NewAcc),
                Pid ! {Ref, {ok, self(), Answer}},
                Fu(Fu, Ds, u, u, Rest, u)
            end 
        end,
        CursorFun(CursorFun, init, u, u, [], u)
      end),
  cursor(ClusterName, Cursor, Opts);


%%
cursor(_ClusterName, Cursor, Opts) when is_pid(Cursor) ->
  case erlang:is_process_alive(Cursor) of
    true ->
      Timeout = maps:get(timeout, Opts, 60000),
      Count   = maps:get(count, Opts, 500),
      Ref     = erlang:make_ref(),
      Cursor ! {next, self(), Ref, Count},
      receive
        {Ref, {ok, Pid, Value}} -> {ok, Pid, Value};
        {Ref, Else} -> Else
      after
        Timeout -> ?e(timeout)
      end;
    false ->
      ?e(cursor_not_exists)
  end.



%% MISC
get_srv_pid(Cluster) ->
   case [Pid || {C,Pid,_,_} <- supervisor:which_children(ecldb_sup), C == Cluster] of
    [SupPid] -> 
      case [Pid || {srv,Pid,_,_} <- supervisor:which_children(SupPid)] of
        [SrvPid] -> {ok, SrvPid};
        _ -> ?e(srv_not_exists)
      end;
    _ ->
      ?e(srv_not_exists)
    end.

get_sup_pid(Cluster) ->
   case [Pid || {C,Pid,_,_} <- supervisor:which_children(ecldb_sup), C == Cluster] of
    [SupPid] -> 
      case [Pid || {sup,Pid,_,_} <- supervisor:which_children(SupPid)] of
        [SrvPid] -> {ok, SrvPid};
        _ -> ?e(srv_not_exists)
      end;
    _ ->
      ?e(srv_not_exists)
    end.

