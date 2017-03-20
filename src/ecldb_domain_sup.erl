
-module(ecldb_domain_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%
-export([start_domain/2, stop_domain/2]).

-define(SERVER, ?MODULE).

-include("../include/ecldb.hrl").


%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link(?MODULE, []).


%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
  SupFlags = #{
    strategy  => one_for_one,
    intensity => 10,
    period    => 10},

  Childs = [],

  {ok, {SupFlags, Childs}}.



%%====================================================================
%% Internal functions
%%====================================================================

%%
start_domain(Cluster, Domain) when is_atom(Cluster), is_atom(Domain) ->
  {ok, ClusterSupPid} = ecldb:get_sup_pid(Cluster),
  ChildSpec = #{
      id       => Domain,
      start    => {ecldb_domain, start_link, [Cluster, Domain]},
      restart  => permanent,
      shutdown => 20000, %% Large for stop all childs
      type     => worker},
  case supervisor:start_child(ClusterSupPid, ChildSpec) of
    {ok, _Pid} -> ok;
    {error,R} -> ?e(start_error, R);
    Else      -> ?e(start_error, Else)
  end;
%%
start_domain(Cluster, Domain) ->
  ?e(wrong_args, {Cluster, Domain}).


%%
stop_domain(Cluster, Domain) ->
  {ok, ClusterSupPid} = ecldb:get_sup_pid(Cluster),
  case supervisor:terminate_child(ClusterSupPid, Domain) of
    ok   -> 
      case supervisor:delete_child(ClusterSupPid, Domain) of
        ok        -> ok;
        {error,R} -> ?e(start_error, R);
        Else      -> ?e(start_error, Else)
      end;
    Else -> Else
  end.

