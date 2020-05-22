
-module(ecldb_domain_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%%
-export([start_domain/2, stop_domain/2]).

-define(SERVER, ?MODULE).

-include("../include/ecldb.hrl").


%%====================================================================
%% API functions
%%====================================================================

start_link(Timeout) ->
    supervisor:start_link(?MODULE, [Timeout]).


%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Timeout]) ->
  SupFlags = #{
    strategy  => simple_one_for_one,
    intensity => 10,
    period    => 10},

  Childs = [#{
    id       => domain,
    start    => {ecldb_domain, start_link, []},
    restart  => permanent,
    shutdown => Timeout,
    type     => worker
  }],

  {ok, {SupFlags, Childs}}.



%%====================================================================
%% Internal functions
%%====================================================================

start_domain(Cluster, Domain) when is_atom(Cluster), is_atom(Domain) ->
  {ok, ClusterSupPid} = ecldb:get_sup_pid(Cluster),
  case supervisor:start_child(ClusterSupPid, [Cluster, Domain]) of
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
    ok   -> ok;
    Else -> ?e(stop_error, Else)
  end.

