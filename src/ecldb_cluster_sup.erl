%%%-------------------------------------------------------------------
%% @doc ecldb top level supervisor.
%% @end
%%%-------------------------------------------------------------------


-module(ecldb_cluster_sup).

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

-include("../include/ecldb.hrl").


%%====================================================================
%% API functions
%%====================================================================

start_link(Name, Args) ->
    supervisor:start_link(?MODULE, [Name, Args]).


%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([Name, Args]) ->
  SupFlags = #{
    strategy  => one_for_one,
    intensity => 10,
    period    => 10},

  Childs = [
    #{id       => sup,
      start    => {ecldb_domain_sup, start_link, []},
      restart  => permanent,
      shutdown => 50000,
      type     => supervisor},
    #{id       => srv,
      start    => {ecldb_cluster, start_link, [Args#{name => Name}]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker}
  ],


  {ok, {SupFlags, Childs}}.

