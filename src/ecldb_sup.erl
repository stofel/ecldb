%%%-------------------------------------------------------------------
%% @doc ecldb top level supervisor.
%% @end
%%%-------------------------------------------------------------------


-module(ecldb_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%
-export([start_cluster/2, stop_cluster/1]).


-include("../include/ecldb.hrl").


%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).


%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    SupFlags = #{
      strategy  => one_for_one,
      intensity => 10,
      period    => 10},
    {ok, {SupFlags, []}}.



%%====================================================================
%% Internal functions
%%====================================================================

%%
start_cluster(Name, Args) when is_map(Args) ->
  ChildSpec = #{
      id       => Name,
      start    => {ecldb_cluster_sup, start_link, [Name, Args]},
      restart  => transient,
      shutdown => 30000, %% Large for stop all childs
      type     => supervisor},
  case supervisor:start_child(?MODULE, ChildSpec) of
    {ok, Pid}                       -> {ok, Pid};
    {error,{already_started, Pid}}  -> {already_started, Pid};
    {error, Reason}                 -> ?e(start_error, Reason);
    Else                            -> ?e(start_error, Else)
  end;

start_cluster(Name, Args) -> 
  ?e(wrong_args, {Name, Args}).


%%
stop_cluster(Name) ->
      case supervisor:terminate_child(?MODULE, Name) of
        ok   -> supervisor:delete_child(?MODULE, Name);
        Else -> Else
      end.


