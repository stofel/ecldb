%% Compile to net code:
%
%-module(ClusterName).
%-export([route/1, mode/0, first/0, second/0, domains/0, nodes/0, rev/0]).
%route(KeyHash) -> ecldb_ring:route(?MODULE, mode(), KeyHash).
%mode()         -> norma|proxy|merge.
%first()        -> Ring1.
%second()       -> Ring2.
%domains()      -> Domains.
%nodes()        -> Nodes.
%rev            -> Rev.
%
%


-module(ecldb_compile).

-include("../include/ecldb.hrl").

-export([c/3, t/0]).

t() ->
  try
    FunList = [
      {mode,    norma},
      {first,   ecldb_ring:new()},
      {second,  ecldb_ring:new()},
      {domains, []},
      {nodes,   []},
      {rev,     ecldb_misc:rev()}
    ],
    c(ecldb_test, none, FunList)

  catch
    _:_ -> erlang:display(erlang:get_stacktrace())
  end.


c(ModuleName, Path, FunList) ->

  %% -module and -export forms {{{
  % define the module form
  Module  = erl_syntax:attribute(erl_syntax:atom(module), [erl_syntax:atom(ModuleName)]),
  ModuleForm = erl_syntax:revert(Module),

  % define the export form
  RouteExport = erl_syntax:arity_qualifier(erl_syntax:atom(route), erl_syntax:integer(1)),
  ExportFun   = fun(F) -> erl_syntax:arity_qualifier(erl_syntax:atom(F), erl_syntax:integer(0)) end,
  ExportList  = [RouteExport] ++ [ExportFun(FName) || {FName,_} <- FunList],
  Export      = erl_syntax:attribute(erl_syntax:atom(export), [erl_syntax:list(ExportList)]),
  ExportForm  = erl_syntax:revert(Export), 
  %% }}}

  % ecldb_ring:route(?MODULE, mode(), KeyHash). {{{
  RouteArgs   = [erl_syntax:atom(ModuleName),
                 erl_syntax:application(erl_syntax:atom(mode), []),
                 erl_syntax:variable("KeyHash")],
  RouteClause = erl_syntax:application(erl_syntax:atom(ecldb_ring), erl_syntax:atom(route), RouteArgs),
  RouteData   = erl_syntax:clause([erl_syntax:variable("KeyHash")], none, [RouteClause]),
  Route       = erl_syntax:function(erl_syntax:atom(route), [RouteData]),
  RouteForm   = erl_syntax:revert(Route),
  %% }}}

  %% data funs froms {{{
  ConstructFun = 
    fun({FName, FData}) -> 
      FBody = erl_syntax:clause(none, [erl_syntax:abstract(FData)]),
      FFun  = erl_syntax:function(erl_syntax:atom(FName), [FBody]),
      erl_syntax:revert(FFun)
    end,
  DataForms = [ConstructFun(F) || F <- FunList],
  %% }}}

  %% compile and load code {{{
  {ok, Mod, Bin} = compile:forms([ModuleForm, ExportForm, RouteForm] ++ DataForms),

  {module, ModuleName} = code:load_binary(Mod, [], Bin),
  _Res = case Path /= none of
    true  -> file:write_file(Path, Bin);
    false -> do_nothing
  end,
  %?INF("Create beams result", _Res),
  %% }}}

  ok.

