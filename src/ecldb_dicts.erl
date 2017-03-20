%%
%% Manage cluster dicts nodes and domains, compiled and precompiled.
%% Do here all nodes and domains add/del and run ecldb_cluster:compile/1
%% 

-module(ecldb_dicts).

-include("../include/ecldb.hrl").

-export([show_nodes/1,   add_node/2,   del_node/2,   flush_node/2,   flush_nodes/1]).
-export([show_domains/1, add_domain/2, del_domain/2, flush_domain/2, flush_domains/1]).
-export([flush_all/1]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% show_... {{{
%
show_nodes(Cluster) when is_atom(Cluster) ->
  gen_server:call(Cluster, show_nodes);
show_nodes({call, S = #{nodes := {ANs, DNs}, name := Name}}) ->
  Reply = #{
    compiled => Name:nodes(), 
    add      => ANs, 
    del      => DNs},
  {reply, Reply, S}.

%
show_domains(Cluster) when is_atom(Cluster) ->
  gen_server:call(Cluster, show_domains);
show_domains({call, S = #{domains := {ADs, DDs}, name := Name}}) ->
  Reply = #{compiled => Name:domains(), 
            add      => ADs,
            del      => DDs},
  {reply, Reply, S}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% add_... {{{
%
add_node(Cluster, Node) when is_atom(Cluster), is_atom(Node) ->
  gen_server:call(Cluster, {add_node, Node});
add_node({call, S = #{nodes := {ANs, DNs} }}, Node) ->
  NewNodes = 
    case lists:member(Node, ANs) of
      true  -> {ANs, DNs};
      false -> {[Node|ANs], DNs}
    end,
  {reply, ok, S#{nodes := NewNodes}}.

%
add_domain(Cluster, Domain) when is_atom(Cluster), is_atom(Domain) ->
  add_domain(Cluster, #{node => node(), name => Domain});
add_domain(Cluster, Domain = #{node := _, name := _}) when is_atom(Cluster) ->
  gen_server:call(Cluster, {add_domain, Domain});
add_domain({call, S = #{domains := {ADs, DDs} }}, Domain) ->
  NewDomains = 
    case lists:member(Domain, ADs) of 
      true  -> {ADs, DDs};
      false -> {[Domain|ADs], DDs}
    end,
  {reply, ok, S#{domains := NewDomains}}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% del_... {{{
%
del_node(Cluster, Node) when is_atom(Cluster), is_atom(Node) ->
  gen_server:call(Cluster, {del_node, Node});
del_node({call, S = #{nodes := {ANs, DNs} }}, Node) ->
  NewNodes = 
    case lists:member(Node, DNs) of
      true  -> {ANs, DNs};
      false -> {ANs, [Node|DNs]}
    end,
  {reply, ok, S#{nodes := NewNodes}}.

%
del_domain(Cluster, Domain) when is_atom(Cluster), is_atom(Domain) ->
  del_domain(Cluster, #{node => node(), name => Domain});
del_domain(Cluster, Domain) when is_atom(Cluster) ->
  gen_server:call(Cluster, {del_domain, Domain});
del_domain({call, S = #{domains := {ADs, DDs} }}, Domain) ->
  NewDomains = 
    case lists:member(Domain, DDs) of 
      true  -> {ADs, DDs};
      false -> {ADs, [Domain|DDs]}
    end,
  {reply, ok, S#{domains := NewDomains}}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% flush_... {{{

%%
flush_all(Cluster) when is_atom(Cluster) ->
  gen_server:call(Cluster, flush_all);
flush_all({call, S}) ->
  {reply, ok, S#{nodes := {[],[]}, domains := {[],[]} }}.


%%
flush_nodes(Cluster) when is_atom(Cluster) ->
  gen_server:call(Cluster, flush_nodes);
flush_nodes({call, S}) ->
  {reply, ok, S#{nodes := {[],[]} }}.


%%
flush_domains(Cluster) when is_atom(Cluster) ->
  gen_server:call(Cluster, flush_domains);
flush_domains({call, S}) ->
  {reply, ok, S#{domains := {[],[]} }}.



%%
flush_node(Cluster, Node) when is_atom(Cluster), is_atom(Node) ->
  gen_server:call(Cluster, {flush_node, Node});
flush_node({call, S = #{nodes := {ANs, DNs} }}, Node) ->
  NewNodes = {lists:delete(Node, ANs), lists:delete(Node, DNs)},
  {reply, ok, S#{nodes := NewNodes}}.


%%
flush_domain(Cluster, Domain) when is_atom(Cluster), is_atom(Domain) ->
  DomainMap = #{name => Domain, node => node()},
  flush_domain(Cluster, DomainMap);
flush_domain(Cluster, Domain) when is_atom(Cluster), is_map(Domain) ->
  gen_server:call(Cluster, {flush_domain, Domain});
flush_domain({call, S = #{domains := {ADs, DDs} }}, Domain) ->
  NewDomains = {lists:delete(Domain, ADs), lists:delete(Domain, DDs)},
  {reply, ok, S#{domains := NewDomains}}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% reg unreg {{{
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
