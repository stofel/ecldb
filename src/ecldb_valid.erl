%%
%% Vlidate objects
%%

-module(ecldb_valid).

-include("../include/ecldb.hrl").

-export([is_node/1, is_domain/1]).


%
% Node is atom(). Node should be alive
is_node(Node) when is_atom(Node) ->
  case net_adm:ping(Node) of
    pong -> ok;
    pang -> ?e(node_ping_error)
  end;
is_node(Node) -> 
  ?e(wrong_node_name, Node).
 

%
% Domain = #{node := Node, domain := Domain}, where ok = is_node(Node)
is_domain(Domain) ->
  case Domain of
    #{node := Node, name := Name} when is_atom(Name) -> is_node(Node);
    _ -> ?e(wrong_domain_name)
  end.
