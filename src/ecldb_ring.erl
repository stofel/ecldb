%%
%% Hash ring module
%%


-module(ecldb_ring).

-include("../include/ecldb.hrl").

-export([
    new/0,
    ring_size/1,
    add_domain/2,   %% Add domain to ring
    add_domain/3,
    del_domain/2,   %% Del domain from ring
    del_domain/3,
    list_domains/1,
    list_nodes/1,

    route/3,
    r/2, r/3
  ]).

-compile({no_auto_import, [size/1]}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Bisect exports {{{
%-export([new/2, new/3, insert/3, bulk_insert/2, append/3, find/2, foldl/3]).
%-export([next/2, next_nth/3, first/1, last/1, delete/2, compact/1, cas/4, update/4]).
%-export([serialize/1, deserialize/1, from_orddict/2, to_orddict/1, find_many/2]).
%-export([merge/2, intersection/1, intersection/2]).
%-export([expected_size/2, expected_size_mb/2, num_keys/1, size/1]).
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new() -> new(32,32).
  
ring_size(Ring) -> size(Ring).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ADD & DEL {{{
%% add N domains to ring, return count domains in ring
add_domain(DomainKey, Ring) ->
  add_domain(DomainKey, Ring, 16).

add_domain(DomainKey, Ring, N) when N > 0 ->
  Key = ecldb_misc:md5_hex(ecldb_misc:random_bin(16)),
  NewRing = insert(Ring, Key, DomainKey),
  add_domain(DomainKey, NewRing, N-1);
add_domain(DomainKey, Ring, _N) -> 
  DomainsCount = proplists:get_value(DomainKey, list_domains(Ring), 0),  
  {ok, Ring, DomainsCount}.


% Del N damains from ring, return count domains in ring
del_domain(DomainKey, Ring) -> 
  del_domain(DomainKey, Ring, 1).

del_domain(DomainKey, Ring, Num) -> 
  KFun = fun (K, V, Acc) when V == DomainKey -> [K|Acc]; (_, _, Acc) -> Acc end,
  Keys = ecldb_misc:shuffle_list(foldl(Ring, KFun, [])),
  DFun = fun
    (Fu, [K|RestK], R,  N) when N > 0 -> Fu(Fu, RestK, delete(R, K), N - 1);
    (_F, _,         R, _N) -> R
  end,
  NewRing = DFun(DFun, Keys, Ring, Num),
  DomainsCount = proplists:get_value(DomainKey, list_domains(NewRing), 0),
  {ok, NewRing, DomainsCount}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%
list_domains(Ring) ->
  F =
    fun(_Key, Value, Acc) ->
      Counter = proplists:get_value(Value, Acc, 0),
      lists:keystore(Value, 1, Acc, {Value, Counter+1})
    end,
  foldl(Ring, F, []).


%%
list_nodes(Ring) ->
  List = list_domains(Ring),
  F = fun
    ({{ok, #{node := Node}}, Len}, Acc) ->
        Counter = proplists:get_value(Node, Acc, 0),
        lists:keystore(Node, 1, Acc, {Node, Counter + Len});
    (_, Acc) -> Acc
  end,
  lists:foldl(F, [], List).


r(A, B) -> {A, B}.
r(A, B, C) -> {A, B, C}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ROUTING {{{
%% Route thru the rings
%% 1. use dynamic compiling beam with name same as claster name
%% 2. In dynamic compiling module:
%-define(MODE, proxy).
%route(KeyHash) -> ecldb_ring:route(?MODULE, KeyHash).
%mode()         -> ?MODE.
%first()        -> ring1.
%second()       -> ring2.
%domains()      -> domains.
%% 
-define(ZERO_HASH, <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>).

% Module = ClusterName
route(Module, norma, KeyHash) -> 
  case get_domain(Module, first, KeyHash) of
    {ok, Domain} -> {norma, Domain};
    Else         -> Else
  end;
route(Module, Mode,  KeyHash) -> 
  case [get_domain(Module, Ring, KeyHash) || Ring <- [first, second]] of
    [{ok,V1}, {ok,V2}] when V1 == V2 -> {norma, V1};
    [{ok,V1}, {ok,V2}]               -> {Mode, V1, V2};
    Else                             -> ?e(cluster_rings_error, ?p(Else))
  end.

%
get_domain(Module, Ring, KeyHash) ->
  case get_value_from_ring(Module, Ring, KeyHash) of
    {ok, ValueKey} ->
      case Module:domains() of
        #{ValueKey := Domain} -> {ok, Domain};
        error   -> ?e(domain_not_found)
      end;
    Else -> Else
  end.

%
get_value_from_ring(Module, Ring, KeyHash) ->
  case next(Module:Ring(), KeyHash) of
    {_Next, V} -> {ok, V};
    not_found  -> %% TODO optimize this
      case next(Module:Ring(), ?ZERO_HASH) of
        {_Next, V} -> {ok, V};
        not_found  -> ?e(domain_not_found)
      end
  end.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%








%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Bisect {{{
%% @doc: Space-efficient dictionary implemented using a binary
%%
%% This module implements a space-efficient dictionary with no
%% overhead per entry. Read and write access is O(log n).
%%
%% Keys and values are fixed size binaries stored ordered in a larger
%% binary which acts as a sparse array. All operations are implemented
%% using a binary search.
%%
%% As large binaries can be shared among processes, there can be
%% multiple concurrent readers of an instance of this structure.
%%
%% serialize/1 and deserialize/1
%-module(ecl_bisect).
%-author('Knut Nesheim <knutin@gmail.com>').
%-compile({no_auto_import, [size/1]}).

%%
%% TYPES
%%

-type key_size()   :: pos_integer().
-type value_size() :: pos_integer().
-type block_size() :: pos_integer().

-type key()        :: binary().
-type value()      :: binary().

-type index()      :: pos_integer().

-record(bindict, {
          key_size   :: key_size(),
          value_size :: value_size(),
          block_size :: block_size(),
          b          :: binary()
}).
-type bindict() :: #bindict{}.


%%
%% API
%%

-spec new(key_size(), value_size()) -> bindict().
%% @doc: Returns a new empty dictionary where where the keys and
%% values will always be of the given size.
new(KeySize, ValueSize) when is_integer(KeySize)
                             andalso is_integer(ValueSize) ->
    new(KeySize, ValueSize, <<>>).

-spec new(key_size(), value_size(), binary()) -> bindict().
%% @doc: Returns a new dictionary with the given data
new(KeySize, ValueSize, Data) when is_integer(KeySize)
                                   andalso is_integer(ValueSize)
                                   andalso is_binary(Data) ->
    #bindict{key_size = KeySize,
             value_size = ValueSize,
             block_size = KeySize + ValueSize,
             b = Data}.


-spec insert(bindict(), key(), value()) -> bindict().
%% @doc: Inserts the key and value into the dictionary. If the size of
%% key and value is wrong, throws badarg. If the key is already in the
%% array, the value is updated.
insert(B, K, V) when byte_size(K) =/= B#bindict.key_size orelse
                     byte_size(V) =/= B#bindict.value_size ->
    erlang:error(badarg);

insert(#bindict{b = <<>>} = B, K, V) ->
    B#bindict{b = <<K/binary, V/binary>>};

insert(B, K, V) ->
    Index = index(B, K),
    LeftOffset = Index * B#bindict.block_size,
    RightOffset = byte_size(B#bindict.b) - LeftOffset,

    KeySize = B#bindict.key_size,
    ValueSize = B#bindict.value_size,

    case B#bindict.b of
        <<Left:LeftOffset/binary, K:KeySize/binary, _:ValueSize/binary, Right/binary>> ->
            B#bindict{b = iolist_to_binary([Left, K, V, Right])};

        <<Left:LeftOffset/binary, Right:RightOffset/binary>> ->
            B#bindict{b = iolist_to_binary([Left, K, V, Right])}
    end.



-spec delete(bindict(), key()) -> bindict().
delete(B, K) ->
    LeftOffset = index2offset(B, index(B, K)),
    KeySize = B#bindict.key_size,
    ValueSize = B#bindict.value_size,

    case B#bindict.b of
        <<Left:LeftOffset/binary, K:KeySize/binary, _:ValueSize/binary, Right/binary>> ->
            B#bindict{b = <<Left/binary, Right/binary>>};
        _ ->
            erlang:error(badarg)
    end.

-spec next(bindict(), key()) -> {key(), value()} | not_found.
%% @doc: Returns the next larger key and value associated with it or
%% 'not_found' if no larger key exists.
next(B, K) ->
  next_nth(B, K, 1).

%% @doc: Returns the nth next larger key and value associated with it
%% or 'not_found' if it does not exist.
-spec next_nth(bindict(), key(), non_neg_integer()) -> value() | not_found.
next_nth(B, K, Steps) ->
    at(B, index(B, inc(K)) + Steps - 1).



-spec first(bindict()) -> {key(), value()} | not_found.
%% @doc: Returns the first key-value pair or 'not_found' if the dict is empty
first(B) ->
    at(B, 0).

-spec foldl(bindict(), fun(), any()) -> any().
foldl(B, F, Acc) ->
    case first(B) of
        {Key, Value} ->
            do_foldl(B, F, Key, F(Key, Value, Acc));
        not_found ->
            Acc
    end.

do_foldl(B, F, PrevKey, Acc) ->
    case next(B, PrevKey) of
        {Key, Value} ->
            do_foldl(B, F, Key, F(Key, Value, Acc));
        not_found ->
            Acc
    end.

size(#bindict{b = B}) ->
    erlang:byte_size(B).


at(B, I) ->
    Offset = index2offset(B, I),
    KeySize = B#bindict.key_size,
    ValueSize = B#bindict.value_size,
    case B#bindict.b of
        <<_:Offset/binary, Key:KeySize/binary, Value:ValueSize/binary, _/binary>> ->
            {Key, Value};
        _ ->
            not_found
    end.


%%
%% INTERNAL HELPERS
%%

index2offset(_, 0) -> 0;
index2offset(B, I) -> I * B#bindict.block_size.

%% @doc: Uses binary search to find the index of the given key. If the
%% key does not exist, the index where it should be inserted is
%% returned.
-spec index(bindict(), key()) -> index().
index(<<>>, _) ->
    0;
index(B, K) ->
    N = byte_size(B#bindict.b) div B#bindict.block_size,
    index(B, 0, N, K).

index(_B, Low, High, _K) when High =:= Low ->
    Low;

index(_B, Low, High, _K) when High < Low ->
    -1;

index(B, Low, High, K) ->
    Mid = (Low + High) div 2,
    MidOffset = index2offset(B, Mid),

    KeySize = B#bindict.key_size,
    case byte_size(B#bindict.b) > MidOffset of
        true ->
            <<_:MidOffset/binary, MidKey:KeySize/binary, _/binary>> = B#bindict.b,

            if
                MidKey > K ->
                    index(B, Low, Mid, K);
                MidKey < K ->
                    index(B, Mid + 1, High, K);
                MidKey =:= K ->
                    Mid
            end;
        false ->
            Mid
    end.

inc(B) ->
    IncInt = binary:decode_unsigned(B) + 1,
    SizeBits = erlang:size(B) * 8,
    <<IncInt:SizeBits>>.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
