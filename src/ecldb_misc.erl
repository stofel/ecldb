-module(ecldb_misc).

%
-compile(export_all).

-include("../include/ecldb.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%               
%% Randoms                                           
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%               
%
random_bin(N)  -> list_to_binary(random_str(N)).
%% random string
random_str(short) -> random_str(4);
random_str(long)  -> random_str(8);
random_str(Length) ->
  AllowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789",
  lists:foldl(
    fun(_, Acc) ->
      [lists:nth(rand:uniform(length(AllowedChars)), AllowedChars)] ++ Acc
    end, [], lists:seq(1, Length)).

%
random_int(1) -> 1;
random_int(N) -> rand:uniform(N).
random_int(S, T) when S > 0, T > 0, T > S -> rand:uniform(T-S+1)+S-1.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%               
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%               



rev() -> 
  Hash = ecldb_misc:random_bin(4),
  <<Hash/binary, "_1">>.
rev(<<_:5/binary, Rev/binary>>) ->
  RevNum = integer_to_binary(binary_to_integer(Rev) + 1),
  Hash = ecldb_misc:random_bin(4),
  <<Hash/binary, "_", RevNum/binary>>.


% recursion for function list
c_r(FunList, Args) ->
  case_recursion(FunList, Args).
case_recursion(FunList, Args) ->
  Fun = fun
            (F, [N|R], Acc = #{status := ok}) -> F(F, R, apply(N, [Acc]));
            (_F, _,    Acc = #{status := done}) -> Acc#{status := ok};
            (_F, _,    Acc) -> Acc
        end,
  Fun(Fun, FunList, Args).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% md5
md5_hex(Bin) ->
  list_to_binary(lists:flatten(list_to_hex(binary_to_list(erlang:md5(Bin))))).

list_to_hex(L) ->
  lists:map(fun(X) -> int_to_hex(X) end, L).

int_to_hex(N) when N < 256 ->
  [hex(N div 16), hex(N rem 16)].

hex(N) when N < 10 ->
  $0+N;
hex(N) when N >= 10, N < 16 ->
  $a + (N-10).
% md5
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



% This method of shuffling produces biased results unless you can guarantee 
% that no duplicate random numbers are generated (which you can not with rand:uniform/0
shuffle_list(L) -> [X||{_,X} <- lists:sort([{rand:uniform(), E} || E <- L])].

