
%% Types
-type err() :: {err, {atom(), binary()}}.



% NOW time in seconds
-define(mnow, erlang:system_time(millisecond)).
-define(now,  erlang:system_time(seconds)).
-define(stime, ecldb_dh_date:format("Y-m-d H:i:s",{date(),time()})).

%% IF
-define(IF(Cond, TrueVal, FalseVal), case Cond of true -> TrueVal; false -> FalseVal end).

%% Point
-define(p,         list_to_binary(io_lib:format("Mod:~w line:~w", [?MODULE,?LINE]))).
-define(p(Reason), list_to_binary(io_lib:format("Mod:~w line:~w ~100P", [?MODULE,?LINE, Reason, 300]))).

-define(e(ErrCode), {err, {ErrCode, ?p}}).
-define(e(ErrCode, Reason), {err, {ErrCode, ?p(Reason)}}).


%% Log messages
-define(INF(Str, Term), io:format("~p ECLDB: ~p:~p ~p ~100P~n",
                                  [?stime, ?MODULE, ?LINE, Str, Term, 300])).
-define(INF(X1, X2, Str, Term), ?IF(X1 == X2, ?INF(Str, Term), do_nothing)).

