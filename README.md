ecldb
=====

Erlang cluster process-value data base

Build
-----

    Add https://github.com/zimad/ecldb.git to deps in your release.
    Add ecldb in you_project.app.src in applications section.
    
Usage
-----

    %% Example of start cluster for two nodes with ten domains on each
    %% Worker process start with gen_server:start(test_session, Args, [])
    start_cluster() ->
        CName = ecldb_test,
        Args = #{
          new  => do_try, %% true|false|do_try Start or not new cluster. 
                          %% if do_try, look cluster file existance, if not exists start new cluster
          ma   => {test_session, #{}}, %% Module Arguments to start worker process
          opt  => empty_opts,
          path => "./"},  %% Path for dinamic compile files with rings and monitoring

        Nodes = ['node01@10.0.1.3', 'node02@10.0.1.4'],
        %% ted = test ecldb domain
        Domes = [ted01, ted02, ted03, ted04, ted05, ted06, ted07, ted08, ted09, ted10],

        %% Start Cluster
        [{ok, _} = rpc:call(N, ecldb, start_cluster, [CName, Args]) || N <- Nodes],
        %% Add Nodes
        [ok      = ecldb:add_node(CName, N) || N <- Nodes],
        %% Add Domains
        [{ok, _} = rpc:call(N, ecldb, add_domain, [CName, D]) || D <- Domes, N <- Nodes],
        %% Apply changes
        ok = ecldb:merge(CName),
        ok = ecldb:norma(CName),
        ok.


    %% Worker process adaptation
    %% Add registration to init function and save Reg#{pid => self()} map in workers State
    init(Args = #{reg := Reg}) ->
      ...
      ecldb_domain:reg(Reg#{reply => Reply}),
      ...
      {ok, #{req => Reg#{pid => self()}}}.


    %% Add unregistration to timeout function or before terminate procces
    timeout(S = #{reg := Reg}) ->
      ...
      ecldb_domain:unreg(Reg)
      ...


    %% Example of send request
    test_send() ->
      CName = ecldb_test,
      Key   = <<"123">>,
      Msg   = ping,
      Flags = #{mode => init},
      ecldb:call(CName, Key, Msg, Flags).

