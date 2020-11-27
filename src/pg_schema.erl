%% Copyright (c) 2020 Nicolas Martyanoff <khaelin@gmail.com>.
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
%% REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
%% AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
%% INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
%% LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
%% OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
%% PERFORMANCE OF THIS SOFTWARE.

-module(pg_schema).

-include_lib("kernel/include/logger.hrl").

-export([update/3]).

-export_type([name/0, version/0, migration/0]).

-type name() :: binary().

-type version() :: calendar:datetime().

-type migration() :: #{name := name(),
                       version := version(),
                       code := binary()}.

-spec domain() -> [atom()].
domain() ->
  [pg_schema].

-spec update(pgc:pool_id(), App :: atom(), name()) ->
        ok | {error, term()}.
update(Pool, App, Name) ->
  DirPath = pg_schema_migrations:migration_directory(App, Name),
  case pg_schema_migrations:load(Name, DirPath) of
    {ok, Migrations} ->
      do_update(Pool, Name, Migrations);
    {error, Reason} ->
      {error, Reason}
  end.

-spec do_update(pgc:pool_id(), name(), [migration()]) -> ok | {error, term()}.
do_update(_Pool, Name, []) ->
  ?LOG_INFO("no migration available", #{domain => domain(), schema => Name});
do_update(Pool, Name, Migrations) ->
  F = fun (C) ->
          try
            %% Take a lock to make sure only one application tries to update the
            %% schema at the same time.
            take_advisory_lock(C),

            %% Create the table if it does not exist. Note that we use a
            %% separate client because we need each migration, which will be
            %% executed in its own transaction (i.e. before the the end of
            %% the main transaction), to see it.
            create_version_table(Pool),

            %% Load currently applied versions.
            Versions = load_versions(C, Name),
            length(Versions) =:= 0 andalso
              ?LOG_INFO("database uninitialized",
                        #{domain => domain(), schema => Name}),

            %% Find missing migrations
            Migrations2 = unapplied_migrations(Migrations, Versions),

            %% Apply them in order
            apply_migrations(Pool, pg_schema_migrations:sort(Migrations2))
          catch
            throw:{error, Error} ->
              {error, Error}
          end
      end,
  pgc:with_transaction(Pool, F).

-spec take_advisory_lock(pgc_client:ref()) -> ok.
take_advisory_lock(Client) ->
  %% Advisory locks are identified by two 32 bit integers. We arbitrarily
  %% reserve the first one for all locks taken by the pg_schema application.
  Query = "SELECT pg_advisory_xact_lock($1, $2)",
  must_exec(Client, Query, [16#00ff, 16#0001]).

-spec create_version_table(pgc:pool_id()) -> ok.
create_version_table(Pool) ->
  Query =
    "CREATE TABLE IF NOT EXISTS schema_versions("
    "  schema VARCHAR NOT NULL,"
    "  version TIMESTAMP,"
    "  migration_date TIMESTAMP NOT NULL"
    "    DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),"
    "  PRIMARY KEY (schema, version)"
    ")",
  case pgc:with_client(Pool, fun (C) -> must_exec(C, Query) end) of
    ok ->
      ok;
    {error, Reason} ->
      throw({error, Reason})
  end.

-spec load_versions(pgc_client:ref(), name()) -> [version()].
load_versions(Client, Name) ->
  Query = "SELECT version FROM schema_versions WHERE schema = $1",
  case pgc:query(Client, Query, [Name]) of
    {ok, _, Rows, _} ->
      [pgc_utils:timestamp_to_erlang_datetime(T) || [T] <- Rows];
    {error, Error} ->
      throw({error, Error})
  end.

-spec unapplied_migrations([migration()], [version()]) -> [migration()].
unapplied_migrations(Migrations, Versions) ->
  [M || M = #{version := V} <- Migrations, not lists:member(V, Versions)].

-spec apply_migrations(pgc:pool_id(), [migration()]) -> ok.
apply_migrations(Pool, Migrations) ->
  F = fun (Migration = #{name := Name, version := Version}) ->
          ?LOG_INFO("applying migration ~s",
                    [pg_schema_versions:format(Version)],
                    #{domain => domain(), schema => Name}),
          apply_migration(Pool, Migration)
      end,
  lists:foreach(F, Migrations).

-spec apply_migration(pgc:pool_id(), migration()) -> ok.
apply_migration(Pool, #{name := Name, version := Version, code := Code}) ->
  F = fun (C) ->
          must_simple_exec(C, Code),
          Query =
            "INSERT INTO schema_versions (schema, version)"
            "  VALUES ($1, $2)",
          must_exec(C, Query,
                    [Name, {timestamp, pgc_utils:timestamp(Version)}])
      end,
  pgc:with_transaction(Pool, F).

-spec must_simple_exec(pgc_client:ref(), unicode:chardata()) -> ok.
must_simple_exec(Client, Query) ->
  case pgc:simple_exec(Client, Query) of
    {ok, _} ->
      ok;
    {error, Error} ->
      throw({error, Error})
  end.

-spec must_exec(pgc_client:ref(), unicode:chardata()) -> ok.
must_exec(Client, Query) ->
  case pgc:exec(Client, Query) of
    {ok, _} ->
      ok;
    {error, Error} ->
      throw({error, Error})
  end.

-spec must_exec(pgc_client:ref(), unicode:chardata(), [term()]) -> ok.
must_exec(Client, Query, Params) ->
  case pgc:exec(Client, Query, Params) of
    {ok, _} ->
      ok;
    {error, Error} ->
      throw({error, Error})
  end.
