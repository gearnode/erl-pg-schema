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

-module(pg_schema_migrations).

-export([migration_directory/2, load/2, sort/1]).

-spec migration_directory(App :: atom(), pg_schema:name()) ->
        file:filename_all().
migration_directory(App, Name) ->
  case code:priv_dir(App) of
    {error, bad_name} ->
      error({unknown_application, App});
    Path ->
      filename:join([Path, "pg-schemas", Name])
  end.

-spec load(pg_schema:name(), file:filename_all()) ->
        {ok, [pg_schema:migration()]} | {error, Reason} when
    Reason :: {list_dir, term()}.
load(Name, DirPath) ->
  case file:list_dir(DirPath) of
    {ok, Filenames0} ->
      Filenames = lists:sort(Filenames0),
      Fun = fun (Filename, Acc) ->
                case filename:extension(Filename) of
                  ".sql" ->
                    VersionString = filename:rootname(Filename),
                    case pg_schema_versions:parse(VersionString) of
                      {ok, Version} ->
                        Path = filename:join(DirPath, Filename),
                        Code = case file:read_file(Path) of
                                 {ok, Data} -> Data;
                                 {error, Reason} -> throw({error, Reason})
                               end,
                        Migration = #{name => Name,
                                      version => Version,
                                      code => Code},
                        [Migration | Acc];
                      {error, Reason} ->
                        {error, {invalid_version, Reason, VersionString}}
                    end;
                  _ ->
                    Acc
                end
            end,
      try
        Migrations = lists:foldl(Fun, [], Filenames),
        {ok, Migrations}
      catch
        throw:{error, Reason} ->
          {error, Reason}
      end;
    {error, Reason} ->
      {error, {list_dir, Reason}}
  end.

-spec sort([pg_schema:migration()]) -> [pg_schema:migration()].
sort(Migrations) ->
  lists:sort(fun (#{version := V1}, #{version := V2}) -> V1 =< V2 end,
             Migrations).
