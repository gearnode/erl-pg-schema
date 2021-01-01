%% Copyright (c) 2020-2021 Nicolas Martyanoff <khaelin@gmail.com>.
%%
%% Permission to use, copy, modify, and/or distribute this software for any
%% purpose with or without fee is hereby granted, provided that the above
%% copyright notice and this permission notice appear in all copies.
%%
%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
%% SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR
%% IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

-module(pg_schema_versions).

-export([parse/1, format/1]).

-spec parse(file:name_all()) ->
        {ok, pg_schema:version()} | {error, term()}.
parse(String) when is_list(String) ->
  parse(list_to_binary(String));
parse(String) ->
  RE = "([0-9]{4})([0-9]{2})([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})Z",
  case re:run(String, RE, [{capture, all_but_first, binary}]) of
    {match, [YearBin, MonthBin, DayBin, HourBin, MinuteBin, SecondBin]} ->
      try
        Year = parse_version_part(YearBin, invalid_year, 0, 9999),
        Month = parse_version_part(MonthBin, invalid_month, 1, 12),
        Day = parse_version_part(DayBin, invalid_day, 1, 31),
        Hour = parse_version_part(HourBin, invalid_hour, 0, 23),
        Minute = parse_version_part(MinuteBin, invalid_minute, 0, 59),
        Second = parse_version_part(SecondBin, invalid_second, 0, 59),
        Date = {Year, Month, Day},
        case calendar:valid_date(Date) of
          true ->
            Datetime = {Date, {Hour, Minute, Second}},
            {ok, Datetime};
          false ->
            throw({error, invalid_date})
        end
      catch
        throw:{error, Reason} ->
          {error, Reason}
      end;
    nomatch ->
      {error, invalid_format}
  end.

-spec parse_version_part(binary(), Error, Min, Max) -> non_neg_integer() when
    Error :: atom(),
    Min :: non_neg_integer(),
    Max :: non_neg_integer().
parse_version_part(String, Error, Min, Max) ->
  try
    erlang:binary_to_integer(String)
  of
    I when I < Min; I > Max ->
      throw({error, Error});
    I ->
      I
  catch
    error:_ ->
      throw({error, invalid_format})
  end.

-spec format(pg_schema:version()) -> binary().
format({{Year, Month, Day}, {Hour, Minute, Second}}) ->
  Data = io_lib:format(<<"~4..0b~2..0b~2..0bT~2..0b~2..0b~2..0bZ">>,
                       [Year, Month, Day, Hour, Minute, Second]),
  iolist_to_binary(Data).
