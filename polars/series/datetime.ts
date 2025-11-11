import { col } from "../lazy/functions";
import type { DateFunctions } from "../shared_traits";
import { _Series, type Series } from ".";
/**
 * DateTime functions for Series
 */
export interface SeriesDateFunctions extends DateFunctions<Series> {}

export const SeriesDateFunctions = (_s): SeriesDateFunctions => {
  const wrap = (method, ...args: any[]): Series => {
    return _Series(_s[method](...args)) as any;
  };

  const wrapNullArgs = (method: string) => () => wrap(method);

  return {
    day: wrapNullArgs("day"),
    hour: wrapNullArgs("hour"),
    minute: wrapNullArgs("minute"),
    month: wrapNullArgs("month"),
    nanosecond: wrapNullArgs("nanosecond"),
    ordinalDay: wrapNullArgs("ordinalDay"),
    second: wrapNullArgs("second"),
    strftime: (fmt) => wrap("strftime", fmt),
    timestamp: wrapNullArgs("timestamp"),
    week: wrapNullArgs("week"),
    weekday: wrapNullArgs("weekday"),
    year: wrapNullArgs("year"),
    truncate: (every) =>
      _Series(_s)
        .toFrame()
        .select(col(_s.name).dt.truncate(every))
        .getColumn(_s.name),
    round: (every) =>
      _Series(_s)
        .toFrame()
        .select(col(_s.name).dt.round(every))
        .getColumn(_s.name),
  };
};
