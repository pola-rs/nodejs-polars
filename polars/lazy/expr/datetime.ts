import type { DateFunctions } from "../../shared_traits";
import { _Expr, type Expr, exprToLitOrExpr } from "../expr";

/**
 * DateTime functions for expression
 */
export interface ExprDateTime extends DateFunctions<Expr> {}

export const ExprDateTimeFunctions = (_expr: any): ExprDateTime => {
  const wrap = (method, ...args: any[]): Expr => {
    return _Expr(_expr[method](...args));
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
    truncate: (every) => wrap("dtTruncate", exprToLitOrExpr(every)._expr),
    round: (every) => wrap("dtRound", exprToLitOrExpr(every)._expr),
    replaceTimeZone: (
      timeZone: string,
      ambiguous: string | Expr = "raise",
      nonExistent: string = "raise",
    ) =>
      wrap(
        "dtReplaceTimeZone",
        timeZone,
        exprToLitOrExpr(ambiguous)._expr,
        nonExistent,
      ),
    convertTimeZone: (timeZone: string) => wrap("dtConvertTimeZone", timeZone),
  };
};
