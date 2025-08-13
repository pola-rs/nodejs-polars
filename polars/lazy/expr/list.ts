import pli from "../../internals/polars_internal";
import { Series } from "../../series";
import type { ListFunctions } from "../../shared_traits";
import { _Expr, Expr, exprToLitOrExpr } from "../expr";
import { concatList } from "../functions";

/**
 * List functions for Lazy dataframes
 */
export interface ExprList extends ListFunctions<Expr> {}

export const ExprListFunctions = (_expr: any): ExprList => {
  const wrap = (method, ...args: any[]): Expr => {
    return _Expr(_expr[method](...args));
  };

  return {
    argMax() {
      return wrap("listArgMax") as any;
    },
    argMin() {
      return wrap("listArgMin") as any;
    },
    concat(other) {
      if (
        Array.isArray(other) &&
        !(
          Expr.isExpr(other[0]) ||
          Series.isSeries(other[0]) ||
          typeof other[0] === "string"
        )
      ) {
        return this.concat(pli.Series([other]));
      }
      let otherList: any;
      if (!Array.isArray(other)) {
        otherList = [other];
      } else {
        otherList = [...other];
      }
      otherList = [_Expr(_expr), ...otherList];

      return concatList(otherList) as any;
    },
    contains(item, nullsEqual?: boolean) {
      return wrap(
        "listContains",
        exprToLitOrExpr(item)._expr,
        nullsEqual ?? true,
      ) as any;
    },
    diff(n = 1, nullBehavior = "ignore") {
      return wrap("listDiff", n, nullBehavior) as any;
    },
    get(index: number | Expr, nullOnOob?: boolean) {
      if (Expr.isExpr(index)) {
        return wrap("listGet", index._expr, nullOnOob ?? true) as any;
      }
      return wrap("listGet", pli.lit(index), nullOnOob ?? true) as any;
    },
    head(n = 5) {
      return this.slice(0, n) as any;
    },
    tail(n = 5) {
      return this.slice(-n, n) as any;
    },
    eval(expr) {
      if (Expr.isExpr(expr)) {
        return wrap("listEval", expr._expr) as any;
      }
      return wrap("listEval", expr) as any;
    },
    first() {
      return this.get(0) as any;
    },
    join(options?) {
      if (typeof options === "string") {
        options = { separator: options };
      }
      options = options ?? {};
      let separator = options?.separator ?? ",";
      const ignoreNulls = options?.ignoreNulls ?? false;

      if (!Expr.isExpr(separator)) {
        separator = pli.lit(separator);
      }

      return wrap("listJoin", separator, ignoreNulls) as any;
    },
    last() {
      return this.get(-1) as any;
    },
    lengths() {
      return wrap("listLengths") as any;
    },
    max() {
      return wrap("listMax") as any;
    },
    mean() {
      return wrap("listMean") as any;
    },
    min() {
      return wrap("listMin") as any;
    },
    reverse() {
      return wrap("listReverse") as any;
    },
    shift(n) {
      return wrap("listShift", exprToLitOrExpr(n)._expr) as any;
    },
    slice(offset, length) {
      return wrap(
        "listSlice",
        exprToLitOrExpr(offset)._expr,
        exprToLitOrExpr(length)._expr,
      ) as any;
    },
    sort(descending: any = false) {
      return (
        typeof descending === "boolean"
          ? wrap("listSort", descending)
          : wrap("listSort", descending.descending)
      ) as any;
    },
    sum() {
      return wrap("listSum") as any;
    },
    unique() {
      return wrap("listUnique") as any;
    },
  };
};
