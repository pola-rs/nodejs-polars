import pli from "../../internals/polars_internal";
import { Series } from "../../series";
import type { ListFunctions } from "../../shared_traits";
import { Expr, _Expr, exprToLitOrExpr } from "../expr";
import { concatList } from "../functions";

/**
 * List functions for Lazy dataframes
 */
export interface ExprList extends ListFunctions<Expr> {}
// export interface ListNamespace extends ListFunctions<Expr> {}

export const ExprListFunctions = (_expr: any): ExprList => {
  const wrap = (method, ...args: any[]): Expr => {
    return _Expr(_expr[method](...args));
  };

  return {
    argMax() {
      return wrap("listArgMax");
    },
    argMin() {
      return wrap("listArgMin");
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

      return concatList(otherList);
    },
    contains(item) {
      return wrap("listContains", exprToLitOrExpr(item)._expr);
    },
    diff(n = 1, nullBehavior = "ignore") {
      return wrap("listDiff", n, nullBehavior);
    },
    get(index: number | Expr) {
      if (Expr.isExpr(index)) {
        return wrap("listGet", index._expr, true);
      }
      return wrap("listGet", pli.lit(index), true);
    },
    head(n = 5) {
      return this.slice(0, n);
    },
    tail(n = 5) {
      return this.slice(-n, n);
    },
    eval(expr, parallel = true) {
      if (Expr.isExpr(expr)) {
        return wrap("listEval", expr._expr, parallel);
      }
      return wrap("listEval", expr, parallel);
    },
    first() {
      return this.get(0);
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

      return wrap("listJoin", separator, ignoreNulls);
    },
    last() {
      return this.get(-1);
    },
    lengths() {
      return wrap("listLengths");
    },
    max() {
      return wrap("listMax");
    },
    mean() {
      return wrap("listMean");
    },
    min() {
      return wrap("listMin");
    },
    reverse() {
      return wrap("listReverse");
    },
    shift(n) {
      return wrap("listShift", exprToLitOrExpr(n)._expr);
    },
    slice(offset, length) {
      return wrap(
        "listSlice",
        exprToLitOrExpr(offset)._expr,
        exprToLitOrExpr(length)._expr,
      );
    },
    sort(descending: any = false) {
      return typeof descending === "boolean"
        ? wrap("listSort", descending)
        : wrap("listSort", descending.descending);
    },
    sum() {
      return wrap("listSum");
    },
    unique() {
      return wrap("listUnique");
    },
  };
};
