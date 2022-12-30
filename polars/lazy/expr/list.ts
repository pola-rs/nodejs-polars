import { Expr, _Expr } from "../expr";
import { ListFunctions } from "../../shared_traits";
import { Series } from "../../series";
import pli from "../../internals/polars_internal";
import { concatList } from "../functions";

/**
 * namespace containing expr list functions
 */
export interface ExprList extends ListFunctions<Expr> {}
export const ExprListFunctions = (_expr: any): ExprList => {
  const wrap = (method, ...args: any[]): Expr => {
    return _Expr(_expr[method](...args));
  };

  return {
    argMax() {
      return wrap("lstArgMax");
    },
    argMin() {
      return wrap("lstArgMin");
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
      let otherList;
      if (!Array.isArray(other)) {
        otherList = [other];
      } else {
        otherList = [...other];
      }
      otherList = [_Expr(_expr), ...otherList];

      return concatList(otherList);
    },
    contains(item) {
      throw new Error("not yet implemented");
    },
    diff(n = 1, nullBehavior = "ignore") {
      return wrap("lstDiff", n, nullBehavior);
    },
    get(index: number | Expr) {
      if (Expr.isExpr(index)) {
        return wrap("lstGet", index._expr);
      } else {
        return wrap("lstGet", pli.lit(index));
      }
    },
    head(n = 5) {
      return this.slice(0, n);
    },
    tail(n = 5) {
      return this.slice(-n, n);
    },
    eval(expr, parallel = true) {
      if (Expr.isExpr(expr)) {
        return wrap("lstEval", expr._expr, parallel);
      } else {
        return wrap("lstEval", expr, parallel);
      }
    },
    first() {
      return this.get(0);
    },
    join(separator = ",") {
      return wrap("lstJoin", separator);
    },
    last() {
      return this.get(-1);
    },
    lengths() {
      return wrap("lstLengths");
    },
    max() {
      return wrap("lstMax");
    },
    mean() {
      return wrap("lstMean");
    },
    min() {
      return wrap("lstMin");
    },
    reverse() {
      return wrap("lstReverse");
    },
    shift(n) {
      return wrap("lstShift", n);
    },
    slice(offset, length) {
      return wrap("lstSlice", offset, length);
    },
    sort(reverse: any = false) {
      return typeof reverse === "boolean"
        ? wrap("lstSort", reverse)
        : wrap("lstSort", reverse.reverse);
    },
    sum() {
      return wrap("lstSum");
    },
    unique() {
      return wrap("lstUnique");
    },
  };
};
