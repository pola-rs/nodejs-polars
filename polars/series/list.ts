import { type Series, _Series } from ".";
import { exprToLitOrExpr } from "..";
import { col } from "../lazy/functions";
import type { ListFunctions } from "../shared_traits";

// export type ListNamespace = ListFunctions<Series>;

/**
 * List functions for Series
 */
export interface SeriesListFunctions extends ListFunctions<Series> {}

export const SeriesListFunctions = (_s): SeriesListFunctions => {
  const wrap = (method, ...args) => {
    const s = _Series(_s);

    return s
      .toFrame()
      .select(
        col(s.name)
          .lst[method](...args)
          .as(s.name),
      )
      .getColumn(s.name);
  };

  return {
    argMax() {
      return wrap("argMax");
    },
    argMin() {
      return wrap("argMin");
    },
    concat(other) {
      return wrap("concat", other);
    },
    contains(item) {
      return wrap("contains", item);
    },
    diff(n, nullBehavior) {
      return wrap("diff", n, nullBehavior);
    },
    get(index: number) {
      return wrap("get", index);
    },
    eval(expr, parallel) {
      return wrap("eval", expr, parallel);
    },
    first() {
      return wrap("get", 0);
    },
    head(n = 5) {
      return this.slice(0, n);
    },
    tail(n = 5) {
      return this.slice(-n, n);
    },
    join(options?) {
      return wrap("join", options);
    },
    last() {
      return wrap("get", -1);
    },
    lengths() {
      return wrap("lengths");
    },
    max() {
      return wrap("max");
    },
    mean() {
      return wrap("mean");
    },
    min() {
      return wrap("min");
    },
    reverse() {
      return wrap("reverse");
    },
    shift(n) {
      return wrap("shift", n);
    },
    slice(offset, length) {
      return wrap("slice", offset, length);
    },
    sort(descending: any = false) {
      return typeof descending === "boolean"
        ? wrap("sort", descending)
        : wrap("sort", descending.descending ?? descending.reverse ?? false);
    },
    sum() {
      return wrap("sum");
    },
    unique() {
      return wrap("unique");
    },
  };
};
