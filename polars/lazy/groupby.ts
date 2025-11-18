import pli from "../internals/polars_internal";
import { selectionToExprList } from "../utils";
import { _LazyDataFrame, type LazyDataFrame } from "./dataframe";
import type { Expr } from "./expr";

/** @ignore */
export const _LazyGroupBy = (_lgb: any): LazyGroupBy => {
  return {
    agg(...aggs: Expr[]) {
      aggs = selectionToExprList(aggs, false);
      const ret = _lgb.agg(aggs.flat());

      return _LazyDataFrame(ret);
    },
    head(n = 5) {
      return _LazyDataFrame(_lgb.head(n));
    },
    len(name?: string) {
      const expr = name ? pli.len().alias(name) : pli.len();
      return _LazyDataFrame(_lgb.agg([expr]));
    },
    tail(n = 5) {
      return _LazyDataFrame(_lgb.tail(n));
    },
  };
};

/**
 * LazyGroupBy
 * @category lazy
 */
export interface LazyGroupBy {
  /**
   * Aggregate the groupby.
   */
  agg(...aggs: Expr[]): LazyDataFrame;
  /**
   * Return the first n rows of the groupby.
   * @param n number of rows to return
   */
  head(n?: number): LazyDataFrame;
  /**
   * Return the number of rows in each group.
   * @param name Assign a name to the resulting column; if unset, defaults to "len".
   */
  len(name?: string): LazyDataFrame;
  /**
   * Return the last n rows of the groupby.
   * @param n number of rows to return
   */
  tail(n?: number): LazyDataFrame;
}
