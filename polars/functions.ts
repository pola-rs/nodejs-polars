import { type DataFrame, _DataFrame } from "./dataframe";
/* eslint-disable no-redeclare */
import { jsTypeToPolarsType } from "./internals/construction";
import pli from "./internals/polars_internal";
import { type LazyDataFrame, _LazyDataFrame } from "./lazy/dataframe";
import { type Series, _Series } from "./series";
import type { ConcatOptions } from "./types";
import { isDataFrameArray, isLazyDataFrameArray, isSeriesArray } from "./utils";

/**
 * _Repeat a single value n times and collect into a Series._
 * @param value - Value to repeat.
 * @param n - Number of repeats
 * @param name - Optional name of the Series
 * @example
 *
 * ```
 *
 * >  const s = pl.repeat("a", 5)
 * >  s.toArray()
 * ["a", "a", "a", "a", "a"]
 *
 * ```
 */
export function repeat<V>(value: V, n: number, name = ""): Series {
  const dtype = jsTypeToPolarsType(value);
  const s = pli.JsSeries.repeat(name, value, n, dtype);

  return _Series(s) as any;
}

/**
 * Aggregate all the Dataframes/Series in a List of DataFrames/Series to a single DataFrame/Series.
 * @param items DataFrames/Series/LazyFrames to concatenate.
 * @param options.rechunk rechunk the final DataFrame/Series.
 * @param options.how Only used if the items are DataFrames. *Defaults to 'vertical'*
 *     - Vertical: Applies multiple `vstack` operations.
 *     - Horizontal: Stacks Series horizontally and fills with nulls if the lengths don't match.
 *     - Diagonal: Finds a union between the column schemas and fills missing column values with ``null``.
 * @example
 * > const df1 = pl.DataFrame({"a": [1], "b": [3]});
 * > const df2 = pl.DataFrame({"a": [2], "b": [4]});
 * > pl.concat([df1, df2]);
 * shape: (2, 2)
 * ┌─────┬─────┐
 * │ a   ┆ b   │
 * │ --- ┆ --- │
 * │ i64 ┆ i64 │
 * ╞═════╪═════╡
 * │ 1   ┆ 3   │
 * ├╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 2   ┆ 4   │
 * └─────┴─────┘
 *
 * > const a = pl.DataFrame({ a: ["a", "b"], b: [1, 2] });
 * > const b = pl.DataFrame({ c: [5, 6], d: [7, 8], e: [9, 10]});
 * > pl.concat([a, b], { how: "horizontal" });
 *
 * shape: (2, 5)
 * ┌─────┬─────┬─────┬─────┬──────┐
 * │ a   ┆ b   ┆ c   ┆ d   ┆ e    │
 * │ --- ┆ --- ┆ --- ┆ --- ┆ ---  │
 * │ str ┆ f64 ┆ f64 ┆ f64 ┆ f64  │
 * ╞═════╪═════╪═════╪═════╪══════╡
 * │ a   ┆ 1.0 ┆ 5.0 ┆ 7.0 ┆ 9.0  │
 * │ b   ┆ 2.0 ┆ 6.0 ┆ 8.0 ┆ 10.0 │
 * └─────┴─────┴─────┴─────┴──────┘
 *
 * > const df_d1 = pl.DataFrame({"a": [1], "b": [3]});
 * > const df_d2 = pl.DataFrame({"a": [2], "c": [4]});
 * > pl.concat([df_d1, df_d2], { how: "diagonal" });
 *
 * shape: (2, 3)
 * ┌─────┬──────┬──────┐
 * │ a   ┆ b    ┆ c    │
 * │ --- ┆ ---  ┆ ---  │
 * │ i64 ┆ i64  ┆ i64  │
 * ╞═════╪══════╪══════╡
 * │ 1   ┆ 3    ┆ null │
 * │ 2   ┆ null ┆ 4    │
 * └─────┴──────┴──────┘
 *
 */
export function concat(
  items: Array<DataFrame>,
  options?: ConcatOptions,
): DataFrame;
export function concat(
  items: Array<Series>,
  options?: { rechunk: boolean },
): Series;
export function concat(
  items: Array<LazyDataFrame>,
  options?: ConcatOptions,
): LazyDataFrame;
export function concat(
  items,
  options: ConcatOptions = { rechunk: true, how: "vertical" },
) {
  const { rechunk, how } = options;

  if (!items.length) {
    throw new RangeError("cannot concat empty list");
  }

  if (isDataFrameArray(items)) {
    let df: DataFrame;
    switch (how) {
      case "vertical":
        df = items.reduce((acc, curr) => acc.vstack(curr));
        break;
      case "horizontal":
        df = _DataFrame(pli.horizontalConcat(items.map((i: any) => i.inner())));
        break;
      case "diagonal":
        df = _DataFrame(pli.diagonalConcat(items.map((i: any) => i.inner())));
        break;
      default:
        throw new TypeError("unknown concat how option");
    }
    return rechunk ? df.rechunk() : df;
  }

  if (isLazyDataFrameArray(items)) {
    const df: LazyDataFrame = _LazyDataFrame(
      pli.concatLf(
        items.map((i: any) => i.inner()),
        how,
        rechunk,
      ),
    );

    return df;
  }

  if (isSeriesArray(items)) {
    const s = items.reduce((acc, curr) => acc.concat(curr));

    return rechunk ? s.rechunk() : s;
  }
  throw new TypeError("can only concat series and dataframes");
}
