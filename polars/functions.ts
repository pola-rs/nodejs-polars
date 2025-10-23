import { _DataFrame, type DataFrame } from "./dataframe";
/* eslint-disable no-redeclare */
import { jsTypeToPolarsType } from "./internals/construction";
import pli from "./internals/polars_internal";
import { _LazyDataFrame, type LazyDataFrame } from "./lazy/dataframe";
import { _Series, type Series } from "./series";
import type { ConcatOptions, JoinType } from "./types";
import {
  commonValue,
  isDataFrameArray,
  isLazyDataFrameArray,
  isSeriesArray,
} from "./utils";

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
 * @param options.rechunk Make sure that the result data is in contiguous memory.
 * @param options.how Only used if the items are DataFrames. *Defaults to 'vertical'*
 *     - vertical: Applies multiple `vstack` operations.
 *     - verticalRelaxed: Same as `vertical`, but additionally coerces columns to their common supertype *if* they are mismatched (eg: Int32 → Int64).
 *     - horizontal: Stacks Series horizontally and fills with nulls if the lengths don't match.
 *     - diagonal: Finds a union between the column schemas and fills missing column values with ``null``.
 *     - diagonalRelaxed: Same as `diagonal`, but additionally coerces columns to their common supertype *if* they are mismatched (eg: Int32 → Int64).
       - align, alignFull, alignLeft, alignRight: Combines frames horizontally,
          auto-determining the common key columns and aligning rows using the same
          logic as `alignFrames` (note that "align" is an alias for "alignFull").
          The "align" strategy determines the type of join used to align the frames,
          equivalent to the "how" parameter on `alignFrames`. Note that the common
          join columns are automatically coalesced, but other column collisions
          will raise an error (if you need more control over this you should use
          a suitable `join` method directly).
 * @param parallel - Only relevant for LazyFrames. This determines if the concatenated lazy computations may be executed in parallel.
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
 *  The "align" strategies require at least one common column to align on:

    >>> df1 = pl.DataFrame({"id": [1, 2], "x": [3, 4]});
    >>> df2 = pl.DataFrame({"id": [2, 3], "y": [5, 6]});
    >>> df3 = pl.DataFrame({"id": [1, 3], "z": [7, 8]});
    >>> pl.concat([df1, df2, df3], how="align") // equivalent to "alignFull"
    shape: (3, 4)
    ┌─────┬──────┬──────┬──────┐
    │ id  ┆ x    ┆ y    ┆ z    │
    │ --- ┆ ---  ┆ ---  ┆ ---  │
    │ i64 ┆ i64  ┆ i64  ┆ i64  │
    ╞═════╪══════╪══════╪══════╡
    │ 1   ┆ 3    ┆ null ┆ 7    │
    │ 2   ┆ 4    ┆ 5    ┆ null │
    │ 3   ┆ null ┆ 6    ┆ 8    │
    └─────┴──────┴──────┴──────┘
    >>> pl.concat([df1, df2, df3], how="alignLeft");
    shape: (2, 4)
    ┌─────┬─────┬──────┬──────┐
    │ id  ┆ x   ┆ y    ┆ z    │
    │ --- ┆ --- ┆ ---  ┆ ---  │
    │ i64 ┆ i64 ┆ i64  ┆ i64  │
    ╞═════╪═════╪══════╪══════╡
    │ 1   ┆ 3   ┆ null ┆ 7    │
    │ 2   ┆ 4   ┆ 5    ┆ null │
    └─────┴─────┴──────┴──────┘
    >>> pl.concat([df1, df2, df3], how="alignRight");
    shape: (2, 4)
    ┌─────┬──────┬──────┬─────┐
    │ id  ┆ x    ┆ y    ┆ z   │
    │ --- ┆ ---  ┆ ---  ┆ --- │
    │ i64 ┆ i64  ┆ i64  ┆ i64 │
    ╞═════╪══════╪══════╪═════╡
    │ 1   ┆ null ┆ null ┆ 7   │
    │ 3   ┆ null ┆ 6    ┆ 8   │
    └─────┴──────┴──────┴─────┘
    >>> pl.concat([df1, df2, df3], how="alignInner");
    shape: (0, 4)
    ┌─────┬─────┬─────┬─────┐
    │ id  ┆ x   ┆ y   ┆ z   │
    │ --- ┆ --- ┆ --- ┆ --- │
    │ i64 ┆ i64 ┆ i64 ┆ i64 │
    ╞═════╪═════╪═════╪═════╡
    └─────┴─────┴─────┴─────┘
 */
export function concat(
  items: Array<DataFrame>,
  options?: ConcatOptions,
): DataFrame;
export function concat(
  items: Array<Series>,
  options?: { rechunk: boolean; parallel: boolean },
): Series;
export function concat(
  items: Array<LazyDataFrame>,
  options?: ConcatOptions,
): LazyDataFrame;
export function concat(
  items,
  options: ConcatOptions = { rechunk: true, parallel: true, how: "vertical" },
) {
  const { rechunk, how, parallel } = options;

  if (!items.length) {
    throw new RangeError("cannot concat empty list");
  }

  if (isDataFrameArray(items)) {
    let df: DataFrame;
    switch (how) {
      case "align":
      case "alignFull":
      case "alignInner":
      case "alignLeft":
      case "alignRight": {
        if (items.length === 1) return items[0];
        const commonCols = commonValue(...items.map((c) => c.columns));
        const uniqueCols = [...new Set(items.flatMap((c) => c.columns))].sort();
        // Join methods allowed: "full" | "left" | "inner" | "semi" | "anti" | undefined
        const joinMethod: Exclude<JoinType, "cross"> =
          how === "align"
            ? "full"
            : (how.replace("align", "").toLocaleLowerCase() as Exclude<
                JoinType,
                "cross"
              >);

        df = _DataFrame(
          items.reduce((acc, curr) =>
            acc.join(curr, { on: commonCols, how: joinMethod, coalesce: true }),
          ),
        )
          ._df.sort(commonCols)
          .select(uniqueCols);
        break;
      }
      case "vertical":
        df = items.reduce((acc, curr) => acc.vstack(curr));
        break;
      case "verticalRelaxed":
      case "diagonalRelaxed":
        df = _LazyDataFrame(
          pli.concatLf(
            items.map((i: any) => i.inner().lazy()),
            how,
            rechunk ?? false,
            parallel ?? true,
            true, // to_supertypes
            true, // maintain_order
          ),
        ).collectSync();
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
        rechunk ?? false,
        parallel ?? true,
        true, // to_supertypes
        true, // maintain_order
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
