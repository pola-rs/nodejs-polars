import { ColumnsOrExpr, StartBy } from "./utils";
import { Expr, _Expr } from "./lazy/expr";

import {
  InterpolationMethod,
  RollingOptions,
  RollingQuantileOptions,
  RollingSkewOptions,
  ClosedWindow,
} from "./types";
import { DataType } from "./datatypes";

/**
 * Arithmetic operations
 */
export interface Arithmetic<T> {
  /**
   * Add self to other
   * @category Arithmetic
   */
  add(other: any): T;
  /**
   * Subtract other from self
   * @category Arithmetic
   */
  sub(other: any): T;
  /**
   * Divide self by other
   * @category Arithmetic
   */
  div(other: any): T;
  /**
   * Multiply self by other
   * @category Arithmetic
   */
  mul(other: any): T;
  /**
   * Get the remainder of self divided by other
   * @category Arithmetic
   */
  rem(other: any): T;
  /**
   * Add self to other
   * @category Arithmetic
   */
  plus(other: any): T;
  /**
   * Subtract other from self
   * @category Arithmetic
   */
  minus(other: any): T;
  /**
   * Divide self by other
   * @category Arithmetic
   */
  divideBy(other: any): T;
  /**
   * Multiply self by other
   * @category Arithmetic
   */
  multiplyBy(other: any): T;
  /**
   * Get the remainder of self divided by other
   * @category Arithmetic
   */
  modulo(other: any): T;
}

export interface Comparison<T> {
  /**
   * Compare self to other: `self == other`
   * @category Comparison
   */
  eq(other: any): T;
  /**
   * Compare self to other: `self == other`
   * @category Comparison
   */
  equals(other: any): T;
  /**
   * Compare self to other:  `self >= other`
   * @category Comparison
   */
  gtEq(other: any): T;
  /**
   * Compare self to other:  `self >= other`
   * @category Comparison
   */
  greaterThanEquals(other: any): T;
  /**
   * Compare self to other:  `self > other`
   * @category Comparison
   */
  gt(other: any): T;
  /**
   * Compare self to other:  `self > other`
   * @category Comparison
   */
  greaterThan(other: any): T;
  /**
   * Compare self to other:  `self <= other`
   * @category Comparison
   */
  ltEq(other: any): T;
  /**
   * Compare self to other:  `self =< other`
   * @category Comparison
   */
  lessThanEquals(other: any): T;
  /**
   * Compare self to other:  `self < other`
   * @category Comparison
   */
  lt(other: any): T;
  /**
   * Compare self to other:  `self < other`
   * @category Comparison
   */
  lessThan(other: any): T;
  /**
   * Compare self to other:  `self !== other`
   * @category Comparison
   */
  neq(other: any): T;
  /**
   * Compare self to other:  `self !== other`
   * @category Comparison
   */
  notEquals(other: any): T;
}

/**
 * A trait for cumulative operations.
 */
export interface Cumulative<T> {
  /**
   * Get an array with the cumulative count computed at every element.
   * @category Cumulative
   */
  cumCount(reverse?: boolean): T;
  cumCount({ reverse }: { reverse: boolean }): T;
  /**
   * __Get an array with the cumulative max computes at every element.__
   * ___
   * @param reverse - reverse the operation
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  s.cumMax()
   * shape: (3,)
   * Series: 'b' [i64]
   * [
   *         1
   *         2
   *         3
   * ]
   * ```
   * @category Cumulative
   */
  cumMax(reverse?: boolean): T;
  cumMax({ reverse }: { reverse: boolean }): T;
  /**
   * __Get an array with the cumulative min computed at every element.__
   * ___
   * @param reverse - reverse the operation
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  s.cumMin()
   * shape: (3,)
   * Series: 'b' [i64]
   * [
   *         1
   *         1
   *         1
   * ]
   * ```
   * @category Cumulative
   */
  cumMin(reverse?: boolean): T;
  cumMin({ reverse }: { reverse: boolean }): T;
  /**
   * __Get an array with the cumulative product computed at every element.__
   * ___
   * @param reverse - reverse the operation
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  s.cumProd()
   * shape: (3,)
   * Series: 'b' [i64]
   * [
   *         1
   *         2
   *         6
   * ]
   * ```
   * @category Cumulative
   */
  cumProd(reverse?: boolean): T;
  cumProd({ reverse }: { reverse: boolean }): T;
  /**
   * __Get an array with the cumulative sum computed at every element.__
   * ___
   * @param reverse - reverse the operation
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  s.cumSum()
   * shape: (3,)
   * Series: 'b' [i64]
   * [
   *         1
   *         3
   *         6
   * ]
   * ```
   * @category Cumulative
   */
  cumSum(reverse?: boolean): T;
  cumSum({ reverse }: { reverse: boolean }): T;
}

/**
 * __A trait for DataFrame and Series that allows for the application of a rolling window.__
 */
export interface Rolling<T> {
  /**
   * __Apply a rolling max (moving max) over the values in this Series.__
   *
   * A window of length `window_size` will traverse the series. The values that fill this window
   * will (optionally) be multiplied with the weights given by the `weight` vector.
   *
   * The resulting parameters' values will be aggregated into their sum.
   * ___
   * @param windowSize - The length of the window.
   * @param weights - An optional slice with the same length as the window that will be multiplied
   * elementwise with the values in the window.
   * @param minPeriods The number of values in the window that should be non-null before computing a result.
   * If undefined, it will be set equal to window size.
   * @param center - Set the labels at the center of the window
   * @category Rolling
   */
  rollingMax(options: RollingOptions): T;
  rollingMax(
    windowSize: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
  ): T;
  /**
   * __Apply a rolling mean (moving mean) over the values in this Series.__
   *
   * A window of length `window_size` will traverse the series. The values that fill this window
   * will (optionally) be multiplied with the weights given by the `weight` vector.
   *
   * The resulting parameters' values will be aggregated into their sum.
   * ___
   * @param windowSize - The length of the window.
   * @param weights - An optional slice with the same length as the window that will be multiplied
   * elementwise with the values in the window.
   * @param minPeriods The number of values in the window that should be non-null before computing a result.
   * If undefined, it will be set equal to window size.
   * @param center - Set the labels at the center of the window
   * @category Rolling
   */
  rollingMean(options: RollingOptions): T;
  rollingMean(
    windowSize: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
  ): T;
  /**
   * __Apply a rolling min (moving min) over the values in this Series.__
   *
   * A window of length `window_size` will traverse the series. The values that fill this window
   * will (optionally) be multiplied with the weights given by the `weight` vector.
   *
   * The resulting parameters' values will be aggregated into their sum.
   * ___
   * @param windowSize - The length of the window.
   * @param weights - An optional slice with the same length as the window that will be multiplied
   * elementwise with the values in the window.
   * @param minPeriods The number of values in the window that should be non-null before computing a result.
   * If undefined, it will be set equal to window size.
   * @param center - Set the labels at the center of the window
   * @category Rolling
   */
  rollingMin(options: RollingOptions): T;
  rollingMin(
    windowSize: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
  ): T;
  /**
   * Compute a rolling std dev
   *
   * A window of length `window_size` will traverse the array. The values that fill this window
   * will (optionally) be multiplied with the weights given by the `weight` vector. The resulting
   * values will be aggregated to their sum.
   * ___
   * @param windowSize - The length of the window.
   * @param weights - An optional slice with the same length as the window that will be multiplied
   * elementwise with the values in the window.
   * @param minPeriods The number of values in the window that should be non-null before computing a result.
   * If undefined, it will be set equal to window size.
   * @param center - Set the labels at the center of the window
   * @param ddof
   *        "Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements.
   *        By default ddof is 1.
   * @category Rolling
   */
  rollingStd(options: RollingOptions): T;
  rollingStd(
    windowSize: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
    ddof?: number,
  ): T;
  /**
   * __Apply a rolling sum (moving sum) over the values in this Series.__
   *
   * A window of length `window_size` will traverse the series. The values that fill this window
   * will (optionally) be multiplied with the weights given by the `weight` vector.
   *
   * The resulting parameters' values will be aggregated into their sum.
   * ___
   * @param windowSize - The length of the window.
   * @param weights - An optional slice with the same length as the window that will be multiplied
   * elementwise with the values in the window.
   * @param minPeriods The number of values in the window that should be non-null before computing a result.
   * If undefined, it will be set equal to window size.
   * @param center - Set the labels at the center of the window
   * @category Rolling
   */
  rollingSum(options: RollingOptions): T;
  rollingSum(
    windowSize: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
  ): T;
  /**
   * __Compute a rolling variance.__
   *
   * A window of length `window_size` will traverse the series. The values that fill this window
   * will (optionally) be multiplied with the weights given by the `weight` vector.
   *
   * The resulting parameters' values will be aggregated into their sum.
   * ___
   * @param windowSize - The length of the window.
   * @param weights - An optional slice with the same length as the window that will be multiplied
   * elementwise with the values in the window.
   * @param minPeriods The number of values in the window that should be non-null before computing a result.
   * If undefined, it will be set equal to window size.
   * @param center - Set the labels at the center of the window
   * @param ddof
   *        "Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements.
   *        By default ddof is 1.
   * @category Rolling
   */
  rollingVar(options: RollingOptions): T;
  rollingVar(
    windowSize: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
    ddof?: number,
  ): T;
  /**
   * Compute a rolling median
   * @category Rolling
   */
  rollingMedian(options: RollingOptions): T;
  rollingMedian(
    windowSize: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
  ): T;
  /**
   * Compute a rolling quantile
   * @param quantile quantile to compute
   * @param interpolation interpolation type
   * @param windowSize Size of the rolling window
   * @param weights - An optional slice with the same length as the window that will be multiplied
   * elementwise with the values in the window.
   * @param minPeriods The number of values in the window that should be non-null before computing a result.
   * If undefined, it will be set equal to window size.
   * @param center - Set the labels at the center of the window
   * @category Rolling
   */
  rollingQuantile(options: RollingQuantileOptions): T;
  rollingQuantile(
    quantile: number,
    interpolation?: InterpolationMethod,
    windowSize?: number,
    weights?: Array<number>,
    minPeriods?: Array<number>,
    center?: boolean,
    by?: string,
    closed?: ClosedWindow,
  ): T;
  /**
   * Compute a rolling skew
   * @param windowSize Size of the rolling window
   * @param bias If false, then the calculations are corrected for statistical bias.
   * @category Rolling
   */
  rollingSkew(windowSize: number, bias?: boolean): T;
  /**
   * Compute a rolling skew
   * @param options
   * @param options.windowSize Size of the rolling window
   * @param options.bias If false, then the calculations are corrected for statistical bias.
   * @category Rolling
   */
  rollingSkew(options: RollingSkewOptions): T;
}

export interface Round<T> {
  /**
   * Round underlying floating point data by `decimals` digits.
   *
   * Similar functionality to javascript `toFixed`
   * @param decimals number of decimals to round by.
   * @category Math
   */
  round(decimals: number): T;
  round(options: { decimals: number }): T;
  /**
   * Floor underlying floating point array to the lowest integers smaller or equal to the float value.
   * Only works on floating point Series
   * @category Math
   */
  floor(): T;
  /**
   * Ceil underlying floating point array to the highest integers smaller or equal to the float value.
   * Only works on floating point Series
   * @category Math
   */
  ceil(): T;

  /**
   * Clip (limit) the values in an array to any value that fits in 64 floating point range.
   * Only works for the following dtypes: {Int32, Int64, Float32, Float64, UInt32}.
   * If you want to clip other dtypes, consider writing a when -> then -> otherwise expression
   * @param min Minimum value
   * @param max Maximum value
   * @category Math
   */
  clip(min: number, max: number): T;
  clip(options: { min: number; max: number });
}

export interface Sample<T> {
  /**
   * Sample from this DataFrame by setting either `n` or `frac`.
   * @param n - Number of samples < self.len() .
   * @param frac - Fraction between 0.0 and 1.0 .
   * @param withReplacement - Sample with replacement.
   * @param seed - Seed initialization. If not provided, a random seed will be used
   * @example
   * ```
   * > df = pl.DataFrame({
   * >   "foo": [1, 2, 3],
   * >   "bar": [6, 7, 8],
   * >   "ham": ['a', 'b', 'c']
   * > })
   * > df.sample({n: 2})
   * shape: (2, 3)
   * ╭─────┬─────┬─────╮
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 6   ┆ "a" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 3   ┆ 8   ┆ "c" │
   * ╰─────┴─────┴─────╯
   * ```
   * @category Math
   */

  sample(opts?: {
    n: number;
    withReplacement?: boolean;
    seed?: number | bigint;
  }): T;
  sample(opts?: {
    frac: number;
    withReplacement?: boolean;
    seed?: number | bigint;
  }): T;
  sample(
    n?: number,
    frac?: number,
    withReplacement?: boolean,
    seed?: number | bigint,
  ): T;
}

export interface Bincode<T> {
  (bincode: Uint8Array): T;
  getState(T): Uint8Array;
}

/**
 * Functions that can be applied to dtype List
 */
export interface ListFunctions<T> {
  argMin(): T;
  argMax(): T;
  /**
   * Concat the arrays in a Series dtype List in linear time.
   * @param other Column(s) to concat into a List Series
   * @example
   * -------
   * ```
   * df = pl.DataFrame({
   *   "a": [["a"], ["x"]],
   *   "b": [["b", "c"], ["y", "z"]],
   * })
   * df.select(pl.col("a").lst.concat("b"))
   * shape: (2, 1)
   * ┌─────────────────┐
   * │ a               │
   * │ ---             │
   * │ list[str]       │
   * ╞═════════════════╡
   * │ ["a", "b", "c"] │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ ["x", "y", "z"] │
   * └─────────────────┘
   * ```
   * @category List
   */
  concat(other: (string | T)[] | string | T): T;
  /**
   * Check if sublists contain the given item.
   * @param item Item that will be checked for membership
   * @example
   * --------
   * ```
   * df = pl.DataFrame({"foo": [[3, 2, 1], [], [1, 2]]})
   * df.select(pl.col("foo").lst.contains(1))
   * shape: (3, 1)
   * ┌───────┐
   * │ foo   │
   * │ ---   │
   * │ bool  │
   * ╞═══════╡
   * │ true  │
   * ├╌╌╌╌╌╌╌┤
   * │ false │
   * ├╌╌╌╌╌╌╌┤
   * │ true  │
   * └───────┘
   * ```
   * @category List
   */
  contains(item: any): T;
  /**
   * Calculate the n-th discrete difference of every sublist.
   * @param n number of slots to shift
   * @param nullBehavior 'ignore' | 'drop'
   * ```
   * s = pl.Series("a", [[1, 2, 3, 4], [10, 2, 1]])
   * s.lst.diff()
   *
   * shape: (2,)
   * Series: 'a' [list]
   * [
   *     [null, 1, ... 1]
   *     [null, -8, -1]
   * ]
   * ```
   * @category List
   */
  diff(n?: number, nullBehavior?: "ignore" | "drop"): T;
  /**
   * Get the value by index in the sublists.
   * So index `0` would return the first item of every sublist
   * and index `-1` would return the last item of every sublist
   * if an index is out of bounds, it will return a `null`.
   * @category List
   */
  get(index: number | Expr): T;
  /**
   *  Run any polars expression against the lists' elements
   *  Parameters
   *  ----------
   * @param expr
   *   Expression to run. Note that you can select an element with `pl.first()`, or `pl.col()`
   * @param parallel
   *   Run all expression parallel. Don't activate this blindly.
   *   Parallelism is worth it if there is enough work to do per thread.
   *   This likely should not be use in the groupby context, because we already parallel execution per group
   * @example
   *  >df = pl.DataFrame({"a": [1, 8, 3], "b": [4, 5, 2]})
   *  >df.withColumn(
   *  ...   pl.concatList(["a", "b"]).lst.eval(pl.first().rank()).alias("rank")
   *  ... )
   *  shape: (3, 3)
   *  ┌─────┬─────┬────────────┐
   *  │ a   ┆ b   ┆ rank       │
   *  │ --- ┆ --- ┆ ---        │
   *  │ i64 ┆ i64 ┆ list [f32] │
   *  ╞═════╪═════╪════════════╡
   *  │ 1   ┆ 4   ┆ [1.0, 2.0] │
   *  ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   *  │ 8   ┆ 5   ┆ [2.0, 1.0] │
   *  ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   *  │ 3   ┆ 2   ┆ [2.0, 1.0] │
   *  └─────┴─────┴────────────┘
   * @category List
   */
  eval(expr: Expr, parallel?: boolean): T;
  /**
   * Get the first value of the sublists.
   * @category List
   */
  first(): T;
  /**
   * Slice the head of every sublist
   * @param n - How many values to take in the slice.
   * @example
   * ```
   * s = pl.Series("a", [[1, 2, 3, 4], [10, 2, 1]])
   * s.lst.head(2)
   * shape: (2,)
   * Series: 'a' [list]
   * [
   *     [1, 2]
   *     [10, 2]
   * ]
   * ```
   * @category List
   */
  head(n: number): T;
  /**
   * Slice the tail of every sublist
   * @param n - How many values to take in the slice.
   * @example
   * ```
   * s = pl.Series("a", [[1, 2, 3, 4], [10, 2, 1]])
   * s.lst.tail(2)
   * shape: (2,)
   * Series: 'a' [list]
   * [
   *     [3, 4]
   *     [2, q]
   * ]
   * ```
   * @category List
   */
  tail(n: number): T;
  /**
   * Join all string items in a sublist and place a separator between them.
   * This errors if inner type of list `!= Utf8`.
   * @param separator A string used to separate one element of the list from the next in the resulting string.
   * If omitted, the list elements are separated with a comma.
   * @category List
   */
  join(separator?: string): T;
  /**
   * Get the last value of the sublists.
   * @category List
   */
  last(): T;
  /**
   * Get the length of the sublists.
   * @category List
   */
  lengths(): T;
  /**
   * Get the maximum value of the sublists.
   * @category List
   */
  max(): T;
  /**
   * Get the mean value of the sublists.
   * @category List
   */
  mean(): T;
  /**
   * Get the median value of the sublists.
   * @category List
   */
  min(): T;
  /**
   * Reverse the sublists.
   * @category List
   */
  reverse(): T;
  /**
   * Shift the sublists.
   * @param periods - Number of periods to shift. Can be positive or negative.
   * @category List
   */
  shift(periods: number): T;
  /**
   * Slice the sublists.
   * @param offset - The offset of the slice.
   * @param length - The length of the slice.
   * @category List
   */
  slice(offset: number, length: number): T;
  /**
   * Sort the sublists.
   * @param reverse - Sort in reverse order.
   * @category List
   */
  sort(reverse?: boolean): T;
  sort(opt: { reverse: boolean }): T;
  /**
   * Sum all elements of the sublists.
   * @category List
   */
  sum(): T;
  /**
   * Get the unique values of the sublists.
   * @category List
   */
  unique(): T;
}

/**
 * Functions that can be applied to a Date or Datetime column.
 */
export interface DateFunctions<T> {
  /**
   * Extract day from underlying Date representation.
   * Can be performed on Date and Datetime.
   *
   * Returns the day of month starting from 1.
   * The return value ranges from 1 to 31. (The last day of month differs by months.)
   * @returns day as pl.UInt32
   */
  day(): T;
  /**
   * Extract hour from underlying DateTime representation.
   * Can be performed on Datetime.
   *
   * Returns the hour number from 0 to 23.
   * @returns Hour as UInt32
   */
  hour(): T;
  /**
   * Extract minutes from underlying DateTime representation.
   * Can be performed on Datetime.
   *
   * Returns the minute number from 0 to 59.
   * @returns minute as UInt32
   */
  minute(): T;
  /**
   * Extract month from underlying Date representation.
   * Can be performed on Date and Datetime.
   *
   * Returns the month number starting from 1.
   * The return value ranges from 1 to 12.
   * @returns Month as UInt32
   */
  month(): T;
  /**
   * Extract seconds from underlying DateTime representation.
   * Can be performed on Datetime.
   *
   * Returns the number of nanoseconds since the whole non-leap second.
   * The range from 1,000,000,000 to 1,999,999,999 represents the leap second.
   * @returns Nanosecond as UInt32
   */
  nanosecond(): T;
  /**
   * Extract ordinal day from underlying Date representation.
   * Can be performed on Date and Datetime.
   *
   * Returns the day of year starting from 1.
   * The return value ranges from 1 to 366. (The last day of year differs by years.)
   * @returns Day as UInt32
   */
  ordinalDay(): T;
  /**
   * Extract seconds from underlying DateTime representation.
   * Can be performed on Datetime.
   *
   * Returns the second number from 0 to 59.
   * @returns Second as UInt32
   */
  second(): T;
  /**
   * Format Date/datetime with a formatting rule: See [chrono strftime/strptime](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html).
   */
  strftime(fmt: string): T;
  /** Return timestamp in ms as Int64 type. */
  timestamp(): T;
  /**
   * Extract the week from the underlying Date representation.
   * Can be performed on Date and Datetime
   *
   * Returns the ISO week number starting from 1.
   * The return value ranges from 1 to 53. (The last week of year differs by years.)
   * @returns Week number as UInt32
   */
  week(): T;
  /**
   * Extract the week day from the underlying Date representation.
   * Can be performed on Date and Datetime.
   *
   * Returns the weekday number where monday = 0 and sunday = 6
   * @returns Week day as UInt32
   */
  weekday(): T;
  /**
   * Extract year from underlying Date representation.
   * Can be performed on Date and Datetime.
   *
   * Returns the year number in the calendar date.
   * @returns Year as Int32
   */
  year(): T;
}

export interface StringFunctions<T> {
  /**
   * Vertically concat the values in the Series to a single string value.
   * @example
   * ```
   * > df = pl.DataFrame({"foo": [1, null, 2]})
   * > df = df.select(pl.col("foo").str.concat("-"))
   * > df
   * shape: (1, 1)
   * ┌──────────┐
   * │ foo      │
   * │ ---      │
   * │ str      │
   * ╞══════════╡
   * │ 1-null-2 │
   * └──────────┘
   * ```
   */
  concat(delimiter: string): T;
  /** Check if strings in Series contain regex pattern. */
  contains(pat: string | RegExp): T;
  /**
   * Decodes a value using the provided encoding
   * @param encoding - hex | base64
   * @param strict - how to handle invalid inputs
   *
   *     - true: method will throw error if unable to decode a value
   *     - false: unhandled values will be replaced with `null`
   * @example
   * ```
   * > df = pl.DataFrame({"strings": ["666f6f", "626172", null]})
   * > df.select(col("strings").str.decode("hex"))
   * shape: (3, 1)
   * ┌─────────┐
   * │ strings │
   * │ ---     │
   * │ str     │
   * ╞═════════╡
   * │ foo     │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ bar     │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ null    │
   * └─────────┘
   * ```
   */
  decode(encoding: "hex" | "base64", strict?: boolean): T;
  decode(options: { encoding: "hex" | "base64"; strict?: boolean }): T;
  /**
   * Encodes a value using the provided encoding
   * @param encoding - hex | base64
   * @example
   * ```
   * > df = pl.DataFrame({"strings", ["foo", "bar", null]})
   * > df.select(col("strings").str.encode("hex"))
   * shape: (3, 1)
   * ┌─────────┐
   * │ strings │
   * │ ---     │
   * │ str     │
   * ╞═════════╡
   * │ 666f6f  │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ 626172  │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ null    │
   * └─────────┘
   * ```
   */
  encode(encoding: "hex" | "base64"): T;
  /**
   * Extract the target capture group from provided patterns.
   * @param pattern A valid regex pattern
   * @param groupIndex Index of the targeted capture group.
   * Group 0 mean the whole pattern, first group begin at index 1
   * Default to the first capture group
   * @returns Utf8 array. Contain null if original value is null or regex capture nothing.
   * @example
   * ```
   * >  df = pl.DataFrame({
   * ...   'a': [
   * ...       'http://vote.com/ballon_dor?candidate=messi&ref=polars',
   * ...       'http://vote.com/ballon_dor?candidat=jorginho&ref=polars',
   * ...       'http://vote.com/ballon_dor?candidate=ronaldo&ref=polars'
   * ...   ]})
   * >  df.select(pl.col('a').str.extract(/candidate=(\w+)/, 1))
   * shape: (3, 1)
   * ┌─────────┐
   * │ a       │
   * │ ---     │
   * │ str     │
   * ╞═════════╡
   * │ messi   │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ null    │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ ronaldo │
   * └─────────┘
   * ```
   */
  extract(pat: string | RegExp, groupIndex: number): T;
  /**
   * Extract the first match of json string with provided JSONPath expression.
   * Throw errors if encounter invalid json strings.
   * All return value will be casted to Utf8 regardless of the original value.
   * @see https://goessner.net/articles/JsonPath/
   * @param jsonPath - A valid JSON path query string
   * @returns Utf8 array. Contain null if original value is null or the `jsonPath` return nothing.
   * @example
   * ```
   * > df = pl.DataFrame({
   * ...   'json_val': [
   * ...     '{"a":"1"}',
   * ...     null,
   * ...     '{"a":2}',
   * ...     '{"a":2.1}',
   * ...     '{"a":true}'
   * ...   ]
   * ... })
   * > df.select(pl.col('json_val').str.jsonPathMatch('$.a')
   * shape: (5,)
   * Series: 'json_val' [str]
   * [
   *     "1"
   *     null
   *     "2"
   *     "2.1"
   *     "true"
   * ]
   * ```
   */
  jsonPathMatch(pat: string): T;
  /**  Get length of the string values in the Series. */
  lengths(): T;
  /** Remove leading whitespace. */
  lstrip(): T;
  /** Replace first regex match with a string value. */
  replace(pat: string | RegExp, val: string): T;
  /** Replace all regex matches with a string value. */
  replaceAll(pat: string | RegExp, val: string): T;
  /** Modify the strings to their lowercase equivalent. */
  toLowerCase(): T;
  /** Modify the strings to their uppercase equivalent. */
  toUpperCase(): T;
  /** Remove trailing whitespace. */
  rstrip(): T;
  /**
   * Create subslices of the string values of a Utf8 Series.
   * @param start - Start of the slice (negative indexing may be used).
   * @param length - Optional length of the slice.
   */
  slice(start: number, length?: number): T;
  /**
   * Split a string into substrings using the specified separator and return them as a Series.
   * @param separator — A string that identifies character or characters to use in separating the string.
   * @param inclusive Include the split character/string in the results
   */
  split(by: string, options?: { inclusive?: boolean } | boolean): T;
  /** Remove leading and trailing whitespace. */
  strip(): T;
  /**
   * Parse a Series of dtype Utf8 to a Date/Datetime Series.
   * @param datatype Date or Datetime.
   * @param fmt formatting syntax. [Read more](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html)
   */
  strptime(datatype: DataType.Date | DataType.Datetime, fmt?: string): T;
}

export interface Serialize {
  /**
   * Serializes object to desired format via [serde](https://serde.rs/)
   *
   * @param format [json](https://github.com/serde-rs/json) | [bincode](https://github.com/bincode-org/bincode)
   *
   */
  serialize(format: "json" | "bincode"): Buffer;
}
export interface Deserialize<T> {
  /**
   * De-serializes buffer via [serde](https://serde.rs/)
   * @param buf buffer to deserialize
   * @param format [json](https://github.com/serde-rs/json) | [bincode](https://github.com/bincode-org/bincode)
   *
   */
  deserialize(buf: Buffer, format: "json" | "bincode"): T;
}

/**
 * GroupBy operations that can be applied to a DataFrame or LazyFrame.
 */
export interface GroupByOps<T> {
  /**
    Create rolling groups based on a time column (or index value of type Int32, Int64).

    Different from a rolling groupby the windows are now determined by the individual values and are not of constant
    intervals. For constant intervals use {@link groupByDynamic}

    The `period` and `offset` arguments are created with
    the following string language:

    - 1ns   (1 nanosecond)
    - 1us   (1 microsecond)
    - 1ms   (1 millisecond)
    - 1s    (1 second)
    - 1m    (1 minute)
    - 1h    (1 hour)
    - 1d    (1 day)
    - 1w    (1 week)
    - 1mo   (1 calendar month)
    - 1y    (1 calendar year)
    - 1i    (1 index count)

    Or combine them:
    "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds

    In case of a groupby_rolling on an integer column, the windows are defined by:

    - "1i"      # length 1
    - "10i"     # length 10


    @param indexColumn Column used to group based on the time window.
    Often to type Date/Datetime
    This column must be sorted in ascending order. If not the output will not make sense.

    In case of a rolling groupby on indices, dtype needs to be one of {Int32, Int64}. Note that
    Int32 gets temporarily cast to Int64, so if performance matters use an Int64 column.
    @param period length of the window
    @param offset offset of the window. Default is `-period`
    @param closed Defines if the window interval is closed or not.
    @param check_sorted
            When the ``by`` argument is given, polars can not check sortedness
            by the metadata and has to do a full scan on the index column to
            verify data is sorted. This is expensive. If you are sure the
            data within the by groups is sorted, you can set this to ``False``.
            Doing so incorrectly will lead to incorrect output

    Any of `{"left", "right", "both" "none"}`
    @param by Also group by this column/these columns

    @example
    ```

    >dates = [
    ...     "2020-01-01 13:45:48",
    ...     "2020-01-01 16:42:13",
    ...     "2020-01-01 16:45:09",
    ...     "2020-01-02 18:12:48",
    ...     "2020-01-03 19:45:32",
    ...     "2020-01-08 23:16:43",
    ... ]
    >df = pl.DataFrame({"dt": dates, "a": [3, 7, 5, 9, 2, 1]}).withColumn(
    ...     pl.col("dt").str.strptime(pl.Datetime)
    ... )
    >out = df.groupbyRolling({indexColumn:"dt", period:"2d"}).agg(
    ...     [
    ...         pl.sum("a").alias("sum_a"),
    ...         pl.min("a").alias("min_a"),
    ...         pl.max("a").alias("max_a"),
    ...     ]
    ... )
    >assert(out["sum_a"].toArray() === [3, 10, 15, 24, 11, 1])
    >assert(out["max_a"].toArray() === [3, 7, 7, 9, 9, 1])
    >assert(out["min_a"].toArray() === [3, 3, 3, 3, 2, 1])
    >out
    shape: (6, 4)
    ┌─────────────────────┬───────┬───────┬───────┐
    │ dt                  ┆ a_sum ┆ a_max ┆ a_min │
    │ ---                 ┆ ---   ┆ ---   ┆ ---   │
    │ datetime[ms]        ┆ i64   ┆ i64   ┆ i64   │
    ╞═════════════════════╪═══════╪═══════╪═══════╡
    │ 2020-01-01 13:45:48 ┆ 3     ┆ 3     ┆ 3     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2020-01-01 16:42:13 ┆ 10    ┆ 7     ┆ 3     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2020-01-01 16:45:09 ┆ 15    ┆ 7     ┆ 3     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2020-01-02 18:12:48 ┆ 24    ┆ 9     ┆ 3     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2020-01-03 19:45:32 ┆ 11    ┆ 9     ┆ 2     │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ 2020-01-08 23:16:43 ┆ 1     ┆ 1     ┆ 1     │
    └─────────────────────┴───────┴───────┴───────┘
    ```
   */
  groupByRolling(opts: {
    indexColumn: ColumnsOrExpr;
    by?: ColumnsOrExpr;
    period: string;
    offset?: string;
    closed?: "left" | "right" | "both" | "none";
    check_sorted?: boolean;
  }): T;

  /**
  Groups based on a time value (or index value of type Int32, Int64). Time windows are calculated and rows are assigned to windows.
  Different from a normal groupby is that a row can be member of multiple groups. The time/index window could
  be seen as a rolling window, with a window size determined by dates/times/values instead of slots in the DataFrame.


  A window is defined by:
  - every: interval of the window
  - period: length of the window
  - offset: offset of the window

  The `every`, `period` and `offset` arguments are created with
  the following string language:

  - 1ns   (1 nanosecond)
  - 1us   (1 microsecond)
  - 1ms   (1 millisecond)
  - 1s    (1 second)
  - 1m    (1 minute)
  - 1h    (1 hour)
  - 1d    (1 day)
  - 1w    (1 week)
  - 1mo   (1 calendar month)
  - 1y    (1 calendar year)
  - 1i    (1 index count)

  Or combine them:
  "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds

  In case of a groupbyDynamic on an integer column, the windows are defined by:

  - "1i"      # length 1
  - "10i"     # length 10

  Parameters
  ----------
  @param index_column Column used to group based on the time window.
      Often to type Date/Datetime
      This column must be sorted in ascending order. If not the output will not make sense.

      In case of a dynamic groupby on indices, dtype needs to be one of {Int32, Int64}. Note that
      Int32 gets temporarily cast to Int64, so if performance matters use an Int64 column.
  @param every interval of the window
  @param period length of the window, if None it is equal to 'every'
  @param offset offset of the window if None and period is None it will be equal to negative `every`
  @param truncate truncate the time value to the window lower bound
  @param includeBoundaries add the lower and upper bound of the window to the "_lower_bound" and "_upper_bound" columns. This will impact performance because it's harder to parallelize
  @param closed Defines if the window interval is closed or not.
      Any of {"left", "right", "both" "none"}
  @param check_sorted
      When the ``by`` argument is given, polars can not check sortedness
      by the metadata and has to do a full scan on the index column to
      verify data is sorted. This is expensive. If you are sure the
      data within the by groups is sorted, you can set this to ``False``.
      Doing so incorrectly will lead to incorrect output
  @param by Also group by this column/these columns
 */
  groupByDynamic(options: {
    indexColumn: string;
    every: string;
    period?: string;
    offset?: string;
    truncate?: boolean;
    includeBoundaries?: boolean;
    closed?: "left" | "right" | "both" | "none";
    by?: ColumnsOrExpr;
    start_by: StartBy;
    check_sorted?: boolean;
  }): T;
}

/***
 * Exponentially-weighted operations that can be applied to a Series and Expr
 */
export interface EwmOps<T> {
  /**
   * Exponentially-weighted moving average.
   *
   * @param alpha Specify smoothing factor alpha directly, :math:`0 < \alpha \leq 1`.
   * @param adjust Divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings
   *       - When ``adjust: true`` the EW function is calculated using weights :math:`w_i = (1 - \alpha)^i`
   *       - When ``adjust=false`` the EW function is calculated recursively
   * @param bias When ``bias: false``, apply a correction to make the estimate statistically unbiased.
   * @param minPeriods Minimum number of observations in window required to have a value (otherwise result is null).
   * @param ignoreNulls Ignore missing values when calculating weights.
   *       - When ``ignoreNulls: false`` (default), weights are based on absolute positions.
   *       - When ``ignoreNulls: true``, weights are based on relative positions.
   * @returns Expr that evaluates to a float 64 Series.
   * @examples
   * ```
   * > const df = pl.DataFrame({a: [1, 2, 3]});
   * > df.select(pl.col("a").ewmMean())
   * shape: (3, 1)
   * ┌──────────┐
   * │ a        │
   * | ---      │
   * │ f64      │
   * ╞══════════╡
   * │ 1.0      │
   * │ 1.666667 │
   * │ 2.428571 │
   * └──────────┘
   * ```
   */
  ewmMean(): T;
  ewmMean(
    alpha?: number,
    adjust?: boolean,
    bias?: boolean,
    minPeriods?: number,
    ignoreNulls?: boolean,
  ): T;
  ewmMean(opts: {
    alpha?: number;
    adjust?: boolean;
    bias?: boolean;
    minPeriods?: number;
    ignoreNulls?: boolean;
  }): T;
  /**
   * Exponentially-weighted standard deviation.
   *
   * @param alpha Specify smoothing factor alpha directly, :math:`0 < \alpha \leq 1`.
   * @param adjust Divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings
   *       - When ``adjust: true`` the EW function is calculated using weights :math:`w_i = (1 - \alpha)^i`
   *       - When ``adjust: false`` the EW function is calculated recursively
   * @param bias When ``bias: false``, apply a correction to make the estimate statistically unbiased.
   * @param minPeriods Minimum number of observations in window required to have a value (otherwise result is null).
   * @param ignoreNulls Ignore missing values when calculating weights.
   *       - When ``ignoreNulls: false`` (default), weights are based on absolute positions.
   *         For example, the weights of :math:`x_0` and :math:`x_2` used in calculating the final weighted average of
   *       - When ``ignoreNulls: true``, weights are based on relative positions.
   * @returns Expr that evaluates to a float 64 Series.
   * @examples
   * ```
   * > const df = pl.DataFrame({a: [1, 2, 3]});
   * > df.select(pl.col("a").ewmStd())
   * shape: (3, 1)
   * ┌──────────┐
   * │ a        │
   * | ---      │
   * │ f64      │
   * ╞══════════╡
   * │ 0.0      │
   * │ 0.707107 │
   * │ 0.963624 │
   * └──────────┘
   * ```
   */
  ewmStd(): T;
  ewmStd(
    alpha?: number,
    adjust?: boolean,
    bias?: boolean,
    minPeriods?: number,
    ignoreNulls?: boolean,
  ): T;
  ewmStd(opts: {
    alpha?: number;
    adjust?: boolean;
    bias?: boolean;
    minPeriods?: number;
    ignoreNulls?: boolean;
  }): T;
  /**
   * Exponentially-weighted variance.
   *
   * @param alpha Specify smoothing factor alpha directly, :math:`0 < \alpha \leq 1`.
   * @param adjust Divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings
   *       - When ``adjust: true`` the EW function is calculated using weights :math:`w_i = (1 - \alpha)^i`
   *       - When ``adjust: false`` the EW function is calculated recursively
   * @param bias When ``bias: false``, apply a correction to make the estimate statistically unbiased.
   * @param minPeriods Minimum number of observations in window required to have a value (otherwise result is null).
   * @param ignoreNulls Ignore missing values when calculating weights.
   *       - When ``ignoreNulls: false`` (default), weights are based on absolute positions.
   *       - When ``ignoreNulls=true``, weights are based on relative positions.
   * @returns Expr that evaluates to a float 64 Series.
   * @examples
   * ```
   * > const df = pl.DataFrame({a: [1, 2, 3]});
   * > df.select(pl.col("a").ewmVar())
   * shape: (3, 1)
   * ┌──────────┐
   * │ a        │
   * | ---      │
   * │ f64      │
   * ╞══════════╡
   * │ 0.0      │
   * │ 0.5      │
   * │ 0.928571 │
   * └──────────┘
   * ```
   */
  ewmVar(): T;
  ewmVar(
    alpha?: number,
    adjust?: boolean,
    bias?: boolean,
    minPeriods?: number,
    ignoreNulls?: boolean,
  ): T;
  ewmVar(opts: {
    alpha?: number;
    adjust?: boolean;
    bias?: boolean;
    minPeriods?: number;
    ignoreNulls?: boolean;
  }): T;
}
