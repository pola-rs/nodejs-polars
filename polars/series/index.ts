import { _DataFrame, DataFrame } from "../dataframe";
import {
  type Bool,
  DataType,
  DTYPE_TO_FFINAME,
  type DTypeToJs,
  type DTypeToJsLoose,
  type DtypeToJsName,
  type JsToDtype,
  type JsType,
  type Optional,
} from "../datatypes";
import { InvalidOperationError } from "../error";
import { arrayToJsSeries } from "../internals/construction";
import pli from "../internals/polars_internal";
import { col } from "../lazy/functions";
import type {
  Arithmetic,
  Comparison,
  Cumulative,
  Deserialize,
  EwmOps,
  Rolling,
  Round,
  Sample,
  Serialize,
} from "../shared_traits";
import type { InterpolationMethod, RankMethod } from "../types";
import { SeriesDateFunctions } from "./datetime";
import { SeriesListFunctions } from "./list";
import { SeriesStringFunctions } from "./string";
import { SeriesStructFunctions } from "./struct";

// For documentation
export type { SeriesDateFunctions as DatetimeSeries } from "./datetime";
export type { SeriesListFunctions as ListSeries } from "./list";
export type { SeriesStringFunctions as StringSeries } from "./string";
export type { SeriesStructFunctions as StructSeries } from "./struct";

const inspect = Symbol.for("nodejs.util.inspect.custom");
/**
 * A Series represents a single column in a polars DataFrame.
 */
export interface Series<T extends DataType = any, Name extends string = string>
  extends ArrayLike<T>,
    Rolling<Series<T>>,
    Arithmetic<Series<T>>,
    Comparison<Series<T>>,
    Cumulative<Series<T>>,
    Round<Series<T>>,
    Sample<Series<T>>,
    EwmOps<Series<T>>,
    Serialize {
  inner(): any;
  name: Name;
  dtype: T;
  str: SeriesStringFunctions;
  lst: SeriesListFunctions;
  struct: SeriesStructFunctions;
  date: SeriesDateFunctions;
  [inspect](): string;
  [Symbol.iterator](): IterableIterator<DTypeToJs<T>>;
  // inner(): JsSeries
  bitand(other: Series<T>): Series<T, Name>;
  bitor(other: Series<T>): Series<T, Name>;
  bitxor(other: Series<T>): Series<T, Name>;
  /**
   * Take absolute values
   */
  abs(): Series<T, Name>;
  /**
   * __Rename this Series.__
   *
   * @param name - new name
   * @see {@link rename}
   */
  alias<U extends string>(name: U): Series<T, U>;
  /**
   * __Append a Series to this one.__
   * ___
   * @param {Series} other - Series to append.
   * @example
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  const s2 = pl.Series("b", [4, 5, 6])
   * >  s.append(s2)
   * shape: (6,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   *         3
   *         4
   *         5
   *         6
   * ]
   */
  append(other: Series): void;
  // TODO!
  // /**
  //  * __Apply a function over elements in this Series and return a new Series.__
  //  *
  //  * If the function returns another datatype, the returnType arg should be set, otherwise the method will fail.
  //  * ___
  //  * @param {CallableFunction} func - function or lambda.
  //  * @param {DataType} returnType - Output datatype. If none is given, the same datatype as this Series will be used.
  //  * @returns {SeriesType} `Series<T> | Series<returnType>`
  //  * @example
  //  * ```
  //  * >  const s = pl.Series("a", [1, 2, 3])
  //  * >  s.apply(x => x + 10)
  //  * shape: (3,)
  //  * Series: 'a' [i64]
  //  * [
  //  *         11
  //  *         12
  //  *         13
  //  * ]
  //  * ```
  //  */
  // apply<U>(func: (s: T) => U): Series<U>
  /**
   * Get the index of the maximal value.
   */
  argMax(): Optional<number>;
  /**
   * Get the index of the minimal value.
   */
  argMin(): Optional<number>;
  /**
   * Get index values where Boolean Series evaluate True.
   */
  argTrue(): Series<T, Name>;
  /**
   * Get unique index as Series.
   */
  argUnique(): Series<T, Name>;
  /**
   * Get the index values that would sort this Series.
   * ___
   * @param descending - Sort in descending order.
   * @param nullsLast - Place null values last instead of first.
   * @return {SeriesType} indexes - Indexes that can be used to sort this array.
   */
  argSort(descending?: boolean, nullsLast?: boolean): Series<T, Name>;
  argSort({
    descending,
    nullsLast,
  }: {
    descending?: boolean;
    nullsLast?: boolean;
  }): Series<T, Name>;
  /* @deprecated Use descending instead */
  argSort({
    reverse, // deprecated
    nullsLast,
  }: {
    reverse?: boolean;
    nullsLast?: boolean;
  }): Series<T, Name>;
  argSort(): Series<T, Name>;
  /**
   * __Rename this Series.__
   *
   * @param name - new name
   * @see {@link rename} {@link alias}
   */
  as<U extends string>(name: string): Series<T, U>;
  /**
   * Cast between data types.
   */
  cast<const U extends DataType>(dtype: U, strict?: boolean): Series<U>;
  cast(dtype: DataType, strict?: boolean): Series;
  /**
   * Get the length of each individual chunk
   */
  chunkLengths(): Array<T>;
  /**
   * Cheap deep clones.
   */
  clone(): Series<T, Name>;
  concat(other: Series<T>): Series<T, Name>;

  /**
   * __Quick summary statistics of a series. __
   *
   * Series with mixed datatypes will return summary statistics for the datatype of the first value.
   * ___
   * @example
   * ```
   * >  const seriesNum = pl.Series([1,2,3,4,5])
   * >  series_num.describe()
   *
   * shape: (6, 2)
   * ┌──────────────┬────────────────────┐
   * │ statistic    ┆ value              │
   * │ ---          ┆ ---                │
   * │ str          ┆ f64                │
   * ╞══════════════╪════════════════════╡
   * │ "min"        ┆ 1                  │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "max"        ┆ 5                  │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "null_count" ┆ 0.0                │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "mean"       ┆ 3                  │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "std"        ┆ 1.5811388300841898 │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "count"      ┆ 5                  │
   * └──────────────┴────────────────────┘
   *
   * >  series_str = pl.Series(["a", "a", None, "b", "c"])
   * >  series_str.describe()
   *
   * shape: (3, 2)
   * ┌──────────────┬───────┐
   * │ statistic    ┆ value │
   * │ ---          ┆ ---   │
   * │ str          ┆ i64   │
   * ╞══════════════╪═══════╡
   * │ "unique"     ┆ 4     │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ "null_count" ┆ 1     │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ "count"      ┆ 5     │
   * └──────────────┴───────┘
   * ```
   */
  describe(): DataFrame;
  /**
   * Calculates the n-th discrete difference.
   * @param n - number of slots to shift
   * @param nullBehavior - `'ignore' | 'drop'`
   */
  diff(n: number, nullBehavior: "ignore" | "drop"): Series<T, Name>;
  diff({
    n,
    nullBehavior,
  }: {
    n: number;
    nullBehavior: "ignore" | "drop";
  }): Series<T, Name>;
  /**
   * Compute the dot/inner product between two Series
   * ___
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  const s2 = pl.Series("b", [4.0, 5.0, 6.0])
   * >  s.dot(s2)
   * 32.0
   * ```
   */
  dot(other: Series): number | undefined | null;
  /**
   * Create a new Series that copies data from this Series without null values.
   */
  dropNulls(): Series<T, Name>;
  /**
   * __Explode a list or utf8 Series.__
   *
   * This means that every item is expanded to a new row.
   * ___
   * @example
   * ```
   * >  const s = pl.Series('a', [[1, 2], [3, 4], [9, 10]])
   * >  s.explode()
   * shape: (6,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   *         3
   *         4
   *         9
   *         10
   * ]
   * ```
   */
  explode(): any;
  /**
   * Extend the Series with given number of values.
   * @param value The value to extend the Series with. This value may be null to fill with nulls.
   * @param n The number of values to extend.
   */
  extendConstant(value: any, n: number): Series;
  extendConstant(opt: { value: any; n: number }): Series;
  /**
   * __Fill null values with a filling strategy.__
   * ___
   * @param strategy - Filling Strategy
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3, None])
   * >  s.fill_null('forward'))
   * shape: (4,)
   * Series: '' [i64]
   * [
   *         1
   *         2
   *         3
   *         3
   * ]
   * >  s.fill_null('min'))
   * shape: (4,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   *         3
   *         1
   * ]
   * ```
   */
  fillNull(
    strategy: "backward" | "forward" | "min" | "max" | "mean" | "one" | "zero",
  ): Series;
  fillNull({
    strategy,
  }: {
    strategy: "backward" | "forward" | "min" | "max" | "mean" | "one" | "zero";
  }): Series;
  /**
   * __Filter elements by a boolean mask.__
   * @param {SeriesType} predicate - Boolean mask
   */
  filter(predicate: Series): Series;
  filter({ predicate }: { predicate: Series }): Series;
  get(index: number): any;
  getIndex(n: number): any;
  /**
   * Returns True if the Series has a validity bitmask.
   * If there is none, it means that there are no null values.
   */
  hasValidity(): boolean;
  /**
   * Hash the Series
   * The hash value is of type `UInt64`
   * ___
   * @param k0 - seed parameter
   * @param k1 - seed parameter
   * @param k2 - seed parameter
   * @param k3 - seed parameter
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  s.hash(42)
   * shape: (3,)
   * Series: 'a' [u64]
   * [
   *   7499844439152382372
   *   821952831504499201
   *   6685218033491627602
   * ]
   * ```
   */
  hash(
    k0?: number | bigint,
    k1?: number | bigint,
    k2?: number | bigint,
    k3?: number | bigint,
  ): Series;
  hash({
    k0,
    k1,
    k2,
    k3,
  }: {
    k0?: number | bigint;
    k1?: number | bigint;
    k2?: number | bigint;
    k3?: number | bigint;
  }): Series;
  /**
   * __Get first N elements as Series.__
   * ___
   * @param length  Length of the head
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  s.head(2)
   * shape: (2,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   * ]
   * ```
   */
  head(length?: number): Series;
  /**
   * __Interpolate intermediate values.__
   *
   * The interpolation method is linear.
   * ___
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, None, None, 5])
   * >  s.interpolate()
   * shape: (5,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   *         3
   *         4
   *         5
   * ]
   * ```
   */
  interpolate(method?: InterpolationMethod): Series;
  /**
   * Check if this Series is a Boolean.
   */
  isBoolean(): this is Series<Bool>;
  /**
   * Check if this Series is a DataTime.
   */
  isDateTime(): boolean;
  /**
   * __Get mask of all duplicated values.__
   *
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 2, 3])
   * >  s.isDuplicated()
   *
   * shape: (4,)
   * Series: 'a' [bool]
   * [
   *         false
   *         true
   *         true
   *         false
   * ]
   * ```
   */
  isDuplicated(): Series;
  /**
   * Get mask of finite values if Series dtype is Float.
   */
  isFinite(): Series;
  /**
   * Get a mask of the first unique value.
   */
  isFirstDistinct(): Series;
  /**
   * Check if this Series is a Float.
   */
  isFloat(): boolean;
  /**
   * Check if elements of this Series are in the right Series, or List values of the right Series.
   */
  isIn<U>(other: Series | U[]): Series;
  /**
   * __Get mask of infinite values if Series dtype is Float.__
   * @example
   * ```
   * >  const s = pl.Series("a", [1.0, 2.0, 3.0])
   * >  s.isInfinite()
   *
   * shape: (3,)
   * Series: 'a' [bool]
   * [
   *         false
   *         false
   *         false
   * ]
   * ```
   */
  isInfinite(): Series;
  /**
   * __Get mask of non null values.__
   *
   * *`undefined` values are treated as null*
   * ___
   * @example
   * ```
   * >  const s = pl.Series("a", [1.0, undefined, 2.0, 3.0, null])
   * >  s.isNotNull()
   * shape: (5,)
   * Series: 'a' [bool]
   * [
   *         true
   *         false
   *         true
   *         true
   *         false
   * ]
   * ```
   */
  isNotNull(): Series;
  /**
   * __Get mask of null values.__
   *
   * `undefined` values are treated as null
   * ___
   * @example
   * ```
   * >  const s = pl.Series("a", [1.0, undefined, 2.0, 3.0, null])
   * >  s.isNull()
   * shape: (5,)
   * Series: 'a' [bool]
   * [
   *         false
   *         true
   *         false
   *         false
   *         true
   * ]
   * ```
   */
  isNull(): Series;
  /**
   * Check if this Series datatype is numeric.
   */
  isNumeric(): this is Series<
    | DataType.Int8
    | DataType.Int16
    | DataType.Int32
    | DataType.Int64
    | DataType.UInt8
    | DataType.UInt16
    | DataType.UInt32
    | DataType.UInt64
    | DataType.Float32
    | DataType.Float64
    | DataType.Decimal
  >;
  /**
   * __Get mask of unique values.__
   * ___
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 2, 3])
   * >  s.isUnique()
   * shape: (4,)
   * Series: 'a' [bool]
   * [
   *         true
   *         false
   *         false
   *         true
   * ]
   * ```
   */
  isUnique(): Series;
  /**
   * Checks if this Series datatype is a String.
   */
  isString(): boolean;
  /**
   * __Compute the kurtosis (Fisher or Pearson) of a dataset.__
   *
   * Kurtosis is the fourth central moment divided by the square of the
   * variance. If Fisher's definition is used, then 3.0 is subtracted from
   * the result to give 0.0 for a normal distribution.
   * If bias is False then the kurtosis is calculated using k statistics to
   * eliminate bias coming from biased moment estimators
   * ___
   * @param fisher -
   * - If True, Fisher's definition is used (normal ==> 0.0).
   * - If False, Pearson's definition is used (normal ==> 3.0)
   * @param bias : bool, optional If False, the calculations are corrected for statistical bias.
   */
  kurtosis(fisher: boolean, bias?: boolean): Optional<number>;
  kurtosis({
    fisher,
    bias,
  }: {
    fisher?: boolean;
    bias?: boolean;
  }): Optional<number>;
  kurtosis(): Optional<number>;
  /**
   * __Length of this Series.__
   * ___
   * @example
   * ```
   * >  const s = pl.Series("a", [1, 2, 3])
   * >  s.len()
   * 3
   * ```
   */
  len(): number;
  /**
   * __Take `n` elements from this Series.__
   * ___
   * @param n - Amount of elements to take.
   * @see {@link head}
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3])
   * s.limit(2)
   * shape: (2,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   * ]
   * ```
   */
  limit(n?: number): Series;
  /**
   * Get the maximum value in this Series.
   * @example
   * ```
   * > s = pl.Series("a", [1, 2, 3])
   * > s.max()
   * 3
   * ```
   */
  max(): number;
  /**
   * Reduce this Series to the mean value.
   * @example
   * ```
   * > s = pl.Series("a", [1, 2, 3])
   * > s.mean()
   * 2
   * ```
   */
  mean(): number;
  /**
   * Get the median of this Series
   * @example
   * ```
   * > s = pl.Series("a", [1, 2, 3])
   * > s.median()
   * 2
   * ```
   */
  median(): number;
  /**
   * Get the minimal value in this Series.
   * @example
   * ```
   * > s = pl.Series("a", [1, 2, 3])
   * > s.min()
   * 1
   * ```
   */
  min(): number;
  /**
   * __Compute the most occurring value(s). Can return multiple Values__
   * ___
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 2, 3])
   * s.mode()
   * shape: (1,)
   * Series: 'a' [i64]
   * [
   *         2
   * ]
   *
   * s = pl.Series("a", ['a', 'b', 'c', 'c', 'b'])
   * s.mode()
   * shape: (1,)
   * Series: 'a' [str]
   * [
   *         'b'
   *         'c'
   * ]
   * ```
   */
  mode(): Series;
  /**
   * Get the number of chunks that this Series contains.
   */
  nChunks(): number;
  /**
   * __Count the number of unique values in this Series.__
   * ___
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 2, 3])
   * s.nUnique()
   * 3
   * ```
   */
  nUnique(): number;
  /**
   * Count the null values in this Series. --
   * _`undefined` values are treated as null_
   */
  nullCount(): number;
  /**
   * Get a boolean mask of the local maximum peaks.
   * ___
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3, 4, 5])
   * s.peakMax()
   * shape: (5,)
   * Series: '' [bool]
   * [
   *         false
   *         false
   *         false
   *         false
   *         true
   * ]
   * ```
   */
  peakMax(): Series;
  /**
   * Get a boolean mask of the local minimum peaks.
   * ___
   * @example
   * ```
   * s = pl.Series("a", [4, 1, 3, 2, 5])
   * s.peakMin()
   * shape: (5,)
   * Series: '' [bool]
   * [
   *         false
   *         true
   *         false
   *         true
   *         false
   * ]
   * ```
   */
  peakMin(): Series;
  /**
   * Get the quantile value of this Series.
   * ___
   * @param quantile
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3])
   * s.quantile(0.5)
   * 2
   * ```
   */
  quantile(quantile: number, interpolation?: string): number;
  /**
   * Assign ranks to data, dealing with ties appropriately.
   * The method used to assign ranks to tied elements.
   * @param method {'average', 'min', 'max', 'dense', 'ordinal', 'random'}
   * The following methods are available: _default is 'average'_
   *
   *  *   __'average'__: The average of the ranks that would have been assigned to
   *    all the tied values is assigned to each value.
   *  * __'min'__: The minimum of the ranks that would have been assigned to all
   *    the tied values is assigned to each value.  _This is also
   *    referred to as "competition" ranking._
   *  * __'max'__: The maximum of the ranks that would have been assigned to all
   *    the tied values is assigned to each value.
   *  * __'dense'__: Like 'min', but the rank of the next highest element is
   *    assigned the rank immediately after those assigned to the tied
   *    elements.
   *  * __'ordinal'__: All values are given a distinct rank, corresponding to
   *    the order that the values occur in `a`.
   *  * __'random'__: Like 'ordinal', but the rank for ties is not dependent
   *    on the order that the values occur in `a`.
   * @param descending - Rank in descending order.
   */
  rank(method?: RankMethod, descending?: boolean): Series;
  rechunk(): Series;
  rechunk(inPlace: true): Series;
  rechunk(inPlace: false): void;
  /**
   * __Reinterpret the underlying bits as a signed/unsigned integer.__
   *
   * This operation is only allowed for 64bit integers. For lower bits integers,
   * you can safely use that cast operation.
   * ___
   * @param signed signed or unsigned
   *
   * - True -> pl.Int64
   * - False -> pl.UInt64
   * @see {@link cast}
   */
  reinterpret(signed?: boolean): Series;
  /**
   * __Rename this Series.__
   *
   * @param name - new name
   * @param inPlace - Modify the Series in-place.
   * @see {@link alias}
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3])
   * s.rename('b')
   * shape: (3,)
   * Series: 'b' [i64]
   * [
   *         1
   *         2
   *         3
   * ]
   * ```
   */
  rename(name: string, inPlace: boolean): void;
  rename(name: string): Series;
  rename({ name, inPlace }: { name: string; inPlace?: boolean }): void;
  rename({ name, inPlace }: { name: string; inPlace: true }): void;

  /**
   * __Check if series is equal with another Series.__
   * @param other - Series to compare with.
   * @param nullEqual - Consider null values as equal. _('undefined' is treated as null)_
   * ___
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3])
   * s2 = pl.Series("b", [4, 5, 6])
   * s.series_equal(s)
   * true
   * s.series_equal(s2)
   * false
   * ```
   */
  seriesEqual<_U1>(
    other: Series,
    nullEqual?: boolean,
    strict?: boolean,
  ): boolean;
  /**
   * __Set masked values__
   * @param filter Boolean mask
   * @param value value to replace masked values with
   */
  set(filter: Series, value: any): Series;
  scatter(indices: number[] | Series, value: any): void;
  /**
   * __Shift the values by a given period__
   *
   * the parts that will be empty due to this operation will be filled with `null`.
   * ___
   * @param periods - Number of places to shift (may be negative).
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3])
   * s.shift(1)
   * shape: (3,)
   * Series: 'a' [i64]
   * [
   *         null
   *         1
   *         2
   * ]
   * s.shift(-1)
   * shape: (3,)
   * Series: 'a' [i64]
   * [
   *         2
   *         3
   *         null
   * ]
   * ```
   */
  shift(periods: number): Series;
  /**
   * Shift the values by a given period
   *
   * the parts that will be empty due to this operation will be filled with `fillValue`.
   * ___
   * @param periods - Number of places to shift (may be negative).
   * @param fillValue - Fill null & undefined values with the result of this expression.
   */
  shiftAndFill(periods: number, fillValue: number): Series;
  shiftAndFill(args: { periods: number; fillValue: number }): Series;

  /**
   * __Shrink memory usage of this Series to fit the exact capacity needed to hold the data.__
   * @param inPlace - Modify the Series in-place.
   */
  shrinkToFit(inPlace: true): void;
  shrinkToFit(): Series;
  /**
   * __Compute the sample skewness of a data set.__
   *
   * For normally distributed data, the skewness should be about zero. For
   * unimodal continuous distributions, a skewness value greater than zero means
   * that there is more weight in the right tail of the distribution. The
   * function `skewtest` can be used to determine if the skewness value
   * is close enough to zero, statistically speaking.
   * ___
   * @param bias - If false, then the calculations are corrected for statistical bias.
   */
  skew(bias?: boolean): number | undefined;
  /**
   * Create subslices of the Series.
   *
   * @param start - Start of the slice (negative indexing may be used).
   * @param length - length of the slice.
   */
  slice(start: number, length?: number): Series;
  /**
   * __Sort this Series.__
   * @param options.descending - Sort in descending order.
   * @param options.nullsLast - Place nulls at the end.
   * @example
   * ```
   * s = pl.Series("a", [1, 3, 4, 2])
   * s.sort()
   * shape: (4,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   *         3
   *         4
   * ]
   * s.sort({descending: true})
   * shape: (4,)
   * Series: 'a' [i64]
   * [
   *         4
   *         3
   *         2
   *         1
   * ]
   * ```
   */
  sort(options: { descending?: boolean; nullsLast?: boolean }): Series;
  /* @deprecated Use descending instead */
  sort(options: { reverse?: boolean; nullsLast?: boolean }): Series;
  sort(): Series;
  /**
   * Reduce this Series to the sum value.
   * @example
   * ```
   * > s = pl.Series("a", [1, 2, 3])
   * > s.sum()
   * 6
   * ```
   */
  sum(): number;
  /**
   * __Get last N elements as Series.__
   *
   * ___
   * @param length - Length of the tail
   * @see {@link head}
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3])
   * s.tail(2)
   * shape: (2,)
   * Series: 'a' [i64]
   * [
   *         2
   *         3
   * ]
   * ```
   */
  tail(length?: number): Series;
  /**
   * Take every nth value in the Series and return as new Series.
   * @param n - Gather every *n*-th row
   * @param offset - Start the row count at this offset
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3, 4])
   * s.gatherEvery(2))
   * shape: (2,)
   * Series: 'a' [i64]
   * [
   *     1
   *     3
   * ]
   * s.gather_every(2, offset=1)
   * shape: (2,)
   * Series: 'a' [i64]
   * [
   *     2
   *     4
   * ]
   * ```
   */
  gatherEvery(n: number, offset?: number): Series;
  /**
   * Take values by index.
   * ___
   * @param indices - Index location used for the selection
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 3, 4])
   * s.gather([1, 3])
   * shape: (2,)
   * Series: 'a' [i64]
   * [
   *         2
   *         4
   * ]
   * ```
   */
  gather(indices: Array<number>): Series;

  /**
   * __Get unique elements in series.__
   * ___
   * @param maintainOrder Maintain order of data. This requires more work.
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 2, 3])
   * s.unique()
   * shape: (3,)
   * Series: 'a' [i64]
   * [
   *         1
   *         2
   *         3
   * ]
   * ```
   */
  unique(maintainOrder?: boolean | { maintainOrder: boolean }): Series;
  /**
   * __Count the unique values in a Series.__
   * @param sort - Sort the output by count in descending order.
   *               If set to `False` (default), the order of the output is random.
   * @param parallel - Execute the computation in parallel.
            .. note::
                This option should likely not be enabled in a group by context,
                as the computation is already parallelized per group.
   * @param name - Give the resulting count column a specific name;
            if `normalize` is True defaults to "count", otherwise defaults to "proportion".
   * @param normalize - If true gives relative frequencies of the unique values
   * ___
   * @example
   * ```
   * s = pl.Series("a", [1, 2, 2, 3])
   * s.valueCounts()
   * shape: (3, 2)
   * ╭─────┬────────╮
   * │ a   ┆ counts │
   * │ --- ┆ ---    │
   * │ i64 ┆ u32    │
   * ╞═════╪════════╡
   * │ 2   ┆ 2      │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   * │ 1   ┆ 1      │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   * │ 3   ┆ 1      │
   * ╰─────┴────────╯
   * ```
   */
  valueCounts(
    sort?: boolean,
    parallel?: boolean,
    name?: string,
    normalize?: boolean,
  ): DataFrame;
  /**
   * Where mask evaluates true, take values from self.
   *
   * Where mask evaluates false, take values from other.
   * ___
   * @param mask - Boolean Series
   * @param other - Series of same type
   */
  zipWith(mask: Series, other: Series): Series;

  /**
   * __Convert this Series to a Javascript Array.__
   *
   * This operation clones data, and is very slow, but maintains greater precision for all dtypes.
   * Often times `series.toObject().values` is faster, but less precise
   * ___
   * @example
   * ```
   * const s = pl.Series("a", [1, 2, 3])
   * const arr = s.toArray()
   * [1, 2, 3]
   * Array.isArray(arr)
   * true
   * ```
   */
  toArray(): Array<DTypeToJs<T>>;
  /**
   * Converts series to a javascript typedArray.
   *
   * __Warning:__
   * This will throw an error if you have nulls, or are using non numeric data types
   */
  toTypedArray(): any;

  /**
   * Get dummy/indicator variables.
   * @param separator str = "_",
   * @param dropFirst bool = False
   *
   * @example
   * const s = pl.Series("a", [1, 2, 3])
    >>> s.toDummies()
    shape: (3, 3)
    ┌─────┬─────┬─────┐
    │ a_1 ┆ a_2 ┆ a_3 │
    │ --- ┆ --- ┆ --- │
    │ u8  ┆ u8  ┆ u8  │
    ╞═════╪═════╪═════╡
    │ 1   ┆ 0   ┆ 0   │
    │ 0   ┆ 1   ┆ 0   │
    │ 0   ┆ 0   ┆ 1   │
    └─────┴─────┴─────┘

    >>> s.toDummies(":", true)
    shape: (3, 2)
    ┌─────┬─────┐
    │ a:2 ┆ a:3 │
    │ --- ┆ --- │
    │ u8  ┆ u8  │
    ╞═════╪═════╡
    │ 0   ┆ 0   │
    │ 1   ┆ 0   │
    │ 0   ┆ 1   │
    └─────┴─────┘
   *
   */
  toDummies(separator?: string, dropFirst?: boolean): DataFrame;

  /**
   * _Returns a Javascript object representation of Series_
   * Often this is much faster than the iterator, or `values` method
   *
   * @example
   * ```
   * const s = pl.Series("foo", [1,2,3])
   * s.toObject()
   * {
   *   name: "foo",
   *   datatype: "Float64",
   *   values: [1,2,3]
   * }
   * ```
   */
  toObject(): {
    name: Name;
    datatype: DtypeToJsName<T>;
    values: DTypeToJs<T>[];
  };
  toFrame(): DataFrame;
  /** compat with `JSON.stringify */
  toJSON(): string;
  /** Returns an iterator over the values */
  values(): IterableIterator<any>;
}
/** @ignore */
export function _Series(_s: any): Series {
  const unwrap = (method: keyof any, ...args: any[]) => {
    return _s[method as any](...args);
  };
  const wrap = (method, ...args): Series => {
    return _Series(unwrap(method, ...args));
  };
  const dtypeWrap = (method: string, ...args: any[]) => {
    const dtype = _s.dtype;

    const dt = (DTYPE_TO_FFINAME as any)[dtype];
    const internalMethod = `series${method}${dt}`;

    return _Series(pli[internalMethod](_s, ...args));
  };

  const dtypeUnwrap = (method: string, ...args: any[]) => {
    const dtype = _s.dtype;

    const dt = (DTYPE_TO_FFINAME as any)[dtype];
    const internalMethod = `series${method}${dt}`;

    return pli[internalMethod](_s, ...args);
  };

  const expr_op = (method: string, ...args) => {
    return _Series(_s)
      .toFrame()
      .select(col(_s.name)[method](...args))
      .getColumn(_s.name);
  };

  const series = {
    _s,
    [inspect]() {
      return _s.toString();
    },
    *[Symbol.iterator]() {
      let start = 0;
      const len = _s.len();
      while (start < len) {
        const v = _s.getIdx(start);
        start++;
        yield v as any;
      }
    },
    toString() {
      return _s.toString();
    },
    serialize(format) {
      return _s.serialize(format);
    },
    [Symbol.toStringTag]() {
      return "Series";
    },
    get dtype(): DataType {
      return DataType.deserialize(_s.dtype);
    },
    get name() {
      return _s.name;
    },
    get length() {
      return _s.len();
    },
    get str() {
      return SeriesStringFunctions(_s);
    },
    get lst() {
      return SeriesListFunctions(_s);
    },
    get date() {
      return SeriesDateFunctions(_s);
    },
    get struct() {
      return SeriesStructFunctions(_s);
    },
    abs() {
      return wrap("abs");
    },
    add(field) {
      return dtypeWrap("Add", field);
    },
    alias(name: string) {
      const s = _s.clone();
      s.rename(name);

      return _Series(s);
    },
    append(other: Series) {
      _s.append(other.inner());
    },
    argMax() {
      return _s.argMax();
    },
    argMin() {
      return _s.argMin();
    },
    argSort(
      descending: any = false,
      nullsLast = true,
      multithreaded = true,
      maintainOrder = false,
    ) {
      if (typeof descending === "boolean") {
        return _Series(
          _s.argsort(descending, nullsLast, multithreaded, maintainOrder),
        );
      }

      return _Series(
        _s.argsort(
          descending.descending ?? descending.reverse ?? false,
          descending.nullsLast ?? nullsLast,
          descending.multithreaded ?? multithreaded,
          descending.maintainOrder ?? maintainOrder,
        ),
      );
    },
    argTrue() {
      return _Series(
        this.toFrame()
          ._df.lazy()
          .select([pli.argWhere(pli.col(this.name))])
          .collectSync()
          .column(this.name),
      );
    },
    argUnique() {
      return _Series(_s.argUnique());
    },
    as(name) {
      return this.alias(name);
    },
    bitand(other) {
      return _Series(_s.bitand(other._s));
    },
    bitor(other) {
      return _Series(_s.bitor(other._s));
    },
    bitxor(other) {
      return _Series(_s.bitxor(other._s));
    },
    cast(dtype, strict = false) {
      return _Series(_s.cast(dtype, strict));
    },
    chunkLengths() {
      return _s.chunkLengths();
    },
    clone() {
      return _Series(_s.clone());
    },
    concat(other) {
      const s = _s.clone();
      s.append(other.inner());

      return _Series(s);
    },
    cumCount(reverse?) {
      return expr_op("cumCount", reverse);
    },
    cumSum(reverse?) {
      return _Series(_s.cumSum(reverse));
    },
    cumMax(reverse?) {
      return _Series(_s.cumMax(reverse));
    },
    cumMin(reverse?) {
      return _Series(_s.cumMin(reverse));
    },
    cumProd(reverse?) {
      return _Series(_s.cumProd(reverse));
    },
    describe() {
      let s = this.clone();
      let stats = {};
      if (!this.length) {
        throw new RangeError("Series must contain at least one value");
      }
      if (this.isNumeric()) {
        s = s.cast(DataType.Float64);
        stats = {
          min: s.min(),
          max: s.max(),
          null_count: s.nullCount(),
          mean: s.mean(),
          count: s.len(),
        };
      } else if (s.isBoolean()) {
        stats = {
          sum: s.sum(),
          null_count: s.nullCount(),
          count: s.len(),
        };
      } else if (s.isString()) {
        stats = {
          unique: s.nUnique(),
          null_count: s.nullCount(),
          count: s.len(),
        };
      } else {
        throw new InvalidOperationError("describe", s.dtype);
      }

      return DataFrame({
        statistic: Object.keys(stats),
        value: Object.values(stats),
      });
    },
    diff(n: any = 1, nullBehavior = "ignore") {
      return typeof n === "number"
        ? _Series(_s.diff(n, nullBehavior))
        : _Series(_s.diff(n?.n ?? 1, n.nullBehavior ?? nullBehavior));
    },
    div(field: Series) {
      return dtypeWrap("Div", field);
    },
    divideBy(field: Series) {
      return this.div(field);
    },
    dot(other: Series) {
      return wrap("dot", (other as any)._s) as any;
    },
    dropNulls() {
      return wrap("dropNulls");
    },
    eq(field) {
      return dtypeWrap("Eq", field);
    },
    equals(field: Series) {
      return this.eq(field);
    },
    ewmMean(...args) {
      return expr_op("ewmMean", ...args);
    },
    ewmStd(...args) {
      return expr_op("ewmStd", ...args);
    },
    ewmVar(...args) {
      return expr_op("ewmVar", ...args);
    },
    explode() {
      return wrap("explode");
    },
    extendConstant(o, n?) {
      if (n !== null && typeof n === "number")
        return wrap("extendConstant", o, n);

      return wrap("extendConstant", o.value, o.n);
    },
    fillNull(strategy) {
      return typeof strategy === "string"
        ? wrap("fillNull", strategy)
        : wrap("fillNull", strategy.strategy);
    },
    filter(predicate) {
      return Series.isSeries(predicate)
        ? wrap("filter", (predicate as any)._s)
        : wrap("filter", (SeriesConstructor("", predicate) as any)._s);
    },
    get(field) {
      return dtypeUnwrap("Get", field);
    },
    getIndex(idx) {
      return _s.getIdx(idx);
    },
    gt(field) {
      return dtypeWrap("Gt", field);
    },
    greaterThan(field) {
      return this.gt(field);
    },
    gtEq(field) {
      return dtypeWrap("GtEq", field);
    },
    greaterThanEquals(field) {
      return this.gtEq(field);
    },
    hash(obj: any = 0n, k1 = 1n, k2 = 2n, k3 = 3n) {
      if (typeof obj === "number" || typeof obj === "bigint") {
        return wrap("hash", BigInt(obj), BigInt(k1), BigInt(k2), BigInt(k3));
      }
      const o = { k0: obj, k1: k1, k2: k2, k3: k3, ...obj };

      return wrap(
        "hash",
        BigInt(o.k0),
        BigInt(o.k1),
        BigInt(o.k2),
        BigInt(o.k3),
      );
    },
    hasValidity() {
      return _s.hasValidity();
    },
    head(length = 5) {
      return wrap("head", length);
    },
    inner() {
      return _s;
    },
    interpolate() {
      return expr_op("interpolate");
    },
    isBoolean() {
      const dtype = this.dtype;

      return dtype.equals(DataType.Bool);
    },
    isDateTime() {
      const dtype = this.dtype;

      return [DataType.Date.variant, "Datetime"].includes(dtype.variant);
    },
    isDuplicated() {
      return wrap("isDuplicated");
    },
    isFinite() {
      const dtype = this.dtype;

      if (
        ![DataType.Float32.variant, DataType.Float64.variant].includes(
          dtype.variant,
        )
      ) {
        throw new InvalidOperationError("isFinite", dtype);
      }
      return wrap("isFinite");
    },
    isFirstDistinct() {
      return wrap("isFirstDistinct");
    },
    isFloat() {
      const dtype = this.dtype;

      return [DataType.Float32.variant, DataType.Float64.variant].includes(
        dtype.variant,
      );
    },
    isIn(other, nullsEquals = false) {
      return Series.isSeries(other)
        ? wrap("isIn", (other as any)._s, nullsEquals)
        : wrap("isIn", (Series("", other) as any)._s, nullsEquals);
    },
    isInfinite() {
      const dtype = this.dtype;

      if (
        ![DataType.Float32.variant, DataType.Float64.variant].includes(
          dtype.variant,
        )
      ) {
        throw new InvalidOperationError("isFinite", dtype);
      }
      return wrap("isInfinite");
    },
    isNotNull() {
      return wrap("isNotNull");
    },
    isNull() {
      return wrap("isNull");
    },
    isNaN() {
      return wrap("isNan");
    },
    isNotNaN() {
      return wrap("isNotNan");
    },
    isNumeric() {
      const dtype = this.dtype;

      const numericTypes = [
        DataType.Int8.variant,
        DataType.Int16.variant,
        DataType.Int32.variant,
        DataType.Int64.variant,
        DataType.UInt8.variant,
        DataType.UInt16.variant,
        DataType.UInt32.variant,
        DataType.UInt64.variant,
        DataType.Float32.variant,
        DataType.Float64.variant,
      ];

      return numericTypes.includes(dtype.variant);
    },
    isUnique() {
      return wrap("isUnique");
    },
    isString() {
      return this.dtype.equals(DataType.String);
    },
    kurtosis(fisher: any = true, bias = true) {
      if (typeof fisher === "boolean") {
        return _s.kurtosis(fisher, bias);
      }
      const d = {
        fisher: true,
        bias,
        ...fisher,
      };

      return _s.kurtosis(d.fisher, d.bias);
    },
    len() {
      return this.length;
    },
    lt(field) {
      if (typeof field === "number") return dtypeWrap("Lt", field);
      if (Series.isSeries(field)) {
        return wrap("lt", (field as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    lessThan(field) {
      if (typeof field === "number") return dtypeWrap("Lt", field);
      if (Series.isSeries(field)) {
        return wrap("lt", (field as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    ltEq(field) {
      if (typeof field === "number") return dtypeWrap("LtEq", field);
      if (Series.isSeries(field)) {
        return wrap("ltEq", (field as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    lessThanEquals(field) {
      if (typeof field === "number") return dtypeWrap("LtEq", field);
      if (Series.isSeries(field)) {
        return wrap("ltEq", (field as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    limit(n = 10) {
      return wrap("limit", n);
    },
    max() {
      return _s.max() as any;
    },
    mean() {
      return _s.mean() as any;
    },
    median() {
      return _s.median() as any;
    },
    min() {
      return _s.min() as any;
    },
    mode() {
      return wrap("mode");
    },
    minus(other) {
      if (typeof other === "number") return dtypeWrap("Sub", other);
      if (Series.isSeries(other)) {
        return wrap("sub", (other as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    mul(other) {
      if (typeof other === "number") return dtypeWrap("Mul", other);
      if (Series.isSeries(other)) {
        return wrap("mul", (other as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    nChunks() {
      return _s.nChunks();
    },
    neq(other) {
      if (typeof other === "number") return dtypeWrap("Neq", other);
      if (Series.isSeries(other)) {
        return wrap("neq", (other as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    notEquals(other) {
      return this.neq(other);
    },
    nullCount() {
      return _s.nullCount();
    },
    nUnique() {
      return _s.nUnique();
    },
    peakMax() {
      return expr_op("peakMax");
    },
    peakMin() {
      return expr_op("peakMin");
    },
    plus(other) {
      if (typeof other === "number") return dtypeWrap("Add", other);
      if (Series.isSeries(other)) {
        return wrap("add", (other as any)._s);
      }
      throw new Error("Not a number nor a series");
    },
    quantile(quantile, interpolation = "nearest") {
      return _s.quantile(quantile, interpolation);
    },
    rank(method = "average", descending = false) {
      return wrap("rank", method, descending);
    },
    rechunk(inPlace = false) {
      return wrap("rechunk", inPlace);
    },
    reinterpret(signed = true) {
      const dtype = this.dtype;
      if (
        [DataType.UInt64.variant, DataType.Int64.variant].includes(
          dtype.variant,
        )
      ) {
        return wrap("reinterpret", signed);
      }
      throw new InvalidOperationError("reinterpret", dtype);
    },
    rem(field) {
      return dtypeWrap("Rem", field);
    },
    modulo(field) {
      return this.rem(field);
    },
    rename(obj: any, inPlace = false): any {
      if (obj?.inPlace ?? inPlace) {
        _s.rename(obj?.name ?? obj);
      } else {
        return this.alias(obj?.name ?? obj);
      }
    },

    rollingMax(...args) {
      return expr_op("rollingMax", ...args);
    },
    rollingMean(...args) {
      return expr_op("rollingMean", ...args);
    },
    rollingMin(...args) {
      return expr_op("rollingMin", ...args);
    },
    rollingSum(...args) {
      return expr_op("rollingSum", ...args);
    },
    rollingStd(...args) {
      return expr_op("rollingStd", ...args);
    },
    rollingVar(...args) {
      return expr_op("rollingVar", ...args);
    },
    rollingMedian(...args) {
      return expr_op("rollingMedian", ...args);
    },
    rollingQuantile(...args) {
      return expr_op("rollingQuantile", ...args);
    },
    rollingSkew(...args) {
      return expr_op("rollingSkew", ...args);
    },
    floor() {
      return wrap("floor");
    },
    ceil() {
      return wrap("ceil");
    },
    round(opt): any {
      const mode = "halftoeven";
      if (this.isNumeric()) {
        if (typeof opt === "number") {
          return wrap("round", opt, mode);
        }
        return wrap("round", opt.decimals, mode);
      }
      throw new InvalidOperationError("round", this.dtype);
    },
    clip(...args) {
      return expr_op("clip", ...args);
    },
    scatter(indices, value) {
      indices = Series.isSeries(indices)
        ? indices.cast(DataType.UInt32)
        : Series(indices);
      if (!Series.isSeries(value)) {
        if (!Array.isArray(value)) {
          value = [value];
        }
        value = Series(value);
      }

      if (indices.length > 0) {
        value = value.extendConstant(value[0], indices.length - 1);
      }
      _s.scatter(indices._s, value._s);
    },
    set(mask, value) {
      mask = Series.isSeries(mask) ? mask : Series.from(mask);

      return dtypeWrap("SetWithMask", mask.inner(), value);
    },
    sample(opts?, frac?, withReplacement = false, seed?) {
      if (arguments.length === 0) {
        return wrap("sampleN", 1, withReplacement, false, seed);
      }
      if (opts?.n !== undefined || opts?.frac !== undefined) {
        return this.sample(opts.n, opts.frac, opts.withReplacement, seed);
      }
      if (typeof opts === "number") {
        return wrap("sampleN", opts, withReplacement, false, seed);
      }
      if (typeof frac === "number") {
        return wrap("sampleFrac", frac, withReplacement, false, seed);
      }
      throw new TypeError("must specify either 'frac' or 'n'");
    },
    seriesEqual(other, nullEqual: any = true, strict = false) {
      return _s.seriesEqual(other._s, nullEqual, strict);
    },
    shift(periods = 1) {
      return wrap("shift", periods);
    },
    shiftAndFill(...args) {
      return expr_op("shiftAndFill", ...args);
    },
    shrinkToFit(inPlace?: boolean) {
      if (inPlace) {
        _s.shrinkToFit();
      } else {
        const s = this.clone();
        s.shrinkToFit();

        return s as any;
      }
    },
    skew(bias: any = true) {
      if (typeof bias === "boolean") {
        return _s.skew(bias) as any;
      }

      return _s.skew(bias?.bias ?? true) as any;
    },
    slice(offset, length?) {
      if (typeof offset === "number") {
        return wrap("slice", offset, length);
      }

      return wrap("slice", offset.offset, offset.length);
    },
    sort(options?) {
      options = { descending: false, nullsLast: false, ...(options ?? {}) };
      return wrap(
        "sort",
        options.descending ?? options.reverse ?? false,
        options.nullsLast,
      );
    },
    sub(field) {
      return dtypeWrap("Sub", field);
    },
    sum() {
      return _s.sum() as any;
    },
    tail(length = 5) {
      return wrap("tail", length);
    },
    gather(indices) {
      return wrap("take", indices);
    },
    gatherEvery(n, offset?) {
      return wrap("gatherEvery", n, offset ?? 0);
    },
    multiplyBy(field) {
      return this.mul(field);
    },
    toArray() {
      return _s.toArray();
    },
    toTypedArray() {
      if (!this.hasValidity() || this.nullCount() === 0) {
        return _s.toTypedArray();
      }
      throw new Error("data contains nulls, unable to convert to TypedArray");
    },
    toDummies(separator = "_", dropFirst = false) {
      return _DataFrame(_s.toDummies(separator, dropFirst));
    },
    toFrame() {
      return _DataFrame(new pli.JsDataFrame([_s]));
    },
    toBinary() {
      return _s.toBinary();
    },
    toJSON(...args: any[]) {
      // this is passed by `JSON.stringify` when calling `toJSON()`
      if (args[0] === "") {
        return _s.toJs();
      }
      return _s.serialize("json").toString();
    },
    toObject() {
      return _s.toJs();
    },
    unique(maintainOrder?) {
      if (maintainOrder) {
        return wrap("uniqueStable");
      }
      return wrap("unique");
    },
    valueCounts(
      sort?: boolean,
      parallel?: boolean,
      name?: string,
      normalize?: boolean,
    ) {
      name = name ?? (normalize ? "proportion" : "count");
      return _DataFrame(
        unwrap(
          "valueCounts",
          sort ?? false,
          parallel ?? false,
          name,
          normalize ?? false,
        ),
      );
    },
    values() {
      return this[Symbol.iterator]();
    },
    zipWith(mask, other) {
      return wrap("zipWith", mask._s, other._s);
    },
  };

  return new Proxy(series as unknown as Series, {
    get: (target, prop, receiver) => {
      if (typeof prop !== "symbol" && !Number.isNaN(Number(prop))) {
        return target.get(Number(prop));
      }
      return Reflect.get(target, prop, receiver);
    },
    set: (series, prop, input): any => {
      if (typeof prop !== "symbol" && !Number.isNaN(Number(prop))) {
        series.scatter([Number(prop)], input);

        return true;
      }
    },
  });
}

/**
 * @ignore
 * @inheritDoc {Series}
 */
export interface SeriesConstructor extends Deserialize<Series> {
  /**
   * Creates a new Series from a set of values.
   * @param values — A set of values to include in the new Series object.
   * @example
   * ```
   * >  pl.Series([1, 2, 3])
   * shape: (3,)
   * Series: '' [f64]
   * [
   *         1
   *         2
   *         3
   * ]
   * ```
   */
  <T extends JsType>(values: ArrayLike<T | null>): Series<JsToDtype<T>>;
  (values: any): Series;
  /**
   * Create a new named series
   * @param name - The name of the series
   * @param values - A set of values to include in the new Series object.
   * @example
   * ```
   * >  pl.Series('foo', [1, 2, 3])
   * shape: (3,)
   * Series: 'foo' [f64]
   * [
   *         1
   *         2
   *         3
   * ]
   * ```
   */
  <T1 extends JsType, Name extends string>(
    name: Name,
    values: ArrayLike<T1 | null>,
  ): Series<JsToDtype<T1>, Name>;
  <T2 extends DataType, Name extends string>(
    name: Name,
    values: ArrayLike<DTypeToJsLoose<T2 | null>>,
    dtype?: T2,
  ): Series<T2, Name>;
  (name: string, values: any[], dtype?): Series;

  /**
   * Creates an array from an array-like object.
   * @param arrayLike — An array-like object to convert to an array.
   */
  from<T1 extends JsType>(arrayLike: ArrayLike<T1>): Series<JsToDtype<T1>>;
  from<T1>(arrayLike: ArrayLike<T1>): Series;
  from<T2 extends JsType, Name extends string>(
    name: Name,
    arrayLike: ArrayLike<T2>,
  ): Series<JsToDtype<T2>, Name>;
  from<T2>(name: string, arrayLike: ArrayLike<T2>): Series;
  /**
   * Returns a new Series from a set of elements.
   * @param items — A set of elements to include in the new Series object.
   */
  of<T3 extends JsType>(...items: T3[]): Series<JsToDtype<T3>>;
  of<T3>(...items: T3[]): Series;
  isSeries(arg: any): arg is Series;
  /**
   * @param binary used to serialize/deserialize series. This will only work with the output from series.toBinary().
   */
  // fromBinary(binary: Buffer): Series
}

const SeriesConstructor = (
  arg0: any,
  arg1?: any,
  dtype?: any,
  strict?: any,
): Series => {
  if (typeof arg0 === "string") {
    const _s = arrayToJsSeries(arg0, arg1, dtype, strict);

    return _Series(_s) as any;
  }

  return SeriesConstructor("", arg0);
};
const isSeries = (anyVal: any): anyVal is Series => {
  try {
    return anyVal?.[Symbol.toStringTag]?.() === "Series";
  } catch (_err) {
    return false;
  }
};

const from = (name, values?: ArrayLike<any>): Series => {
  if (Array.isArray(name)) {
    return SeriesConstructor("", values);
  }
  return SeriesConstructor(name, values);
};
const of = (...values: any[]): Series => {
  return Series.from(values);
};

export const Series: SeriesConstructor = Object.assign(SeriesConstructor, {
  isSeries,
  from,
  of,
  deserialize: (buf, fmt) => _Series(pli.JsSeries.deserialize(buf, fmt)),
});
