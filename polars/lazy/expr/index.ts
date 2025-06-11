import * as dt from "./datetime";
import * as lst from "./list";
import * as str from "./string";
import * as struct from "./struct";
export type { ExprString as StringNamespace } from "./string";
export type { ExprList as ListNamespace } from "./list";
export type { ExprDateTime as DatetimeNamespace } from "./datetime";
export type { ExprStruct as StructNamespace } from "./struct";

import { isRegExp } from "node:util/types";
import type { DataType } from "../../datatypes";
import pli from "../../internals/polars_internal";
import { Series } from "../../series";
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
} from "../../shared_traits";
import type {
  FillNullStrategy,
  InterpolationMethod,
  RankMethod,
} from "../../types";
import {
  type ExprOrString,
  INSPECT_SYMBOL,
  regexToString,
  selectionToExprList,
} from "../../utils";
/**
 * Expressions that can be used in various contexts.
 */
export interface Expr
  extends Rolling<Expr>,
    Arithmetic<Expr>,
    Comparison<Expr>,
    Cumulative<Expr>,
    Sample<Expr>,
    Round<Expr>,
    EwmOps<Expr>,
    Serialize {
  _expr: any;
  /**
   * Datetime namespace
   */
  get date(): dt.ExprDateTime;
  /**
   * String namespace
   */
  get str(): str.ExprString;
  /**
   * List namespace
   */
  get lst(): lst.ExprList;
  /**
   * Struct namespace
   */
  get struct(): struct.ExprStruct;
  [Symbol.toStringTag](): string;
  [INSPECT_SYMBOL](): string;
  toString(): string;
  /** compat with `JSON.stringify` */
  toJSON(): string;
  /** Take absolute values */
  abs(): Expr;
  /**
   * Get the group indexes of the group by operation.
   * Should be used in aggregation context only.
   * @example
   * ```
    >>> const df = pl.DataFrame(
    ...     {
    ...         "group": [
    ...             "one",
    ...             "one",
    ...             "one",
    ...             "two",
    ...             "two",
    ...             "two",
    ...         ],
    ...         "value": [94, 95, 96, 97, 97, 99],
    ...     }
    ... )
    >>> df.group_by("group", maintainOrder=True).agg(pl.col("value").aggGroups())
    shape: (2, 2)
    ┌───────┬───────────┐
    │ group ┆ value     │
    │ ---   ┆ ---       │
    │ str   ┆ list[u32] │
    ╞═══════╪═══════════╡
    │ one   ┆ [0, 1, 2] │
    │ two   ┆ [3, 4, 5] │
    └───────┴───────────┘
   *```
   */
  aggGroups(): Expr;
  /**
   * Rename the output of an expression.
   * @param name new name
   * @see {@link Expr.as}
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "a": [1, 2, 3],
   * ...   "b": ["a", "b", None],
   * ... });
   * > df
   * shape: (3, 2)
   * ╭─────┬──────╮
   * │ a   ┆ b    │
   * │ --- ┆ ---  │
   * │ i64 ┆ str  │
   * ╞═════╪══════╡
   * │ 1   ┆ "a"  │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 2   ┆ "b"  │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 3   ┆ null │
   * ╰─────┴──────╯
   * > df.select([
   * ...   pl.col("a").alias("bar"),
   * ...   pl.col("b").alias("foo"),
   * ... ])
   * shape: (3, 2)
   * ╭─────┬──────╮
   * │ bar ┆ foo  │
   * │ --- ┆ ---  │
   * │ i64 ┆ str  │
   * ╞═════╪══════╡
   * │ 1   ┆ "a"  │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 2   ┆ "b"  │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 3   ┆ null │
   * ╰─────┴──────╯
   *```
   */
  alias(name: string): Expr;
  and(other: any): Expr;
  /**
   * Compute the element-wise value for the inverse cosine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
      >>> const df = pl.DataFrame({"a": [0.0]})
      >>> df.select(pl.col("a").acrcos())
      shape: (1, 1)
      ┌──────────┐
      │ a        │
      │ ---      │
      │ f64      │
      ╞══════════╡
      │ 1.570796 │
      └──────────┘
   * ```
   */
  arccos(): Expr;
  /**
   * Compute the element-wise value for the inverse hyperbolic cosine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
      >>> const df = pl.DataFrame({"a": [1.0]})
      >>> df.select(pl.col("a").acrcosh())
      shape: (1, 1)
      ┌─────┐
      │ a   │
      │ --- │
      │ f64 │
      ╞═════╡
      │ 0.0 │
      └─────┘
   * ```
   */
  arccosh(): Expr;
  /**
   * Compute the element-wise value for the inverse sine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
  >>> const df = pl.DataFrame({"a": [1.0]})
  >>> df.select(pl.col("a").acrsin())
  shape: (1, 1)
  ┌──────────┐
  │ a        │
  │ ---      │
  │ f64      │
  ╞══════════╡
  │ 1.570796 │
  └──────────┘
  * ```
  */
  arcsin(): Expr;
  /**
   * Compute the element-wise value for the inverse hyperbolic sine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [1.0]})
    >>> df.select(pl.col("a").acrsinh())
    shape: (1, 1)
    ┌──────────┐
    │ a        │
    │ ---      │
    │ f64      │
    ╞══════════╡
    │ 0.881374 │
    └──────────┘
    * ```
    */
  arcsinh(): Expr;
  /**
   * Compute the element-wise value for the inverse tangent.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [1.0]})
    >>> df.select(pl.col("a").arctan())
    shape: (1, 1)
    ┌──────────┐
    │ a        │
    │ ---      │
    │ f64      │
    ╞══════════╡
    │ 0.785398 │
    └──────────┘
   * ```
   */
  arctan(): Expr;
  /**
   * Compute the element-wise value for the inverse hyperbolic tangent.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [1.0]})
    >>> df.select(pl.col("a").arctanh())
    shape: (1, 1)
    ┌─────┐
    │ a   │
    │ --- │
    │ f64 │
    ╞═════╡
    │ inf │
    └─────┘
   * ```
   */
  arctanh(): Expr;
  /** Get the index of the maximal value. */
  argMax(): Expr;
  /** Get the index of the minimal value. */
  argMin(): Expr;
  /**
   * Get the index values that would sort this column.
   * @param descending
   *     - false -> order from small to large.
   *     - true -> order from large to small.
   * @returns UInt32 Series
   */
  argSort(descending?: boolean, maintainOrder?: boolean): Expr;
  argSort({
    reverse, // deprecated
    maintainOrder,
  }: { reverse?: boolean; maintainOrder?: boolean }): Expr;
  argSort({
    descending,
    maintainOrder,
  }: { descending?: boolean; maintainOrder?: boolean }): Expr;
  /** Get index of first unique value. */
  argUnique(): Expr;
  /** @see {@link Expr.alias} */
  as(name: string): Expr;
  /** Fill missing values with the next to be seen values */
  backwardFill(): Expr;
  /** Cast between data types. */
  cast(dtype: DataType, strict?: boolean): Expr;
  /**
   * Compute the element-wise value for the cosine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
      >>> const df = pl.DataFrame({"a": [0.0]})
      >>> df.select(pl.col("a").cos())
      shape: (1, 1)
      ┌─────┐
      │ a   │
      │ --- │
      │ f64 │
      ╞═════╡
      │ 1.0 │
      └─────┘
   * ```
   */
  cos(): Expr;
  /**
   * Compute the element-wise value for the hyperbolic cosine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
      >>> const df = pl.DataFrame({"a": [1.0]})
      >>> df.select(pl.col("a").cosh())
      shape: (1, 1)
      ┌──────────┐
      │ a        │
      │ ---      │
      │ f64      │
      ╞══════════╡
      │ 1.543081 │
      └──────────┘
   * ```
   */
  cosh(): Expr;
  /**
   * Compute the element-wise value for the cotangent.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [1.0]})
    >>> df.select(pl.col("a").cot().round(2))
    shape: (1, 1)
    ┌──────┐
    │ a    │
    │ ---  │
    │ f64  │
    ╞══════╡
    │ 0.64 │
    └──────┘
   * ```
   */
  cot(): Expr;
  /** Count the number of values in this expression */
  count(): Expr;
  /** Calculate the first discrete difference between shifted items.
   * @param n number of slots to shift
   * @param nullBehavior How to handle null values. ignore or drop
   * @example
   * ```
   * const df = pl.DataFrame({"int": [20, 10, 30, 25, 35]})
   * df.withColumns(pl.col("int").diff())
    shape: (5, 2)
    ┌─────┬────────┐
    │ int ┆ change │
    │ --- ┆ ---    │
    │ i64 ┆ i64    │
    ╞═════╪════════╡
    │ 20  ┆ null   │
    │ 10  ┆ -10    │
    │ 30  ┆ 20     │
    │ 25  ┆ -5     │
    │ 35  ┆ 10     │
    └─────┴────────┘

    >>> df.withColumns(pl.col("int").diff(n=2))
    shape: (5, 2)
    ┌─────┬────────┐
    │ int ┆ change │
    │ --- ┆ ---    │
    │ i64 ┆ i64    │
    ╞═════╪════════╡
    │ 20  ┆ null   │
    │ 10  ┆ null   │
    │ 30  ┆ 10     │
    │ 25  ┆ 15     │
    │ 35  ┆ 5      │
    └─────┴────────┘

    >>> df.select(pl.col("int").diff(2,"drop").alias("diff"))
    shape: (3, 1)
    ┌──────┐
    │ diff │
    │ ---  │
    │ i64  │
    ╞══════╡
    │ 10   │
    │ 15   │
    │ 5    │
    └──────┘
   * ```
   */
  diff(n: number, nullBehavior: "ignore" | "drop"): Expr;
  diff(o: { n: number; nullBehavior: "ignore" | "drop" }): Expr;
  /**
   * Compute the dot/inner product between two Expressions
   * @param other Expression to compute dot product with
   */
  dot(other: any): Expr;
  /**
   * Exclude certain columns from a wildcard/regex selection.
   *
   * You may also use regexes in the exclude list. They must start with `^` and end with `$`.
   *
   * @param columns Column(s) to exclude from selection
   * @example
   * ```
   *  > const df = pl.DataFrame({
   *  ...   "a": [1, 2, 3],
   *  ...   "b": ["a", "b", None],
   *  ...   "c": [None, 2, 1],
   *  ...});
   *  > df
   *  shape: (3, 3)
   *  ╭─────┬──────┬──────╮
   *  │ a   ┆ b    ┆ c    │
   *  │ --- ┆ ---  ┆ ---  │
   *  │ i64 ┆ str  ┆ i64  │
   *  ╞═════╪══════╪══════╡
   *  │ 1   ┆ "a"  ┆ null │
   *  ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   *  │ 2   ┆ "b"  ┆ 2    │
   *  ├╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   *  │ 3   ┆ null ┆ 1    │
   *  ╰─────┴──────┴──────╯
   *  > df.select(
   *  ...   pl.col("*").exclude("b"),
   *  ... );
   * shape: (3, 2)
   * ╭─────┬──────╮
   * │ a   ┆ c    │
   * │ --- ┆ ---  │
   * │ i64 ┆ i64  │
   * ╞═════╪══════╡
   * │ 1   ┆ null │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 2   ┆ 2    │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 3   ┆ 1    │
   * ╰─────┴──────╯
   * ```
   */
  exclude(column: string, ...columns: string[]): Expr;
  /**
   * Compute the exponential, element-wise.
   * @example
   * ``` 
    >>> const df = pl.DataFrame({"values": [1.0, 2.0, 4.0]})
    >>> df.select(pl.col("values").exp())
    shape: (3, 1)
    ┌──────────┐
    │ values   │
    │ ---      │
    │ f64      │
    ╞══════════╡
    │ 2.718282 │
    │ 7.389056 │
    │ 54.59815 │
    └──────────┘
   * ```
   */
  exp(): Expr;
  /**
   * Explode a list or utf8 Series.
   *
   * This means that every item is expanded to a new row.
   */
  explode(): Expr;
  /**
   * Extend the Series with given number of values.
   * @param value The value to extend the Series with. This value may be null to fill with nulls.
   * @param n The number of values to extend.
   */
  extendConstant(value: any, n: number): Expr;
  extendConstant(opt: { value: any; n: number }): Expr;
  /** Fill nan value with a fill value */
  fillNan(other: any): Expr;
  /** Fill null value with a fill value or strategy */
  fillNull(other: any | FillNullStrategy): Expr;
  /**
   * Filter a single column.
   *
   * Mostly useful in in aggregation context.
   * If you want to filter on a DataFrame level, use `LazyFrame.filter`.
   * @param predicate Boolean expression.
   */
  filter(predicate: Expr): Expr;
  /** Get the first value. */
  first(): Expr;
  /** @see {@link Expr.explode} */
  flatten(): Expr;
  /** Fill missing values with the latest seen values */
  forwardFill(): Expr;
  /**
   * Take values by index.
   * @param index An expression that leads to a UInt32 dtyped Series.
   */
  gather(index: Expr | number[] | Series): Expr;
  gather({ index }: { index: Expr | number[] | Series }): Expr;
  /** Take every nth value in the Series and return as a new Series. */
  gatherEvery(n: number, offset?: number): Expr;
  /** Hash the Series. */
  hash(k0?: number, k1?: number, k2?: number, k3?: number): Expr;
  hash({
    k0,
    k1,
    k2,
    k3,
  }: { k0?: number; k1?: number; k2?: number; k3?: number }): Expr;
  /** Take the first n values.  */
  head(length?: number): Expr;
  head({ length }: { length: number }): Expr;
  implode(): Expr;
  inner(): any;
  /** Interpolate intermediate values. The interpolation method is linear. */
  interpolate(): Expr;
  /** Get mask of duplicated values. */
  isDuplicated(): Expr;
  /** Create a boolean expression returning `true` where the expression values are finite. */
  isFinite(): Expr;
  /** Get a mask of the first unique value. */
  isFirstDistinct(): Expr;
  /**
   * Check if elements of this Series are in the right Series, or List values of the right Series.
   *
   * @param other Series of primitive type or List type.
   * @param nullsEquals default False, If True, treat null as a distinct value. Null values will not propagate.
   * @returns Expr that evaluates to a Boolean Series.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "sets": [[1, 2, 3], [1, 2], [9, 10]],
   * ...    "optional_members": [1, 2, 3]
   * ... });
   * > df.select(
   * ...   pl.col("optional_members").isIn("sets").alias("contains")
   * ... );
   * shape: (3, 1)
   * ┌──────────┐
   * │ contains │
   * │ ---      │
   * │ bool     │
   * ╞══════════╡
   * │ true     │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ true     │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ false    │
   * └──────────┘
   * ```
   */
  isIn(other, nullsEquals?: boolean): Expr;
  /** Create a boolean expression returning `true` where the expression values are infinite. */
  isInfinite(): Expr;
  /** Create a boolean expression returning `true` where the expression values are NaN (Not A Number). */
  isNan(): Expr;
  /** Create a boolean expression returning `true` where the expression values are not NaN (Not A Number). */
  isNotNan(): Expr;
  /** Create a boolean expression returning `true` where the expression does not contain null values. */
  isNotNull(): Expr;
  /** Create a boolean expression returning `True` where the expression contains null values. */
  isNull(): Expr;
  /** Get mask of unique values. */
  isUnique(): Expr;
  /**
   *  Keep the original root name of the expression.
   *
   * A groupby aggregation often changes the name of a column.
   * With `keepName` we can keep the original name of the column
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "a": [1, 2, 3],
   * ...   "b": ["a", "b", None],
   * ... });
   *
   * > df
   * ...   .groupBy("a")
   * ...   .agg(pl.col("b").list())
   * ...   .sort({by:"a"});
   *
   * shape: (3, 2)
   * ╭─────┬────────────╮
   * │ a   ┆ b_agg_list │
   * │ --- ┆ ---        │
   * │ i64 ┆ list [str] │
   * ╞═════╪════════════╡
   * │ 1   ┆ [a]        │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 2   ┆ [b]        │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 3   ┆ [null]     │
   * ╰─────┴────────────╯
   *
   * Keep the original column name:
   *
   * > df
   * ...   .groupby("a")
   * ...   .agg(col("b").list().keepName())
   * ...   .sort({by:"a"})
   *
   * shape: (3, 2)
   * ╭─────┬────────────╮
   * │ a   ┆ b          │
   * │ --- ┆ ---        │
   * │ i64 ┆ list [str] │
   * ╞═════╪════════════╡
   * │ 1   ┆ [a]        │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 2   ┆ [b]        │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 3   ┆ [null]     │
   * ╰─────┴────────────╯
   * ```
   */
  keepName(): Expr;
  kurtosis(): Expr;
  kurtosis(fisher: boolean, bias?: boolean): Expr;
  kurtosis({ fisher, bias }: { fisher?: boolean; bias?: boolean }): Expr;
  /** Get the last value.  */
  last(): Expr;
  /** Aggregate to list. */
  list(): Expr;
  /***
   * Compute the natural logarithm of each element plus one.
   * This computes `log(1 + x)` but is more numerically stable for `x` close to zero.
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [1, 2, 3]})
    >>> df.select(pl.col("a").log1p())
    shape: (3, 1)
    ┌──────────┐
    │ a        │
    │ ---      │
    │ f64      │
    ╞══════════╡
    │ 0.693147 │
    │ 1.098612 │
    │ 1.386294 │
    └──────────┘
   * ```
   */
  log1p(): Expr;
  /**
   * Compute the logarithm to a given base.
   * @param base - Given base, defaults to `e`
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [1, 2, 3]})
    >>> df.select(pl.col("a").log(base=2))
    shape: (3, 1)
    ┌──────────┐
    │ a        │
    │ ---      │
    │ f64      │
    ╞══════════╡
    │ 0.0      │
    │ 1.0      │
    │ 1.584963 │
    └──────────┘
   * ```
   */
  log(base?: number): Expr;
  /** Returns a unit Series with the lowest value possible for the dtype of this expression. */
  lowerBound(): Expr;
  peakMax(): Expr;
  peakMin(): Expr;
  /** Compute the max value of the arrays in the list */
  max(): Expr;
  /** Compute the mean value of the arrays in the list */
  mean(): Expr;
  /** Get median value. */
  median(): Expr;
  /** Get minimum value. */
  min(): Expr;
  /** Compute the most occurring value(s). Can return multiple Values */
  mode(): Expr;
  /** Negate a boolean expression. */
  not(): Expr;
  /** Count unique values. */
  nUnique(): Expr;
  or(other: any): Expr;
  /**
   * Apply window function over a subgroup.
   *
   * This is similar to a groupby + aggregation + self join.
   * Or similar to [window functions in Postgres](https://www.postgresql.org/docs/9.1/tutorial-window.html)
   * @param partitionBy Column(s) to partition by.
   *
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...  "groups": [1, 1, 2, 2, 1, 2, 3, 3, 1],
   * ...  "values": [1, 2, 3, 4, 5, 6, 7, 8, 8],
   * ... });
   * > df.select(
   * ...     pl.col("groups").sum().over("groups")
   * ... );
   *     ╭────────┬────────╮
   *     │ groups ┆ values │
   *     │ ---    ┆ ---    │
   *     │ i32    ┆ i32    │
   *     ╞════════╪════════╡
   *     │ 1      ┆ 16     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 1      ┆ 16     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 2      ┆ 13     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 2      ┆ 13     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ ...    ┆ ...    │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 1      ┆ 16     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 2      ┆ 13     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 3      ┆ 15     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 3      ┆ 15     │
   *     ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
   *     │ 1      ┆ 16     │
   *     ╰────────┴────────╯
   * ```
   */
  over(by: ExprOrString, ...partitionBy: ExprOrString[]): Expr;
  /** Raise expression to the power of exponent. */
  pow(exponent: number): Expr;
  pow({ exponent }: { exponent: number }): Expr;
  /**
   * Add a prefix the to root column name of the expression.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "A": [1, 2, 3, 4, 5],
   * ...   "fruits": ["banana", "banana", "apple", "apple", "banana"],
   * ...   "B": [5, 4, 3, 2, 1],
   * ...   "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
   * ... });
   * shape: (5, 4)
   * ╭─────┬──────────┬─────┬──────────╮
   * │ A   ┆ fruits   ┆ B   ┆ cars     │
   * │ --- ┆ ---      ┆ --- ┆ ---      │
   * │ i64 ┆ str      ┆ i64 ┆ str      │
   * ╞═════╪══════════╪═════╪══════════╡
   * │ 1   ┆ "banana" ┆ 5   ┆ "beetle" │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
   * │ 2   ┆ "banana" ┆ 4   ┆ "audi"   │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
   * │ 3   ┆ "apple"  ┆ 3   ┆ "beetle" │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
   * │ 4   ┆ "apple"  ┆ 2   ┆ "beetle" │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
   * │ 5   ┆ "banana" ┆ 1   ┆ "beetle" │
   * ╰─────┴──────────┴─────┴──────────╯
   * > df.select(
   * ...   pl.col("*").reverse().prefix("reverse_"),
   * ... )
   * shape: (5, 8)
   * ╭───────────┬────────────────┬───────────┬──────────────╮
   * │ reverse_A ┆ reverse_fruits ┆ reverse_B ┆ reverse_cars │
   * │ ---       ┆ ---            ┆ ---       ┆ ---          │
   * │ i64       ┆ str            ┆ i64       ┆ str          │
   * ╞═══════════╪════════════════╪═══════════╪══════════════╡
   * │ 5         ┆ "banana"       ┆ 1         ┆ "beetle"     │
   * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 4         ┆ "apple"        ┆ 2         ┆ "beetle"     │
   * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 3         ┆ "apple"        ┆ 3         ┆ "beetle"     │
   * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 2         ┆ "banana"       ┆ 4         ┆ "audi"       │
   * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 1         ┆ "banana"       ┆ 5         ┆ "beetle"     │
   * ╰───────────┴────────────────┴───────────┴──────────────╯
   * ```
   */
  prefix(prefix: string): Expr;
  /** Get quantile value. */
  quantile(quantile: number | Expr): Expr;
  /**
   * Assign ranks to data, dealing with ties appropriately.
   * @param method : {'average', 'min', 'max', 'dense', 'ordinal', 'random'}
   * @param descending - Rank in descending order.
   * */
  rank(method?: RankMethod, descending?: boolean): Expr;
  rank({ method, descending }: { method: string; descending: boolean }): Expr;
  reinterpret(signed?: boolean): Expr;
  reinterpret({ signed }: { signed: boolean }): Expr;
  /**
   * Repeat the elements in this Series `n` times by dictated by the number given by `by`.
   * The elements are expanded into a `List`
   * @param by Numeric column that determines how often the values will be repeated.
   *
   * The column will be coerced to UInt32. Give this dtype to make the coercion a no-op.
   */
  repeatBy(by: Expr | string): Expr;
  /**
   * Replace values by different values.
   * @param old - Value or sequence of values to replace.
                  Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.
   * @param new_ - Value or sequence of values to replace by.
                  Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.
                  Length must match the length of `old` or have length 1.
   * @param default_ - Set values that were not replaced to this value.
                      Defaults to keeping the original value.
                      Accepts expression input. Non-expression inputs are parsed as literals.
   * @param returnDtype - The data type of the resulting expression. If set to `None` (default), the data type is determined automatically based on the other inputs.
   * @see {@link replace}
   * @example
   * Replace a single value by another value. Values that were not replaced remain unchanged.
   * ```
    >>> const df = pl.DataFrame({"a": [1, 2, 2, 3]});
    >>> df.withColumns(pl.col("a").replace(2, 100).alias("replaced"));
    shape: (4, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ i64 ┆ i64      │
    ╞═════╪══════════╡
    │ 1   ┆ 1        │
    │ 2   ┆ 100      │
    │ 2   ┆ 100      │
    │ 3   ┆ 3        │
    └─────┴──────────┘
   * ```
   * Replace multiple values by passing sequences to the `old` and `new_` parameters.
   * ```
    >>> df.withColumns(pl.col("a").replace([2, 3], [100, 200]).alias("replaced"));
    shape: (4, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ i64 ┆ i64      │
    ╞═════╪══════════╡
    │ 1   ┆ 1        │
    │ 2   ┆ 100      │
    │ 2   ┆ 100      │
    │ 3   ┆ 200      │
    └─────┴──────────┘
  * ```
  * Passing a mapping with replacements is also supported as syntactic sugar.
    Specify a default to set all values that were not matched.
  * ```
    >>> const mapping = {2: 100, 3: 200};
    >>> df.withColumns(pl.col("a").replaceStrict({ old: mapping, default_: -1, returnDtype: pl.Int64 }).alias("replaced");
    shape: (4, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ i64 ┆ i64      │
    ╞═════╪══════════╡
    │ 1   ┆ -1       │
    │ 2   ┆ 100      │
    │ 2   ┆ 100      │
    │ 3   ┆ 200      │
    └─────┴──────────┘
  * ```
    Replacing by values of a different data type sets the return type based on
    a combination of the `new_` data type and either the original data type or the
    default data type if it was set.
  * ```
    >>> const df = pl.DataFrame({"a": ["x", "y", "z"]});
    >>> const mapping = {"x": 1, "y": 2, "z": 3};
    >>> df.withColumns(pl.col("a").replaceStrict({ old: mapping }).alias("replaced"));
    shape: (3, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ str ┆ str      │
    ╞═════╪══════════╡
    │ x   ┆ 1        │
    │ y   ┆ 2        │
    │ z   ┆ 3        │
    └─────┴──────────┘
    >>> df.withColumns(pl.col("a").replaceStrict({ old: mapping, default_: None }).alias("replaced"));
    shape: (3, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ str ┆ i64      │
    ╞═════╪══════════╡
    │ x   ┆ 1        │
    │ y   ┆ 2        │
    │ z   ┆ 3        │
    └─────┴──────────┘
  * ```
    Set the `returnDtype` parameter to control the resulting data type directly.
  * ```
    >>> df.withColumns(pl.col("a").replaceStrict({ old: mapping, returnDtype: pl.UInt8 }).alias("replaced"));
    shape: (3, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ str ┆ u8       │
    ╞═════╪══════════╡
    │ x   ┆ 1        │
    │ y   ┆ 2        │
    │ z   ┆ 3        │
    └─────┴──────────┘
  * ```
  * Expression input is supported for all parameters.
  * ```
    >>> const df = pl.DataFrame({"a": [1, 2, 2, 3], "b": [1.5, 2.5, 5.0, 1.0]});
    >>> df.withColumns(
    ...         pl.col("a").replaceStrict({
    ...         old: pl.col("a").max(),
    ...         new_: pl.col("b").sum(),
    ...         default_: pl.col("b"),
    ...     }).alias("replaced")
    ... );
    shape: (4, 3)
    ┌─────┬─────┬──────────┐
    │ a   ┆ b   ┆ replaced │
    │ --- ┆ --- ┆ ---      │
    │ i64 ┆ f64 ┆ f64      │
    ╞═════╪═════╪══════════╡
    │ 1   ┆ 1.5 ┆ 1.5      │
    │ 2   ┆ 2.5 ┆ 2.5      │
    │ 2   ┆ 5.0 ┆ 5.0      │
    │ 3   ┆ 1.0 ┆ 10.0     │
    └─────┴─────┴──────────┘
   * ``` 
   */
  replaceStrict(
    old: Expr | string | number | (number | string)[],
    new_: Expr | string | number | (number | string)[],
    default_?: Expr | string | number | (number | string)[],
    returnDtype?: DataType,
  ): Expr;
  replaceStrict({
    old,
    new_,
    default_,
    returnDtype,
  }: {
    old: unknown | Expr | string | number | (number | string)[];
    new_?: Expr | string | number | (number | string)[];
    default_?: Expr | string | number | (number | string)[];
    returnDtype?: DataType;
  }): Expr;
  /**
   * Replace the given values by different values of the same data type.
   * @param old - Value or sequence of values to replace.
                  Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.
   * @param new_ - Value or sequence of values to replace by.
                  Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals.
                  Length must match the length of `old` or have length 1.
   * @see {@link replaceStrict}
   * @see {@link replace}
   * @example
   * Replace a single value by another value. Values that were not replaced remain unchanged.
   * ```
    >>> const df = pl.DataFrame({"a": [1, 2, 2, 3]});
    >>> df.withColumns(pl.col("a").replace(2, 100).alias("replaced"));
    shape: (4, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ i64 ┆ i64      │
    ╞═════╪══════════╡
    │ 1   ┆ 1        │
    │ 2   ┆ 100      │
    │ 2   ┆ 100      │
    │ 3   ┆ 3        │
    └─────┴──────────┘
   * ```
   * Replace multiple values by passing sequences to the `old` and `new_` parameters.
   * ```
    >>> df.withColumns(pl.col("a").replace([2, 3], [100, 200]).alias("replaced"));
    shape: (4, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ i64 ┆ i64      │
    ╞═════╪══════════╡
    │ 1   ┆ 1        │
    │ 2   ┆ 100      │
    │ 2   ┆ 100      │
    │ 3   ┆ 200      │
    └─────┴──────────┘
  * ```
  * Passing a mapping with replacements is also supported as syntactic sugar.
    Specify a default to set all values that were not matched.
  * ```
    >>> const mapping = {2: 100, 3: 200};
    >>> df.withColumns(pl.col("a").replace({ old: mapping }).alias("replaced");
    shape: (4, 2)
    ┌─────┬──────────┐
    │ a   ┆ replaced │
    │ --- ┆ ---      │
    │ i64 ┆ i64      │
    ╞═════╪══════════╡
    │ 1   ┆ -1       │
    │ 2   ┆ 100      │
    │ 2   ┆ 100      │
    │ 3   ┆ 200      │
    └─────┴──────────┘
  * ```
  */
  replace(
    old: Expr | string | number | (number | string)[],
    new_: Expr | string | number | (number | string)[],
  ): Expr;
  replace({
    old,
    new_,
  }: {
    old: unknown | Expr | string | number | (number | string)[];
    new_?: Expr | string | number | (number | string)[];
  }): Expr;
  /** Reverse the arrays in the list */
  reverse(): Expr;
  /**
   * Shift the values by a given period and fill the parts that will be empty due to this operation
   * @param periods number of places to shift (may be negative).
   */
  shift(periods?: number): Expr;
  shift({ periods }: { periods: number }): Expr;
  /**
   * Shift the values by a given period and fill the parts that will be empty due to this operation
   * @param periods Number of places to shift (may be negative).
   * @param fillValue Fill null values with the result of this expression.
   */
  shiftAndFill(periods: number, fillValue: number): Expr;
  shiftAndFill({
    periods,
    fillValue,
  }: { periods: number; fillValue: number }): Expr;
  /**
   * Compute the element-wise value for the sine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [0.0]})
    >>> df.select(pl.col("a").sin())
    shape: (1, 1)
    ┌─────┐
    │ a   │
    │ --- │
    │ f64 │
    ╞═════╡
    │ 0.0 │
    └─────┘
   *```
   */
  sin(): Expr;
  /**
   * Compute the element-wise value for the hyperbolic sine.
   * @returns Expression of data type :class:`Float64`.
   * @example
   * ```
    >>> const df = pl.DataFrame({"a": [1.0]})
    >>> df.select(pl.col("a").sinh())
    shape: (1, 1)
    ┌──────────┐
    │ a        │
    │ ---      │
    │ f64      │
    ╞══════════╡
    │ 1.175201 │
    └──────────┘
   *```
   */
  sinh(): Expr;
  /**
   * Compute the sample skewness of a data set.
   * For normally distributed data, the skewness should be about zero. For
   * unimodal continuous distributions, a skewness value greater than zero means
   * that there is more weight in the right tail of the distribution.
   * ___
   * @param bias If False, then the calculations are corrected for statistical bias.
   */
  skew(bias?: boolean): Expr;
  skew({ bias }: { bias: boolean }): Expr;
  /** Slice the Series. */
  slice(offset: number | Expr, length: number | Expr): Expr;
  slice({
    offset,
    length,
  }: { offset: number | Expr; length: number | Expr }): Expr;
  /**
   * Sort this column. In projection/ selection context the whole column is sorted.
   * @param descending
   * * false -> order from small to large.
   * * true -> order from large to small.
   * @param nullsLast If true nulls are considered to be larger than any valid value
   */
  sort(descending?: boolean, nullsLast?: boolean): Expr;
  sort({
    descending,
    nullsLast,
  }: { descending?: boolean; nullsLast?: boolean }): Expr;
  sort({
    reverse, // deprecated
    nullsLast,
  }: { reverse?: boolean; nullsLast?: boolean }): Expr;
  /**
   * Sort this column by the ordering of another column, or multiple other columns.
      In projection/ selection context the whole column is sorted.
      If used in a groupby context, the groups are sorted.

      Parameters
      ----------
      @param by The column(s) used for sorting.
      @param descending
          false -> order from small to large.
          true -> order from large to small.
   */
  sortBy(
    by: ExprOrString[] | ExprOrString,
    descending?: boolean | boolean[],
  ): Expr;
  sortBy(options: {
    by: ExprOrString[] | ExprOrString;
    descending?: boolean | boolean[];
  }): Expr;
  sortBy(options: {
    by: ExprOrString[] | ExprOrString;
    reverse?: boolean | boolean[];
  }): Expr;
  /** Get standard deviation. */
  std(): Expr;
  /** Add a suffix the to root column name of the expression. */
  suffix(suffix: string): Expr;
  /**
   * Get sum value.
   * Dtypes in {Int8, UInt8, Int16, UInt16} are cast to Int64 before summing to prevent overflow issues.
   */
  sum(): Expr;
  /** Take the last n values. */
  tail(length?: number): Expr;
  tail({ length }: { length: number }): Expr;
  /**
   * Compute the element-wise value for the tangent.
   * @returns Expression of data type :class:`Float64`.
   * @example
   *```
    >>> const df = pl.DataFrame({"a": [1.0]})
    >>> df.select(pl.col("a").tan().round(2))
    shape: (1, 1)
    ┌──────┐
    │ a    │
    │ ---  │
    │ f64  │
    ╞══════╡
    │ 1.56 │
    └──────┘
   *```
   */
  tan(): Expr;
  /**
   * Compute the element-wise value for the hyperbolic tangent.
   * @returns Expression of data type :class:`Float64`.
   * @example
   *```
    >>> const df = pl.DataFrame({"a": [1.0]})
    >>> df.select(pl.col("a").tanh())
    shape: (1, 1)
    ┌──────────┐
    │ a        │
    │ ---      │
    │ f64      │
    ╞══════════╡
    │ 0.761594 │
    └──────────┘
   *```
   */
  tanh(): Expr;
  /**
   * Get the unique values of this expression;
   * @param maintainOrder Maintain order of data. This requires more work.
   */
  unique(maintainOrder?: boolean): Expr;
  unique(opt: { maintainOrder: boolean }): Expr;
  /** Returns a unit Series with the highest value possible for the dtype of this expression. */
  upperBound(): Expr;
  /** Get variance. */
  var(): Expr;
  /** Alias for filter: @see {@link filter} */
  where(predicate: Expr): Expr;
}
/** @ignore */
export const _Expr = (_expr: any): Expr => {
  const unwrap = (method: string, ...args: any[]) => {
    return _expr[method as any](...args);
  };
  const wrap = (method, ...args): Expr => {
    return _Expr(unwrap(method, ...args));
  };

  const wrapExprArg =
    (method: string, lit = false) =>
    (other: any) => {
      const expr = exprToLitOrExpr(other, lit).inner();
      return wrap(method, expr);
    };

  const rolling =
    (method: string) =>
    (opts, weights?, minPeriods?, center?): Expr => {
      const windowSize =
        opts?.["windowSize"] ?? (typeof opts === "number" ? opts : null);
      if (windowSize === null) {
        throw new Error("window size is required");
      }
      const callOpts = {
        windowSize: windowSize,
        weights: opts?.["weights"] ?? weights,
        minPeriods: opts?.["minPeriods"] ?? minPeriods ?? windowSize,
        center: opts?.["center"] ?? center ?? false,
      };

      return wrap(method, callOpts);
    };

  return {
    _expr,
    [Symbol.toStringTag]() {
      return "Expr";
    },
    [INSPECT_SYMBOL]() {
      return _expr.toString();
    },
    serialize(format): any {
      return _expr.serialize(format);
    },
    toString() {
      return _expr.toString();
    },
    toJSON(...args: any[]) {
      // this is passed by `JSON.stringify` when calling `toJSON()`
      if (args[0] === "") {
        return _expr.toJs();
      }

      return _expr.serialize("json").toString();
    },
    get str() {
      return str.ExprStringFunctions(_expr);
    },
    get lst() {
      return lst.ExprListFunctions(_expr);
    },
    get date() {
      return dt.ExprDateTimeFunctions(_expr);
    },
    get struct() {
      return struct.ExprStructFunctions(_expr);
    },
    abs() {
      return _Expr(_expr.abs());
    },
    aggGroups() {
      return _Expr(_expr.aggGroups());
    },
    alias(name) {
      return _Expr(_expr.alias(name));
    },
    inner() {
      return _expr;
    },
    and(other) {
      const expr = (exprToLitOrExpr(other, false) as any).inner();
      return _Expr(_expr.and(expr));
    },
    arccos() {
      return _Expr(_expr.arccos());
    },
    arccosh() {
      return _Expr(_expr.arccosh());
    },
    arcsin() {
      return _Expr(_expr.arcsin());
    },
    arcsinh() {
      return _Expr(_expr.arcsinh());
    },
    arctan() {
      return _Expr(_expr.arctan());
    },
    arctanh() {
      return _Expr(_expr.arctanh());
    },
    argMax() {
      return _Expr(_expr.argMax());
    },
    argMin() {
      return _Expr(_expr.argMin());
    },
    argSort(descending: any = false, maintainOrder?: boolean) {
      descending = descending?.descending ?? descending?.reverse ?? descending;
      maintainOrder = descending?.maintainOrder ?? maintainOrder;
      return _Expr(_expr.argSort(descending, false, false, maintainOrder));
    },
    argUnique() {
      return _Expr(_expr.argUnique());
    },
    as(name) {
      return _Expr(_expr.alias(name));
    },
    backwardFill() {
      return _Expr(_expr.backwardFill());
    },
    cast(dtype, strict = false) {
      return _Expr(_expr.cast(dtype, strict));
    },
    ceil() {
      return _Expr(_expr.ceil());
    },
    clip(arg, max?) {
      if (typeof arg === "number") {
        return _Expr(
          _expr.clip(exprToLitOrExpr(arg)._expr, exprToLitOrExpr(max)._expr),
        );
      }
      return _Expr(
        _expr.clip(
          exprToLitOrExpr(arg.min)._expr,
          exprToLitOrExpr(arg.max)._expr,
        ),
      );
    },
    cos() {
      return _Expr(_expr.cos());
    },
    cosh() {
      return _Expr(_expr.cosh());
    },
    cot() {
      return _Expr(_expr.cot());
    },
    count() {
      return _Expr(_expr.count());
    },
    cumCount(reverse: any = false) {
      reverse = reverse?.reverse ?? reverse;

      return _Expr(_expr.cumCount(reverse?.reverse ?? reverse));
    },
    cumMax(reverse: any = false) {
      reverse = reverse?.reverse ?? reverse;

      return _Expr(_expr.cumMax(reverse));
    },
    cumMin(reverse: any = false) {
      reverse = reverse?.reverse ?? reverse;

      return _Expr(_expr.cumMin(reverse));
    },
    cumProd(reverse: any = false) {
      reverse = reverse?.reverse ?? reverse;

      return _Expr(_expr.cumProd(reverse));
    },
    cumSum(reverse: any = false) {
      reverse = reverse?.reverse ?? reverse;

      return _Expr(_expr.cumSum(reverse));
    },
    diff(n, nullBehavior = "ignore") {
      if (typeof n === "number") {
        return _Expr(_expr.diff(exprToLitOrExpr(n, false), nullBehavior));
      }
      return _Expr(_expr.diff(exprToLitOrExpr(n.n, false), n.nullBehavior));
    },
    dot(other) {
      const expr = (exprToLitOrExpr(other, false) as any).inner();

      return _Expr(_expr.dot(expr));
    },
    ewmMean(
      opts: {
        alpha?: number;
        adjust?: boolean;
        minPeriods?: number;
        bias?: boolean;
        ignoreNulls?: boolean;
      },
      adjust?: boolean,
      minPeriods?: number,
      bias?: boolean,
      ignoreNulls?: boolean,
    ) {
      if (opts) {
        if (typeof opts === "number") {
          return wrap(
            "ewmMean",
            opts,
            adjust ?? true,
            minPeriods ?? 1,
            bias ?? false,
            ignoreNulls ?? true,
          );
        }
        return wrap(
          "ewmMean",
          opts.alpha ?? 0.5,
          opts.adjust ?? true,
          opts.minPeriods ?? 1,
          opts.bias ?? false,
          opts.ignoreNulls ?? true,
        );
      }
      return wrap("ewmMean", 0.5, true, 1, false, true);
    },
    ewmStd(
      opts: {
        alpha?: number;
        adjust?: boolean;
        minPeriods?: number;
        bias?: boolean;
        ignoreNulls?: boolean;
      },
      adjust?: boolean,
      minPeriods?: number,
      bias?: boolean,
      ignoreNulls?: boolean,
    ) {
      if (opts) {
        if (typeof opts === "number") {
          return wrap(
            "ewmStd",
            opts,
            adjust ?? true,
            minPeriods ?? 1,
            bias ?? false,
            ignoreNulls ?? true,
          );
        }
        return wrap(
          "ewmStd",
          opts.alpha ?? 0.5,
          opts.adjust ?? true,
          opts.minPeriods ?? 1,
          opts.bias ?? false,
          opts.ignoreNulls ?? true,
        );
      }
      return wrap("ewmStd", 0.5, true, 1, false, true);
    },
    ewmVar(
      opts: {
        alpha?: number;
        adjust?: boolean;
        minPeriods?: number;
        bias?: boolean;
        ignoreNulls?: boolean;
      },
      adjust?: boolean,
      minPeriods?: number,
      bias?: boolean,
      ignoreNulls?: boolean,
    ) {
      if (opts) {
        if (typeof opts === "number") {
          return wrap(
            "ewmVar",
            opts,
            adjust ?? true,
            minPeriods ?? 1,
            bias ?? false,
            ignoreNulls ?? true,
          );
        }
        return wrap(
          "ewmVar",
          opts.alpha ?? 0.5,
          opts.adjust ?? true,
          opts.minPeriods ?? 1,
          opts.bias ?? false,
          opts.ignoreNulls ?? true,
        );
      }
      return wrap("ewmVar", 0.5, true, 1, false, true);
    },
    exclude(...columns) {
      return _Expr(_expr.exclude(columns.flat(2)));
    },
    explode() {
      return _Expr(_expr.explode());
    },
    exp() {
      return _Expr(_expr.exp());
    },
    extendConstant(o, n?) {
      if (n !== null && typeof n === "number") {
        return _Expr(_expr.extendConstant(o, n));
      }

      return _Expr(_expr.extendConstant(o.value, o.n));
    },
    fillNan(other) {
      const expr = (exprToLitOrExpr(other, true) as any).inner();

      return _Expr(_expr.fillNan(expr));
    },
    fillNull(fillValue) {
      if (
        ["backward", "forward", "mean", "min", "max", "zero", "one"].includes(
          fillValue,
        )
      ) {
        return _Expr(_expr.fillNullWithStrategy(fillValue));
      }

      const expr = exprToLitOrExpr(fillValue).inner();

      return _Expr(_expr.fillNull(expr));
    },
    filter(predicate) {
      const expr = exprToLitOrExpr(predicate).inner();

      return _Expr(_expr.filter(expr));
    },
    first() {
      return _Expr(_expr.first());
    },
    flatten() {
      return _Expr(_expr.explode());
    },
    floor() {
      return _Expr(_expr.floor());
    },
    forwardFill() {
      return _Expr(_expr.forwardFill());
    },
    gather(indices) {
      if (Array.isArray(indices)) {
        indices = pli.lit(Series("", indices, pli.Int64).inner());
      } else {
        indices = indices.inner();
      }
      return wrap("gather", indices);
    },
    gatherEvery(n, offset = 0) {
      return _Expr(_expr.gatherEvery(n, offset));
    },
    hash(obj: any = 0, k1 = 1, k2 = 2, k3 = 3) {
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
    head(length) {
      if (typeof length === "number") {
        return wrap("head", length);
      }

      return wrap("head", length.length);
    },
    implode() {
      return _Expr(_expr.implode());
    },
    interpolate(method: InterpolationMethod = "linear") {
      return _Expr(_expr.interpolate(method));
    },
    isDuplicated() {
      return _Expr(_expr.isDuplicated());
    },
    isFinite() {
      return _Expr(_expr.isFinite());
    },
    isInfinite() {
      return _Expr(_expr.isInfinite());
    },
    isFirstDistinct() {
      return _Expr(_expr.isFirstDistinct());
    },
    isNan() {
      return _Expr(_expr.isNan());
    },
    isNotNan() {
      return _Expr(_expr.isNotNan());
    },
    isNotNull() {
      return _Expr(_expr.isNotNull());
    },
    isNull() {
      return _Expr(_expr.isNull());
    },
    isUnique() {
      return _Expr(_expr.isUnique());
    },
    isIn(other, nullsEquals = false) {
      if (Array.isArray(other)) {
        other = pli.lit(Series(other).inner());
      } else {
        other = exprToLitOrExpr(other, false).inner();
      }
      return wrap("isIn", other, nullsEquals);
    },
    keepName() {
      return _Expr(_expr.keepName());
    },
    kurtosis(obj?, bias = true) {
      const fisher = obj?.["fisher"] ?? (typeof obj === "boolean" ? obj : true);
      bias = obj?.["bias"] ?? bias;
      return _Expr(_expr.kurtosis(fisher, bias));
    },
    last() {
      return _Expr(_expr.last());
    },
    list() {
      return _Expr(_expr.list());
    },
    log1p() {
      console.log(_expr.log1p);
      return _Expr(_expr.log1p());
    },
    log(base?: number) {
      return _Expr(_expr.log(base ?? Math.E));
    },
    lowerBound() {
      return _Expr(_expr.lowerBound());
    },
    peakMax() {
      return _Expr(_expr.peakMax());
    },
    peakMin() {
      return _Expr(_expr.peakMin());
    },
    max() {
      return _Expr(_expr.max());
    },
    mean() {
      return _Expr(_expr.mean());
    },
    median() {
      return _Expr(_expr.median());
    },
    min() {
      return _Expr(_expr.min());
    },
    mode() {
      return _Expr(_expr.mode());
    },
    not() {
      return _Expr(_expr.not());
    },
    nUnique() {
      return _Expr(_expr.nUnique());
    },
    or(other) {
      const expr = exprToLitOrExpr(other).inner();

      return _Expr(_expr.or(expr));
    },
    over(...exprs) {
      const partitionBy = selectionToExprList(exprs, false);

      return wrap("over", partitionBy);
    },
    pow(exponent) {
      return _Expr(_expr.pow(exponent?.exponent ?? exponent));
    },
    prefix(prefix) {
      return _Expr(_expr.prefix(prefix));
    },
    quantile(quantile, interpolation: InterpolationMethod = "nearest") {
      if (Expr.isExpr(quantile)) {
        quantile = quantile._expr;
      } else {
        quantile = pli.lit(quantile);
      }

      return _Expr(_expr.quantile(quantile, interpolation));
    },
    rank(method: any = "average", descending = false) {
      return _Expr(
        _expr.rank(method?.method ?? method, method?.descending ?? descending),
      );
    },
    reinterpret(signed: any = true) {
      signed = signed?.signed ?? signed;

      return _Expr(_expr.reinterpret(signed));
    },
    repeatBy(expr) {
      const e = exprToLitOrExpr(expr, false)._expr;

      return _Expr(_expr.repeatBy(e));
    },
    replace(old, newValue) {
      let oldIn: any = old;
      let newIn = newValue;
      if (old && typeof old === "object" && !Array.isArray(old)) {
        oldIn = Object.keys(old["old"]);
        newIn = Object.values(old["old"]);
      }
      return _Expr(
        _expr.replace(
          exprToLitOrExpr(oldIn)._expr,
          exprToLitOrExpr(newIn)._expr,
        ),
      );
    },
    replaceStrict(old, newValue, defaultValue, returnDtype) {
      let oldIn: any = old;
      let newIn = newValue;
      let defIn = defaultValue;
      if (old && typeof old === "object" && !Array.isArray(old)) {
        oldIn = Object.keys(old["old"]);
        newIn = Object.values(old["old"]);
        defIn = old["default_"];
      }
      return _Expr(
        _expr.replaceStrict(
          exprToLitOrExpr(oldIn)._expr,
          exprToLitOrExpr(newIn)._expr,
          defIn ? exprToLitOrExpr(defIn)._expr : undefined,
          returnDtype,
        ),
      );
    },
    reverse() {
      return _Expr(_expr.reverse());
    },
    rollingMax: rolling("rollingMax"),
    rollingMean: rolling("rollingMean"),
    rollingMin: rolling("rollingMin"),
    rollingSum: rolling("rollingSum"),
    rollingStd: rolling("rollingStd"),
    rollingVar: rolling("rollingVar"),
    rollingMedian: rolling("rollingMedian"),
    rollingQuantile(
      val,
      interpolation?,
      windowSize?,
      weights?,
      minPeriods?,
      center?,
      by?,
      closedWindow?,
      warnIfUnsorted?,
    ) {
      if (typeof val === "number") {
        return wrap("rollingQuantile", {
          windowSize: `${windowSize}i`,
          weights,
          minPeriods,
          center,
        });
      }
      windowSize =
        val?.["windowSize"] ?? (typeof val === "number" ? val : null);
      if (windowSize === null) {
        throw new Error("window size is required");
      }
      return wrap(
        "rollingQuantile",
        val.quantile,
        val.interpolation ?? "nearest",
        windowSize,
        val?.["weights"] ?? weights ?? null,
        val?.["minPeriods"] ?? minPeriods ?? windowSize,
        val?.["center"] ?? center ?? false,
        val?.["by"] ?? by,
        closedWindow,
        val?.["warnIfUnsorted"] ?? warnIfUnsorted ?? true,
      );
    },
    rollingSkew(val, bias = true) {
      if (typeof val === "number") {
        return wrap("rollingSkew", val, bias);
      }

      return wrap("rollingSkew", val.windowSize, val.bias ?? bias);
    },
    round(decimals) {
      return _Expr(_expr.round(decimals?.decimals ?? decimals, "halftoeven"));
    },
    sample(opts?, frac?, withReplacement = false, seed?) {
      if (opts?.n !== undefined || opts?.frac !== undefined) {
        return (this as any).sample(
          opts.n,
          opts.frac,
          opts.withReplacement,
          seed,
        );
      }
      if (typeof opts === "number") {
        throw new Error("sample_n is not yet supported for expr");
      }
      if (typeof frac === "number") {
        return wrap("sampleFrac", frac, withReplacement, false, seed);
      }
      throw new TypeError("must specify either 'frac' or 'n'");
    },
    shift(periods) {
      return _Expr(_expr.shift(exprToLitOrExpr(periods)._expr));
    },
    shiftAndFill(optOrPeriods, fillValue?) {
      if (typeof optOrPeriods === "number") {
        return wrap("shiftAndFill", optOrPeriods, fillValue);
      }
      return wrap("shiftAndFill", optOrPeriods.periods, optOrPeriods.fillValue);
    },
    skew(bias) {
      return wrap("skew", bias?.bias ?? bias ?? true);
    },
    sin() {
      return _Expr(_expr.sin());
    },
    sinh() {
      return _Expr(_expr.sinh());
    },
    slice(arg, len?) {
      if (typeof arg === "number") {
        return wrap("slice", pli.lit(arg), pli.lit(len));
      }

      return wrap("slice", pli.lit(arg.offset), pli.lit(arg.length));
    },
    sort(descending: any = false, nullsLast = false, maintainOrder = false) {
      if (typeof descending === "boolean") {
        return wrap("sortWith", descending, nullsLast, false, maintainOrder);
      }

      return wrap(
        "sortWith",
        descending?.descending ?? descending?.reverse ?? false,
        descending?.nullsLast ?? nullsLast,
        false,
        descending?.maintainOrder ?? maintainOrder,
      );
    },
    sortBy(arg, descending = false) {
      if (arg?.by !== undefined) {
        return this.sortBy(arg.by, arg.descending ?? arg.reverse ?? false);
      }

      descending = Array.isArray(descending)
        ? descending.flat()
        : ([descending] as any);
      const by = selectionToExprList(arg, false);

      return wrap("sortBy", by, descending);
    },
    std() {
      return _Expr(_expr.std());
    },
    suffix(suffix) {
      return _Expr(_expr.suffix(suffix));
    },
    sum() {
      return _Expr(_expr.sum());
    },
    tail(length) {
      return _Expr(_expr.tail(length));
    },
    tan() {
      return _Expr(_expr.tan());
    },
    tanh() {
      return _Expr(_expr.tanh());
    },
    unique(opt?) {
      if (opt || opt?.maintainOrder) {
        return wrap("uniqueStable");
      }
      return wrap("unique");
    },
    upperBound() {
      return _Expr(_expr.upperBound());
    },
    where(expr) {
      return this.filter(expr);
    },
    var() {
      return _Expr(_expr.var());
    },
    add: wrapExprArg("add"),
    sub: wrapExprArg("sub"),
    div: wrapExprArg("div"),
    mul: wrapExprArg("mul"),
    rem: wrapExprArg("rem"),

    plus: wrapExprArg("add"),
    minus: wrapExprArg("sub"),
    divideBy: wrapExprArg("div"),
    multiplyBy: wrapExprArg("mul"),
    modulo: wrapExprArg("rem"),

    eq: wrapExprArg("eq"),
    equals: wrapExprArg("eq"),
    gtEq: wrapExprArg("gtEq"),
    greaterThanEquals: wrapExprArg("gtEq"),
    gt: wrapExprArg("gt"),
    greaterThan: wrapExprArg("gt"),
    ltEq: wrapExprArg("ltEq"),
    lessThanEquals: wrapExprArg("ltEq"),
    lt: wrapExprArg("lt"),
    lessThan: wrapExprArg("lt"),
    neq: wrapExprArg("neq"),
    notEquals: wrapExprArg("neq"),
  } as Expr;
};

export interface ExprConstructor extends Deserialize<Expr> {
  isExpr(arg: any): arg is Expr;
}

const isExpr = (anyVal: any): anyVal is Expr => {
  try {
    return anyVal?.[Symbol.toStringTag]?.() === "Expr";
  } catch (err) {
    return false;
  }
};

const deserialize = (buf, format) => {
  return _Expr(pli.JsExpr.deserialize(buf, format));
};

export const Expr: ExprConstructor = Object.assign(_Expr, {
  isExpr,
  deserialize,
});

export const exprToLitOrExpr = (expr: any, stringToLit = true): Expr => {
  if (isRegExp(expr)) {
    return _Expr(pli.lit(regexToString(expr)));
  }
  if (typeof expr === "string" && !stringToLit) {
    return _Expr(pli.col(expr));
  }
  if (Expr.isExpr(expr)) {
    return expr;
  }
  if (Series.isSeries(expr)) {
    return _Expr(pli.lit((expr as any)._s));
  }
  return _Expr(pli.lit(expr));
};
