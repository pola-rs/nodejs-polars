import { Stream, Writable } from "node:stream";
import { DataType, type DTypeToJs, type JsToDtype } from "./datatypes";
import { concat } from "./functions";
import {
  _GroupBy,
  DynamicGroupBy,
  type GroupBy,
  RollingGroupBy,
} from "./groupby";
import { escapeHTML } from "./html";
import { arrayToJsDataFrame } from "./internals/construction";
import pli from "./internals/polars_internal";
import { _LazyDataFrame, type LazyDataFrame } from "./lazy/dataframe";
import { Expr } from "./lazy/expr";
import { col, element } from "./lazy/functions";
import { _Series, Series } from "./series";

import type {
  Arithmetic,
  Deserialize,
  GroupByOps,
  Sample,
  Serialize,
} from "./shared_traits";
import type {
  CrossJoinOptions,
  CsvWriterOptions,
  DifferentNameColumnJoinOptions,
  FillNullStrategy,
  JoinOptions,
  SameNameColumnJoinOptions,
  WriteAvroOptions,
  WriteIPCOptions,
  WriteParquetOptions,
} from "./types";
import {
  type ColumnSelection,
  type ColumnsOrExpr,
  columnOrColumns,
  columnOrColumnsStrict,
  type ExprOrString,
  isSeriesArray,
  type Simplify,
} from "./utils";

const inspect = Symbol.for("nodejs.util.inspect.custom");
const jupyterDisplay = Symbol.for("Jupyter.display");

export const writeCsvDefaultOptions: Partial<CsvWriterOptions> = {
  includeBom: false,
  includeHeader: true,
  separator: ",",
  quoteChar: '"',
  lineTerminator: "\n",
  batchSize: 1024,
  maintainOrder: true,
};

/**
 * Write methods for DataFrame
 */
interface WriteMethods {
  /**
   * __Write DataFrame to comma-separated values file (csv).__
   *
   * If no options are specified, it will return a new string containing the contents
   * ___
   * @param dest file or stream to write to
   * @param options.includeBom - Whether to include UTF-8 BOM in the CSV output.
   * @param options.lineTerminator - String used to end each row.
   * @param options.includeHeader - Whether or not to include header in the CSV output.
   * @param options.separator - Separate CSV fields with this symbol. Defaults: `,`
   * @param options.quoteChar - Character to use for quoting. Default: '"'
   * @param options.batchSize - Number of rows that will be processed per thread.
   * @param options.datetimeFormat - A format string, with the specifiers defined by the
   *    `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
   *   Rust crate. If no format specified, the default fractional-second
   *   precision is inferred from the maximum timeunit found in the frame's
   *   Datetime cols (if any).
   * @param options.dateFormat - A format string, with the specifiers defined by the
   *    `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
   *   Rust crate.
   * @param options.timeFormat A format string, with the specifiers defined by the
   *    `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
   *   Rust crate.
   * @param options.floatPrecision - Number of decimal places to write, applied to both `Float32` and `Float64` datatypes.
   * @param options.nullValue - A string representing null values (defaulting to the empty string).
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.writeCSV();
   * foo,bar,ham
   * 1,6,a
   * 2,7,b
   * 3,8,c
   *
   * // using a file path
   * > df.head(1).writeCSV("./foo.csv")
   * // foo.csv
   * foo,bar,ham
   * 1,6,a
   *
   * // using a write stream
   * > const writeStream = new Stream.Writable({
   * ...   write(chunk, encoding, callback) {
   * ...     console.log("writeStream: %O', chunk.toString());
   * ...     callback(null);
   * ...   }
   * ... });
   * > df.head(1).writeCSV(writeStream, {includeHeader: false});
   * writeStream: '1,6,a'
   * ```
   * @category IO
   */
  writeCSV(dest: string | Writable, options?: CsvWriterOptions): void;
  writeCSV(): Buffer;
  writeCSV(options: CsvWriterOptions): Buffer;
  /**
   * Write Dataframe to JSON string, file, or write stream
   * @param destination file or write stream
   * @param options.format - json | lines
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   foo: [1,2,3],
   * ...   bar: ['a','b','c']
   * ... })
   *
   * > df.writeJSON({format:"json"})
   * `[ {"foo":1.0,"bar":"a"}, {"foo":2.0,"bar":"b"}, {"foo":3.0,"bar":"c"}]`
   *
   * > df.writeJSON({format:"lines"})
   * `{"foo":1.0,"bar":"a"}
   * {"foo":2.0,"bar":"b"}
   * {"foo":3.0,"bar":"c"}`
   *
   * // writing to a file
   * > df.writeJSON("/path/to/file.json", {format:'lines'})
   * ```
   * @category IO
   */
  writeJSON(
    destination: string | Writable,
    options?: { format: "lines" | "json" },
  ): void;
  writeJSON(options?: { format: "lines" | "json" }): Buffer;
  /**
   * Write to Arrow IPC feather file, either to a file path or to a write stream.
   * @param destination File path to which the file should be written, or writable.
   * @param options.compression Compression method *defaults to "uncompressed"*
   * @category IO
   */
  writeIPC(destination: string | Writable, options?: WriteIPCOptions): void;
  writeIPC(options?: WriteIPCOptions): Buffer;
  /**
   * Write to Arrow IPC stream file, either to a file path or to a write stream.
   * @param destination File path to which the file should be written, or writable.
   * @param options.compression Compression method *defaults to "uncompressed"*
   * @category IO
   */
  writeIPCStream(
    destination: string | Writable,
    options?: WriteIPCOptions,
  ): void;
  writeIPCStream(options?: WriteIPCOptions): Buffer;
  /**
   * Write the DataFrame disk in parquet format.
   * @param destination File path to which the file should be written, or writable.
   * @param options.compression Compression method *defaults to "uncompressed"*
   * @category IO
   */
  writeParquet(
    destination: string | Writable,
    options?: WriteParquetOptions,
  ): void;
  writeParquet(options?: WriteParquetOptions): Buffer;
  /**
   * Write the DataFrame disk in avro format.
   * @param destination File path to which the file should be written, or writable.
   * @param options.compression Compression method *defaults to "uncompressed"*
   * @category IO
   */
  writeAvro(destination: string | Writable, options?: WriteAvroOptions): void;
  writeAvro(options?: WriteAvroOptions): Buffer;
}

export type Schema = Record<string, DataType>;
type SchemaToSeriesRecord<T extends Record<string, DataType>> = {
  [K in keyof T]: K extends string ? Series<T[K], K> : never;
};
type ArrayLikeLooseRecordToSchema<T extends Record<string, ArrayLike<any>>> = {
  [K in keyof T]: K extends string | number
    ? T[K] extends ArrayLike<infer V>
      ? V extends DataType
        ? V
        : JsToDtype<V>
      : never
    : never;
};

type ExtractJoinKeys<T> = T extends string[] ? T[number] : T;
type ExtractSuffix<T extends JoinOptions> = T extends { suffix: infer Suffix }
  ? Suffix
  : "_right";
export type JoinSchemas<
  S1 extends Schema,
  S2 extends Schema,
  Opt extends JoinOptions,
> = Simplify<
  {
    [K1 in keyof S1]: S1[K1];
  } & {
    [K2 in Exclude<keyof S2, keyof S1>]: K2 extends keyof S1 ? never : S2[K2];
  } & {
    [K_SUFFIXED in keyof S1 &
      Exclude<
        keyof S2,
        Opt extends CrossJoinOptions
          ? never
          : Opt extends SameNameColumnJoinOptions
            ? ExtractJoinKeys<Opt["on"]>
            : Opt extends DifferentNameColumnJoinOptions
              ? ExtractJoinKeys<Opt["rightOn"]>
              : never
      > as `${K_SUFFIXED extends string ? K_SUFFIXED : never}${ExtractSuffix<Opt>}`]: K_SUFFIXED extends string
      ? S2[K_SUFFIXED]
      : never;
  }
>;

/**
 * A DataFrame is a two-dimensional data structure that represents data as a table
 * with rows and columns.
 *
 * @param data -  Object, Array, or Series
 *     Two-dimensional data in various forms. object must contain Arrays.
 *     Array may contain Series or other Arrays.
 * @param columns - Array of str, default undefined
 *     Column labels to use for resulting DataFrame. If specified, overrides any
 *     labels already present in the data. Must match data dimensions.
 * @param orient - 'col' | 'row' default undefined
 *     Whether to interpret two-dimensional data as columns or as rows. If None,
 *     the orientation is inferred by matching the columns and data dimensions. If
 *     this does not yield conclusive results, column orientation is used.
 * @example
 * Constructing a DataFrame from an object :
 * ```
 * > const data = {'a': [1n, 2n], 'b': [3, 4]};
 * > const df = pl.DataFrame(data);
 * > console.log(df.toString());
 * shape: (2, 2)
 * ╭─────┬─────╮
 * │ a   ┆ b   │
 * │ --- ┆ --- │
 * │ u64 ┆ i64 │
 * ╞═════╪═════╡
 * │ 1   ┆ 3   │
 * ├╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 2   ┆ 4   │
 * ╰─────┴─────╯
 * ```
 * Notice that the dtype is automatically inferred as a polars Int64:
 * ```
 * > df.dtypes
 * ['UInt64', `Int64']
 * ```
 * In order to specify dtypes for your columns, initialize the DataFrame with a list
 * of Series instead:
 * ```
 * > const data = [pl.Series('col1', [1, 2], pl.Float32), pl.Series('col2', [3, 4], pl.Int64)];
 * > const df2 = pl.DataFrame(series);
 * > console.log(df2.toString());
 * shape: (2, 2)
 * ╭──────┬──────╮
 * │ col1 ┆ col2 │
 * │ ---  ┆ ---  │
 * │ f32  ┆ i64  │
 * ╞══════╪══════╡
 * │ 1    ┆ 3    │
 * ├╌╌╌╌╌╌┼╌╌╌╌╌╌┤
 * │ 2    ┆ 4    │
 * ╰──────┴──────╯
 * ```
 *
 * Constructing a DataFrame from a list of lists, row orientation inferred:
 * ```
 * > const data = [[1, 2, 3], [4, 5, 6]];
 * > const df4 = pl.DataFrame(data, ['a', 'b', 'c']);
 * > console.log(df4.toString());
 * shape: (2, 3)
 * ╭─────┬─────┬─────╮
 * │ a   ┆ b   ┆ c   │
 * │ --- ┆ --- ┆ --- │
 * │ i64 ┆ i64 ┆ i64 │
 * ╞═════╪═════╪═════╡
 * │ 1   ┆ 2   ┆ 3   │
 * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 4   ┆ 5   ┆ 6   │
 * ╰─────┴─────┴─────╯
 * ```
 */
export interface DataFrame<S extends Schema = any>
  extends Arithmetic<DataFrame<S>>,
    Sample<DataFrame<S>>,
    WriteMethods,
    Serialize,
    GroupByOps<RollingGroupBy> {
  /** @ignore */
  _df: any;
  dtypes: DataType[];
  height: number;
  shape: { height: number; width: number };
  width: number;
  get columns(): string[];
  set columns(cols: string[]);
  [inspect](): string;
  [Symbol.iterator](): Generator<any, void, any>;
  /**
   * Very cheap deep clone.
   */
  clone(): DataFrame<S>;
  /**
   * __Summary statistics for a DataFrame.__
   *
   * Only summarizes numeric datatypes at the moment and returns nulls for non numeric datatypes.
   * ___
   * Example
   * ```
   * >  const df = pl.DataFrame({
   * ...      'a': [1.0, 2.8, 3.0],
   * ...      'b': [4, 5, 6],
   * ...      "c": [True, False, True]
   * ...      });
   * ... df.describe()
   * shape: (5, 4)
   * ╭──────────┬───────┬─────┬──────╮
   * │ describe ┆ a     ┆ b   ┆ c    │
   * │ ---      ┆ ---   ┆ --- ┆ ---  │
   * │ str      ┆ f64   ┆ f64 ┆ f64  │
   * ╞══════════╪═══════╪═════╪══════╡
   * │ "mean"   ┆ 2.267 ┆ 5   ┆ null │
   * ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ "std"    ┆ 1.102 ┆ 1   ┆ null │
   * ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ "min"    ┆ 1     ┆ 4   ┆ 0.0  │
   * ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ "max"    ┆ 3     ┆ 6   ┆ 1    │
   * ├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ "median" ┆ 2.8   ┆ 5   ┆ null │
   * ╰──────────┴───────┴─────┴──────╯
   * ```
   */
  describe(): DataFrame;
  /**
   * __Remove column from DataFrame and return as new.__
   * ___
   * @param name
   * @example
   * ```
   * >  const df = pl.DataFrame({
   * ...    "foo": [1, 2, 3],
   * ...    "bar": [6.0, 7.0, 8.0],
   * ...    "ham": ['a', 'b', 'c'],
   * ...    "apple": ['a', 'b', 'c']
   * ...  });
   *    // df: pl.DataFrame<{
   *    //     foo: pl.Series<Float64, "foo">;
   *    //     bar: pl.Series<Float64, "bar">;
   *    //     ham: pl.Series<Utf8, "ham">;
   *    //     apple: pl.Series<Utf8, "apple">;
   *    // }>
   * >  const df2 = df.drop(['ham', 'apple']);
   *    // df2: pl.DataFrame<{
   *    //     foo: pl.Series<Float64, "foo">;
   *    //     bar: pl.Series<Float64, "bar">;
   *    // }>
   * >  console.log(df2.toString());
   * shape: (3, 2)
   * ╭─────┬─────╮
   * │ foo ┆ bar │
   * │ --- ┆ --- │
   * │ i64 ┆ f64 │
   * ╞═════╪═════╡
   * │ 1   ┆ 6   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 2   ┆ 7   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 3   ┆ 8   │
   * ╰─────┴─────╯
   * ```
   */
  drop<U extends string>(name: U): DataFrame<Simplify<Omit<S, U>>>;
  drop<const U extends string[]>(
    names: U,
  ): DataFrame<Simplify<Omit<S, U[number]>>>;
  drop<U extends string, const V extends string[]>(
    name: U,
    ...names: V
  ): DataFrame<Simplify<Omit<S, U | V[number]>>>;
  /**
   * __Return a new DataFrame where the null values are dropped.__
   *
   * This method only drops nulls row-wise if any single value of the row is null.
   * ___
   * @example
   * ```
   * >  const df = pl.DataFrame({
   * ...    "foo": [1, 2, 3],
   * ...    "bar": [6, null, 8],
   * ...    "ham": ['a', 'b', 'c']
   * ...  });
   * > console.log(df.dropNulls().toString());
   * shape: (2, 3)
   * ┌─────┬─────┬─────┐
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 6   ┆ "a" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 3   ┆ 8   ┆ "c" │
   * └─────┴─────┴─────┘
   * ```
   */
  dropNulls(column: keyof S): DataFrame<S>;
  dropNulls(columns: (keyof S)[]): DataFrame<S>;
  dropNulls(...columns: (keyof S)[]): DataFrame<S>;
  /**
   * __Explode `DataFrame` to long format by exploding a column with Lists.__
   * ___
   * @param columns - column or columns to explode
   * @example
   * ```
   * >  const df = pl.DataFrame({
   * ...    "letters": ["c", "c", "a", "c", "a", "b"],
   * ...    "nrs": [[1, 2], [1, 3], [4, 3], [5, 5, 5], [6], [2, 1, 2]]
   * ...  });
   * >  console.log(df.toString());
   * shape: (6, 2)
   * ╭─────────┬────────────╮
   * │ letters ┆ nrs        │
   * │ ---     ┆ ---        │
   * │ str     ┆ list [i64] │
   * ╞═════════╪════════════╡
   * │ "c"     ┆ [1, 2]     │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "c"     ┆ [1, 3]     │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "a"     ┆ [4, 3]     │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "c"     ┆ [5, 5, 5]  │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "a"     ┆ [6]        │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ "b"     ┆ [2, 1, 2]  │
   * ╰─────────┴────────────╯
   * >  df.explode("nrs")
   * shape: (13, 2)
   * ╭─────────┬─────╮
   * │ letters ┆ nrs │
   * │ ---     ┆ --- │
   * │ str     ┆ i64 │
   * ╞═════════╪═════╡
   * │ "c"     ┆ 1   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 2   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 1   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 3   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ ...     ┆ ... │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 5   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "a"     ┆ 6   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "b"     ┆ 2   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "b"     ┆ 1   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "b"     ┆ 2   │
   * ╰─────────┴─────╯
   * ```
   */
  explode(columns: ExprOrString[]): DataFrame;
  explode(column: ExprOrString): DataFrame;
  explode(column: ExprOrString, ...columns: ExprOrString[]): DataFrame;
  /**
   *
   *
   * __Extend the memory backed by this `DataFrame` with the values from `other`.__
   * ___

    Different from `vstack` which adds the chunks from `other` to the chunks of this `DataFrame`
    `extent` appends the data from `other` to the underlying memory locations and thus may cause a reallocation.

    If this does not cause a reallocation, the resulting data structure will not have any extra chunks
    and thus will yield faster queries.

    Prefer `extend` over `vstack` when you want to do a query after a single append. For instance during
    online operations where you add `n` rows and rerun a query.

    Prefer `vstack` over `extend` when you want to append many times before doing a query. For instance
    when you read in multiple files and when to store them in a single `DataFrame`.
    In the latter case, finish the sequence of `vstack` operations with a `rechunk`.

   * @param other DataFrame to vertically add.
   */
  extend(other: DataFrame<S>): DataFrame<S>;
  /**
   * Fill null/missing values by a filling strategy
   *
   * @param strategy - One of:
   *   - "backward"
   *   - "forward"
   *   - "mean"
   *   - "min'
   *   - "max"
   *   - "zero"
   *   - "one"
   * @returns DataFrame with None replaced with the filling strategy.
   */
  fillNull(strategy: FillNullStrategy): DataFrame<S>;
  /**
   * Filter the rows in the DataFrame based on a predicate expression.
   * ___
   * @param predicate - Expression that evaluates to a boolean Series.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * // Filter on one condition
   * > df.filter(pl.col("foo").lt(3))
   * shape: (2, 3)
   * ┌─────┬─────┬─────┐
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 6   ┆ a   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 2   ┆ 7   ┆ b   │
   * └─────┴─────┴─────┘
   * // Filter on multiple conditions
   * > df.filter(
   * ... pl.col("foo").lt(3)
   * ...   .and(pl.col("ham").eq(pl.lit("a")))
   * ... )
   * shape: (1, 3)
   * ┌─────┬─────┬─────┐
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 6   ┆ a   │
   * └─────┴─────┴─────┘
   * ```
   */
  filter(predicate: any): DataFrame<S>;
  /**
   * Find the index of a column by name.
   * ___
   * @param name -Name of the column to find.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.findIdxByName("ham"))
   * 2
   * ```
   */
  findIdxByName(name: keyof S): number;
  /**
   * __Apply a horizontal reduction on a DataFrame.__
   *
   * This can be used to effectively determine aggregations on a row level,
   * and can be applied to any DataType that can be supercasted (casted to a similar parent type).
   *
   * An example of the supercast rules when applying an arithmetic operation on two DataTypes are for instance:
   *  - Int8 + Utf8 = Utf8
   *  - Float32 + Int64 = Float32
   *  - Float32 + Float64 = Float64
   * ___
   * @param operation - function that takes two `Series` and returns a `Series`.
   * @returns Series
   * @example
   * ```
   * > // A horizontal sum operation
   * > let df = pl.DataFrame({
   * ...   "a": [2, 1, 3],
   * ...   "b": [1, 2, 3],
   * ...   "c": [1.0, 2.0, 3.0]
   * ... });
   * > df.fold((s1, s2) => s1.plus(s2))
   * Series: 'a' [f64]
   * [
   *     4
   *     5
   *     9
   * ]
   * > // A horizontal minimum operation
   * > df = pl.DataFrame({
   * ...   "a": [2, 1, 3],
   * ...   "b": [1, 2, 3],
   * ...   "c": [1.0, 2.0, 3.0]
   * ... });
   * > df.fold((s1, s2) => s1.zipWith(s1.lt(s2), s2))
   * Series: 'a' [f64]
   * [
   *     1
   *     1
   *     3
   * ]
   * > // A horizontal string concatenation
   * > df = pl.DataFrame({
   * ...   "a": ["foo", "bar", 2],
   * ...   "b": [1, 2, 3],
   * ...   "c": [1.0, 2.0, 3.0]
   * ... })
   * > df.fold((s1, s2) => s.plus(s2))
   * Series: '' [f64]
   * [
   *     "foo11"
   *     "bar22
   *     "233"
   * ]
   * ```
   */
  fold<
    D extends DataType,
    F extends (
      s1: SchemaToSeriesRecord<S>[keyof S] | Series<D>,
      s2: SchemaToSeriesRecord<S>[keyof S],
    ) => Series<D>,
  >(operation: F): Series<D>;
  /**
   * Check if DataFrame is equal to other.
   * ___
   * @param other DataFrame to compare.
   * @param nullEqual Consider null values as equal.
   * @example
   * ```
   * > const df1 = pl.DataFrame({
   * ...    "foo": [1, 2, 3],
   * ...    "bar": [6.0, 7.0, 8.0],
   * ...    "ham": ['a', 'b', 'c']
   * ... })
   * > const df2 = pl.DataFrame({
   * ...   "foo": [3, 2, 1],
   * ...   "bar": [8.0, 7.0, 6.0],
   * ...   "ham": ['c', 'b', 'a']
   * ... })
   * > df1.frameEqual(df1)
   * true
   * > df1.frameEqual(df2)
   * false
   * ```
   */
  frameEqual(other: DataFrame, nullEqual: boolean): boolean;
  frameEqual(other: DataFrame): boolean;
  /**
   * Get a single column as Series by name.
   *
   * ---
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...     foo: [1, 2, 3],
   * ...     bar: [6, null, 8],
   * ...     ham: ["a", "b", "c"],
   * ... });
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * > const column = df.getColumn("foo");
   * // column: pl.Series<Float64, "foo">
   * ```
   */
  getColumn<U extends keyof S>(name: U): SchemaToSeriesRecord<S>[U];
  getColumn(name: string): Series;
  /**
   * Get the DataFrame as an Array of Series.
   * ---
   * @example
   * ```
   * >  const df = pl.DataFrame({
   * ...     foo: [1, 2, 3],
   * ...     bar: [6, null, 8],
   * ...     ham: ["a", "b", "c"],
   * ... });
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * > const columns = df.getColumns();
   * // columns: (pl.Series<Float64, "foo"> | pl.Series<Float64, "bar"> | pl.Series<Utf8, "ham">)[]
   * ```
   */
  getColumns(): SchemaToSeriesRecord<S>[keyof S][];
  /**
   * Start a groupby operation.
   * ___
   * @param by - Column(s) to group by.
   */
  groupBy(...by: ColumnSelection[]): GroupBy;
  /**
   * Hash and combine the rows in this DataFrame. _(Hash value is UInt64)_
   * @param k0 - seed parameter
   * @param k1 - seed parameter
   * @param k2 - seed parameter
   * @param k3 - seed parameter
   */
  hashRows(k0?: number, k1?: number, k2?: number, k3?: number): Series;
  hashRows(options: {
    k0?: number;
    k1?: number;
    k2?: number;
    k3?: number;
  }): Series;
  /**
   * Get first N rows as DataFrame.
   * ___
   * @param length -  Length of the head.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3, 4, 5],
   * ...   "bar": [6, 7, 8, 9, 10],
   * ...   "ham": ['a', 'b', 'c', 'd','e']
   * ... });
   * > df.head(3)
   * shape: (3, 3)
   * ╭─────┬─────┬─────╮
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 6   ┆ "a" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 2   ┆ 7   ┆ "b" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 3   ┆ 8   ┆ "c" │
   * ╰─────┴─────┴─────╯
   * ```
   */
  head(length?: number): DataFrame<S>;
  /**
   * Return a new DataFrame grown horizontally by stacking multiple Series to it.
   * @param columns - array of Series or DataFrame to stack
   * @param inPlace - Modify in place
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * > const x = pl.Series("apple", [10, 20, 30])
   * // x: pl.Series<Float64, "apple">
   * > df.hstack([x])
   * // pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * //     apple: pl.Series<Float64, "apple">;
   * // }>
   * shape: (3, 4)
   * ╭─────┬─────┬─────┬───────╮
   * │ foo ┆ bar ┆ ham ┆ apple │
   * │ --- ┆ --- ┆ --- ┆ ---   │
   * │ i64 ┆ i64 ┆ str ┆ i64   │
   * ╞═════╪═════╪═════╪═══════╡
   * │ 1   ┆ 6   ┆ "a" ┆ 10    │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ 2   ┆ 7   ┆ "b" ┆ 20    │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ 3   ┆ 8   ┆ "c" ┆ 30    │
   * ╰─────┴─────┴─────┴───────╯
   * ```
   */
  hstack(columns: Array<Series> | DataFrame, inPlace?: boolean): void;
  hstack<S2 extends Schema = Schema>(
    columns: DataFrame<S2>,
  ): DataFrame<Simplify<S & S2>>;
  hstack<U extends Series[]>(
    columns: U,
  ): DataFrame<Simplify<S & { [K in U[number] as K["name"]]: K }>>;
  hstack(columns: Array<Series> | DataFrame, inPlace?: boolean): void;
  /**
   * Insert a Series at a certain column index. This operation is in place.
   * @param index - Column position to insert the new `Series` column.
   * @param series - `Series` to insert
   */
  insertAtIdx(index: number, series: Series): void;
  /**
   * Interpolate intermediate values. The interpolation method is linear.
   */
  interpolate(): DataFrame<S>;
  /**
   * Get a mask of all duplicated rows in this DataFrame.
   */
  isDuplicated(): Series;
  /**
   * Check if the dataframe is empty
   */
  isEmpty(): boolean;
  /**
   * Get a mask of all unique rows in this DataFrame.
   */
  isUnique(): Series;
  /**
   *  __SQL like joins.__
   * @param other - DataFrame to join with.
   * @param options
   * @param options.on - Name(s) of the join columns in both DataFrames.
   * @param options.how - Join strategy {'inner', 'left', 'right', 'full', 'semi', 'anti', 'cross'}
   * @param options.suffix - Suffix to append to columns with a duplicate name.
   * @param options.coalesce - Coalescing behavior (merging of join columns). default: undefined
            * - **undefined** - *(Default)* Coalesce unless `how='full'` is specified.
            * - **true**      - Always coalesce join columns.
            * - **false**     - Never coalesce join columns.
   * @param options.validate - Checks if join is of specified type. default: m:m 
            valid options: {'m:m', 'm:1', '1:m', '1:1'}
            * - **m:m** - *(Default)* Many-to-many (default). Does not result in checks.
            * - **1:1** - One-to-one. Checks if join keys are unique in both left and right datasets.
            * - **1:m** - One-to-many. Checks if join keys are unique in left dataset.
            * - **m:1** - Many-to-one. Check if join keys are unique in right dataset.
   * @see {@link SameNameColumnJoinOptions}
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6.0, 7.0, 8.0],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > const otherDF = pl.DataFrame({
   * ...   "apple": ['x', 'y', 'z'],
   * ...   "ham": ['a', 'b', 'd']
   * ... });
   * > df.join(otherDF, {on: 'ham'})
   * shape: (2, 4)
   * ╭─────┬─────┬─────┬───────╮
   * │ foo ┆ bar ┆ ham ┆ apple │
   * │ --- ┆ --- ┆ --- ┆ ---   │
   * │ i64 ┆ f64 ┆ str ┆ str   │
   * ╞═════╪═════╪═════╪═══════╡
   * │ 1   ┆ 6   ┆ "a" ┆ "x"   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ 2   ┆ 7   ┆ "b" ┆ "y"   │
   * ╰─────┴─────┴─────┴───────╯
   * ```
   */
  join<
    S2 extends Schema,
    const Opts extends SameNameColumnJoinOptions<
      Extract<keyof S, string>,
      Extract<keyof S2, string>
    >,
  >(
    other: DataFrame<S2>,
    // the right & part is only used for typedoc to understend which fields are used
    options: Opts & SameNameColumnJoinOptions,
  ): DataFrame<JoinSchemas<S, S2, Opts>>;
  /**
   *  __SQL like joins with different names for left and right dataframes.__
   * @param other - DataFrame to join with.
   * @param options
   * @param options.leftOn - Name(s) of the left join column(s).
   * @param options.rightOn - Name(s) of the right join column(s).
   * @param options.how - Join strategy
   * @param options.suffix - Suffix to append to columns with a duplicate name.
   * @param options.coalesce - Coalescing behavior (merging of join columns).
   * @see {@link DifferentNameColumnJoinOptions}
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6.0, 7.0, 8.0],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > const otherDF = pl.DataFrame({
   * ...   "apple": ['x', 'y', 'z'],
   * ...   "ham": ['a', 'b', 'd']
   * ... });
   * > df.join(otherDF, {leftOn: 'ham', rightOn: 'ham'})
   * shape: (2, 4)
   * ╭─────┬─────┬─────┬───────╮
   * │ foo ┆ bar ┆ ham ┆ apple │
   * │ --- ┆ --- ┆ --- ┆ ---   │
   * │ i64 ┆ f64 ┆ str ┆ str   │
   * ╞═════╪═════╪═════╪═══════╡
   * │ 1   ┆ 6   ┆ "a" ┆ "x"   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ 2   ┆ 7   ┆ "b" ┆ "y"   │
   * ╰─────┴─────┴─────┴───────╯
   * ```
   */
  join<
    S2 extends Schema,
    const Opts extends DifferentNameColumnJoinOptions<
      Extract<keyof S, string>,
      Extract<keyof S2, string>
    >,
  >(
    other: DataFrame<S2>,
    // the right & part is only used for typedoc to understend which fields are used
    options: Opts & DifferentNameColumnJoinOptions,
  ): DataFrame<JoinSchemas<S, S2, Opts>>;
  /**
   *  __SQL like cross joins.__
   * @param other - DataFrame to join with.
   * @param options
   * @param options.how - Join strategy
   * @param options.suffix - Suffix to append to columns with a duplicate name.
   * @param options.coalesce - Coalescing behavior (merging of join columns).
   * @see {@link CrossJoinOptions}
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2],
   * ...   "bar": [6.0, 7.0],
   * ...   "ham": ['a', 'b']
   * ... });
   * > const otherDF = pl.DataFrame({
   * ...   "apple": ['x', 'y'],
   * ...   "ham": ['a', 'b']
   * ... });
   * > df.join(otherDF, {how: 'cross'})
   * shape: (4, 5)
   * ╭─────┬─────┬─────┬───────┬───────────╮
   * │ foo ┆ bar ┆ ham ┆ apple ┆ ham_right │
   * │ --- ┆ --- ┆ --- ┆ ---   ┆ ---       │
   * │ f64 ┆ f64 ┆ str ┆ str   ┆ str       │
   * ╞═════╪═════╪═════╪═══════╪═══════════╡
   * │ 1.0 ┆ 6.0 ┆ a   ┆ x     ┆ a         │
   * │ 1.0 ┆ 6.0 ┆ a   ┆ y     ┆ b         │
   * │ 2.0 ┆ 7.0 ┆ b   ┆ x     ┆ a         │
   * │ 2.0 ┆ 7.0 ┆ b   ┆ y     ┆ b         │
   * ╰─────┴─────┴─────┴───────┴───────────╯
   * ```
   */
  join<S2 extends Schema, const Opts extends CrossJoinOptions>(
    other: DataFrame<S2>,
    // the right & part is only used for typedoc to understend which fields are used
    options: Opts & CrossJoinOptions,
  ): DataFrame<JoinSchemas<S, S2, Opts>>;

  /**
   * Perform an asof join. This is similar to a left-join except that we
   * match on nearest key rather than equal keys.
   *
   * Both DataFrames must be sorted by the asofJoin key.
   *
   * For each row in the left DataFrame:
   * - A "backward" search selects the last row in the right DataFrame whose
   *   'on' key is less than or equal to the left's key.
   *
   *  - A "forward" search selects the first row in the right DataFrame whose
   *    'on' key is greater than or equal to the left's key.
   *
   *  - A "nearest" search selects the last row in the right DataFrame whose value
   *    is nearest to the left's key. String keys are not currently supported for a
   *    nearest search.
   *
   * The default is "backward".
   *
   * @param other DataFrame to join with.
   * @param options.leftOn Join column of the left DataFrame.
   * @param options.rightOn Join column of the right DataFrame.
   * @param options.on Join column of both DataFrames. If set, `leftOn` and `rightOn` should be undefined.
   * @param options.byLeft join on these columns before doing asof join
   * @param options.byRight join on these columns before doing asof join
   * @param options.strategy One of 'forward', 'backward', 'nearest'
   * @param options.suffix Suffix to append to columns with a duplicate name.
   * @param options.tolerance
   *   Numeric tolerance. By setting this the join will only be done if the near keys are within this distance.
   *   If an asof join is done on columns of dtype "Date", "Datetime" you
   *   use the following string language:
   *
   *   - 1ns   *(1 nanosecond)*
   *   - 1us   *(1 microsecond)*
   *   - 1ms   *(1 millisecond)*
   *   - 1s    *(1 second)*
   *   - 1m    *(1 minute)*
   *   - 1h    *(1 hour)*
   *   - 1d    *(1 day)*
   *   - 1w    *(1 week)*
   *   - 1mo   *(1 calendar month)*
   *   - 1y    *(1 calendar year)*
   *   - 1i    *(1 index count)*
   *
   * Or combine them:
   *   - "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
   * @param options.allowParallel Allow the physical plan to optionally evaluate the computation of both DataFrames up to the join in parallel.
   * @param options.forceParallel Force the physical plan to evaluate the computation of both DataFrames up to the join in parallel.
   * @param options.checkSortedness
   *    Check the sortedness of the asof keys. If the keys are not sorted Polars
   *    will error, or in case of 'by' argument raise a warning. This might become
   *    a hard error in the future.
   *
   * @example
   * ```
   * > const gdp = pl.DataFrame({
   * ...   date: [
   * ...     new Date('2016-01-01'),
   * ...     new Date('2017-01-01'),
   * ...     new Date('2018-01-01'),
   * ...     new Date('2019-01-01'),
   * ...   ],  // note record date: Jan 1st (sorted!)
   * ...   gdp: [4164, 4411, 4566, 4696],
   * ... })
   * > const population = pl.DataFrame({
   * ...   date: [
   * ...     new Date('2016-05-12'),
   * ...     new Date('2017-05-12'),
   * ...     new Date('2018-05-12'),
   * ...     new Date('2019-05-12'),
   * ...   ],  // note record date: May 12th (sorted!)
   * ...   "population": [82.19, 82.66, 83.12, 83.52],
   * ... })
   * > population.joinAsof(
   * ...   gdp,
   * ...   {leftOn:"date", rightOn:"date", strategy:"backward"}
   * ... )
   *   shape: (4, 3)
   *   ┌─────────────────────┬────────────┬──────┐
   *   │ date                ┆ population ┆ gdp  │
   *   │ ---                 ┆ ---        ┆ ---  │
   *   │ datetime[μs]        ┆ f64        ┆ i64  │
   *   ╞═════════════════════╪════════════╪══════╡
   *   │ 2016-05-12 00:00:00 ┆ 82.19      ┆ 4164 │
   *   ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   *   │ 2017-05-12 00:00:00 ┆ 82.66      ┆ 4411 │
   *   ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   *   │ 2018-05-12 00:00:00 ┆ 83.12      ┆ 4566 │
   *   ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   *   │ 2019-05-12 00:00:00 ┆ 83.52      ┆ 4696 │
   *   └─────────────────────┴────────────┴──────┘
   * ```
   */
  joinAsof(
    other: DataFrame,
    options: {
      leftOn?: string;
      rightOn?: string;
      on?: string;
      byLeft?: string | string[];
      byRight?: string | string[];
      by?: string | string[];
      strategy?: "backward" | "forward" | "nearest";
      suffix?: string;
      tolerance?: number | string;
      allowParallel?: boolean;
      forceParallel?: boolean;
      checkSortedness?: boolean;
    },
  ): DataFrame;
  lazy(): LazyDataFrame<S>;
  /**
   * Get first N rows as DataFrame.
   * @see {@link head}
   */
  limit(length?: number): DataFrame<S>;
  map<ReturnT>(
    // TODO: strong types for the mapping function
    func: (row: any[], i: number, arr: any[][]) => ReturnT,
  ): ReturnT[];

  /**
   * Aggregate the columns of this DataFrame to their maximum value.
   * ___
   * @param axis - either 0 or 1
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.max()
   * shape: (1, 3)
   * ╭─────┬─────┬──────╮
   * │ foo ┆ bar ┆ ham  │
   * │ --- ┆ --- ┆ ---  │
   * │ i64 ┆ i64 ┆ str  │
   * ╞═════╪═════╪══════╡
   * │ 3   ┆ 8   ┆ null │
   * ╰─────┴─────┴──────╯
   * ```
   */
  max(axis: 0): DataFrame<S>;
  max(axis: 1): Series;
  max(): DataFrame<S>;
  /**
   * Aggregate the columns of this DataFrame to their mean value.
   * ___
   *
   * @param axis - either 0 or 1
   * @param nullStrategy - this argument is only used if axis == 1
   */
  mean(axis: 1, nullStrategy?: "ignore" | "propagate"): Series;
  mean(): DataFrame<S>;
  mean(axis: 0): DataFrame<S>;
  mean(axis: 1): Series;
  /**
   * Aggregate the columns of this DataFrame to their median value.
   * ___
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.median();
   * shape: (1, 3)
   * ╭─────┬─────┬──────╮
   * │ foo ┆ bar ┆ ham  │
   * │ --- ┆ --- ┆ ---  │
   * │ f64 ┆ f64 ┆ str  │
   * ╞═════╪═════╪══════╡
   * │ 2   ┆ 7   ┆ null │
   * ╰─────┴─────┴──────╯
   * ```
   */
  median(): DataFrame<S>;
  /**
   * Unpivot a DataFrame from wide to long format.
   * ___
   *
   * @param idVars - Columns to use as identifier variables.
   * @param valueVars - Values to use as value variables.
   * @param options.variableName - Name to give to the `variable` column. Defaults to "variable"
   * @param options.valueName - Name to give to the `value` column. Defaults to "value"
   * @example
   * ```
   * > const df1 = pl.DataFrame({
   * ...   'id': [1],
   * ...   'asset_key_1': ['123'],
   * ...   'asset_key_2': ['456'],
   * ...   'asset_key_3': ['abc'],
   * ... });
   * > df1.unpivot('id', ['asset_key_1', 'asset_key_2', 'asset_key_3']);
   * shape: (3, 3)
   * ┌─────┬─────────────┬───────┐
   * │ id  ┆ variable    ┆ value │
   * │ --- ┆ ---         ┆ ---   │
   * │ f64 ┆ str         ┆ str   │
   * ╞═════╪═════════════╪═══════╡
   * │ 1   ┆ asset_key_1 ┆ 123   │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ 1   ┆ asset_key_2 ┆ 456   │
   * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
   * │ 1   ┆ asset_key_3 ┆ abc   │
   * └─────┴─────────────┴───────┘
   * ```
   */
  unpivot(
    idVars: ColumnSelection,
    valueVars: ColumnSelection,
    options?: {
      variableName?: string | null;
      valueName?: string | null;
    },
  ): DataFrame;
  /**
   * Aggregate the columns of this DataFrame to their minimum value.
   * ___
   * @param axis - either 0 or 1
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.min();
   * shape: (1, 3)
   * ╭─────┬─────┬──────╮
   * │ foo ┆ bar ┆ ham  │
   * │ --- ┆ --- ┆ ---  │
   * │ i64 ┆ i64 ┆ str  │
   * ╞═════╪═════╪══════╡
   * │ 1   ┆ 6   ┆ null │
   * ╰─────┴─────┴──────╯
   * ```
   */
  min(axis: 0): DataFrame<S>;
  min(axis: 1): Series;
  min(): DataFrame<S>;
  /**
   * Get number of chunks used by the ChunkedArrays of this DataFrame.
   */
  nChunks(): number;
  /**
   * Create a new DataFrame that shows the null counts per column.
   * ___
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, null, 3],
   * ...   "bar": [6, 7, null],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.nullCount();
   * shape: (1, 3)
   * ┌─────┬─────┬─────┐
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ u32 ┆ u32 ┆ u32 │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 1   ┆ 0   │
   * └─────┴─────┴─────┘
   * ```
   */
  nullCount(): DataFrame<{
    [K in keyof S]: JsToDtype<number>;
  }>;
  partitionBy(
    cols: string | string[],
    stable?: boolean,
    includeKey?: boolean,
  ): DataFrame<S>[];
  partitionBy<T>(
    cols: string | string[],
    stable: boolean,
    includeKey: boolean,
    mapFn: (df: DataFrame) => T,
  ): T[];
  /**
   *   Create a spreadsheet-style pivot table as a DataFrame.
   *
   *   @param values The existing column(s) of values which will be moved under the new columns from index. If an
   *                  aggregation is specified, these are the values on which the aggregation will be computed.
   *                  If None, all remaining columns not specified on `on` and `index` will be used.
   *                  At least one of `index` and `values` must be specified.
   *   @param options.index The column(s) that remain from the input to the output. The output DataFrame will have one row
   *                        for each unique combination of the `index`'s values.
   *                        If None, all remaining columns not specified on `on` and `values` will be used. At least one
   *                        of `index` and `values` must be specified.
   *   @param options.on The column(s) whose values will be used as the new columns of the output DataFrame.
   *   @param options.aggregateFunc
   *       Any of:
   *       - "sum"
   *       - "max"
   *       - "min"
   *       - "mean"
   *       - "median"
   *       - "first"
   *       - "last"
   *       - "count"
   *     Defaults to "first"
   *   @param options.maintainOrder Sort the grouped keys so that the output order is predictable.
   *   @param options.sortColumns Sort the transposed columns by name. Default is by order of discovery.
   *   @param options.separator Used as separator/delimiter in generated column names.
   *   @example
   * ```
   *   > const df = pl.DataFrame(
   *   ...     {
   *   ...         "foo": ["one", "one", "one", "two", "two", "two"],
   *   ...         "bar": ["A", "B", "C", "A", "B", "C"],
   *   ...         "baz": [1, 2, 3, 4, 5, 6],
   *   ...     }
   *   ... );
   *   > df.pivot("baz", {index:"foo", on:"bar"});
   *   shape: (2, 4)
   *   ┌─────┬─────┬─────┬─────┐
   *   │ foo ┆ A   ┆ B   ┆ C   │
   *   │ --- ┆ --- ┆ --- ┆ --- │
   *   │ str ┆ f64 ┆ f64 ┆ f64 │
   *   ╞═════╪═════╪═════╪═════╡
   *   │ one ┆ 1   ┆ 2   ┆ 3   │
   *   ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   *   │ two ┆ 4   ┆ 5   ┆ 6   │
   *   └─────┴─────┴─────┴─────┘
   *    ```
   */
  pivot(
    values: string | string[],
    options: {
      index: string | string[];
      on: string | string[];
      aggregateFunc?:
        | "sum"
        | "max"
        | "min"
        | "mean"
        | "median"
        | "first"
        | "last"
        | "count"
        | Expr;
      maintainOrder?: boolean;
      sortColumns?: boolean;
      separator?: string;
    },
  ): DataFrame;
  pivot(options: {
    values: string | string[];
    index: string | string[];
    on: string | string[];
    aggregateFunc?:
      | "sum"
      | "max"
      | "min"
      | "mean"
      | "median"
      | "first"
      | "last"
      | "count"
      | Expr;
    maintainOrder?: boolean;
    sortColumns?: boolean;
    separator?: string;
  }): DataFrame;
  // TODO!
  // /**
  //  * Apply a function on Self.
  //  */
  // pipe(func: (...args: any[]) => T, ...args: any[]): T
  /**
   * Aggregate the columns of this DataFrame to their quantile value.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.quantile(0.5);
   * shape: (1, 3)
   * ╭─────┬─────┬──────╮
   * │ foo ┆ bar ┆ ham  │
   * │ --- ┆ --- ┆ ---  │
   * │ i64 ┆ i64 ┆ str  │
   * ╞═════╪═════╪══════╡
   * │ 2   ┆ 7   ┆ null │
   * ╰─────┴─────┴──────╯
   * ```
   */
  quantile(quantile: number): DataFrame<S>;
  /**
   * __Rechunk the data in this DataFrame to a contiguous allocation.__
   *
   * This will make sure all subsequent operations have optimal and predictable performance.
   */
  rechunk(): DataFrame<S>;
  /**
   * __Rename column names.__
   * ___
   *
   * @param mapping - Key value pairs that map from old name to new name.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * > df.rename({"foo": "apple"});
   * ╭───────┬─────┬─────╮
   * │ apple ┆ bar ┆ ham │
   * │ ---   ┆ --- ┆ --- │
   * │ i64   ┆ i64 ┆ str │
   * ╞═══════╪═════╪═════╡
   * │ 1     ┆ 6   ┆ "a" │
   * ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 2     ┆ 7   ┆ "b" │
   * ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 3     ┆ 8   ┆ "c" │
   * ╰───────┴─────┴─────╯
   * ```
   */
  rename<const U extends Partial<Record<keyof S, string>>>(
    mapping: U,
  ): DataFrame<{ [K in keyof S as U[K] extends string ? U[K] : K]: S[K] }>;
  rename(mapping: Record<string, string>): DataFrame;
  /**
   * Replace a column at an index location.
   *
   * Warning: typescript cannot encode type mutation,
   * so the type of the DataFrame will be incorrect. cast the type of dataframe manually.
   * ___
   * @param index - Column index
   * @param newColumn - New column to insert
   * @example
   * ```
   * > const df: pl.DataFrame = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * > const x = pl.Series("apple", [10, 20, 30]);
   * // x: pl.Series<Float64, "apple">
   * > df.replaceAtIdx(0, x);
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">; <- notice how the type is still the same!
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * shape: (3, 3)
   * ╭───────┬─────┬─────╮
   * │ apple ┆ bar ┆ ham │
   * │ ---   ┆ --- ┆ --- │
   * │ i64   ┆ i64 ┆ str │
   * ╞═══════╪═════╪═════╡
   * │ 10    ┆ 6   ┆ "a" │
   * ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 20    ┆ 7   ┆ "b" │
   * ├╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 30    ┆ 8   ┆ "c" │
   * ╰───────┴─────┴─────╯
   * ```
   */
  replaceAtIdx(index: number, newColumn: Series): void;
  /**
   * Get a row as Array
   * @param index - row index
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.row(2)
   * [3, 8, 'c']
   * ```
   */
  row(index: number): Array<any>;
  /**
   * Convert columnar data to rows as arrays
   */
  rows(): Array<Array<any>>;
  /**
   * @example
   * ```
   * > const df: pl.DataFrame = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * > df.schema
   * // {
   * //     foo: Float64;
   * //     bar: Float64;
   * //     ham: Utf8;
   * // }
   * ```
   */
  get schema(): S;
  /**
   * Select columns from this DataFrame.
   * ___
   * @param columns - Column or columns to select.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...     "foo": [1, 2, 3],
   * ...     "bar": [6, 7, 8],
   * ...     "ham": ['a', 'b', 'c']
   * ...     });
   * // df: pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * //     bar: pl.Series<Float64, "bar">;
   * //     ham: pl.Series<Utf8, "ham">;
   * // }>
   * > df.select('foo');
   * // pl.DataFrame<{
   * //     foo: pl.Series<Float64, "foo">;
   * // }>
   * shape: (3, 1)
   * ┌─────┐
   * │ foo │
   * │ --- │
   * │ i64 │
   * ╞═════╡
   * │ 1   │
   * ├╌╌╌╌╌┤
   * │ 2   │
   * ├╌╌╌╌╌┤
   * │ 3   │
   * └─────┘
   * ```
   */
  select<U extends keyof S>(...columns: U[]): DataFrame<{ [P in U]: S[P] }>;
  select(...columns: ExprOrString[]): DataFrame<S>;
  /**
   * Shift the values by a given period and fill the parts that will be empty due to this operation
   * with `Nones`.
   * ___
   * @param periods - Number of places to shift (may be negative).
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.shift(1);
   * shape: (3, 3)
   * ┌──────┬──────┬──────┐
   * │ foo  ┆ bar  ┆ ham  │
   * │ ---  ┆ ---  ┆ ---  │
   * │ i64  ┆ i64  ┆ str  │
   * ╞══════╪══════╪══════╡
   * │ null ┆ null ┆ null │
   * ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 1    ┆ 6    ┆ "a"  │
   * ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 2    ┆ 7    ┆ "b"  │
   * └──────┴──────┴──────┘
   * > df.shift(-1)
   * shape: (3, 3)
   * ┌──────┬──────┬──────┐
   * │ foo  ┆ bar  ┆ ham  │
   * │ ---  ┆ ---  ┆ ---  │
   * │ i64  ┆ i64  ┆ str  │
   * ╞══════╪══════╪══════╡
   * │ 2    ┆ 7    ┆ "b"  │
   * ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ 3    ┆ 8    ┆ "c"  │
   * ├╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌┤
   * │ null ┆ null ┆ null │
   * └──────┴──────┴──────┘
   * ```
   */
  shift(periods: number): DataFrame<S>;
  shift({ periods }: { periods: number }): DataFrame<S>;
  /**
   * Shift the values by a given period and fill the parts that will be empty due to this operation
   * with the result of the `fill_value` expression.
   * ___
   * @param n - Number of places to shift (may be negative).
   * @param fillValue - fill null values with this value.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.shiftAndFill({n:1, fill_value:0});
   * shape: (3, 3)
   * ┌─────┬─────┬─────┐
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 0   ┆ 0   ┆ "0" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 1   ┆ 6   ┆ "a" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 2   ┆ 7   ┆ "b" │
   * └─────┴─────┴─────┘
   * ```
   */
  shiftAndFill(n: number, fillValue: number): DataFrame<S>;
  shiftAndFill({
    n,
    fillValue,
  }: {
    n: number;
    fillValue: number;
  }): DataFrame<S>;
  /**
   * Shrink memory usage of this DataFrame to fit the exact capacity needed to hold the data.
   */
  shrinkToFit(): DataFrame<S>;
  shrinkToFit(inPlace: true): void;
  shrinkToFit({ inPlace }: { inPlace: true }): void;
  /**
   * Slice this DataFrame over the rows direction.
   * ___
   * @param opts
   * @param opts.offset - Offset index.
   * @param opts.length - Length of the slice
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6.0, 7.0, 8.0],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.slice(1, 2); // Alternatively `df.slice({offset:1, length:2})`
   * shape: (2, 3)
   * ┌─────┬─────┬─────┐
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 2   ┆ 7   ┆ "b" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 3   ┆ 8   ┆ "c" │
   * └─────┴─────┴─────┘
   * ```
   */
  slice({ offset, length }: { offset: number; length: number }): DataFrame<S>;
  slice(offset: number, length: number): DataFrame<S>;
  /**
   * Sort the DataFrame by column.
   * ___
   * @param by - Column(s) to sort by. Accepts expression input, including selectors. Strings are parsed as column names.
   * @param descending - Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans.
   * @param nullsLast - Place null values last; can specify a single boolean applying to all columns or a sequence of booleans for per-column control.
   * @param maintainOrder - Whether the order should be maintained if elements are equal.
   */
  sort(
    by: ColumnsOrExpr,
    descending?: boolean,
    nullsLast?: boolean,
    maintainOrder?: boolean,
  ): DataFrame<S>;
  sort({
    by,
    descending,
    maintainOrder,
  }: {
    by: ColumnsOrExpr;
    descending?: boolean;
    nullsLast?: boolean;
    maintainOrder?: boolean;
  }): DataFrame<S>;
  /**
   * Aggregate the columns of this DataFrame to their standard deviation value.
   * ___
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "foo": [1, 2, 3],
   * ...   "bar": [6, 7, 8],
   * ...   "ham": ['a', 'b', 'c']
   * ... });
   * > df.std();
   * shape: (1, 3)
   * ╭─────┬─────┬──────╮
   * │ foo ┆ bar ┆ ham  │
   * │ --- ┆ --- ┆ ---  │
   * │ f64 ┆ f64 ┆ str  │
   * ╞═════╪═════╪══════╡
   * │ 1   ┆ 1   ┆ null │
   * ╰─────┴─────┴──────╯
   * ```
   */
  std(): DataFrame<S>;
  /**
   * Aggregate the columns of this DataFrame to their mean value.
   * ___
   *
   * @param axis - either 0 or 1
   * @param nullStrategy - this argument is only used if axis == 1
   */
  sum(axis: 1, nullStrategy?: "ignore" | "propagate"): Series;
  sum(): DataFrame<S>;
  sum(axis: 0): DataFrame<S>;
  sum(axis: 1): Series;
  /**
   * @example
   * ```
   * > const df = pl.DataFrame({
   * ...   "letters": ["c", "c", "a", "c", "a", "b"],
   * ...   "nrs": [1, 2, 3, 4, 5, 6]
   * ... });
   * > console.log(df.toString());
   * shape: (6, 2)
   * ╭─────────┬─────╮
   * │ letters ┆ nrs │
   * │ ---     ┆ --- │
   * │ str     ┆ i64 │
   * ╞═════════╪═════╡
   * │ "c"     ┆ 1   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 2   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "a"     ┆ 3   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 4   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "a"     ┆ 5   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "b"     ┆ 6   │
   * ╰─────────┴─────╯
   * > df.groupby("letters")
   * ...   .tail(2)
   * ...   .sort("letters")
   * shape: (5, 2)
   * ╭─────────┬─────╮
   * │ letters ┆ nrs │
   * │ ---     ┆ --- │
   * │ str     ┆ i64 │
   * ╞═════════╪═════╡
   * │ "a"     ┆ 3   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "a"     ┆ 5   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "b"     ┆ 6   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 2   │
   * ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
   * │ "c"     ┆ 4   │
   * ╰─────────┴─────╯
   * ```
   */
  tail(length?: number): DataFrame<S>;
  /**
   * Converts dataframe object into row oriented javascript objects
   * @example
   * ```
   * > df.toRecords()
   * [
   *   {"foo":1.0,"bar":"a"},
   *   {"foo":2.0,"bar":"b"},
   *   {"foo":3.0,"bar":"c"}
   * ]
   * ```
   * @category IO
   */
  toRecords(): { [K in keyof S]: DTypeToJs<S[K]> | null }[];
  /**
   * Converts dataframe object into a {@link TabularDataResource}
   */
  toDataResource(): TabularDataResource;

  /**
   * Converts dataframe object into HTML
   */
  toHTML(): string;

  /**
   * Converts dataframe object into column oriented javascript objects
   * @example
   * ```
   * > df.toObject()
   * {
   *  "foo": [1,2,3],
   *  "bar": ["a", "b", "c"]
   * }
   * ```
   * @category IO
   */
  toObject(): { [K in keyof S]: DTypeToJs<S[K] | null>[] };
  toSeries(index?: number): SchemaToSeriesRecord<S>[keyof S];
  toString(): string;
  /**
   *  Convert a ``DataFrame`` to a ``Series`` of type ``Struct``
   *  @param name Name for the struct Series
   *  @example
   *  ```
   *  > const df = pl.DataFrame({
   *  ...   "a": [1, 2, 3, 4, 5],
   *  ...   "b": ["one", "two", "three", "four", "five"],
   *  ... });
   *  > df.toStruct("nums");
   *  shape: (5,)
   *  Series: 'nums' [struct[2]{'a': i64, 'b': str}]
   *  [
   *          {1,"one"}
   *          {2,"two"}
   *          {3,"three"}
   *          {4,"four"}
   *          {5,"five"}
   *  ]
   *  ```
   */
  toStruct(name: string): Series;
  /**
   * Transpose a DataFrame over the diagonal.
   *
   * @remarks This is a very expensive operation. Perhaps you can do it differently.
   * @param options
   * @param options.includeHeader If set, the column names will be added as first column.
   * @param options.headerName If `includeHeader` is set, this determines the name of the column that will be inserted
   * @param options.columnNames Optional generator/iterator that yields column names. Will be used to replace the columns in the DataFrame.
   *
   * @example
   * > const df = pl.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3]});
   * > df.transpose({includeHeader:true})
   * shape: (2, 4)
   * ┌────────┬──────────┬──────────┬──────────┐
   * │ column ┆ column_0 ┆ column_1 ┆ column_2 │
   * │ ---    ┆ ---      ┆ ---      ┆ ---      │
   * │ str    ┆ i64      ┆ i64      ┆ i64      │
   * ╞════════╪══════════╪══════════╪══════════╡
   * │ a      ┆ 1        ┆ 2        ┆ 3        │
   * ├╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
   * │ b      ┆ 1        ┆ 2        ┆ 3        │
   * └────────┴──────────┴──────────┴──────────┘
   * // replace the auto generated column names with a list
   * > df.transpose({includeHeader:false, columnNames:["a", "b", "c"]})
   * shape: (2, 3)
   * ┌─────┬─────┬─────┐
   * │ a   ┆ b   ┆ c   │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ i64 │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 2   ┆ 3   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 1   ┆ 2   ┆ 3   │
   * └─────┴─────┴─────┘
   *
   * // Include the header as a separate column
   * > df.transpose({
   * ...     includeHeader:true,
   * ...     headerName:"foo",
   * ...     columnNames:["a", "b", "c"]
   * ... })
   * shape: (2, 4)
   * ┌─────┬─────┬─────┬─────┐
   * │ foo ┆ a   ┆ b   ┆ c   │
   * │ --- ┆ --- ┆ --- ┆ --- │
   * │ str ┆ i64 ┆ i64 ┆ i64 │
   * ╞═════╪═════╪═════╪═════╡
   * │ a   ┆ 1   ┆ 2   ┆ 3   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ b   ┆ 1   ┆ 2   ┆ 3   │
   * └─────┴─────┴─────┴─────┘
   *
   * // Replace the auto generated column with column names from a generator function
   * > function *namesGenerator() {
   * ...     const baseName = "my_column_";
   * ...     let count = 0;
   * ...     let name = `${baseName}_${count}`;
   * ...     count++;
   * ...     yield name;
   * ... }
   * > df.transpose({includeHeader:false, columnNames:namesGenerator})
   * shape: (2, 3)
   * ┌─────────────┬─────────────┬─────────────┐
   * │ my_column_0 ┆ my_column_1 ┆ my_column_2 │
   * │ ---         ┆ ---         ┆ ---         │
   * │ i64         ┆ i64         ┆ i64         │
   * ╞═════════════╪═════════════╪═════════════╡
   * │ 1           ┆ 2           ┆ 3           │
   * ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
   * │ 1           ┆ 2           ┆ 3           │
   * └─────────────┴─────────────┴─────────────┘
   */
  transpose(options?: {
    includeHeader?: boolean;
    headerName?: string;
    columnNames?: Iterable<string>;
  }): DataFrame;
  /**
   * Drop duplicate rows from this DataFrame.
   * Note that this fails if there is a column of type `List` in the DataFrame.
   * @param subset Column name(s), selector(s) to consider when identifying duplicate rows. If set to `None` (default), all columns are considered.
   * @param keep : 'first', 'last', 'any', 'none' 
   *        Which of the duplicate rows to keep. 
            * 'any': Defaut, does not give any guarantee of which row is kept. This allows more optimizations.
            * 'none': Don't keep duplicate rows.
            * 'first': Keep the first unique row.
            * 'last': Keep the last unique row.
   * @param maintainOrder Keep the same order as the original DataFrame. This is more expensive to compute. Default: false
   * @returns DataFrame with unique rows.
   * @example
   * const df = pl.DataFrame({
           foo: [1, 2, 2, 3],
           bar: [1, 2, 2, 4],
           ham: ["a", "d", "d", "c"],
         });
    > df.unique();
    By default, all columns are considered when determining which rows are unique:
    shape: (3, 3)
    ┌─────┬─────┬─────┐
    │ foo ┆ bar ┆ ham │
    │ --- ┆ --- ┆ --- │
    │ f64 ┆ f64 ┆ str │
    ╞═════╪═════╪═════╡
    │ 3.0 ┆ 4.0 ┆ c   │
    │ 1.0 ┆ 1.0 ┆ a   │
    │ 2.0 ┆ 2.0 ┆ d   │
    └─────┴─────┴─────┘
    > df.unique("foo");
    shape: (3, 3)
    ┌─────┬─────┬─────┐
    │ foo ┆ bar ┆ ham │
    │ --- ┆ --- ┆ --- │
    │ f64 ┆ f64 ┆ str │
    ╞═════╪═════╪═════╡
    │ 3.0 ┆ 4.0 ┆ c   │
    │ 1.0 ┆ 1.0 ┆ a   │
    │ 2.0 ┆ 2.0 ┆ d   │
    └─────┴─────┴─────┘
    > df.unique(["foo", "ham"], "first", true); or df.unique({ subset: ["foo", "ham"], keep: "first", maintainOrder: true });
    shape: (3, 3)
    ┌─────┬─────┬─────┐
    │ foo ┆ bar ┆ ham │
    │ --- ┆ --- ┆ --- │
    │ f64 ┆ f64 ┆ str │
    ╞═════╪═════╪═════╡
    │ 1.0 ┆ 1.0 ┆ a   │
    │ 2.0 ┆ 2.0 ┆ d   │
    │ 3.0 ┆ 4.0 ┆ c   │
    └─────┴─────┴─────┘
  */
  unique(
    subset?: ColumnSelection,
    keep?: "first" | "last" | "any" | "none",
    maintainOrder?: boolean,
  ): DataFrame<S>;
  unique(opts: {
    subset?: ColumnSelection;
    keep?: "first" | "last" | "any" | "none";
    maintainOrder?: boolean;
  }): DataFrame<S>;
  /**
    Decompose struct columns into separate columns for each of their fields.
    The new columns will be inserted into the DataFrame at the location of the struct column.
    @param columns Name of the struct column(s) that should be unnested.
    @param separator Rename output column names as combination of the struct column name, name separator and field name.
    @example
    ```
    > const df = pl.DataFrame({
    ...   "int": [1, 2],
    ...   "str": ["a", "b"],
    ...   "bool": [true, null],
    ...   "list": [[1, 2], [3]],
    ... })
    ...  .toStruct("my_struct")
    ...  .toFrame();
    > df
    shape: (2, 1)
    ┌─────────────────────────────┐
    │ my_struct                   │
    │ ---                         │
    │ struct[4]{'int',...,'list'} │
    ╞═════════════════════════════╡
    │ {1,"a",true,[1, 2]}         │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ {2,"b",null,[3]}            │
    └─────────────────────────────┘
    > df.unnest("my_struct")
    shape: (2, 4)
    ┌─────┬─────┬──────┬────────────┐
    │ int ┆ str ┆ bool ┆ list       │
    │ --- ┆ --- ┆ ---  ┆ ---        │
    │ i64 ┆ str ┆ bool ┆ list [i64] │
    ╞═════╪═════╪══════╪════════════╡
    │ 1   ┆ a   ┆ true ┆ [1, 2]     │
    ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 2   ┆ b   ┆ null ┆ [3]        │
    └─────┴─────┴──────┴────────────┘
    ```
  */
  unnest(columns: string | string[], separator?: string): DataFrame;
  /**
   * Aggregate the columns of this DataFrame to their variance value.
   * @example
   * ```
   * > const df = pl.DataFrame({
   * >   "foo": [1, 2, 3],
   * >   "bar": [6, 7, 8],
   * >   "ham": ['a', 'b', 'c']
   * > });
   * > df.var()
   * shape: (1, 3)
   * ╭─────┬─────┬──────╮
   * │ foo ┆ bar ┆ ham  │
   * │ --- ┆ --- ┆ ---  │
   * │ f64 ┆ f64 ┆ str  │
   * ╞═════╪═════╪══════╡
   * │ 1   ┆ 1   ┆ null │
   * ╰─────┴─────┴──────╯
   * ```
   */
  var(): DataFrame<S>;
  /**
   * Grow this DataFrame vertically by stacking a DataFrame to it.
   * @param df - DataFrame to stack.
   * @example
   * ```
   * > const df1 = pl.DataFrame({
   * ...   "foo": [1, 2],
   * ...   "bar": [6, 7],
   * ...   "ham": ['a', 'b']
   * ... });
   * > const df2 = pl.DataFrame({
   * ...   "foo": [3, 4],
   * ...   "bar": [8 , 9],
   * ...   "ham": ['c', 'd']
   * ... });
   * > df1.vstack(df2);
   * shape: (4, 3)
   * ╭─────┬─────┬─────╮
   * │ foo ┆ bar ┆ ham │
   * │ --- ┆ --- ┆ --- │
   * │ i64 ┆ i64 ┆ str │
   * ╞═════╪═════╪═════╡
   * │ 1   ┆ 6   ┆ "a" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 2   ┆ 7   ┆ "b" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 3   ┆ 8   ┆ "c" │
   * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 4   ┆ 9   ┆ "d" │
   * ╰─────┴─────┴─────╯
   * ```
   */
  vstack(df: DataFrame<S>): DataFrame<S>;
  /**
   * Return a new DataFrame with the column added or replaced.
   * @param column - Series, where the name of the Series refers to the column in the DataFrame.
   */
  withColumn<SeriesTypeT extends DataType, SeriesNameT extends string>(
    column: Series<SeriesTypeT, SeriesNameT>,
  ): DataFrame<Simplify<S & { [K in SeriesNameT]: SeriesTypeT }>>;
  withColumn(column: Series | Expr): DataFrame;
  withColumns(...columns: (Expr | Series)[]): DataFrame;
  /**
   * Return a new DataFrame with the column renamed.
   * @param existingName
   * @param replacement
   */
  withColumnRenamed<Existing extends keyof S, New extends string>(
    existingName: Existing,
    replacement: New,
  ): DataFrame<{ [K in keyof S as K extends Existing ? New : K]: S[K] }>;
  withColumnRenamed(existing: string, replacement: string): DataFrame;

  withColumnRenamed<Existing extends keyof S, New extends string>(opts: {
    existingName: Existing;
    replacement: New;
  }): DataFrame<{ [K in keyof S as K extends Existing ? New : K]: S[K] }>;
  withColumnRenamed(opts: { existing: string; replacement: string }): DataFrame;
  /**
   * Add a column at index 0 that counts the rows.
   * @param name - name of the column to add
   * @deprecated - *since 0.23.0 use withRowIndex instead
   */
  withRowCount(name?: string): DataFrame;
  /**
   * Add a row index as the first column in the DataFrame.
   * @param name Name of the index column.
   * @param offset Start the index at this offset. Cannot be negative.
   * @example
   * 
   * >>> df = pl.DataFrame(
    ...     {
    ...         "a": [1, 3, 5],
    ...         "b": [2, 4, 6],
    ...     }
    ... )
    >>> df.withRowIndex()
    shape: (3, 3)
    ┌───────┬─────┬─────┐
    │ index ┆ a   ┆ b   │
    │ ---   ┆ --- ┆ --- │
    │ u32   ┆ i64 ┆ i64 │
    ╞═══════╪═════╪═════╡
    │ 0     ┆ 1   ┆ 2   │
    │ 1     ┆ 3   ┆ 4   │
    │ 2     ┆ 5   ┆ 6   │
    └───────┴─────┴─────┘
    >>> df.withRowIndex("id", offset=1000)
    shape: (3, 3)
    ┌──────┬─────┬─────┐
    │ id   ┆ a   ┆ b   │
    │ ---  ┆ --- ┆ --- │
    │ u32  ┆ i64 ┆ i64 │
    ╞══════╪═════╪═════╡
    │ 1000 ┆ 1   ┆ 2   │
    │ 1001 ┆ 3   ┆ 4   │
    │ 1002 ┆ 5   ┆ 6   │
    └──────┴─────┴─────┘
   */
  withRowIndex(name?: string, offset?: number): DataFrame;
  /** @see {@link filter} */
  where(predicate: any): DataFrame<S>;
  /**
    Upsample a DataFrame at a regular frequency.

    The `every` and `offset` arguments are created with the following string language:
    - 1ns   (1 nanosecond)
    - 1us   (1 microsecond)
    - 1ms   (1 millisecond)
    - 1s    (1 second)
    - 1m    (1 minute)
    - 1h    (1 hour)
    - 1d    (1 calendar day)
    - 1w    (1 calendar week)
    - 1mo   (1 calendar month)
    - 1q    (1 calendar quarter)
    - 1y    (1 calendar year)
    - 1i    (1 index count)

    Or combine them:
    - "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds

    By "calendar day", we mean the corresponding time on the next day (which may not be 24 hours, due to daylight savings).
    Similarly for "calendar week", "calendar month", "calendar quarter", and "calendar year".

    Parameters
    ----------
    @param timeColumn Time column will be used to determine a date range.
                        Note that this column has to be sorted for the output to make sense.
    @param every Interval will start 'every' duration.
    @param by First group by these columns and then upsample for every group.
    @param maintainOrder Keep the ordering predictable. This is slower.

    Returns
    -------
    DataFrame
        Result will be sorted by `timeColumn` (but note that if `by` columns are passed, it will only be sorted within each `by` group).

    Examples
    --------
    Upsample a DataFrame by a certain interval.

    >>> const df = pl.DataFrame({
            "date": [
                new Date(2024, 1, 1),
                new Date(2024, 3, 1),
                new Date(2024, 4, 1),
                new Date(2024, 5, 1),
            ],
            "groups": ["A", "B", "A", "B"],
            "values": [0, 1, 2, 3],
         })
          .withColumn(pl.col("date").cast(pl.Date).alias("date"))
          .sort("date");

    >>> df.upsample({timeColumn: "date", every: "1mo", by: "groups", maintainOrder: true})
          .select(pl.col("*").forwardFill());
shape: (7, 3)
┌────────────┬────────┬────────┐
│ date       ┆ groups ┆ values │
│ ---        ┆ ---    ┆ ---    │
│ date       ┆ str    ┆ f64    │
╞════════════╪════════╪════════╡
│ 2024-02-01 ┆ A      ┆ 0.0    │
│ 2024-03-01 ┆ A      ┆ 0.0    │
│ 2024-04-01 ┆ A      ┆ 0.0    │
│ 2024-05-01 ┆ A      ┆ 2.0    │
│ 2024-04-01 ┆ B      ┆ 1.0    │
│ 2024-05-01 ┆ B      ┆ 1.0    │
│ 2024-06-01 ┆ B      ┆ 3.0    │
└────────────┴────────┴────────┘
  */
  upsample(
    timeColumn: string,
    every: string,
    by?: string | string[],
    maintainOrder?: boolean,
  ): DataFrame<S>;
  upsample(opts: {
    timeColumn: string;
    every: string;
    by?: string | string[];
    maintainOrder?: boolean;
  }): DataFrame<S>;
}

function prepareOtherArg(anyValue: any): Series {
  if (Series.isSeries(anyValue)) {
    return anyValue;
  }
  return Series([anyValue]) as Series;
}

function map(df: DataFrame, fn: (...args: any[]) => any[]) {
  return df.rows().map(fn);
}

type DataResourceField = {
  name: string;
  type: string;
};

/**
 * Tabular Data Resource from https://specs.frictionlessdata.io/schemas/tabular-data-resource.json,
 */
type TabularDataResource = {
  data: any[];
  schema: {
    fields: DataResourceField[];
  };
};

function mapPolarsTypeToJSONSchema(colType: DataType): string {
  const typeMapping: { [key: string]: string } = {
    Null: "null",
    Bool: "boolean",
    Int8: "integer",
    Int16: "integer",
    Int32: "integer",
    Int64: "integer",
    UInt8: "integer",
    UInt16: "integer",
    UInt32: "integer",
    UInt64: "integer",
    Float32: "number",
    Float64: "number",
    Date: "string",
    Datetime: "string",
    Utf8: "string",
    Categorical: "string",
    List: "array",
    Struct: "object",
  };

  const dataType = colType.variant;
  return typeMapping[dataType] || "string";
}

/** @ignore */
export const _DataFrame = <S extends Schema>(_df: any): DataFrame<S> => {
  const unwrap = (method: string, ...args: any[]) => {
    return _df[method as any](...args);
  };
  const wrap = (method, ...args): DataFrame<any> => {
    return _DataFrame(unwrap(method, ...args));
  };

  const df = {
    /** @ignore */
    _df,
    [inspect]() {
      return _df.toString();
    },
    *[Symbol.iterator]() {
      let start = 0;
      const len = this.width;

      while (start < len) {
        const s = this.toSeries(start);
        start++;
        yield s;
      }
    },
    get [Symbol.toStringTag]() {
      return "DataFrame";
    },
    get dtypes() {
      return _df.dtypes().map(DataType.deserialize);
    },
    get height() {
      return _df.height;
    },
    get width() {
      return _df.width;
    },
    get shape() {
      return _df.shape;
    },
    get columns() {
      return _df.columns;
    },
    set columns(names) {
      _df.columns = names;
    },
    /**
     * Return back text/html and application/vnd.dataresource+json representations
     * of the DataFrame. This is intended to be a simple view of the DataFrame
     * inside of notebooks.
     *
     * @returns Media bundle / mimetype keys for Jupyter frontends
     */
    [jupyterDisplay]() {
      let rows = 50;
      if (process.env.POLARS_FMT_MAX_ROWS) {
        rows = Number.parseInt(process.env.POLARS_FMT_MAX_ROWS, 10);
      }

      const limited = this.limit(rows);
      return {
        "application/vnd.dataresource+json": limited.toDataResource(),
        "text/html": limited.toHTML(),
      };
    },
    get schema(): any {
      return this.getColumns().reduce((acc, curr) => {
        acc[curr.name] = curr.dtype;

        return acc;
      }, {});
    },
    clone() {
      return wrap("clone");
    },
    describe() {
      const describeCast = (df: DataFrame<S>) => {
        return DataFrame(
          df.getColumns().map((s) => {
            if (s.isNumeric() || s.isBoolean()) {
              return s.cast(DataType.Float64);
            }
            return s;
          }),
        );
      };
      const summary = concat([
        describeCast(this.mean()),
        describeCast(this.std()),
        describeCast(this.min()),
        describeCast(this.max()),
        describeCast(this.median()),
      ] as any);
      summary.insertAtIdx(
        0,
        Series("describe", ["mean", "std", "min", "max", "median"]),
      );

      return summary;
    },
    inner() {
      return _df;
    },
    drop(...names) {
      if (!Array.isArray(names[0]) && names.length === 1) {
        return wrap("drop", names[0]);
      }
      const df: any = this.clone();
      for (const name of names.flat(2)) {
        df.inner().dropInPlace(name);
      }
      return df;
    },
    dropNulls(...subset) {
      if (subset.length) {
        return wrap("dropNulls", subset.flat(2));
      }
      return wrap("dropNulls");
    },
    explode(...columns) {
      return _DataFrame(_df)
        .lazy()
        .explode(columns)
        .collectSync({ noOptimization: true });
    },
    extend(other) {
      return wrap("extend", (other as any).inner());
    },
    filter(predicate) {
      return this.lazy().filter(predicate).collectSync();
    },
    fillNull(strategy) {
      return wrap("fillNull", strategy);
    },
    findIdxByName(name) {
      return unwrap("findIdxByName", name);
    },
    fold(fn: (s1, s2) => any) {
      if (this.width === 1) {
        return this.toSeries(0);
      }

      return this.getColumns().reduce((acc, curr) => fn(acc, curr));
    },
    frameEqual(other, nullEqual = true) {
      return unwrap("frameEqual", other._df, nullEqual);
    },
    getColumn(name) {
      return _Series(_df.column(name)) as any;
    },
    getColumns() {
      return _df.getColumns().map(_Series) as any;
    },
    groupBy(...by) {
      return _GroupBy(_df as any, columnOrColumnsStrict(by));
    },
    groupByRolling(opts) {
      return RollingGroupBy(
        _DataFrame(_df) as any,
        opts.indexColumn,
        opts.period,
        opts.offset,
        opts.closed,
        opts.by,
      );
    },
    groupByDynamic({
      indexColumn,
      every,
      period,
      offset,
      label,
      includeBoundaries,
      closed,
      by,
      startBy,
    }) {
      return DynamicGroupBy(
        _DataFrame(_df) as any,
        indexColumn,
        every,
        period,
        offset,
        label,
        includeBoundaries,
        closed,
        by,
        startBy,
      );
    },
    upsample(opts, every?, by?, maintainOrder?): any {
      let timeColumn;
      if (typeof opts === "string") {
        timeColumn = opts;
      } else {
        timeColumn = opts.timeColumn;
        by = opts.by;
        every = opts.every;
        maintainOrder = opts.maintainOrder ?? false;
      }

      if (typeof by === "string") {
        by = [by];
      } else {
        by = by ?? [];
      }

      return _DataFrame(_df.upsample(by, timeColumn, every, maintainOrder));
    },
    hashRows(obj: any = 0n, k1 = 1n, k2 = 2n, k3 = 3n) {
      if (typeof obj === "number" || typeof obj === "bigint") {
        return _Series(
          _df.hashRows(BigInt(obj), BigInt(k1), BigInt(k2), BigInt(k3)),
        );
      }
      const o = { k0: obj, k1: k1, k2: k2, k3: k3, ...obj };

      return _Series(
        _df.hashRows(BigInt(o.k0), BigInt(o.k1), BigInt(o.k2), BigInt(o.k3)),
      ) as any;
    },
    head(length = 5) {
      return wrap("head", length);
    },
    hstack(columns, inPlace = false) {
      if (!Array.isArray(columns)) {
        columns = columns.getColumns();
      }
      const method = inPlace ? "hstackMut" : "hstack";

      return wrap(
        method,
        columns.map((col) => col.inner()),
      );
    },
    insertAtIdx(idx, series) {
      _df.insertAtIdx(idx, series.inner());
    },
    interpolate() {
      return this.select(col("*").interpolate());
    },
    isDuplicated: () => _Series(_df.isDuplicated()) as any,
    isEmpty: () => _df.height === 0,
    isUnique: () => _Series(_df.isUnique()) as any,
    join(other, options): any {
      options = { how: "inner", ...options };
      const on = columnOrColumns(options.on);
      const how: string = options.how;
      const suffix: string = options.suffix;
      const coalesce: boolean = options.coalesce;
      const validate: string = options.validate;
      if (how === "cross") {
        return _DataFrame(
          _df.join(other._df, [], [], how, suffix, coalesce, validate),
        );
      }
      let leftOn = columnOrColumns(options.leftOn);
      let rightOn = columnOrColumns(options.rightOn);

      if (on) {
        leftOn = on;
        rightOn = on;
      }
      if ((leftOn && !rightOn) || (rightOn && !leftOn)) {
        throw new TypeError(
          "You should pass the column to join on as an argument.",
        );
      }
      return wrap(
        "join",
        other._df,
        leftOn,
        rightOn,
        how,
        suffix,
        coalesce,
        validate,
      );
    },
    joinAsof(other, options) {
      return this.lazy()
        .joinAsof(other.lazy(), options as any)
        .collectSync();
    },
    lazy: () => _LazyDataFrame(_df.lazy()) as unknown as LazyDataFrame<S>,
    limit: (length = 5) => wrap("head", length),
    max(axis = 0) {
      if (axis === 1) {
        return _Series(_df.hmax() as any) as any;
      }
      return this.lazy().max().collectSync();
    },
    mean(axis = 0, nullStrategy = "ignore") {
      if (axis === 1) {
        return _Series(_df.hmean(nullStrategy) as any) as any;
      }
      return this.lazy().mean().collectSync();
    },
    median() {
      return this.lazy().median().collectSync();
    },
    unpivot(ids, values, options) {
      options = {
        variableName: null,
        valueName: null,
        ...options,
      };
      return wrap(
        "unpivot",
        columnOrColumns(ids),
        columnOrColumns(values),
        options.variableName,
        options.valueName,
      );
    },
    min(axis = 0) {
      if (axis === 1) {
        return _Series(_df.hmin() as any) as any;
      }
      return this.lazy().min().collectSync();
    },
    nChunks() {
      return _df.nChunks();
    },
    nullCount() {
      return wrap("nullCount");
    },
    partitionBy(by, strict = false, includeKey = true, mapFn = (df) => df) {
      by = Array.isArray(by) ? by : [by];
      return _df
        .partitionBy(by, strict, includeKey)
        .map((d) => mapFn(_DataFrame(d)));
    },
    pivot(values, options?) {
      if (values && !options) {
        options = values;
      }
      let {
        values: values_,
        index,
        on,
        maintainOrder = true,
        sortColumns = false,
        aggregateFunc = "first",
        separator,
      } = options;
      values = values_ ?? values;
      values = typeof values === "string" ? [values] : values;
      index = typeof index === "string" ? [index] : index;
      on = typeof on === "string" ? [on] : on;

      let fn: Expr;
      if (Expr.isExpr(aggregateFunc)) {
        fn = aggregateFunc;
      } else {
        fn =
          {
            first: element().first(),
            sum: element().sum(),
            max: element().max(),
            min: element().min(),
            mean: element().mean(),
            median: element().median(),
            last: element().last(),
            count: element().count(),
          }[aggregateFunc] ??
          new Error(`Unknown aggregate function ${aggregateFunc}`);
        if (fn instanceof Error) {
          throw fn;
        }
      }

      return _DataFrame(
        _df.pivotExpr(
          values,
          on,
          index,
          fn,
          maintainOrder,
          sortColumns,
          separator,
        ),
      );
    },
    quantile(quantile) {
      return this.lazy().quantile(quantile).collectSync();
    },
    rechunk() {
      return wrap("rechunk");
    },
    rename(mapping): any {
      const df = this.clone();
      for (const [column, new_col] of Object.entries(mapping)) {
        (df as any).inner().rename(column, new_col);
      }
      return df;
    },
    replaceAtIdx(index, newColumn) {
      _df.replaceAtIdx(index, newColumn.inner());

      return this;
    },
    rows(callback?: any) {
      if (callback) {
        return _df.toRowsCb(callback);
      }

      return _df.toRows();
    },
    sample(opts?, frac?, withReplacement = false, seed?) {
      if (arguments.length === 0) {
        return wrap(
          "sampleN",
          Series("", [1]).inner(),
          withReplacement,
          false,
          seed,
        );
      }
      if (opts?.n !== undefined || opts?.frac !== undefined) {
        return this.sample(opts.n, opts.frac, opts.withReplacement, seed);
      }
      if (typeof opts === "number") {
        return wrap(
          "sampleN",
          Series("", [opts]).inner(),
          withReplacement,
          false,
          seed,
        );
      }
      if (typeof frac === "number") {
        return wrap(
          "sampleFrac",
          Series("", [frac]).inner(),
          withReplacement,
          false,
          seed,
        );
      }
      throw new TypeError("must specify either 'frac' or 'n'");
    },
    select(...selection) {
      const hasExpr = selection.flat().some((s) => Expr.isExpr(s));
      if (hasExpr) {
        return _DataFrame(_df).lazy().select(selection).collectSync();
      }
      return wrap("select", columnOrColumnsStrict(selection as any));
    },
    shift: (opt) => wrap("shift", opt?.periods ?? opt),
    shiftAndFill(n: any, fillValue?: number | undefined): any {
      if (typeof n === "number" && fillValue) {
        return _DataFrame(_df).lazy().shiftAndFill(n, fillValue).collectSync();
      }
      return _DataFrame(_df)
        .lazy()
        .shiftAndFill(n.n, n.fillValue)
        .collectSync();
    },
    shrinkToFit(inPlace: any = false): any {
      if (inPlace) {
        _df.shrinkToFit();
      } else {
        const d = this.clone() as any;
        d.inner().shrinkToFit();

        return d;
      }
    },
    slice(opts, length?) {
      if (typeof opts === "number") {
        return wrap("slice", opts, length);
      }
      return wrap("slice", opts.offset, opts.length);
    },
    sort(arg, descending = false, nullsLast = false, maintainOrder = false) {
      if (arg?.by !== undefined) {
        return this.sort(
          arg.by,
          arg.descending ?? false,
          arg.nullsLast,
          arg.maintainOrder,
        );
      }
      if (Array.isArray(arg) || Expr.isExpr(arg)) {
        return _DataFrame(_df)
          .lazy()
          .sort(arg, descending, nullsLast, maintainOrder)
          .collectSync({ noOptimization: true });
      }
      return wrap("sort", arg, descending, nullsLast, maintainOrder);
    },
    std() {
      return this.lazy().std().collectSync();
    },
    sum(axis = 0, nullStrategy = "ignore") {
      if (axis === 1) {
        return _Series(_df.hsum(nullStrategy) as any) as any;
      }
      return this.lazy().sum().collectSync();
    },
    tail: (length = 5) => wrap("tail", length),
    serialize(format) {
      return _df.serialize(format);
    },
    writeCSV(dest?, options?) {
      options = { ...writeCsvDefaultOptions, ...options };
      if (dest instanceof Writable || typeof dest === "string") {
        return _df.writeCsv(dest, options) as any;
      }
      const buffers: Buffer[] = [];
      const writeStream = new Stream.Writable({
        write(chunk, _encoding, callback) {
          buffers.push(chunk);
          callback(null);
        },
      });
      _df.writeCsv(writeStream as any, dest ?? options);
      writeStream.end("");

      return Buffer.concat(buffers);
    },
    toRecords() {
      return _df.toObjects();
    },
    toHTML(): string {
      let htmlTable = "<table>";

      // Add table headers
      htmlTable += "<thead><tr>";
      for (const field of this.getColumns()) {
        htmlTable += `<th>${escapeHTML(field.name)}</th>`;
      }
      htmlTable += "</tr></thead>";
      // Add table data
      htmlTable += "<tbody>";
      for (const row of this.toRecords()) {
        htmlTable += "<tr>";
        for (const field of this.getColumns()) {
          htmlTable += `<td>${escapeHTML(String(row[field.name]))}</td>`;
        }
        htmlTable += "</tr>";
      }
      htmlTable += "</tbody></table>";

      return htmlTable;
    },
    toDataResource(): TabularDataResource {
      const data = this.toRecords();

      const fields = this.getColumns().map((column) => ({
        name: column.name,
        type: mapPolarsTypeToJSONSchema(column.dtype),
      }));

      return { data, schema: { fields } };
    },
    toObject(): any {
      return this.getColumns().reduce((acc, curr) => {
        acc[curr.name] = curr.toArray();

        return acc;
      }, {});
    },
    withRowIndex(name = "index", offset = 0) {
      return wrap("withRowIndex", name, offset);
    },
    writeJSON(dest?, options = { format: "lines" }) {
      if (dest instanceof Writable || typeof dest === "string") {
        return _df.writeJson(dest, options) as any;
      }
      const buffers: Buffer[] = [];
      const writeStream = new Stream.Writable({
        write(chunk, _encoding, callback) {
          buffers.push(chunk);
          callback(null);
        },
      });

      _df.writeJson(writeStream, { ...options, ...dest });
      writeStream.end("");

      return Buffer.concat(buffers);
    },
    writeParquet(dest?, options = { compression: "uncompressed" }) {
      if (dest instanceof Writable || typeof dest === "string") {
        return _df.writeParquet(dest, options.compression) as any;
      }
      const buffers: Buffer[] = [];
      const writeStream = new Stream.Writable({
        write(chunk, _encoding, callback) {
          buffers.push(chunk);
          callback(null);
        },
      });

      _df.writeParquet(writeStream, dest?.compression ?? options?.compression);
      writeStream.end("");

      return Buffer.concat(buffers);
    },
    writeAvro(dest?, options = { compression: "uncompressed" }) {
      if (dest instanceof Writable || typeof dest === "string") {
        return _df.writeAvro(dest, options.compression) as any;
      }
      const buffers: Buffer[] = [];
      const writeStream = new Stream.Writable({
        write(chunk, _encoding, callback) {
          buffers.push(chunk);
          callback(null);
        },
      });

      _df.writeAvro(writeStream, dest?.compression ?? options?.compression);
      writeStream.end("");

      return Buffer.concat(buffers);
    },
    writeIPC(dest?, options = { compression: "uncompressed" }) {
      if (dest instanceof Writable || typeof dest === "string") {
        return _df.writeIpc(dest, options.compression) as any;
      }
      const buffers: Buffer[] = [];
      const writeStream = new Stream.Writable({
        write(chunk, _encoding, callback) {
          buffers.push(chunk);
          callback(null);
        },
      });

      _df.writeIpc(writeStream, dest?.compression ?? options?.compression);
      writeStream.end("");

      return Buffer.concat(buffers);
    },
    writeIPCStream(dest?, options = { compression: "uncompressed" }) {
      if (dest instanceof Writable || typeof dest === "string") {
        return _df.writeIpcStream(dest, options.compression) as any;
      }
      const buffers: Buffer[] = [];
      const writeStream = new Stream.Writable({
        write(chunk, _encoding, callback) {
          buffers.push(chunk);
          callback(null);
        },
      });

      _df.writeIpcStream(
        writeStream,
        dest?.compression ?? options?.compression,
      );
      writeStream.end("");

      return Buffer.concat(buffers);
    },
    toSeries: (index = 0) => _Series(_df.selectAtIdx(index) as any) as any,
    toStruct(name) {
      return _Series(_df.toStruct(name));
    },
    toString() {
      return _df.toString();
    },
    transpose(options?) {
      const includeHeader = options?.includeHeader ?? false;
      const headeName = options?.headerName ?? "column";
      const keep_names_as = includeHeader ? headeName : undefined;
      if (options?.columnNames) {
        function takeNItems(iterable: Iterable<string>, n: number) {
          const result: Array<string> = [];
          let i = 0;
          for (const item of iterable) {
            if (i >= n) {
              break;
            }
            result.push(item);
            i++;
          }
          return result;
        }
        options.columnNames = Array.isArray(options.columnNames)
          ? options.columnNames.slice(0, this.height)
          : takeNItems(options.columnNames, this.height);
      }
      if (!options?.columnNames) {
        return wrap("transpose", keep_names_as, undefined);
      }
      return wrap("transpose", keep_names_as, options.columnNames);
    },
    unique(
      opts?:
        | ColumnSelection
        | { subset?: ColumnSelection; keep?: string; maintainOrder?: boolean }
        | null,
      keep = "any",
      maintainOrder = false,
    ) {
      // no arguments -> signal call with defaults
      if (arguments.length === 0)
        return wrap("unique", null, keep, maintainOrder);

      // string -> single-element array
      if (typeof opts === "string")
        return wrap("unique", [opts], keep, maintainOrder);

      // array -> use as-is
      if (Array.isArray(opts)) return wrap("unique", opts, keep, maintainOrder);

      // object -> merge defaults, normalize subset to array (if present)
      if (opts && typeof opts === "object") {
        const o = { keep, maintainOrder, ...(opts as any) };
        const subset = o.subset ? ([o.subset].flat(3) as string[]) : undefined;
        return wrap("unique", subset, o.keep, o.maintainOrder);
      }
    },
    unnest(columns, separator) {
      columns = Array.isArray(columns) ? columns : [columns];
      return _DataFrame(_df.unnest(columns, separator));
    },
    var() {
      return this.lazy().var().collectSync();
    },
    map: (fn) => map(_DataFrame(_df), fn as any) as any,
    row(idx) {
      return _df.toRow(idx);
    },
    vstack: (other) => wrap("vstack", (other as any).inner()),
    withColumn(column: Series | Expr) {
      if (Series.isSeries(column)) {
        return wrap("withColumn", column.inner());
      }
      return this.withColumns(column);
    },
    withColumns(...columns: (Expr | Series)[]) {
      if (isSeriesArray(columns)) {
        return columns.reduce(
          (acc, curr) => acc.withColumn(curr),
          _DataFrame(_df),
        );
      }
      return this.lazy()
        .withColumns(...columns)
        .collectSync({ noOptimization: true });
    },
    withColumnRenamed(opt, replacement?): any {
      if (typeof opt === "string") {
        return this.rename({ [opt]: replacement });
      }
      return this.rename({ [opt.existing]: opt.replacement });
    },
    withRowCount(name = "row_nr") {
      return wrap("withRowCount", name);
    },
    where(predicate) {
      return this.filter(predicate);
    },

    add: (other) => wrap("add", prepareOtherArg(other).inner()),
    sub: (other) => wrap("sub", prepareOtherArg(other).inner()),
    div: (other) => wrap("div", prepareOtherArg(other).inner()),
    mul: (other) => wrap("mul", prepareOtherArg(other).inner()),
    rem: (other) => wrap("rem", prepareOtherArg(other).inner()),
    plus: (other) => wrap("add", prepareOtherArg(other).inner()),
    minus: (other) => wrap("sub", prepareOtherArg(other).inner()),
    divideBy: (other) => wrap("div", prepareOtherArg(other).inner()),
    multiplyBy: (other) => wrap("mul", prepareOtherArg(other).inner()),
    modulo: (other) => wrap("rem", prepareOtherArg(other).inner()),
  } as DataFrame<S>;

  return new Proxy(df, {
    get(target: DataFrame<S>, prop, receiver) {
      if (typeof prop === "string" && target.columns.includes(prop)) {
        return target.getColumn(prop);
      }
      if (typeof prop !== "symbol" && !Number.isNaN(Number(prop))) {
        return target.row(Number(prop));
      }
      return Reflect.get(target, prop, receiver);
    },
    set(target: DataFrame<S>, prop, receiver) {
      if (Series.isSeries(receiver)) {
        if (typeof prop === "string" && target.columns.includes(prop)) {
          const idx = target.columns.indexOf(prop);
          target.replaceAtIdx(idx, receiver.alias(prop));

          return true;
        }
      }

      Reflect.set(target, prop, receiver);

      return true;
    },
    has(target, p) {
      if (p === jupyterDisplay) {
        return true;
      }
      return target.columns.includes(p as any);
    },
    ownKeys(target) {
      return target.columns as any;
    },
    getOwnPropertyDescriptor(target, prop) {
      return {
        configurable: true,
        enumerable: true,
        value: target.getColumn(prop as any),
      };
    },
  });
};

interface DataFrameOptions<
  S extends Schema = Schema,
  O extends Partial<S> = any,
> {
  columns?: any[];
  orient?: "row" | "col";
  schema?: S;
  schemaOverrides?: O;
  inferSchemaLength?: number;
}

/**
 * DataFrame constructor
 */
export interface DataFrameConstructor extends Deserialize<DataFrame> {
  /**
   * Create an empty DataFrame
   */
  (): DataFrame;
  /**
   * Create a DataFrame from a JavaScript object
   *
   * @param data - object or array of data
   * @param options - options
   * @param options.columns - column names
   * @param options.orient - orientation of the data [row, col]
   * Whether to interpret two-dimensional data as columns or as rows. If None, the orientation is inferred by matching the columns and data dimensions. If this does not yield conclusive results, column orientation is used.
   * @param options.schema - The schema of the resulting DataFrame. The schema may be declared in several ways:
   *
   *     - As a dict of {name:type} pairs; if type is None, it will be auto-inferred.
   *
   *     - As a list of column names; in this case types are automatically inferred.
   *
   *     - As a list of (name,type) pairs; this is equivalent to the dictionary form.
   *
   * If you supply a list of column names that does not match the names in the underlying data, the names given here will overwrite them. The number of names given in the schema should match the underlying data dimensions.
   *
   * If set to null (default), the schema is inferred from the data.
   * @param options.schemaOverrides - Support type specification or override of one or more columns; note that any dtypes inferred from the schema param will be overridden.
   *
   * @param options.inferSchemaLength - The maximum number of rows to scan for schema inference. If set to None, the full data may be scanned (this can be slow). This parameter only applies if the input data is a sequence or generator of rows; other input is read as-is.
   * The number of entries in the schema should match the underlying data dimensions, unless a sequence of dictionaries is being passed, in which case a partial schema can be declared to prevent specific fields from being loaded.
   *
   * @example
   * ```
   * data = {'a': [1n, 2n], 'b': [3, 4]}
   * df = pl.DataFrame(data)
   * df
   * shape: (2, 2)
   * ╭─────┬─────╮
   * │ a   ┆ b   │
   * │ --- ┆ --- │
   * │ u64 ┆ i64 │
   * ╞═════╪═════╡
   * │ 1   ┆ 3   │
   * ├╌╌╌╌╌┼╌╌╌╌╌┤
   * │ 2   ┆ 4   │
   * ╰─────┴─────╯
   * ```
   */
  <T1 extends ArrayLike<Series>>(
    data: T1,
    options?: DataFrameOptions,
  ): DataFrame<{
    [K in T1[number] as K["name"]]: K["dtype"];
  }>;
  <
    RecordInput extends Record<string, ArrayLike<any>> = any,
    S extends Simplify<ArrayLikeLooseRecordToSchema<RecordInput>> = Simplify<
      ArrayLikeLooseRecordToSchema<RecordInput>
    >,
  >(
    data: RecordInput,
    options?: DataFrameOptions<S>,
  ): DataFrame<S>;
  (data: any, options?: DataFrameOptions): DataFrame;
  isDataFrame(arg: any): arg is DataFrame;
}

function DataFrameConstructor<S extends Schema = Schema>(
  data?,
  options?,
): DataFrame<S> {
  if (!data) {
    return _DataFrame(objToDF({}));
  }

  if (Array.isArray(data)) {
    return _DataFrame(arrayToJsDataFrame(data, options));
  }

  return _DataFrame(objToDF(data as any, options));
}

function objToDF(
  obj: Record<string, Array<any>>,
  options?: {
    columns?: any[];
    orient?: "row" | "col";
    schema?: Record<string, string | DataType>;
    schemaOverrides?: Record<string, string | DataType>;
    inferSchemaLength?: number;
  },
): any {
  let columns;
  if (options?.schema && options?.schemaOverrides) {
    throw new Error("Cannot use both 'schema' and 'schemaOverrides'");
  }
  // explicit schema
  if (options?.schema) {
    const schema = options.schema;
    const schemaKeys = Object.keys(options.schema);
    const values = Object.values(obj);
    if (schemaKeys.length !== values.length) {
      throw new Error(
        "The number of columns in the schema does not match the number of columns in the data",
      );
    }
    columns = values.map((values, idx) => {
      const name = schemaKeys[idx];
      const dtype = schema[name];
      return Series(name, values, dtype).inner();
    });
  } else {
    columns = Object.entries(obj).map(([name, values]) => {
      if (Series.isSeries(values)) {
        return values.rename(name).inner();
      }
      // schema overrides
      if (options?.schemaOverrides) {
        const dtype = options.schemaOverrides[name];
        if (dtype) {
          return Series(name, values, dtype).inner();
        }
      }
      return Series(name, values).inner();
    });
  }

  return new pli.JsDataFrame(columns);
}
const isDataFrame = (anyVal: any): anyVal is DataFrame =>
  anyVal?.[Symbol.toStringTag] === "DataFrame";

export const DataFrame: DataFrameConstructor = Object.assign(
  DataFrameConstructor,
  {
    isDataFrame,
    deserialize: (buf, fmt) =>
      _DataFrame(pli.JsDataFrame.deserialize(buf, fmt)),
  },
);
