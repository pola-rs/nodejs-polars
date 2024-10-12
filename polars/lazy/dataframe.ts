import { type DataFrame, _DataFrame } from "../dataframe";
import pli from "../internals/polars_internal";
import type { Series } from "../series";
import type { Deserialize, GroupByOps, Serialize } from "../shared_traits";
import type {
  LazyJoinOptions,
  LazyOptions,
  SinkCsvOptions,
  SinkParquetOptions,
} from "../types";
import {
  type ColumnSelection,
  type ColumnsOrExpr,
  type ExprOrString,
  type ValueOrArray,
  columnOrColumnsStrict,
  selectionToExprList,
} from "../utils";
import { Expr, exprToLitOrExpr } from "./expr";
import { type LazyGroupBy, _LazyGroupBy } from "./groupby";

const inspect = Symbol.for("nodejs.util.inspect.custom");

/**
 * Representation of a Lazy computation graph / query.
 */
export interface LazyDataFrame extends Serialize, GroupByOps<LazyGroupBy> {
  /** @ignore */
  _ldf: any;
  [inspect](): string;
  [Symbol.toStringTag]: string;
  get columns(): string[];
  /**
   * Cache the result once the execution of the physical plan hits this node.
   */
  cache(): LazyDataFrame;
  clone(): LazyDataFrame;
  /**
   *
   * Collect into a DataFrame.
   * Note: use `fetch` if you want to run this query on the first `n` rows only.
   * This can be a huge time saver in debugging queries.
   * @param typeCoercion -Do type coercion optimization.
   * @param predicatePushdown - Do predicate pushdown optimization.
   * @param projectionPushdown - Do projection pushdown optimization.
   * @param simplifyExpression - Run simplify expressions optimization.
   * @param noOptimization - Turn off optimizations.
   * @param commSubplanElim - Will try to cache branching subplans that occur on self-joins or unions.
   * @param commSubexprElim - Common subexpressions will be cached and reused.
   * @param streaming - Process the query in batches to handle larger-than-memory data.
            If set to `False` (default), the entire query is processed in a single
            batch.

            .. warning::
                Streaming mode is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.
   * @return DataFrame
   *
   */
  collect(opts?: LazyOptions): Promise<DataFrame>;
  collectSync(opts?: LazyOptions): DataFrame;
  /**
   * A string representation of the optimized query plan.
   */
  describeOptimizedPlan(opts?: LazyOptions): string;
  /**
   * A string representation of the unoptimized query plan.
   */
  describePlan(): string;
  /**
   * Remove one or multiple columns from a DataFrame.
   * @param columns - column or list of columns to be removed
   */
  drop(name: string): LazyDataFrame;
  drop(names: string[]): LazyDataFrame;
  drop(name: string, ...names: string[]): LazyDataFrame;
  /**
   * Drop duplicate rows from this DataFrame.
   * Note that this fails if there is a column of type `List` in the DataFrame.
   * @param maintainOrder
   * @param subset - subset to drop duplicates for
   * @param keep "first" | "last"
   * @deprecated @since 0.4.0 @use {@link unique}
   */
  distinct(
    maintainOrder?: boolean,
    subset?: ColumnSelection,
    keep?: "first" | "last",
  ): LazyDataFrame;
  distinct(opts: {
    maintainOrder?: boolean;
    subset?: ColumnSelection;
    keep?: "first" | "last";
  }): LazyDataFrame;
  /**
   * Drop rows with null values from this DataFrame.
   * This method only drops nulls row-wise if any single value of the row is null.
   */
  dropNulls(column: string): LazyDataFrame;
  dropNulls(columns: string[]): LazyDataFrame;
  dropNulls(...columns: string[]): LazyDataFrame;
  /**
   * Explode lists to long format.
   */
  explode(column: ExprOrString): LazyDataFrame;
  explode(columns: ExprOrString[]): LazyDataFrame;
  explode(column: ExprOrString, ...columns: ExprOrString[]): LazyDataFrame;
  /**
   * Fetch is like a collect operation, but it overwrites the number of rows read by every scan
   *
   * Note that the fetch does not guarantee the final number of rows in the DataFrame.
   * Filter, join operations and a lower number of rows available in the scanned file influence
   * the final number of rows.
   * @param numRows - collect 'n' number of rows from data source
   * @param opts
   * @param opts.typeCoercion -Do type coercion optimization.
   * @param opts.predicatePushdown - Do predicate pushdown optimization.
   * @param opts.projectionPushdown - Do projection pushdown optimization.
   * @param opts.simplifyExpression - Run simplify expressions optimization.
   * @param opts.commSubplanElim - Will try to cache branching subplans that occur on self-joins or unions.
   * @param opts.commSubexprElim - Common subexpressions will be cached and reused.
   * @param opts.streaming - Process the query in batches to handle larger-than-memory data.
            If set to `False` (default), the entire query is processed in a single
            batch.

            .. warning::
                Streaming mode is considered **unstable**. It may be changed
                at any point without it being considered a breaking change.
   * 
   */
  fetch(numRows?: number): Promise<DataFrame>;
  fetch(numRows: number, opts: LazyOptions): Promise<DataFrame>;
  /** Behaves the same as fetch, but will perform the actions synchronously */
  fetchSync(numRows?: number): DataFrame;
  fetchSync(numRows: number, opts: LazyOptions): DataFrame;
  /**
   * Fill missing values
   * @param fillValue value to fill the missing values with
   */
  fillNull(fillValue: string | number | Expr): LazyDataFrame;
  /**
   * Filter the rows in the DataFrame based on a predicate expression.
   * @param predicate - Expression that evaluates to a boolean Series.
   * @example
   * ```
   * > lf = pl.DataFrame({
   * >   "foo": [1, 2, 3],
   * >   "bar": [6, 7, 8],
   * >   "ham": ['a', 'b', 'c']
   * > }).lazy()
   * > // Filter on one condition
   * > lf.filter(pl.col("foo").lt(3)).collect()
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
   * ```
   */
  filter(predicate: Expr | string): LazyDataFrame;
  /**
   * Get the first row of the DataFrame.
   */
  first(): DataFrame;
  /**
   * Start a groupby operation.
   */
  groupBy(by: ColumnsOrExpr, maintainOrder?: boolean): LazyGroupBy;
  groupBy(by: ColumnsOrExpr, opts: { maintainOrder: boolean }): LazyGroupBy;

  /**
   * Gets the first `n` rows of the DataFrame. You probably don't want to use this!
   *
   * Consider using the `fetch` operation.
   * The `fetch` operation will truly load the first `n`rows lazily.
   */
  head(length?: number): LazyDataFrame;
  inner(): any;
  /**
   *  __SQL like joins.__
   * @param df - DataFrame to join with.
   * @param options
   * @param options.leftOn - Name(s) of the left join column(s).
   * @param options.rightOn - Name(s) of the right join column(s).
   * @param options.on - Name(s) of the join columns in both DataFrames.
   * @param options.how - Join strategy
   * @param options.suffix - Suffix to append to columns with a duplicate name.
   * @param options.allowParallel - Allow the physical plan to optionally evaluate the computation of both DataFrames up to the join in parallel.
   * @param options.forceParallel - Force the physical plan to evaluate the computation of both DataFrames up to the join in parallel.
   * @see {@link LazyJoinOptions}
   * @example
   * ```
   * >>> const df = pl.DataFrame({
   * >>>     foo: [1, 2, 3],
   * >>>     bar: [6.0, 7.0, 8.0],
   * >>>     ham: ['a', 'b', 'c'],
   * >>>   }).lazy()
   * >>>
   * >>> const otherDF = pl.DataFrame({
   * >>>     apple: ['x', 'y', 'z'],
   * >>>     ham: ['a', 'b', 'd'],
   * >>>   }).lazy();
   * >>> const result = await df.join(otherDF, { on: 'ham', how: 'inner' }).collect();
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
  join(
    other: LazyDataFrame,
    joinOptions: { on: ValueOrArray<string | Expr> } & LazyJoinOptions,
  ): LazyDataFrame;
  join(
    other: LazyDataFrame,
    joinOptions: {
      leftOn: ValueOrArray<string | Expr>;
      rightOn: ValueOrArray<string | Expr>;
    } & LazyJoinOptions,
  ): LazyDataFrame;
  join(
    other: LazyDataFrame,
    options: {
      how: "cross";
      suffix?: string;
      allowParallel?: boolean;
      forceParallel?: boolean;
    },
  ): LazyDataFrame;

  /**
     * Perform an asof join. This is similar to a left-join except that we
     * match on nearest key rather than equal keys.
     *
     * Both DataFrames must be sorted by the asof_join key.
     *
      For each row in the left DataFrame:

        - A "backward" search selects the last row in the right DataFrame whose
          'on' key is less than or equal to the left's key.

        - A "forward" search selects the first row in the right DataFrame whose
          'on' key is greater than or equal to the left's key.

        - A "nearest" search selects the last row in the right DataFrame whose value
          is nearest to the left's key. String keys are not currently supported for a
          nearest search.

      The default is "backward".

      Parameters
      ----------
      @param other DataFrame to join with.
      @param options.leftOn Join column of the left DataFrame.
      @param options.rightOn Join column of the right DataFrame.
      @param options.on Join column of both DataFrames. If set, `leftOn` and `rightOn` should be undefined.
      @param options.byLeft join on these columns before doing asof join
      @param options.byRight join on these columns before doing asof join
      @param options.strategy One of {'forward', 'backward', 'nearest'}
      @param options.suffix Suffix to append to columns with a duplicate name.
      @param options.tolerance
        Numeric tolerance. By setting this the join will only be done if the near keys are within this distance.
        If an asof join is done on columns of dtype "Date", "Datetime" you
        use the following string language:

        - 1ns   *(1 nanosecond)*
        - 1us   *(1 microsecond)*
        - 1ms   *(1 millisecond)*
        - 1s    *(1 second)*
        - 1m    *(1 minute)*
        - 1h    *(1 hour)*
        - 1d    *(1 day)*
        - 1w    *(1 week)*
        - 1mo   *(1 calendar month)*
        - 1y    *(1 calendar year)*
        - 1i    *(1 index count)*

      Or combine them:
        - "3d12h4m25s" # 3 days, 12 hours, 4 minutes, and 25 seconds
      @param options.allowParallel Allow the physical plan to optionally evaluate the computation of both DataFrames up to the join in parallel.
      @param options.forceParallel Force the physical plan to evaluate the computation of both DataFrames up to the join in parallel.


      @example
      ```
      >const gdp = pl.DataFrame({
      ...   date: [
      ...     new Date('2016-01-01'),
      ...     new Date('2017-01-01'),
      ...     new Date('2018-01-01'),
      ...     new Date('2019-01-01'),
      ...   ],  // note record date: Jan 1st (sorted!)
      ...   gdp: [4164, 4411, 4566, 4696],
      ... })
      >const population = pl.DataFrame({
      ...   date: [
      ...     new Date('2016-05-12'),
      ...     new Date('2017-05-12'),
      ...     new Date('2018-05-12'),
      ...     new Date('2019-05-12'),
      ...   ],  // note record date: May 12th (sorted!)
      ...   "population": [82.19, 82.66, 83.12, 83.52],
      ... })
      >population.joinAsof(
      ...   gdp,
      ...   {leftOn:"date", rightOn:"date", strategy:"backward"}
      ... )
        shape: (4, 3)
        ┌─────────────────────┬────────────┬──────┐
        │ date                ┆ population ┆ gdp  │
        │ ---                 ┆ ---        ┆ ---  │
        │ datetime[μs]        ┆ f64        ┆ i64  │
        ╞═════════════════════╪════════════╪══════╡
        │ 2016-05-12 00:00:00 ┆ 82.19      ┆ 4164 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 2017-05-12 00:00:00 ┆ 82.66      ┆ 4411 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 2018-05-12 00:00:00 ┆ 83.12      ┆ 4566 │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 2019-05-12 00:00:00 ┆ 83.52      ┆ 4696 │
        └─────────────────────┴────────────┴──────┘
      ```
     */
  joinAsof(
    other: LazyDataFrame,
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
    },
  ): LazyDataFrame;
  /**
   * Get the last row of the DataFrame.
   */
  last(): LazyDataFrame;
  /**
   * @see {@link head}
   */
  limit(n?: number): LazyDataFrame;
  /**
   * @see {@link DataFrame.max}
   */
  max(): LazyDataFrame;
  /**
   * @see {@link DataFrame.mean}
   */
  mean(): LazyDataFrame;
  /**
   * @see {@link DataFrame.median}
   */
  median(): LazyDataFrame;
  /**
   * @see {@link DataFrame.unpivot}
   * @deprecated *since 0.13.0* use {@link unpivot}
   */
  melt(idVars: ColumnSelection, valueVars: ColumnSelection): LazyDataFrame;
  unpivot(idVars: ColumnSelection, valueVars: ColumnSelection): LazyDataFrame;
  /**
   * @see {@link DataFrame.min}
   */
  min(): LazyDataFrame;
  /**
   * @see {@link DataFrame.quantile}
   */
  quantile(quantile: number): LazyDataFrame;
  /**
   * @see {@link DataFrame.rename}
   */
  rename(mapping: Record<string, string>): LazyDataFrame;
  /**
   * Reverse the DataFrame.
   */
  reverse(): LazyDataFrame;
  /**
   * @see {@link DataFrame.select}
   */
  select(column: ExprOrString): LazyDataFrame;
  select(columns: ExprOrString[]): LazyDataFrame;
  select(...columns: ExprOrString[]): LazyDataFrame;
  /**
   * @see {@link DataFrame.shift}
   */
  shift(periods: number): LazyDataFrame;
  shift(opts: { periods: number }): LazyDataFrame;
  /**
   * @see {@link DataFrame.shiftAndFill}
   */
  shiftAndFill(n: number, fillValue: number): LazyDataFrame;
  shiftAndFill(opts: {
    n: number;
    fillValue: number;
  }): LazyDataFrame;
  /**
   * @see {@link DataFrame.slice}
   */
  slice(offset: number, length: number): LazyDataFrame;
  slice(opts: { offset: number; length: number }): LazyDataFrame;
  /**
   * @see {@link DataFrame.sort}
   */
  sort(
    by: ColumnsOrExpr,
    descending?: ValueOrArray<boolean>,
    nulls_last?: boolean,
    maintain_order?: boolean,
  ): LazyDataFrame;
  sort(opts: {
    by: ColumnsOrExpr;
    descending?: ValueOrArray<boolean>;
    nulls_last?: boolean;
    maintain_order?: boolean;
  }): LazyDataFrame;
  /**
   * @see {@link DataFrame.std}
   */
  std(): LazyDataFrame;
  /**
   * Aggregate the columns in the DataFrame to their sum value.
   */
  sum(): LazyDataFrame;
  /**
   * Get the last `n` rows of the DataFrame.
   * @see {@link DataFrame.tail}
   */
  tail(length?: number): LazyDataFrame;
  /**
   * compatibility with `JSON.stringify`
   */
  toJSON(): string;
  /**
   * Drop duplicate rows from this DataFrame.
   * Note that this fails if there is a column of type `List` in the DataFrame.
   * @param maintainOrder
   * @param subset - subset to drop duplicates for
   * @param keep "first" | "last"
   */
  unique(
    maintainOrder?: boolean,
    subset?: ColumnSelection,
    keep?: "first" | "last",
  ): LazyDataFrame;
  unique(opts: {
    maintainOrder?: boolean;
    subset?: ColumnSelection;
    keep?: "first" | "last";
  }): LazyDataFrame;
  /**
   * Aggregate the columns in the DataFrame to their variance value.
   */
  var(): LazyDataFrame;
  /**
   * Add or overwrite column in a DataFrame.
   * @param expr - Expression that evaluates to column.
   */
  withColumn(expr: Expr): LazyDataFrame;
  /**
   * Add or overwrite multiple columns in a DataFrame.
   * @param exprs - List of Expressions that evaluate to columns.
   *
   */
  withColumns(exprs: (Expr | Series)[]): LazyDataFrame;
  withColumns(...exprs: (Expr | Series)[]): LazyDataFrame;
  withColumnRenamed(existing: string, replacement: string): LazyDataFrame;
  /**
   * Add a column at index 0 that counts the rows.
   * @see {@link DataFrame.withRowCount}
   */
  withRowCount(): LazyDataFrame;
  /***
  *
  * Evaluate the query in streaming mode and write to a CSV file.

    .. warning::
        Streaming mode is considered **unstable**. It may be changed
        at any point without it being considered a breaking change.

    This allows streaming results that are larger than RAM to be written to disk.

    Parameters
    ----------
    @param path - File path to which the file should be written.
    @param includeBom - Whether to include UTF-8 BOM in the CSV output.
    @param includeHeader - Whether to include header in the CSV output.
    @param separator - Separate CSV fields with this symbol.
    @param lineTerminator - String used to end each row.
    @param quoteChar - Byte to use as quoting character.
    @param batchSize - Number of rows that will be processed per thread. Default - 1024
    @param datetimeFormat - A format string, with the specifiers defined by the
        `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
        Rust crate. If no format specified, the default fractional-second
        precision is inferred from the maximum timeunit found in the frame's
        Datetime cols (if any).
    @param dateFormat - A format string, with the specifiers defined by the
        `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
        Rust crate.
    @param timeFormat A format string, with the specifiers defined by the
        `chrono <https://docs.rs/chrono/latest/chrono/format/strftime/index.html>`_
        Rust crate.
    @param floatPrecision - Number of decimal places to write, applied to both `Float32` and `Float64` datatypes.
    @param nullValue - A string representing null values (defaulting to the empty string).
    @param quoteStyle - Determines the quoting strategy used. : {'necessary', 'always', 'non_numeric', 'never'}
        - necessary (default): This puts quotes around fields only when necessary.
          They are necessary when fields contain a quote,
          delimiter or record terminator.
          Quotes are also necessary when writing an empty record
          (which is indistinguishable from a record with one empty field).
          This is the default.
        - always: This puts quotes around every field. Always.
        - never: This never puts quotes around fields, even if that results in
          invalid CSV data (e.g.: by not quoting strings containing the
          separator).
        - non_numeric: This puts quotes around all fields that are non-numeric.
          Namely, when writing a field that does not parse as a valid float
          or integer, then quotes will be used even if they aren`t strictly
          necessary.
    @param maintainOrder - Maintain the order in which data is processed.
        Setting this to `False` will  be slightly faster.

    Examples
    --------
    >>> const lf = pl.scanCsv("/path/to/my_larger_than_ram_file.csv")
    >>> lf.sinkCsv("out.csv")
  */

  sinkCSV(path: string, options?: SinkCsvOptions): void;

  /***
   *
   * Evaluate the query in streaming mode and write to a Parquet file.

    .. warning::
        Streaming mode is considered **unstable**. It may be changed
        at any point without it being considered a breaking change.

    This allows streaming results that are larger than RAM to be written to disk.

    Parameters
    ----------
    @param path - File path to which the file should be written.
    @param compression : {'lz4', 'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'zstd'}
        Choose "zstd" for good compression performance. (default)
        Choose "lz4" for fast compression/decompression.
        Choose "snappy" for more backwards compatibility guarantees
        when you deal with older parquet readers.
    @param compressionLevel - The level of compression to use. Higher compression means smaller files on disk.
        - "gzip" : min-level: 0, max-level: 10.
        - "brotli" : min-level: 0, max-level: 11.
        - "zstd" : min-level: 1, max-level: 22.
    @param statistics - Write statistics to the parquet headers. This requires extra compute. Default - false
    @param rowGroupSize - Size of the row groups in number of rows.
        If None (default), the chunks of the `DataFrame` are
        used. Writing in smaller chunks may reduce memory pressure and improve
        writing speeds.
    @param dataPagesizeLimit - Size limit of individual data pages.
        If not set defaults to 1024 * 1024 bytes
    @param maintainOrder - Maintain the order in which data is processed. Default -> true
        Setting this to `False` will  be slightly faster.
    @param typeCoercion - Do type coercion optimization. Default -> true
    @param predicatePushdown - Do predicate pushdown optimization. Default -> true
    @param projectionPushdown - Do projection pushdown optimization. Default -> true
    @param simplifyExpression - Run simplify expressions optimization. Default -> true
    @param slicePushdown - Slice pushdown optimization. Default -> true
    @param noOptimization - Turn off (certain) optimizations. Default -> false

    Examples
    --------
    >>> const lf = pl.scanCsv("/path/to/my_larger_than_ram_file.csv")  # doctest: +SKIP
    >>> lf.sinkParquet("out.parquet")  # doctest: +SKIP
   */
  sinkParquet(path: string, options?: SinkParquetOptions): void;
}

const prepareGroupbyInputs = (by) => {
  if (Array.isArray(by)) {
    const newBy: any = [];
    for (let e of by) {
      if (typeof e === "string") {
        e = pli.col(e);
      }
      newBy.push(e);
    }

    return newBy;
  }
  if (typeof by === "string") {
    return [pli.col(by)];
  }
  if (Expr.isExpr(by)) {
    return [by._expr];
  }
  return [];
};

/** @ignore */
export const _LazyDataFrame = (_ldf: any): LazyDataFrame => {
  const unwrap = (method: string, ...args: any[]) => {
    return _ldf[method as any](...args);
  };
  const wrap = (method, ...args): LazyDataFrame => {
    return _LazyDataFrame(unwrap(method, ...args));
  };

  return {
    _ldf,
    [inspect]() {
      return _ldf.describeOptimizedPlan();
    },
    get [Symbol.toStringTag]() {
      return "LazyDataFrame";
    },

    get columns() {
      return _ldf.columns;
    },
    describePlan() {
      return _ldf.describePlan();
    },
    describeOptimizedPlan() {
      return _ldf.describeOptimizedPlan();
    },
    cache() {
      return _LazyDataFrame(_ldf.cache());
    },
    clone() {
      return _LazyDataFrame((_ldf as any).clone());
    },
    collectSync() {
      return _DataFrame(_ldf.collectSync());
    },
    collect(opts?) {
      if (opts?.noOptimization) {
        opts.predicatePushdown = false;
        opts.projectionPushdown = false;
        opts.slicePushdown = false;
        opts.commSubplanElim = false;
        opts.commSubexprElim = false;
      }

      if (opts?.streaming) opts.commSubplanElim = false;

      if (opts) {
        _ldf = _ldf.optimizationToggle(
          opts.typeCoercion,
          opts.predicatePushdown,
          opts.projectionPushdown,
          opts.simplifyExpression,
          opts.slicePushdown,
          opts.commSubplanElim,
          opts.commSubexprElim,
          opts.streaming,
        );
      }

      return _ldf.collect().then(_DataFrame);
    },
    drop(...cols) {
      return _LazyDataFrame(_ldf.dropColumns(cols.flat(2)));
    },
    distinct(...args: any[]) {
      return _LazyDataFrame((_ldf.unique as any)(...args));
    },
    unique(opts: any = false, subset?, keep = "first") {
      const defaultOptions = {
        maintainOrder: false,
        keep: "first",
      };

      if (typeof opts === "boolean") {
        const o = { ...defaultOptions, maintainOrder: opts, subset, keep };

        return _LazyDataFrame(
          _ldf.unique(o.maintainOrder, o?.subset?.flat(2), o.keep),
        );
      }

      if (opts.subset) {
        opts.subset = [opts.subset].flat(3);
      }
      const o = { ...defaultOptions, ...opts };

      return _LazyDataFrame(_ldf.unique(o.maintainOrder, o.subset, o.keep));
    },
    dropNulls(...subset) {
      if (subset.length) {
        return wrap("dropNulls", subset.flat(2));
      }
      return wrap("dropNulls");
    },
    explode(...columns) {
      if (!columns.length) {
        const cols = selectionToExprList(_ldf.columns, false);

        return wrap("explode", cols);
      }
      const column = selectionToExprList(columns, false);

      return wrap("explode", column);
    },
    fetchSync(numRows, opts?) {
      if (opts?.noOptimization) {
        opts.predicatePushdown = false;
        opts.projectionPushdown = false;
        opts.slicePushdown = false;
        opts.commSubplanElim = false;
        opts.commSubexprElim = false;
      }
      if (opts?.streaming) opts.commSubplanElim = false;
      if (opts) {
        _ldf = _ldf.optimizationToggle(
          opts.typeCoercion,
          opts.predicatePushdown,
          opts.projectionPushdown,
          opts.simplifyExpr,
          opts.slicePushdown,
          opts.commSubplanElim,
          opts.commSubexprElim,
          opts.streaming,
        );
      }

      return _DataFrame(_ldf.fetchSync(numRows));
    },
    fetch(numRows, opts?) {
      if (opts?.noOptimization) {
        opts.predicatePushdown = false;
        opts.projectionPushdown = false;
        opts.slicePushdown = false;
        opts.commSubplanElim = false;
        opts.commSubexprElim = false;
      }
      if (opts?.streaming) opts.commSubplanElim = false;
      if (opts) {
        _ldf = _ldf.optimizationToggle(
          opts.typeCoercion,
          opts.predicatePushdown,
          opts.projectionPushdown,
          opts.simplifyExpr,
          opts.slicePushdown,
          opts.commSubplanElim,
          opts.commSubexprElim,
          opts.streaming,
        );
      }

      return _ldf.fetch(numRows).then(_DataFrame);
    },
    first() {
      return this.fetchSync(1);
    },
    fillNull(exprOrValue) {
      const fillValue = exprToLitOrExpr(exprOrValue)._expr;

      return _LazyDataFrame(_ldf.fillNull(fillValue));
    },
    filter(exprOrValue) {
      const predicate = exprToLitOrExpr(exprOrValue, false)._expr;

      return _LazyDataFrame(_ldf.filter(predicate));
    },
    groupBy(opt, maintainOrder: any = true): LazyGroupBy {
      if (opt?.by !== undefined) {
        const by = selectionToExprList([opt.by], false);

        return _LazyGroupBy(_ldf.groupby(by, opt.maintainOrder));
      }
      const by = selectionToExprList([opt], false);

      return _LazyGroupBy(_ldf.groupby(by, maintainOrder));
    },
    groupByRolling({ indexColumn, by, period, offset, closed, check_sorted }) {
      offset = offset ?? `-${period}`;
      closed = closed ?? "right";
      by = prepareGroupbyInputs(by);
      check_sorted = check_sorted ?? false;
      const lgb = _ldf.groupbyRolling(
        pli.col(indexColumn),
        period,
        offset,
        closed,
        by,
        check_sorted,
      );

      return _LazyGroupBy(lgb);
    },
    groupByDynamic({
      indexColumn,
      every,
      period,
      offset,
      includeBoundaries,
      closed,
      by,
      start_by,
      check_sorted,
    }) {
      period = period ?? every;
      offset = offset ?? `-${period}`;
      closed = closed ?? "right";
      by = prepareGroupbyInputs(by);
      includeBoundaries = includeBoundaries ?? false;
      start_by = start_by ?? "monday";
      check_sorted = check_sorted ?? false;

      const lgb = _ldf.groupbyDynamic(
        pli.col(indexColumn),
        every,
        period,
        offset,
        includeBoundaries,
        closed,
        by,
        start_by,
        check_sorted,
      );

      return _LazyGroupBy(lgb);
    },
    head(len = 5) {
      return _LazyDataFrame(_ldf.slice(0, len));
    },
    inner() {
      return _ldf;
    },
    join(df, options) {
      options = {
        how: "inner",
        suffix: "right",
        allowParallel: true,
        forceParallel: false,
        ...options,
      };
      const { how, suffix, allowParallel, forceParallel } = options;
      if (how === "cross") {
        return _LazyDataFrame(
          _ldf.join(
            df._ldf,
            [],
            [],
            allowParallel,
            forceParallel,
            how,
            suffix,
            [],
            [],
          ),
        );
      }
      let leftOn;
      let rightOn;
      if (options.on) {
        const on = selectionToExprList(options.on, false);
        leftOn = on;
        rightOn = on;
      } else if (
        (options.leftOn && !options.rightOn) ||
        (options.rightOn && !options.leftOn)
      ) {
        throw new TypeError(
          "You should pass the column to join on as an argument.",
        );
      } else {
        leftOn = selectionToExprList(options.leftOn, false);
        rightOn = selectionToExprList(options.rightOn, false);
      }

      const ldf = (_ldf.join as any)(
        df._ldf,
        leftOn,
        rightOn,
        allowParallel,
        forceParallel,
        how,
        suffix,
        [],
        [],
      );

      return _LazyDataFrame(ldf);
    },
    joinAsof(other, options) {
      options = {
        suffix: "_right",
        allowParallel: true,
        forceParallel: false,
        strategy: "backward",
        ...options,
      };
      const { suffix, strategy, allowParallel, forceParallel } = options;
      let leftOn: string | undefined;
      let rightOn: string | undefined;
      if (!other?._ldf) {
        throw new TypeError("Expected a 'lazyFrame' as join table");
      }
      if (options.on) {
        leftOn = rightOn = options.on;
      } else if (
        (options.leftOn && !options.rightOn) ||
        (options.rightOn && !options.leftOn)
      ) {
        throw new TypeError(
          "You should pass the column to join on as an argument.",
        );
      } else {
        leftOn = options.leftOn;
        rightOn = options.rightOn;
      }
      let byLeft: string[] | number | undefined;
      if (typeof options.byLeft === "string") {
        byLeft = [options.byLeft];
      } else if (Array.isArray(options.byLeft)) {
        byLeft = options.byLeft;
      }
      let byRight: string[] | number | undefined;
      if (typeof options.byRight === "string") {
        byRight = [options.byRight];
      } else if (Array.isArray(options.byRight)) {
        byRight = options.byRight;
      }

      if (typeof options.by === "string") {
        byLeft = byRight = [options.by];
      } else if (Array.isArray(options.by)) {
        byLeft = byRight = options.by;
      }
      let toleranceStr: string | undefined;
      let toleranceNum: number | undefined;
      if (typeof options.tolerance === "string") {
        toleranceStr = options.tolerance;
      } else {
        toleranceNum = options.tolerance;
      }

      const ldf = _ldf.joinAsof(
        other._ldf,
        pli.col(leftOn),
        pli.col(rightOn),
        byLeft,
        byRight,
        allowParallel,
        forceParallel,
        suffix,
        strategy,
        toleranceNum,
        toleranceStr,
      );

      return _LazyDataFrame(ldf);
    },
    last() {
      return _LazyDataFrame(_ldf.tail(1));
    },
    limit(len = 5) {
      return _LazyDataFrame(_ldf.slice(0, len));
    },
    max() {
      return _LazyDataFrame(_ldf.max());
    },
    mean() {
      return _LazyDataFrame(_ldf.mean());
    },
    median() {
      return _LazyDataFrame(_ldf.median());
    },
    melt(ids, values) {
      return _LazyDataFrame(
        _ldf.unpivot(columnOrColumnsStrict(ids), columnOrColumnsStrict(values)),
      );
    },
    unpivot(ids, values) {
      return _LazyDataFrame(
        _ldf.unpivot(columnOrColumnsStrict(ids), columnOrColumnsStrict(values)),
      );
    },
    min() {
      return _LazyDataFrame(_ldf.min());
    },
    quantile(quantile, interpolation = "nearest") {
      return _LazyDataFrame(_ldf.quantile(quantile, interpolation));
    },
    rename(mapping) {
      const existing = Object.keys(mapping);
      const replacements = Object.values(mapping);

      return _LazyDataFrame(_ldf.rename(existing, replacements));
    },
    reverse() {
      return _LazyDataFrame(_ldf.reverse());
    },
    select(...exprs) {
      const selections = selectionToExprList(exprs, false);

      return _LazyDataFrame(_ldf.select(selections));
    },
    shift(periods) {
      return _LazyDataFrame(_ldf.shift(periods));
    },
    shiftAndFill(opts: any, fillValue?: number | undefined) {
      if (typeof opts === "number") {
        return _LazyDataFrame(_ldf.shiftAndFill(opts, fillValue));
      }
      return _LazyDataFrame(_ldf.shiftAndFill(opts?.n, opts?.fillValue));
    },
    slice(opt, len?) {
      if (opt?.offset !== undefined) {
        return _LazyDataFrame(_ldf.slice(opt.offset, opt.length));
      }
      return _LazyDataFrame(_ldf.slice(opt, len));
    },
    sort(arg, descending = false, nulls_last = false, maintain_order = false) {
      if (arg?.by !== undefined) {
        return this.sort(
          arg.by,
          arg.descending,
          arg.nulls_last,
          arg.maintain_order,
        );
      }
      if (typeof arg === "string") {
        return wrap("sort", arg, descending, nulls_last, maintain_order);
      }
      const by = selectionToExprList(arg, false);
      return wrap("sortByExprs", by, descending, nulls_last, maintain_order);
    },
    std() {
      return _LazyDataFrame(_ldf.std());
    },
    sum() {
      return _LazyDataFrame(_ldf.sum());
    },
    var() {
      return _LazyDataFrame(_ldf.var());
    },
    tail(length = 5) {
      return _LazyDataFrame(_ldf.tail(length));
    },
    toJSON(...args: any[]) {
      // this is passed by `JSON.stringify` when calling `toJSON()`
      if (args[0] === "") {
        return JSON.parse(_ldf.serialize("json").toString());
      }

      return _ldf.serialize("json").toString();
    },
    serialize(format) {
      return _ldf.serialize(format);
    },
    withColumn(expr) {
      return _LazyDataFrame(_ldf.withColumn(expr._expr));
    },
    withColumns(...columns) {
      const exprs = selectionToExprList(columns, false);

      return _LazyDataFrame(_ldf.withColumns(exprs));
    },
    withColumnRenamed(existing, replacement) {
      return _LazyDataFrame(_ldf.rename([existing], [replacement]));
    },
    withRowCount(name = "row_nr") {
      return _LazyDataFrame(_ldf.withRowCount(name));
    },
    sinkCSV(path, options: SinkCsvOptions = {}) {
      options.maintainOrder = options.maintainOrder ?? false;
      _ldf.sinkCsv(path, options);
    },
    sinkParquet(path: string, options: SinkParquetOptions = {}) {
      options.compression = options.compression ?? "zstd";
      options.statistics = options.statistics ?? false;
      _ldf.sinkParquet(path, options);
    },
  };
};

export interface LazyDataFrameConstructor extends Deserialize<LazyDataFrame> {
  fromExternal(external: any): LazyDataFrame;
  isLazyDataFrame(arg: any): arg is LazyDataFrame;
}

const isLazyDataFrame = (anyVal: any): anyVal is LazyDataFrame =>
  anyVal?.[Symbol.toStringTag] === "LazyDataFrame";

/** @ignore */
export const LazyDataFrame: LazyDataFrameConstructor = Object.assign(
  _LazyDataFrame,
  {
    deserialize: (buf, fmt) =>
      _LazyDataFrame(pli.JsLazyFrame.deserialize(buf, fmt)),
    fromExternal(external) {
      return _LazyDataFrame(pli.JsLazyFrame.cloneExternal(external));
    },
    isLazyDataFrame,
  },
);
