import { Expr, _Expr, exprToLitOrExpr } from "./expr";
import { DataType } from "../datatypes";
import { Series } from "../series";
import { DataFrame } from "../dataframe";
import { type ExprOrString, range, selectionToExprList } from "../utils";
import pli from "../internals/polars_internal";

/**
 * __A column in a DataFrame.__
 * Can be used to select:
 *   * a single column by name
 *   * all columns by using a wildcard `"*"`
 *   * column by regular expression if the regex starts with `^` and ends with `$`
 * @param col
 * @example
 * ```
 * > df = pl.DataFrame({
 * > "ham": [1, 2, 3],
 * > "hamburger": [11, 22, 33],
 * > "foo": [3, 2, 1]})
 * > df.select(col("foo"))
 * shape: (3, 1)
 * ╭─────╮
 * │ foo │
 * │ --- │
 * │ i64 │
 * ╞═════╡
 * │ 3   │
 * ├╌╌╌╌╌┤
 * │ 2   │
 * ├╌╌╌╌╌┤
 * │ 1   │
 * ╰─────╯
 * > df.select(col("*"))
 * shape: (3, 3)
 * ╭─────┬───────────┬─────╮
 * │ ham ┆ hamburger ┆ foo │
 * │ --- ┆ ---       ┆ --- │
 * │ i64 ┆ i64       ┆ i64 │
 * ╞═════╪═══════════╪═════╡
 * │ 1   ┆ 11        ┆ 3   │
 * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 2   ┆ 22        ┆ 2   │
 * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 3   ┆ 33        ┆ 1   │
 * ╰─────┴───────────┴─────╯
 * > df.select(col("^ham.*$"))
 * shape: (3, 2)
 * ╭─────┬───────────╮
 * │ ham ┆ hamburger │
 * │ --- ┆ ---       │
 * │ i64 ┆ i64       │
 * ╞═════╪═══════════╡
 * │ 1   ┆ 11        │
 * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
 * │ 2   ┆ 22        │
 * ├╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
 * │ 3   ┆ 33        │
 * ╰─────┴───────────╯
 * > df.select(col("*").exclude("ham"))
 * shape: (3, 2)
 * ╭───────────┬─────╮
 * │ hamburger ┆ foo │
 * │ ---       ┆ --- │
 * │ i64       ┆ i64 │
 * ╞═══════════╪═════╡
 * │ 11        ┆ 3   │
 * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 22        ┆ 2   │
 * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 33        ┆ 1   │
 * ╰───────────┴─────╯
 * > df.select(col(["hamburger", "foo"])
 * shape: (3, 2)
 * ╭───────────┬─────╮
 * │ hamburger ┆ foo │
 * │ ---       ┆ --- │
 * │ i64       ┆ i64 │
 * ╞═══════════╪═════╡
 * │ 11        ┆ 3   │
 * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 22        ┆ 2   │
 * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 33        ┆ 1   │
 * ╰───────────┴─────╯
 * > df.select(col(pl.Series(["hamburger", "foo"]))
 * shape: (3, 2)
 * ╭───────────┬─────╮
 * │ hamburger ┆ foo │
 * │ ---       ┆ --- │
 * │ i64       ┆ i64 │
 * ╞═══════════╪═════╡
 * │ 11        ┆ 3   │
 * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 22        ┆ 2   │
 * ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┤
 * │ 33        ┆ 1   │
 * ╰───────────┴─────╯
 * ```
 */
export function col(col: string | string[] | Series | DataType): Expr {
  if (Series.isSeries(col)) {
    col = col.toArray() as string[];
  }
  if (Array.isArray(col)) {
    return _Expr(pli.cols(col));
  }
  if (typeof col === "object" && Object.values(col)[0] === "DataType") {
    return _Expr(pli.dtypeCols([col]));
  }
  return _Expr(pli.col(col));
}

export function cols(col: string | string[]): Expr;
export function cols(col: string, ...cols2: string[]): Expr;
export function cols(...cols): Expr {
  return col(cols.flat());
}

/**
 * Select nth column index in a DataFrame.
 * @param n - Column index to select, starting at 0.
 * @example
 * ```
 * > df = pl.DataFrame({
 * > "ham": [1, 2, 3],
 * > "hamburger": [11, 22, 33],
 * > "foo": [3, 2, 1]})
 * > df.select(nth(2))
 * shape: (3, 1)
 * ╭─────╮
 * │ foo │
 * │ --- │
 * │ i64 │
 * ╞═════╡
 * │ 3   │
 * ├╌╌╌╌╌┤
 * │ 2   │
 * ├╌╌╌╌╌┤
 * │ 1   │
 * ╰─────╯
 * ```
 */
export function nth(n: number): Expr {
  return _Expr(pli.nth(n));
}

export function lit(value: any): Expr {
  if (Array.isArray(value)) {
    value = Series(value);
  }
  if (Series.isSeries(value)) {
    return _Expr(pli.lit(value.inner()));
  }

  return _Expr(pli.lit(value));
}

// // ----------
// // Helper Functions
// // ------

/**
 * __Create a range expression.__
 * ___
 *
 * This can be used in a `select`, `with_column` etc.
 * Be sure that the range size is equal to the DataFrame you are collecting.
 * @param low - Lower bound of range.
 * @param high - Upper bound of range.
 * @param step - Step size of the range
 * @param eager - If eager evaluation is `true`, a Series is returned instead of an Expr
 * @example
 * ```
 * > df.lazy()
 * >   .filter(pl.col("foo").lt(pl.intRange(0, 100)))
 * >   .collect()
 * ```
 */
export function intRange<T>(opts: {
  low: any;
  high: any;
  step: number;
  dtype: DataType;
  eager?: boolean;
});
export function intRange(
  low: any,
  high?: any,
  step?: number,
  dtype?: DataType,
  eager?: true,
): Series;
export function intRange(
  low: any,
  high?: any,
  step?: number,
  dtype?: DataType,
  eager?: false,
): Expr;
export function intRange(
  opts: any,
  high?,
  step = 1,
  dtype = DataType.Int64,
  eager?,
): Series | Expr {
  if (typeof opts?.low === "number") {
    return intRange(opts.low, opts.high, opts.step, opts.dtype, opts.eager);
  }
  // if expression like pl.len() passed
  if (high === undefined || high === null) {
    high = opts;
    opts = 0;
  }
  const low = exprToLitOrExpr(opts, false);
  high = exprToLitOrExpr(high, false);
  if (eager) {
    const df = DataFrame({ a: [1] });

    return df
      .select(intRange(low, high, step).alias("intRange") as any)
      .getColumn("intRange") as any;
  }
  return _Expr(pli.intRange(low, high, step, dtype));
}

/***
 * Generate a range of integers for each row of the input columns.
 * @param start - Lower bound of the range (inclusive).
 * @param end - Upper bound of the range (exclusive).
 * @param step - Step size of the range.
 * @param eager - Evaluate immediately and return a ``Series``. If set to ``False`` (default), return an expression instead.
 * @return - Column of data type ``List(dtype)``.
 *  * @example
 * ```
 * const df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
 * const result = df.select(pl.intRanges("a", "b"));
 * ```
 */

export function intRanges(
  start: any,
  end: any,
  step?: number,
  dtype?: DataType,
  eager?: false,
): Expr;
export function intRanges(
  start: any,
  end: any,
  step?: number,
  dtype?: DataType,
  eager?: true,
): Series;
export function intRanges(
  start: any,
  end: any,
  step: any = 1,
  dtype: DataType = DataType.Int64,
  eager?: boolean,
): Series | Expr {
  start = exprToLitOrExpr(start, false);
  end = exprToLitOrExpr(end, false);
  step = exprToLitOrExpr(step, false);

  if (eager) {
    const df = DataFrame({ a: [1] });
    return df
      .select(intRanges(start, end, step, dtype).alias("intRanges") as any)
      .getColumn("intRanges") as any;
  }
  return _Expr(pli.intRanges(start, end, step, dtype));
}

/**  Alias for `pl.col("*")` */
export function all(): Expr {
  return col("*");
}
/**
 * Return the row indices that would sort the columns.
 * @param exprs Column(s) to arg sort by. Accepts expression input.
 * @param *more_exprs Additional columns to arg sort by, specified as positional arguments.
 * @param descending Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans.
 * @example
 * ```
 * const df = pl.DataFrame({"a": [0, 1, 1, 0], "b": [3, 2, 3, 2],});
 * df.select(pl.argSortBy(pl.col("a")));
 * shape: (4, 1)
 * ┌─────┐
 * │ a   │
 * │ --- │
 * │ u32 │
 * ╞═════╡
 * │ 0   │
 * │ 3   │
 * │ 1   │
 * │ 2   │
 * └─────┘
 * ```
 */
export function argSortBy(
  exprs: Expr[] | string[],
  descending: boolean | boolean[] = false,
): Expr {
  if (!Array.isArray(descending)) {
    descending = Array.from(
      { length: exprs.length },
      () => descending,
    ) as boolean[];
  }
  const by = selectionToExprList(exprs);

  return _Expr(pli.argSortBy(by, descending as boolean | boolean[]));
}
/** Alias for mean. @see {@link mean} */
export function avg(column: string): Expr;
export function avg(column: Series): number;
export function avg(column: Series | string): number | Expr {
  return mean(column as any);
}

/**
 * Concat the arrays in a Series dtype List in linear time.
 * @param exprs Columns to concat into a List Series
 */
export function concatList(exprs: ExprOrString[]): Expr;
export function concatList(expr: ExprOrString, ...exprs: ExprOrString[]): Expr;
export function concatList(
  expr: ExprOrString,
  expr2: ExprOrString,
  ...exprs: ExprOrString[]
): Expr;
export function concatList(...exprs): Expr {
  const items = selectionToExprList(exprs as any, false);

  return (Expr as any)(pli.concatLst(items));
}

/** Concat Utf8 Series in linear time. Non utf8 columns are cast to utf8. */
export function concatString(opts: {
  exprs: ExprOrString[];
  sep: string;
  ignoreNulls?: boolean;
});
export function concatString(
  exprs: ExprOrString[],
  sep?: string,
  ignoreNulls?: boolean,
);
export function concatString(opts, sep = ",", ignoreNulls = true) {
  if (opts?.exprs) {
    return concatString(opts.exprs, opts.sep, opts.ignoreNulls);
  }
  const items = selectionToExprList(opts as any, false);

  return (Expr as any)(pli.concatStr(items, sep, ignoreNulls));
}

/** Count the number of values in this column. */
export function count(column: string): Expr;
export function count(column: Series): number;
export function count(column) {
  if (Series.isSeries(column)) {
    return column.len();
  }
  return col(column).count();
}

/** Compute the covariance between two columns/ expressions. */
export function cov(a: ExprOrString, b: ExprOrString, ddof = 1): Expr {
  a = exprToLitOrExpr(a, false);
  b = exprToLitOrExpr(b, false);

  return _Expr(pli.cov(a, b, ddof));
}
/**
 * Exclude certain columns from a wildcard expression.
 *
 * Syntactic sugar for:
 * ```
 * > pl.col("*").exclude(columns)
 * ```
 */
export function exclude(columns: string[] | string): Expr;
export function exclude(col: string, ...cols: string[]): Expr;
export function exclude(...columns): Expr {
  return col("*").exclude(columns as any);
}

/** Get the first value. */
export function first(): Expr;
export function first(column: string): Expr;
export function first<T>(column: Series): T;
export function first<T>(column?: string | Series): Expr | T {
  if (!column) {
    return _Expr(pli.first());
  }
  if (Series.isSeries(column)) {
    if (column.length) {
      return column.get(0);
    }
    throw new RangeError(
      "The series is empty, so no first value can be returned.",
    );
  }
  return col(column).first();
}

/**
 * String format utility for expressions
 * Note: strings will be interpolated as `col(<value>)`. if you want a literal string, use `lit(<value>)`
 * @example
 * ```
 * > df = pl.DataFrame({
 * ...   "a": ["a", "b", "c"],
 * ...   "b": [1, 2, 3],
 * ... })
 * > df.select(
 * ...   pl.format("foo_{}_bar_{}", pl.col("a"), "b").alias("fmt"),
 * ... )
 * shape: (3, 1)
 * ┌─────────────┐
 * │ fmt         │
 * │ ---         │
 * │ str         │
 * ╞═════════════╡
 * │ foo_a_bar_1 │
 * ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
 * │ foo_b_bar_2 │
 * ├╌╌╌╌╌╌╌╌╌╌╌╌╌┤
 * │ foo_c_bar_3 │
 * └─────────────┘
 *
 * // You can use format as tag function as well
 * > pl.format("foo_{}_bar_{}", pl.col("a"), "b") // is the same as
 * > pl.format`foo_${pl.col("a")}_bar_${"b"}`
 * ```
 */
export function format(
  strings: string | TemplateStringsArray,
  ...expr: ExprOrString[]
): Expr {
  if (typeof strings === "string") {
    const s = strings.split("{}");
    if (s.length - 1 !== expr.length) {
      throw new RangeError(
        "number of placeholders should equal the number of arguments",
      );
    }

    return format(s as any, ...expr);
  }
  const d = range(0, Math.max(strings.length, expr.length))
    .flatMap((i) => {
      const sVal = strings[i] ? lit(strings[i]) : [];
      const exprVal = expr[i] ? exprToLitOrExpr(expr[i], false) : [];

      return [sVal, exprVal];
    })
    .flat();

  return concatString(d, "");
}

/** Syntactic sugar for `pl.col(column).aggGroups()` */
export function groups(column: string): Expr {
  return col(column).aggGroups();
}

/** Get the first n rows of an Expression. */
export function head(column: ExprOrString, n?: number): Expr;
export function head(column: Series, n?: number): Series;
export function head(column: Series | ExprOrString, n?): Series | Expr {
  if (Series.isSeries(column)) {
    return column.head(n);
  }
  return exprToLitOrExpr(column, false).head(n);
}
/** Return the number of elements in the column. */
export function len(): any {
  return _Expr(pli.len());
}
/** Get the last value. */
export function last(column: ExprOrString | Series): any {
  if (Series.isSeries(column)) {
    if (column.length) {
      return column.get(-1);
    }
    throw new RangeError(
      "The series is empty, so no last value can be returned.",
    );
  }
  return exprToLitOrExpr(column, false).last();
}

/** Get the mean value. */
export function mean(column: ExprOrString): Expr;
export function mean(column: Series): number;
export function mean(column: Series | ExprOrString): number | Expr {
  if (Series.isSeries(column)) {
    return column.mean();
  }

  return exprToLitOrExpr(column, false).mean();
}

/** Get the median value. */
export function median(column: ExprOrString): Expr;
export function median(column: Series): number;
export function median(column: Series | ExprOrString): number | Expr {
  if (Series.isSeries(column)) {
    return column.median();
  }

  return exprToLitOrExpr(column, false).median();
}

/** Count unique values. */
export function nUnique(column: ExprOrString): Expr;
export function nUnique(column: Series): number;
export function nUnique(column: Series | ExprOrString): number | Expr {
  if (Series.isSeries(column)) {
    return column.nUnique();
  }

  return exprToLitOrExpr(column, false).nUnique();
}

/** Compute the pearson's correlation between two columns. */
export function pearsonCorr(a: ExprOrString, b: ExprOrString): Expr {
  a = exprToLitOrExpr(a, false);
  b = exprToLitOrExpr(b, false);

  return _Expr(pli.pearsonCorr(a, b));
}

/** Get the quantile */
export function quantile(column: ExprOrString, q: number): Expr;
export function quantile(column: Series, q: number): number;
export function quantile(column, q) {
  if (Series.isSeries(column)) {
    return column.quantile(q);
  }

  return exprToLitOrExpr(column, false).quantile(q);
}

/**
 * __Run polars expressions without a context.__
 *
 * This is syntactic sugar for running `df.select` on an empty DataFrame.
 */
export function select(expr: ExprOrString, ...exprs: ExprOrString[]) {
  return DataFrame({}).select(expr, ...exprs);
}

/** Compute the spearman rank correlation between two columns. */
export function spearmanRankCorr(a: ExprOrString, b: ExprOrString): Expr {
  a = exprToLitOrExpr(a, false);
  b = exprToLitOrExpr(b, false);

  return _Expr(pli.spearmanRankCorr(a, b, null, false));
}

/** Get the last n rows of an Expression. */
export function tail(column: ExprOrString, n?: number): Expr;
export function tail(column: Series, n?: number): Series;
export function tail(column: Series | ExprOrString, n?: number): Series | Expr {
  if (Series.isSeries(column)) {
    return column.tail(n);
  }
  return exprToLitOrExpr(column, false).tail(n);
}

/** Syntactic sugar for `pl.col(column).list()` */
export function list(column: ExprOrString): Expr {
  return exprToLitOrExpr(column, false).list();
}

/**
    Collect several columns into a Series of dtype Struct
    Parameters
    ----------
    @param exprs
        Columns/Expressions to collect into a Struct
    @param eager
        Evaluate immediately

    Examples
    --------
    ```
    >pl.DataFrame(
    ...     {
    ...         "int": [1, 2],
    ...         "str": ["a", "b"],
    ...         "bool": [True, None],
    ...         "list": [[1, 2], [3]],
    ...     }
    ... ).select([pl.struct(pl.all()).alias("my_struct")])
    shape: (2, 1)
    ┌───────────────────────┐
    │ my_struct             │
    │ ---                   │
    │ struct{int, ... list} │
    ╞═══════════════════════╡
    │ {1,"a",true,[1, 2]}   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ {2,"b",null,[3]}      │
    └───────────────────────┘

    // Only collect specific columns as a struct:
    >df = pl.DataFrame({
    ...   "a": [1, 2, 3, 4],
    ...   "b": ["one", "two", "three", "four"],
    ...   "c": [9, 8, 7, 6]
    ... })
    >df.withColumn(pl.struct(pl.col(["a", "b"])).alias("a_and_b"))
    shape: (4, 4)
    ┌─────┬───────┬─────┬───────────────────────────────┐
    │ a   ┆ b     ┆ c   ┆ a_and_b                       │
    │ --- ┆ ---   ┆ --- ┆ ---                           │
    │ i64 ┆ str   ┆ i64 ┆ struct[2]{'a': i64, 'b': str} │
    ╞═════╪═══════╪═════╪═══════════════════════════════╡
    │ 1   ┆ one   ┆ 9   ┆ {1,"one"}                     │
    ├╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 2   ┆ two   ┆ 8   ┆ {2,"two"}                     │
    ├╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 3   ┆ three ┆ 7   ┆ {3,"three"}                   │
    ├╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 4   ┆ four  ┆ 6   ┆ {4,"four"}                    │
    └─────┴───────┴─────┴───────────────────────────────┘
```
*/
export function struct(exprs: Series[]): Series;
export function struct(exprs: ExprOrString | ExprOrString[]): Expr;
export function struct(
  exprs: ExprOrString | ExprOrString[] | Series[],
): Expr | Series {
  exprs = Array.isArray(exprs) ? exprs : [exprs];

  if (Series.isSeries(exprs[0])) {
    return select(
      _Expr(pli.asStruct(exprs.map((e) => pli.lit(e.inner())))),
    ).toSeries();
  }
  exprs = selectionToExprList(exprs);

  return _Expr(pli.asStruct(exprs));
}

/**
 * Alias for an element in evaluated in an `eval` expression.

 * @example
  *
  *  A horizontal rank computation by taking the elements of a list
  *
  *  >df = pl.DataFrame({"a": [1, 8, 3], "b": [4, 5, 2]})
  *  >df.withColumn(
  *  ...     pl.concatList(["a", "b"]).arr.eval(pl.element().rank()).alias("rank")
  *  ... )
  *  shape: (3, 3)
  *  ┌─────┬─────┬────────────┐
  *  │ a   ┆ b   ┆ rank       │
  *  │ --- ┆ --- ┆ ---        │
  *  │ i64 ┆ i64 ┆ list[f32]  │
  *  ╞═════╪═════╪════════════╡
  *  │ 1   ┆ 4   ┆ [1.0, 2.0] │
  *  ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
  *  │ 8   ┆ 5   ┆ [2.0, 1.0] │
  *  ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
  *  │ 3   ┆ 2   ┆ [2.0, 1.0] │
  *  └─────┴─────┴────────────┘
  *
  *  A mathematical operation on array elements
  *
  *  >df = pl.DataFrame({"a": [1, 8, 3], "b": [4, 5, 2]})
  *  >df.withColumn(
  *  ...     pl.concatList(["a", "b"]).arr.eval(pl.element().multiplyBy(2)).alias("a_b_doubled")
  *  ... )
  *  shape: (3, 3)
  *  ┌─────┬─────┬─────────────┐
  *  │ a   ┆ b   ┆ a_b_doubled │
  *  │ --- ┆ --- ┆ ---         │
  *  │ i64 ┆ i64 ┆ list[i64]   │
  *  ╞═════╪═════╪═════════════╡
  *  │ 1   ┆ 4   ┆ [2, 8]      │
  *  ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
  *  │ 8   ┆ 5   ┆ [16, 10]    │
  *  ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
  *  │ 3   ┆ 2   ┆ [6, 4]      │
  *  └─────┴─────┴─────────────┘
 */

export function element(): Expr {
  return col("");
}

/**
 * Compute the bitwise AND horizontally across columns.
 * @param *exprs
 *         Column(s) to use in the aggregation. Accepts expression input. Strings are
 *         parsed as column names, other non-expression inputs are parsed as literals.
 *
 * @example
 * ```
 *  >>> const df = pl.DataFrame(
 *        {
 *            "a": [false, false, true, true],
 *            "b": [false, true, null, true],
 *            "c": ["w", "x", "y", "z"],
 *        }
 *      )
 *  >>> df.withColumns(pl.allHorizontal([pl.col("a"), pl.col("b")]))
 *  shape: (4, 4)
 *  ┌───────┬───────┬─────┬───────┐
 *  │ a     ┆ b     ┆ c   ┆ all   │
 *  │ ---   ┆ ---   ┆ --- ┆ ---   │
 *  │ bool  ┆ bool  ┆ str ┆ bool  │
 *  ╞═══════╪═══════╪═════╪═══════╡
 *  │ false ┆ false ┆ w   ┆ false │
 *  │ false ┆ true  ┆ x   ┆ false │
 *  │ true  ┆ null  ┆ y   ┆ null  │
 *  │ true  ┆ true  ┆ z   ┆ true  │
 *  └───────┴───────┴─────┴───────┘
 * ```
 */

export function allHorizontal(exprs: ExprOrString | ExprOrString[]): Expr {
  exprs = Array.isArray(exprs) ? exprs : [exprs];

  exprs = selectionToExprList(exprs);

  return _Expr(pli.allHorizontal(exprs));
}

/**
 * Compute the bitwise OR horizontally across columns.
 * @param *exprs
 *       Column(s) to use in the aggregation. Accepts expression input. Strings are
 *       parsed as column names, other non-expression inputs are parsed as literals.
 * @example
 * ```
 *  >>> const df = pl.DataFrame(
 *  ...     {
 *  ...         "a": [false, false, true, null],
 *  ...         "b": [false, true, null, null],
 *  ...         "c": ["w", "x", "y", "z"],
 *  ...     }
 *  ... )
 *  >>> df.withColumns(pl.anyHorizontal([pl.col("a"), pl.col("b")]))
 *  shape: (4, 4)
 *  ┌───────┬───────┬─────┬───────┐
 *  │ a     ┆ b     ┆ c   ┆ any   │
 *  │ ---   ┆ ---   ┆ --- ┆ ---   │
 *  │ bool  ┆ bool  ┆ str ┆ bool  │
 *  ╞═══════╪═══════╪═════╪═══════╡
 *  │ false ┆ false ┆ w   ┆ false │
 *  │ false ┆ true  ┆ x   ┆ true  │
 *  │ true  ┆ null  ┆ y   ┆ true  │
 *  │ null  ┆ null  ┆ z   ┆ null  │
 *  └───────┴───────┴─────┴───────┘
 * ```
 */

export function anyHorizontal(exprs: ExprOrString | ExprOrString[]): Expr {
  exprs = Array.isArray(exprs) ? exprs : [exprs];

  exprs = selectionToExprList(exprs);

  return _Expr(pli.anyHorizontal(exprs));
}

/**
 * Get the maximum value horizontally across columns.
 * @param *exprs
 *       Column(s) to use in the aggregation. Accepts expression input. Strings are
 *       parsed as column names, other non-expression inputs are parsed as literals.
 * @example
 * ```
 *  >>> const df = pl.DataFrame(
 *  ...     {
 *  ...         "a": [1, 8, 3],
 *  ...         "b": [4, 5, null],
 *  ...         "c": ["x", "y", "z"],
 *  ...     }
 *  ... )
 *  >>> df.withColumns(pl.maxHorizontal(pl.col("a"), pl.col("b")))
 *  shape: (3, 4)
 *  ┌─────┬──────┬─────┬─────┐
 *  │ a   ┆ b    ┆ c   ┆ max │
 *  │ --- ┆ ---  ┆ --- ┆ --- │
 *  │ i64 ┆ i64  ┆ str ┆ i64 │
 *  ╞═════╪══════╪═════╪═════╡
 *  │ 1   ┆ 4    ┆ x   ┆ 4   │
 *  │ 8   ┆ 5    ┆ y   ┆ 8   │
 *  │ 3   ┆ null ┆ z   ┆ 3   │
 *  └─────┴──────┴─────┴─────┘
 * ```
 */
export function maxHorizontal(exprs: ExprOrString | ExprOrString[]): Expr {
  exprs = Array.isArray(exprs) ? exprs : [exprs];

  exprs = selectionToExprList(exprs);

  return _Expr(pli.maxHorizontal(exprs));
}
/**
 * Get the minimum value horizontally across columns.
 * @param *exprs
 *       Column(s) to use in the aggregation. Accepts expression input. Strings are
 *       parsed as column names, other non-expression inputs are parsed as literals.
 * @example
 * ```
 *  >>> const df = pl.DataFrame(
 *  ...     {
 *  ...         "a": [1, 8, 3],
 *  ...         "b": [4, 5, null],
 *  ...         "c": ["x", "y", "z"],
 *  ...     }
 *  ... )
 *  >>> df.withColumns(pl.minHorizontal(pl.col("a"), pl.col("b")))
 *  shape: (3, 4)
 *  ┌─────┬──────┬─────┬─────┐
 *  │ a   ┆ b    ┆ c   ┆ min │
 *  │ --- ┆ ---  ┆ --- ┆ --- │
 *  │ i64 ┆ i64  ┆ str ┆ i64 │
 *  ╞═════╪══════╪═════╪═════╡
 *  │ 1   ┆ 4    ┆ x   ┆ 1   │
 *  │ 8   ┆ 5    ┆ y   ┆ 5   │
 *  │ 3   ┆ null ┆ z   ┆ 3   │
 *  └─────┴──────┴─────┴─────┘
 * ```
 */

export function minHorizontal(exprs: ExprOrString | ExprOrString[]): Expr {
  exprs = Array.isArray(exprs) ? exprs : [exprs];

  exprs = selectionToExprList(exprs);

  return _Expr(pli.minHorizontal(exprs));
}

/**
 * Sum all values horizontally across columns.
 * @param *exprs
 *       Column(s) to use in the aggregation. Accepts expression input. Strings are
 *       parsed as column names, other non-expression inputs are parsed as literals.
 * @example
 * ```
 *  >>> const df = pl.DataFrame(
 *  ...     {
 *  ...         "a": [1, 8, 3],
 *  ...         "b": [4, 5, null],
 *  ...         "c": ["x", "y", "z"],
 *  ...     }
 *  ... )
 *  >>> df.withColumns(pl.sumHorizontal(pl.col("a"), ol.col("b")))
 *  shape: (3, 4)
 *  ┌─────┬──────┬─────┬──────┐
 *  │ a   ┆ b    ┆ c   ┆ sum  │
 *  │ --- ┆ ---  ┆ --- ┆ ---  │
 *  │ i64 ┆ i64  ┆ str ┆ i64  │
 *  ╞═════╪══════╪═════╪══════╡
 *  │ 1   ┆ 4    ┆ x   ┆ 5    │
 *  │ 8   ┆ 5    ┆ y   ┆ 13   │
 *  │ 3   ┆ null ┆ z   ┆ null │
 *  └─────┴──────┴─────┴──────┘
 * ```
 */
export function sumHorizontal(exprs: ExprOrString | ExprOrString[]): Expr {
  exprs = Array.isArray(exprs) ? exprs : [exprs];

  exprs = selectionToExprList(exprs);

  return _Expr(pli.sumHorizontal(exprs));
}

// // export function collect_all() {}
// // export function all() {} // fold
// // export function any() {} // fold
// // export function apply() {} // lambda
// // export function fold() {}
// // export function map_binary() {} //lambda
// // export function map() {} //lambda
// // export function max() {} // fold
// // export function min() {} // fold
// // export function sum() {} // fold
