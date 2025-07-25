import pli from "../internals/polars_internal";
import { Expr } from "./expr";

export interface When {
  /** Values to return in case of the predicate being `true`.*/
  then(expr: Expr): Then;
}

export interface Then {
  /** Start another when, then, otherwise layer. */
  when(predicate: Expr): ChainedWhen;
  /** Values to return in case of the predicate being `false`. */
  otherwise(expr: Expr): Expr;
}

export interface ChainedWhen {
  /** Values to return in case of the predicate being `true`. */
  then(expr: Expr): ChainedThen;
}

export interface ChainedThen {
  /** Start another when, then, otherwise layer. */
  when(predicate: Expr): ChainedWhen;
  /** Values to return in case of the predicate being `true`. */
  then(expr: Expr): ChainedThen;
  /** Values to return in case of the predicate being `false`. */
  otherwise(expr: Expr): Expr;
}

function ChainedWhen(_chainedwhen: any): ChainedWhen {
  return {
    then: ({ _expr }: Expr): ChainedThen =>
      ChainedThen(_chainedwhen.then(_expr)),
  };
}
function ChainedThen(_chainedthen: any): ChainedThen {
  return {
    when: ({ _expr }: Expr): ChainedWhen =>
      ChainedWhen(_chainedthen.when(_expr)),
    then: ({ _expr }: Expr): ChainedThen =>
      ChainedThen(_chainedthen.then(_expr)),
    otherwise: ({ _expr }: Expr): Expr =>
      (Expr as any)(_chainedthen.otherwise(_expr)),
  };
}

function Then(_then: any): Then {
  return {
    when: ({ _expr }: Expr): ChainedWhen => ChainedWhen(_then.when(_expr)),
    otherwise: ({ _expr }: Expr): Expr => (Expr as any)(_then.otherwise(_expr)),
  };
}

/**
 * Utility function.
 * @see {@link when}
 */
function When(_when: any): When {
  return {
    then: ({ _expr }: Expr): Then => Then(_when.then(_expr)),
  };
}

/**
 * Start a when, then, otherwise expression.
 *
 * @example
 * ```
 * // Below we add a column with the value 1, where column "foo" > 2 and the value -1 where it isn't.
 * > df = pl.DataFrame({"foo": [1, 3, 4], "bar": [3, 4, 0]})
 * > df.withColumn(pl.when(pl.col("foo").gt(2)).then(pl.lit(1)).otherwise(pl.lit(-1)))
 * shape: (3, 3)
 * ┌─────┬─────┬─────────┐
 * │ foo ┆ bar ┆ literal │
 * │ --- ┆ --- ┆ ---     │
 * │ i64 ┆ i64 ┆ i32     │
 * ╞═════╪═════╪═════════╡
 * │ 1   ┆ 3   ┆ -1      │
 * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
 * │ 3   ┆ 4   ┆ 1       │
 * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
 * │ 4   ┆ 0   ┆ 1       │
 * └─────┴─────┴─────────┘
 *
 * // Or with multiple `when, thens` chained:
 * > df.with_column(
 * ...     pl.when(pl.col("foo").gt(2))
 * ...     .then(1)
 * ...     .when(pl.col("bar").gt(2))
 * ...     .then(4)
 * ...     .otherwise(-1)
 * ... )
 * shape: (3, 3)
 * ┌─────┬─────┬─────────┐
 * │ foo ┆ bar ┆ literal │
 * │ --- ┆ --- ┆ ---     │
 * │ i64 ┆ i64 ┆ i32     │
 * ╞═════╪═════╪═════════╡
 * │ 1   ┆ 3   ┆ 4       │
 * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
 * │ 3   ┆ 4   ┆ 1       │
 * ├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
 * │ 4   ┆ 0   ┆ 1       │
 * └─────┴─────┴─────────┘
 * ```
 */
export function when(expr: Expr): When {
  return When(pli.when(expr._expr));
}
