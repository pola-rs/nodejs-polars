import path from "node:path";
import { isRegExp } from "node:util/types";
import { DataFrame } from "./dataframe";
import type { DataType } from "./datatypes";
import { LazyDataFrame } from "./lazy/dataframe";
import { Expr, exprToLitOrExpr } from "./lazy/expr";
import { Series } from "./series";
/** @ignore */
export type ValueOrArray<T> = T | Array<ValueOrArray<T>>;
/** @ignore */
export type ColumnSelection = ValueOrArray<string>;
/** @ignore */
export type ExpressionSelection = ValueOrArray<Expr>;
/** @ignore */
export type ColumnsOrExpr = ColumnSelection | ExpressionSelection;
/** @ignore */
export type ExprOrString = Expr | string;

// Transitional types (no-ops now that Expr<T,Name> is generic)
export type TypedExpr<T extends DataType> = Expr<T>;
export type NamedExpr<
  Name extends string,
  T extends DataType = DataType,
> = Expr<T, Name>;

/**
 * @typeParam StartBy - The strategy to determine the start of the first window by.
 */
export type StartBy =
  | "window"
  | "datapoint"
  | "monday"
  | "tuesday"
  | "wednesday"
  | "thursday"
  | "friday"
  | "saturday"
  | "sunday";

/** @ignore */
export function columnOrColumns(
  columns: ColumnSelection | string | Array<string> | undefined,
): Array<string> | undefined {
  if (columns) {
    return columnOrColumnsStrict(columns);
  }
}
/** @ignore */
export function columnOrColumnsStrict(
  ...columns: string[] | ValueOrArray<string>[]
): Array<string> {
  return columns.flat(3) as any;
}
/** @ignore */
export function selectionToExprList(columns: any[], stringToLit?) {
  return [columns]
    .flat(3)
    .map((expr) => exprToLitOrExpr(expr, stringToLit)._expr);
}

/** @ignore */
export function isPath(s: string, expectedExtensions?: string[]): boolean {
  const { base, ext, name } = path.parse(s);

  return Boolean(base && ext && name) && !!expectedExtensions?.includes(ext);
}

export const range = (start: number, end: number) => {
  const length = end - start;

  return Array.from({ length }, (_, i) => start + i);
};

export const isDataFrameArray = (ty: any): ty is DataFrame[] =>
  Array.isArray(ty) && DataFrame.isDataFrame(ty[0]);
export const isLazyDataFrameArray = (ty: any): ty is LazyDataFrame[] =>
  Array.isArray(ty) && LazyDataFrame.isLazyDataFrame(ty[0]);
export const isSeriesArray = <_T>(ty: any): ty is Series[] =>
  Array.isArray(ty) && ty.every(Series.isSeries);
export const isExprArray = (ty: any): ty is Expr[] =>
  Array.isArray(ty) && Expr.isExpr(ty[0]);
export const isIterator = <T>(ty: any): ty is Iterable<T> =>
  ty !== null && typeof ty[Symbol.iterator] === "function";

export const regexToString = (r: string | RegExp): string => {
  if (isRegExp(r)) {
    if (r.flags.includes("g")) {
      throw new Error("global flag is not supported");
    }
    if (r.flags.includes("y")) {
      throw new Error("sticky flag is not supported");
    }
    return r.flags ? `(?${r.flags})${r.source}` : r.source;
  }

  return r;
};

export const INSPECT_SYMBOL = Symbol.for("nodejs.util.inspect.custom");

export type Simplify<T> = { [K in keyof T]: T[K] } & {};

// Type utilities for Expr/Series dtype inference and promotion
export type DTypeOf<V> = V extends import("./lazy/expr").Expr<
  infer DT extends DataType,
  any
>
  ? DT
  : V extends import("./series").Series<infer DT extends DataType, any>
    ? DT
    : V extends number
      ? DataType.Float64
      : V extends bigint
        ? DataType.UInt64
        : V extends boolean
          ? DataType.Bool
          : V extends string
            ? DataType.String
            : DataType;

// Simple numeric promotion: any float/decimal -> Float64; otherwise -> Int64.
export type Promote<A extends DataType, B extends DataType> = A extends
  | DataType.Float64
  | DataType.Float32
  | DataType.Decimal
  ? DataType.Float64
  : B extends DataType.Float64 | DataType.Float32 | DataType.Decimal
    ? DataType.Float64
    : DataType.Int64;

// Aggregation result types
export type SumResult<T extends DataType> =
  // small ints and bool -> Int64
  T extends
    | DataType.Int8
    | DataType.Int16
    | DataType.Int32
    | DataType.UInt8
    | DataType.UInt16
    | DataType.UInt32
    | DataType.Bool
    ? DataType.Int64
    : // big ints/floats/decimal remain
      T extends
          | DataType.Int64
          | DataType.UInt64
          | DataType.Float32
          | DataType.Float64
          | DataType.Decimal
      ? T
      : DataType;

export type FloatAggResult = DataType.Float64;

// Map Expr/Series tuple to a schema
export type NameOf<V> = V extends import("./lazy/expr").Expr<
  any,
  infer Name extends string
>
  ? Name
  : V extends import("./series").Series<any, infer Name2 extends string>
    ? Name2
    : never;

export type DTypeOfExprSeries<V> = V extends import("./lazy/expr").Expr<
  infer DT extends DataType,
  any
>
  ? DT
  : V extends import("./series").Series<infer DT2 extends DataType, any>
    ? DT2
    : DataType;

export type SchemaFromTuple<T extends ReadonlyArray<any>> = {
  [K in T[number] as NameOf<K>]: DTypeOfExprSeries<K>;
};
