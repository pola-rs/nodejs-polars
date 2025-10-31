export * from "./conversion";
export * from "./datatype";
export { Field } from "./field";

import { Duration } from "..";
import pli from "../internals/polars_internal";
import { type DataType, Time } from "./datatype";

/** @ignore */
export type TypedArray =
  | Int8Array
  | Int16Array
  | Int32Array
  | BigInt64Array
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | BigInt64Array
  | Float32Array
  | Float64Array;

/**
 *  @ignore
 */
export type Optional<T> = T | undefined | null;

/**
 * @ignore
 */
export type JsDataFrame = any;
export type NullValues = string | Array<string> | Record<string, string>;

/**
 * @ignore
 */
export const DTYPE_TO_FFINAME = {
  Int8: "I8",
  Int16: "I16",
  Int32: "I32",
  Int64: "I64",
  UInt8: "U8",
  UInt16: "U16",
  UInt32: "U32",
  UInt64: "U64",
  Float32: "F32",
  Float64: "F64",
  Bool: "Bool",
  Utf8: "Str",
  String: "Str",
  List: "List",
  Date: "Date",
  Datetime: "Datetime",
  Duration: "Duration",
  Time: "Time",
  Object: "Object",
  Categorical: "Categorical",
  Struct: "Struct",
};

const POLARS_TYPE_TO_CONSTRUCTOR: Record<string, any> = {
  Float32(name, values) {
    return pli.JsSeries.newOptF64(name, values);
  },
  Float64(name, values) {
    return pli.JsSeries.newOptF64(name, values);
  },
  Int8(name, values) {
    return pli.JsSeries.newOptI32(name, values);
  },
  Int16(name, values) {
    return pli.JsSeries.newOptI32(name, values);
  },
  Int32(name, values) {
    return pli.JsSeries.newOptI32(name, values);
  },
  Int64(name, values) {
    return pli.JsSeries.newOptI64(name, values);
  },
  UInt8(name, values) {
    return pli.JsSeries.newOptU32(name, values);
  },
  UInt16(name, values) {
    return pli.JsSeries.newOptU32(name, values);
  },
  UInt32(name, values) {
    return pli.JsSeries.newOptU32(name, values);
  },
  UInt64(name, values) {
    return pli.JsSeries.newOptU64(name, values);
  },
  Date(name, values) {
    return pli.JsSeries.newOptI64(name, values);
  },
  Datetime(name, values) {
    return pli.JsSeries.newOptI64(name, values);
  },
  Time(name, values) {
    return pli.JsSeries.newAnyValue(name, values, Time);
  },
  Duration(name, values) {
    return pli.JsSeries.newAnyValue(name, values, Duration("ns"));
  },
  Bool(name, values) {
    return pli.JsSeries.newOptBool(name, values);
  },
  Utf8(name, values) {
    return (pli.JsSeries.newOptStr as any)(name, values);
  },
  String(name, values) {
    return (pli.JsSeries.newOptStr as any)(name, values);
  },
  Categorical(name, values) {
    return (pli.JsSeries.newOptStr as any)(name, values);
  },
  List(name, values, dtype: DataType) {
    return pli.JsSeries.newList(name, values, dtype);
  },
  FixedSizeList(name, values, dtype: DataType) {
    return pli.JsSeries.newList(name, values, dtype);
  },
};

/** @ignore */
export const polarsTypeToConstructor = (dtype: DataType): CallableFunction => {
  const ctor = POLARS_TYPE_TO_CONSTRUCTOR[dtype.variant];
  if (!ctor) {
    throw new Error(`Cannot construct Series for type ${dtype.variant}.`);
  }

  return ctor;
};
