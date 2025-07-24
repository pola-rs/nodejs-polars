import { isTypedArray } from "node:util/types";
import { DataType, polarsTypeToConstructor } from "../datatypes";
import { FixedSizeList, TimeUnit } from "../datatypes/datatype";
import { Field } from "../datatypes/field";
import { Series } from "../series";
import pli from "./polars_internal";

export const jsTypeToPolarsType = (value: unknown): DataType => {
  if (value === null) {
    return DataType.Float64;
  }
  if (Array.isArray(value)) {
    return jsTypeToPolarsType(firstNonNull(value));
  }
  if (isTypedArray(value)) {
    switch (value.constructor.name) {
      case Int8Array.name:
        return DataType.Int8;
      case Int16Array.name:
        return DataType.Int16;
      case Int32Array.name:
        return DataType.Int32;
      case BigInt64Array.name:
        return DataType.Int64;
      case Uint8Array.name:
        return DataType.UInt8;
      case Uint16Array.name:
        return DataType.UInt16;
      case Uint32Array.name:
        return DataType.UInt32;
      case BigUint64Array.name:
        return DataType.UInt64;
      case Float32Array.name:
        return DataType.Float32;
      case Float64Array.name:
        return DataType.Float64;
      default:
        throw new Error(`unknown  typed array type: ${value.constructor.name}`);
    }
  }

  if (value instanceof Date) {
    return DataType.Datetime(TimeUnit.Milliseconds);
  }
  if (typeof value === "object" && (value as any).constructor === Object) {
    const flds = Object.entries(value as any).map(([name, value]) => {
      const dtype = jsTypeToPolarsType(value);

      return Field.from(name, dtype);
    });

    return DataType.Struct(flds);
  }

  switch (typeof value) {
    case "bigint":
      return DataType.UInt64;
    case "number":
      return DataType.Float64;
    case "string":
      return DataType.Utf8;
    case "boolean":
      return DataType.Bool;
    default:
      return DataType.Float64;
  }
};

/**
 * __finds the first non null value in the inputs__
 * ___
 * If the first value is an array
 * it will find the first scalar type in the array and return it wrapped into the array
 *
 * @example
 * ```
 * > const input = [null, [], [null, "a", "b"]]
 * > firstNonNull(input)
 * ["a"]
 * > const ints = [null, 1]
 * > firstNonNull(ints)
 * 1
 * ```
 */
const firstNonNull = (arr: any[]): any => {
  const first = arr.find((x) => x !== null && x !== undefined);
  if (Array.isArray(first)) {
    return [firstNonNull(arr.flat())];
  }

  return first;
};

const fromTypedArray = (name, value) => {
  switch (value.constructor.name) {
    case Int8Array.name:
      return pli.JsSeries.newInt8Array(name, value);
    case Int16Array.name:
      return pli.JsSeries.newInt16Array(name, value);
    case Int32Array.name:
      return pli.JsSeries.newInt32Array(name, value);
    case BigInt64Array.name:
      return pli.JsSeries.newBigint64Array(name, value);
    case Uint8Array.name:
      return pli.JsSeries.newUint8Array(name, value);
    case Uint8ClampedArray.name:
      return pli.JsSeries.newUint8ClampedArray(name, value);
    case Uint16Array.name:
      return pli.JsSeries.newUint16Array(name, value);
    case Uint32Array.name:
      return pli.JsSeries.newUint32Array(name, value);
    case BigUint64Array.name:
      return pli.JsSeries.newBiguint64Array(name, value);
    case Float32Array.name:
      return pli.JsSeries.newFloat32Array(name, value);
    case Float64Array.name:
      return pli.JsSeries.newFloat64Array(name, value);
    default:
      throw new Error(`unknown  typed array type: ${value.constructor.name}`);
  }
};

/**
 * Construct an internal `JsSeries` from an array
 */
export function arrayToJsSeries(
  name = "",
  values: any[] = [],
  dtype?: any,
  strict = false,
): any {
  if (isTypedArray(values)) {
    return fromTypedArray(name, values);
  }

  //Empty sequence defaults to Float64 type
  if (!(values?.length || dtype)) {
    dtype = DataType.Float64;
  }
  const firstValue = firstNonNull(values);
  if (Array.isArray(firstValue) || isTypedArray(firstValue)) {
    const listDtype = jsTypeToPolarsType(firstValue);
    const ctor = polarsTypeToConstructor(DataType.List(listDtype));
    const s = ctor(name, values, strict, listDtype);
    if (dtype instanceof FixedSizeList) {
      // TODO: build a FixedSizeList natively in Rust
      return s.cast(dtype, strict);
    }

    return s;
  }

  dtype = dtype ?? jsTypeToPolarsType(firstValue);
  let series: Series;
  if (dtype?.variant === "Struct") {
    const df = pli.fromRows(values, null, 1);

    return df.toStruct(name);
  }

  if (dtype?.variant === "Decimal") {
    if (typeof firstValue !== "bigint") {
      throw new Error("Decimal type can only be constructed from BigInt");
    }
    return pli.JsSeries.newAnyvalue(name, values, dtype, strict);
  }
  if (firstValue instanceof Date) {
    series = pli.JsSeries.newOptDate(name, values, strict);
  } else {
    const ctor = polarsTypeToConstructor(dtype);
    series = ctor(name, values, strict);
  }

  if (
    [
      "Datetime",
      "Date",
      "Categorical",
      "Int8",
      "Int16",
      "UInt8",
      "UInt16",
      "Float32",
    ].includes(dtype.variant)
  ) {
    series = series.cast(dtype, strict);
  }

  return series;
}

export function arrayToJsDataFrame(data: any[], options?): any {
  const columns = options?.columns;
  let orient = options?.orient;
  const schema = options?.schema;
  const inferSchemaLength = options?.inferSchemaLength;

  let dataSeries: any[];

  if (!data.length) {
    dataSeries = [];
  } else if (data[0]?._s) {
    dataSeries = [];
    for (const [idx, series] of data.entries()) {
      if (!series.name) {
        series.rename(`column_${idx}`, true);
      }
      dataSeries.push(series._s);
    }
  } else if (data[0].constructor.name === "Object") {
    const df = pli.fromRows(data, schema, inferSchemaLength);

    if (columns) {
      df.columns = columns;
    }

    return df;
  } else if (Array.isArray(data[0])) {
    if (!orient && columns) {
      orient = columns.length === data.length ? "col" : "row";
    }

    if (orient === "row") {
      const df = pli.fromRows(data);
      if (columns) df.columns = columns;

      return df;
    }
    dataSeries = data.map((s, idx) => (Series(`column_${idx}`, s) as any)._s);
  } else {
    dataSeries = [(Series("column_0", data) as any)._s];
  }
  dataSeries = handleColumnsArg(dataSeries, columns);

  return new pli.JsDataFrame(dataSeries);
}

function handleColumnsArg(data: any[], columns?: string[]) {
  if (!columns) {
    return data;
  }
  if (!data) {
    return columns.map((c) => (Series.from(c, []) as any)._s);
  }
  if (data.length === columns.length) {
    for (const [idx, name] of columns.entries()) {
      data[idx].rename(name);
    }
    return data;
  }
  throw new TypeError("Dimensions of columns arg must match data dimensions.");
}
