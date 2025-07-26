import { Field } from "./field";

export abstract class DataType<Dtype extends DataTypeName = any> {
  declare readonly __dtype: Dtype;
  get variant() {
    return this.constructor.name as DataTypeName;
  }
  protected identity = "DataType";
  protected get inner(): null | any[] {
    return null;
  }
  equals(other: DataType): boolean {
    return (
      this.variant === other.variant &&
      this.inner === null &&
      other.inner === null
    );
  }

  /** Null type */
  public static get Null() {
    return new Null();
  }
  /** `true` and `false`. */
  public static get Bool() {
    return new Bool();
  }
  /** An `i8` */
  public static get Int8() {
    return new Int8();
  }
  /** An `i16` */
  public static get Int16() {
    return new Int16();
  }
  /** An `i32` */
  public static get Int32() {
    return new Int32();
  }
  /** An `i64` */
  public static get Int64() {
    return new Int64();
  }
  /** An `u8` */
  public static get UInt8() {
    return new UInt8();
  }
  /** An `u16` */
  public static get UInt16() {
    return new UInt16();
  }
  /** An `u32` */
  public static get UInt32() {
    return new UInt32();
  }
  /** An `u64` */
  public static get UInt64() {
    return new UInt64();
  }

  /** A `f32` */
  public static get Float32() {
    return new Float32();
  }
  /** A `f64` */
  public static get Float64() {
    return new Float64();
  }
  public static get Date() {
    return new Date();
  }
  /** Time of day type */
  public static get Time() {
    return new Time();
  }
  /** Type for wrapping arbitrary JS objects */
  public static get Object() {
    return new Object_();
  }
  /** A categorical encoding of a set of strings  */
  public static get Categorical() {
    return new Categorical();
  }

  /** Decimal type */
  public static Decimal(precision?: number, scale?: number) {
    return new Decimal(precision, scale);
  }
  /**
   * Calendar date and time type
   * @param timeUnit any of 'ms' | 'ns' | 'us'
   * @param timeZone timezone string as defined by Intl.DateTimeFormat `America/New_York` for example.
   */
  public static Datetime(
    timeUnit?: TimeUnit | "ms" | "ns" | "us",
    timeZone: string | null | undefined = null,
  ) {
    return new Datetime(timeUnit ?? "ms", timeZone);
  }

  /**
   * Nested list/array type
   *
   * @param inner The `DataType` of values within the list
   *
   */
  public static List(inner: DataType) {
    return new List(inner);
  }

  /**
   * List of fixed length
   * This is called `Array` in other polars implementations, but `Array` is widely used in JS, so we use `FixedSizeList` instead.
   *
   */
  public static FixedSizeList(inner: DataType, listSize: number) {
    return new FixedSizeList(inner, listSize);
  }
  /**
   * Struct type
   */
  public static Struct(fields: Field[]): DataType;
  public static Struct(fields: { [key: string]: DataType }): DataType;
  public static Struct(fields: Field[] | { [key: string]: DataType }) {
    return new Struct(fields);
  }
  /** A variable-length UTF-8 encoded string whose offsets are represented as `i64`. */
  public static get Utf8() {
    return new Utf8();
  }

  public static get String() {
    return new String();
  }

  toString() {
    if (this.inner) {
      return `${this.identity}(${this.variant}(${this.inner}))`;
    }
    return `${this.identity}(${this.variant})`;
  }

  toJSON() {
    const inner = (this as any).inner;
    if (inner) {
      return {
        [this.identity]: {
          [this.variant]: inner[0],
        },
      };
    }
    return {
      [this.identity]: this.variant,
    };
  }
  [Symbol.for("nodejs.util.inspect.custom")]() {
    return this.toString();
  }
  asFixedSizeList() {
    if (this instanceof FixedSizeList) {
      return this;
    }
    return null;
  }
}

export class Null extends DataType<"Null"> {
  declare __dtype: "Null";
}

export class Bool extends DataType<"Bool"> {
  declare __dtype: "Bool";
}
export class Int8 extends DataType<"Int8"> {
  declare __dtype: "Int8";
}
export class Int16 extends DataType<"Int16"> {
  declare __dtype: "Int16";
}
export class Int32 extends DataType<"Int32"> {
  declare __dtype: "Int32";
}
export class Int64 extends DataType<"Int64"> {
  declare __dtype: "Int64";
}
export class UInt8 extends DataType<"UInt8"> {
  declare __dtype: "UInt8";
}
export class UInt16 extends DataType<"UInt16"> {
  declare __dtype: "UInt16";
}
export class UInt32 extends DataType<"UInt32"> {
  declare __dtype: "UInt32";
}
export class UInt64 extends DataType<"UInt64"> {
  declare __dtype: "UInt64";
}
export class Float32 extends DataType<"Float32"> {
  declare __dtype: "Float32";
}
export class Float64 extends DataType<"Float64"> {
  declare __dtype: "Float64";
}

// biome-ignore lint/suspicious/noShadowRestrictedNames: Using Polars Date
export class Date extends DataType<"Date"> {
  declare __dtype: "Date";
}
export class Time extends DataType<"Time"> {
  declare __dtype: "Time";
}
export class Object_ extends DataType<"Object"> {
  declare __dtype: "Object";
}
export class Utf8 extends DataType<"Utf8"> {
  declare __dtype: "Utf8";
}
// biome-ignore lint/suspicious/noShadowRestrictedNames: Using Polars String
export class String extends DataType<"String"> {
  declare __dtype: "String";
}

export class Categorical extends DataType<"Categorical"> {
  declare __dtype: "Categorical";
}

export class Decimal extends DataType<"Decimal"> {
  declare __dtype: "Decimal";
  private precision: number | null;
  private scale: number | null;
  constructor(precision?: number, scale?: number) {
    super();
    this.precision = precision ?? null;
    this.scale = scale ?? null;
  }
  override get inner() {
    return [this.precision, this.scale];
  }
  override equals(other: DataType): boolean {
    if (other.variant === this.variant) {
      return (
        this.precision === (other as Decimal).precision &&
        this.scale === (other as Decimal).scale
      );
    }
    return false;
  }

  override toJSON() {
    return {
      [this.identity]: {
        [this.variant]: {
          precision: this.precision,
          scale: this.scale,
        },
      },
    };
  }
}

/**
 * Datetime type
 */
export class Datetime extends DataType<"Datetime"> {
  declare __dtype: "Datetime";
  constructor(
    private timeUnit: TimeUnit | "ms" | "ns" | "us" = "ms",
    private timeZone?: string | null,
  ) {
    super();
  }
  override get inner() {
    return [this.timeUnit, this.timeZone];
  }

  override equals(other: DataType): boolean {
    if (other.variant === this.variant) {
      return (
        this.timeUnit === (other as Datetime).timeUnit &&
        this.timeZone === (other as Datetime).timeZone
      );
    }
    return false;
  }
}

export class List extends DataType<"List"> {
  declare __dtype: "List";
  constructor(protected __inner: DataType) {
    super();
  }
  override get inner() {
    return [this.__inner];
  }
  override equals(other: DataType): boolean {
    if (other.variant === this.variant) {
      return this.inner[0].equals((other as List).inner[0]);
    }
    return false;
  }
}

export class FixedSizeList extends DataType<"FixedSizeList"> {
  declare __dtype: "FixedSizeList";
  constructor(
    protected __inner: DataType,
    protected listSize: number,
  ) {
    super();
  }

  override get inner(): [DataType, number] {
    return [this.__inner, this.listSize];
  }

  override equals(other: DataType): boolean {
    if (other.variant === this.variant) {
      return (
        this.inner[0].equals((other as FixedSizeList).inner[0]) &&
        this.inner[1] === (other as FixedSizeList).inner[1]
      );
    }
    return false;
  }
  override toJSON() {
    return {
      [this.identity]: {
        [this.variant]: {
          type: this.inner[0].toJSON(),
          size: this.inner[1],
        },
      },
    };
  }
}

export class Struct extends DataType<"Struct"> {
  declare __dtype: "Struct";
  private fields: Field[];

  constructor(
    inner:
      | {
          [name: string]: DataType;
        }
      | Field[],
  ) {
    super();
    if (Array.isArray(inner)) {
      this.fields = inner;
    } else {
      this.fields = Object.entries(inner).map(Field.from);
    }
  }
  override get inner() {
    return this.fields;
  }
  override equals(other: DataType): boolean {
    if (other.variant === this.variant) {
      return this.inner
        .map((fld, idx) => {
          const otherfld = (other as Struct).fields[idx];

          return otherfld.name === fld.name && otherfld.dtype.equals(fld.dtype);
        })
        .every((value) => value);
    }
    return false;
  }
  override toJSON() {
    return {
      [this.identity]: {
        [this.variant]: this.fields,
      },
    } as any;
  }
}

/**
 * Datetime time unit
 */
export enum TimeUnit {
  Nanoseconds = "ns",
  Microseconds = "us",
  Milliseconds = "ms",
}

/**
 * @ignore
 * Timeunit namespace
 */
export namespace TimeUnit {
  export function from(s: "ms" | "ns" | "us"): TimeUnit {
    return TimeUnit[s];
  }
}

/**
 * Datatype namespace
 */
export namespace DataType {
  export type Categorical = import(".").Categorical;
  export type Int8 = import(".").Int8;
  export type Int16 = import(".").Int16;
  export type Int32 = import(".").Int32;
  export type Int64 = import(".").Int64;
  export type UInt8 = import(".").UInt8;
  export type UInt16 = import(".").UInt16;
  export type UInt32 = import(".").UInt32;
  export type UInt64 = import(".").UInt64;
  export type Float32 = import(".").Float32;
  export type Float64 = import(".").Float64;
  export type Bool = import(".").Bool;
  export type Utf8 = import(".").Utf8;
  export type String = import(".").String;
  export type List = import(".").List;
  export type FixedSizeList = import(".").FixedSizeList;
  export type Date = import(".").Date;
  export type Datetime = import(".").Datetime;
  export type Time = import(".").Time;
  export type Object = import(".").Object_;
  export type Null = import(".").Null;
  export type Struct = import(".").Struct;
  export type Decimal = import(".").Decimal;
  /**
   * deserializes a datatype from the serde output of rust polars `DataType`
   * @param dtype dtype object
   */
  export function deserialize(dtype: any): DataType {
    if (typeof dtype === "string") {
      return DataType[dtype];
    }

    let { variant, inner } = dtype;
    if (variant === "Struct") {
      inner = [
        inner[0].map((fld) => Field.from(fld.name, deserialize(fld.dtype))),
      ];
    }
    if (variant === "List") {
      inner = [deserialize(inner[0])];
    }

    if (variant === "FixedSizeList") {
      inner = [deserialize(inner[0]), inner[1]];
    }

    return DataType[variant](...inner);
  }
}

export type DataTypeName =
  | "Null"
  | "Bool"
  | "Int8"
  | "Int16"
  | "Int32"
  | "Int64"
  | "UInt8"
  | "UInt16"
  | "UInt32"
  | "UInt64"
  | "Float32"
  | "Float64"
  | "Decimal"
  | "Date"
  | "Datetime"
  | "Time"
  | "Object"
  | "Utf8"
  | "String"
  | "Categorical"
  | "List"
  | "FixedSizeList"
  | "Struct";

export type JsType = number | boolean | string;
export type DTypeToJs<T> = T extends DataType.Decimal
  ? bigint
  : T extends DataType.Float64
    ? number
    : T extends DataType.Float32
      ? number
      : T extends DataType.Int64
        ? bigint
        : T extends DataType.Int32
          ? number
          : T extends DataType.Int16
            ? number
            : T extends DataType.Int8
              ? number
              : T extends DataType.Bool
                ? boolean
                : T extends DataType.Utf8
                  ? string
                  : T extends DataType.String
                    ? string
                    : never;
// some objects can be constructed with a looser JS type than they’d return when converted back to JS
export type DTypeToJsLoose<T> = T extends DataType.Decimal
  ? number | bigint
  : T extends DataType.Float64
    ? number | bigint
    : T extends DataType.Float32
      ? number | bigint
      : T extends DataType.Int64
        ? number | bigint
        : T extends DataType.Int32
          ? number | bigint
          : T extends DataType.Int16
            ? number | bigint
            : T extends DataType.Int8
              ? number | bigint
              : T extends DataType.Bool
                ? boolean
                : T extends DataType.Utf8
                  ? string
                  : T extends DataType.String
                    ? string
                    : never;
export type DtypeToJsName<T> = T extends DataType.Decimal
  ? "Decimal"
  : T extends DataType.Float64
    ? "Float64"
    : T extends DataType.Float32
      ? "Float32"
      : T extends DataType.Int64
        ? "Int64"
        : T extends DataType.Int32
          ? "Int32"
          : T extends DataType.Int16
            ? "Int16"
            : T extends DataType.Int8
              ? "Int8"
              : T extends DataType.UInt64
                ? "UInt64"
                : T extends DataType.UInt32
                  ? "UInt32"
                  : T extends DataType.UInt16
                    ? "UInt16"
                    : T extends DataType.UInt8
                      ? "UInt8"
                      : T extends DataType.Bool
                        ? "Bool"
                        : T extends DataType.Utf8
                          ? "Utf8"
                          : never;
