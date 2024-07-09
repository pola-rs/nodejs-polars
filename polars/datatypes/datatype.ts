import { Field } from "./field";

export abstract class DataType {
  get variant() {
    return this.constructor.name;
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
  public static get Null(): DataType {
    return new Null();
  }
  /** `true` and `false`. */
  public static get Bool(): DataType {
    return new Bool();
  }
  /** An `i8` */
  public static get Int8(): DataType {
    return new Int8();
  }
  /** An `i16` */
  public static get Int16(): DataType {
    return new Int16();
  }
  /** An `i32` */
  public static get Int32(): DataType {
    return new Int32();
  }
  /** An `i64` */
  public static get Int64(): DataType {
    return new Int64();
  }
  /** An `u8` */
  public static get UInt8(): DataType {
    return new UInt8();
  }
  /** An `u16` */
  public static get UInt16(): DataType {
    return new UInt16();
  }
  /** An `u32` */
  public static get UInt32(): DataType {
    return new UInt32();
  }
  /** An `u64` */
  public static get UInt64(): DataType {
    return new UInt64();
  }

  /** A `f32` */
  public static get Float32(): DataType {
    return new Float32();
  }
  /** A `f64` */
  public static get Float64(): DataType {
    return new Float64();
  }
  public static get Date(): DataType {
    return new Date();
  }
  /** Time of day type */
  public static get Time(): DataType {
    return new Time();
  }
  /** Type for wrapping arbitrary JS objects */
  public static get Object(): DataType {
    return new Object_();
  }
  /** A categorical encoding of a set of strings  */
  public static get Categorical(): DataType {
    return new Categorical();
  }

  /** Decimal type */
  public static Decimal(precision?: number, scale?: number): DataType {
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
  ): DataType {
    return new Datetime(timeUnit ?? "ms", timeZone);
  }

  /**
   * Nested list/array type
   *
   * @param inner The `DataType` of values within the list
   *
   */
  public static List(inner: DataType): DataType {
    return new List(inner);
  }

  /**
   * List of fixed length
   * This is called `Array` in other polars implementations, but `Array` is widely used in JS, so we use `FixedSizeList` instead.
   *
   */
  public static FixedSizeList(inner: DataType, listSize: number): DataType {
    return new FixedSizeList(inner, listSize);
  }
  /**
   * Struct type
   */
  public static Struct(fields: Field[]): DataType;
  public static Struct(fields: { [key: string]: DataType }): DataType;
  public static Struct(
    fields: Field[] | { [key: string]: DataType },
  ): DataType {
    return new Struct(fields);
  }
  /** A variable-length UTF-8 encoded string whose offsets are represented as `i64`. */
  public static get Utf8(): DataType {
    return new Utf8();
  }

  public static get String(): DataType {
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

export class Null extends DataType {}
export class Bool extends DataType {}
export class Int8 extends DataType {}
export class Int16 extends DataType {}
export class Int32 extends DataType {}
export class Int64 extends DataType {}
export class UInt8 extends DataType {}
export class UInt16 extends DataType {}
export class UInt32 extends DataType {}
export class UInt64 extends DataType {}
export class Float32 extends DataType {}
export class Float64 extends DataType {}
// biome-ignore lint/suspicious/noShadowRestrictedNames: <explanation>
export class Date extends DataType {}
export class Time extends DataType {}
export class Object_ extends DataType {}
export class Utf8 extends DataType {}
// biome-ignore lint/suspicious/noShadowRestrictedNames: <explanation>
export class String extends DataType {}

export class Categorical extends DataType {}

export class Decimal extends DataType {
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
export class Datetime extends DataType {
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

export class List extends DataType {
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

export class FixedSizeList extends DataType {
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

export class Struct extends DataType {
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
