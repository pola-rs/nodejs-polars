import type { DataType } from "./datatype";

export type JsToDtype<T> = T extends bigint
  ? DataType.UInt64
  : T extends number
    ? DataType.Float64
    : T extends boolean
      ? DataType.Bool
      : T extends string
        ? DataType.String
        : T extends Date
          ? DataType.Datetime
          : never;
