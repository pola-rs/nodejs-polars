import * as df from "./dataframe";
import { Field as _field, DataType } from "./datatypes";
import * as series from "./series";

export { DataType } from "./datatypes";

import * as cfg from "./cfg";
import * as func from "./functions";
import pli from "./internals/polars_internal";
import * as io from "./io";
import * as ldf from "./lazy/dataframe";

export * from "./cfg";
export * from "./dataframe";
export * from "./functions";
export * from "./io";
export * from "./lazy";
export * from "./lazy/dataframe";
export * from "./series";

import * as lazy from "./lazy";

export * from "./types";

import * as sql from "./sql";

export type { GroupBy } from "./groupby";

export namespace pl {
  export import Expr = lazy.Expr;
  export import DataFrame = df.DataFrame;
  export import LazyDataFrame = ldf.LazyDataFrame;
  export import Series = series.Series;
  export type LazyGroupBy = lazy.LazyGroupBy;
  export type When = lazy.When;
  export type Then = lazy.Then;
  export type ChainedWhen = lazy.ChainedWhen;
  export type ChainedThen = lazy.ChainedThen;
  export import Config = cfg.Config;

  export import Field = _field;
  export import repeat = func.repeat;
  export import concat = func.concat;

  // IO
  export import scanCSV = io.scanCSV;
  export import scanJson = io.scanJson;
  export import scanIPC = io.scanIPC;
  export import scanParquet = io.scanParquet;

  export import readRecords = io.readRecords;
  export import readCSV = io.readCSV;
  export import readIPC = io.readIPC;
  export import readIPCStream = io.readIPCStream;
  export import readJSON = io.readJSON;
  export import readParquet = io.readParquet;
  export import readAvro = io.readAvro;

  export import readCSVStream = io.readCSVStream;
  export import readJSONStream = io.readJSONStream;

  // lazy
  export import all = lazy.all;
  export import col = lazy.col;
  export import nth = lazy.nth;
  export import cols = lazy.cols;
  export import lit = lazy.lit;
  export import intRange = lazy.intRange;
  export import intRanges = lazy.intRanges;
  export import argSortBy = lazy.argSortBy;
  export import avg = lazy.avg;
  export import concatList = lazy.concatList;
  export import concatString = lazy.concatString;
  export import count = lazy.count;
  export import cov = lazy.cov;
  export import exclude = lazy.exclude;
  export import element = lazy.element;
  export import first = lazy.first;
  export import format = lazy.format;
  export import groups = lazy.groups;
  export import head = lazy.head;
  export import last = lazy.last;
  export import len = lazy.len;
  export import mean = lazy.mean;
  export import median = lazy.median;
  export import nUnique = lazy.nUnique;
  export import pearsonCorr = lazy.pearsonCorr;
  export import quantile = lazy.quantile;
  export import select = lazy.select;
  export import struct = lazy.struct;
  export import allHorizontal = lazy.allHorizontal;
  export import anyHorizontal = lazy.anyHorizontal;
  export import minHorizontal = lazy.minHorizontal;
  export import maxHorizontal = lazy.maxHorizontal;
  export import sumHorizontal = lazy.sumHorizontal;
  export import spearmanRankCorr = lazy.spearmanRankCorr;
  export import tail = lazy.tail;
  export import list = lazy.list;
  export import when = lazy.when;
  export const version = pli.version();

  export type Categorical = import("./datatypes").Categorical;
  export type Int8 = import("./datatypes").Int8;
  export type Int16 = import("./datatypes").Int16;
  export type Int32 = import("./datatypes").Int32;
  export type Int64 = import("./datatypes").Int64;
  export type UInt8 = import("./datatypes").UInt8;
  export type UInt16 = import("./datatypes").UInt16;
  export type UInt32 = import("./datatypes").UInt32;
  export type UInt64 = import("./datatypes").UInt64;
  export type Float32 = import("./datatypes").Float32;
  export type Float64 = import("./datatypes").Float64;
  export type Bool = import("./datatypes").Bool;
  export type Utf8 = import("./datatypes").Utf8;
  export type String = import("./datatypes").String;
  export type List = import("./datatypes").List;
  export type FixedSizeList = import("./datatypes").FixedSizeList;
  export type Date = import("./datatypes").Date;
  export type Datetime = import("./datatypes").Datetime;
  export type Time = import("./datatypes").Time;
  export type Duration = import("./datatypes").Duration;
  export type Object = import("./datatypes").Object_;
  export type Null = import("./datatypes").Null;
  export type Struct = import("./datatypes").Struct;
  export type Decimal = import("./datatypes").Decimal;

  export const Categorical = DataType.Categorical;
  export const Int8 = DataType.Int8;
  export const Int16 = DataType.Int16;
  export const Int32 = DataType.Int32;
  export const Int64 = DataType.Int64;
  export const UInt8 = DataType.UInt8;
  export const UInt16 = DataType.UInt16;
  export const UInt32 = DataType.UInt32;
  export const UInt64 = DataType.UInt64;
  export const Float32 = DataType.Float32;
  export const Float64 = DataType.Float64;
  export const Bool = DataType.Bool;
  export const Utf8 = DataType.Utf8;
  // biome-ignore lint/suspicious/noShadowRestrictedNames: pl.String
  export const String = DataType.String;
  export const List = DataType.List;
  export const FixedSizeList = DataType.FixedSizeList;
  // biome-ignore lint/suspicious/noShadowRestrictedNames: pl.Date
  export const Date = DataType.Date;
  export const Datetime = DataType.Datetime;
  export const Duration = DataType.Duration;
  export const Time = DataType.Time;
  // biome-ignore lint/suspicious/noShadowRestrictedNames: pl.Object
  export const Object = DataType.Object;
  export const Null = DataType.Null;
  export const Struct = DataType.Struct;
  export const Decimal = DataType.Decimal;

  /**
   * Run SQL queries against DataFrame/LazyFrame data.
   *
   * @experimental This functionality is considered **unstable**, although it is close to being
   * considered stable. It may be changed at any point without it being considered a breaking change.
   */
  export function SQLContext(
    frames?: Record<string, DataFrame | LazyDataFrame>,
  ): sql.SQLContext {
    return new sql.SQLContext(frames);
  }
}
export default pl;

// ------
// CommonJS compatibility.
// This allows the user to use any of the following
// `import * as pl from "nodejs-polars"`
// `import pl from "nodejs-polars"`.
// `const pl = require("nodejs-polars")`.
// `const { DataFrame, Series, } = require("nodejs-polars")`.
// ------

export import Expr = lazy.Expr;
export import DataFrame = df.DataFrame;
export import LazyDataFrame = ldf.LazyDataFrame;
export import Series = series.Series;
export type LazyGroupBy = lazy.LazyGroupBy;
export type When = lazy.When;
export type Then = lazy.Then;
export type ChainedWhen = lazy.ChainedWhen;
export type ChainedThen = lazy.ChainedThen;
export import Config = cfg.Config;
export import Field = _field;
export import repeat = func.repeat;
export import concat = func.concat;

// IO
export import scanCSV = io.scanCSV;
export import scanJson = io.scanJson;
export import scanIPC = io.scanIPC;
export import scanParquet = io.scanParquet;

export import readRecords = io.readRecords;
export import readCSV = io.readCSV;
export import readIPC = io.readIPC;
export import readIPCStream = io.readIPCStream;
export import readJSON = io.readJSON;
export import readParquet = io.readParquet;
export import readAvro = io.readAvro;

export import readCSVStream = io.readCSVStream;
export import readJSONStream = io.readJSONStream;

// lazy
export import col = lazy.col;
export import cols = lazy.cols;
export import lit = lazy.lit;
export import intRange = lazy.intRange;
export import intRanges = lazy.intRanges;
export import argSortBy = lazy.argSortBy;
export import avg = lazy.avg;
export import concatList = lazy.concatList;
export import concatString = lazy.concatString;
export import count = lazy.count;
export import cov = lazy.cov;
export import exclude = lazy.exclude;
export import element = lazy.element;
export import first = lazy.first;
export import format = lazy.format;
export import groups = lazy.groups;
export import head = lazy.head;
export import last = lazy.last;
export import mean = lazy.mean;
export import median = lazy.median;
export import nUnique = lazy.nUnique;
export import pearsonCorr = lazy.pearsonCorr;
export import quantile = lazy.quantile;
export import select = lazy.select;
export import struct = lazy.struct;
export import allHorizontal = lazy.allHorizontal;
export import anyHorizontal = lazy.anyHorizontal;
export import minHorizontal = lazy.minHorizontal;
export import maxHorizontal = lazy.maxHorizontal;
export import sumHorizontal = lazy.sumHorizontal;
export import spearmanRankCorr = lazy.spearmanRankCorr;
export import tail = lazy.tail;
export import list = lazy.list;
export import when = lazy.when;
export const version = pli.version();

export type Categorical = import("./datatypes").Categorical;
export type Int8 = import("./datatypes").Int8;
export type Int16 = import("./datatypes").Int16;
export type Int32 = import("./datatypes").Int32;
export type Int64 = import("./datatypes").Int64;
export type UInt8 = import("./datatypes").UInt8;
export type UInt16 = import("./datatypes").UInt16;
export type UInt32 = import("./datatypes").UInt32;
export type UInt64 = import("./datatypes").UInt64;
export type Float32 = import("./datatypes").Float32;
export type Float64 = import("./datatypes").Float64;
export type Bool = import("./datatypes").Bool;
export type Utf8 = import("./datatypes").Utf8;
export type String = import("./datatypes").String;
export type List = import("./datatypes").List;
export type FixedSizeList = import("./datatypes").FixedSizeList;
export type Date = import("./datatypes").Date;
export type Datetime = import("./datatypes").Datetime;
export type Time = import("./datatypes").Time;
export type Duration = import("./datatypes").Duration;
export type Object = import("./datatypes").Object_;
export type Null = import("./datatypes").Null;
export type Struct = import("./datatypes").Struct;
export type Decimal = import("./datatypes").Decimal;

export const Categorical = DataType.Categorical;
export const Int8 = DataType.Int8;
export const Int16 = DataType.Int16;
export const Int32 = DataType.Int32;
export const Int64 = DataType.Int64;
export const UInt8 = DataType.UInt8;
export const UInt16 = DataType.UInt16;
export const UInt32 = DataType.UInt32;
export const UInt64 = DataType.UInt64;
export const Float32 = DataType.Float32;
export const Float64 = DataType.Float64;
export const Bool = DataType.Bool;
export const Utf8 = DataType.Utf8;
// biome-ignore lint/suspicious/noShadowRestrictedNames: pl.String
export const String = DataType.String;
export const List = DataType.List;
export const FixedSizeList = DataType.FixedSizeList;
// biome-ignore lint/suspicious/noShadowRestrictedNames: pl.Date
export const Date = DataType.Date;
export const Datetime = DataType.Datetime;
export const Duration = DataType.Duration;
export const Time = DataType.Time;
// biome-ignore lint/suspicious/noShadowRestrictedNames: pl.Object
export const Object = DataType.Object;
export const Null = DataType.Null;
export const Struct = DataType.Struct;
export const Decimal = DataType.Decimal;

/**
 * Run SQL queries against DataFrame/LazyFrame data.
 *
 * @experimental This functionality is considered **unstable**, although it is close to being
 * considered stable. It may be changed at any point without it being considered a breaking change.
 */
export function SQLContext(
  frames?: Record<string, DataFrame | LazyDataFrame>,
): sql.SQLContext {
  return new sql.SQLContext(frames);
}
