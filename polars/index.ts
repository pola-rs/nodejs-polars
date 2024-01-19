import * as series from "./series";
import * as df from "./dataframe";
import { DataType, Field as _field } from "./datatypes";
import * as func from "./functions";
import * as io from "./io";
import * as cfg from "./cfg";
import * as ldf from "./lazy/dataframe";
import pli from "./internals/polars_internal";
export { DataType, Field, TimeUnit } from "./datatypes";
export * from "./series";
export { Expr } from "./lazy/expr";
export * from "./dataframe";
export * from "./functions";
export * from "./io";
export * from "./cfg";
export * from "./lazy/dataframe";
export * from "./lazy";
import * as lazy from "./lazy";
export * from "./types";
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
  export import Int8 = DataType.Int8;
  export import Int16 = DataType.Int16;
  export import Int32 = DataType.Int32;
  export import Int64 = DataType.Int64;
  export import UInt8 = DataType.UInt8;
  export import UInt16 = DataType.UInt16;
  export import UInt32 = DataType.UInt32;
  export import UInt64 = DataType.UInt64;
  export import Float32 = DataType.Float32;
  export import Float64 = DataType.Float64;
  export import Bool = DataType.Bool;
  export import Utf8 = DataType.Utf8;
  // biome-ignore lint/suspicious/noShadowRestrictedNames: pl.String
  export import String = DataType.String;
  export import List = DataType.List;
  // biome-ignore lint/suspicious/noShadowRestrictedNames: pl.Date
  export import Date = DataType.Date;
  export import Datetime = DataType.Datetime;
  export import Time = DataType.Time;
  // biome-ignore lint/suspicious/noShadowRestrictedNames: pl.Object
  export import Object = DataType.Object;
  export import Null = DataType.Null;
  export import Struct = DataType.Struct;
  export import Categorical = DataType.Categorical;
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
}
// eslint-disable-next-line no-undef
export default pl;
