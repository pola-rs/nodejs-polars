import { DataFrame } from ".";
import { LazyDataFrame } from "./lazy/dataframe";
import type { ValueOrArray } from "./utils";

/**
 * Downsample rules
 */
export type DownsampleRule =
  | "month"
  | "week"
  | "day"
  | "hour"
  | "minute"
  | "second";
/**
 * Fill null strategies
 */
export type FillNullStrategy =
  | "backward"
  | "forward"
  | "mean"
  | "min"
  | "max"
  | "zero"
  | "one";

/**
 * Rank methods
 */
export type RankMethod =
  | "average"
  | "min"
  | "max"
  | "dense"
  | "ordinal"
  | "random";

/**
 * Options for {@link concat}
 */
export interface ConcatOptions {
  rechunk?: boolean;
  how?: "vertical" | "horizontal" | "diagonal";
}
/**
 * Options for @see {@link DataFrame.writeCSV}
 * Options for @see {@link LazyDataFrame.sinkCSV}
 * @category Options
 */
export interface CsvWriterOptions {
  includeBom?: boolean;
  includeHeader?: boolean;
  separator?: string;
  quoteChar?: string;
  lineTerminator?: string;
  batchSize?: number;
  datetimeFormat?: string;
  dateFormat?: string;
  timeFormat?: string;
  floatPrecision?: number;
  nullValue?: string;
  maintainOrder?: boolean;
}

export interface SinkOptions {
  syncOnClose: any; // Call sync when closing the file.
  maintainOrder: boolean; // The output file needs to maintain order of the data that comes in.
  mkdir: boolean; // Recursively create all the directories in the path.
}

/**
 * Options for @see {@link LazyDataFrame.sinkParquet }
 * @category Options
 */
export interface SinkParquetOptions {
  compression?: string;
  compressionLevel?: number;
  statistics?: boolean;
  rowGroupSize?: number;
  dataPagesizeLimit?: number;
  maintainOrder?: boolean;
  typeCoercion?: boolean;
  predicatePushdown?: boolean;
  projectionPushdown?: boolean;
  simplifyExpression?: boolean;
  slicePushdown?: boolean;
  noOptimization?: boolean;
  cloudOptions?: Map<string, string>;
  retries?: number;
  sinkOptions?: SinkOptions;
}
/**
 * Options for @see {@link LazyDataFrame.sinkNdJson}
 * @category Options
 */
export interface SinkJsonOptions {
  cloudOptions?: Map<string, string>;
  retries?: number;
  syncOnClose?: string; // Call sync when closing the file.
  maintainOrder?: boolean; // The output file needs to maintain order of the data that comes in.
  mkdir?: boolean; // Recursively create all the directories in the path.
}
/**
 * Options for @see {@link LazyDataFrame.sinkIpc}
 * @category Options
 */
export interface SinkIpcOptions {
  compression?: string;
  compatLevel?: string;
  cloudOptions?: Map<string, string>;
  retries?: number;
  syncOnClose?: string; // Call sync when closing the file.
  maintainOrder?: boolean; // The output file needs to maintain order of the data that comes in.
  mkdir?: boolean; // Recursively create all the directories in the path.
}
/**
 * Options for {@link DataFrame.writeJSON}
 * @category Options
 */
export interface WriteJsonOptions {
  orient?: "row" | "col" | "dataframe";
  multiline?: boolean;
}

/**
 * Options for {@link scanJson}
 */
export interface JsonScanOptions {
  inferSchemaLength?: number;
  nThreads?: number;
  batchSize?: number;
  lowMemory?: boolean;
  numRows?: number;
  skipRows?: number;
  rowCount?: RowCount;
}

/**
 * Options for {@link DataFrame.writeParquet}
 * @category Options
 */
export interface WriteParquetOptions {
  compression?:
    | "uncompressed"
    | "snappy"
    | "gzip"
    | "lzo"
    | "brotli"
    | "lz4"
    | "zstd";
}
/**
 * Options for {@link readParquet}
 */
export interface ReadParquetOptions {
  columns?: string[] | number[];
  numRows?: number;
  parallel?: "auto" | "columns" | "row_groups" | "none";
  rowCount?: RowCount;
}
/**
 * Options for {@link scanParquet}
 */
export interface ScanParquetOptions {
  nRows?: number;
  rowIndexName?: string;
  rowIndexOffset?: number;
  cache?: boolean;
  parallel?: "auto" | "columns" | "row_groups" | "none";
  glob?: boolean;
  hivePartitioning?: boolean;
  hiveSchema?: unknown;
  tryParseHiveDates?: boolean;
  rechunk?: boolean;
  lowMemory?: boolean;
  useStatistics?: boolean;
  cloudOptions?: Map<string, string>;
  retries?: number;
  includeFilePaths?: string;
  allowMissingColumns?: boolean;
}

/**
 * Add row count as column
 */
export interface RowCount {
  /** name of column */
  name: string;
  /** offset */
  offset: number;
}

/**
 * Options for {@link DataFrame.writeIPC}
 * @category Options
 */
export interface WriteIPCOptions {
  compression?: "uncompressed" | "lz4" | "zstd";
}

/**
 * Options for writing Avro files
 * @category Options
 */
export interface WriteAvroOptions {
  compression?: "uncompressed" | "snappy" | "deflate";
}

/**
 * Interpolation types
 */
export type InterpolationMethod =
  | "nearest"
  | "higher"
  | "lower"
  | "midpoint"
  | "linear";

/**
 * Join types
 */
export type JoinType = "left" | "inner" | "full" | "semi" | "anti" | "cross";

/**
 * options for same named column join @see {@link DataFrame.join}
 */
export type SameNameColumnJoinOptions<
  L extends string = string,
  R extends string = string,
> = {
  /** Name(s) of the join columns in both DataFrames. */
  on: ValueOrArray<L & R>;
  /** Join strategy */
  how?: Exclude<JoinType, "cross">;
  /** Suffix to append to columns with a duplicate name. */
  suffix?: string;
};
/**
 * options for differently named column join @see {@link DataFrame.join}
 */
export type DifferentNameColumnJoinOptions<
  L extends string = string,
  R extends string = string,
> = {
  /** Name(s) of the left join column(s). */
  leftOn: ValueOrArray<L>;
  /** Name(s) of the right join column(s). */
  rightOn: ValueOrArray<R>;
  /** Join strategy */
  how?: Exclude<JoinType, "cross">;
  /** Suffix to append to columns with a duplicate name. */
  suffix?: string;
};
/**
 * options for cross join @see {@link DataFrame.join}
 */
export type CrossJoinOptions = {
  /** Join strategy */
  how: "cross";
  /** Suffix to append to columns with a duplicate name. */
  suffix?: string;
};
/**
 * options for join operations @see {@link DataFrame.join}
 */
export type JoinOptions<L extends string = string, R extends string = string> =
  | SameNameColumnJoinOptions<L, R>
  | DifferentNameColumnJoinOptions<L, R>
  | CrossJoinOptions;

type LazyJoinBase = {
  /** Allow the physical plan to optionally evaluate the computation of both DataFrames up to the join in parallel. */
  allowParallel?: boolean;
  /** Force the physical plan to evaluate the computation of both DataFrames up to the join in parallel. */
  forceParallel?: boolean;
};
export type LazySameNameColumnJoinOptions<
  L extends string = string,
  R extends string = string,
> = SameNameColumnJoinOptions<L, R> & LazyJoinBase;
export type LazyDifferentNameColumnJoinOptions<
  L extends string = string,
  R extends string = string,
> = DifferentNameColumnJoinOptions<L, R> & LazyJoinBase;
export type LazyCrossJoinOptions = CrossJoinOptions & LazyJoinBase;
/**
 * options for lazy join operations @see {@link LazyDataFrame.join}
 */
export type LazyJoinOptions<
  L extends string = string,
  R extends string = string,
> =
  | LazySameNameColumnJoinOptions<L, R>
  | LazyDifferentNameColumnJoinOptions<L, R>
  | LazyCrossJoinOptions;

/**
 * options for lazy operations @see {@link LazyDataFrame.collect}
 */
export type LazyOptions = {
  typeCoercion?: boolean;
  predicatePushdown?: boolean;
  projectionPushdown?: boolean;
  simplifyExpression?: boolean;
  slicePushdown?: boolean;
  noOptimization?: boolean;
  commSubplanElim?: boolean;
  commSubexprElim?: boolean;
  streaming?: boolean;
};

/**
 * options for rolling window operations
 * @category Options
 */
export interface RollingOptions {
  windowSize: number;
  weights?: Array<number>;
  minPeriods?: number;
  center?: boolean;
  ddof?: number;
}

/**
 * options for rolling quantile operations
 * @category Options
 */
export interface RollingQuantileOptions extends RollingOptions {
  quantile: number;
  interpolation?: InterpolationMethod;
}

/**
 * options for rolling mean operations
 * @category Options
 */
export interface RollingSkewOptions {
  windowSize: number;
  bias?: boolean;
}

/**
 * ClosedWindow types
 */
export type ClosedWindow = "None" | "Both" | "Left" | "Right";
