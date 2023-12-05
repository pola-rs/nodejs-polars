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
 * Options for {@link DataFrame.writeCSV}
 * @category Options
 */
export interface WriteCsvOptions {
  includeHeader?: boolean;
  sep?: string;
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
  columns?: string[] | number[];
  numRows?: number;
  parallel?: "auto" | "columns" | "row_groups" | "none";
  rowCount?: RowCount;
  cache?: boolean;
  rechunk?: boolean;
}

/**
 * Add row count as column
 */
export interface RowCount {
  /** name of column */
  name: string;
  /** offset */
  offset: string;
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
export type JoinType = "left" | "inner" | "outer" | "semi" | "anti" | "cross";

/** @ignore */
export type JoinBaseOptions = {
  how?: JoinType;
  suffix?: string;
};
/**
 * options for join operations @see {@link DataFrame.join}
 */
export interface JoinOptions {
  /** left join column */
  leftOn?: string | Array<string>;
  /** right join column */
  rightOn?: string | Array<string>;
  /** left and right join column */
  on?: string | Array<string>;
  /** join type */
  how?: JoinType;
  suffix?: string;
}

/**
 * options for lazy join operations @see {@link LazyDataFrame.join}
 */
export interface LazyJoinOptions extends JoinOptions {
  allowParallel?: boolean;
  forceParallel?: boolean;
}

/**
 * options for lazy operations @see {@link LazyDataFrame.collect}
 */
export type LazyOptions = {
  typeCoercion?: boolean;
  predicatePushdown?: boolean;
  projectionPushdown?: boolean;
  simplifyExpression?: boolean;
  stringCache?: boolean;
  noOptimization?: boolean;
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
