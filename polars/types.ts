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
 * Round modes
 */
export type RoundMode = "halftoeven" | "halfawayfromzero";

/**
 * Options for {@link concat}
 */
export interface ConcatOptions {
  rechunk?: boolean;
  parallel?: boolean;
  how?:
    | "vertical"
    | "verticalRelaxed"
    | "horizontal"
    | "diagonal"
    | "diagonalRelaxed"
    | "align"
    | "alignInner"
    | "alignFull"
    | "alignLeft"
    | "alignRight";
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
  floatScientific?: boolean;
  floatPrecision?: number;
  decimalComma?: boolean;
  nullValue?: string;
  quoteStyle?: "always" | "necessary" | "non_numeric" | "never";
  compression?: "uncompressed" | "gzip" | "zstd";
  compressionLevel?: number;
  checkExtension?: boolean;
  maintainOrder?: boolean;
  cloudOptions?: Record<string, string | number | boolean>;
  syncOnClose?: "none" | "data" | "all";
  mkdir?: boolean;
}

export interface SinkOptions {
  syncOnClose: "none" | "data" | "all"; // Call sync when closing the file.
  maintainOrder: boolean; // The output file needs to maintain order of the data that comes in.
  mkdir: boolean; // Recursively create all the directories in the path.
}

/**
 * Options for @see {@link LazyDataFrame.sinkParquet }
 * @category Options
 */
export interface SinkParquetOptions {
  compression?:
    | "uncompressed"
    | "snappy"
    | "gzip"
    | "lzo"
    | "brotli"
    | "lz4"
    | "zstd";
  compressionLevel?: number;
  statistics?: boolean | "full" | ParquetStatisticsOptions;
  rowGroupSize?: number;
  dataPagesizeLimit?: number;
  maintainOrder?: boolean;
  cloudOptions?: Record<string, string | number | boolean>;
  syncOnClose?: "none" | "data" | "all"; // Call sync when closing the file.
  mkdir?: boolean; // Recursively create all the directories in the path.
}

/**
 * Per-statistic toggles for @see {@link SinkParquetOptions.statistics}
 * @category Options
 */
export interface ParquetStatisticsOptions {
  min?: boolean;
  max?: boolean;
  distinctCount?: boolean;
  nullCount?: boolean;
}
/**
 * Options for @see {@link LazyDataFrame.sinkNdJson}
 * @category Options
 */
export interface SinkJsonOptions {
  compression?: "uncompressed" | "gzip" | "zstd";
  compressionLevel?: number; // The compression level to use, typically 0-9.
  checkExtension?: boolean; // Whether to check if the filename matches the compression settings.
  cloudOptions?: Record<string, string | number | boolean>;
  syncOnClose?: "none" | "data" | "all"; // Call sync when closing the file.
  maintainOrder?: boolean; // The output file needs to maintain order of the data that comes in.
  mkdir?: boolean; // Recursively create all the directories in the path.
}
/**
 * Options for @see {@link LazyDataFrame.sinkIpc}
 * @category Options
 */
export interface SinkIpcOptions {
  compression?: "uncompressed" | "gzip" | "zstd";
  compatLevel?: "newest" | "oldest";
  cloudOptions?: Record<string, string | number | boolean>;
  syncOnClose?: "none" | "data" | "all"; // Call sync when closing the file.
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
  nRows?: number;
  parallel?: "auto" | "columns" | "row_groups" | "prefiltered" | "none";
  rowIndexName?: string;
  rowIndexOffset?: number;
}
/**
 * Options for {@link scanParquet}
 */
export interface ScanParquetOptions {
  nRows?: number;
  rowIndexName?: string;
  rowIndexOffset?: number;
  cache?: boolean;
  parallel?: "auto" | "columns" | "row_groups" | "prefiltered" | "none";
  glob?: boolean;
  hiddenFilePrefix?: string | string[];
  hivePartitioning?: boolean;
  schema?: unknown;
  hiveSchema?: unknown;
  tryParseHiveDates?: boolean;
  rechunk?: boolean;
  lowMemory?: boolean;
  useStatistics?: boolean;
  cloudOptions?: Record<string, string | number | boolean>;
  includeFilePaths?: string;
  missingColumns?: "insert" | "raise";
  /** @deprecated Use {@link missingColumns} instead. */
  allowMissingColumns?: boolean;
  extraColumns?: "ignore" | "raise";
  castOptions?: ScanCastOptions;
}

/**
 * Cast options applied when scanning files.
 * Options for @see {@link ScanParquetOptions.castOptions}
 * @category Options
 */
export interface ScanCastOptions {
  /**
   * Configuration for casting from integer types:
   * * `upcast`: Allow lossless casting to wider integer types.
   * * `allow-float`: Allow casting integers to float types.
   * * `forbid`: Raises an error if dtypes do not match (default).
   */
  integerCast?: IntegerCastOption | IntegerCastOption[];
  /**
   * Configuration for casting from float types:
   * * `upcast`: Allow casting to higher precision float types.
   * * `downcast`: Allow casting to lower precision float types.
   * * `forbid`: Raises an error if dtypes do not match (default).
   */
  floatCast?: FloatCastOption | FloatCastOption[];
  /**
   * Configuration for casting from datetime types:
   * * `nanosecond-downcast`: Allow nanosecond precision datetime to be downcasted
   *   to any lower precision.
   * * `microsecond-downcast`: Allow microsecond precision datetime to be
   *   downcasted to millisecond precision.
   * * `downcast`: Allow downcasting to any lower precision (convenience aggregate
   *   of `nanosecond-downcast` and `microsecond-downcast`).
   * * `convert-timezone`: Allow casting to a different timezone.
   * * `forbid`: Raises an error if dtypes do not match (default).
   */
  datetimeCast?: DatetimeCastOption | DatetimeCastOption[];
  /** Behavior when struct fields defined in the schema are missing from the data. Default -> 'raise' */
  missingStructFields?: "insert" | "raise";
  /** Behavior when extra struct fields outside the defined schema are encountered. Default -> 'raise' */
  extraStructFields?: "ignore" | "raise";
  /** Whether to allow casting categoricals to string. Default -> 'forbid' */
  categoricalToString?: "allow" | "forbid";
}

export type IntegerCastOption = "upcast" | "allow-float" | "forbid";
export type FloatCastOption = "upcast" | "downcast" | "forbid";
export type DatetimeCastOption =
  | "nanosecond-downcast"
  | "microsecond-downcast"
  | "downcast"
  | "convert-timezone"
  | "forbid";

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
/**
 * Which side of the join to build. @see {@link DataFrame.join}
 */
export type JoinBuildSide =
  | "auto"
  | "left"
  | "right"
  | "force_left"
  | "force_right";
/**
 * Which frame's row order to preserve in the output. @see {@link DataFrame.join}
 */
export type MaintainOrderJoin =
  | "none"
  | "left"
  | "right"
  | "left_right"
  | "right_left";
/**
 * Common join options shared by all join variants.
 */
type CommonJoinOptions = {
  /** Suffix to append to columns with a duplicate name. */
  suffix?: string;
  /**
   * Coalescing behavior (merging of join columns). Default: undefined
   * - **undefined** - *(Default)* Coalesce unless `how='full'` is specified.
   * - **true** - Always coalesce join columns.
   * - **false** - Never coalesce join columns.
   */
  coalesce?: boolean;
  /**
   * Checks if join is of specified type. Default: 'm:m'
   * Valid options: {'m:m', 'm:1', '1:m', '1:1'}
   * - **m:m** - *(Default)* Many-to-many. Does not result in checks.
   * - **1:1** - One-to-one. Checks if join keys are unique in both left and right datasets.
   * - **1:m** - One-to-many. Checks if join keys are unique in left dataset.
   * - **m:1** - Many-to-one. Checks if join keys are unique in right dataset.
   */
  validate?: string;
  /** Join on null values. When false, null values will never produce matches. Default: false */
  nullsEqual?: boolean;
  /**
   * Which frame's row order to preserve, if any.
   * One of {'none', 'left', 'right', 'left_right', 'right_left'}. Default: 'none'
   */
  maintainOrder?: MaintainOrderJoin;
  /**
   * Which side to prefer/force as the build side of the join.
   * One of {'auto', 'left', 'right', 'force_left', 'force_right'}. Default: 'auto'
   */
  buildSide?: JoinBuildSide;
};
export type SameNameColumnJoinOptions<
  L extends string = string,
  R extends string = string,
> = CommonJoinOptions & {
  /** Name(s) of the join columns in both DataFrames. */
  on: ValueOrArray<L & R>;
  /** Join strategy {'inner', 'left', 'right', 'full', 'semi', 'anti'}. Default: 'inner' */
  how?: Exclude<JoinType, "cross">;
};
/**
 * options for differently named column join @see {@link DataFrame.join}
 */
export type DifferentNameColumnJoinOptions<
  L extends string = string,
  R extends string = string,
> = CommonJoinOptions & {
  /** Name(s) of the left join column(s). */
  leftOn: ValueOrArray<L>;
  /** Name(s) of the right join column(s). */
  rightOn: ValueOrArray<R>;
  /** Join strategy {'inner', 'left', 'right', 'full', 'semi', 'anti'}. Default: 'inner' */
  how?: Exclude<JoinType, "cross">;
};
/**
 * options for cross join @see {@link DataFrame.join}
 */
export type CrossJoinOptions = CommonJoinOptions & {
  /** Join strategy */
  how: "cross";
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
 * options for lazy operations @see {@link LazyDataFrame.collectSync}
 */
export type CollectSyncOptions = {
  engine?: Engine;
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

/**
 * Engine types
 * @category Options
 * Options for {@link LazyDataFrame.collectSync}
 */
export type Engine = "auto" | "cpu" | "in-memory" | "streaming";
