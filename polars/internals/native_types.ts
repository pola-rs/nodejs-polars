// Internal branded native handle types to keep "any" out of public APIs.
// These represent opaque handles returned by the native addon.

// Using `any` here keeps compile compatibility; this centralizes unsafety for future tightening.
export type NativeDataFrame = any;
export type NativeLazyFrame = any;
export type NativeExpr = any;
export type NativeSeries = any;
