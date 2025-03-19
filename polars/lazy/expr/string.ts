import { DataType } from "../../datatypes";
import type { StringFunctions } from "../../shared_traits";
import { regexToString } from "../../utils";
import { Expr, _Expr, exprToLitOrExpr } from "../expr";
import { lit } from "../functions";

/**
 * namespace containing expr string functions
 */
export interface StringNamespace extends StringFunctions<Expr> {
  /**
   * Vertically concat the values in the Series to a single string value.
   * @example
   * ```
   * >>> df = pl.DataFrame({"foo": [1, null, 2]})
   * >>> df = df.select(pl.col("foo").str.concat("-"))
   * >>> df
   * shape: (1, 1)
   * ┌──────────┐
   * │ foo      │
   * │ ---      │
   * │ str      │
   * ╞══════════╡
   * │ 1-null-2 │
   * └──────────┘
   * ```
   */
  concat(delimiter: string, ignoreNulls?: boolean): Expr;
  /**
   * Check if strings in Series contain a substring that matches a pattern.
   * @param pat A valid regular expression pattern, compatible with the `regex crate
   * @param literal Treat `pattern` as a literal string, not as a regular expression.
   * @param strict Raise an error if the underlying pattern is not a valid regex, otherwise mask out with a null value.
   * @returns Boolean mask
   * @example
   * ```
   * const df = pl.DataFrame({"txt": ["Crab", "cat and dog", "rab$bit", null]})
   * df.select(
   * ...     pl.col("txt"),
   * ...     pl.col("txt").str.contains("cat|bit").alias("regex"),
   * ...     pl.col("txt").str.contains("rab$", true).alias("literal"),
   * ... )
   * shape: (4, 3)
   * ┌─────────────┬───────┬─────────┐
   * │ txt         ┆ regex ┆ literal │
   * │ ---         ┆ ---   ┆ ---     │
   * │ str         ┆ bool  ┆ bool    │
   * ╞═════════════╪═══════╪═════════╡
   * │ Crab        ┆ false ┆ false   │
   * │ cat and dog ┆ true  ┆ false   │
   * │ rab$bit     ┆ true  ┆ true    │
   * │ null        ┆ null  ┆ null    │
   * └─────────────┴───────┴─────────┘
   * ```
   */
  contains(
    pat: string | RegExp | Expr,
    literal?: boolean,
    strict?: boolean,
  ): Expr;
  /**
   * Decodes a value using the provided encoding
   * @param encoding - hex | base64
   * @param strict - how to handle invalid inputs
   *
   *     - true: method will throw error if unable to decode a value
   *     - false: unhandled values will be replaced with `null`
   * @example
   * ```
   * >>> df = pl.DataFrame({"strings": ["666f6f", "626172", null]})
   * >>> df.select(col("strings").str.decode("hex"))
   * shape: (3, 1)
   * ┌─────────┐
   * │ strings │
   * │ ---     │
   * │ str     │
   * ╞═════════╡
   * │ foo     │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ bar     │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ null    │
   * └─────────┘
   * ```
   */
  decode(encoding: "hex" | "base64", strict?: boolean): Expr;
  decode(options: { encoding: "hex" | "base64"; strict?: boolean }): Expr;
  /**
   * Encodes a value using the provided encoding
   * @param encoding - hex | base64
   * @example
   * ```
   * >>> df = pl.DataFrame({"strings", ["foo", "bar", null]})
   * >>> df.select(col("strings").str.encode("hex"))
   * shape: (3, 1)
   * ┌─────────┐
   * │ strings │
   * │ ---     │
   * │ str     │
   * ╞═════════╡
   * │ 666f6f  │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ 626172  │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ null    │
   * └─────────┘
   * ```
   */
  encode(encoding: "hex" | "base64"): Expr;
  /**
   * Extract the target capture group from provided patterns.
   * @param pattern A valid regex pattern
   * @param groupIndex Index of the targeted capture group.
   * Group 0 mean the whole pattern, first group begin at index 1
   * Default to the first capture group
   * @returns Utf8 array. Contain null if original value is null or regex capture nothing.
   * @example
   * ```
   * > df = pl.DataFrame({
   * ...   'a': [
   * ...       'http://vote.com/ballon_dor?candidate=messi&ref=polars',
   * ...       'http://vote.com/ballon_dor?candidat=jorginho&ref=polars',
   * ...       'http://vote.com/ballon_dor?candidate=ronaldo&ref=polars'
   * ...   ]})
   * > df.select(pl.col('a').str.extract(/candidate=(\w+)/, 1))
   * shape: (3, 1)
   * ┌─────────┐
   * │ a       │
   * │ ---     │
   * │ str     │
   * ╞═════════╡
   * │ messi   │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ null    │
   * ├╌╌╌╌╌╌╌╌╌┤
   * │ ronaldo │
   * └─────────┘
   * ```
   */
  extract(pat: any, groupIndex: number): Expr;
  /**
  * Parse string values as JSON.
  * Throw errors if encounter invalid JSON strings.
  * @returns DF with struct
  * @example

  * >>> df = pl.DataFrame( {json: ['{"a":1, "b": true}', null, '{"a":2, "b": false}']} )
  * >>> df.select(pl.col("json").str.jsonDecode())
  * shape: (3, 1)
  * ┌─────────────┐
  * │ json        │
  * │ ---         │
  * │ struct[2]   │
  * ╞═════════════╡
  * │ {1,true}    │
  * │ {null,null} │
  * │ {2,false}   │
  * └─────────────┘
  * See Also
  * ----------
  * jsonPathMatch : Extract the first match of json string with provided JSONPath expression.
  */
  jsonDecode(dtype?: DataType, inferSchemaLength?: number): Expr;
  /**
   * Extract the first match of json string with provided JSONPath expression.
   * Throw errors if encounter invalid json strings.
   * All return value will be casted to Utf8 regardless of the original value.
   * @see https://goessner.net/articles/JsonPath/
   * @param jsonPath - A valid JSON path query string
   * @param dtype - The dtype to cast the extracted value to. If None, the dtype will be inferred from the JSON value.
   * @param inferSchemaLength - How many rows to parse to determine the schema. If `null` all rows are used.
   * @returns Utf8 array. Contain null if original value is null or the `jsonPath` return nothing.
   * @example
   * ```
   * >>> df = pl.DataFrame({
   * ...   'json_val': [
   * ...     '{"a":"1"}',
   * ...     null,
   * ...     '{"a":2}',
   * ...     '{"a":2.1}',
   * ...     '{"a":true}'
   * ...   ]
   * ... })
   * >>> df.select(pl.col('json_val').str.jsonPathMatch('$.a')
   * shape: (5,)
   * Series: 'json_val' [str]
   * [
   *     "1"
   *     null
   *     "2"
   *     "2.1"
   *     "true"
   * ]
   * ```
   */
  jsonPathMatch(pat: string): Expr;
  /**  Get length of the string values in the Series. */
  lengths(): Expr;
  /** Remove leading whitespace. */
  lstrip(): Expr;
  /** Replace first regex match with a string value. */
  replace(pat: string | RegExp, val: string): Expr;
  /** Replace all regex matches with a string value. */
  replaceAll(pat: string | RegExp, val: string): Expr;
  /** Modify the strings to their lowercase equivalent. */
  toLowerCase(): Expr;
  /** Modify the strings to their uppercase equivalent. */
  toUpperCase(): Expr;
  /** Remove trailing whitespace. */
  rstrip(): Expr;
  /**
   *  Add a leading fillChar to a string until string length is reached.
   * If string is longer or equal to given length no modifications will be done
   * @param {number} length  - of the final string
   * @param {string} fillChar  - that will fill the string.
   * If a string longer than 1 character is provided only the first character will be used
   * @example
   * ```
   * > df = pl.DataFrame({
   * ...   'foo': [
   * ...       "a",
   * ...       "b",
   * ...       "LONG_WORD",
   * ...       "cow"
   * ...   ]})
   * > df.select(pl.col('foo').str.padStart("_", 3)
   * shape: (4, 1)
   * ┌──────────┐
   * │ a        │
   * │ -------- │
   * │ str      │
   * ╞══════════╡
   * │ __a      │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ __b      │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ LONG_WORD│
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ cow      │
   * └──────────┘
   * ```
   */
  padStart(length: number, fillChar: string): Expr;
  /**
   *  Add  leading "0" to a string until string length is reached.
   * If string is longer or equal to given length no modifications will be done
   * @param {number} length  - of the final string
   * @see {@link padStart}
   *    * @example
   * ```
   * > df = pl.DataFrame({
   * ...   'foo': [
   * ...       "a",
   * ...       "b",
   * ...       "LONG_WORD",
   * ...       "cow"
   * ...   ]})
   * > df.select(pl.col('foo').str.justify(3)
   * shape: (4, 1)
   * ┌──────────┐
   * │ a        │
   * │ -------- │
   * │ str      │
   * ╞══════════╡
   * │ 00a      │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ 00b      │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ LONG_WORD│
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ cow      │
   * └──────────┘
   * ```
   */
  zFill(length: number | Expr): Expr;
  /**
   *  Add a trailing fillChar to a string until string length is reached.
   * If string is longer or equal to given length no modifications will be done
   * @param {number} length  - of the final string
   * @param {string} fillChar  - that will fill the string.
   * If a string longer than 1 character is provided only the first character will be used
   *    * @example
   * ```
   * > df = pl.DataFrame({
   * ...   'foo': [
   * ...       "a",
   * ...       "b",
   * ...       "LONG_WORD",
   * ...       "cow"
   * ...   ]})
   * > df.select(pl.col('foo').str.padEnd("_", 3)
   * shape: (4, 1)
   * ┌──────────┐
   * │ a        │
   * │ -------- │
   * │ str      │
   * ╞══════════╡
   * │ a__      │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ b__      │
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ LONG_WORD│
   * ├╌╌╌╌╌╌╌╌╌╌┤
   * │ cow      │
   * └──────────┘
   * ```
   */
  padEnd(length: number, fillChar: string): Expr;
  /**
   * Create subslices of the string values of a Utf8 Series.
   * @param start - Start of the slice (negative indexing may be used).
   * @param length - Optional length of the slice.
   */
  slice(start: number | Expr, length?: number | Expr): Expr;
  /**
   * Split a string into substrings using the specified separator and return them as a Series.
   * @param separator — A string that identifies character or characters to use in separating the string.
   * @param inclusive Include the split character/string in the results
   */
  split(by: string, options?: { inclusive?: boolean } | boolean): Expr;
  /** Remove leading and trailing whitespace. */
  strip(): Expr;
  /**
   * Parse a Series of dtype Utf8 to a Date/Datetime Series.
   * @param datatype Date or Datetime.
   * @param fmt formatting syntax. [Read more](https://docs.rs/chrono/0.4.19/chrono/format/strftime/index.html)
   */
  strptime(datatype: DataType.Date, fmt?: string): Expr;
  strptime(datatype: DataType.Datetime, fmt?: string): Expr;
  strptime(datatype: typeof DataType.Datetime, fmt?: string): Expr;
}

export const ExprStringFunctions = (_expr: any): StringNamespace => {
  const wrap = (method, ...args: any[]): Expr => {
    return _Expr(_expr[method](...args));
  };

  const handleDecode = (encoding, strict: boolean) => {
    switch (encoding) {
      case "hex":
        return wrap("strHexDecode", strict);
      case "base64":
        return wrap("strBase64Decode", strict);
      default:
        throw new RangeError("supported encodings are 'hex' and 'base64'");
    }
  };

  return {
    concat(delimiter: string, ignoreNulls = true) {
      return wrap("strConcat", delimiter, ignoreNulls);
    },
    contains(pat: string | Expr, literal = false, strict = true) {
      return wrap("strContains", exprToLitOrExpr(pat)._expr, literal, strict);
    },
    decode(arg, strict = false) {
      if (typeof arg === "string") {
        return handleDecode(arg, strict);
      }

      return handleDecode(arg.encoding, arg.strict);
    },
    encode(encoding) {
      switch (encoding) {
        case "hex":
          return wrap("strHexEncode");
        case "base64":
          return wrap("strBase64Encode");
        default:
          throw new RangeError("supported encodings are 'hex' and 'base64'");
      }
    },
    extract(pat: any, groupIndex: number) {
      return wrap("strExtract", exprToLitOrExpr(pat, true)._expr, groupIndex);
    },
    jsonDecode(dtype?: DataType, inferSchemaLength?: number) {
      return wrap("strJsonDecode", dtype, inferSchemaLength);
    },
    jsonPathMatch(pat: string) {
      return wrap("strJsonPathMatch", [pat]);
    },
    lengths() {
      return wrap("strLengths");
    },
    lstrip() {
      return wrap("strLstrip");
    },
    replace(pat: RegExp, val: string) {
      return wrap("strReplace", regexToString(pat), val);
    },
    replaceAll(pat: RegExp, val: string) {
      return wrap("strReplaceAll", regexToString(pat), val);
    },
    rstrip() {
      return wrap("strRstrip");
    },
    padStart(length: number, fillChar: string) {
      return wrap("strPadStart", length, fillChar);
    },
    zFill(length: number | Expr) {
      if (!Expr.isExpr(length)) {
        length = lit(length)._expr;
      }
      return wrap("zfill", length);
    },
    padEnd(length: number, fillChar: string) {
      return wrap("strPadEnd", length, fillChar);
    },
    slice(start, length?) {
      if (!Expr.isExpr(start)) {
        start = lit(start)._expr;
      }
      if (!Expr.isExpr(length)) {
        length = lit(length)._expr;
      }

      return wrap("strSlice", start, length);
    },
    split(by: string, options?) {
      const inclusive =
        typeof options === "boolean" ? options : options?.inclusive;
      return wrap("strSplit", exprToLitOrExpr(by)._expr, inclusive);
    },
    strip() {
      return wrap("strStrip");
    },
    strptime(
      dtype: DataType.Date | DataType.Datetime | typeof DataType.Datetime,
      format?: string,
    ) {
      const dt = dtype instanceof DataType ? dtype : dtype();
      if (dt.equals(DataType.Date)) {
        return wrap("strToDate", format, false, false, false);
      }
      if (dt.equals(DataType.Datetime("ms"))) {
        return wrap(
          "strToDatetime",
          format,
          undefined,
          undefined,
          false,
          false,
          false,
        );
      }
      throw new Error(
        `only "DataType.Date" and "DataType.Datetime" are supported`,
      );
    },
    toLowerCase() {
      return wrap("strToLowercase");
    },
    toUpperCase() {
      return wrap("strToUppercase");
    },
  };
};
