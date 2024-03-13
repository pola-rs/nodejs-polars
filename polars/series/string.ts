import type { Expr } from "./../lazy/expr/index";
import type { DataType } from "../datatypes";
import { _Series, type Series } from ".";
import { regexToString } from "../utils";
import { col } from "../lazy/functions";
import type { StringFunctions } from "../shared_traits";

/**
 * namespace containing series string functions
 */
export interface StringNamespace extends StringFunctions<Series> {
  /**
   * Vertically concat the values in the Series to a single string value.
   * @example
   * ```
   * > pl.Series([1, null, 2]).str.concat("-")[0]
   * '1-null-2'
   * ```
   */
  concat(delimiter: string, ignoreNulls?: boolean): Series;
  /**
   * Check if strings in Series contain regex pattern.
   * @param pattern A valid regex pattern
   * @returns Boolean mask
   */
  contains(pattern: string | RegExp): Series;
  /**
   * Decodes a value using the provided encoding
   * @param encoding - hex | base64
   * @param strict - how to handle invalid inputs
   *
   *     - true: method will throw error if unable to decode a value
   *     - false: unhandled values will be replaced with `null`
   * @example
   * ```
   * s = pl.Series("strings", ["666f6f", "626172", null])
   * s.str.decode("hex")
   * shape: (3,)
   * Series: 'strings' [str]
   * [
   *     "foo",
   *     "bar",
   *     null
   * ]
   * ```
   */
  decode(encoding: "hex" | "base64", strict?: boolean): Series;
  decode(options: { encoding: "hex" | "base64"; strict?: boolean }): Series;
  /**
   * Encodes a value using the provided encoding
   * @param encoding - hex | base64
   * @example
   * ```
   * s = pl.Series("strings", ["foo", "bar", null])
   * s.str.encode("hex")
   * shape: (3,)
   * Series: 'strings' [str]
   * [
   *     "666f6f",
   *     "626172",
   *     null
   * ]
   * ```
   */
  encode(encoding: "hex" | "base64"): Series;
  /**
   * Extract the target capture group from provided patterns.
   * @param pattern A valid regex pattern
   * @param groupIndex Index of the targeted capture group.
   * Group 0 mean the whole pattern, first group begin at index 1
   * Default to the first capture group
   * @returns Utf8 array. Contain null if original value is null or regex capture nothing.
   * @example
   * ```
   * >  df = pl.DataFrame({
   * ...   'a': [
   * ...       'http://vote.com/ballon_dor?candidate=messi&ref=polars',
   * ...       'http://vote.com/ballon_dor?candidat=jorginho&ref=polars',
   * ...       'http://vote.com/ballon_dor?candidate=ronaldo&ref=polars'
   * ...   ]})
   * >  df.getColumn("a").str.extract(/candidate=(\w+)/, 1)
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
  extract(pattern: any, groupIndex: number): Series;
  /***
   * Parse string values as JSON.
   * @returns Utf8 array. Contain null if original value is null or the `jsonPath` return nothing.
   * @deprecated @since 0.8.4 @use {@link jsonDecode}
   * @example
   * s = pl.Series("json", ['{"a":1, "b": true}', null, '{"a":2, "b": false}']);
   * s.str.jsonExtract().as("json");
   * shape: (3,)
   * Series: 'json' [struct[2]]
   * [
   *     {1,true}
   *     {null,null}
   *     {2,false}
   * ]
   */
  jsonExtract(dtype?: DataType, inferSchemaLength?: number): Series;
  /***
   * Parse string values as JSON.
   * @returns Utf8 array. Contain null if original value is null or the `jsonPath` return nothing.
   * @example
   * s = pl.Series("json", ['{"a":1, "b": true}', null, '{"a":2, "b": false}']);
   * s.str.jsonDecode().as("json");
   * shape: (3,)
   * Series: 'json' [struct[2]]
   * [
   *     {1,true}
   *     {null,null}
   *     {2,false}
   * ]
   */
  jsonDecode(dtype?: DataType, inferSchemaLength?: number): Series;
  /**
   * Extract the first match of json string with provided JSONPath expression.
   * Throw errors if encounter invalid json strings.
   * All return value will be casted to Utf8 regardless of the original value.
   * @see https://goessner.net/articles/JsonPath/
   * @param jsonPath - A valid JSON path query string
   * @param dtype - The dtype to cast the extracted value to. If None, the dtype will be inferred from the JSON value.
   * @param inferSchemaLength - How many rows to parse to determine the schema. If ``None`` all rows are used.
   * @returns Utf8 array. Contain null if original value is null or the `jsonPath` return nothing.
   * @example
   * ```
   * > s = pl.Series('json_val', [
   * ...   '{"a":"1"}',
   * ...   null,
   * ...   '{"a":2}',
   * ...   '{"a":2.1}',
   * ...   '{"a":true}'
   * ... ])
   * > s.str.jsonPathMatch('$.a')
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
  jsonPathMatch(jsonPath: string): Series;
  /**  Get length of the string values in the Series. */
  lengths(): Series;
  /** Remove leading whitespace. */
  lstrip(): Series;
  /**
   *  Add a leading fillChar to a string until string length is reached.
   * If string is longer or equal to given length no modifications will be done
   * @param {number} length  - of the final string
   * @param {string} fillChar  - that will fill the string.
   * @note If a string longer than 1 character is provided only the first character will be used
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
  padStart(length: number, fillChar: string): Series;
  /**
   *  Add a leading '0' to a string until string length is reached.
   * If string is longer or equal to given length no modifications will be done
   * @param {number} length  - of the final string
   * @example
   * ```
   * > df = pl.DataFrame({
   * ...   'foo': [
   * ...       "a",
   * ...       "b",
   * ...       "LONG_WORD",
   * ...       "cow"
   * ...   ]})
   * > df.select(pl.col('foo').str.padStart(3)
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
  zFill(length: number | Expr): Series;
  /** Add trailing zeros */
  padEnd(length: number, fillChar: string): Series;
  /**
   * Replace first regex match with a string value.
   * @param pattern A valid regex pattern
   * @param value Substring to replace.
   */
  replace(pattern: string | RegExp, value: string): Series;

  /**
   * Replace all regex matches with a string value.
   * @param pattern - A valid regex pattern
   * @param value Substring to replace.
   */
  replaceAll(pattern: string | RegExp, value: string): Series;
  /** Modify the strings to their lowercase equivalent. */
  toLowerCase(): Series;
  /** Modify the strings to their uppercase equivalent. */
  toUpperCase(): Series;
  /** Remove trailing whitespace. */
  rstrip(): Series;
  /** Remove leading and trailing whitespace. */
  strip(): Series;
  /**
   * Create subslices of the string values of a Utf8 Series.
   * @param start - Start of the slice (negative indexing may be used).
   * @param length - Optional length of the slice.
   */
  slice(start: number | Expr, length?: number | Expr): Series;
  /**
   * Split a string into substrings using the specified separator.
   * The return type will by of type List<Utf8>
   * @param separator — A string that identifies character or characters to use in separating the string.
   * @param inclusive Include the split character/string in the results
   */
  split(separator: string, options?: { inclusive?: boolean } | boolean): Series;
  /**
   * Parse a Series of dtype Utf8 to a Date/Datetime Series.
   * @param datatype Date or Datetime.
   * @param fmt formatting syntax. [Read more](https://docs.rs/chrono/0.4.19/chrono/format/strptime/index.html)
   */
  strptime(datatype: DataType.Date, fmt?: string): Series;
  strptime(datatype: DataType.Datetime, fmt?: string): Series;
}

export const SeriesStringFunctions = (_s: any): StringNamespace => {
  const wrap = (method, ...args): any => {
    const ret = _s[method](...args);

    return _Series(ret);
  };

  const handleDecode = (encoding, strict) => {
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
      return _Series(_s)
        .toFrame()
        .select(col(_s.name).str.concat(delimiter, ignoreNulls).as(_s.name))
        .getColumn(_s.name);
    },
    contains(pat: string | RegExp) {
      return wrap("strContains", regexToString(pat), false);
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
      const s = _Series(_s);
      return s
        .toFrame()
        .select(col(s.name).str.extract(pat, groupIndex).as(s.name))
        .getColumn(s.name);
    },
    jsonExtract(dtype?: DataType, inferSchemaLength?: number) {
      return wrap("strJsonDecode", dtype, inferSchemaLength);
    },
    jsonDecode(dtype?: DataType, inferSchemaLength?: number) {
      return wrap("strJsonDecode", dtype, inferSchemaLength);
    },
    jsonPathMatch(pat: string) {
      return wrap("strJsonPathMatch", pat);
    },
    lengths() {
      return wrap("strLengths");
    },
    lstrip() {
      return wrap("strReplace", /^\s*/.source, "");
    },
    padStart(length: number, fillChar: string) {
      return wrap("strPadStart", length, fillChar);
    },
    zFill(length) {
      return _Series(_s)
        .toFrame()
        .select(col(_s.name).str.zFill(length).as(_s.name))
        .getColumn(_s.name);
    },
    padEnd(length: number, fillChar: string) {
      return wrap("strPadEnd", length, fillChar);
    },
    replace(pat: RegExp, val: string) {
      return wrap("strReplace", regexToString(pat), val);
    },
    replaceAll(pat: RegExp, val: string) {
      return wrap("strReplaceAll", regexToString(pat), val);
    },
    rstrip() {
      return wrap("strReplace", /[ \t]+$/.source, "");
    },
    slice(start, length?) {
      const s = _Series(_s);

      return s
        .toFrame()
        .select(col(s.name).str.slice(start, length).as(s.name))
        .getColumn(s.name);
    },
    split(by: string, options?) {
      const inclusive =
        typeof options === "boolean" ? options : options?.inclusive;
      const s = _Series(_s);

      return s
        .toFrame()
        .select(col(s.name).str.split(by, inclusive).as(s.name))
        .getColumn(s.name);
    },
    strip() {
      const s = _Series(_s);

      return s
        .toFrame()
        .select(col(s.name).str.strip().as(s.name))
        .getColumn(s.name);
    },
    strptime(dtype, fmt?) {
      const s = _Series(_s);

      return s
        .toFrame()
        .select(col(s.name).str.strptime(dtype, fmt).as(s.name))
        .getColumn(s.name);
    },
    toLowerCase() {
      return wrap("strToLowercase");
    },
    toUpperCase() {
      return wrap("strToUppercase");
    },
  };
};
