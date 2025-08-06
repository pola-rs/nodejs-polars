import type { DataType } from "../datatypes";
import type { Expr } from "./../lazy/expr/index";
import { col } from "../lazy/functions";
import type { StringFunctions } from "../shared_traits";
import { regexToString } from "../utils";
import { _Series, type Series } from ".";

/**
 * String functions for Series
 */
export interface SeriesStringFunctions extends StringFunctions<Series> {
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
   * Check if strings in Series contain a substring that matches a pattern.
   * @param pat A valid regular expression pattern, compatible with the `regex crate
   * @param literal Treat `pattern` as a literal string, not as a regular expression.
   * @param strict Raise an error if the underlying pattern is not a valid regex, otherwise mask out with a null value.
   * @returns Boolean mask
   * @example
   * ```
   * > pl.Series(["Crab", "cat and dog", "rab$bit", null]).str.contains("cat|bit")
   * shape: (4,)
   * Series: '' [bool]
   * [
        false
        true
        true
        null
   * ]
   * ```
   */
  contains(pat: string | RegExp, literal?: boolean, strict?: boolean): Series;
  /**
   * Decodes a value in Series using the provided encoding
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
   * Encodes a value in Series using the provided encoding
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
  extract(pattern: string | RegExp, groupIndex: number): Series;
  /***
   * Parse string values in Series as JSON.
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
   * Extract the first match of json string in Series with provided JSONPath expression.
   * Throw errors if encounter invalid json strings.
   * All return value will be casted to Utf8 regardless of the original value.
   * @see https://goessner.net/articles/JsonPath/
   * @param jsonPath - A valid JSON path query string
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
  /**  Get number of chars of the string values in Series.
   * df = pl.Series(["Café", "345", "東京", null])
   *    .str.lengths().alias("n_chars")
   * shape: (4,)
   * Series: 'n_chars' [u32]
   * [
   *      4
   *      3
   *      2
   *      null
   * ]
   */
  lengths(): Series;
  /** Remove leading whitespace of the string values in Series. */
  lstrip(): Series;
  /**
   *  Add a leading fillChar to a string in Series until string length is reached.
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
   * Replace first regex match with a string value in Series.
   * @param pattern A valid regex pattern or string
   * @param value Substring to replace.
   * @example
   * ```
   * df = pl.Series(["#12.34", "#56.78"]).str.replace(/#(\d+)/, "$$$1")
   * shape: (2,)
   * Series: '' [str]
   * [
   *        "$12.34"
   *        "$56.78"
   * ]
   * ```
   */
  replace(pattern: string | RegExp, value: string): Series;
  /**
   * Replace all regex matches with a string value in Series.
   * @param pattern - A valid regex pattern or string
   * @param value Substring to replace.
   * @example
   * ```
   * df = pl.Series(["abcabc", "123a123"]).str.replaceAll("a", "-");
   * shape: (2,)
   * Series: '' [str]
   * [
   *         "-bc-bc"
   *         "123-123"
   * ]
   * ```
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
   * @param options.inclusive Include the split character/string in the results
   */
  split(separator: string, options?: { inclusive?: boolean } | boolean): Series;
  /**
   * Parse a Series of dtype Utf8 to a Date/Datetime Series.
   * @param datatype Date or Datetime.
   * @param fmt formatting syntax. [Read more](https://docs.rs/chrono/0.4.19/chrono/format/strptime/index.html)
   */
  strptime(datatype: DataType.Date, fmt?: string): Series<DataType.Date>;
  strptime(
    datatype: DataType.Datetime,
    fmt?: string,
  ): Series<DataType.Datetime>;
  strptime(datatype: typeof DataType.Datetime, fmt?: string): Series;
}

export const SeriesStringFunctions = (_s: any): SeriesStringFunctions => {
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
    contains(pat: string | RegExp, literal = false, strict = true) {
      return wrap("strContains", regexToString(pat as RegExp), literal, strict);
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
      return wrap("strReplace", /^\s*/.source, "");
    },
    padStart(length: number, fillChar: string) {
      return wrap("strPadStart", [length], fillChar);
    },
    zFill(length) {
      return _Series(_s)
        .toFrame()
        .select(col(_s.name).str.zFill(length).as(_s.name))
        .getColumn(_s.name);
    },
    padEnd(length: number, fillChar: string) {
      return wrap("strPadEnd", [length], fillChar);
    },
    replace(pat: string | RegExp, val: string) {
      return wrap("strReplace", regexToString(pat as RegExp), val);
    },
    replaceAll(pat: string | RegExp, val: string) {
      return wrap("strReplaceAll", regexToString(pat as RegExp), val);
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
