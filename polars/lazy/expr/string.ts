import { DataType } from "../../datatypes";
import type { StringFunctions } from "../../shared_traits";
import { _Expr, Expr, exprToLitOrExpr } from "../expr";
import { lit } from "../functions";

/**
 * String functions for Lazy dataframes
 */
export interface ExprString extends StringFunctions<Expr> {
  /**
   * Vertically concat the values in the Expression to a single string value.
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
   * Check if strings in Expression contain a substring that matches a pattern.
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
   * Decodes a value in Expression using the provided encoding
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
   * Encodes a value in Expression using the provided encoding
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
  /** Check if string values in Expression ends with a substring.
   * @param suffix - Suffix substring or expression
   * @example
   * ```
   * >>> df = pl.DataFrame({"fruits": ["apple", "mango", None]})
   * >>> df.withColumns(
   * ...     pl.col("fruits").str.endsWith("go").alias("has_suffix"),
   * ... )
   * shape: (3, 2)
   * ┌────────┬────────────┐
   * │ fruits ┆ has_suffix │
   * │ ---    ┆ ---        │
   * │ str    ┆ bool       │
   * ╞════════╪════════════╡
   * │ apple  ┆ false      │
   * │ mango  ┆ true       │
   * │ null   ┆ null       │
   * └────────┴────────────┘
   *
   * >>> df = pl.DataFrame(
   * ...     {"fruits": ["apple", "mango", "banana"], "suffix": ["le", "go", "nu"]}
   * ... )
   * >>> df.withColumns(
   * ...     pl.col("fruits").str.endsWith(pl.col("suffix")).alias("has_suffix"),
   * ... )
   * shape: (3, 3)
   * ┌────────┬────────┬────────────┐
   * │ fruits ┆ suffix ┆ has_suffix │
   * │ ---    ┆ ---    ┆ ---        │
   * │ str    ┆ str    ┆ bool       │
   * ╞════════╪════════╪════════════╡
   * │ apple  ┆ le     ┆ true       │
   * │ mango  ┆ go     ┆ true       │
   * │ banana ┆ nu     ┆ false      │
   * └────────┴────────┴────────────┘
   *
   *    Using `ends_with` as a filter condition:
   *
   * >>> df.filter(pl.col("fruits").str.endsWith("go"))
   * shape: (1, 2)
   * ┌────────┬────────┐
   * │ fruits ┆ suffix │
   * │ ---    ┆ ---    │
   * │ str    ┆ str    │
   * ╞════════╪════════╡
   * │ mango  ┆ go     │
   * └────────┴────────┘
   * ```
   */
  endsWith(suffix: string | Expr): Expr;
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
  extract(pattern: string | RegExp | Expr, groupIndex: number): Expr;
  /**
   * Parse string values in Expression as JSON.
   * Throw errors if encounter invalid JSON strings.
   * @param dtype The dtype to cast the extracted value to. If None, the dtype will be inferred from the JSON value.
   * @param inferSchemaLength The maximum number of rows to scan for schema inference.
   * @returns DF with struct
   * @example
   * ```
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
  jsonDecode(dtype?: DataType): Expr;
  /**
   * Extract the first match of json string in Expression with provided JSONPath expression.
   * Throw errors if encounter invalid json strings.
   * All return value will be casted to Utf8 regardless of the original value.
   * @see https://goessner.net/articles/JsonPath/
   * @param pat - A valid JSON path query string
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
   * shape: (5, 1)
   *┌──────────┐
   *│ json_val │
   *│ ---      │
   *│ str      │
   *╞══════════╡
   *│ 1        │
   *│ null     │
   *│ 2        │
   *│ 2.1      │
   *│ true     │
   *└──────────┘
   * ```
   */
  jsonPathMatch(pat: string): Expr;
  /**  Get number of chars of the string values in Expression.
   * ```
   * df = pl.DataFrame({"a": ["Café", "345", "東京", null]})
   * df.withColumns(
   *    pl.col("a").str.lengths().alias("n_chars"),
   * )
   * shape: (4, 3)
   * ┌──────┬─────────┬─────────┐
   * │ a    ┆ n_chars ┆ n_bytes │
   * │ ---  ┆ ---     ┆ ---     │
   * │ str  ┆ u32     ┆ u32     │
   * ╞══════╪═════════╪═════════╡
   * │ Café ┆ 4       ┆ 5       │
   * │ 345  ┆ 3       ┆ 3       │
   * │ 東京 ┆ 2       ┆ 6       │
   * │ null ┆ null    ┆ null    │
   * └──────┴─────────┴─────────┘
   * ```
   */
  lengths(): Expr;
  /** Remove leading whitespace of the string values in Expression. */
  lstrip(): Expr;
  /** Replace first match with a string value in Expression.
   * @param pattern - A valid regex pattern, string or expression
   * @param value Substring or expression to replace.
   * @param literal Treat pattern as a literal string.
   * Note: pattern as expression is not yet supported by polars
   * @example
   * ```
   * df = pl.DataFrame({"cost": ["#12.34", "#56.78"], "text": ["123abc", "abc456"]})
   * df = df.withColumns(
   *     pl.col("cost").str.replace(/#(\d+)/, "$$$1"),
   *     pl.col("text").str.replace("ab", "-")
   *     pl.col("text").str.replace("abc", pl.col("cost")).alias("expr")
   * );
   * shape: (2, 2)
   * ┌────────┬───────┬───────────┐
   * │ cost   ┆ text  │ expr      │
   * │ ---    ┆ ---   │ ---       │
   * │ str    ┆ str   │ str       │
   * ╞════════╪═══════╪═══════════╡
   * │ $12.34 ┆ 123-c │ 123#12.34 │
   * │ $56.78 ┆ -c456 │ #56.78456 │
   * └────────┴───────┴───────────┘
   * ```
   */
  replace(
    pattern: string | RegExp | Expr,
    value: string | Expr,
    literal?: boolean,
    n?: number,
  ): Expr;
  /** Replace all regex matches with a string value in Expression.
   * @param pattern - A valid regex pattern, string or expression
   * @param value Substring or expression to replace.
   * @param literal Treat pattern as a literal string.
   * Note: pattern as expression is not yet supported by polars
   * @example
   * ```
   * df = df = pl.DataFrame({"weather": ["Rainy", "Sunny", "Cloudy", "Snowy"], "text": ["abcabc", "123a123", null, null]})
   * df = df.withColumns(
   *     pl.col("weather").str.replaceAll(/foggy|rainy/i, "Sunny"),
   *     pl.col("text").str.replaceAll("a", "-")
   * )
   * shape: (4, 2)
   * ┌─────────┬─────────┐
   * │ weather ┆ text    │
   * │ ---     ┆ ---     │
   * │ str     ┆ str     │
   * ╞═════════╪═════════╡
   * │ Sunny   ┆ -bc-bc  │
   * │ Sunny   ┆ 123-123 │
   * │ Cloudy  ┆ null    │
   * │ Snowy   ┆ null    │
   * └─────────┴─────────┘
   * ```
   */
  replaceAll(
    pattern: string | RegExp | Expr,
    value: string | Expr,
    literal?: boolean,
  ): Expr;
  /** Modify the string in Expression to their lowercase equivalent. */
  toLowerCase(): Expr;
  /** Modify the string in Expression to their uppercase equivalent. */
  toUpperCase(): Expr;
  /** Remove trailing whitespace. */
  rstrip(): Expr;
  /**
   *  Add a leading fillChar to a string in Expression until string length is reached.
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
   * @example
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
   * Note: If a string longer than 1 character is provided only the first character will be used
   * @example
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
   * @param by — A string that identifies character or characters to use in separating the string.
   * @param options.inclusive Include the split character/string in the results
   */
  split(by: string, options?: { inclusive?: boolean } | boolean): Expr;
  /** Check if string values start with a substring.
   * @param prefix - Prefix substring or expression
   * @example
   * ```
   * >>> df = pl.DataFrame({"fruits": ["apple", "mango", None]})
   * >>> df.withColumns(
   * ...     pl.col("fruits").str.startsWith("app").alias("has_prefix"),
   * ... )
   * shape: (3, 2)
   * ┌────────┬────────────┐
   * │ fruits ┆ has_prefix │
   * │ ---    ┆ ---        │
   * │ str    ┆ bool       │
   * ╞════════╪════════════╡
   * │ apple  ┆ true       │
   * │ mango  ┆ false      │
   * │ null   ┆ null       │
   * └────────┴────────────┘
   *
   * >>> df = pl.DataFrame(
   * ...     {"fruits": ["apple", "mango", "banana"], "prefix": ["app", "na", "ba"]}
   * ... )
   * >>> df.withColumns(
   * ...     pl.col("fruits").str.startsWith(pl.col("prefix")).alias("has_prefix"),
   * ... )
   * shape: (3, 3)
   * ┌────────┬────────┬────────────┐
   * │ fruits ┆ prefix ┆ has_prefix │
   * │ ---    ┆ ---    ┆ ---        │
   * │ str    ┆ str    ┆ bool       │
   * ╞════════╪════════╪════════════╡
   * │ apple  ┆ app    ┆ true       │
   * │ mango  ┆ na     ┆ false      │
   * │ banana ┆ ba     ┆ true       │
   * └────────┴────────┴────────────┘
   *
   *    Using `starts_with` as a filter condition:
   *
   * >>> df.filter(pl.col("fruits").str.startsWith("app"))
   * shape: (1, 2)
   * ┌────────┬────────┐
   * │ fruits ┆ prefix │
   * │ ---    ┆ ---    │
   * │ str    ┆ str    │
   * ╞════════╪════════╡
   * │ apple  ┆ app    │
   * └────────┴────────┘
   * ```
   */
  startsWith(prefix: string | Expr): Expr;
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

  /** Remove leading and trailing whitespace.
   * @param prefix - Prefix substring or expression (null means whitespace)
   * @example
   * ```
   * >>> df = pl.DataFrame({
   *         os: [
   *           "#Kali-Linux###",
   *           "$$$Debian-Linux$",
   *           null,
   *           "Ubuntu-Linux    ",
   *           "  Mac-Sierra",
   *         ],
   *         chars: ["#", "$", " ", " ", null],
   *       })
   * >>> df.select(col("os").str.stripChars(col("chars")).as("os"))
   * shape: (5, 1)
   *      ┌──────────────┐
   *      │ os           │
   *      │ ---          │
   *      │ str          │
   *      ╞══════════════╡
   *      │ Kali-Linux   │
   *      │ Debian-Linux │
   *      │ null         │
   *      │ Ubuntu-Linux │
   *      │ Mac-Sierra   │
   *      └──────────────┘
   * ```
   */
  stripChars(prefix: string | Expr): Expr;
  /** Remove trailing characters.
   * @param prefix - Prefix substring or expression (null means whitespace)
   * @see stripChars
   */
  stripCharsEnd(prefix: string | Expr): Expr;
  /** Remove leading characters.
   * @param prefix - Prefix substring or expression (null means whitespace)
   * @see stripChars
   */
  stripCharsStart(prefix: string | Expr): Expr;
}

export const ExprStringFunctions = (_expr: any): ExprString => {
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
    contains(pat: string | RegExp | Expr, literal = false, strict = true) {
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
    endsWith(suffix: string | Expr) {
      return wrap("strEndsWith", exprToLitOrExpr(suffix)._expr);
    },
    extract(pattern: RegExp | Expr, groupIndex: number) {
      return wrap(
        "strExtract",
        exprToLitOrExpr(pattern, true)._expr,
        groupIndex,
      );
    },
    jsonDecode(dtype?: DataType) {
      return wrap("strJsonDecode", dtype);
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
    replace(
      pat: string | RegExp | Expr,
      val: string | Expr,
      literal = false,
      n = 1,
    ) {
      return wrap(
        "strReplace",
        exprToLitOrExpr(pat)._expr,
        exprToLitOrExpr(val)._expr,
        literal,
        n,
      );
    },
    replaceAll(
      pat: string | RegExp | Expr,
      val: string | Expr,
      literal = false,
    ) {
      return wrap(
        "strReplaceAll",
        exprToLitOrExpr(pat)._expr,
        exprToLitOrExpr(val)._expr,
        literal,
      );
    },
    rstrip() {
      return wrap("strRstrip");
    },
    padStart(length: number, fillChar: string) {
      return wrap("strPadStart", lit(length)._expr, fillChar);
    },
    zFill(length: number | Expr) {
      if (!Expr.isExpr(length)) {
        length = lit(length)._expr;
      }
      return wrap("zfill", length);
    },
    padEnd(length: number, fillChar: string) {
      return wrap("strPadEnd", lit(length)._expr, fillChar);
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
    startsWith(prefix: string | Expr) {
      return wrap("strStartsWith", exprToLitOrExpr(prefix)._expr);
    },
    strip() {
      return wrap("strStrip");
    },
    stripChars(pattern: string | Expr) {
      return wrap("strStripChars", exprToLitOrExpr(pattern)._expr, true, true);
    },
    stripCharsEnd(pattern: string | Expr) {
      return wrap("strStripChars", exprToLitOrExpr(pattern)._expr, false, true);
    },
    stripCharsStart(pattern: string | Expr) {
      return wrap("strStripChars", exprToLitOrExpr(pattern)._expr, true, false);
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
          undefined,
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
