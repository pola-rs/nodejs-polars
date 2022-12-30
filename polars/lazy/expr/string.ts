import { StringFunctions } from "../../shared_traits";
import { DataType } from "../../datatypes";
import { regexToString } from "../../utils";
import { Expr, _Expr } from "../expr";

/**
 * namespace containing expr string functions
 */
export interface StringNamespace extends StringFunctions<Expr> {}

export const ExprStringFunctions = (_expr: any): StringNamespace => {
  const wrap = (method, ...args: any[]): Expr => {
    return _Expr(_expr[method](...args));
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
    concat(delimiter: string) {
      return wrap("strConcat", delimiter);
    },
    contains(pat: string | RegExp) {
      return wrap("strContains", regexToString(pat));
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
    extract(pat: string | RegExp, groupIndex: number) {
      return wrap("strExtract", regexToString(pat), groupIndex);
    },
    jsonPathMatch(pat: string) {
      return wrap("strJsonPathMatch", pat);
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
    slice(start: number, length?: number) {
      return wrap("strSlice", start, length);
    },
    split(by: string, options?) {
      const inclusive =
        typeof options === "boolean" ? options : options?.inclusive;

      return wrap("strSplit", by, inclusive);
    },
    strip() {
      return wrap("strStrip");
    },
    strptime(dtype, fmt?) {
      if (dtype.equals(DataType.Date)) {
        return wrap("strParseDate", fmt, false, false);
      } else if (dtype.equals(DataType.Datetime("ms"))) {
        return wrap("strParseDatetime", fmt, false, false);
      } else {
        throw new Error(
          `only "DataType.Date" and "DataType.Datetime" are supported`,
        );
      }
    },
    toLowerCase() {
      return wrap("strToLowercase");
    },
    toUpperCase() {
      return wrap("strToUppercase");
    },
  };
};
