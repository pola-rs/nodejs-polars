import assert from "node:assert/strict";
import {
  after,
  afterEach,
  before,
  beforeEach,
  describe,
  it,
  test,
} from "node:test";
import { isDeepStrictEqual } from "node:util";
import pl from "../polars";

type TestCase = unknown[] | Record<string, unknown>;
type Done = (error?: unknown) => void;

const runWithDone = (fn: (...args: unknown[]) => unknown, args: unknown[]) => {
  if (fn.length > args.length) {
    return new Promise<void>((resolve, reject) => {
      const done: Done = (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      };

      try {
        fn(...args, done);
      } catch (error) {
        reject(error);
      }
    });
  }

  return fn(...args);
};

const parseEachCases = (
  tableOrCases: TestCase[] | TemplateStringsArray,
  values: unknown[],
): TestCase[] => {
  if (Array.isArray(tableOrCases) && !Object.hasOwn(tableOrCases, "raw")) {
    return tableOrCases as TestCase[];
  }

  const strings = tableOrCases as TemplateStringsArray;
  const tokens: string[] = [strings[0] ?? ""];
  for (const [idx, _value] of values.entries()) {
    tokens.push(`__EACH_${idx}__`);
    tokens.push(strings[idx + 1] ?? "");
  }

  const rows = tokens
    .join("")
    .trim()
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);

  const keys = rows[0]
    .split("|")
    .map((key) => key.trim())
    .filter(Boolean);

  return rows.slice(1).map((row) => {
    const rawCols = row
      .split("|")
      .map((col) => col.trim())
      .filter(Boolean);

    const parsed = rawCols.map((col) => {
      const match = /^__EACH_(\d+)__$/.exec(col);
      if (!match) {
        return col;
      }
      return values[Number(match[1])];
    });

    return Object.fromEntries(keys.map((key, idx) => [key, parsed[idx]]));
  });
};

const createEach = (base: typeof test) => {
  return (
    tableOrCases: TestCase[] | TemplateStringsArray,
    ...values: unknown[]
  ) => {
    const cases = parseEachCases(tableOrCases, values);

    return (name: string, fn: (...args: unknown[]) => unknown) => {
      cases.forEach((testCase, idx) => {
        const args = Array.isArray(testCase) ? testCase : [testCase];
        base(`${name} [${idx + 1}]`, async () => {
          await runWithDone(fn, args);
        });
      });
    };
  };
};

const createCompatTest = (base: typeof test) => {
  const compat = ((name: string, fn: (...args: unknown[]) => unknown) => {
    base(name, async () => {
      await runWithDone(fn, []);
    });
  }) as typeof test & {
    each: (
      tableOrCases: TestCase[] | TemplateStringsArray,
      ...values: unknown[]
    ) => (name: string, fn: (...args: unknown[]) => unknown) => void;
  };

  compat.each = createEach(base);
  return compat;
};

const compatTest = createCompatTest(test);
const compatIt = createCompatTest(it);

export const assertSeriesStrictEqual = (actual, expected) => {
  assert.ok(
    actual.seriesEqual(expected),
    `\nExpected:\n>>${expected}\nReceived:\n>>${actual}`,
  );
  assert.ok(
    actual.dtype.equals(expected.dtype),
    `\nExpected dtype:\n>>${expected.dtype}\nReceived dtype:\n>>${actual.dtype}`,
  );
};

export const assertSeriesEqual = (actual, expected) => {
  assert.ok(
    actual.seriesEqual(expected),
    `\nExpected:\n>>${expected}\nReceived:\n>>${actual}`,
  );
};

export const assertFrameEqual = (actual, expected, nullEqual?) => {
  assert.ok(
    actual.frameEqual(expected, nullEqual),
    `\nExpected:\n>>${expected}\nReceived:\n>>${actual}`,
  );
};

export const assertFrameStrictEqual = (actual, expected) => {
  assert.ok(
    actual.frameEqual(expected),
    `\nExpected:\n>>${expected}\nReceived:\n>>${actual}`,
  );
  assert.ok(
    isDeepStrictEqual(actual.dtypes, expected.dtypes),
    `\nExpected dtypes:\n>>${expected.dtypes}\nReceived dtypes:\n>>${actual.dtypes}`,
  );
};

export const assertFrameEqualIgnoringOrder = (
  actualFrame: pl.DataFrame,
  expectedFrame: pl.DataFrame,
) => {
  const actual = actualFrame.sort(actualFrame.columns.sort());
  const expected = expectedFrame.sort(expectedFrame.columns.sort());

  assert.ok(
    actual.frameEqual(expected),
    `\nExpected:\n>>${expected}\nReceived:\n>>${actual}`,
  );
};

Object.assign(globalThis, {
  afterAll: after,
  afterEach,
  assert,
  assertFrameEqual,
  assertFrameEqualIgnoringOrder,
  assertFrameStrictEqual,
  assertSeriesEqual,
  assertSeriesStrictEqual,
  beforeAll: before,
  beforeEach,
  describe,
  it: compatIt,
  test: compatTest,
});

export const df = () => {
  const df = pl.DataFrame({
    bools: [false, true, false],
    bools_nulls: [null, true, false],
    int: [1, 2, 3],
    int_nulls: [1, null, 3],
    bigint: [1n, 2n, 3n],
    bigint_nulls: [1n, null, 3n],
    floats: [1.0, 2.0, 3.0],
    floats_nulls: [1.0, null, 3.0],
    strings: ["foo", "bar", "ham"],
    strings_nulls: ["foo", null, "ham"],
    date: [new Date(), new Date(), new Date()],
    datetime: [13241324, 12341256, 12341234],
  });

  return df.withColumns(
    pl.col("date").cast(pl.Date),
    pl.col("datetime").cast(pl.Datetime("ms")),
    pl.col("strings").cast(pl.Categorical).alias("cat"),
  );
};
