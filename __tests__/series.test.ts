/* eslint-disable newline-per-chained-call */

import Chance from "chance";
import pl, { DataType } from "../polars";

describe("mapElements", () => {
  test("mapElements string", () => {
    const mapping: Record<string, string> = {
      A: "AA",
      B: "BB",
      C: "CC",
      D: "OtherD",
    };
    const funcMap = (k: string): string => mapping[k] ?? "";
    const actual = pl
      .Series("foo", ["A", "B", "C", "D", "F", null], pl.String)
      .mapElements(funcMap);
    const expected = pl.Series(
      "foo",
      ["AA", "BB", "CC", "OtherD", "", ""],
      pl.String,
    );
    assertSeriesEqual(actual, expected);
  });
  test("mapElements int", () => {
    const mapping: Record<number, number> = { 1: 11, 2: 22, 3: 33, 4: 44 };
    const funcMap = (k: number): number => mapping[k] ?? "";
    let actual = pl.Series("foo", [1, 2, 3, 5], pl.Int32).mapElements(funcMap);
    let expected = pl.Series("foo", [11, 22, 33, null], pl.Int32);
    assertSeriesEqual(actual, expected);
    const multiFunc = (k: number): number => k * 2;
    actual = pl.Series("foo", [1, 2, 3, 5], pl.Int32).mapElements(multiFunc);
    expected = pl.Series("foo", [2, 4, 6, 10], pl.Int32);
    assertSeriesEqual(actual, expected);
    const funcStr = (k: number): string => `${k}x`;
    actual = pl.Series("foo", [1, 2, 3, 5], pl.Int32).mapElements(funcStr);
    expected = pl.Series("foo", ["1x", "2x", "3x", "5x"], pl.String);
    assertSeriesEqual(actual, expected);
  });
});
describe("from lists", () => {
  test("bool", () => {
    const expected = [[true, false], [true], [null], []];
    const actual = pl.Series(expected).toArray();
    assert.deepStrictEqual(actual, expected);
  });
  test("number", () => {
    const expected = [[1, 2], [3], [null], []];
    const actual = pl.Series(expected).toArray();
    assert.deepStrictEqual(actual, expected);
  });
  test("bigint", () => {
    const expected = [[1n, 2n], [3n], [null], []];
    const actual = pl.Series(expected).toArray();
    assert.deepStrictEqual(actual, expected);
  });
  test("string", () => {
    const expected = [[], [null], ["a"], [null], ["b", "c"]];
    const actual = pl.Series(expected).toArray();
    assert.deepStrictEqual(actual, expected);
  });
  test("fromArray", () => {
    const actual = pl.Series.from("foo", [1, 2, 3]);
    const expected = pl.Series("foo", [1, 2, 3]);
    assertSeriesEqual(actual, expected);
    const actual2 = pl.Series.from([1, 2, 3]);
    assertSeriesEqual(actual2, expected);
  });
  test("of", () => {
    const actual = pl.Series.of([1, 2, 3]);
    const expected = pl.Series("", [1, 2, 3]);
    assertSeriesEqual(actual, expected);
  });
});
describe("typedArrays", () => {
  test("int8", () => {
    const int8Array = new Int8Array([1, 2, 3]);
    const actual = pl.Series(int8Array).toArray();
    const expected = [...int8Array];
    assert.deepStrictEqual(actual, expected);
  });
  test("int8:list", () => {
    const int8Arrays = [new Int8Array([1, 2, 3]), new Int8Array([33, 44, 55])];
    const expected = int8Arrays.map((i) => [...i]);
    const actual = pl.Series(int8Arrays).toArray();
    assert.deepStrictEqual(actual, expected);
  });
  test("int16", () => {
    const int16Array = new Int16Array([1, 2, 3]);
    const actual = pl.Series(int16Array).toArray();
    const expected = Array.from(int16Array);
    assert.deepStrictEqual(actual, expected);
  });
  test("int16:list", () => {
    const int16Arrays = [
      new Int16Array([1, 2, 3]),
      new Int16Array([33, 44, 55]),
    ];
    const actual = pl.Series(int16Arrays).toArray();
    const expected = int16Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });
  test("int32", () => {
    const int32Array = new Int32Array([1, 2, 3]);
    const actual = pl.Series(int32Array).toArray();
    assert.deepStrictEqual(actual, [...int32Array]);
  });
  test("int32:list", () => {
    const int32Arrays = [
      new Int32Array([1, 2, 3]),
      new Int32Array([33, 44, 55]),
    ];
    const actual = pl.Series(int32Arrays).toArray();
    const expected = int32Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });

  // serde downcasts int64 to 'number'
  test("int64", () => {
    const int64Array = new BigInt64Array([1n, 2n, 3n]);
    const actual = pl.Series(int64Array).toArray();

    const expected = Array.from(int64Array).map((v: any) =>
      Number.parseInt(v, 10),
    );

    assert.deepStrictEqual(actual, expected);
  });
  // serde downcasts int64 to 'number'
  test("int64:list", () => {
    const int64Arrays = [
      new BigInt64Array([1n, 2n, 3n]),
      new BigInt64Array([33n, 44n, 55n]),
    ] as any;

    const actual = pl.Series(int64Arrays).toArray();
    const expected = [
      [1, 2, 3],
      [33, 44, 55],
    ];
    assert.deepStrictEqual(actual, expected);
  });
  test("uint8", () => {
    const uint8Array = new Uint8Array([1, 2, 3]);
    const actual = pl.Series(uint8Array).toArray();
    const expected = [...uint8Array];
    assert.deepStrictEqual(actual, expected);
  });
  test("uint8:list", () => {
    const uint8Arrays = [
      new Uint8Array([1, 2, 3]),
      new Uint8Array([33, 44, 55]),
    ];
    const actual = pl.Series(uint8Arrays).toArray();
    const expected = uint8Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });
  test("uint16", () => {
    const uint16Array = new Uint16Array([1, 2, 3]);
    const actual = pl.Series(uint16Array).toArray();
    const expected = [...uint16Array];
    assert.deepStrictEqual(actual, expected);
  });
  test("uint16:list", () => {
    const uint16Arrays = [
      new Uint16Array([1, 2, 3]),
      new Uint16Array([33, 44, 55]),
    ];
    const actual = pl.Series(uint16Arrays).toArray();
    const expected = uint16Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });
  test("uint32", () => {
    const uint32Array = new Uint32Array([1, 2, 3]);
    const actual = pl.Series(uint32Array).toArray();
    const expected = [...uint32Array];
    assert.deepStrictEqual(actual, expected);
  });
  test("uint32:list", () => {
    const uint32Arrays = [
      new Uint32Array([1, 2, 3]),
      new Uint32Array([33, 44, 55]),
    ];
    const actual = pl.Series(uint32Arrays).toArray();
    const expected = uint32Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });
  test("uint64", () => {
    const uint64Array = new BigUint64Array([1n, 2n, 3n]);
    const actual = pl.Series(uint64Array).toArray();
    const expected = [...uint64Array];
    assert.deepStrictEqual(actual, expected);
  });
  test("uint64:list", () => {
    const uint64Arrays = [
      new BigUint64Array([1n, 2n, 3n]),
      new BigUint64Array([33n, 44n, 55n]),
    ];
    const actual = pl.Series(uint64Arrays).toArray();
    const expected = uint64Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });
  test("float32", () => {
    const float32Array = new Float32Array([1, 2, 3]);
    const actual = pl.Series(float32Array).toArray();
    const expected = [...float32Array];
    assert.deepStrictEqual(actual, expected);
  });
  test("float32:list", () => {
    const float32Arrays = [
      new Float32Array([1, 2, 3]),
      new Float32Array([33, 44, 55]),
    ];
    const actual = pl.Series(float32Arrays).toArray();
    const expected = float32Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });
  test("float64", () => {
    const float64Array = new Float64Array([1, 2, 3]);
    const actual = pl.Series(float64Array).toArray();
    const expected = [...float64Array];
    assert.deepStrictEqual(actual, expected);
  });
  test("float64:list", () => {
    const float64Arrays = [
      new Float64Array([1, 2, 3]),
      new Float64Array([33, 44, 55]),
    ];
    const actual = pl.Series(float64Arrays).toArray();
    const expected = float64Arrays.map((i) => [...i]);
    assert.deepStrictEqual(actual, expected);
  });
  test("toTypedArray", () => {
    const float64Array = new Float64Array([1, 2, 3]);
    const actual = pl.Series(float64Array).toTypedArray();
    assert.deepStrictEqual(
      JSON.stringify(actual),
      JSON.stringify(float64Array),
    );
  });

  test("decimal", () => {
    const expected = [1n, 2n, 3n];
    const expectedDtype = pl.Decimal(10, 2);
    assert.ok(expectedDtype.equals(expectedDtype));
    const actual = pl.Series("", expected, expectedDtype);
    assert.deepStrictEqual(actual.dtype, expectedDtype);
    try {
      actual.toArray();
    } catch (e: any) {
      assert.ok(
        e.message.includes(
          "Decimal is not a supported type in javascript, please convert to string or number before collecting to js",
        ),
      );
    }
  });

  test("fixed list", () => {
    const expectedDtype = pl.FixedSizeList(pl.Float32, 3);
    const expected = [
      [1, 2, 3],
      [4, 5, 6],
    ];
    const actual = pl.Series("", expected, expectedDtype);
    assert.deepStrictEqual(actual.dtype, expectedDtype);
    const actualValues = actual.toArray();
    assert.deepStrictEqual(actualValues, expected);
    const lst = expectedDtype.asFixedSizeList();
    assert.deepStrictEqual(lst?.inner[0], pl.Float32);
    assert.ok(expectedDtype.equals(expectedDtype));
  });
});
describe("series", () => {
  const chance = new Chance();

  describe("create series", () => {
    it.each`
      values                    | dtype                        | type
      ${["foo", "bar", "baz"]}  | ${pl.String}                 | ${"string"}
      ${[1, 2, 3]}              | ${pl.Float64}                | ${"f64"}
      ${[1n, 2n, 3n]}           | ${pl.UInt64}                 | ${"bigint"}
      ${[true, false]}          | ${pl.Bool}                   | ${"boolean"}
      ${[]}                     | ${pl.Float64}                | ${"empty"}
      ${[new Date(Date.now())]} | ${pl.Datetime("ms", "")}     | ${"Date"}
      ${[1, 2, 3]}              | ${pl.Duration("ms")}         | ${"duration[ms]"}
      ${[[1, 2, 3, 4, 5, 6]]}   | ${pl.List(DataType.Int16)}   | ${"[list[i16]]"}
      ${[[1, 2, 3, 4, 5, 6]]}   | ${pl.List(DataType.UInt16)}  | ${"[list[u16]]"}
      ${[[1, 2, 3, 4, 5, 6]]}   | ${pl.List(DataType.Int32)}   | ${"[list[i32]]"}
      ${[[1, 2, 3, 4, 5, 6]]}   | ${pl.List(DataType.UInt32)}  | ${"[list[u32]]"}
      ${[[1, 2, 3, 4, 5, 6]]}   | ${pl.List(DataType.Float32)} | ${"[list[f32]]"}
      ${[[1, 2, 3, 4, 5, 6]]}   | ${pl.List(DataType.Float64)} | ${"[list[f64]]"}
    `("defaults to $type for $values", ({ values, dtype }) => {
      const name = chance.string();
      const s = pl.Series(name, values, dtype);
      assert.deepStrictEqual(s.name, name);
      assert.deepStrictEqual(s.length, values.length);
      assert.deepStrictEqual(s.dtype, dtype);
    });

    it.each`
      values                   | dtype         | type
      ${["foo", "bar", "baz"]} | ${pl.String}  | ${"string"}
      ${[1, 2, 3]}             | ${pl.Float64} | ${"f64"}
      ${[1n, 2n, 3n]}          | ${pl.UInt64}  | ${"u64"}
    `("defaults to $type for $values", ({ values, dtype }) => {
      const name = chance.string();
      const s = pl.Series(name, values);
      assert.deepStrictEqual(s.name, name);
      assert.deepStrictEqual(s.length, values.length);
      assert.deepStrictEqual(s.dtype, dtype);
    });
  });
});
describe("series functions", () => {
  const numSeries = () => pl.Series("foo", [1, 2, 3], pl.Int32);
  const fltSeries = () => pl.Series("float", [1, 2, 3], pl.Float64);
  const boolSeries = () => pl.Series("bool", [true, false, false]);
  const other = () => pl.Series("bar", [3, 4, 5], pl.Int32);
  const chance = new Chance();
  it.each`
    series         | getter
    ${numSeries()} | ${"dtype"}
    ${numSeries()} | ${"name"}
    ${numSeries()} | ${"length"}
  `("$# $getter does not error", ({ series, getter }) => {
    try {
      series[getter];
    } catch (err) {
      assert.strictEqual(err, undefined);
    }
  });
  it.each`
    series          | method               | args
    ${numSeries()}  | ${"abs"}             | ${[]}
    ${numSeries()}  | ${"as"}              | ${[chance.string()]}
    ${numSeries()}  | ${"alias"}           | ${[chance.string()]}
    ${numSeries()}  | ${"append"}          | ${[other()]}
    ${numSeries()}  | ${"argMax"}          | ${[]}
    ${numSeries()}  | ${"argMin"}          | ${[]}
    ${numSeries()}  | ${"argSort"}         | ${[]}
    ${boolSeries()} | ${"argTrue"}         | ${[]}
    ${numSeries()}  | ${"argUnique"}       | ${[]}
    ${numSeries()}  | ${"cast"}            | ${[pl.UInt32]}
    ${numSeries()}  | ${"chunkLengths"}    | ${[]}
    ${numSeries()}  | ${"clone"}           | ${[]}
    ${numSeries()}  | ${"cumMax"}          | ${[]}
    ${numSeries()}  | ${"cumMin"}          | ${[]}
    ${numSeries()}  | ${"cumProd"}         | ${[]}
    ${numSeries()}  | ${"cumSum"}          | ${[]}
    ${numSeries()}  | ${"describe"}        | ${[]}
    ${numSeries()}  | ${"diff"}            | ${[]}
    ${numSeries()}  | ${"diff"}            | ${[{ n: 1, nullBehavior: "drop" }]}
    ${numSeries()}  | ${"diff"}            | ${[{ nullBehavior: "drop" }]}
    ${numSeries()}  | ${"diff"}            | ${[1, "drop"]}
    ${numSeries()}  | ${"dot"}             | ${[other()]}
    ${numSeries()}  | ${"dropNulls"}       | ${[]}
    ${numSeries()}  | ${"explode"}         | ${[]}
    ${numSeries()}  | ${"fillNull"}        | ${["zero"]}
    ${numSeries()}  | ${"fillNull"}        | ${[{ strategy: "zero" }]}
    ${numSeries()}  | ${"filter"}          | ${[boolSeries()]}
    ${fltSeries()}  | ${"floor"}           | ${[]}
    ${numSeries()}  | ${"hasNulls"}        | ${[]}
    ${numSeries()}  | ${"hash"}            | ${[]}
    ${numSeries()}  | ${"hash"}            | ${[{ k0: 10 }]}
    ${numSeries()}  | ${"hash"}            | ${[{ k0: 10, k1: 29 }]}
    ${numSeries()}  | ${"hash"}            | ${[{ k0: 10, k1: 29, k2: 3 }]}
    ${numSeries()}  | ${"hash"}            | ${[{ k0: 10, k1: 29, k3: 1, k2: 3 }]}
    ${numSeries()}  | ${"hash"}            | ${[1]}
    ${numSeries()}  | ${"hash"}            | ${[1, 2]}
    ${numSeries()}  | ${"hash"}            | ${[1, 2, 3]}
    ${numSeries()}  | ${"hash"}            | ${[1, 2, 3, 4]}
    ${numSeries()}  | ${"head"}            | ${[]}
    ${numSeries()}  | ${"head"}            | ${[1]}
    ${numSeries()}  | ${"inner"}           | ${[]}
    ${numSeries()}  | ${"interpolate"}     | ${[]}
    ${numSeries()}  | ${"isBoolean"}       | ${[]}
    ${numSeries()}  | ${"isDateTime"}      | ${[]}
    ${numSeries()}  | ${"isDuplicated"}    | ${[]}
    ${fltSeries()}  | ${"isFinite"}        | ${[]}
    ${numSeries()}  | ${"isFirstDistinct"} | ${[]}
    ${numSeries()}  | ${"isFloat"}         | ${[]}
    ${numSeries()}  | ${"isIn"}            | ${[other()]}
    ${fltSeries()}  | ${"isInfinite"}      | ${[]}
    ${numSeries()}  | ${"isNotNull"}       | ${[]}
    ${numSeries()}  | ${"isNull"}          | ${[]}
    ${numSeries()}  | ${"isNaN"}           | ${[]}
    ${numSeries()}  | ${"isNotNaN"}        | ${[]}
    ${numSeries()}  | ${"isNumeric"}       | ${[]}
    ${numSeries()}  | ${"isUnique"}        | ${[]}
    ${numSeries()}  | ${"kurtosis"}        | ${[]}
    ${numSeries()}  | ${"kurtosis"}        | ${[{ fisher: true, bias: true }]}
    ${numSeries()}  | ${"kurtosis"}        | ${[{ bias: false }]}
    ${numSeries()}  | ${"kurtosis"}        | ${[{ fisher: false }]}
    ${numSeries()}  | ${"kurtosis"}        | ${[false, false]}
    ${numSeries()}  | ${"kurtosis"}        | ${[false]}
    ${numSeries()}  | ${"len"}             | ${[]}
    ${numSeries()}  | ${"limit"}           | ${[]}
    ${numSeries()}  | ${"limit"}           | ${[2]}
    ${numSeries()}  | ${"max"}             | ${[]}
    ${numSeries()}  | ${"mean"}            | ${[]}
    ${numSeries()}  | ${"median"}          | ${[]}
    ${numSeries()}  | ${"min"}             | ${[]}
    ${numSeries()}  | ${"mode"}            | ${[]}
    ${numSeries()}  | ${"nChunks"}         | ${[]}
    ${numSeries()}  | ${"nUnique"}         | ${[]}
    ${numSeries()}  | ${"nullCount"}       | ${[]}
    ${numSeries()}  | ${"peakMax"}         | ${[]}
    ${numSeries()}  | ${"peakMin"}         | ${[]}
    ${numSeries()}  | ${"quantile"}        | ${[0.4]}
    ${numSeries()}  | ${"rank"}            | ${[]}
    ${numSeries()}  | ${"rank"}            | ${["average"]}
    ${numSeries()}  | ${"rechunk"}         | ${[]}
    ${numSeries()}  | ${"rechunk"}         | ${[true]}
    ${numSeries()}  | ${"rename"}          | ${["new name"]}
    ${numSeries()}  | ${"rename"}          | ${["new name", true]}
    ${numSeries()}  | ${"rename"}          | ${[{ name: "new name" }]}
    ${numSeries()}  | ${"rename"}          | ${[{ name: "new name", inPlace: true }]}
    ${numSeries()}  | ${"rename"}          | ${[{ name: "new name" }]}
    ${numSeries()}  | ${"rollingMax"}      | ${[{ windowSize: 1 }]}
    ${numSeries()}  | ${"rollingMax"}      | ${[{ windowSize: 1, weights: [0.33] }]}
    ${numSeries()}  | ${"rollingMax"}      | ${[{ windowSize: 1, weights: [0.11], minPeriods: 1 }]}
    ${numSeries()}  | ${"rollingMax"}      | ${[{ windowSize: 1, weights: [0.44], minPeriods: 1, center: false }]}
    ${numSeries()}  | ${"rollingMax"}      | ${[1]}
    ${numSeries()}  | ${"rollingMax"}      | ${[1, [0.11]]}
    ${numSeries()}  | ${"rollingMax"}      | ${[1, [0.11], 1]}
    ${numSeries()}  | ${"rollingMax"}      | ${[1, [0.23], 1, true]}
    ${numSeries()}  | ${"rollingMean"}     | ${[{ windowSize: 1 }]}
    ${numSeries()}  | ${"rollingMean"}     | ${[{ windowSize: 1, weights: [0.33] }]}
    ${numSeries()}  | ${"rollingMean"}     | ${[{ windowSize: 1, weights: [0.11], minPeriods: 1 }]}
    ${numSeries()}  | ${"rollingMean"}     | ${[{ windowSize: 1, weights: [0.44], minPeriods: 1, center: false }]}
    ${numSeries()}  | ${"rollingMean"}     | ${[1]}
    ${numSeries()}  | ${"rollingMean"}     | ${[1, [0.11]]}
    ${numSeries()}  | ${"rollingMean"}     | ${[1, [0.11], 1]}
    ${numSeries()}  | ${"rollingMean"}     | ${[1, [0.23], 1, true]}
    ${numSeries()}  | ${"rollingMin"}      | ${[{ windowSize: 1 }]}
    ${numSeries()}  | ${"rollingMin"}      | ${[{ windowSize: 1, weights: [0.33] }]}
    ${numSeries()}  | ${"rollingMin"}      | ${[{ windowSize: 1, weights: [0.11], minPeriods: 1 }]}
    ${numSeries()}  | ${"rollingMin"}      | ${[{ windowSize: 1, weights: [0.44], minPeriods: 1, center: false }]}
    ${numSeries()}  | ${"rollingMin"}      | ${[1]}
    ${numSeries()}  | ${"rollingMin"}      | ${[1, [0.11]]}
    ${numSeries()}  | ${"rollingMin"}      | ${[1, [0.11], 1]}
    ${numSeries()}  | ${"rollingMin"}      | ${[1, [0.23], 1, true]}
    ${numSeries()}  | ${"rollingSum"}      | ${[{ windowSize: 1 }]}
    ${numSeries()}  | ${"rollingSum"}      | ${[{ windowSize: 1, weights: [0.33] }]}
    ${numSeries()}  | ${"rollingSum"}      | ${[{ windowSize: 1, weights: [0.11], minPeriods: 1 }]}
    ${numSeries()}  | ${"rollingSum"}      | ${[{ windowSize: 1, weights: [0.44], minPeriods: 1, center: false }]}
    ${numSeries()}  | ${"rollingSum"}      | ${[1]}
    ${numSeries()}  | ${"rollingSum"}      | ${[1, [0.11]]}
    ${numSeries()}  | ${"rollingSum"}      | ${[1, [0.11], 1]}
    ${numSeries()}  | ${"rollingSum"}      | ${[1, [0.23], 1, true]}
    ${numSeries()}  | ${"rollingStd"}      | ${[1, [0.23], 1, true]}
    ${numSeries()}  | ${"rollingVar"}      | ${[{ windowSize: 1 }]}
    ${numSeries()}  | ${"rollingVar"}      | ${[{ windowSize: 1, weights: [0.33] }]}
    ${numSeries()}  | ${"rollingVar"}      | ${[{ windowSize: 1, weights: [0.11], minPeriods: 1 }]}
    ${numSeries()}  | ${"rollingVar"}      | ${[{ windowSize: 1, weights: [0.44], minPeriods: 1, center: false }]}
    ${numSeries()}  | ${"rollingVar"}      | ${[1]}
    ${numSeries()}  | ${"rollingVar"}      | ${[1, [0.11]]}
    ${numSeries()}  | ${"rollingVar"}      | ${[1, [0.11], 1]}
    ${numSeries()}  | ${"rollingVar"}      | ${[1, [0.23], 1, true]}
    ${fltSeries()}  | ${"round"}           | ${[1]}
    ${numSeries()}  | ${"sample"}          | ${[]}
    ${numSeries()}  | ${"sample"}          | ${[1, null, true]}
    ${numSeries()}  | ${"sample"}          | ${[null, 1]}
    ${numSeries()}  | ${"sample"}          | ${[{ n: 1 }]}
    ${numSeries()}  | ${"sample"}          | ${[{ frac: 0.5 }]}
    ${numSeries()}  | ${"sample"}          | ${[{ n: 1, withReplacement: true }]}
    ${numSeries()}  | ${"sample"}          | ${[{ frac: 0.1, withReplacement: true }]}
    ${numSeries()}  | ${"sample"}          | ${[{ frac: 0.1, withReplacement: true, seed: 1n }]}
    ${numSeries()}  | ${"sample"}          | ${[{ frac: 0.1, withReplacement: true, seed: 1 }]}
    ${numSeries()}  | ${"sample"}          | ${[{ n: 1, withReplacement: true, seed: 1 }]}
    ${numSeries()}  | ${"seriesEqual"}     | ${[other()]}
    ${numSeries()}  | ${"seriesEqual"}     | ${[other(), true]}
    ${numSeries()}  | ${"seriesEqual"}     | ${[other(), false]}
    ${numSeries()}  | ${"set"}             | ${[boolSeries(), 2]}
    ${fltSeries()}  | ${"scatter"}         | ${[[0, 1], 1]}
    ${numSeries()}  | ${"shift"}           | ${[]}
    ${numSeries()}  | ${"shift"}           | ${[1]}
    ${numSeries()}  | ${"shiftAndFill"}    | ${[1, 2]}
    ${numSeries()}  | ${"shiftAndFill"}    | ${[{ periods: 1, fillValue: 2 }]}
    ${numSeries()}  | ${"shrinkToFit"}     | ${[1, 2]}
    ${numSeries()}  | ${"skew"}            | ${[]}
    ${numSeries()}  | ${"skew"}            | ${[true]}
    ${numSeries()}  | ${"skew"}            | ${[false]}
    ${numSeries()}  | ${"skew"}            | ${[{ bias: true }]}
    ${numSeries()}  | ${"skew"}            | ${[{ bias: false }]}
    ${numSeries()}  | ${"slice"}           | ${[1, 2]}
    ${numSeries()}  | ${"slice"}           | ${[{ offset: 1, length: 2 }]}
    ${numSeries()}  | ${"sort"}            | ${[]}
    ${numSeries()}  | ${"sort"}            | ${[false]}
    ${numSeries()}  | ${"sort"}            | ${[true]}
    ${numSeries()}  | ${"sort"}            | ${[{ descending: true }]}
    ${numSeries()}  | ${"sort"}            | ${[{ descending: false }]}
    ${numSeries()}  | ${"sum"}             | ${[]}
    ${numSeries()}  | ${"tail"}            | ${[]}
    ${numSeries()}  | ${"gather"}          | ${[[1, 2]]}
    ${numSeries()}  | ${"gatherEvery"}     | ${[1]}
    ${numSeries()}  | ${"toArray"}         | ${[]}
    ${numSeries()}  | ${"unique"}          | ${[]}
    ${numSeries()}  | ${"valueCounts"}     | ${[]}
    ${numSeries()}  | ${"zipWith"}         | ${[boolSeries(), other()]}
    ${boolSeries()} | ${"all"}             | ${[]}
    ${boolSeries()} | ${"all"}             | ${[false]}
    ${boolSeries()} | ${"any"}             | ${[]}
    ${boolSeries()} | ${"any"}             | ${[false]}
    ${boolSeries()} | ${"not"}             | ${[]}
  `("$# $method is callable", ({ series, method, args }) => {
    try {
      series[method](...args);
    } catch (err) {
      assert.strictEqual(err, undefined);
    }
  });

  it.each`
    name                 | actual                                                                                 | expected
    ${"dtype:String"}    | ${pl.Series(["foo"]).dtype}                                                            | ${pl.String}
    ${"dtype:UInt64"}    | ${pl.Series([1n]).dtype}                                                               | ${pl.UInt64}
    ${"dtype:Float64"}   | ${pl.Series([1]).dtype}                                                                | ${pl.Float64}
    ${"dtype"}           | ${pl.Series(["foo"]).dtype}                                                            | ${pl.String}
    ${"name"}            | ${pl.Series("a", ["foo"]).name}                                                        | ${"a"}
    ${"length"}          | ${pl.Series([1, 2, 3, 4]).length}                                                      | ${4}
    ${"abs"}             | ${pl.Series([1, 2, -3]).abs()}                                                         | ${pl.Series([1, 2, 3])}
    ${"alias"}           | ${pl.Series([1, 2, 3]).as("foo")}                                                      | ${pl.Series("foo", [1, 2, 3])}
    ${"alias"}           | ${pl.Series([1, 2, 3]).alias("foo")}                                                   | ${pl.Series("foo", [1, 2, 3])}
    ${"argMax"}          | ${pl.Series([1, 2, 3]).argMax()}                                                       | ${2}
    ${"argMin"}          | ${pl.Series([1, 2, 3]).argMin()}                                                       | ${0}
    ${"argSort"}         | ${pl.Series([3, 2, 1]).argSort()}                                                      | ${pl.Series([2, 1, 0])}
    ${"argSort"}         | ${pl.Series([null, 3, 2, 1]).argSort({ descending: true })}                            | ${pl.Series([1, 2, 3, 0])}
    ${"argTrue"}         | ${pl.Series([true, false]).argTrue()}                                                  | ${pl.Series([0])}
    ${"argUnique"}       | ${pl.Series([1, 1, 2]).argUnique()}                                                    | ${pl.Series([0, 2])}
    ${"cast-Int16"}      | ${pl.Series("", [1, 1, 2]).cast(pl.Int16)}                                             | ${pl.Series("", [1, 1, 2], pl.Int16)}
    ${"cast-Int32"}      | ${pl.Series("", [1, 1, 2]).cast(pl.Int32)}                                             | ${pl.Series("", [1, 1, 2], pl.Int32)}
    ${"cast-Int64"}      | ${pl.Series("", [1, 1, 2]).cast(pl.Int64)}                                             | ${pl.Series("", [1, 1, 2], pl.Int64)}
    ${"cast-UInt16"}     | ${pl.Series("", [1, 1, 2]).cast(pl.UInt16)}                                            | ${pl.Series("", [1, 1, 2], pl.UInt16)}
    ${"cast-UInt32"}     | ${pl.Series("", [1, 1, 2]).cast(pl.UInt32)}                                            | ${pl.Series("", [1, 1, 2], pl.UInt32)}
    ${"cast-UInt64"}     | ${pl.Series("", [1, 1, 2]).cast(pl.UInt64)}                                            | ${pl.Series("", [1n, 1n, 2n])}
    ${"cast-Utf8"}       | ${pl.Series("", [1, 1, 2]).cast(pl.Utf8)}                                              | ${pl.Series("", ["1.0", "1.0", "2.0"])}
    ${"chunkLengths"}    | ${pl.Series([1, 2, 3]).chunkLengths()[0]}                                              | ${3}
    ${"clone"}           | ${pl.Series([1, 2, 3]).clone()}                                                        | ${pl.Series([1, 2, 3])}
    ${"concat"}          | ${pl.Series([1]).concat(pl.Series([2, 3]))}                                            | ${pl.Series([1, 2, 3])}
    ${"cumMax"}          | ${pl.Series([3, 2, 4]).cumMax()}                                                       | ${pl.Series([3, 3, 4])}
    ${"cumMin"}          | ${pl.Series([3, 2, 4]).cumMin()}                                                       | ${pl.Series([3, 2, 2])}
    ${"cumProd"}         | ${pl.Series("", [1, 2, 3], pl.Int32).cumProd()}                                        | ${pl.Series("", [1, 2, 6], pl.Int64)}
    ${"cumSum"}          | ${pl.Series("", [1, 2, 3], pl.Int32).cumSum()}                                         | ${pl.Series("", [1, 3, 6], pl.Int32)}
    ${"diff"}            | ${pl.Series([1, 2, 12]).diff(1, "drop").toObject()}                                    | ${pl.Series([1, 10]).toObject()}
    ${"diff"}            | ${pl.Series([1, 11]).diff(1, "ignore")}                                                | ${pl.Series("", [null, 10], pl.Float64)}
    ${"dropNulls"}       | ${pl.Series([1, null, 2]).dropNulls()}                                                 | ${pl.Series([1, 2])}
    ${"dropNulls"}       | ${pl.Series([1, undefined, 2]).dropNulls()}                                            | ${pl.Series([1, 2])}
    ${"dropNulls"}       | ${pl.Series(["a", null, "f"]).dropNulls()}                                             | ${pl.Series(["a", "f"])}
    ${"explode"}         | ${pl.Series.from("foo", [[1n, 2n], [3n, 4n], [null], []]).explode()}                   | ${pl.Series([1, 2, 3, 4, null, null])}
    ${"fillNull:zero"}   | ${pl.Series([1, null, 2]).fillNull("zero")}                                            | ${pl.Series([1, 0, 2])}
    ${"fillNull:one"}    | ${pl.Series([1, null, 2]).fillNull("one")}                                             | ${pl.Series([1, 1, 2])}
    ${"fillNull:max"}    | ${pl.Series([1, null, 5]).fillNull("max")}                                             | ${pl.Series([1, 5, 5])}
    ${"fillNull:min"}    | ${pl.Series([1, null, 5]).fillNull("min")}                                             | ${pl.Series([1, 1, 5])}
    ${"fillNull:mean"}   | ${pl.Series([1, 1, null, 10]).fillNull("mean")}                                        | ${pl.Series([1, 1, 4, 10])}
    ${"fillNull:back"}   | ${pl.Series([1, 1, null, 10]).fillNull("backward")}                                    | ${pl.Series([1, 1, 10, 10])}
    ${"fillNull:fwd"}    | ${pl.Series([1, 1, null, 10]).fillNull("forward")}                                     | ${pl.Series([1, 1, 1, 10])}
    ${"floor"}           | ${pl.Series([1.1, 2.2]).floor()}                                                       | ${pl.Series([1, 2])}
    ${"get"}             | ${pl.Series(["foo"]).get(0)}                                                           | ${"foo"}
    ${"get"}             | ${pl.Series([1, 2, 3]).get(2)}                                                         | ${3}
    ${"getIndex"}        | ${pl.Series(["a", "b", "c"]).getIndex(0)}                                              | ${"a"}
    ${"hasNulls"}        | ${pl.Series([1, null, 2]).hasNulls()}                                                  | ${true}
    ${"hasNulls"}        | ${pl.Series([1, 1, 2]).hasNulls()}                                                     | ${false}
    ${"hash"}            | ${pl.Series([1]).hash()}                                                               | ${pl.Series([11654340066941867156n])}
    ${"head"}            | ${pl.Series([1, 2, 3, 4, 5, 5, 5]).head()}                                             | ${pl.Series([1, 2, 3, 4, 5])}
    ${"head"}            | ${pl.Series([1, 2, 3, 4, 5, 5, 5]).head(2)}                                            | ${pl.Series([1, 2])}
    ${"interpolate"}     | ${pl.Series([1, 2, null, null, 5]).interpolate()}                                      | ${pl.Series([1, 2, 3, 4, 5])}
    ${"isBoolean"}       | ${pl.Series([1, 2, 3]).isBoolean()}                                                    | ${false}
    ${"isBoolean"}       | ${pl.Series([true, false]).isBoolean()}                                                | ${true}
    ${"isDateTime"}      | ${pl.Series([new Date(Date.now())]).isDateTime()}                                      | ${true}
    ${"isDuplicated"}    | ${pl.Series([1, 3, 3]).isDuplicated()}                                                 | ${pl.Series([false, true, true])}
    ${"isFinite"}        | ${pl.Series([1.0, 3.1]).isFinite()}                                                    | ${pl.Series([true, true])}
    ${"isFinite"}        | ${pl.Series([1, 1 / 0]).isFinite()}                                                    | ${pl.Series([true, false])}
    ${"isInfinite"}      | ${pl.Series([1.0, 2]).isInfinite()}                                                    | ${pl.Series([false, false])}
    ${"implode"}         | ${pl.Series("implode", [1, 2, 3], pl.Int32).implode()}                                 | ${pl.Series([[1, 2, 3]])}
    ${"isNotNull"}       | ${pl.Series([1, null, undefined, 2]).isNotNull()}                                      | ${pl.Series([true, false, false, true])}
    ${"isNull"}          | ${pl.Series([1, null, undefined, 2]).isNull()}                                         | ${pl.Series([false, true, true, false])}
    ${"isNumeric"}       | ${pl.Series([1, 2, 3]).isNumeric()}                                                    | ${true}
    ${"isUnique"}        | ${pl.Series([1, 2, 3, 1]).isUnique()}                                                  | ${pl.Series([false, true, true, false])}
    ${"isUtf8"}          | ${pl.Series([1, 2, 3, 1]).dtype.equals(pl.String)}                                     | ${false}
    ${"kurtosis"}        | ${pl.Series([1, 2, 3, 3, 4]).kurtosis()?.toFixed(6)}                                   | ${"-1.044379"}
    ${"isUtf8"}          | ${pl.Series(["foo"]).dtype.equals(pl.String)}                                          | ${true}
    ${"isString"}        | ${pl.Series(["foo"]).isString()}                                                       | ${true}
    ${"len"}             | ${pl.Series([1, 2, 3, 4, 5]).len()}                                                    | ${5}
    ${"limit"}           | ${pl.Series([1, 2, 3, 4, 5, 5, 5]).limit(2)}                                           | ${pl.Series([1, 2])}
    ${"max"}             | ${pl.Series([-1, 10, 3]).max()}                                                        | ${10}
    ${"mean"}            | ${pl.Series([1, 1, 10]).mean()}                                                        | ${4}
    ${"median"}          | ${pl.Series([1, 1, 10]).median()}                                                      | ${1}
    ${"min"}             | ${pl.Series([-1, 10, 3]).min()}                                                        | ${-1}
    ${"nChunks"}         | ${pl.Series([1, 2, 3, 4, 4]).nChunks()}                                                | ${1}
    ${"nullCount"}       | ${pl.Series([1, null, null, 4, 4]).nullCount()}                                        | ${2}
    ${"peakMax"}         | ${pl.Series([9, 4, 5]).peakMax()}                                                      | ${pl.Series([true, false, true])}
    ${"peakMin"}         | ${pl.Series([4, 1, 3, 2, 5]).peakMin()}                                                | ${pl.Series([false, true, false, true, false])}
    ${"product"}         | ${pl.Series([1, 2, 3]).product()}                                                      | ${6}
    ${"quantile"}        | ${pl.Series([1, 2, 3]).quantile(0.5)}                                                  | ${2}
    ${"rank"}            | ${pl.Series([1, 2, 3, 2, 2, 3, 0]).rank("dense")}                                      | ${pl.Series("", [2, 3, 4, 3, 3, 4, 1], pl.UInt32)}
    ${"rename"}          | ${pl.Series([1, 3, 0]).rename("b")}                                                    | ${pl.Series("b", [1, 3, 0])}
    ${"rollingMax"}      | ${pl.Series([1, 2, 3, 2, 1]).rollingMax(2)}                                            | ${pl.Series("", [null, 2, 3, 3, 2], pl.Float64)}
    ${"rollingMin"}      | ${pl.Series([1, 2, 3, 2, 1]).rollingMin(2)}                                            | ${pl.Series("", [null, 1, 2, 2, 1], pl.Float64)}
    ${"rollingSum"}      | ${pl.Series([1, 2, 3, 2, 1]).rollingSum(2)}                                            | ${pl.Series("", [null, 3, 5, 5, 3], pl.Float64)}
    ${"rollingMean"}     | ${pl.Series([1, 2, 3, 2, 1]).rollingMean(2)}                                           | ${pl.Series("", [null, 1.5, 2.5, 2.5, 1.5], pl.Float64)}
    ${"rollingVar"}      | ${pl.Series([1, 2, 3, 2, 1]).rollingVar(2)[1]}                                         | ${0.5}
    ${"rollingStd"}      | ${pl.Series([1, 2, 3, 2, 1]).rollingStd(2).round(2)[1]}                                | ${0.71}
    ${"rollingSkew"}     | ${pl.Series([1, 2, 3, 2, 1]).rollingSkew(2).round(2)[1]}                               | ${0}
    ${"rollingMedian"}   | ${pl.Series([1, 2, 3, 3, 2, 10, 8]).rollingMedian({ windowSize: 2 })}                  | ${pl.Series([null, 1.5, 2.5, 3, 2.5, 6, 9])}
    ${"rollingQuantile"} | ${pl.Series([1, 2, 3, 3, 2, 10, 8]).rollingQuantile({ windowSize: 2, quantile: 0.5 })} | ${pl.Series([null, 2, 3, 3, 3, 10, 10])}
    ${"sample:n"}        | ${pl.Series([1, 2, 3, 4, 5]).sample(2).len()}                                          | ${2}
    ${"sample:frac"}     | ${pl.Series([1, 2, 3, 4, 5]).sample({ frac: 0.4, seed: 0 }).len()}                     | ${2}
    ${"shift"}           | ${pl.Series([1, 2, 3]).shift(1)}                                                       | ${pl.Series([null, 1, 2])}
    ${"shift"}           | ${pl.Series([1, 2, 3]).shift(-1)}                                                      | ${pl.Series([2, 3, null])}
    ${"skew"}            | ${pl.Series([1, 2, 3, 3, 0]).skew()?.toPrecision(6)}                                   | ${"-0.363173"}
    ${"slice"}           | ${pl.Series([1, 2, 3, 3, 0]).slice(-3, 3)}                                             | ${pl.Series([3, 3, 0])}
    ${"slice"}           | ${pl.Series([1, 2, 3, 3, 0]).slice(1, 3)}                                              | ${pl.Series([2, 3, 3])}
    ${"sort"}            | ${pl.Series([4, 2, 5, 1, 2, 3, 3, 0]).sort()}                                          | ${pl.Series([0, 1, 2, 2, 3, 3, 4, 5])}
    ${"sort"}            | ${pl.Series([4, 2, 5, 0]).sort({ descending: true })}                                  | ${pl.Series([5, 4, 2, 0])}
    ${"sort"}            | ${pl.Series([4, 2, 5, 0]).sort({ descending: false })}                                 | ${pl.Series([0, 2, 4, 5])}
    ${"sum"}             | ${pl.Series([1, 2, 2, 1]).sum()}                                                       | ${6}
    ${"tail"}            | ${pl.Series([1, 2, 2, 1]).tail(2)}                                                     | ${pl.Series([2, 1])}
    ${"gatherEvery"}     | ${pl.Series([1, 3, 2, 9, 1]).gatherEvery(2)}                                           | ${pl.Series([1, 2, 1])}
    ${"gather"}          | ${pl.Series([1, 3, 2, 9, 1]).gather([0, 1, 3])}                                        | ${pl.Series([1, 3, 9])}
    ${"gather:array"} | ${pl
  .Series([[1, 2, 3], [4, 5], [6, 7, 8]])
  .gather([2])} | ${pl.Series([[6, 7, 8]])}
    ${"toArray"}         | ${pl.Series([1, 2, 3]).toArray()}                                                      | ${[1, 2, 3]}
    ${"unique"}          | ${pl.Series([1, 2, 3, 3]).unique().sort()}                                             | ${pl.Series([1, 2, 3])}
    ${"cumCount"}        | ${pl.Series([1, 2, 3, 3]).cumCount()}                                                  | ${pl.Series([1, 2, 3, 4])}
    ${"shiftAndFill"}    | ${pl.Series("foo", [1, 2, 3]).shiftAndFill(1, 99)}                                     | ${pl.Series("foo", [99, 1, 2])}
    ${"shrinkToFit"}     | ${pl.Series("foo", [1, 2, 3]).shrinkToFit()}                                           | ${pl.Series("foo", [1, 2, 3])}
    ${"bitand"}          | ${pl.Series("bit", [1, 2, 3], pl.Int32).bitand(pl.Series("bit", [0, 1, 1], pl.Int32))} | ${pl.Series("bit", [0, 0, 1])}
    ${"bitor"}           | ${pl.Series("bit", [1, 2, 3], pl.Int32).bitor(pl.Series("bit", [0, 1, 1], pl.Int32))}  | ${pl.Series("bit", [1, 3, 3])}
    ${"bitxor"}          | ${pl.Series("bit", [1, 2, 3], pl.Int32).bitxor(pl.Series("bit", [0, 1, 1], pl.Int32))} | ${pl.Series("bit", [1, 3, 2])}
    ${"all"}             | ${pl.Series("foo", [true, true], pl.Bool).all()}                                       | ${true}
    ${"all"}             | ${pl.Series("foo", [false, true], pl.Bool).all()}                                      | ${false}
    ${"all"}             | ${pl.Series("foo", [null, true], pl.Bool).all()}                                       | ${true}
    ${"all"}             | ${pl.Series("foo", [null, true], pl.Bool).all(false)}                                  | ${null}
    ${"any"}             | ${pl.Series("foo", [true, false], pl.Bool).any()}                                      | ${true}
    ${"any"}             | ${pl.Series("foo", [false, false], pl.Bool).any()}                                     | ${false}
    ${"any"}             | ${pl.Series("foo", [null, false], pl.Bool).any()}                                      | ${false}
    ${"any"}             | ${pl.Series("foo", [null, false], pl.Bool).any(false)}                                 | ${null}
    ${"not"}             | ${pl.Series("foo", [true, false, false], pl.Bool).not()}                               | ${pl.Series("foo", [false, true, true], pl.Bool)}
  `("$# $name: expected matches actual ", ({ expected, actual }) => {
    if (pl.Series.isSeries(expected) && pl.Series.isSeries(actual)) {
      assertSeriesEqual(actual, expected);
    } else {
      assert.deepStrictEqual(actual, expected);
    }
  });
  it("describe", () => {
    assert.throws(
      () => pl.Series([]).describe(),
      /Series must contain at least one value/,
    );
    assert.throws(
      () => pl.Series("dt", [null], pl.Date).describe(),
      /Invalid operation: describe is not supported for DataType\(Date\)/,
    );
    {
      const actual = pl.Series([true, false, true]).describe();
      const expected = pl.DataFrame({
        statistic: ["sum", "null_count", "count"],
        value: [false, null, null],
      });
      assertFrameEqual(actual, expected);
    }
    {
      const actual = pl.Series(["a", "b", "c", null]).describe();
      const expected = pl.DataFrame({
        statistic: ["unique", "null_count", "count"],
        value: [4, 1, 4],
      });
      assertFrameEqual(actual, expected);
    }
  });
  it("series:valueCounts", () => {
    {
      const actual = pl.Series("a", [1, 2, 2, 3]).valueCounts(true);
      const expected = pl.DataFrame({
        a: [2, 1, 3],
        count: [2, 1, 1],
      });
      assertFrameEqual(actual, expected);
    }
    {
      const actual = pl
        .Series("a", [1, 2, 2, 3])
        .valueCounts(true, true, undefined, true);
      const expected = pl.DataFrame({
        a: [2, 1, 3],
        proportion: [0.5, 0.25, 0.25],
      });
      assertFrameEqual(actual, expected);
    }
    {
      const actual = pl
        .Series("a", [1, 2, 2, 3])
        .valueCounts(true, true, "foo", false);
      const expected = pl.DataFrame({
        a: [2, 1, 3],
        foo: [2, 1, 1],
      });
      assertFrameEqual(actual, expected);
    }
    {
      const actual = pl
        .Series("a", [1, 2, 2, 3])
        .valueCounts(true, true, "foo", true);
      const expected = pl.DataFrame({
        a: [2, 1, 3],
        foo: [0.5, 0.25, 0.25],
      });
      assertFrameEqual(actual, expected);
    }
  });
  it("set: expected matches actual", () => {
    const expected = pl.Series([99, 2, 3]);
    const mask = pl.Series([true, false, false]);
    const actual = pl.Series([1, 2, 3]).set(mask, 99);
    assertSeriesEqual(actual, expected);
  });
  it("set: throws error", () => {
    const mask = pl.Series([true]);
    assert.throws(() => pl.Series([1, 2, 3]).set(mask, 99));
  });
  it("scatter:array expected matches actual", () => {
    const expected = pl.Series([99, 2, 99]);
    const actual = pl.Series([1, 2, 3]);
    actual.scatter([0, 2], 99);
    assertSeriesEqual(actual, expected);
  });
  it("scatter:series expected matches actual", () => {
    const expected = pl.Series([99, 2, 99]);
    const indices = pl.Series([0, 2]);
    const actual = pl.Series([1, 2, 3]);
    actual.scatter(indices, 99);
    assertSeriesEqual(actual, expected);
  });
  it("scatter: throws error", () => {
    const mask = pl.Series([true]);
    assert.throws(() => pl.Series([1, 2, 3]).set(mask, 99));
  });
  it.each`
    name            | fn                                                  | errorType
    ${"isFinite"}   | ${pl.Series(["foo"]).isFinite}                      | ${TypeError}
    ${"isInfinite"} | ${pl.Series(["foo"]).isInfinite}                    | ${TypeError}
    ${"rollingMax"} | ${() => pl.Series(["foo"]).rollingMax(null as any)} | ${Error}
    ${"sample"}     | ${() => pl.Series(["foo"]).sample(null as any)}     | ${Error}
  `("$# $name throws an error ", ({ fn, errorType }) => {
    assert.throws(fn, errorType);
  });
  test("reinterpret", () => {
    const s = pl.Series("reinterpret", [1, 2], pl.Int64);
    const unsignedExpected = pl.Series("reinterpret", [1n, 2n], pl.UInt64);
    const signedExpected = pl.Series("reinterpret", [1, 2], pl.Int64);
    const unsigned = s.reinterpret(false);
    const signed = unsigned.reinterpret(true);

    assertSeriesStrictEqual(unsigned, unsignedExpected);
    assertSeriesStrictEqual(signed, signedExpected);
  });
  test("reinterpret:invalid", () => {
    const s = pl.Series("reinterpret", [1, 2]);
    const fn = () => s.reinterpret();
    assert.throws(fn);
  });
  test("extendConstant", () => {
    const s = pl.Series("extended", [1], pl.UInt16);
    const expected = pl.Series("extended", [1, null, null], pl.UInt16);
    const actual = s.extendConstant(null, 2);
    assertSeriesStrictEqual(actual, expected);
  });
  test("extend", () => {
    const a = pl.Series("a", [1, 2, 3]);
    const b = pl.Series("b", [4, 5]);
    const actual = a.extend(b);
    const expected = pl.Series("a", [1, 2, 3, 4, 5]);
    assertSeriesEqual(a, expected);
    assertSeriesEqual(actual, expected);
  });
  test("round invalid", () => {
    const s = pl.Series([true, false]);
    const fn = () => s.round(2);
    assert.throws(fn);
  });
  test("round:positional", () => {
    const s = pl.Series([1.1111, 2.2222]);
    const expected = pl.Series([1.11, 2.22]);
    const actual = s.round(2);
    assertSeriesEqual(actual, expected);
  });
  test("round:halfawayfromzero", () => {
    const s = pl.Series([1.5, 2.5, -1.5, -2.5]);
    const expected = pl.Series([2, 3, -2, -3]);
    const actual = s.round(0, "halfawayfromzero");
    assertSeriesEqual(actual, expected);
  });
  test("round:halfawayfromzero:opt", () => {
    const s = pl.Series([1.5, 2.5, -1.5, -2.5]);
    const expected = pl.Series([2, 3, -2, -3]);
    const actual = s.round({ decimals: 0, mode: "halfawayfromzero" });
    assertSeriesEqual(actual, expected);
  });
  test("round:named", () => {
    const s = pl.Series([1.1111, 2.2222]);
    const expected = pl.Series([1.11, 2.22]);
    const actual = s.round({ decimals: 2 });
    assertSeriesEqual(actual, expected);
  });
  test("toTypedArray handles nulls", () => {
    const s = pl.Series("ints and nulls", [1, 2, 3, null, 5], pl.UInt8);
    assert.throws(() => s.toTypedArray());
    assert.doesNotThrow(() => s.dropNulls().toTypedArray());
    assert.deepStrictEqual(
      s.dropNulls().toTypedArray(),
      new Uint8Array([1, 2, 3, 5]),
    );
  });
  test("values()", () => {
    const s = pl.Series.from("foo", [1, 2, 3]);
    const actual = s.values().next();
    const expected = { done: false, value: 1 };
    assert.deepStrictEqual(actual, expected);
  });
  test("from:Uint8ClampedArray", () => {
    const actual: pl.Series = pl.Series.from(
      "Uint8ClampedArray",
      new Uint8ClampedArray([3, 2, 1]),
    );
    const expected = pl.Series("Uint8ClampedArray", [3, 2, 1], pl.UInt8);
    assertSeriesEqual(actual, expected);
  });
  test("toDummies", () => {
    const s = pl.Series("a", [1, 2, 3]);
    {
      const actual = s.toDummies();
      const expected = pl.DataFrame(
        { "a_1.0": [1, 0, 0], "a_2.0": [0, 1, 0], "a_3.0": [0, 0, 1] },
        { schema: { "a_1.0": pl.UInt8, "a_2.0": pl.UInt8, "a_3.0": pl.UInt8 } },
      );
      assertFrameEqual(actual, expected);
    }
    {
      const actual = s.toDummies(":", true, false);
      const expected = pl.DataFrame(
        { "a:2.0": [0, 1, 0], "a:3.0": [0, 0, 1] },
        { schema: { "a:2.0": pl.UInt8, "a:3.0": pl.UInt8 } },
      );
      assertFrameEqual(actual, expected);
    }
  });

  test("entrypoint alias getters expose the same wrappers", () => {
    const listSeries = pl.Series("items", [[1, 2], [3], []]);
    const dateSeries = pl.Series("dt", [new Date("2020-01-01T00:00:00.000Z")]);

    assertSeriesEqual(listSeries.list.lengths(), listSeries.lst.lengths());
    assertSeriesEqual(dateSeries.date.year(), dateSeries.dt.year());
  });

  test("filter and isIn accept array inputs", () => {
    const actual = pl.Series([1, 2, 3]).filter([true, false, true] as any);
    const expected = pl.Series([1, 3]);
    assertSeriesEqual(actual, expected);

    const membership = pl.Series([1, 2, 3]).isIn([2, 4]);
    assertSeriesEqual(membership, pl.Series([false, true, false]));
  });

  test("hasValidity aliases hasNulls", () => {
    assert.strictEqual(pl.Series([1, null, 3]).hasValidity(), true);
    assert.strictEqual(pl.Series([1, 2, 3]).hasValidity(), false);
  });

  test("sample covers default and object n overloads", () => {
    const s = pl.Series([1, 2, 3, 4, 5]);

    assert.strictEqual(s.sample().len(), 1);
    assert.strictEqual(s.sample({ n: 2, withReplacement: false }).len(), 2);
  });

  test("isFloat returns true for float series", () => {
    assert.strictEqual(pl.Series("", [1.5, 2.5], pl.Float64).isFloat(), true);
  });

  test("shrinkToFit in place keeps series contents", () => {
    const s = pl.Series("foo", [1, 2, 3]);

    s.shrinkToFit(true);

    assertSeriesEqual(s, pl.Series("foo", [1, 2, 3]));
  });

  test("slice supports object arguments", () => {
    const actual = pl
      .Series([1, 2, 3, 4])
      .slice({ offset: 1, length: 2 } as any);
    const expected = pl.Series([2, 3]);
    assertSeriesEqual(actual, expected);
  });

  test("unique supports stable ordering", () => {
    const actual = pl.Series([3, 1, 3, 2, 1]).unique(true);
    const expected = pl.Series([3, 1, 2]);
    assertSeriesEqual(actual, expected);
  });

  test("toJSON supports direct calls and JSON.stringify", () => {
    const s = pl.Series("nums", [1, 2, 3]);
    const nestedJson = JSON.parse(JSON.stringify({ values: s }));
    const directJson = (s.toJSON as any)("");

    assert.strictEqual(typeof s.toJSON(), "string");
    assert.strictEqual(Buffer.isBuffer(directJson), true);
    assert.deepStrictEqual(nestedJson, { values: s.toJSON() });
  });

  test("numeric proxy access supports get and set", () => {
    const s = pl.Series([1, 2, 3]);

    assert.strictEqual((s as any)[1], 2);

    (s as any)[1] = 99;

    assertSeriesEqual(s, pl.Series([1, 99, 3]));
  });

  test("isFinite and isInfinite throw InvalidOperationError for non-floats", () => {
    assert.throws(
      () => pl.Series(["foo"]).isFinite(),
      /Invalid operation: isFinite is not supported for DataType\(String\)/,
    );
    assert.throws(
      () => pl.Series(["foo"]).isInfinite(),
      /Invalid operation: isFinite is not supported for DataType\(String\)/,
    );
  });
});
describe("comparators & math", () => {
  test("duration/add/series", () => {
    const drs = pl.Series("dur", [1], pl.Duration("ms"));
    const dt = new Date(Date.now());
    const ds = pl.Series("dt", [dt], pl.Datetime("ms", ""));
    const expected = dt.getMilliseconds() + 1;
    const actual = ds.add(drs).values().next().value.getMilliseconds();
    assert.deepStrictEqual(actual, expected);
  });
  test("add/plus/series", () => {
    const s = pl.Series([1, 2, 3]);
    const expected = pl.Series([2, 4, 6]);
    assertSeriesEqual(s.add(s), expected);
    assertSeriesEqual(s.plus(s), expected);
  });
  test("minus/series", () => {
    const s = pl.Series([1, 2, 3]);
    assertSeriesEqual(s.plus(s).minus(s), s);
    assertSeriesEqual(s.add(s).sub(s), s);
  });
  test("eq/series", () => {
    const s = pl.Series([1, 2, 3]);
    const s2 = pl.Series([1, 3, 3]);
    const expected = pl.Series([true, false, true]);
    assertSeriesEqual(s.eq(s2), expected);
    assertSeriesEqual(s.equals(s2), expected);
  });
  test("gt/series", () => {
    const s = pl.Series([1, 2, 3]);
    const s2 = pl.Series([2, 2, 4]);
    const expected = pl.Series([true, false, true]);
    assertSeriesEqual(s2.gt(s), expected);
    assertSeriesEqual(s2.greaterThan(s), expected);
  });
  test("gteq/series", () => {
    const s = pl.Series([1, 2, 3]);
    const s2 = pl.Series([2, 2, 4]);
    const expected = pl.Series([true, true, true]);
    assertSeriesEqual(s2.gtEq(s), expected);
    assertSeriesEqual(s2.greaterThanEquals(s), expected);
  });
  test("rem/modulo/series", () => {
    const s = pl.Series([1, 2, 3]);
    const s2 = pl.Series([2, 3, 4]);
    assertSeriesEqual(s.rem(s2), s);
    assertSeriesEqual(s.modulo(s2), s);
  });
  test("div/series", () => {
    const s = pl.Series([1, 2, 3]);
    const expected = pl.Series([2, 2, 2]);
    assertSeriesEqual(s.plus(s).div(s), expected);
    assertSeriesEqual(s.plus(s).divideBy(s), expected);
  });
  test("add/plus", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([2, 3]);
    assertSeriesEqual(s.add(1), expected);
    assertSeriesEqual(s.plus(1), expected);
  });
  test("sub/minus", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([0, 1]);
    assertSeriesEqual(s.sub(1), expected);
    assertSeriesEqual(s.minus(1), expected);
  });
  test("mul/multiplyBy", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([10, 20]);
    assertSeriesEqual(s.mul(10), expected);
    assertSeriesEqual(s.multiplyBy(10), expected);
  });
  test("div/divideBy", () => {
    const s = pl.Series([2, 4]);
    const expected = pl.Series([1, 2]);
    assertSeriesEqual(s.div(2), expected);
    assertSeriesEqual(s.divideBy(2), expected);
  });
  test("div/divideBy", () => {
    const s = pl.Series([2, 4]);
    const expected = pl.Series([1, 2]);
    assertSeriesEqual(s.div(2), expected);
    assertSeriesEqual(s.divideBy(2), expected);
  });
  test("rem/modulo", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([1, 0]);
    assertSeriesEqual(s.rem(2), expected);
    assertSeriesEqual(s.modulo(2), expected);
  });
  test("eq/equals", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([true, false]);
    assertSeriesEqual(s.eq(1), expected);
    assertSeriesEqual(s.equals(1), expected);
  });
  test("neq/notEquals", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([false, true]);
    assertSeriesEqual(s.neq(1), expected);
    assertSeriesEqual(s.notEquals(1), expected);
  });
  test("gt/greaterThan", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([false, true]);
    assertSeriesEqual(s.gt(1), expected);
    assertSeriesEqual(s.greaterThan(1), expected);
  });
  test("gtEq/equals", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([true, true]);
    assertSeriesEqual(s.gtEq(1), expected);
    assertSeriesEqual(s.greaterThanEquals(1), expected);
  });
  test("lt/lessThan", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([false, false]);
    assertSeriesEqual(s.lt(1), expected);
    assertSeriesEqual(s.lessThan(1), expected);
  });
  test("ltEq/lessThanEquals", () => {
    const s = pl.Series([1, 2]);
    const expected = pl.Series([true, false]);
    assertSeriesEqual(s.ltEq(1), expected);
    assertSeriesEqual(s.lessThanEquals(1), expected);
    let fn = () => s.ltEq("1");
    assert.throws(fn, /Not a number nor a series/);
    fn = () => s.lessThanEquals("1");
    assert.throws(fn, /Not a number nor a series/);
  });
});
describe("StringFunctions", () => {
  it.each`
    name             | actual                                         | expected
    ${"toUpperCase"} | ${pl.Series(["foo"]).str.toUpperCase()}        | ${pl.Series(["FOO"])}
    ${"strip"}       | ${pl.Series([" foo "]).str.strip()}            | ${pl.Series(["foo"])}
    ${"lstrip"}      | ${pl.Series(["  foo"]).str.lstrip()}           | ${pl.Series(["foo"])}
    ${"rstrip"}      | ${pl.Series(["foo   "]).str.rstrip()}          | ${pl.Series(["foo"])}
    ${"toLowerCase"} | ${pl.Series(["FOO"]).str.toLowerCase()}        | ${pl.Series(["foo"])}
    ${"contains"}    | ${pl.Series(["f1", "f0"]).str.contains(/[0]/)} | ${pl.Series([false, true])}
    ${"lengths"}     | ${pl.Series(["apple", "ham"]).str.lengths()}   | ${pl.Series([5, 3])}
    ${"slice"}       | ${pl.Series(["apple", "ham"]).str.slice(1)}    | ${pl.Series(["pple", "am"])}
  `("$# $name expected matches actual", ({ expected, actual }) => {
    assertSeriesEqual(expected, actual);
  });
  test("hex encode", () => {
    const s = pl.Series("strings", ["foo", "bar", null]);
    const expected = pl.Series("encoded", ["666f6f", "626172", null]);
    const encoded = s.str.encode("hex").alias("encoded");
    assertSeriesEqual(encoded, expected);
  });
  test("hex decode", () => {
    const s = pl.Series("encoded", ["666f6f", "626172", "invalid", null]);
    const expected = pl.Series("decoded", ["foo", "bar", null, null]);
    const decoded = s.str.decode("hex").alias("decoded");
    assertSeriesEqual(decoded, expected);
  });
  test("hex decode strict", () => {
    const s = pl.Series("encoded", ["666f6f", "626172", "invalid", null]);
    const fn0 = () => s.str.decode("hex", true).alias("decoded");
    const fn1 = () =>
      s.str.decode({ encoding: "hex", strict: true }).alias("decoded");
    assert.throws(fn0);
    assert.throws(fn1);
  });
  test("encode base64", () => {
    const s = pl.Series("strings", ["foo", "bar"]);
    const expected = pl.Series("encoded", ["Zm9v", "YmFy"]);
    const encoded = s.str.encode("base64").alias("encoded");
    assertSeriesEqual(encoded, expected);
  });
  test("base64 decode strict", () => {
    const s = pl.Series("encoded", [
      "Zm9v",
      "YmFy",
      "not base64 encoded",
      null,
    ]);
    const fn0 = () => s.str.decode("base64", true).alias("decoded");
    const fn1 = () =>
      s.str.decode({ encoding: "base64", strict: true }).alias("decoded");
    assert.throws(fn0);
    assert.throws(fn1);
  });
  test("base64 decode", () => {
    const s = pl.Series("encoded", ["Zm9v", "YmFy", "invalid", null]);
    const decoded = pl.Series("decoded", ["foo", "bar", null, null]);

    const actual = s.str.decode("base64").alias("decoded");
    assertSeriesEqual(actual, decoded);
  });
  test("inspect", () => {
    const s = pl.Series("strings", ["foo", "bar"]);
    const actualInspect = s[Symbol.for("nodejs.util.inspect.custom")]();
    const serString = s.toString();
    assert.deepStrictEqual(actualInspect, serString);
  });
  test("str contains", () => {
    const s = pl.Series(["linux-kali", "linux-debian", "windows-vista"]);
    const expected = pl.Series([true, true, false]);
    const encoded = s.str.contains("linux");
    assertSeriesEqual(encoded, expected);
  });
});
describe("series struct", () => {
  test("struct:fields", () => {
    const expected = [{ foo: 1, bar: 2, ham: "c" }];
    const actual = pl.Series(expected);
    const actualFields = actual.struct.fields;
    const expectedKeys = new Set(expected.flatMap((item) => Object.keys(item)));
    const expectedFields = [...expectedKeys];
    assert.deepStrictEqual(actualFields, expectedFields);
  });
  test("struct:field", () => {
    const expected = [{ foo: 1, bar: 2, ham: "c" }];
    const actual = pl.Series(expected).struct.field("foo").toArray();
    assert.deepStrictEqual(actual, [expected[0]["foo"]]);
  });
  test("struct:frame", () => {
    const array = [{ foo: 1, bar: 2, ham: "c" }];
    const actual = pl.Series(array).struct.toFrame();
    const expected = pl.DataFrame({
      foo: [1],
      bar: [2],
      ham: ["c"],
    });
    assertFrameEqual(actual, expected);
  });
  test("struct:renameFields", () => {
    const expected = [{ foo: 1, bar: 2, ham: "c" }];
    const actual = pl
      .Series(expected)
      .struct.renameFields(["foo", "bar", "ham"])
      .toArray();
    assert.deepStrictEqual(actual, expected);
  });
  test("struct:nth", () => {
    const arr = [
      { foo: 1, bar: 2, ham: "c" },
      { foo: null, bar: 10, ham: null },
      { foo: 2, bar: 0, ham: "z" },
    ];
    const expected = [1, null, 2];
    const actual = pl.Series(arr).struct.nth(0).toArray();
    assert.deepStrictEqual(actual, expected);
  });
});
describe("generics", () => {
  const series = pl.Series([1, 2, 3]);
  test("dtype", () => {
    assert.deepStrictEqual(series.dtype, DataType.Float64);
  });
  test("to array", () => {
    const arr = series.toArray();
    assert.deepStrictEqual(arr, [1, 2, 3]);
    const arr2 = [...series];
    assert.deepStrictEqual(arr2, [1, 2, 3]);
  });
});
describe("series date", () => {
  test("truncate time", () => {
    const s = pl.Series("datetime", [
      new Date(Date.parse("2020-01-01T01:32:00.002+00:00")),
      new Date(Date.parse("2020-01-01T02:02:01.030+00:00")),
      new Date(Date.parse("2020-01-01T04:42:20.001+00:00")),
    ]);
    const actual = s.dt.truncate("30m").dt.minute().alias("30m");
    const expected = pl.Series("30min", [30, 0, 30], pl.Int8);
    assertSeriesStrictEqual(actual, expected);

    const actual2 = s.dt.truncate("1h").dt.hour().alias("1hr");
    const expected2 = pl.Series("1hr", [1, 2, 4], pl.Int8);
    assertSeriesStrictEqual(actual2, expected2);
  });
  test("round time", () => {
    const s = pl.Series("datetime", [
      new Date(Date.parse("2020-01-01T01:32:00.002+00:00")),
      new Date(Date.parse("2020-01-01T02:02:01.030+00:00")),
      new Date(Date.parse("2020-01-01T04:42:20.001+00:00")),
    ]);
    const actual = s.dt.round("30m").dt.minute().alias("30m");
    const expected = pl.Series("30min", [30, 0, 30], pl.Int8);
    assertSeriesStrictEqual(actual, expected);

    const actual2 = s.dt.round("1h").dt.hour().alias("1hr");
    const expected2 = pl.Series("1hr", [2, 2, 5], pl.Int8);
    assertSeriesStrictEqual(actual2, expected2);
  });
});
