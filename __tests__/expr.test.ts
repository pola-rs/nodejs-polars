/* eslint-disable newline-per-chained-call */
import pl, { col, lit } from "@polars/index";
const df = () => {
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
describe("expr", () => {
  test("abs", () => {
    const expected = pl.Series("abs", [1, 2, 3]);
    const df = pl.DataFrame({ a: [1, -2, -3] });
    const actual = df.select(col("a").as("abs").abs()).getColumn("abs");
    expect(actual).toSeriesEqual(expected);
  });
  test("alias", () => {
    const name = "alias";
    const actual = pl.select(lit("a").alias(name));
    expect(actual.columns[0]).toStrictEqual(name);
  });
  test("and", () => {
    const actual = df().filter(col("bools").eq(false).and(col("int").eq(3)));
    expect(actual.height).toStrictEqual(1);
  });
  test("arccos", () => {
    const df = pl.DataFrame({ a: [1, 2] });
    const expected = pl.DataFrame({ arccos: [0.0, Number.NaN] });
    const actual = df.select(col("a").arccos().as("arccos"));
    expect(actual).toFrameEqual(expected);
  });
  test("arcsin", () => {
    const df = pl.DataFrame({ a: [1, 2] });
    const expected = pl.DataFrame({ arcsin: [1.570796, Number.NaN] });
    const actual = df.select(col("a").arcsin().round(6).as("arcsin"));
    expect(actual).toFrameEqual(expected);
  });
  test("arcyan", () => {
    const df = pl.DataFrame({ a: [1, 2] });
    const expected = pl.DataFrame({ arctan: [0.785398, 1.107149] });
    const actual = df.select(col("a").arctan().round(6).as("arctan"));
    expect(actual).toFrameEqual(expected);
  });
  test("argMax", () => {
    const actual = df().select(col("int").argMax()).row(0)[0];
    expect(actual).toStrictEqual(2);
  });
  test("argMin", () => {
    const actual = df().select(col("int").argMin()).row(0)[0];
    expect(actual).toStrictEqual(0);
  });
  test.each`
    args                    | expectedSort
    ${undefined}            | ${[1, 0, 3, 2]}
    ${true}                 | ${[2, 3, 0, 1]}
    ${{ descending: true }} | ${[2, 3, 0, 1]}
    ${{ reverse: true }}    | ${[2, 3, 0, 1]}
  `("argSort", ({ args, expectedSort }) => {
    const df = pl.DataFrame({ a: [1, 0, 2, 1.5] });
    const expected = pl.DataFrame({ argSort: expectedSort });
    const actual = df.select(col("a").argSort(args).alias("argSort"));
    expect(actual).toFrameEqual(expected);
  });
  test("argUnique", () => {
    const df = pl.DataFrame({ a: [1, 1, 4, 1.5] });
    const expected = pl.DataFrame({ argUnique: [0, 2, 3] });
    const actual = df.select(
      col("a").argUnique().cast(pl.Float64).alias("argUnique"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("as", () => {
    const name = "as";
    const actual = pl.select(lit("a").as(name));
    expect(actual.columns[0]).toStrictEqual(name);
  });
  test("backwardFill", () => {
    const df = pl.DataFrame({ a: [null, 1, null, 3] });
    const expected = pl.DataFrame({ a: [1, 1, 3, 3] });
    const actual = df.select(col("a").backwardFill());
    expect(actual).toFrameEqual(expected);
  });
  test("cast", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.Series("a", [1, 2, 3], pl.Int16).toFrame();
    const actual = df.select(col("a").cast(pl.Int16));
    expect(actual).toFrameEqual(expected);
  });
  test("cast:strict", () => {
    const df = pl.DataFrame({
      a: [11111111111111n, 222222222222222222n, null],
    });

    const fn = () => df.select(col("a").cast(pl.Int16, true));
    expect(fn).toThrow();
  });
  test("cos", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ cos: [0.540302, -0.416147, -0.989992] });
    const actual = df.select(col("a").cos().round(6).as("cos"));
    expect(actual).toFrameEqual(expected);
  });
  test("cot", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ cot: [0.642093, -0.457658, -7.015253] });
    const actual = df.select(col("a").cot().round(6).as("cot"));
    expect(actual).toFrameEqual(expected);
  });
  test("count", () => {
    const df = pl.DataFrame({ a: [1, 0, 3, 4, 6, 0] });
    const expected = pl.DataFrame({ a: [6] });
    const actual = df.select(col("a").count().cast(pl.Float64));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    args                 | cumCount
    ${undefined}         | ${[1, 2, 3]}
    ${true}              | ${[3, 2, 1]}
    ${{ reverse: true }} | ${[3, 2, 1]}
  `("$# cumCount", ({ args, cumCount }) => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ a: cumCount });
    const actual = df.select(col("a").cumCount(args).cast(pl.Float64));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    args                 | cumMax
    ${undefined}         | ${[1, 1, 2, 2, 3]}
    ${true}              | ${[3, 3, 3, 3, 3]}
    ${{ reverse: true }} | ${[3, 3, 3, 3, 3]}
  `("$# cumMax", ({ args, cumMax }) => {
    const df = pl.DataFrame({ a: [1, 0, 2, 1, 3] });
    const expected = pl.DataFrame({ a: cumMax });
    const actual = df.select(col("a").cumMax(args).cast(pl.Float64));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    args                 | cumMin
    ${undefined}         | ${[1, 0, 0, 0, 0]}
    ${true}              | ${[0, 0, 1, 1, 3]}
    ${{ reverse: true }} | ${[0, 0, 1, 1, 3]}
  `("$# cumMin", ({ args, cumMin }) => {
    const df = pl.DataFrame({ a: [1, 0, 2, 1, 3] });
    const expected = pl.DataFrame({ a: cumMin });
    const actual = df.select(col("a").cumMin(args).cast(pl.Float64));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    args                 | cumProd
    ${undefined}         | ${[1, 2, 4, 4, 12]}
    ${true}              | ${[12, 12, 6, 3, 3]}
    ${{ reverse: true }} | ${[12, 12, 6, 3, 3]}
  `("$# cumProd", ({ args, cumProd }) => {
    const df = pl.DataFrame({ a: [1, 2, 2, 1, 3] });
    const expected = pl.DataFrame({ a: cumProd });
    const actual = df.select(
      col("a").cast(pl.Int64).cumProd(args).cast(pl.Float64),
    );
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    args                 | cumSum
    ${undefined}         | ${[1, 3, 5, 6, 9]}
    ${true}              | ${[9, 8, 6, 4, 3]}
    ${{ reverse: true }} | ${[9, 8, 6, 4, 3]}
  `("cumSum", ({ args, cumSum }) => {
    const df = pl.DataFrame({ a: [1, 2, 2, 1, 3] });
    const expected = pl.DataFrame({ a: cumSum });
    const actual = df.select(col("a").cumSum(args).cast(pl.Float64));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    args                                  | diff
    ${[1, "ignore"]}                      | ${[null, 2, null, null, 7]}
    ${[{ n: 1, nullBehavior: "ignore" }]} | ${[null, 2, null, null, 7]}
    ${[{ n: 1, nullBehavior: "drop" }]}   | ${[2, null, null, 7]}
    ${[1, "drop"]}                        | ${[2, null, null, 7]}
    ${[2, "drop"]}                        | ${[null, 0, null]}
    ${[{ n: 2, nullBehavior: "drop" }]}   | ${[null, 0, null]}
  `("$# diff", ({ args, diff }: any) => {
    const df = pl.DataFrame({ a: [1, 3, null, 3, 10] });
    const expected = pl.DataFrame({ a: diff });
    const actual = df.select((col("a").diff as any)(...args));
    expect(actual).toFrameEqual(expected);
  });
  test("dot", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3, 4],
      b: [2, 2, 2, 2],
    });
    const expected = pl.DataFrame({ a: [20] });

    const actual0 = df.select(col("a").dot(col("b")));
    const actual1 = df.select(col("a").dot("b"));
    expect(actual0).toFrameEqual(expected);
    expect(actual1).toFrameEqual(expected);
  });
  test.each`
    other       | eq
    ${col("b")} | ${[false, true, false]}
    ${lit(1)}   | ${[true, false, false]}
  `("$# eq", ({ other, eq }) => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: [2, 2, 2],
    });
    const expected = pl.DataFrame({ eq: eq });
    const actual = df.select(col("a").eq(other).as("eq"));
    expect(actual).toFrameEqual(expected);
  });
  test("exclude", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: [2, 2, 2],
      c: ["mac", "windows", "linux"],
    });
    const expected = pl.DataFrame({ a: [1, 2, 3] });
    const actual = df.select(col("*").exclude("b", "c"));
    expect(actual).toFrameEqual(expected);
  });
  test("exp", () => {
    const df = pl.DataFrame({ a: [1] });
    const actual = df.select(pl.col("a").exp().round(6));
    const expected = pl.DataFrame({
      a: [Math.round(Math.E * 1_000_000) / 1_000_000],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("explode", () => {
    const df = pl.DataFrame({
      letters: ["c", "a"],
      nrs: [
        [1, 2],
        [1, 3],
      ],
    });
    const expected = pl.DataFrame({
      nrs: [1, 2, 1, 3],
    });
    const actual = df.select(col("nrs").explode());
    expect(actual).toFrameEqual(expected);
  });
  test("implode", () => {
    const df = pl.DataFrame({
      nrs: [1, 2, 1, 3],
      strs: ["a", "b", null, "d"],
    });
    const expected = pl.DataFrame({
      nrs: [[1, 2, 1, 3]],
      strs: [["a", "b", null, "d"]],
    });
    const actual = df.select(col("nrs").implode(), col("strs").implode());
    expect(actual).toFrameEqual(expected);
  });
  test("extend", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3, 4, 5],
      b: [2, 3, 4, 5, 6],
    });
    const other = pl.Series("c", ["a", "b", "c"]);
    const expected = pl.DataFrame({
      a: [1, 2, 3, 4, 5],
      b: [2, 3, 4, 5, 6],
      c: ["a", "b", "c", null, null],
    });
    const actual = df.withColumn(other.extendConstant({ value: null, n: 2 }));
    expect(actual).toFrameEqual(expected);
  });
  test("extend:positional", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3, 4, 5],
      b: [2, 3, 4, 5, 6],
    });
    const other = pl.Series("c", ["a", "b", "c"]);
    const expected = pl.DataFrame({
      a: [1, 2, 3, 4, 5],
      b: [2, 3, 4, 5, 6],
      c: ["a", "b", "c", "foo", "foo"],
    });
    const actual = df.withColumn(other.extendConstant("foo", 2));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    replacement | filled
    ${lit(1)}   | ${1}
    ${2}        | ${2}
  `("$# fillNan", ({ replacement, filled }) => {
    const df = pl.DataFrame({
      a: [1, Number.NaN, 2],
      b: [2, 1, 1],
    });
    const expected = pl.DataFrame({ fillNan: [1, filled, 2] });
    const actual = df.select(col("a").fillNan(replacement).as("fillNan"));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    fillValue     | filled
    ${"backward"} | ${[1, 2, 2, 9, 9, null]}
    ${"forward"}  | ${[1, 1, 2, 2, 9, 9]}
    ${"mean"}     | ${[1, 4, 2, 4, 9, 4]}
    ${"min"}      | ${[1, 1, 2, 1, 9, 1]}
    ${"max"}      | ${[1, 9, 2, 9, 9, 9]}
    ${"zero"}     | ${[1, 0, 2, 0, 9, 0]}
    ${"one"}      | ${[1, 1, 2, 1, 9, 1]}
    ${-1}         | ${[1, -1, 2, -1, 9, -1]}
  `("$# fillNull:'$fillValue'", ({ fillValue, filled }) => {
    const df = pl.DataFrame({ a: [1, null, 2, null, 9, null] });
    const expected = pl.DataFrame({ fillNull: filled });
    const actual = df.select(col("a").fillNull(fillValue).as("fillNull"));
    expect(actual).toFrameEqual(expected);
  });
  test("filter", () => {
    const df = pl.DataFrame({ a: [-1, 2, -3, 4] });
    const expected = pl.DataFrame({ a: [2, 4] });
    const actual = df.select(col("a").filter(col("a").gt(0)));
    expect(actual).toFrameEqual(expected);
  });
  test("first", () => {
    const df = pl.DataFrame({ a: [0, 1, 2] });
    const expected = pl.DataFrame({ a: [0] });
    const actual = df.select(col("a").first());
    expect(actual).toFrameEqual(expected);
  });
  test("flatten", () => {
    const df = pl.DataFrame({
      letters: ["c", "a"],
      nrs: [
        [1, 2],
        [1, 3],
      ],
    });
    const expected = pl.DataFrame({
      nrs: [1, 2, 1, 3],
    });
    const actual = df.select(col("nrs").flatten());
    expect(actual).toFrameEqual(expected);
  });
  test("floor", () => {
    const df = pl.DataFrame({ a: [0.2, 1, 2.9] });
    const expected = pl.DataFrame({ a: [0, 1, 2] });
    const actual = df.select(col("a").floor());
    expect(actual).toFrameEqual(expected);
  });
  test("forwardFill", () => {
    const df = pl.DataFrame({ a: [1, null, 2, null, 9, null] });
    const expected = pl.DataFrame({ forwardFill: [1, 1, 2, 2, 9, 9] });
    const actual = df.select(col("a").forwardFill().as("forwardFill"));
    expect(actual).toFrameEqual(expected);
  });
  test("gt", () => {
    const df = pl.DataFrame({ a: [0, 1, 2, -1] });
    const expected = pl.DataFrame({ a: [false, true, true, false] });
    const actual = df.select(col("a").gt(0));
    expect(actual).toFrameEqual(expected);
  });
  test("gtEq", () => {
    const df = pl.DataFrame({ a: [0, 1, 2, -1] });
    const expected = pl.DataFrame({ a: [true, true, true, false] });
    const actual = df.select(col("a").gtEq(0));
    expect(actual).toFrameEqual(expected);
  });
  test("gatherEvery", () => {
    const df = pl.DataFrame({ a: [1, 1, 2, 2, 3, 3, 8, null, 1] });
    let expected = pl.DataFrame({ everyother: [1, 2, 3, 8, 1] });
    let actual = df.select(col("a").gatherEvery(2).as("everyother"));
    expect(actual).toFrameEqual(expected);
    expected = pl.DataFrame({ everyother: [2, 3, 8, 1] });
    actual = df.select(col("a").gatherEvery(2, 2).as("everyother"));
    expect(actual).toFrameEqual(expected);
  });
  test.each`
    args                   | hashValue
    ${[0]}                 | ${3464615199868688860n}
    ${[{ k0: 1n, k1: 1 }]} | ${9891435580050628982n}
  `("$# hash", ({ args, hashValue }) => {
    const df = pl.DataFrame({ a: [1] });
    const expected = pl.DataFrame({ hash: [hashValue] });
    const actual = df.select((col("a").hash as any)(...args).as("hash"));
    expect(actual).toFrameEqual(expected);
  });
  test("head", () => {
    const df = pl.DataFrame({ a: [0, 1, 2] });
    const expected = pl.DataFrame({ a: [0, 1] });
    const actual0 = df.select(col("a").head(2));
    const actual1 = df.select(col("a").head({ length: 2 }));
    expect(actual0).toFrameEqual(expected);
    expect(actual1).toFrameEqual(expected);
  });
  test("interpolate", () => {
    const df = pl.DataFrame({ a: [0, null, 2] });
    const expected = pl.DataFrame({ a: [0, 1, 2] });
    const actual = df.select(col("a").interpolate());
    expect(actual).toFrameEqual(expected);
  });
  test("isDuplicated", () => {
    const df = pl.DataFrame({ a: [0, 1, 2, 2] });
    const expected = pl.DataFrame({ a: [false, false, true, true] });
    const actual = df.select(col("a").isDuplicated());
    expect(actual).toFrameEqual(expected);
  });
  test("isFinite", () => {
    const df = pl.DataFrame({ a: [0, Number.POSITIVE_INFINITY, 1] });
    const expected = pl.DataFrame({ a: [true, false, true] });
    const actual = df.select(col("a").isFinite());
    expect(actual).toFrameEqual(expected);
  });
  test("isFirstDistinct", () => {
    const df = pl.DataFrame({ a: [0, 1, 2, 2] });
    const expected = pl.DataFrame({ a: [true, true, true, false] });
    const actual = df.select(col("a").isFirstDistinct());
    expect(actual).toFrameEqual(expected);
  });
  test("isIn:list", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ a: [true, true, false] });
    const actual = df.select(col("a").isIn([1, 2]));
    expect(actual).toFrameEqual(expected);
  });
  test("isIn:expr-eval", () => {
    const df = pl.DataFrame({
      sets: [
        [1, 2, 3],
        [1, 2],
        [9, 10],
      ],
      optional_members: [1, 2, 3],
    });
    const expected = pl.DataFrame({ isIn: [true, true, false] });
    const actual = df.select(col("optional_members").isIn("sets").as("isIn"));
    expect(actual).toFrameEqual(expected);
  });
  test("isIn:expr", () => {
    const df = pl.DataFrame({
      sets: [
        [1, 2, 3],
        [1, 2],
        [9, 10],
      ],
      optional_members: [1, 2, 3],
    });
    const expected = pl.DataFrame({ isIn: [true, true, false] });
    const actual = df.select(
      col("optional_members").isIn(col("sets")).as("isIn"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("isInfinite", () => {
    const df = pl.DataFrame({ a: [0, Number.POSITIVE_INFINITY, 1] });
    const expected = pl.DataFrame({ a: [false, true, false] });
    const actual = df.select(col("a").isInfinite());
    expect(actual).toFrameEqual(expected);
  });
  test("isNan", () => {
    const df = pl.DataFrame({ a: [1, Number.NaN, 2] });
    const expected = pl.DataFrame({ isNan: [false, true, false] });
    const actual = df.select(col("a").isNan().as("isNan"));
    expect(actual).toFrameEqual(expected);
  });
  test("isNotNan", () => {
    const df = pl.DataFrame({ a: [1, Number.NaN, 2] });
    const expected = pl.DataFrame({ isNotNan: [true, false, true] });
    const actual = df.select(col("a").isNotNan().as("isNotNan"));
    expect(actual).toFrameEqual(expected);
  });
  test("isNull", () => {
    const df = pl.DataFrame({ a: [1, null, 2] });
    const expected = pl.DataFrame({ isNull: [false, true, false] });
    const actual = df.select(col("a").isNull().as("isNull"));
    expect(actual).toFrameEqual(expected);
  });
  test("isNotNull", () => {
    const df = pl.DataFrame({ a: [1, null, 2] });
    const expected = pl.DataFrame({ isNotNull: [true, false, true] });
    const actual = df.select(col("a").isNotNull().as("isNotNull"));
    expect(actual).toFrameEqual(expected);
  });
  test("isUnique", () => {
    const df = pl.DataFrame({ a: [1, 1, 2] });
    const expected = pl.DataFrame({ isUnique: [false, false, true] });
    const actual = df.select(col("a").isUnique().as("isUnique"));
    expect(actual).toFrameEqual(expected);
  });
  test("keepName", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: ["a", "b", "c"],
    });
    const expected = pl.DataFrame({
      a: [1, 2, 3],
      b: [["a"], ["b"], ["c"]],
    });
    const actual = df
      .groupBy("a")
      .agg(col("b").list().keepName())
      .sort({ by: "a" });
    expect(actual).toFrameEqual(expected);
  });
  test("kurtosis", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 3, 4] });
    const expected = pl.DataFrame({
      kurtosis: [-1.044],
    });
    const actual = df.select(
      col("a").kurtosis().round({ decimals: 3 }).as("kurtosis"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("last", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ last: [3] });
    const actual = df.select(col("a").last().as("last"));
    expect(actual).toFrameEqual(expected);
  });
  test("len", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3, 3, 3],
      b: ["a", "a", "b", "a", "a"],
    });
    const actual = df.select(pl.len());
    const expected = pl.DataFrame({ len: [5] });
    expect(actual).toFrameEqual(expected);

    const actual2 = df.withColumn(pl.len());
    const expected2 = df.withColumn(pl.lit(5).alias("len"));
    expect(actual2).toFrameEqual(expected2);

    const actual3 = df.withColumn(pl.intRange(pl.len()).alias("index"));
    const expected3 = df.withColumn(
      pl.Series("index", [0, 1, 2, 3, 4], pl.Int64),
    );
    expect(actual3).toFrameEqual(expected3);

    const actual4 = df.groupBy("b").agg(pl.len());
    expect(actual4.shape).toEqual({ height: 2, width: 2 });
  });
  test("list", () => {
    const df = pl.DataFrame({
      a: ["a", "b", "c"],
    });
    const expected = pl.DataFrame({
      list: ["a", "b", "c"],
    });
    const actual = df.select(col("a").list().alias("list"));
    expect(actual).toFrameEqual(expected);
  });
  test("lowerBound", () => {
    const df = pl.DataFrame([
      pl.Series("int16", [1, 2, 3], pl.Int16),
      pl.Series("int32", [1, 2, 3], pl.Int32),
      pl.Series("uint16", [1, 2, 3], pl.UInt16),
      pl.Series("uint32", [1, 2, 3], pl.UInt32),
      pl.Series("uint64", [1n, 2n, 3n], pl.UInt64),
    ]);

    const expected = pl.DataFrame([
      pl.Series("int16", [-32768], pl.Int16),
      pl.Series("int32", [-2147483648], pl.Int32),
      pl.Series("uint16", [0], pl.UInt16),
      pl.Series("uint32", [0], pl.UInt32),
      pl.Series("uint64", [0n], pl.UInt64),
    ]);

    const actual = df.select(col("*").lowerBound().keepName());
    expect(actual).toFrameStrictEqual(expected);
  });
  test("lt", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ lt: [true, false, false] });
    const actual = df.select(col("a").lt(2).as("lt"));
    expect(actual).toFrameEqual(expected);
  });
  test("ltEq", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ lt: [true, true, false] });
    const actual = df.select(col("a").ltEq(2).as("lt"));
    expect(actual).toFrameEqual(expected);
  });
  test("log", () => {
    let df = pl.DataFrame({ a: [1, 2, 3] });
    let actual = df.select(col("a").log(2).round(6).as("log"));
    let expected = pl.DataFrame({ log: [0.0, 1.0, 1.584963] });
    expect(actual).toFrameEqual(expected);
    df = pl.DataFrame({ a: [2] });
    actual = df.select(col("a").log().as("log"));
    expected = pl.DataFrame({ log: [Math.LN2] });
    expect(actual).toFrameEqual(expected);
  });
  test("max", () => {
    const df = pl.DataFrame({ a: [1, 5, 3] });
    const expected = pl.DataFrame({ max: [5] });
    const actual = df.select(col("a").max().as("max"));
    expect(actual).toFrameEqual(expected);
  });
  test("mean", () => {
    const df = pl.DataFrame({ a: [2, 2, 8] });
    const expected = pl.DataFrame({ mean: [4] });
    const actual = df.select(col("a").mean().as("mean"));
    expect(actual).toFrameEqual(expected);
  });
  test("median", () => {
    const df = pl.DataFrame({ a: [6, 1, 2] });
    const expected = pl.DataFrame({ median: [2] });
    const actual = df.select(col("a").median().as("median"));
    expect(actual).toFrameEqual(expected);
  });
  test("min", () => {
    const df = pl.DataFrame({ a: [2, 3, 1, 2, 1] });
    const expected = pl.DataFrame({ min: [1] });
    const actual = df.select(col("a").min().as("min"));
    expect(actual).toFrameEqual(expected);
  });
  test("mode", () => {
    const df = pl.DataFrame({ a: [2, 2, 1, 3, 4, 1, 2] });
    const expected = pl.DataFrame({ mode: [2] });
    const actual = df.select(
      col("a").cast(pl.Int64).mode().cast(pl.Float64).as("mode"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("neq", () => {
    const df = pl.DataFrame({ a: [0, 1, 2] });
    const expected = pl.DataFrame({ neq: [true, false, true] });
    const actual = df.select(col("a").neq(1).as("neq"));
    expect(actual).toFrameEqual(expected);
  });
  test("not", () => {
    const df = pl.DataFrame({ a: [true, true, false] });
    const expected = pl.DataFrame({ not: [false, false, true] });
    const actual = df.select(col("a").not().as("not"));
    expect(actual).toFrameEqual(expected);
  });
  test("nUnique", () => {
    const df = pl.DataFrame({ a: [0, 1, 2] });
    const expected = pl.DataFrame({ nUnique: [3] });
    const actual = df.select(col("a").nUnique().as("nUnique"));
    expect(actual).toFrameEqual(expected);
  });
  test("or", () => {
    const df = pl.DataFrame({
      a: [0, null, 1],
      b: [0, 1, 2],
    });
    const expected = pl.DataFrame({
      a: [null, 1],
      b: [1, 2],
    });
    const actual = df.where(col("a").isNull().or(col("b").eq(2)));
    expect(actual).toFrameEqual(expected);
  });
  test("over", () => {
    const df = pl.DataFrame({
      groups: [1, 1, 2, 2],
      values: [1, 2, 3, 4],
    });
    const expected = pl.DataFrame({
      groups: [1, 1, 2, 2],
      sum_groups: [2, 2, 4, 4],
    });
    const actual = df.select(
      col("groups"),
      col("groups").sum().over("groups").alias("sum_groups"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("pow", () => {
    const df = pl.DataFrame({ a: [2, 5, 10] });
    const expected = pl.DataFrame({ pow: [4, 25, 100] });
    const actual = df.select(col("a").pow(2).as("pow"));
    expect(actual).toFrameEqual(expected);
  });
  test("pow:named", () => {
    const df = pl.DataFrame({ a: [2, 5, 10] });
    const expected = pl.DataFrame({ pow: [4, 25, 100] });
    const actual = df.select(col("a").pow({ exponent: 2 }).as("pow"));
    expect(actual).toFrameEqual(expected);
  });
  test("prefix", () => {
    const df = pl.DataFrame({ a: [2, 5, 10] });
    const expected = pl.DataFrame({ prefixed_a: [2, 5, 10] });
    const actual = df.select(col("a").prefix("prefixed_"));
    expect(actual).toFrameEqual(expected);
  });
  test("quantile", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ quantile: [2] });
    const actual = df.select(col("a").quantile(0.5).as("quantile"));
    expect(actual).toFrameEqual(expected);
  });
  test("quantile with expr", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ quantile: [2] });
    const actual = df.select(col("a").quantile(lit(0.5)).as("quantile"));
    expect(actual).toFrameEqual(expected);
  });

  test.each`
    rankMethod               | ranking
    ${"average"}             | ${[2, 4, 6.5, 4, 4, 6.5, 1]}
    ${"min"}                 | ${[2, 3, 6, 3, 3, 6, 1]}
    ${"max"}                 | ${[2, 5, 7, 5, 5, 7, 1]}
    ${"dense"}               | ${[2, 3, 4, 3, 3, 4, 1]}
    ${"ordinal"}             | ${[2, 3, 6, 4, 5, 7, 1]}
    ${{ method: "average" }} | ${[2, 4, 6.5, 4, 4, 6.5, 1]}
    ${{ method: "min" }}     | ${[2, 3, 6, 3, 3, 6, 1]}
    ${{ method: "max" }}     | ${[2, 5, 7, 5, 5, 7, 1]}
    ${{ method: "dense" }}   | ${[2, 3, 4, 3, 3, 4, 1]}
    ${{ method: "ordinal" }} | ${[2, 3, 6, 4, 5, 7, 1]}
  `("rank: $rankMethod", ({ rankMethod, ranking }) => {
    const df = pl.DataFrame({ a: [1, 2, 3, 2, 2, 3, 0] });
    const expected = pl.DataFrame({ rank: ranking });
    const actual = df.select(col("a").rank(rankMethod).alias("rank"));
    expect(actual).toFrameEqual(expected);
  });

  test("reinterpret", () => {
    const df = pl.DataFrame([pl.Series("a", [1n, 2n, 3n], pl.UInt64)]);
    const expected = pl.Series("a", [1, 2, 3], pl.Int64);
    const actual = df.select(col("a").reinterpret()).getColumn("a");
    expect(actual).toSeriesStrictEqual(expected);
  });
  test("repeatBy", () => {
    const df = pl.DataFrame({
      a: ["a", "b", "c"],
      n: [1, 2, 1],
    });
    const expected = pl.DataFrame({
      repeated: [["a"], ["b", "b"], ["c"]],
    });
    const actual = df.select(col("a").repeatBy("n").as("repeated"));
    expect(actual).toFrameEqual(expected);
  });
  test("reverse", () => {
    const df = pl.DataFrame({ a: ["a", "b", "c"] });
    const expected = pl.DataFrame({
      a: ["a", "b", "c"],
      reversed_a: ["c", "b", "a"],
    });
    const actual = df.withColumn(col("a").reverse().prefix("reversed_"));
    expect(actual).toFrameEqual(expected);
  });

  test("round", () => {
    const df = pl.DataFrame({ a: [1.00123, 2.32878, 3.3349999] });
    const expected = pl.DataFrame({ rounded: [1, 2.33, 3.33] });
    const actual = df.select(col("a").round({ decimals: 2 }).as("rounded"));
    expect(actual).toFrameStrictEqual(expected);
  });
  test("shift", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 4] });
    const expected = pl.DataFrame({
      a: [1, 2, 3, 4],
      one: [null, 1, 2, 3],
      negative_one: [2, 3, 4, null],
      two: [null, null, 1, 2],
    });
    const shifts = pl
      .DataFrame({
        name: ["one", "negative_one", "two"],
        values: [1, -1, 2],
      })
      .map(([name, value]) => col("a").shift(value).as(name));
    const actual = df.select(col("a"), ...shifts);
    expect(actual).toFrameStrictEqual(expected);
  });
  test("shiftAndFill", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 4] });
    const expected = pl.DataFrame({
      a: [1, 2, 3, 4],
      one: [0, 1, 2, 3],
      negative_one: [2, 3, 4, 99],
      two: [2, 2, 1, 2],
    });
    const shifts = [
      ["one", 1, 0],
      ["negative_one", -1, 99],
      ["two", 2, 2],
    ].map(([name, periods, fillValue]: any[]) => {
      return col("a").shiftAndFill({ periods, fillValue }).as(name);
    });
    const actual = df.select(col("a"), ...shifts);
    expect(actual).toFrameStrictEqual(expected);
  });
  test("sin", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ sin: [0.841471, 0.909297, 0.14112] });
    const actual = df.select(col("a").sin().round(6).as("sin"));
    expect(actual).toFrameEqual(expected);
  });
  test("skew", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 3] });
    const expected = pl.DataFrame({
      "skew:bias=true": ["-0.49338220021815865"],
      "skew:bias=false": ["-0.8545630383279712"],
    });
    const actual = df.select(
      col("a")
        .cast(pl.UInt64)
        .skew()
        .cast(pl.Utf8) // casted to string to retain precision when extracting to JS
        .as("skew:bias=true"),
      col("a")
        .cast(pl.UInt64)
        .skew({ bias: false })
        .cast(pl.Utf8) // casted to string to retain precision when extracting to JS
        .as("skew:bias=false"),
    );
    expect(actual).toFrameStrictEqual(expected);
  });
  test("slice", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 4] });
    const expected = pl.DataFrame({
      "slice(0,2)": [1, 2],
      "slice(-2,2)": [3, 4],
      "slice(1,2)": [2, 3],
    });
    const actual = df.select(
      col("a").slice(0, 2).as("slice(0,2)"),
      col("a").slice(-2, 2).as("slice(-2,2)"),
      col("a").slice({ offset: 1, length: 2 }).as("slice(1,2)"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("sort", () => {
    const df = pl.DataFrame({
      a: [1, null, 2, 0],
      b: [null, "a", "b", "a"],
    });
    const expected = pl.DataFrame({
      a_sorted_default: [null, 0, 1, 2],
      a_sorted_reverse: [null, 2, 1, 0],
      a_sorted_nulls_last: [0, 1, 2, null],
      a_sorted_reverse_nulls_last: [2, 1, 0, null],
      b_sorted_default: [null, "a", "a", "b"],
      b_sorted_reverse: [null, "b", "a", "a"],
      b_sorted_nulls_last: ["a", "a", "b", null],
      b_sorted_reverse_nulls_last: ["b", "a", "a", null],
    });
    const a = col("a");
    const b = col("b");
    const actual = df.select(
      a.sort().as("a_sorted_default"),
      a.sort({ descending: true }).as("a_sorted_reverse"),
      a.sort({ nullsLast: true }).as("a_sorted_nulls_last"),
      a
        .sort({ descending: true, nullsLast: true })
        .as("a_sorted_reverse_nulls_last"),
      b.sort().as("b_sorted_default"),
      b.sort({ descending: true }).as("b_sorted_reverse"),
      b.sort({ nullsLast: true }).as("b_sorted_nulls_last"),
      b
        .sort({ descending: true, nullsLast: true })
        .as("b_sorted_reverse_nulls_last"),
    );
    expect(actual).toFrameEqual(expected);
  });

  test("sortBy", () => {
    const df = pl.DataFrame({
      label: ["a", "a", "b", "b"],
      name: ["foo", "bar", "baz", "boo"],
      value: [1, 2, 1.5, -1],
    });

    const expected = pl.DataFrame({
      label: ["a", "a", "b", "b"],
      name: ["foo", "bar", "baz", "boo"],
      value: [1, 2, 1.5, -1],
      name_max: ["bar", "bar", "baz", "baz"],
      value_max: [2, 2, 1.5, 1.5],
    });

    const actual = df.withColumns(
      pl
        .col(["name", "value"])
        .sortBy("value")
        .last()
        .over("label")
        .suffix("_max"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("sortBy:named", () => {
    const df = pl.DataFrame({
      label: ["a", "a", "b", "b"],
      name: ["foo", "bar", "baz", "boo"],
      value: [1, 2, 1.5, -1],
    });

    const expected = pl.DataFrame({
      label: ["a", "a", "b", "b"],
      name: ["foo", "bar", "baz", "boo"],
      value: [1, 2, 1.5, -1],
      name_min: ["foo", "foo", "boo", "boo"],
      value_min: [1, 1, -1, -1],
    });

    const actual = df.withColumns(
      pl
        .col(["name", "value"])
        .sortBy({ by: [pl.col("value")], descending: [true] })
        .last()
        .over("label")
        .suffix("_min"),
    );
    expect(actual).toFrameEqual(expected);
  });

  test("std", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 10, 200] });
    const expected = pl.DataFrame({ std: ["87.73"] });
    const actual = df.select(
      col("a").std().round({ decimals: 2 }).cast(pl.Utf8).as("std"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("suffix", () => {
    const df = pl.DataFrame({ a: [2, 5, 10] });
    const expected = pl.DataFrame({ a_suffixed: [2, 5, 10] });
    const actual = df.select(col("a").suffix("_suffixed"));
    expect(actual).toFrameEqual(expected);
  });
  test("sum", () => {
    const df = pl.DataFrame({ a: [2, 5, 10] });
    const expected = pl.DataFrame({ sum: [17] });
    const actual = df.select(col("a").sum().as("sum"));
    expect(actual).toFrameEqual(expected);
  });
  test("tail", () => {
    const df = pl.DataFrame({ a: [1, 2, 2, 3, 3, 8, null, 1] });
    const expected = pl.DataFrame({
      tail3: [8, null, 1],
    });
    const actual = df.select(col("a").tail(3).as("tail3"));
    expect(actual).toFrameEqual(expected);
  });
  test("take", () => {
    const df = pl.DataFrame({ a: [1, 2, 2, 3, 3, 8, null, 1] });
    const expected = pl.DataFrame({
      "take:array": [1, 2, 3, 8],
      "take:list": [1, 2, 2, 3],
    });
    const actual = df.select(
      col("a").gather([0, 2, 3, 5]).as("take:array"),
      col("a")
        .gather(lit([0, 1, 2, 3]))
        .as("take:list"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("tan", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.DataFrame({ tan: [1.557408, -2.18504, -0.142547] });
    const actual = df.select(col("a").tan().round(6).as("tan"));
    expect(actual).toFrameEqual(expected);
  });
  test("unique", () => {
    const df = pl.DataFrame({ a: [2, 3, 1, 2, 1, 3, 8, null, 1] });
    let expected = pl.DataFrame({ uniques: [1, 2, 3, 8, null] });
    let actual = df.select(
      col("a").unique().sort({ nullsLast: true }).as("uniques"),
    );
    expect(actual).toFrameEqual(expected);
    expected = pl.DataFrame({ uniques: [2, 3, 1, 8, null] });
    actual = df.select(col("a").unique(true).as("uniques"));
    expect(actual).toFrameEqual(expected);
    actual = df.select(col("a").unique({ maintainOrder: true }).as("uniques"));
    expect(actual).toFrameEqual(expected);
  });
  test("upperBound", () => {
    const df = pl.DataFrame([
      pl.Series("int16", [1, 2, 3], pl.Int16),
      pl.Series("int32", [1, 2, 3], pl.Int32),
      pl.Series("uint16", [1, 2, 3], pl.UInt16),
      pl.Series("uint32", [1, 2, 3], pl.UInt32),
      pl.Series("uint64", [1n, 2n, 3n], pl.UInt64),
    ]);
    const expected = pl.DataFrame([
      pl.Series("int16", [32767], pl.Int16),
      pl.Series("int32", [2147483647], pl.Int32),
      pl.Series("uint16", [65535], pl.UInt16),
      pl.Series("uint32", [4294967295], pl.UInt32),
      pl.Series("uint64", [18446744073709551615n], pl.UInt64),
    ]);
    const actual = df.select(col("*").upperBound().keepName());
    expect(actual).toFrameStrictEqual(expected);
  });
  test("var", () => {
    const df = pl.DataFrame({ a: [1, 1, 2, 3, 8, null] });
    const expected = pl.DataFrame({
      var: [8.5],
    });
    const actual = df.select(col("a").var().as("var"));
    expect(actual).toFrameEqual(expected);
  });
  test("where", () => {
    const df = pl.DataFrame({ a: [-1, 2, -3, 4] });
    const expected = pl.DataFrame({ a: [2, 4] });
    const actual = df.select(col("a").where(col("a").gt(0)));
    expect(actual).toFrameEqual(expected);
  });
});
describe("expr.str", () => {
  test("concat", () => {
    const df = pl.DataFrame({ os: ["kali", "debian", "ubuntu"] });
    const expected = "kali,debian,ubuntu";
    const actualFromSeries = df.getColumn("os").str.concat(",")[0];

    const actual = df.select(col("os").str.concat(",")).row(0)[0];
    expect(actual).toStrictEqual(expected);
    expect(actualFromSeries).toStrictEqual(expected);
  });
  test("contains", () => {
    const df = pl.DataFrame({
      os: ["linux-kali", "linux-debian", "windows-vista"],
    });
    const expected = pl.DataFrame({
      os: ["linux-kali", "linux-debian", "windows-vista"],
      isLinux: [true, true, false],
    });
    const seriesActual = df
      .getColumn("os")
      .str.contains("linux")
      .rename("isLinux");
    const actual = df.withColumn(col("os").str.contains("linux").as("isLinux"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("isLinux"));
  });
  test("contains:expr", () => {
    const df = pl.DataFrame({
      os: ["linux-kali", "linux-debian", "windows-vista"],
      name: ["kali", "debian", "macos"],
    });
    const expected = df.withColumn(
      pl.Series("isLinux", [true, true, false], pl.Bool),
    );
    const actual = df.withColumn(
      col("os").str.contains(pl.col("name")).as("isLinux"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("contains:regex", () => {
    const df = pl.DataFrame({
      a: ["Foo", "foo", "FoO"],
    });

    const re = /foo/i;
    const expected = pl.DataFrame({
      a: ["Foo", "foo", "FoO"],
      contains: [true, true, true],
    });
    const seriesActual = df.getColumn("a").str.contains(re).rename("contains");
    const actual = df.withColumn(col("a").str.contains(re).as("contains"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("contains"));
  });
  test("contains:regex2", () => {
    const df = pl.DataFrame({ txt: ["Crab", "cat and dog", "rab$bit", null] });
    const actual = df.select(
      pl.col("txt"),
      pl.col("txt").str.contains("cat|bit").alias("regex"),
      pl.col("txt").str.contains("rab$", true).alias("literal"),
    );
    const expected = df.withColumns(
      pl.Series("regex", [false, true, true, null], pl.Bool),
      pl.Series("literal", [false, false, true, null], pl.Bool),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("endsWith", () => {
    const df = pl.DataFrame({
      fruits: ["apple", "mango", null],
    });
    const expected = df.withColumn(
      pl.Series("has_suffix", [false, true, null], pl.Bool),
    );
    const actual = df.withColumn(
      col("fruits").str.endsWith("go").as("has_suffix"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("endsWith:expr", () => {
    const df = pl.DataFrame({
      fruits: ["apple", "mango", "banana"],
      suffix: ["le", "go", "nu"],
    });
    const expected = df.withColumn(
      pl.Series("has_suffix", [true, true, false], pl.Bool),
    );
    const actual = df.withColumn(
      col("fruits").str.endsWith(pl.col("suffix")).as("has_suffix"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("startsWith", () => {
    const df = pl.DataFrame({
      fruits: ["apple", "mango", null],
    });
    const expected = df.withColumn(
      pl.Series("has_prefix", [true, false, null], pl.Bool),
    );
    const actual = df.withColumn(
      col("fruits").str.startsWith("app").as("has_prefix"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("startsWith:expr", () => {
    const df = pl.DataFrame({
      fruits: ["apple", "mango", "banana"],
      prefix: ["app", "na", "ba"],
    });
    const expected = df.withColumn(
      pl.Series("has_prefix", [true, false, true], pl.Bool),
    );
    const actual = df.withColumn(
      col("fruits").str.startsWith(pl.col("prefix")).as("has_prefix"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("split", () => {
    const df = pl.DataFrame({ a: ["ab,cd", "e,fg", "h"] });
    const expected = pl.DataFrame({
      split: [["ab", "cd"], ["e", "fg"], ["h"]],
    });
    const actual = df.select(col("a").str.split(",").as("split"));
    const actualFromSeries = df
      .getColumn("a")
      .str.split(",")
      .rename("split")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("extract", () => {
    const df = pl.DataFrame({
      a: [
        "http://vote.com/ballon_dor?candidate=messi&ref=polars",
        "http://vote.com/ballon_dor?candidat=jorginho&ref=polars",
        "http://vote.com/ballon_dor?candidate=ronaldo&ref=polars",
      ],
    });
    const expected = pl.DataFrame({
      candidate: ["messi", null, "ronaldo"],
    });

    const seriesActual = df
      .getColumn("a")
      .str.extract(/candidate=(\w+)/, 1)
      .rename("candidate")
      .toFrame();

    const actual = df.select(
      col("a")
        .str.extract(/candidate=(\w+)/, 1)
        .as("candidate"),
    );
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("jsonDecode", () => {
    const df = pl.DataFrame({
      json: [
        '{"a":1, "b": true}',
        '{"a": null, "b": null }',
        '{"a":2, "b": false}',
      ],
    });
    const actual = df.select(pl.col("json").str.jsonDecode());
    const expected = pl.DataFrame({
      json: [{ a: 1, b: true }, "null", { a: 2, b: false }],
    });
    expect(actual).toFrameEqual(expected);
    {
      const s = pl.Series(["[1, 2, 3]", null, "[4, 5, 6]"]);
      const dtype = pl.List(pl.Int64);
      const expSeries = pl.Series([[1, 2, 3], null, [4, 5, 6]]);
      expect(s.str.jsonDecode()).toSeriesEqual(expSeries);
      expect(s.str.jsonDecode(dtype)).toSeriesEqual(expSeries);
    }
    {
      const dtype = pl.Struct([
        new pl.Field("a", pl.Int64),
        new pl.Field("b", pl.Bool),
      ]);
      const s = pl.Series("json", [
        '{"a":1, "b": true}',
        '{"a": null, "b": null }',
        '{"a":2, "b": false}',
      ]);
      expect(s.str.jsonDecode().as("json")).toSeriesEqual(
        expected.getColumn("json"),
      );
      expect(s.str.jsonDecode(dtype).as("json")).toSeriesEqual(
        expected.getColumn("json"),
      );
    }
    {
      const s = pl.Series("col_a", [], pl.Utf8);
      const exp = pl.Series("col_a", []).cast(pl.List(pl.Int64));
      const dtype = pl.List(pl.Int64);
      expect(s.str.jsonDecode(dtype).as("col_a")).toSeriesEqual(exp);
    }
  });
  test("jsonPathMatch", () => {
    const df = pl.DataFrame({
      data: [
        `{"os": "linux",   "arch": "debian"}`,
        `{"os": "mac",     "arch": "sierra"}`,
        `{"os": "windows", "arch": "11"}`,
      ],
    });
    const expected = pl.DataFrame({
      os: ["linux", "mac", "windows"],
    });
    const seriesActual = df
      .getColumn("data")
      .str.jsonPathMatch("$.os")
      .rename("os")
      .toFrame();
    const actual = df.select(col("data").str.jsonPathMatch("$.os").as("os"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("lengths", () => {
    const df = pl.DataFrame({ os: ["kali", "debian", "ubuntu"] });
    const expected = pl.DataFrame({
      lengths: [4, 6, 6],
    });
    const seriesActual = df
      .getColumn("os")
      .str.lengths()
      .rename("lengths")
      .toFrame();
    const actual = df.select(col("os").str.lengths().as("lengths"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("str.replace", () => {
    const df = pl.DataFrame({
      os: ["kali-linux", "debian-linux", null, "mac-sierra"],
    });
    const expected = pl.DataFrame({
      os: ["kali:linux", "debian:linux", null, "mac:sierra"],
    });
    const seriesActual = df
      .getColumn("os")
      .str.replace("-", ":")
      .rename("os")
      .toFrame();
    const actual = df.select(col("os").str.replace("-", ":").as("os"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("str.replace:Expr1", () => {
    const df = pl.DataFrame({
      os: ["kali-linux", "debian-linux", null, "mac-sierra"],
      val: ["windows", "acorn", "atari", null],
    });
    const expected = pl.DataFrame({
      os: ["kali-windows", "debian-acorn", null, null],
    });
    const actual = df.select(
      col("os").str.replace("linux", col("val")).as("os"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("str.replace:Expr2", () => {
    const df = pl.DataFrame({
      cost: ["#12.34", "#56.78"],
      text: ["123abc", "abc456"],
    });
    const expected = pl.DataFrame({
      expr: ["123#12.34", "#56.78456"],
    });
    const actual = df.select(
      col("text").str.replace("abc", pl.col("cost")).alias("expr"),
    );
    expect(actual).toFrameEqual(expected);
  });
  // TODO: Remove skip when polars-plan will support for "dynamic pattern length in 'str.replace' expressions"
  test.skip("str.replace:Expr3", () => {
    const df = pl.DataFrame({
      os: ["kali-linux", "debian-linux", "ubuntu-linux", "mac-sierra"],
      pat: ["linux", "linux", "linux", "mac"],
      val: ["windows", "acorn", "atari", "arm"],
    });
    const expected = pl.DataFrame({
      os: ["kali-windows", "debian-acorn", "ubuntu-atari", "arm-sierra"],
    });
    const actual = df.select(
      col("os").str.replace(col("pat"), col("val")).as("os"),
    );
    expect(actual).toFrameEqual(expected);
  });

  test("str.replaceAll", () => {
    const df = pl.DataFrame({
      os: [
        "kali-linux-2021.3a",
        "debian-linux-stable",
        "ubuntu-linux-16.04",
        "mac-sierra-10.12.1",
      ],
    });
    const expected = pl.DataFrame({
      os: [
        "kali:linux:2021.3a",
        "debian:linux:stable",
        "ubuntu:linux:16.04",
        "mac:sierra:10.12.1",
      ],
    });
    const seriesActual = df
      .getColumn("os")
      .str.replaceAll("-", ":")
      .rename("os")
      .toFrame();
    const actual = df.select(col("os").str.replaceAll("-", ":").as("os"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("str.replaceAll:Expr", () => {
    const df = pl.DataFrame({
      os: [
        "kali-linux-2021.3a",
        null,
        "ubuntu-linux-16.04",
        "mac-sierra-10.12.1",
      ],
      val: [":", ":", null, "_"],
    });
    const expected = pl.DataFrame({
      os: ["kali:linux:2021.3a", null, null, "mac_sierra_10.12.1"],
    });
    const actual = df.select(
      col("os").str.replaceAll("-", col("val")).as("os"),
    );
    expect(actual).toFrameEqual(expected);
  });
  // TODO: Remove skip when polars-plan will support for "dynamic pattern length in 'str.replace' expressions"
  test.skip("str.replaceAll:Expr2", () => {
    const df = pl.DataFrame({
      os: [
        "kali-linux-2021.3a",
        null,
        "ubuntu-linux-16.04",
        "mac-sierra-10.12.1",
      ],
      pat: ["-", "-", "-", "."],
      val: [":", ":", null, "_"],
    });
    const expected = pl.DataFrame({
      os: ["kali:linux:2021.3a", null, null, "mac-sierra-10_12_1"],
    });
    const actual = df.select(
      col("os").str.replaceAll(col("pat"), col("val")).as("os"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("struct:field", () => {
    const df = pl.DataFrame({
      objs: [
        { a: 1, b: 2.0, c: "abc" },
        { a: 10, b: 20.0, c: "def" },
      ],
    });
    const expected = pl.DataFrame({
      b: [2.0, 20.0],
      last: ["abc", "def"],
    });
    const actual = df.select(
      col("objs").struct.field("b"),
      col("objs").struct.field("c").as("last"),
    );
    expect(actual).toFrameStrictEqual(expected);
  });
  test("struct:nth", () => {
    const df = pl.DataFrame({
      objs: [
        { a: 1, b: 2.0, c: "abc" },
        { a: 10, b: 20.0, c: "def" },
      ],
    });
    const expected = pl.DataFrame({
      b: [2.0, 20.0],
      last: ["abc", "def"],
    });
    const actual = df.select(
      col("objs").struct.nth(1),
      col("objs").struct.nth(2).as("last"),
    );
    expect(actual).toFrameStrictEqual(expected);
  });
  test("struct:withFields", () => {
    const df = pl.DataFrame({
      objs: [
        { a: 1, b: 2.0, c: "abc" },
        { a: 10, b: 20.0, c: "def" },
      ],
      more: ["text1", "text2"],
      final: [100, null],
    });
    const expected = pl.DataFrame({
      objs: [
        { a: 1, b: 2.0, c: "abc", d: null, e: "text" },
        { a: 10, b: 20.0, c: "def", d: null, e: "text" },
      ],
      new: [
        { a: 1, b: 2.0, c: "abc", more: "text1", final: 100 },
        { a: 10, b: 20.0, c: "def", more: "text2", final: null },
      ],
    });
    const actual = df.select(
      col("objs").struct.withFields([
        pl.lit(null).alias("d"),
        pl.lit("text").alias("e"),
      ]),
      col("objs")
        .struct.withFields([col("more"), col("final")])
        .alias("new"),
    );
    expect(actual).toFrameStrictEqual(expected);
  });
  test("expr.replace", () => {
    const df = pl.DataFrame({ a: [1, 2, 2, 3], b: ["a", "b", "c", "d"] });
    {
      const actual = df.withColumns(
        pl.col("a").replace(2, 100).alias("replaced"),
      );
      const expected = pl.DataFrame({
        a: [1, 2, 2, 3],
        b: ["a", "b", "c", "d"],
        replaced: [1, 100, 100, 3],
      });
      expect(actual).toFrameEqual(expected);
    }
    {
      const actual = df.withColumns(
        pl
          .col("a")
          .replaceStrict([2, 3], [100, 200], -1, pl.Float64)
          .alias("replaced"),
      );
      const expected = pl.DataFrame({
        a: [1, 2, 2, 3],
        b: ["a", "b", "c", "d"],
        replaced: [-1, 100, 100, 200],
      });
      expect(actual).toFrameEqual(expected);
    }
    {
      const actual = df.withColumns(
        pl.col("b").replaceStrict("a", "c", "e", pl.Utf8).alias("replaced"),
      );
      const expected = pl.DataFrame({
        a: [1, 2, 2, 3],
        b: ["a", "b", "c", "d"],
        replaced: ["c", "e", "e", "e"],
      });
      expect(actual).toFrameEqual(expected);
    }
    {
      const actual = df.withColumns(
        pl
          .col("b")
          .replaceStrict(["a", "b"], ["c", "d"], "e", pl.Utf8)
          .alias("replaced"),
      );
      const expected = pl.DataFrame({
        a: [1, 2, 2, 3],
        b: ["a", "b", "c", "d"],
        replaced: ["c", "d", "e", "e"],
      });
      expect(actual).toFrameEqual(expected);
    }
    const mapping = { 2: 100, 3: 200 };
    {
      const actual = df.withColumns(
        pl
          .col("a")
          .replaceStrict({ old: mapping, default_: -1, returnDtype: pl.Int64 })
          .alias("replaced"),
      );
      const expected = pl.DataFrame({
        a: [1, 2, 2, 3],
        b: ["a", "b", "c", "d"],
        replaced: [-1, 100, 100, 200],
      });
      expect(actual).toFrameEqual(expected);
    }
    {
      const actual = df.withColumns(
        pl.col("a").replace({ old: mapping }).alias("replaced"),
      );
      const expected = pl.DataFrame({
        a: [1, 2, 2, 3],
        b: ["a", "b", "c", "d"],
        replaced: [1, 100, 100, 200],
      });
      expect(actual).toFrameEqual(expected);
    }
  });
  test("slice", () => {
    const df = pl.DataFrame({
      os: ["linux-kali", "linux-debian", "windows-vista"],
    });
    const expected = pl.DataFrame({
      first5: ["linux", "linux", "windo"],
    });
    const seriesActual = df
      .getColumn("os")
      .str.slice(0, 5)
      .rename("first5")
      .toFrame();
    const actual = df.select(col("os").str.slice(0, 5).as("first5"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("strptime", () => {
    const df = pl.DataFrame({
      timestamp: [
        "2020-01-01T01:22:00.002+00:00",
        "2020-02-01T01:02:01.030+00:00",
        "2021-11-01T01:02:20.001+00:00",
      ],
    });
    const expected = pl.DataFrame([
      pl.Series("datetime", [
        new Date(Date.parse("2020-01-01T01:22:00.002+00:00")),
        new Date(Date.parse("2020-02-01T01:02:01.030+00:00")),
        new Date(Date.parse("2021-11-01T01:02:20.001+00:00")),
      ]),
      pl.Series(
        "date",
        [
          new Date(Date.parse("2020-01-01T01:22:00.002+00:00")),
          new Date(Date.parse("2020-02-01T01:02:01.030+00:00")),
          new Date(Date.parse("2021-11-01T01:02:20.001+00:00")),
        ],
        pl.Date,
      ),
    ]);

    const datetimeSeries = df
      .getColumn("timestamp")
      .str.strptime(pl.Datetime, "%FT%T%.3f")
      .rename("datetime");
    const dateSeries = df
      .getColumn("timestamp")
      .str.strptime(pl.Date, "%FT%T%.3f%:z")
      .rename("date");

    const actualFromSeries = pl.DataFrame([datetimeSeries, dateSeries]);

    const actual = df.select(
      col("timestamp")
        .str.strptime(pl.Datetime("ms"), "%FT%T%.3f")
        .as("datetime"),
      col("timestamp").str.strptime(pl.Date, "%FT%T%.3f%:z").as("date"),
    );

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("toLowercase", () => {
    const df = pl.DataFrame({
      os: ["Kali-Linux", "Debian-Linux", "Ubuntu-Linux", "Mac-Sierra"],
    });
    const expected = pl.DataFrame({
      os: ["kali-linux", "debian-linux", "ubuntu-linux", "mac-sierra"],
    });
    const seriesActual = df
      .getColumn("os")
      .str.toLowerCase()
      .rename("os")
      .toFrame();
    const actual = df.select(col("os").str.toLowerCase().as("os"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("toUpperCase", () => {
    const df = pl.DataFrame({
      os: ["Kali-Linux", "Debian-Linux", "Ubuntu-Linux", "Mac-Sierra"],
    });
    const expected = pl.DataFrame({
      os: ["KALI-LINUX", "DEBIAN-LINUX", "UBUNTU-LINUX", "MAC-SIERRA"],
    });
    const seriesActual = df
      .getColumn("os")
      .str.toUpperCase()
      .rename("os")
      .toFrame();
    const actual = df.select(col("os").str.toUpperCase().as("os"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("rstrip", () => {
    const df = pl.DataFrame({
      os: [
        "Kali-Linux    ",
        "Debian-Linux    ",
        "Ubuntu-Linux    ",
        "Mac-Sierra",
      ],
    });
    const expected = pl.DataFrame({
      os: ["Kali-Linux", "Debian-Linux", "Ubuntu-Linux", "Mac-Sierra"],
    });
    const seriesActual = df.getColumn("os").str.rstrip().rename("os").toFrame();
    const actual = df.select(col("os").str.rstrip().as("os"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("lstrip", () => {
    const df = pl.DataFrame({
      os: [
        "    Kali-Linux",
        "       Debian-Linux",
        "  Ubuntu-Linux",
        "Mac-Sierra",
      ],
    });
    const expected = pl.DataFrame({
      os: ["Kali-Linux", "Debian-Linux", "Ubuntu-Linux", "Mac-Sierra"],
    });
    const seriesActual = df.getColumn("os").str.lstrip().rename("os").toFrame();
    const actual = df.select(col("os").str.lstrip().as("os"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });

  test("stripChars:Expr", () => {
    const df = pl.DataFrame({
      os: [
        "#Kali-Linux###",
        "$$$Debian-Linux$",
        null,
        "Ubuntu-Linux    ",
        "  Mac-Sierra",
      ],
      chars: ["#", "$", " ", " ", null],
    });
    const expected = pl.DataFrame({
      os: ["Kali-Linux", "Debian-Linux", null, "Ubuntu-Linux", "Mac-Sierra"],
    });
    const actual = df.select(col("os").str.stripChars(col("chars")).as("os"));
    expect(actual).toFrameEqual(expected);
  });
  test("stripCharsStart:Expr", () => {
    const df = pl.DataFrame({
      os: [
        "#Kali-Linux###",
        "$$$Debian-Linux$",
        null,
        " Ubuntu-Linux ",
        "Mac-Sierra",
      ],
      chars: ["#", "$", " ", null, "Mac-"],
    });
    const expected = pl.DataFrame({
      os: ["Kali-Linux###", "Debian-Linux$", null, "Ubuntu-Linux ", "Sierra"],
    });
    const actual = df.select(
      col("os").str.stripCharsStart(col("chars")).as("os"),
    );
    expect(actual).toFrameEqual(expected);
  });
  test("stripCharsEnd:Expr", () => {
    const df = pl.DataFrame({
      os: [
        "#Kali-Linux###",
        "$$$Debian-Linux$",
        null,
        "Ubuntu-Linux    ",
        "  Mac-Sierra",
      ],
      chars: ["#", "$", " ", null, "-Sierra"],
    });
    const expected = pl.DataFrame({
      os: ["#Kali-Linux", "$$$Debian-Linux", null, "Ubuntu-Linux", "  Mac"],
    });
    const actual = df.select(
      col("os").str.stripCharsEnd(col("chars")).as("os"),
    );
    expect(actual).toFrameEqual(expected);
  });

  test("padStart", () => {
    const df = pl.DataFrame({
      foo: ["a", "b", "cow", "longer"],
    });
    const expected = pl.DataFrame({
      foo: ["__a", "__b", "cow", "longer"],
    });
    const seriesActual = df
      .getColumn("foo")
      .str.padStart(3, "_")
      .rename("foo")
      .toFrame();
    const actual = df.select(col("foo").str.padStart(3, "_").as("foo"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("padEnd", () => {
    const df = pl.DataFrame({
      foo: ["a", "b", "cow", "longer"],
    });
    const expected = pl.DataFrame({
      foo: ["a__", "b__", "cow", "longer"],
    });
    const seriesActual = df
      .getColumn("foo")
      .str.padEnd(3, "_")
      .rename("foo")
      .toFrame();
    const actual = df.select(col("foo").str.padEnd(3, "_").as("foo"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("zFill", () => {
    const df = pl.DataFrame({
      foo: ["a", "b", "cow", "longer"],
    });
    const expected = pl.DataFrame({
      foo: ["00a", "00b", "cow", "longer"],
    });
    const seriesActual = df
      .getColumn("foo")
      .str.zFill(3)
      .rename("foo")
      .toFrame();
    const actual = df.select(col("foo").str.zFill(3).as("foo"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("hex encode", () => {
    const df = pl.DataFrame({
      original: ["foo", "bar", null],
    });
    const expected = pl.DataFrame({
      encoded: ["666f6f", "626172", null],
    });
    const seriesActual = df
      .getColumn("original")
      .str.encode("hex")
      .rename("encoded")
      .toFrame();
    const actual = df.select(col("original").str.encode("hex").as("encoded"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("hex decode", () => {
    const df = pl.DataFrame({
      encoded: ["666f6f", "626172", null],
    });
    const expected = pl.DataFrame({
      decoded: ["foo", "bar", null],
    });
    const seriesActual = df
      .getColumn("encoded")
      .str.decode("hex")
      .rename("decoded")
      .toFrame();
    const actual = df.select(col("encoded").str.decode("hex").as("decoded"));
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("hex decode strict", () => {
    const df = pl.DataFrame({
      encoded: ["666f6f", "626172", "not a hex", null],
    });

    const fn = () =>
      df.select(
        col("encoded")
          .str.decode({ encoding: "hex", strict: true })
          .as("decoded"),
      );
    expect(fn).toThrow();
  });

  test("encode base64", () => {
    const df = pl.DataFrame({
      original: ["foo", "bar", null],
    });
    const expected = pl.DataFrame({
      encoded: ["Zm9v", "YmFy", null],
    });
    const seriesActual = df
      .getColumn("original")
      .str.encode("base64")
      .rename("encoded")
      .toFrame();
    const actual = df.select(
      col("original").str.encode("base64").as("encoded"),
    );
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("base64 decode", () => {
    const df = pl.DataFrame({
      encoded: ["Zm9v", "YmFy", null],
    });
    const expected = pl.DataFrame({
      decoded: ["foo", "bar", null],
    });
    const seriesActual = df
      .getColumn("encoded")
      .str.decode("base64")
      .rename("decoded")
      .toFrame();
    const actual = df.select(
      col("encoded").str.decode("base64", false).as("decoded"),
    );
    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("base64 decode strict", () => {
    const df = pl.DataFrame({
      encoded: ["not a base64"],
    });

    const fn = () =>
      df.select(
        col("encoded")
          .str.decode({ encoding: "base64", strict: true })
          .as("decoded"),
      );
    expect(fn).toThrow();
  });
});
describe("expr.lst", () => {
  test("argMax", () => {
    const s0 = pl.Series("a", [[1, 2, 3]]);
    let actual = s0.lst.argMax();
    let expected = pl.Series("a", [2]);
    expect(actual.seriesEqual(expected));
    actual = s0.lst.argMin();
    expected = pl.Series("a", [0]);
    expect(actual.seriesEqual(expected));
  });
  test("contains", () => {
    const s0 = pl.Series("a", [[1, 2]]);
    const actual = s0.lst.contains(1);
    const expected = pl.Series("a", [true]);
    expect(actual.seriesEqual(expected));
  });
  test("concat", () => {
    const s0 = pl.Series("a", [[1, 2]]);
    const s1 = pl.Series("b", [[3, 4, 5]]);
    let expected = pl.Series("a", [[1, 2], [3, 4, 5]]);
    const out = s0.concat(s1);
    expect(out.seriesEqual(expected)).toBeTruthy();
    const df = pl.DataFrame([s0, s1]);
    expected = pl.Series("a", [[1, 2, 3, 4, 5]]);
    expect(
      df
        .select(pl.concatList(["a", "b"]).alias("a"))
        .getColumn("a")
        .seriesEqual(expected),
    ).toBeTruthy();
    expect(
      df
        .select(pl.col("a").lst.concat("b").alias("a"))
        .getColumn("a")
        .seriesEqual(expected),
    ).toBeTruthy();
    expect(
      df
        .select(pl.col("a").lst.concat(["b"]).alias("a"))
        .getColumn("a")
        .seriesEqual(expected),
    );
  });
  test("diff", () => {
    const s0 = pl.Series("a", [[1, 2, 3]]);
    const actual = s0.lst.diff();
    const expected = pl.Series("a", [null, 1, 1]);
    expect(actual.seriesEqual(expected));
  });
  test("get", () => {
    const df = pl.DataFrame({ a: [[1, 10, 11], [2, 10, 12], [1]] });
    const expected = pl.DataFrame({ get: [11, 12, null] });
    const actual = df.select(col("a").lst.get(2).as("get"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.get(2)
      .rename("get")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("get with expr", () => {
    const df = pl.DataFrame({ a: [[1, 10, 11], [2, 10, 12], [1]] });
    const expected = pl.DataFrame({ get: [11, 12, null] });
    const actual = df.select(col("a").lst.get(lit(2)).as("get"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.get(2)
      .rename("get")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("eval", () => {
    const s0 = pl.Series("a", [[3, 5, 6]]);
    const actual = s0.lst.eval(pl.element().rank());
    const expected = pl.Series("a", [1, 2, 3]);
    expect(actual.seriesEqual(expected));
  });
  test("first", () => {
    const df = pl.DataFrame({
      a: [
        [1, 10],
        [2, 10],
      ],
    });
    const expected = pl.DataFrame({ first: [1, 2] });
    const actual = df.select(col("a").lst.first().as("first"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.first()
      .rename("first")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("head", () => {
    const s0 = pl.Series("a", [[3, 5, 6, 7, 8]]);
    let actual = s0.lst.head(1);
    let expected = pl.Series("a", [[3]]);
    expect(actual.seriesEqual(expected));
    actual = s0.lst.head();
    expected = pl.Series("a", [3, 5, 6, 7, 8]);
    expect(actual.seriesEqual(expected));
  });
  test("tail", () => {
    const s0 = pl.Series("a", [[3, 5, 6, 7, 8]]);
    let actual = s0.lst.tail(1);
    let expected = pl.Series("a", [[8]]);
    expect(actual.seriesEqual(expected));
    actual = s0.lst.tail();
    expected = pl.Series("a", [3, 5, 6, 7, 8]);
    expect(actual.seriesEqual(expected));
  });
  test("shift", () => {
    const s0 = pl.Series("a", [[3, 5, 6]]);
    const actual = s0.lst.shift(-1);
    const expected = pl.Series("a", [5, 6, null]);
    expect(actual.seriesEqual(expected));
  });
  test("join", () => {
    const df = pl.DataFrame({ a: [["ab", "cd"], ["e", "fg"], ["h"]] });
    const expected = pl.DataFrame({ joinedString: ["ab,cd", "e,fg", "h"] });
    const actual = df.select(col("a").lst.join().as("joinedString"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.join()
      .rename("joinedString")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("join:separator", () => {
    const df = pl.DataFrame({ a: [["ab", "cd"], ["e", "fg"], ["h"]] });
    const expected = pl.DataFrame({ joinedString: ["ab|cd", "e|fg", "h"] });
    const actual = df.select(col("a").lst.join("|").as("joinedString"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.join("|")
      .rename("joinedString")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });

  test("last", () => {
    const df = pl.DataFrame({
      a: [
        [1, 10],
        [2, 12],
      ],
    });
    const expected = pl.DataFrame({ last: [10, 12] });
    const actual = df.select(col("a").lst.last().as("last"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.last()
      .rename("last")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("lengths", () => {
    const df = pl.DataFrame({ a: [[1], [2, 12], []] });
    const expected = pl.DataFrame({ lengths: [1, 2, 0] });
    const actual = df.select(col("a").lst.lengths().as("lengths"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.lengths()
      .rename("lengths")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("max", () => {
    const df = pl.DataFrame({
      a: [
        [1, -2],
        [2, 12, 1],
        [0, 1, 1, 5, 1],
      ],
    });
    const expected = pl.DataFrame({ max: [1, 12, 5] });
    const actual = df.select(col("a").lst.max().as("max"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.max()
      .rename("max")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("mean", () => {
    const df = pl.DataFrame({
      a: [
        [1, -1],
        [2, 2, 8],
        [1, 1, 5, 1],
      ],
    });
    const expected = pl.DataFrame({ mean: [0, 4, 2] });
    const actual = df.select(col("a").lst.mean().as("mean"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.mean()
      .rename("mean")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("min", () => {
    const df = pl.DataFrame({
      a: [
        [1, -2],
        [2, 12, 1],
        [0, 1, 1, 5, 1],
      ],
    });
    const expected = pl.DataFrame({ min: [-2, 1, 0] });
    const actual = df.select(col("a").lst.min().as("min"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.min()
      .rename("min")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("reverse", () => {
    const df = pl.DataFrame({
      a: [
        [1, 2],
        [2, 0, 1],
        [0, 1, 1, 5, 1],
      ],
    });
    const expected = pl.DataFrame({
      reverse: [
        [2, 1],
        [1, 0, 2],
        [1, 5, 1, 1, 0],
      ],
    });
    const actual = df.select(col("a").lst.reverse().as("reverse"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.reverse()
      .rename("reverse")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
  test("sort", () => {
    const df = pl.DataFrame({
      a: [
        [1, 2, 1],
        [2, 0, 1],
        [0, 1, 1, 5, 1],
      ],
    });
    const expected = pl.DataFrame({
      sort: [
        [1, 1, 2],
        [0, 1, 2],
        [0, 1, 1, 1, 5],
      ],
      "sort:reverse": [
        [2, 1, 1],
        [2, 1, 0],
        [5, 1, 1, 1, 0],
      ],
    });
    const actual = df.select(
      col("a").lst.sort().as("sort"),
      col("a").lst.sort({ descending: true }).as("sort:reverse"),
    );
    const sortSeries = df.getColumn("a").lst.sort().rename("sort");
    const sortReverseSeries = df
      .getColumn("a")
      .lst.sort({ descending: true })
      .rename("sort:reverse");

    const actualFromSeries = pl.DataFrame([sortSeries, sortReverseSeries]);
    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(actual);
  });
  test("sum", () => {
    const df = pl.DataFrame({
      a: [
        [1, 2],
        [2, 0, 1],
        [0, 1, 1, 5, 1],
      ],
    });
    const expected = pl.DataFrame({ sum: [3, 3, 8] });
    const actual = df.select(col("a").lst.sum().as("sum"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.sum()
      .rename("sum")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(actual);
  });
  test("unique", () => {
    const df = pl.DataFrame({
      a: [
        [1, 2, 1],
        [2, 0, 1],
        [5, 5, 5, 5],
      ],
    });
    const expected = pl.DataFrame({ unique: [[1, 2], [0, 1, 2], [5]] });
    const actual = df.select(col("a").lst.unique().lst.sort().as("unique"));
    const actualFromSeries = df
      .getColumn("a")
      .lst.unique()
      .lst.sort()
      .rename("unique")
      .toFrame();

    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(actual);
  });
});
describe("expr.dt", () => {
  test("day", () => {
    const dt = new Date(Date.parse("08 Jan 84 01:10:30 UTC"));
    const df = pl.DataFrame([pl.Series("date_col", [dt], pl.Datetime)]);
    const expected = pl.DataFrame({
      millisecond: pl.Series("", [0.0], pl.UInt32),
      second: pl.Series("", [30], pl.UInt32),
      minute: pl.Series("", [10], pl.UInt32),
      hour: pl.Series("", [1], pl.UInt32),
      day: pl.Series("", [8], pl.UInt32),
      ordinalDay: pl.Series("", [8], pl.UInt32),
      weekday: pl.Series("", [7], pl.UInt32),
      week: pl.Series("", [1], pl.UInt32),
      month: pl.Series("", [1], pl.UInt32),
      year: pl.Series("", [1984], pl.Int32),
    });
    const dtCol = col("date_col").date;
    const dtSeries = df.getColumn("date_col").date;
    const actual = df.select(
      dtCol.nanosecond().as("millisecond"),
      dtCol.second().as("second"),
      dtCol.minute().as("minute"),
      dtCol.hour().as("hour"),
      dtCol.day().as("day"),
      dtCol.ordinalDay().as("ordinalDay"),
      dtCol.weekday().as("weekday"),
      dtCol.week().as("week"),
      dtCol.month().as("month"),
      dtCol.year().as("year"),
    );

    const actualFromSeries = pl.DataFrame([
      dtSeries.nanosecond().rename("millisecond"),
      dtSeries.second().rename("second"),
      dtSeries.minute().rename("minute"),
      dtSeries.hour().rename("hour"),
      dtSeries.day().rename("day"),
      dtSeries.ordinalDay().rename("ordinalDay"),
      dtSeries.weekday().rename("weekday"),
      dtSeries.week().rename("week"),
      dtSeries.month().rename("month"),
      dtSeries.year().rename("year"),
    ]);
    expect(actual).toFrameEqual(expected);
    expect(actualFromSeries).toFrameEqual(expected);
  });
});
describe("expr metadata", () => {
  test("inspect & toString", () => {
    const expr = lit("foo");
    const expected = '"foo"';
    const actualInspect = expr[Symbol.for("nodejs.util.inspect.custom")]();
    const exprString = expr.toString();
    expect(actualInspect).toStrictEqual(expected);
    expect(exprString).toStrictEqual(expected);
  });
});

describe("rolling", () => {
  test("rollingMax", () => {
    const df = pl.Series("rolling", [1, 2, 3, 2, 1]).toFrame();
    const expected = pl
      .Series("rolling", [null, 2, 3, 3, 2], pl.Float64)
      .toFrame();
    const actual = df.select(col("rolling").rollingMax(2));
    expect(actual).toFrameEqual(expected);
  });
  test("rollingMean", () => {
    const df = pl.Series("rolling", [1, 2, 3, 2, 1]).toFrame();
    const expected = pl
      .Series("rolling", [null, 1.5, 2.5, 2.5, 1.5], pl.Float64)
      .toFrame();
    const actual = df.select(col("rolling").rollingMean(2));
    expect(actual).toFrameEqual(expected);
  });
  test("rollingMin", () => {
    const df = pl.Series("rolling", [1, 2, 3, 2, 1]).toFrame();
    const expected = pl
      .Series("rolling", [null, 1, 2, 2, 1], pl.Float64)
      .toFrame();
    const actual = df.select(col("rolling").rollingMin(2));
    expect(actual).toFrameEqual(expected);
  });
  test("rollingSum", () => {
    const df = pl.Series("rolling", [1, 2, 3, 2, 1]).toFrame();
    const expected = pl
      .Series("rolling", [null, 3, 5, 5, 3], pl.Float64)
      .toFrame();
    const actual = df.select(col("rolling").rollingSum(2));
    expect(actual).toFrameEqual(expected);
  });
  test("rollingStd", () => {
    const df = pl.Series("rolling", [1, 2, 3, 2, 1]).toFrame();
    const expected = pl
      .Series(
        "rolling",
        [null, Math.SQRT1_2, Math.SQRT1_2, Math.SQRT1_2, Math.SQRT1_2],
        pl.Float64,
      )
      .round(10)
      .toFrame();
    expect(df.select(col("rolling").rollingStd(2).round(10))).toFrameEqual(
      expected,
    );
    expect(
      df.select(
        col("rolling")
          .rollingStd({ windowSize: 2, center: true, ddof: 4 })
          .round(10),
      ),
    ).toFrameEqual(expected);
  });
  test("rollingVar", () => {
    const df = pl.Series("rolling", [1, 2, 3, 2, 1]).toFrame();
    const actual = df
      .select(
        col("rolling").rollingVar({ windowSize: 2, center: true, ddof: 4 }),
      )
      .row(1);
    expect(actual).toEqual([0.5]);
  });
  test("rollingMedian", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 3, 2, 10, 8] });
    const expected = pl.DataFrame({
      a: [1, 2, 3, 3, 2, 10, 8],
      rolling_median_a: [null, 1.5, 2.5, 3, 2.5, 6, 9],
    });
    const actual = df.withColumn(
      col("a").rollingMedian({ windowSize: 2 }).prefix("rolling_median_"),
    );
    expect(actual).toFrameStrictEqual(expected);
  });
  test("rollingQuantile", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 3, 2, 10, 8] });
    const expected = pl.DataFrame({
      a: [1, 2, 3, 3, 2, 10, 8],
      rolling_quantile_a: [null, 2, 3, 3, 3, 10, 10],
    });
    const actual = df.withColumn(
      col("a")
        .rollingQuantile({ windowSize: 2, quantile: 0.5 })
        .prefix("rolling_quantile_"),
    );

    expect(actual).toFrameStrictEqual(expected);
  });
  // SEE https://github.com/pola-rs/polars/issues/4215
  test("rollingSkew", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 3, 2, 10, 8] });
    const expected = pl.DataFrame({
      a: [1, 2, 3, 3, 2, 10, 8],
      bias_true: [
        null,
        null,
        null,
        "-0.49338220021815865",
        "0.0",
        "1.097025449363867",
        "0.09770939201338157",
      ],
      bias_false: [
        null,
        null,
        null,
        "-0.8545630383279712",
        "0.0",
        "1.9001038154942962",
        "0.16923763134384154",
      ],
    });
    const actual = df.withColumns(
      col("a")
        .cast(pl.UInt64)
        .rollingSkew(4)
        .cast(pl.Utf8) // casted to string to retain precision when extracting to JS
        .as("bias_true"),
      col("a")
        .cast(pl.UInt64)
        .rollingSkew({ windowSize: 4, bias: false })
        .cast(pl.Utf8) // casted to string to retain precision when extracting to JS
        .as("bias_false"),
    );
    expect(actual).toFrameStrictEqual(expected);
  });
});

describe("arithmetic", () => {
  test("add/plus", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
    });
    const expected = pl.DataFrame({
      a: [2, 3, 4],
    });
    const actual = df.select(col("a").add(1));
    expect(actual).toFrameEqual(expected);
    const actual1 = df.select(col("a").plus(1));
    expect(actual1).toFrameEqual(expected);
  });
  test("sub/minus", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
    });
    const expected = pl.DataFrame({
      a: [0, 1, 2],
    });
    const actual = df.select(col("a").sub(1));
    expect(actual).toFrameEqual(expected);
    const actual1 = df.select(col("a").minus(1));
    expect(actual1).toFrameEqual(expected);
  });
  test("div/divideBy", () => {
    const df = pl.DataFrame({
      a: [2, 4, 6],
    });
    const expected = pl.DataFrame({
      a: [1, 2, 3],
    });
    const actual = df.select(col("a").div(2));
    expect(actual).toFrameEqual(expected);
    const actual1 = df.select(col("a").divideBy(2));
    expect(actual1).toFrameEqual(expected);
  });
  test("mul/multiplyBy", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
    });
    const expected = pl.DataFrame({
      a: [2, 4, 6],
    });
    const actual = df.select(col("a").mul(2));
    expect(actual).toFrameEqual(expected);
    const actual1 = df.select(col("a").multiplyBy(2));
    expect(actual1).toFrameEqual(expected);
  });
  test("rem/modulo", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
    });
    const expected = pl.DataFrame({
      a: [1, 0, 1],
    });
    const actual = df.select(col("a").rem(2));
    expect(actual).toFrameEqual(expected);
    const actual1 = df.select(col("a").modulo(2));
    expect(actual1).toFrameEqual(expected);
  });
});

describe("Round<T>", () => {
  test("ceil", () => {
    const df = pl.Series("foo", [1.1, 2.2]).as("ceil").toFrame();

    const expected = pl.DataFrame({
      ceil: [2, 3],
    });

    const seriesActual = df.getColumn("ceil").ceil().toFrame();

    const actual = df.select(col("ceil").ceil().as("ceil"));

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });

  test("clip", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 4, 5] });
    const expected = [2, 2, 3, 4, 4];
    const exprActual = df
      .select(pl.col("a").clip(2, 4))
      .getColumn("a")
      .toArray();

    const seriesActual = pl.Series([1, 2, 3, 4, 5]).clip(2, 4).toArray();

    expect(exprActual).toEqual(expected);
    expect(seriesActual).toEqual(expected);
  });
  test("floor", () => {
    const df = pl.Series("foo", [1.1, 2.2]).as("floor").toFrame();

    const expected = pl.DataFrame({
      floor: [1, 2],
    });

    const seriesActual = df.getColumn("floor").floor().toFrame();

    const actual = df.select(col("floor").floor().as("floor"));

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toFrameEqual(expected);
  });
  test("round", () => {});
  test("round invalid dtype", () => {
    const df = pl.DataFrame({ a: ["1", "2"] });
    const seriesFn = () => df["a"].round({ decimals: 1 });
    const exprFn = () => df.select(col("a").round({ decimals: 1 }));
    expect(seriesFn).toThrow();
    expect(exprFn).toThrow();
  });
  test("clip invalid dtype", () => {
    const df = pl.DataFrame({ a: ["1", "2"] });
    const seriesFn = () => df["a"].clip({ min: 1, max: 2 });
    const exprFn = () => df.select(col("a").clip(1, 2));
    expect(seriesFn).toThrow();
    expect(exprFn).toThrow();
  });
});

describe("EWM", () => {
  test("ewmMean", () => {
    const s = pl.Series("s", [2, 5, 3]);
    const df = pl.DataFrame([s]);
    let expected = pl.DataFrame({ s, ewmMean: [2.0, 4.0, 3.4285714285714284] });
    {
      const seriesActual = df.getColumn("s").ewmMean().rename("ewmMean");
      const actual = df.withColumn(col("s").ewmMean().as("ewmMean"));

      expect(actual).toFrameEqual(expected);
      expect(seriesActual).toSeriesEqual(expected.getColumn("ewmMean"));
    }
    {
      const seriesActual = df
        .getColumn("s")
        .ewmMean({ alpha: 0.5, adjust: true, ignoreNulls: true })
        .rename("ewmMean");
      const actual = df.withColumn(
        col("s")
          .ewmMean({ alpha: 0.5, adjust: true, ignoreNulls: true })
          .as("ewmMean"),
      );

      expect(actual).toFrameEqual(expected);
      expect(seriesActual).toSeriesEqual(expected.getColumn("ewmMean"));
    }
    {
      const seriesActual = df
        .getColumn("s")
        .ewmMean({ alpha: 0.5, adjust: false, ignoreNulls: true })
        .rename("ewmMean");
      const actual = df.withColumn(
        col("s")
          .ewmMean({ alpha: 0.5, adjust: false, ignoreNulls: true })
          .as("ewmMean"),
      );

      expected = pl.DataFrame({ s, ewmMean: [2.0, 3.5, 3.25] });
      expect(actual).toFrameEqual(expected);
      expect(seriesActual).toSeriesEqual(expected.getColumn("ewmMean"));
    }
    {
      const seriesActual = df
        .getColumn("s")
        .ewmMean(0.5, false, 1, true)
        .rename("ewmMean");
      const actual = df.withColumn(
        col("s").ewmMean(0.5, false, 1, true).as("ewmMean"),
      );

      expect(actual).toFrameEqual(expected);
      expect(seriesActual).toSeriesEqual(expected.getColumn("ewmMean"));
    }
    {
      const s = pl.Series("a", [2, 3, 5, 7, 4]);
      const df = pl.DataFrame([s]);

      const seriesActual = df
        .getColumn("a")
        .ewmMean({ adjust: true, minPeriods: 2, ignoreNulls: true })
        .round(5)
        .rename("ewmMean");
      const actual = df.withColumn(
        col("a")
          .ewmMean({ adjust: true, minPeriods: 2, ignoreNulls: true })
          .round(5)
          .as("ewmMean"),
      );

      expected = pl.DataFrame({ ewmMean: [null, 2.66667, 4, 5.6, 4.77419], s });
      expect(seriesActual).toSeriesEqual(expected.getColumn("ewmMean"));
    }
  });

  test("ewmStd", () => {
    const s = pl.Series("s", [2, 5, 3]);
    const df = pl.DataFrame([s]);
    const expected = pl.DataFrame({ s, ewmStd: [0, 2.12132, 1.38873] });

    let seriesActual = df.getColumn("s").ewmStd().round(5).rename("ewmStd");
    let actual = df.withColumn(col("s").ewmStd().round(5).as("ewmStd"));

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("ewmStd"));

    seriesActual = df
      .getColumn("s")
      .ewmStd({ alpha: 0.5, adjust: true, ignoreNulls: true })
      .round(5)
      .rename("ewmStd");
    actual = df.withColumn(
      col("s")
        .ewmStd({ alpha: 0.5, adjust: true, ignoreNulls: true })
        .round(5)
        .as("ewmStd"),
    );

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("ewmStd"));

    seriesActual = df
      .getColumn("s")
      .ewmStd(0.5, true, 1, false)
      .round(5)
      .rename("ewmStd");
    actual = df.withColumn(
      col("s").ewmStd(0.5, true, 1, false).round(5).as("ewmStd"),
    );

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("ewmStd"));
  });

  test("ewmVar", () => {
    const s = pl.Series("s", [2, 5, 3]);
    const df = pl.DataFrame([s]);
    const expected = pl.DataFrame({ s, ewmVar: [0, 4.5, 1.92857] });

    let seriesActual = df.getColumn("s").ewmVar().round(5).rename("ewmVar");
    let actual = df.withColumn(col("s").ewmVar().round(5).as("ewmVar"));

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("ewmVar"));

    seriesActual = df
      .getColumn("s")
      .ewmVar({ alpha: 0.5, adjust: true, ignoreNulls: true })
      .round(5)
      .rename("ewmVar");
    actual = df.withColumn(
      col("s")
        .ewmVar({ alpha: 0.5, adjust: true, ignoreNulls: true })
        .round(5)
        .as("ewmVar"),
    );

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("ewmVar"));

    seriesActual = df
      .getColumn("s")
      .ewmVar(0.5, true, 1, false)
      .round(5)
      .rename("ewmVar");
    actual = df.withColumn(
      col("s").ewmVar(0.5, true, 1, false).round(5).as("ewmVar"),
    );

    expect(actual).toFrameEqual(expected);
    expect(seriesActual).toSeriesEqual(expected.getColumn("ewmVar"));
  });
});
