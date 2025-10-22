import pl from "@polars";

describe("concat", () => {
  it("can concat multiple dataframes vertically", () => {
    const df1 = pl.DataFrame({
      a: [1, 2, 3],
      b: ["a", "b", "c"],
    });
    const df2 = pl.DataFrame({
      a: [4, 5, 6],
      b: ["d", "e", "f"],
    });
    const actual = pl.concat([df1, df2]);
    const expected = pl.DataFrame({
      a: [1, 2, 3, 4, 5, 6],
      b: ["a", "b", "c", "d", "e", "f"],
    });
    expect(actual).toFrameEqual(expected);
  });
  it("can concat multiple dataframes vertically relaxed", () => {
    const df1 = pl.DataFrame(
      {
        a: [1, 2, 3],
        b: ["a", "b", "c"],
      },
      {
        schema: {
          a: pl.Int32,
          b: pl.String,
        },
      },
    );
    const df2 = pl.DataFrame({
      a: [4.5, 5.5, 6],
      b: ["d", "e", "f"],
    });
    const actual = pl.concat([df1, df2], { how: "verticalRelaxed" });
    const expected = pl.DataFrame({
      a: [1, 2, 3, 4.5, 5.5, 6],
      b: ["a", "b", "c", "d", "e", "f"],
    });
    expect(actual).toFrameEqual(expected);
  });
  it("can concat multiple lazy dataframes vertically", () => {
    const df1 = pl
      .DataFrame({
        a: [1, 2, 3],
        b: ["a", "b", "c"],
      })
      .lazy();
    const df2 = pl
      .DataFrame({
        a: [4, 5, 6],
        b: ["d", "e", "f"],
      })
      .lazy();
    const actual = pl.concat([df1, df2]).collectSync();
    const expected = pl.DataFrame({
      a: [1, 2, 3, 4, 5, 6],
      b: ["a", "b", "c", "d", "e", "f"],
    });
    expect(actual).toFrameEqual(expected);
  });

  it("can concat multiple lazy dataframes horizontally", () => {
    const df1 = pl
      .DataFrame({
        a: [1, 2, 3],
        b: ["a", "b", "c"],
      })
      .lazy();
    const df2 = pl
      .DataFrame({
        c: [4, 5, 6],
        d: ["d", "e", "f"],
      })
      .lazy();
    const actual = pl.concat([df1, df2], { how: "horizontal" }).collectSync();
    const expected = pl.DataFrame({
      a: [1, 2, 3],
      b: ["a", "b", "c"],
      c: [4, 5, 6],
      d: ["d", "e", "f"],
    });
    expect(actual).toFrameEqual(expected);
  });
  it("can concat multiple dataframes diagonally relaxed", () => {
    const df1 = pl.DataFrame(
      {
        a: [1],
        b: ["b"],
      },
      {
        schema: {
          a: pl.Int32,
          b: pl.String,
        },
      },
    );
    const df2 = pl.DataFrame({
      a: [2.5],
      d: [4.5],
    });
    const actual = pl.concat([df1, df2], { how: "diagonalRelaxed" });
    const expected = pl.DataFrame({
      a: [1, 2.5],
      b: ["b", null],
      d: [null, 4.5],
    });
    expect(actual).toFrameEqual(expected);
  });
  it("can concat multiple lazy dataframes diagonally", () => {
    const df1 = pl
      .DataFrame({
        a: [1],
        b: [3],
      })
      .lazy();
    const df2 = pl
      .DataFrame({
        a: [2],
        d: [4],
      })
      .lazy();
    const actual = pl.concat([df1, df2], { how: "diagonal" }).collectSync();
    const expected = pl.DataFrame({
      a: [1, 2],
      b: [3, null],
      d: [null, 4],
    });
    expect(actual).toFrameEqual(expected);
  });

  it("can concat multiple series vertically", () => {
    const s1 = pl.Series("a", [1, 2, 3]);
    const s2 = pl.Series("a", [4, 5, 6]);
    const actual = pl.concat([s1, s2]);
    const expected = pl.Series("a", [1, 2, 3, 4, 5, 6]);

    expect(actual).toSeriesEqual(expected);
  });
  it("cant concat empty list", () => {
    const fn = () => pl.concat([]);
    expect(fn).toThrow();
  });

  it("can only concat series and df", () => {
    const fn = () => pl.concat([[1] as any, [2] as any]);
    expect(fn).toThrow();
  });
  test("horizontal concat", () => {
    const a = pl.DataFrame({ a: ["a", "b"], b: [1, 2] });
    const b = pl.DataFrame({
      c: [5, 7, 8, 9],
      d: [1, 2, 1, 2],
      e: [1, 2, 1, 2],
    });
    const actual = pl.concat([a, b], { how: "horizontal" });
    const expected = pl.DataFrame({
      a: ["a", "b", null, null],
      b: [1, 2, null, null],
      c: [5, 7, 8, 9],
      d: [1, 2, 1, 2],
      e: [1, 2, 1, 2],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("diagonal concat", () => {
    const df1 = pl.DataFrame({ a: [1], b: [3] });
    const df2 = pl.DataFrame({ a: [2], c: [4] });
    const actual = pl.concat([df1, df2], { how: "diagonal" });
    const expected = pl.DataFrame({
      a: [1, 2],
      b: [3, null],
      c: [null, 4],
    });
    expect(actual).toFrameEqual(expected);
  });
});
describe("repeat", () => {
  it("repeats a value n number of times into a series", () => {
    const value = "foobar";
    const actual = pl.repeat(value, 4, "foo");
    const expected = pl.Series(
      "foo",
      Array.from({ length: 4 }, () => value),
    );

    expect(actual).toSeriesEqual(expected);
  });
  it("fail to repeat a date value", () => {
    const fn = () => pl.repeat(new Date(), 4, "foo");
    expect(fn).toThrow();
  });
});
describe("horizontal", () => {
  it("compute the bitwise AND horizontally across columns.", () => {
    const df = pl.DataFrame({
      a: [false, false, true, false, null],
      b: [false, false, false, null, null],
      c: [true, true, false, true, false],
    });
    const actual = df.select(pl.allHorizontal([pl.col("b"), pl.col("c")]));
    const expected = pl.DataFrame({ b: [false, false, false, null, false] });
    expect(actual).toFrameEqual(expected);
  });
  it("compute the bitwise OR horizontally across columns.", () => {
    const df = pl.DataFrame({
      a: [false, false, true, false, null],
      b: [false, false, false, null, null],
      c: [true, true, false, true, false],
    });
    const actual = df.select(pl.anyHorizontal([pl.col("b"), pl.col("c")]));
    const expected = pl.DataFrame({ b: [true, true, false, true, null] });
    expect(actual).toFrameEqual(expected);
  });
  it("any and all expression", () => {
    const lf = pl.DataFrame({
      a: [1, null, 2, null],
      b: [1, 2, null, null],
    });

    const actual = lf.select(
      pl.anyHorizontal(pl.col("*").isNull()).alias("null_in_row"),
      pl.allHorizontal(pl.col("*").isNull()).alias("all_null_in_row"),
    );

    const expected = pl.DataFrame({
      null_in_row: [false, true, true, true],
      all_null_in_row: [false, false, false, true],
    });
    expect(actual).toFrameEqual(expected);
  });
  it("min + max across columns", () => {
    const df = pl.DataFrame({ a: [1], b: [2], c: [3], d: [4] });

    const actual = df.withColumns(
      pl
        .maxHorizontal([
          pl.minHorizontal([pl.col("a"), pl.col("b")]),
          pl.minHorizontal([pl.col("c"), pl.col("d")]),
        ])
        .alias("t"),
    );

    const expected = pl.DataFrame({ a: [1], b: [2], c: [3], d: [4], t: [3] });
    expect(actual).toFrameEqual(expected);
  });

  it("sum min and max across columns", () => {
    const df = pl.DataFrame({ a: [1, 2, 3], b: [1.0, 2.0, 3.0] });
    const out = df.select(
      pl.sumHorizontal([pl.col("a"), pl.col("b")]).alias("sum"),
      pl.maxHorizontal([pl.col("a"), pl.col("b").pow(2)]).alias("max"),
      pl.minHorizontal([pl.col("a"), pl.col("b").pow(2)]).alias("min"),
    );
    expect(out["sum"]).toSeriesEqual(pl.Series("sum", [2.0, 4.0, 6.0]));
    expect(out["max"]).toSeriesEqual(pl.Series("max", [1.0, 4.0, 9.0]));
    expect(out["min"]).toSeriesEqual(pl.Series("min", [1.0, 2.0, 3.0]));
  });
});
