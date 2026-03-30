import pl, { col, cols, DataType, lit } from "../polars";
import { df as _df } from "./setup";

describe("lazy functions", () => {
  test("col:string", () => {
    const expected = pl.Series("foo", [1, 2, 3]);
    const other = pl.Series("other", [1, 2, 3]);
    const df = pl.DataFrame([expected, other]);
    const actual = df.select(col("foo"));
    assertFrameEqual(actual, expected.toFrame());
  });
  test("col:string[]", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
        other: ["a", "b", "c"],
      })
      .select(col(["foo", "bar"]));
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [4, 5, 6],
    });
    assertFrameEqual(actual, expected);
  });

  test("col:series", () => {
    const columnsToSelect = pl.Series(["foo", "bar"]);
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
        other: ["a", "b", "c"],
      })
      .select(col(columnsToSelect));
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [4, 5, 6],
    });
    assertFrameEqual(actual, expected);
  });
  test("col:datatype", () => {
    const actual = pl
      .DataFrame({
        ints: [1, 2, 3],
        strs: ["a", "b", "c"],
      })
      .select(col(DataType.Float64));
    const expected = pl.DataFrame({ ints: [1, 2, 3] });
    assertFrameEqual(actual, expected);
  });
  test("cols", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
        other: ["a", "b", "c"],
      })
      .select(cols("foo", "bar"));
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [4, 5, 6],
    });
    assertFrameEqual(actual, expected);
  });
  describe("lit", () => {
    test("string", () => {
      const actual = pl
        .DataFrame({
          foo: [1, 2, 3],
          bar: [4, 5, 6],
        })
        .select(col("foo"), lit("a").as("lit_a"));
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        lit_a: ["a", "a", "a"],
      });
      assertFrameEqual(actual, expected);
    });
    test("number", () => {
      const actual = pl
        .DataFrame({
          foo: [1, 2, 3],
          bar: [4, 5, 6],
        })
        .select(col("foo"), lit(-99).as("number"));
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        number: [-99, -99, -99],
      });
      assertFrameEqual(actual, expected);
    });
    test("bigint", () => {
      const actual = pl
        .DataFrame({
          foo: [1, 2, 3],
          bar: [4, 5, 6],
        })
        .select(col("foo"), lit(999283899189222n).as("bigint"));
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        bigint: [999283899189222n, 999283899189222n, 999283899189222n],
      });
      assertFrameEqual(actual, expected);
    });
    test("series", () => {
      const actual = pl
        .DataFrame({
          foo: [1, 2, 3],
          bar: [4, 5, 6],
        })
        .select(col("foo"), lit("one").as("series:string"));
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        "series:string": ["one", "one", "one"],
      });
      assertFrameEqual(actual, expected);
    });
    test("null", () => {
      const actual = pl
        .DataFrame({
          foo: [1, 2, 3],
          bar: [4, 5, 6],
        })
        .select(col("foo"), lit(null).as("nulls"));
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        nulls: [null, null, null],
      });
      assertFrameEqual(actual, expected);
    });
  });
  test("intRange:positional", () => {
    const df = pl.DataFrame({
      foo: [1, 1, 1],
    });
    const expected = pl.DataFrame({ foo: [1, 1] });
    const actual = df.filter(pl.col("foo").gtEq(pl.intRange(0, 3)));
    assertFrameEqual(actual, expected);
  });
  test("intRange:named", () => {
    const df = pl.DataFrame({
      foo: [1, 1, 1],
    });
    const expected = pl.DataFrame({ foo: [1, 1] });
    const actual = df.filter(
      pl.col("foo").gtEq(pl.intRange({ start: 0, end: 3 })),
    );
    assertFrameEqual(actual, expected);
  });
  test("intRange:eager", () => {
    const df = pl.DataFrame({
      foo: [1, 1, 1],
    });
    const expected = pl.DataFrame({ foo: [1, 1] });
    const actual = df.filter(
      pl.col("foo").gtEq(pl.intRange({ start: 0, end: 3, eager: true })),
    );
    assertFrameEqual(actual, expected);
  });
  test("intRange:len expr", () => {
    const df = pl.DataFrame({ foo: [10, 20, 30] });
    const actual = df.select(pl.intRange(pl.len()).alias("idx"));
    const expected = pl.DataFrame({ idx: [0, 1, 2] });
    assertFrameEqual(actual, expected);
  });
  test.each`
    start  | end  | expected
    ${"a"} | ${"b"} | ${pl.Series("a", [[1, 2], [2, 3]])}
    ${-1} | ${"a"} | ${pl.Series("literal", [[-1, 0], [-1, 0, 1]])}
    ${"b"} | ${4} | ${pl.Series("b", [[3], []])}
  `("$# cumMax", ({ start, end, expected }) => {
    const df = pl.DataFrame({ a: [1, 2], b: [3, 4] });
    const result = df.select(pl.intRanges(start, end)).toSeries();
    assertSeriesEqual(result, expected);
  });

  test("intRanges:dtype", () => {
    const df = pl.DataFrame({ a: [1, 2], b: [3, 4] });
    const result = df.select(pl.intRanges("a", "b"));
    const expected_schema = { a: pl.List(pl.Int64) };
    assert.deepStrictEqual(result.schema, expected_schema);
  });

  test("intRanges:eager", () => {
    const start = pl.Series([1, 2]);
    const result = pl.intRanges(start, 4, 1, DataType.Int64, true);
    let expected = pl.Series("intRanges", [
      [1, 2, 3],
      [2, 3],
    ]);
    assertSeriesEqual(result, expected);

    expected = pl.Series("intRanges", [[5, 4, 3, 2, 1]]);
    assertSeriesEqual(pl.intRanges(5, 0, -1, DataType.Int64, true), expected);
  });
  test("argSortBy", () => {
    const actual = _df()
      .select(
        pl.argSortBy([pl.col("int_nulls"), pl.col("floats")], [false, true]),
      )
      .getColumn("int_nulls");
    const expected = pl.Series("int_nulls", [1, 0, 2]);
    assertSeriesEqual(actual, expected);
  });
  test("argSortBy:scalar descending", () => {
    const actual = pl
      .DataFrame({
        a: [0, 1, 1, 0],
      })
      .select(pl.argSortBy([pl.col("a")], true))
      .toSeries();
    const expected = pl.Series("a", [1, 2, 0, 3]);
    assertSeriesEqual(actual, expected);
  });
  test("avg", () => {
    const df = pl.DataFrame({ foo: [4, 5, 6, 4, 5, 6] });
    const expected = pl.select(lit(5).as("foo"));
    const actual = df.select(pl.avg("foo"));

    assertFrameEqual(actual, expected);
  });
  test("concatList", () => {
    const s0 = pl.Series("a", [[1, 2]]);
    const s1 = pl.Series("b", [[3, 4, 5]]);
    const expected = pl.Series("a", [[1, 2, 3, 4, 5]]);
    const df = pl.DataFrame([s0, s1]);

    const actual = df.select(pl.concatList(["a", "b"]).as("a")).getColumn("a");
    assertSeriesEqual(actual, expected);
  });
  test("concatString", () => {
    const s0 = pl.Series("a", ["a", "b", "c"]);
    const s1 = pl.Series("b", ["d", "e", "f"]);
    const expected = pl.Series("concat", ["a,d", "b,e", "c,f"]);
    const df = pl.DataFrame([s0, s1]);
    const actual = df
      .select(pl.concatString(["a", "b"]).as("concat"))
      .getColumn("concat");
    assertSeriesEqual(actual, expected);
  });
  test("concatString:sep", () => {
    const s0 = pl.Series("a", ["a", "b", "c"]);
    const s1 = pl.Series("b", ["d", "e", "f"]);
    const expected = pl.Series("concat", ["a=d", "b=e", "c=f"]);
    const df = pl.DataFrame([s0, s1]);
    const actual = df
      .select(pl.concatString(["a", "b"], "=").as("concat"))
      .getColumn("concat");
    assertSeriesEqual(actual, expected);
  });
  test("concatString:named", () => {
    const s0 = pl.Series("a", ["a", "b", "c"]);
    const s1 = pl.Series("b", ["d", "e", "f"]);
    const expected = pl.Series("concat", ["a=d", "b=e", "c=f"]);
    const df = pl.DataFrame([s0, s1]);
    const actual = df
      .select(pl.concatString({ exprs: ["a", "b"], sep: "=" }).as("concat"))
      .getColumn("concat");
    assertSeriesEqual(actual, expected);
  });
  test("concatString:ignoreNulls:false", () => {
    const df = pl.DataFrame({
      a: ["x", null],
      b: ["y", "z"],
    });
    const actual = df
      .select(
        pl
          .concatString({ exprs: ["a", "b"], sep: "=", ignoreNulls: false })
          .as("concat"),
      )
      .getColumn("concat");
    const expected = pl.Series("concat", ["x=y", null]);
    assertSeriesEqual(actual, expected);
  });
  test("count:series", () => {
    const s0 = pl.Series([1, 2, 3, 4, 5]);
    const expected = 5;
    const actual = pl.count(s0);
    assert.deepStrictEqual(actual, expected);
  });
  test("count:column", () => {
    const s0 = pl.Series("a", [1, 2, 3, 4, 5]).cast(pl.Int32);
    const s1 = pl.Series("b", [11, 22, 33, 44, 55]).cast(pl.Int32);
    const expected = pl.select(lit(5).cast(pl.Int32).as("a"));
    const actual = pl.DataFrame([s0, s1]).select(pl.count("a"));
    assertFrameEqual(actual, expected);
  });
  test("cov", () => {
    const df = pl.DataFrame([
      pl.Series("A", [1, 2, 3, 4, 5]),
      pl.Series("B", [5, 4, 3, 2, 1]),
    ]);
    const actual = df.select(pl.cov("A", "B")).row(0)[0];
    assert.deepStrictEqual(actual, -2.5);
  });
  test("cov:expr", () => {
    const df = pl.DataFrame([
      pl.Series("A", [1, 2, 3, 4, 5]),
      pl.Series("B", [5, 4, 3, 2, 1]),
    ]);
    const actual = df.select(pl.cov(col("A"), col("B"))).row(0)[0];
    assert.deepStrictEqual(actual, -2.5);
  });
  test("exclude", () => {
    const df = pl.DataFrame([
      pl.Series("A", [1, 2, 3, 4, 5]),
      pl.Series("B", [5, 4, 3, 2, 1]),
      pl.Series("C", ["a", "b", "c", "d", "e"]),
    ]);
    const expected = pl.DataFrame([
      pl.Series("A", [1, 2, 3, 4, 5]),
      pl.Series("C", ["a", "b", "c", "d", "e"]),
    ]);
    const actual = df.select(pl.exclude("B"));
    assertFrameEqual(actual, expected);
  });
  test("exclude:multiple", () => {
    const df = pl.DataFrame([
      pl.Series("A", [1, 2, 3, 4, 5]),
      pl.Series("B", [5, 4, 3, 2, 1]),
      pl.Series("C", ["a", "b", "c", "d", "e"]),
    ]);
    const expected = pl.DataFrame([pl.Series("C", ["a", "b", "c", "d", "e"])]);
    const actual = df.select(pl.exclude("A", "B"));
    assertFrameEqual(actual, expected);
  });
  test("first:series", () => {
    const s = pl.Series("a", [1, 2, 3]);
    const actual = pl.first(s);
    assert.deepStrictEqual(actual, 1);
  });
  test("first:df", () => {
    const actual = _df().select(pl.first("bools")).row(0)[0];
    assert.deepStrictEqual(actual, false);
  });
  test("first:no-arg", () => {
    const df = pl.DataFrame({ a: [5, 6, 7] });
    const actual = df.select(pl.first());
    const expected = pl.DataFrame({ a: [5, 6, 7] });
    assertFrameEqual(actual, expected);
  });
  test("first:invalid", () => {
    const s = pl.Series("a", []);
    const fn = () => pl.first(s);
    assert.throws(fn);
  });
  test("format:tag", () => {
    const df = pl.DataFrame({
      a: ["a", "b", "c"],
      b: [1, 2, 3],
    });
    const expected = pl.DataFrame({
      eq: ["a=a;b=1.0", "a=b;b=2.0", "a=c;b=3.0"],
    });
    const actual = df.select(
      pl.format`${lit("a")}=${col("a")};b=${col("b")}`.as("eq"),
    );
    assertFrameEqual(actual, expected);
  });
  test("format:pattern", () => {
    const df = pl.DataFrame({
      a: ["a", "b", "c"],
      b: [1, 2, 3],
    });
    const fmt = pl.format("{}={};b={}", lit("a"), col("a"), col("b")).as("eq");
    const expected = pl.DataFrame({
      eq: ["a=a;b=1.0", "a=b;b=2.0", "a=c;b=3.0"],
    });
    const actual = df.select(fmt);
    assertFrameEqual(actual, expected);
  });
  test("format:invalid", () => {
    const fn = () =>
      pl.format("{}{}={};b={}", lit("a"), col("a"), col("b")).as("eq");
    assert.throws(fn);
  });
  test("head:series", () => {
    const expected = pl.Series("a", [1, 2]);
    const actual = pl.head(pl.Series("a", [1, 2, 3]), 2);
    assertSeriesEqual(actual, expected);
  });
  test("head:df", () => {
    const df = pl.DataFrame({
      a: [1, 2, 5],
      b: ["foo", "bar", "baz"],
    });
    const expected = pl.DataFrame({
      a: [1],
      b: ["foo"],
    });

    const actual = df.select(pl.head("*", 1));
    assertFrameEqual(actual, expected);
  });
  test("head:expr", () => {
    const df = pl.DataFrame({
      a: [1, 2, 5],
      b: ["foo", "bar", "baz"],
    });
    const expected = pl.DataFrame({
      a: [1],
      b: ["foo"],
    });

    const actual = df.select(pl.head(col("*"), 1));
    assertFrameEqual(actual, expected);
  });
  test("last:series", () => {
    const actual = pl.last(pl.Series("a", [1, 2, 3]));
    assert.deepStrictEqual(actual, 3);
  });
  test("last:string", () => {
    const df = pl.DataFrame({
      a: [1, 2, 5],
      b: ["foo", "bar", "baz"],
    });
    const actual = df.select(pl.last("b"));
    assertFrameEqual(actual, pl.select(lit("baz").as("b")));
  });
  test("last:col", () => {
    const df = pl.DataFrame({
      a: [1, 2, 5],
      b: ["foo", "bar", "baz"],
    });
    const actual = df.select(pl.last(col("b")));
    assertFrameEqual(actual, pl.select(lit("baz").as("b")));
  });
  test("last:invalid", () => {
    const fn = () => pl.last(pl.Series("a", []));
    assert.throws(fn, RangeError);
  });
  test("list", () => {
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
      .agg(pl.list("b").keepName())
      .sort({ by: "a" });
    assertFrameEqual(actual, expected);
  });
  test("groups helper", () => {
    const df = pl.DataFrame({
      g: ["x", "x", "y"],
      v: [1, 2, 3],
    });
    const actual = df
      .withRowIndex()
      .groupBy("g")
      .agg(pl.groups("index").alias("idx"))
      .sort("g");
    const expected = pl
      .DataFrame({
        g: ["x", "y"],
        idx: [[0, 1], [2]],
      })
      .withColumn(pl.col("idx").cast(pl.List(pl.UInt32)));
    assertFrameEqual(actual, expected);
  });
  test("mean:series", () => {
    const actual = pl.mean(pl.Series([2, 2, 8]));
    assert.deepStrictEqual(actual, 4);
  });
  test("mean:string", () => {
    const df = pl.DataFrame({ a: [2, 2, 8] });
    const actual = df.select(pl.mean("a")).getColumn("a")[0];
    assert.deepStrictEqual(actual, 4);
  });
  test("mean:col", () => {
    const df = pl.DataFrame({ a: [2, 2, 8] });
    const actual = df.select(pl.mean(col("a"))).getColumn("a")[0];
    assert.deepStrictEqual(actual, 4);
  });
  test("median:series", () => {
    const actual = pl.median(pl.Series([2, 2, 8]));
    assert.deepStrictEqual(actual, 2);
  });
  test("median:string", () => {
    const df = pl.DataFrame({ a: [2, 2, 8] });
    const actual = df.select(pl.median("a")).getColumn("a")[0];
    assert.deepStrictEqual(actual, 2);
  });
  test("median:col", () => {
    const df = pl.DataFrame({ a: [2, 2, 8] });
    const actual = df.select(pl.median(col("a"))).getColumn("a")[0];
    assert.deepStrictEqual(actual, 2);
  });
  test("nUnique:series", () => {
    const actual = pl.nUnique(pl.Series([2, 2, 8]));
    assert.deepStrictEqual(actual, 2);
  });
  test("nUnique:string", () => {
    const df = pl.DataFrame({ a: [2, 2, 8] });
    const actual = df.select(pl.nUnique("a")).getColumn("a")[0];
    assert.deepStrictEqual(actual, 2);
  });
  test("nUnique:col", () => {
    const df = pl.DataFrame({ a: [2, 2, 8] });
    const actual = df.select(pl.nUnique(col("a"))).getColumn("a")[0];
    assert.deepStrictEqual(actual, 2);
  });
  test("pearsonCorr", () => {
    const df = pl.DataFrame([
      pl.Series("A", [1, 2, 3, 4]),
      pl.Series("B", [2, 4, 6, 8]),
    ]);
    const actual = df.select(pl.pearsonCorr("A", "B").round(1)).row(0)[0];
    assert.deepStrictEqual(actual, 1);
  });
  test("quantile:series", () => {
    const s = pl.Series([1, 2, 3]);
    const actual = pl.quantile(s, 0.5);
    assert.deepStrictEqual(actual, 2);
  });
  test("quantile:string", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const actual = df.select(pl.quantile("a", 0.5)).getColumn("a")[0];
    assert.deepStrictEqual(actual, 2);
  });
  test("quantile:col", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const actual = df.select(pl.quantile(col("a"), 0.5)).getColumn("a")[0];
    assert.deepStrictEqual(actual, 2);
  });
  test("select helper", () => {
    const actual = pl.select(pl.lit(1).alias("one"));
    const expected = pl.DataFrame({ one: [1] });
    assertFrameEqual(actual, expected);
  });
  test("spearmanRankCorr", () => {
    const df = pl.DataFrame([
      pl.Series("A", [1, 2, 3, 4]),
      pl.Series("B", [2, 4, 6, 8]),
    ]);
    const actual = df.select(pl.spearmanRankCorr("A", "B").round(1)).row(0)[0];
    assert.deepStrictEqual(actual, 1);
  });
  test("tail:series", () => {
    const s = pl.Series("a", [1, 2, 3]);
    const expected = pl.Series("a", [2, 3]);
    const actual = pl.tail(s, 2);
    assertSeriesEqual(actual, expected);
  });
  test("tail:string", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.Series("a", [2, 3]);
    const actual = df.select(pl.tail("a", 2)).getColumn("a");
    assertSeriesEqual(actual, expected);
  });
  test("tail:col", () => {
    const df = pl.DataFrame({ a: [1, 2, 3] });
    const expected = pl.Series("a", [2, 3]);
    const actual = df.select(pl.tail(col("a"), 2)).getColumn("a");
    assertSeriesEqual(actual, expected);
  });
  test("element", () => {
    const df = pl.DataFrame({
      a: [
        [1, 2],
        [3, 4],
        [5, 6],
      ],
    });
    const expected = pl.DataFrame({
      a: [
        [2, 4],
        [6, 8],
        [10, 12],
      ],
    });
    const actual = df.select(pl.col("a").lst.eval(pl.element().mul(2)));
    assertFrameEqual(actual, expected);
  });
  test("horizontal aggregations", () => {
    const df = pl.DataFrame({
      a: [1, 8, 3],
      b: [4, 5, null],
      c: [10, 20, 30],
      x: [false, false, true],
      y: [false, true, null],
    });

    const actual = df.select(
      pl.maxHorizontal([pl.col("a"), pl.col("b")]).alias("max"),
      pl.minHorizontal([pl.col("a"), pl.col("b")]).alias("min"),
      pl.sumHorizontal([pl.col("a"), pl.col("b")]).alias("sum"),
      pl.allHorizontal([pl.col("x"), pl.col("y")]).alias("all"),
      pl.anyHorizontal([pl.col("x"), pl.col("y")]).alias("any"),
    );

    const expected = pl.DataFrame({
      max: [4, 8, 3],
      min: [1, 5, 3],
      sum: [5, 13, 3],
      all: [false, false, null],
      any: [false, true, true],
    });
    assertFrameEqual(actual, expected);
  });
});
