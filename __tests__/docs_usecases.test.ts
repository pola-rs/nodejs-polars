import pl, { col, DataType, lit } from "@polars";

describe("docs-inspired common usecases", () => {
  test("basic select/filter/sort", () => {
    const df = pl.DataFrame({ a: [3, 1, 2], b: ["x", "y", "z"] });
    const out = df
      .select({ a2: col("a").plus(1), b: col("b") })
      .filter(col("a2").gt(2))
      .sort("a2");
    expect(out.columns).toEqual(["a2", "b"]);
    expect(out.toRecords()).toEqual([
      { a2: 3, b: "z" },
      { a2: 4, b: "x" },
    ]);
  });

  test("joins: inner/left/cross", () => {
    const left = pl.DataFrame({ k: [1, 2], v1: ["a", "b"] });
    const right = pl.DataFrame({ k: [1, 3], v2: [true, false] });
    const inner = left.join(right, { on: "k", how: "inner" });
    expect(inner.columns).toEqual(["k", "v1", "v2"]);
    expect(inner.shape.height).toBe(1);

    const leftJoin = left.join(right, { on: "k", how: "left" });
    expect(leftJoin.shape.height).toBe(2);

    const cross = left.join(right, { how: "cross" });
    expect(cross.shape.height).toBe(left.shape.height * right.shape.height);
  });

  test("pivot/unpivot (melt)", () => {
    const df = pl.DataFrame({
      id: [1, 1, 2, 2],
      key: ["A", "B", "A", "B"],
      val: [10, 20, 30, 40],
    });
    const pv = df.pivot({ values: "val", index: "id", on: "key" });
    expect(pv.columns).toEqual(["id", "A", "B"]);

    const unp = pv.unpivot("id", ["A", "B"], {
      variableName: "key",
      valueName: "val",
    });
    expect(unp.columns).toEqual(["id", "key", "val"]);
  });

  test("string ops: contains/replace/concat", () => {
    const df = pl.DataFrame({ s: ["foo", "bar", "baz"] });
    const out = df.select({
      has_o: col("s").str.contains(lit("o")),
      rep: col("s").str.replace("a", "@"),
      cat: col("s").str.concat("!"),
    });
    expect(out.toRecords()).toEqual([
      { has_o: true, rep: "foo", cat: "foo!bar!baz" },
      { has_o: false, rep: "b@r", cat: "foo!bar!baz" },
      { has_o: false, rep: "b@z", cat: "foo!bar!baz" },
    ]);
  });

  test("list ops: concatList/contains/lengths", () => {
    const df = pl.DataFrame({ a: [[1, 2], [3], []], b: [[3], [4, 5], [6]] });
    const out = df.select({
      all: pl.concatList([col("a"), col("b")]),
      has3: pl.concatList([col("a"), col("b")]).lst.contains(3),
      len: pl.concatList([col("a"), col("b")]).lst.lengths(),
    });
    expect(out.toRecords()).toEqual([
      { all: [1, 2, 3], has3: true, len: 3 },
      { all: [3, 4, 5], has3: true, len: 3 },
      { all: [6], has3: false, len: 1 },
    ]);
  });

  test("datetime extract", () => {
    const df = pl.DataFrame({ d: [new Date("2020-01-05T04:03:02Z")] });
    const out = df
      .withColumns({ dt: col("d").cast(DataType.Datetime()) })
      .select({
        year: col("dt").date.year(),
        month: col("dt").date.month(),
        day: col("dt").date.day(),
        hour: col("dt").date.hour(),
        minute: col("dt").date.minute(),
      });
    expect(out.toRecords()[0]).toEqual({
      year: 2020,
      month: 1,
      day: 5,
      hour: 4,
      minute: 3,
    });
  });

  test("explode lists", () => {
    const df = pl.DataFrame({ g: ["a", "b"], x: [[1, 2], [3]] });
    const out = df.explode("x").sort(["g", "x"]);
    expect(out.toRecords()).toEqual([
      { g: "a", x: 1 },
      { g: "a", x: 2 },
      { g: "b", x: 3 },
    ]);
  });

  test("withColumn builder-callback", () => {
    const df = pl.DataFrame({ a: [1, 2, 3], b: [4, 5, 6] });
    const out = df
      .withColumn((bld) => bld.col("a").plus(1).alias("a_plus1"))
      .withColumn((bld) => bld.col("b").sum().alias("b_sum"));
    expect(out.columns).toEqual(["a", "b", "a_plus1", "b_sum"]);
    expect(String(out.getColumn("b_sum").dtype)).toBe(String(DataType.Float64));
  });

  test("lazy pipeline select/filter/collect", () => {
    const df = pl.DataFrame({ a: [1, 2, 3, 4], b: ["x", "y", "z", "w"] });
    const out = df
      .lazy()
      .filter(col("a").gt(2))
      .select({
        a2: col("a").multiplyBy(2),
        b: col("b"),
      })
      .collectSync();
    expect(out.toRecords()).toEqual([
      { a2: 6, b: "z" },
      { a2: 8, b: "w" },
    ]);
  });
});
