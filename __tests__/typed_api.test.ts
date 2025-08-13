import pl, { col, DataType } from "@polars";

// This test suite focuses on compile-time type inference and basic runtime sanity
// checks across the typed API: df.$, withColumns record + builder, groupBy.agg
// array + record, select typing, joins, and a few expr ops.

describe("typed api", () => {
  test("withColumns (record) + df.$ builder preserves/extends schema", () => {
    const df = pl.DataFrame({
      strings: ["a", "a", "b"],
      ints: [1, 2, 3],
      bools: [true, false, true],
    });

    const df2 = df.withColumns({
      alps: df.$.col("bools").cast(DataType.Int64),
      sara: df.$.col("ints").cast(DataType.Int64),
    });

    // compile-time type check
    const _schemaCheck: pl.DataFrame<{
      strings: DataType.String;
      ints: DataType.Float64;
      bools: DataType.Bool;
      alps: DataType.Int64;
      sara: DataType.Int64;
    }> = df2;

    expect(df2.shape.height).toBe(3);
    expect(df2.shape.width).toBe(5);
  });

  test("withColumns (builder-callback) infers names/dtypes", () => {
    const df = pl.DataFrame({
      strings: ["a", "a", "b"],
      ints: [1, 2, 3],
      bools: [true, false, true],
    });

    const df3 = df.withColumns((b) => ({
      x: b.col("ints").plus(1).cast(DataType.Int64),
      y: b.col("strings"),
    }));

    const _schemaCheck: pl.DataFrame<{
      strings: DataType.String;
      ints: DataType.Float64;
      bools: DataType.Bool;
      x: DataType.Int64;
      y: DataType.String;
    }> = df3;

    expect(df3.columns).toEqual(["strings", "ints", "bools", "x", "y"]);
  });

  test("groupBy.agg (builder array) infers all items", () => {
    const df = pl
      .DataFrame({
        strings: ["a", "a", "b"],
        ints: [1, 2, 3],
        bools: [true, false, true],
      })
      .withColumns((b) => ({
        alps: b.col("bools").cast(DataType.Int64),
        sara: b.col("ints").cast(DataType.Int64),
      }))
      .withColumns((b) => ({
        cost: b.col("ints").cast(DataType.Int64),
        test: b.col("ints").cast(DataType.Int64),
      }));

    const agg = df
      .groupBy("strings", "bools")
      .agg((g) => [
        g.col("cost").sum().alias("cost_sum"),
        g.col("alps").sum().alias("alps_sum"),
        g.col("sara").sum().alias("sara_sum"),
        g.col("test").sum().alias("test_sum"),
      ]);

    const _schemaCheck: pl.DataFrame<{
      strings: DataType.String;
      bools: DataType.Bool;
      cost_sum: DataType.Int64;
      alps_sum: DataType.Int64;
      sara_sum: DataType.Int64;
      test_sum: DataType.Int64;
    }> = agg;

    expect(agg.columns).toEqual([
      "strings",
      "bools",
      "cost_sum",
      "alps_sum",
      "sara_sum",
      "test_sum",
    ]);
  });

  test("groupBy.agg (builder record) precise names without alias", () => {
    const df = pl.DataFrame({
      g: ["x", "x", "y"],
      a: [1, 2, 3],
      b: [4, 5, 6],
    });

    const out = df.groupBy("g").agg((g) => ({
      a_sum: g.col("a").sum(),
      b_mean: g.col("b").mean(),
    }));

    const _schemaCheck: pl.DataFrame<{
      g: DataType.String;
      a_sum: DataType.Float64;
      b_mean: DataType.Float64;
    }> = out;

    expect(out.columns).toEqual(["g", "a_sum", "b_mean"]);
  });

  test("select with record of exprs infers output names", () => {
    const df = pl.DataFrame({ s: ["a", "b"], n: [1, 2] });
    const sel = df.select({
      s2: col("s"),
      n_i64: col("n").cast(DataType.Int64),
    });
    const _schemaCheck: pl.DataFrame<{
      s2: DataType.String;
      n_i64: DataType.Int64;
    }> = sel.select("s2", "n_i64") as any;
    expect(sel.shape.width).toBe(2);
  });

  test("typed joins", () => {
    const left = pl.DataFrame({ k: [1, 2], v1: ["a", "b"] });
    const right = pl.DataFrame({ k: [1, 3], v2: [true, false] });

    const j = left.join(right, { on: "k", how: "inner" });
    const _schemaCheck: pl.DataFrame<{
      k: DataType.Float64;
      v1: DataType.String;
      v2: DataType.Bool;
    }> = j;
    expect(j.shape.height).toBe(1);
  });

  test("expr arithmetic/comparison preserve dtypes/names", () => {
    const df = pl.DataFrame({ n: [1, 2, 3] });
    const e = df.$.col("n").plus(1);
    const _exprCheck: pl.Expr<DataType.Float64, "n"> = e;
    const b = df.$.col("n").gt(1);
    const _exprBool: pl.Expr<DataType.Bool, "n"> = b;
    // runtime sanity
    const out = df.select({ gt1: b, inc: e }).select("gt1", "inc");
    expect(out.columns).toEqual(["gt1", "inc"]);
  });
});
