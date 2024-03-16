import pl from "@polars";
describe("sql", () => {
  test("execute", () => {
    const df = pl.DataFrame({
      values: [
        ["aa", "bb"],
        [null, "cc"],
        ["dd", null],
      ],
    });

    const ctx = pl.SQLContext({ df });
    const actual = ctx.execute("SELECT * FROM df").collectSync();

    expect(actual).toFrameEqual(df);
    const actual2 = ctx.execute("SELECT * FROM df", { eager: true });
    expect(actual2).toFrameEqual(df);
  });

  test("register and query dataframe", () => {
    const df = pl.DataFrame({ hello: ["world"] });
    const ctx = pl.SQLContext();
    ctx.register("frame_data", df);
    const actual = ctx.execute("SELECT * FROM frame_data", { eager: true });

    const expected = pl.DataFrame({ hello: ["world"] });

    expect(actual).toFrameEqual(expected);
    ctx.register("null_frame", null);

    const actual2 = ctx.execute("SELECT * FROM null_frame", { eager: true });
    const expected2 = pl.DataFrame();
    expect(actual2).toFrameEqual(expected2);
  });
  test("register many", () => {
    const lf1 = pl.DataFrame({ a: [1, 2, 3], b: ["m", "n", "o"] });
    const lf2 = pl.DataFrame({ a: [2, 3, 4], c: ["p", "q", "r"] });

    // Register multiple DataFrames at once
    const ctx = pl.SQLContext().registerMany({ tbl1: lf1, tbl2: lf2 });
    const tables = ctx.tables();

    expect(tables).toEqual(expect.arrayContaining(["tbl1", "tbl2"]));
  });
  test("inspect", () => {
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: ["m", "n", "o"],
    });

    const ctx = pl.SQLContext({ df });
    const actual = ctx[Symbol.for("nodejs.util.inspect.custom")]();

    const expected = "SQLContext: {df}";

    expect(actual).toEqual(expected);
  });
  test("constructor with LazyFrames", () => {
    const lf1 = pl.DataFrame({ a: [1, 2, 3], b: ["m", "n", "o"] }).lazy();
    const lf2 = pl.DataFrame({ a: [2, 3, 4], c: ["p", "q", "r"] }).lazy();

    const ctx = pl.SQLContext({ tbl1: lf1, tbl2: lf2 });
    const tables = ctx.tables();
    expect(tables).toEqual(expect.arrayContaining(["tbl1", "tbl2"]));
  });
  test("unregister", () => {
    const df = pl.DataFrame({ hello: ["world"] });
    const df2 = pl.DataFrame({ hello: ["world"] });
    const df3 = pl.DataFrame({ hello: ["world"] });
    const ctx = pl.SQLContext({ df, df2, df3 });

    ctx.unregister("df");

    const tables = ctx.tables();
    expect(tables).toEqual(["df2", "df3"]);

    ctx.unregister(["df2", "df3"]);
    const tables2 = ctx.tables();
    expect(tables2).toEqual([]);
  });
});
