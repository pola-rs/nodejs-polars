import pl from "../polars";

describe("config", () => {
  pl.Config.setAsciiTables(true);

  test("setAsciiTables", () => {
    const df = pl.DataFrame({
      int: [1, 2],
      str: ["a", "b"],
      bool: [true, null],
      list: [[1, 2], [3]],
    });

    const asciiTable = `shape: (2, 4)
+-----+-----+------+------------+
| int | str | bool | list       |
| --- | --- | ---  | ---        |
| f64 | str | bool | list[f64]  |
+===============================+
| 1.0 | a   | true | [1.0, 2.0] |
| 2.0 | b   | null | [3.0]      |
+-----+-----+------+------------+`;

    pl.Config.setAsciiTables(true);
    assert.deepStrictEqual(df.toString(), asciiTable);

    pl.Config.setAsciiTables(false);

    const utf8Table = `shape: (2, 4)
┌─────┬─────┬──────┬────────────┐
│ int ┆ str ┆ bool ┆ list       │
│ --- ┆ --- ┆ ---  ┆ ---        │
│ f64 ┆ str ┆ bool ┆ list[f64]  │
╞═════╪═════╪══════╪════════════╡
│ 1.0 ┆ a   ┆ true ┆ [1.0, 2.0] │
│ 2.0 ┆ b   ┆ null ┆ [3.0]      │
└─────┴─────┴──────┴────────────┘`;

    assert.deepStrictEqual(df.toString(), utf8Table);
    pl.Config.setAsciiTables();
    assert.deepStrictEqual(df.toString(), utf8Table);
  });

  test("setTblWidthChars", () => {
    const df = pl.DataFrame({
      id: ["SEQ1", "SEQ2"],
      seq: ["ATGATAAAGGAG", "GCAACGCATATA"],
    });
    pl.Config.setTblWidthChars(12);
    assert.deepStrictEqual(df.toString().length, 209);
    pl.Config.setTblWidthChars();
    assert.deepStrictEqual(df.toString().length, 205);
  });
  test("setTblRows", () => {
    const df = pl.DataFrame({
      abc: [1.0, 2.5, 3.5, 5.0],
      xyz: [true, false, true, false],
    });
    pl.Config.setTblRows(2);
    assert.deepStrictEqual(df.toString().length, 157);
    pl.Config.setTblRows();
    assert.deepStrictEqual(df.toString().length, 173);
  });
  test("setTblCols", () => {
    const df = pl.DataFrame({
      abc: [1.0, 2.5, 3.5, 5.0],
      def: ["d", "e", "f", "g"],
      xyz: [true, false, true, false],
    });
    pl.Config.setTblCols(2);
    assert.deepStrictEqual(df.toString().length, 213);
    pl.Config.setTblCols();
    assert.deepStrictEqual(df.toString().length, 233);
    pl.Config.setTblCols(-1);
    assert.deepStrictEqual(df.toString().length, 233);
  });
  test("setTblColumnDataTypeInline", () => {
    const df = pl.DataFrame({
      abc: [1.0, 2.5, 3.5, 5.0],
      def: ["d", "e", "f", "g"],
      xyz: [true, false, true, false],
    });
    pl.Config.setTblColumnDataTypeInline(true);
    assert.deepStrictEqual(df.toString().length, 325);
    pl.Config.setTblColumnDataTypeInline();
    assert.deepStrictEqual(df.toString().length, 233);
  });
  test("setTblHideColumnDataTypes", () => {
    const df = pl.DataFrame({
      abc: [1.0, 2.5, 3.5, 5.0],
      def: ["d", "e", "f", "g"],
      xyz: [true, false, true, false],
    });
    pl.Config.setTblHideColumnDataTypes(true);
    assert.deepStrictEqual(df.toString().length, 189);
    pl.Config.setTblHideColumnDataTypes();
    assert.deepStrictEqual(df.toString().length, 233);
  });
  test("setVerbose", () => {
    pl.Config.setVerbose(true);
    pl.Config.setVerbose(false);
  });
  test("setThousandsSeparator", () => {
    const df = pl.DataFrame({
      x: [1234567, -987654, 10101],
      y: [1234.5, 100000.0, -7654321.25],
    });
    pl.Config.setThousandsSeparator(true);
    assert.deepStrictEqual(df.toString().length, 292);
    pl.Config.setThousandsSeparator("x");
    assert.deepStrictEqual(df.toString().length, 292);
    const fn = () => pl.Config.setThousandsSeparator("xx");
    assert.throws(fn, TypeError);
    pl.Config.setThousandsSeparator();
    assert.deepStrictEqual(df.toString().length, 256);
  });
});
