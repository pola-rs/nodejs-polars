import pl from "@polars";

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
    expect(df.toString()).toEqual(asciiTable);

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

    expect(df.toString()).toEqual(utf8Table);
    pl.Config.setAsciiTables();
    expect(df.toString()).toEqual(utf8Table);
  });

  test("setTblWidthChars", () => {
    const df = pl.DataFrame({
      id: ["SEQ1", "SEQ2"],
      seq: ["ATGATAAAGGAG", "GCAACGCATATA"],
    });
    pl.Config.setTblWidthChars(12);
    expect(df.toString().length).toEqual(209);
    pl.Config.setTblWidthChars();
    expect(df.toString().length).toEqual(205);
  });
  test("setTblRows", () => {
    const df = pl.DataFrame({
      abc: [1.0, 2.5, 3.5, 5.0],
      xyz: [true, false, true, false],
    });
    pl.Config.setTblRows(2);
    expect(df.toString().length).toEqual(157);
    pl.Config.setTblRows();
    expect(df.toString().length).toEqual(173);
  });
  test("setTblCols", () => {
    const df = pl.DataFrame({
      abc: [1.0, 2.5, 3.5, 5.0],
      def: ["d", "e", "f", "g"],
      xyz: [true, false, true, false],
    });
    pl.Config.setTblCols(2);
    expect(df.toString().length).toEqual(213);
    pl.Config.setTblCols();
    expect(df.toString().length).toEqual(233);
    pl.Config.setTblCols(-1);
    expect(df.toString().length).toEqual(233);
  });
});
