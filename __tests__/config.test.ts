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
});