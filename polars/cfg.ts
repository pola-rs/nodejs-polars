import pli from "./internals/polars_internal";

/**
 * Configure polars; offers options for table formatting and more.
 */
export interface Config {
  /** Use ASCII characters to display table outlines.
   * @param active - Set False to revert to the standard UTF8_FULL_CONDENSED formatting style. Default: true
   * 
   * @example
   * const df = pl.DataFrame({abc: [1.0, 2.5, 5.0], xyz: [true, false, true]})
     pl.Config.setAsciiTables(true)
     console.log(df.toString());
    # shape: (3, 2)        shape: (3, 2)
    # ┌─────┬───────┐      +-----+-------+
    # │ abc ┆ xyz   │      | abc | xyz   |
    # │ --- ┆ ---   │      | --- | ---   |
    # │ f64 ┆ bool  │      | f64 | bool  |
    # ╞═════╪═══════╡      +=============+
    # │ 1.0 ┆ true  │  >>  | 1.0 | true  |
    # │ 2.5 ┆ false │      | 2.5 | false |
    # │ 5.0 ┆ true  │      | 5.0 | true  |
    # └─────┴───────┘      +-----+-------+
   */
  setAsciiTables(): Config;
  setAsciiTables(active: boolean): Config;
  /** Set the maximum width of a table in characters.
   * @param width - Maximum table width in characters; if n < 0 (eg: -1), display full width.
   * 
   * @example
   * const df = pl.DataFrame({ id: ["SEQ1", "SEQ2"], seq: ["ATGATAAAGGAG", "GCAACGCATATA"] });
      >>> df
      shape: (2, 2)
      ┌──────┬──────────────┐
      │ id   ┆ seq          │
      │ ---  ┆ ---          │
      │ str  ┆ str          │
      ╞══════╪══════════════╡
      │ SEQ1 ┆ ATGATAAAGGAG │
      │ SEQ2 ┆ GCAACGCATATA │
      └──────┴──────────────┘
      >>> pl.Config.setTblWidthChars(12);
      >>> df
      shape: (2, 2)
      ┌─────┬─────┐
      │ id  ┆ seq │
      │ --- ┆ --- │
      │ str ┆ str │
      ╞═════╪═════╡
      │ SEQ ┆ ATG │
      │ 1   ┆ ATA │
      │     ┆ AAG │
      │     ┆ GAG │
      │ SEQ ┆ GCA │
      │ 2   ┆ ACG │
      │     ┆ CAT │
      │     ┆ ATA │
      └─────┴─────┘
  */
  setTblWidthChars(): Config;
  setTblWidthChars(width: number): Config;
  /** Set the max number of rows used to draw the table (both Dataframe and Series).
   * @param n - Number of rows to display; if `n < 0` (eg: -1), display all rows (DataFrame) and all elements (Series).
   * 
   * @example
    const df = pl.DataFrame( {abc: [1.0, 2.5, 3.5, 5.0], xyz: [True, False, True, False]} );
    pl.Config.setTblRows(2);
    console.log(df.toString());
    shape: (4, 2)
    ┌─────┬───────┐
    │ abc ┆ xyz   │
    │ --- ┆ ---   │
    │ f64 ┆ bool  │
    ╞═════╪═══════╡
    │ 1.0 ┆ true  │
    │ …   ┆ …     │
    │ 5.0 ┆ false │
    └─────┴───────┘
  */
  setTblRows(): Config;
  setTblRows(n: number): Config;
  /** Set the number of columns that are visible when displaying tables.
   * @param n -  Number of columns to display; if `n < 0` (eg: -1), display all columns.
   * 
   * @example

    const df = pl.DataFrame( {abc: [1.0, 2.5, 3.5, 5.0], def: ["d", "e", "f", "g"], xyz: [true, false, true, false] } );
    // Set number of displayed columns to a low value
    pl.Config.setTblCols(2);
    shape: (4, 3)
    ┌─────┬───┬───────┐
    │ abc ┆ … ┆ xyz   │
    │ --- ┆   ┆ ---   │
    │ f64 ┆   ┆ bool  │
    ╞═════╪═══╪═══════╡
    │ 1.0 ┆ … ┆ true  │
    │ 2.5 ┆ … ┆ false │
    │ 3.5 ┆ … ┆ true  │
    │ 5.0 ┆ … ┆ false │
    └─────┴───┴───────┘
  */
  setTblCols(): Config;
  setTblCols(n: number): Config;

  // TODO: Implement these methods
  // set_auto_structify
  // set_decimal_separator
  // set_engine_affinity
  //   set_float_precision
  //   set_fmt_float
  //   set_fmt_str_lengths
  //   set_fmt_table_cell_list_len
  //   set_streaming_chunk_size
  //   set_tbl_cell_alignment
  //   set_tbl_cell_numeric_alignment
  //   set_tbl_column_data_type_inline
  //   set_tbl_dataframe_shape_below
  //   set_tbl_formatting
  //   set_tbl_hide_column_data_types
  //   set_tbl_hide_column_names
  //   set_tbl_hide_dataframe_shape
  //   set_tbl_hide_dtype_separator
  //   set_thousands_separator
  //   set_trim_decimal_zeros
  //   set_verbose
}

export const Config: Config = {
  setAsciiTables(active?: boolean) {
    pli.setAsciiTables(active);
    return this;
  },
  setTblWidthChars(width?: number) {
    pli.setTblWidthChars(width);
    return this;
  },
  setTblRows(n?: number) {
    pli.setTblRows(n);
    return this;
  },
  setTblCols(n?: number) {
    pli.setTblCols(n);
    return this;
  },
};
