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
  setAsciiTables(active: boolean): Config;
  setAsciiTables(): Config;
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
  setTblWidthChars(width: number): Config;
  setTblWidthChars(): Config;
  /** Set the max number of rows used to draw the table (both Dataframe and Series).
   * @param n - Number of rows to display; if `n < 0` (eg: -1), display all rows (DataFrame) and all elements (Series).
   * 
   * @example
    const df = pl.DataFrame({abc: [1.0, 2.5, 5.0], xyz: [true, false, true]})
    pl.Config.setTblRows(2);
    
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
  setTblRows(n: number): Config;
  setTblRows(): Config;
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
  setTblCols(n?: number): Config;

  /**
   * Display the data type next to the column name (to the right, in parentheses).
   * @param active - true / false Default - true
   * 
   * @example
    --------
    const df = pl.DataFrame({abc: [1.0, 2.5, 5.0], xyz: [true, false, true]})
    pl.Config.setTblColumnDataTypeInline(true)

    # shape: (3, 2)        shape: (3, 2)
    # ┌─────┬───────┐      ┌───────────┬────────────┐
    # │ abc ┆ xyz   │      │ abc (f64) ┆ xyz (bool) │
    # │ --- ┆ ---   │      ╞═══════════╪════════════╡
    # │ f64 ┆ bool  │      │ 1.0       ┆ true       │
    # ╞═════╪═══════╡  >>  │ 2.5       ┆ false      │
    # │ 1.0 ┆ true  │      │ 5.0       ┆ true       │
    # │ 2.5 ┆ false │      └───────────┴────────────┘
    # │ 5.0 ┆ true  │
    # └─────┴───────┘
   */

  setTblColumnDataTypeInline(active?: boolean): Config;

  /**
   * Hide table column data types (i64, f64, str etc.).
   * @param active - true / false Default - true

   * @example
    const df = pl.DataFrame({abc: [1.0, 2.5, 5.0], xyz: [true, false, true]})
    pl.Config.setTblHideColumnDataTypes(true)
        
    # shape: (3, 2)        shape: (3, 2)
    # ┌─────┬───────┐      ┌─────┬───────┐
    # │ abc ┆ xyz   │      │ abc ┆ xyz   │
    # │ --- ┆ ---   │      ╞═════╪═══════╡
    # │ f64 ┆ bool  │      │ 1.0 ┆ true  │
    # ╞═════╪═══════╡  >>  │ 2.5 ┆ false │
    # │ 1.0 ┆ true  │      │ 5.0 ┆ true  │
    # │ 2.5 ┆ false │      └─────┴───────┘
    # │ 5.0 ┆ true  │
    # └─────┴───────┘
   */
  setTblHideColumnDataTypes(active?: boolean): Config;

  /**
   * Enable additional verbose/debug logging.
   * @param active - true / false Default - true
   */
  setVerbose(active?: boolean): Config;

  /**
   * Set the thousands grouping separator character.
   * @param separator : string | bool
        Set True to use the default "," (thousands) and "." (decimal) separators.
        Can also set a custom char, or set ``None`` to omit the separator.
   */
  setThousandsSeparator(separator?: string | boolean): Config;
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
  //   set_tbl_dataframe_shape_below
  //   set_tbl_formatting
  //   set_tbl_hide_column_names
  //   set_tbl_hide_dataframe_shape
  //   set_tbl_hide_dtype_separator
  //   set_trim_decimal_zeros
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
  setTblColumnDataTypeInline(active?: boolean) {
    pli.setTblColumnDataTypeInline(active ? 1 : 0);
    return this;
  },
  setTblHideColumnDataTypes(active?: boolean) {
    pli.setTblHideColumnDataTypes(active ? 1 : 0);
    return this;
  },
  setVerbose(active?: boolean) {
    pli.setVerbose(active ? 1 : 0);
    return this;
  },
  setThousandsSeparator(separator?: string | boolean) {
    if (typeof separator === "boolean" && separator) {
      pli.setDecimalSeparator(".");
      pli.setThousandsSeparator(",");
    } else if (typeof separator === "string") {
      if (separator.length > 1)
        throw new TypeError("separator must be a single character;");
      pli.setThousandsSeparator(separator);
    } else {
      pli.setThousandsSeparator();
    }
    return this;
  },
};
