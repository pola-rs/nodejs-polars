import pli from "./internals/polars_internal";

/**
 * Configure polars; offers options for table formatting and more.
 */
export interface Config {
  /** Use utf8 characters to print tables */
  setUtf8Tables(): Config;
  /** Use ascii characters to print tables */
  setAsciiTables(): Config;
  setAsciiTables(active: boolean): Config;
  /** Set the number of character used to draw the table */
  setTblWidthChars(width: number): Config;
  /** Set the number of rows used to print tables */
  setTblRows(n: number): Config;
  /** Set the number of columns used to print tables */
  setTblCols(n: number): Config;

  // set_auto_structify
  // set_decimal_separator
  //   set_engine_affinity
  //   set_float_precision
  //   set_fmt_float
  //   set_fmt_str_lengths
  //   set_fmt_table_cell_list_len
  //   set_streaming_chunk_size
  //   set_tbl_cell_alignment
  //   set_tbl_cell_numeric_alignment
  //   set_tbl_cols
  //   set_tbl_column_data_type_inline
  //   set_tbl_dataframe_shape_below
  //   set_tbl_formatting
  //   set_tbl_hide_column_data_types
  //   set_tbl_hide_column_names
  //   set_tbl_hide_dataframe_shape
  //   set_tbl_hide_dtype_separator
  //   set_tbl_rows
  //   set_tbl_width_chars
  //   set_thousands_separator
  //   set_trim_decimal_zeros
  //   set_verbose

}

/**
 * @ignore
 */
export const Config: Config = {
  setUtf8Tables() {
    process.env["POLARS_FMT_NO_UTF8"] = undefined;
    return this;
  },
  setAsciiTables(active?: boolean) {
    pli.setAsciiTables(active);
    return this;
  },
  setTblWidthChars(width) {
    process.env["POLARS_TABLE_WIDTH"] = String(width);
    return this;
  },
  setTblRows(n) {
    process.env["POLARS_FMT_MAX_ROWS"] = String(n);
    return this;
  },
  setTblCols(n) {
    process.env["POLARS_FMT_MAX_COLS"] = String(n);
    return this;
  },
};
