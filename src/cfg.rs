/// Set the `POLARS_FMT_TABLE_FORMATTING` environment variable.
///
/// - `Some(true)`  => `"ASCII_FULL_CONDENSED"`
/// - `Some(false)` => `"UTF8_FULL_CONDENSED"`
/// - `None`        => remove the variable
#[napi(catch_unwind)]
pub fn set_ascii_tables(enabled: Option<bool>) {
    const ENV_VAR: &str = "POLARS_FMT_TABLE_FORMATTING";

    match enabled {
        Some(true) => std::env::set_var(ENV_VAR, "ASCII_FULL_CONDENSED"),
        Some(false) => std::env::set_var(ENV_VAR, "UTF8_FULL_CONDENSED"),
        None => std::env::remove_var(ENV_VAR),
    }
}

#[napi(catch_unwind)]
pub fn set_thousands_separator(sep: Option<String>) {
    use polars_core::fmt::set_thousands_separator;

    let sep = sep.map_or(None, |q| if q.is_empty() { None } else { q.chars().next() });

    set_thousands_separator(sep);
}

#[napi(catch_unwind)]
pub fn set_decimal_separator(sep: Option<String>) {
    use polars_core::fmt::set_decimal_separator;

    let sep = sep.map_or(None, |q| if q.is_empty() { None } else { q.chars().next() });

    set_decimal_separator(sep);
}

macro_rules! set_var {
    ($name:ident, $var_name:expr) => {
        #[napi(catch_unwind)]
        pub fn $name(n: Option<i32>) {
            match n {
                Some(w) => std::env::set_var($var_name, w.to_string()),
                None => std::env::remove_var($var_name),
            }
        }
    };
}
set_var!(set_verbose, "POLARS_VERBOSE");
set_var!(set_tbl_rows, "POLARS_FMT_MAX_ROWS");
set_var!(set_tbl_cols, "POLARS_FMT_MAX_COLS");
set_var!(set_tbl_width_chars, "POLARS_TABLE_WIDTH");
set_var!(
    set_tbl_column_data_type_inline,
    "POLARS_FMT_TABLE_INLINE_COLUMN_DATA_TYPE"
);
set_var!(
    set_tbl_hide_column_data_types,
    "POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES"
);
