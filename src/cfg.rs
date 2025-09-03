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

/// Set the `POLARS_TABLE_WIDTH` environment variable.
#[napi(catch_unwind)]
pub fn set_tbl_width_chars(width: Option<i32>) {
    const ENV_VAR: &str = "POLARS_TABLE_WIDTH";

    match width {
        Some(w) => std::env::set_var(ENV_VAR, w.to_string()),
        None => std::env::remove_var(ENV_VAR),
    }
}

/// Set the `POLARS_FMT_MAX_ROWS` environment variable.
#[napi(catch_unwind)]
pub fn set_tbl_rows(n: Option<i32>) {
    const ENV_VAR: &str = "POLARS_FMT_MAX_ROWS";

    match n {
        Some(w) => std::env::set_var(ENV_VAR, w.to_string()),
        None => std::env::remove_var(ENV_VAR),
    }
}

/// Set the `POLARS_FMT_MAX_COLS` environment variable.
#[napi(catch_unwind)]
pub fn set_tbl_cols(n: Option<i32>) {
    const ENV_VAR: &str = "POLARS_FMT_MAX_COLS";

    match n {
        Some(w) => std::env::set_var(ENV_VAR, w.to_string()),
        None => std::env::remove_var(ENV_VAR),
    }
}
