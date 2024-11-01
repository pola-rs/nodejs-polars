use crate::dataframe::*;
use crate::export::JsLazyFrame;
use crate::lazy::dsl::JsExpr;
use polars::prelude::{
    concat, concat_lf_diagonal, concat_lf_horizontal, DataFrame, LazyFrame, UnionArgs,
};
use polars_core::functions as pl_functions;

#[napi(catch_unwind)]
pub fn horizontal_concat(dfs: Vec<&JsDataFrame>) -> napi::Result<JsDataFrame> {
    let dfs: Vec<DataFrame> = dfs.iter().map(|df| df.df.clone()).collect();
    let df =
        pl_functions::concat_df_horizontal(&dfs, true).map_err(crate::error::JsPolarsErr::from)?;
    Ok(df.into())
}

#[napi(catch_unwind)]
pub fn diagonal_concat(dfs: Vec<&JsDataFrame>) -> napi::Result<JsDataFrame> {
    let dfs: Vec<DataFrame> = dfs.iter().map(|df| df.df.clone()).collect();
    let df = pl_functions::concat_df_diagonal(&dfs).map_err(crate::error::JsPolarsErr::from)?;
    Ok(df.into())
}

#[napi(catch_unwind)]
pub fn concat_lf(
    ldfs: Vec<&JsLazyFrame>,
    how: Option<String>,
    rechunk: Option<bool>,
) -> napi::Result<JsLazyFrame> {
    let ldfs: Vec<LazyFrame> = ldfs.iter().map(|ldf| ldf.ldf.clone()).collect();

    let union_args = UnionArgs {
        rechunk: rechunk.unwrap_or(false),
        ..Default::default()
    };
    let ldf = match how.as_deref() {
        // Default to vertical
        None => concat(&ldfs, union_args),
        Some("vertical") => concat(&ldfs, union_args),
        Some("horizontal") => concat_lf_horizontal(&ldfs, union_args),
        Some("diagonal") => concat_lf_diagonal(
            &ldfs,
            UnionArgs {
                diagonal: true,
                ..union_args
            },
        ),
        Some(unknown) => {
            return Err(napi::Error::from_reason(format!(
                "Unknown concat method: {}",
                unknown
            )))
        }
    }
    .map_err(crate::error::JsPolarsErr::from)?;

    Ok(ldf.into())
}

#[napi(catch_unwind)]
pub fn arg_where(condition: &JsExpr) -> JsExpr {
    polars::lazy::dsl::arg_where(condition.inner.clone()).into()
}
