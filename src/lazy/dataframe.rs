use crate::dataframe::JsDataFrame;
use crate::lazy::dsl::{JsExpr, ToExprs};
use crate::prelude::*;
use polars::prelude::{lit, ClosedWindow, JoinType};
use polars_io::{HiveOptions, RowIndex};
use polars_utils::slice_enum::Slice;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::ops::BitOrAssign;

#[napi]
#[repr(transparent)]
pub struct JsLazyGroupBy {
    // option because we cannot get a self by value in pyo3
    lgb: Option<LazyGroupBy>,
}

#[napi]
#[repr(transparent)]
#[derive(Clone)]
pub struct JsLazyFrame {
    pub(crate) ldf: LazyFrame,
}
impl From<LazyFrame> for JsLazyFrame {
    fn from(ldf: LazyFrame) -> Self {
        JsLazyFrame { ldf }
    }
}

#[napi]
impl JsLazyGroupBy {
    #[napi(catch_unwind)]
    pub fn agg(&mut self, aggs: Vec<&JsExpr>) -> JsLazyFrame {
        let lgb = self.lgb.take().unwrap();
        lgb.agg(aggs.to_exprs()).into()
    }
    #[napi(catch_unwind)]
    pub fn head(&mut self, n: i64) -> JsLazyFrame {
        let lgb = self.lgb.take().unwrap();
        lgb.head(Some(n as usize)).into()
    }
    #[napi(catch_unwind)]
    pub fn tail(&mut self, n: i64) -> JsLazyFrame {
        let lgb = self.lgb.take().unwrap();
        lgb.tail(Some(n as usize)).into()
    }
}

fn bin_config() -> bincode::config::Configuration {
    bincode::config::standard()
        .with_no_limit()
        .with_variable_int_encoding()
}

#[napi]
impl JsLazyFrame {
    #[napi(catch_unwind)]
    pub fn to_js(&self, env: Env) -> napi::Result<napi::Unknown<'_>> {
        env.to_js_value(&self.ldf.logical_plan)
    }

    #[napi(catch_unwind)]
    pub fn serialize(&self, format: String) -> napi::Result<Buffer> {
        let buf = match format.as_ref() {
            "bincode" => bincode::serde::encode_to_vec(&self.ldf.logical_plan, bin_config())
                .map_err(|err| napi::Error::from_reason(err.to_string()))?,
            "json" => serde_json::to_vec(&self.ldf.logical_plan)
                .map_err(|err| napi::Error::from_reason(err.to_string()))?,
            _ => {
                return Err(napi::Error::from_reason(
                    "unexpected format. \n supported options are 'json', 'bincode'".to_owned(),
                ))
            }
        };
        Ok(Buffer::from(buf))
    }

    #[napi(factory, catch_unwind)]
    pub fn deserialize(buf: Buffer, format: String) -> napi::Result<JsLazyFrame> {
        let lp: DslPlan = match format.as_ref() {
            "bincode" => {
                bincode::serde::decode_from_slice(&buf, bin_config())
                    .map_err(|err| napi::Error::from_reason(err.to_string()))
                    .unwrap()
                    .0
            }
            "json" => serde_json::from_slice(&buf)
                .map_err(|err| napi::Error::from_reason(err.to_string()))?,
            _ => {
                return Err(napi::Error::from_reason(
                    "unexpected format. \n supported options are 'json', 'bincode'".to_owned(),
                ))
            }
        };
        Ok(LazyFrame::from(lp).into())
    }
    #[napi(factory, catch_unwind)]
    pub fn clone_external(lf: &JsLazyFrame) -> napi::Result<JsLazyFrame> {
        Ok(lf.clone())
    }
    #[napi(catch_unwind)]
    pub fn describe_plan(&self) -> napi::Result<String> {
        let result = self.ldf.describe_plan().map_err(JsPolarsErr::from)?;
        Ok(result)
    }
    #[napi(catch_unwind)]
    pub fn describe_optimized_plan(&self) -> napi::Result<String> {
        let result = self
            .ldf
            .describe_optimized_plan()
            .map_err(JsPolarsErr::from)?;
        Ok(result)
    }
    #[napi(catch_unwind)]
    pub fn to_dot(&self, optimized: bool) -> napi::Result<String> {
        let result = self.ldf.to_dot(optimized).map_err(JsPolarsErr::from)?;
        Ok(result)
    }
    #[napi(catch_unwind)]
    pub fn optimization_toggle(
        &self,
        type_coercion: Option<bool>,
        predicate_pushdown: Option<bool>,
        projection_pushdown: Option<bool>,
        simplify_expr: Option<bool>,
        slice_pushdown: Option<bool>,
        comm_subplan_elim: Option<bool>,
        comm_subexpr_elim: Option<bool>,
        streaming: Option<bool>,
    ) -> JsLazyFrame {
        let type_coercion = type_coercion.unwrap_or(true);
        let predicate_pushdown = predicate_pushdown.unwrap_or(true);
        let projection_pushdown = projection_pushdown.unwrap_or(true);
        let simplify_expr = simplify_expr.unwrap_or(true);
        let slice_pushdown = slice_pushdown.unwrap_or(true);
        let comm_subplan_elim = comm_subplan_elim.unwrap_or(true);
        let comm_subexpr_elim = comm_subexpr_elim.unwrap_or(true);
        let streaming = streaming.unwrap_or(false);

        let ldf = self.ldf.clone();
        let ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            .with_new_streaming(streaming)
            .with_projection_pushdown(projection_pushdown)
            .with_comm_subplan_elim(comm_subplan_elim)
            .with_comm_subexpr_elim(comm_subexpr_elim);

        ldf.into()
    }
    #[napi(catch_unwind)]
    pub fn sort(
        &self,
        by_column: String,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
    ) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.sort(
            [&by_column],
            SortMultipleOptions::default()
                .with_order_descending(descending)
                .with_nulls_last(nulls_last)
                .with_maintain_order(maintain_order),
        )
        .into()
    }
    #[napi(catch_unwind)]
    pub fn sort_by_exprs(
        &self,
        by_column: Vec<&JsExpr>,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
    ) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.sort_by_exprs(
            by_column.to_exprs(),
            SortMultipleOptions::default()
                .with_order_descending(descending)
                .with_nulls_last(nulls_last)
                .with_maintain_order(maintain_order),
        )
        .into()
    }
    #[napi(catch_unwind)]
    pub fn cache(&self) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.cache().into()
    }
    #[napi(catch_unwind)]
    pub fn collect_sync(&self) -> napi::Result<JsDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.collect().map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(ts_return_type = "Promise<JsDataFrame>", catch_unwind)]
    pub fn collect(&self) -> AsyncTask<AsyncCollect> {
        let ldf = self.ldf.clone();
        AsyncTask::new(AsyncCollect(ldf))
    }

    #[napi(ts_return_type = "Promise<JsDataFrame>", catch_unwind)]
    pub fn fetch(&self, n_rows: u32) -> AsyncTask<AsyncFetch> {
        let ldf = self.ldf.clone();
        AsyncTask::new(AsyncFetch((ldf, n_rows)))
    }

    #[napi(catch_unwind)]
    pub fn fetch_sync(&self, n_rows: u32) -> napi::Result<JsDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.limit(n_rows).collect().map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn filter(&mut self, predicate: &JsExpr) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.filter(predicate.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn select(&mut self, exprs: Vec<&JsExpr>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.select(exprs.to_exprs()).into()
    }
    #[napi(catch_unwind)]
    pub fn groupby(&mut self, by: Vec<&JsExpr>, maintain_order: bool) -> JsLazyGroupBy {
        let ldf = self.ldf.clone();
        let by = by.to_exprs();
        let lazy_gb = if maintain_order {
            ldf.group_by_stable(by)
        } else {
            ldf.group_by(by)
        };

        JsLazyGroupBy { lgb: Some(lazy_gb) }
    }
    #[napi(catch_unwind)]
    pub fn groupby_rolling(
        &mut self,
        index_column: &JsExpr,
        period: String,
        offset: String,
        closed: Wrap<ClosedWindow>,
        by: Vec<&JsExpr>,
    ) -> JsLazyGroupBy {
        let closed_window = closed.0;
        let ldf = self.ldf.clone();
        let by = by.to_exprs();
        let lazy_gb = ldf.rolling(
            index_column.inner.clone(),
            by,
            RollingGroupOptions {
                index_column: "".into(),
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                closed_window,
            },
        );

        JsLazyGroupBy { lgb: Some(lazy_gb) }
    }

    #[napi(catch_unwind)]
    pub fn groupby_dynamic(
        &mut self,
        index_column: &JsExpr,
        every: String,
        period: String,
        offset: String,
        label: Wrap<Label>,
        include_boundaries: bool,
        closed: Wrap<ClosedWindow>,
        by: Vec<&JsExpr>,
        start_by: Wrap<StartBy>,
    ) -> Result<JsLazyGroupBy> {
        let closed_window = closed.0;
        let by = by.to_exprs();
        let ldf = self.ldf.clone();
        let lazy_gb: LazyGroupBy = ldf.group_by_dynamic(
            index_column.inner.clone(),
            by,
            DynamicGroupOptions {
                every: Duration::try_parse(&every).map_err(JsPolarsErr::from)?,
                period: Duration::try_parse(&period).map_err(JsPolarsErr::from)?,
                offset: Duration::try_parse(&offset).map_err(JsPolarsErr::from)?,
                label: label.0,
                include_boundaries,
                closed_window,
                start_by: start_by.0,
                ..Default::default()
            },
        );

        Ok(JsLazyGroupBy { lgb: Some(lazy_gb) })
    }
    #[allow(clippy::too_many_arguments)]
    #[napi(catch_unwind)]
    pub fn join_asof(
        &self,
        other: &JsLazyFrame,
        left_on: &JsExpr,
        right_on: &JsExpr,
        left_by: Option<Vec<String>>,
        right_by: Option<Vec<String>>,
        allow_parallel: bool,
        force_parallel: bool,
        suffix: String,
        strategy: String,
        tolerance: Option<Wrap<AnyValue<'_>>>,
        tolerance_str: Option<String>,
        check_sortedness: bool,
    ) -> JsLazyFrame {
        let strategy = match strategy.as_ref() {
            "forward" => AsofStrategy::Forward,
            "backward" => AsofStrategy::Backward,
            "nearest" => AsofStrategy::Nearest,
            _ => panic!("expected one of {{'forward', 'backward', 'nearest'}}"),
        };
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = left_on.inner.clone();
        let right_on = right_on.inner.clone();
        ldf.join_builder()
            .with(other)
            .left_on([left_on])
            .right_on([right_on])
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(JoinType::AsOf(Box::new(AsOfOptions {
                strategy,
                left_by: left_by.map(strings_to_pl_smallstr),
                right_by: right_by.map(strings_to_pl_smallstr),
                tolerance: tolerance.map(|t| {
                    let av = t.0.into_static();
                    let dtype = av.dtype();
                    Scalar::new(dtype, av)
                }),
                tolerance_str: tolerance_str.map(|s| s.into()),
                allow_eq: true,
                check_sortedness,
            })))
            .suffix(suffix)
            .finish()
            .into()
    }
    #[allow(clippy::too_many_arguments)]
    #[napi(catch_unwind)]
    pub fn join(
        &self,
        other: &JsLazyFrame,
        left_on: Vec<&JsExpr>,
        right_on: Vec<&JsExpr>,
        allow_parallel: bool,
        force_parallel: bool,
        how: Wrap<JoinType>,
        suffix: String,
        coalesce: Option<bool>,
    ) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = left_on.to_exprs();
        let right_on = right_on.to_exprs();

        let coalesce = match (&how.0, coalesce) {
            (JoinType::Full, None) => JoinCoalesce::KeepColumns,
            (_, Some(false)) => JoinCoalesce::KeepColumns,
            _ => JoinCoalesce::CoalesceColumns, // Default is true
        };

        ldf.join_builder()
            .with(other)
            .left_on(left_on)
            .right_on(right_on)
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(how.0)
            .suffix(suffix)
            .coalesce(coalesce)
            .finish()
            .into()
    }
    #[napi(catch_unwind)]
    pub fn with_column(&mut self, expr: &JsExpr) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.with_column(expr.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn with_columns(&mut self, exprs: Vec<&JsExpr>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.with_columns(exprs.to_exprs()).into()
    }
    #[napi(catch_unwind)]
    pub fn rename(&mut self, existing: Vec<String>, new_names: Vec<String>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.rename(existing, new_names, true).into()
    }
    #[napi(catch_unwind)]
    pub fn reverse(&self) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.reverse().into()
    }
    #[napi(catch_unwind)]
    pub fn shift(&self, periods: i64) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.shift(periods).into()
    }
    #[napi(catch_unwind)]
    pub fn shift_and_fill(&self, periods: i64, fill_value: i64) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.shift_and_fill(periods, fill_value).into()
    }

    #[napi(catch_unwind)]
    pub fn fill_null(&self, fill_value: &JsExpr) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.fill_null(fill_value.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn fill_nan(&self, fill_value: &JsExpr) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.fill_nan(fill_value.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn min(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.min();
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn max(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.max();
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn sum(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.sum();
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn mean(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.mean();
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn std(&self, ddof: Option<u8>) -> napi::Result<JsLazyFrame> {
        let ddof = ddof.unwrap_or(1);
        let ldf = self.ldf.clone();
        let out = ldf.std(ddof);
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn var(&self, ddof: Option<u8>) -> napi::Result<JsLazyFrame> {
        let ddof = ddof.unwrap_or(1);
        let ldf = self.ldf.clone();
        let out = ldf.var(ddof);
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn median(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.median();
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileMethod>,
    ) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.quantile(lit(quantile), interpolation.0);
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn explode(&self, column: Vec<&JsExpr>) -> JsLazyFrame {
        let mut column_selector: Selector = Selector::Empty;
        column.to_exprs().into_iter().for_each(|expr| {
            column_selector.bitor_assign(expr.into_selector().unwrap().into());
        });
        self.ldf.clone().explode(column_selector).into()
    }
    #[napi(catch_unwind)]
    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<Vec<String>>,
        keep: Wrap<UniqueKeepStrategy>,
    ) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        match maintain_order {
            true => ldf.unique_stable(subset.map(|x| strings_to_selector(x)), keep.0),
            false => ldf.unique(subset.map(|x| strings_to_selector(x)), keep.0),
        }
        .into()
    }
    #[napi(catch_unwind)]
    pub fn drop_nulls(&self, subset: Option<Vec<String>>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.drop_nulls(subset.map(|v| strings_to_selector(v)))
            .into()
    }
    #[napi(catch_unwind)]
    pub fn slice(&self, offset: i64, length: u32) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.slice(offset, length).into()
    }
    #[napi(catch_unwind)]
    pub fn tail(&self, n: u32) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.tail(n).into()
    }
    #[napi(catch_unwind)]
    pub fn unpivot(
        &self,
        id_vars: Vec<String>,
        value_vars: Vec<String>,
        variable_name: Option<String>,
        value_name: Option<String>,
    ) -> JsLazyFrame {
        let args = UnpivotArgsDSL {
            on: strings_to_selector(value_vars),
            index: strings_to_selector(id_vars),
            variable_name: variable_name.map(|s| s.into()),
            value_name: value_name.map(|s| s.into()),
        };
        let ldf = self.ldf.clone();
        ldf.unpivot(args).into()
    }

    #[napi(catch_unwind)]
    pub fn with_row_count(&self, name: String, offset: Option<u32>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.with_row_index(&name, offset).into()
    }

    #[napi(catch_unwind)]
    pub fn drop_columns(&self, colss: Vec<String>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.drop(strings_to_selector(colss)).into()
    }
    #[napi(js_name = "clone", catch_unwind)]
    pub fn clone(&self) -> JsLazyFrame {
        self.ldf.clone().into()
    }

    #[napi(getter, js_name = "columns", catch_unwind)]
    pub fn columns(&mut self) -> napi::Result<Vec<String>> {
        Ok(self
            .ldf
            .collect_schema()
            .map_err(JsPolarsErr::from)?
            .iter_names()
            .map(|s| s.as_str().into())
            .collect())
    }

    #[napi(catch_unwind)]
    pub fn unnest(&self, colss: Vec<String>) -> JsLazyFrame {
        self.ldf
            .clone()
            .unnest(strings_to_selector(colss), Some(PlSmallStr::EMPTY))
            .into()
    }

    #[napi(catch_unwind)]
    pub fn sink_csv(
        &self,
        path: String,
        options: Wrap<CsvWriterOptions>,
        sink_options: JsSinkOptions,
        cloud_options: Option<HashMap<String, String>>,
        max_retries: Option<u32>,
    ) -> napi::Result<JsLazyFrame> {
        let cloud_options = parse_cloud_options(&path, cloud_options, max_retries);
        let sink_target = SinkTarget::Path(PlPath::new(&path));
        let ldf = self.ldf.clone().with_comm_subplan_elim(false);
        let rldf = ldf
            .sink_csv(sink_target, options.0, cloud_options, sink_options.into())
            .map_err(JsPolarsErr::from)?;
        Ok(rldf.into())
    }

    #[napi(catch_unwind)]
    pub fn sink_parquet(
        &self,
        path: String,
        options: SinkParquetOptions,
    ) -> napi::Result<JsLazyFrame> {
        let compression_str = options.compression.unwrap_or("zstd".to_string());
        let compression = parse_parquet_compression(compression_str, options.compression_level)?;
        let statistics = if options.statistics.expect("Expect statistics") {
            StatisticsOptions::full()
        } else {
            StatisticsOptions::empty()
        };
        let row_group_size = options.row_group_size.map(|i| i as usize);
        let data_page_size = options.data_pagesize_limit.map(|i| i as usize);
        let cloud_options = parse_cloud_options(&path, options.cloud_options, options.retries);

        let sink_options = options.sink_options.into();
        let options = ParquetWriteOptions {
            compression,
            statistics,
            row_group_size,
            data_page_size,
            key_value_metadata: None,
            field_overwrites: Vec::new(),
        };

        let sink_target = SinkTarget::Path(PlPath::new(&path));
        let ldf = self.ldf.clone().with_comm_subplan_elim(false);
        let rldf = ldf
            .sink_parquet(sink_target, options, cloud_options, sink_options)
            .map_err(JsPolarsErr::from)?;
        Ok(rldf.into())
    }

    #[napi(catch_unwind)]
    pub fn sink_json(&self, path: String, options: SinkJsonOptions) -> napi::Result<JsLazyFrame> {
        let cloud_options = parse_cloud_options(&path, options.cloud_options, options.retries);
        let sink_options: SinkOptions = SinkOptions {
            maintain_order: options.maintain_order.unwrap_or(true),
            sync_on_close: options.sync_on_close.0,
            mkdir: options.mkdir.unwrap_or(true),
        };
        let sink_target = SinkTarget::Path(PlPath::new(&path));
        let rldf = self
            .ldf
            .clone()
            .sink_json(
                sink_target,
                JsonWriterOptions {},
                cloud_options,
                sink_options,
            )
            .map_err(JsPolarsErr::from)?;
        Ok(rldf.into())
    }

    #[napi(catch_unwind)]
    pub fn sink_ipc(&self, path: String, options: SinkIpcOptions) -> napi::Result<JsLazyFrame> {
        let cloud_options = parse_cloud_options(&path, options.cloud_options, options.retries);
        let sink_options: SinkOptions = SinkOptions {
            maintain_order: options.maintain_order.unwrap_or(true),
            sync_on_close: options.sync_on_close.0,
            mkdir: options.mkdir.unwrap_or(true),
        };

        let compat_level: CompatLevel = match options.compat_level.unwrap().as_str() {
            "newest" => CompatLevel::newest(),
            "oldest" => CompatLevel::oldest(),
            _ => {
                return Err(napi::Error::from_reason(
                    "use one of {'newest', 'oldest'}".to_owned(),
                ))
            }
        };

        let ipc_options = IpcWriterOptions {
            compression: options.compression.0,
            compat_level: compat_level,
            ..Default::default()
        };

        let sink_target = SinkTarget::Path(PlPath::new(&path));
        let rldf = self
            .ldf
            .clone()
            .sink_ipc(sink_target, ipc_options, cloud_options, sink_options)
            .map_err(JsPolarsErr::from)?;
        Ok(rldf.into())
    }
}

#[napi(object)]
pub struct ScanCsvOptions {
    pub infer_schema_length: Option<u32>,
    pub cache: Option<bool>,
    pub overwrite_dtype: Option<HashMap<String, Wrap<DataType>>>,
    pub overwrite_dtype_slice: Option<Vec<Wrap<DataType>>>,
    pub has_header: Option<bool>,
    pub ignore_errors: bool,
    pub n_rows: Option<u32>,
    pub skip_rows: Option<u32>,
    pub sep: Option<String>,
    pub rechunk: Option<bool>,
    pub columns: Option<Vec<String>>,
    pub encoding: String,
    pub low_memory: Option<bool>,
    pub comment_prefix: Option<String>,
    pub eol_char: String,
    pub quote_char: Option<String>,
    pub parse_dates: Option<bool>,
    pub skip_rows_after_header: u32,
    pub row_count: Option<JsRowCount>,
    pub null_values: Option<Wrap<NullValues>>,
    pub missing_utf8_is_empty_string: Option<bool>,
    pub raise_if_empty: Option<bool>,
    pub truncate_ragged_lines: Option<bool>,
    pub schema: Option<Wrap<Schema>>,
}
#[napi(catch_unwind)]
pub fn scan_csv(path: String, options: ScanCsvOptions) -> napi::Result<JsLazyFrame> {
    let n_rows = options.n_rows.map(|i| i as usize);
    let row_count = options.row_count.map(RowIndex::from);
    let missing_utf8_is_empty_string: bool = options.missing_utf8_is_empty_string.unwrap_or(false);
    let quote_char = options.quote_char.map_or(None, |q| {
        if q.is_empty() {
            None
        } else {
            Some(q.as_bytes()[0])
        }
    });

    let overwrite_dtype = options.overwrite_dtype.map(|overwrite_dtype| {
        overwrite_dtype
            .iter()
            .map(|(name, dtype)| {
                let dtype = dtype.0.clone();
                Field::new(name.into(), dtype)
            })
            .collect::<Schema>()
    });

    let encoding = match options.encoding.as_ref() {
        "utf8" => CsvEncoding::Utf8,
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        e => return Err(JsPolarsErr::Other(format!("encoding not {} not implemented.", e)).into()),
    };

    let r = LazyCsvReader::new(PlPath::new(&path))
        .with_infer_schema_length(Some(options.infer_schema_length.unwrap_or(100) as usize))
        .with_separator(options.sep.unwrap_or(",".to_owned()).as_bytes()[0])
        .with_has_header(options.has_header.unwrap_or(true))
        .with_ignore_errors(options.ignore_errors)
        .with_skip_rows(options.skip_rows.unwrap_or(0) as usize)
        .with_n_rows(n_rows)
        .with_cache(options.cache.unwrap_or(true))
        .with_dtype_overwrite(overwrite_dtype.map(Arc::new))
        .with_schema(options.schema.map(|schema| Arc::new(schema.0)))
        .with_low_memory(options.low_memory.unwrap_or(false))
        .with_comment_prefix(
            options
                .comment_prefix
                .map_or(None, |s| Some(PlSmallStr::from_string(s))),
        )
        .with_quote_char(quote_char)
        .with_eol_char(options.eol_char.as_bytes()[0])
        .with_rechunk(options.rechunk.unwrap_or(false))
        .with_skip_rows_after_header(options.skip_rows_after_header as usize)
        .with_encoding(encoding)
        .with_row_index(row_count)
        .with_try_parse_dates(options.parse_dates.unwrap_or(false))
        .with_null_values(options.null_values.map(|s| s.0))
        .with_missing_is_null(!missing_utf8_is_empty_string)
        .with_truncate_ragged_lines(options.truncate_ragged_lines.unwrap_or(false))
        .with_raise_if_empty(options.raise_if_empty.unwrap_or(true))
        .finish()
        .map_err(JsPolarsErr::from)?;
    Ok(r.into())
}

#[napi(catch_unwind)]
pub fn scan_parquet(path: String, options: ScanParquetOptions) -> napi::Result<JsLazyFrame> {
    let n_rows = options.n_rows.map(|i| i as usize);
    let cache = options.cache.unwrap_or(true);
    let glob = options.glob.unwrap_or(true);
    let parallel = options.parallel;

    let row_index: Option<RowIndex> = if let Some(idn) = options.row_index_name {
        Some(RowIndex {
            name: idn.into(),
            offset: options.row_index_offset.unwrap_or(0),
        })
    } else {
        None
    };

    let rechunk = options.rechunk.unwrap_or(false);
    let low_memory = options.low_memory.unwrap_or(false);
    let use_statistics = options.use_statistics.unwrap_or(false);

    let cloud_options = parse_cloud_options(&path, options.cloud_options, options.retries);
    let hive_schema = options.hive_schema.map(|s| Arc::new(s.0));
    let schema = options.schema.map(|s| Arc::new(s.0));
    let hive_options = HiveOptions {
        enabled: options.hive_partitioning,
        hive_start_idx: 0,
        schema: hive_schema,
        try_parse_dates: options.try_parse_hive_dates.unwrap_or(true),
    };

    let include_file_paths = options.include_file_paths;
    let allow_missing_columns = options.allow_missing_columns.unwrap_or(false);

    let args = ScanArgsParquet {
        n_rows,
        cache,
        parallel: parallel.0,
        rechunk,
        row_index,
        schema,
        low_memory,
        cloud_options,
        use_statistics,
        hive_options,
        glob,
        include_file_paths: include_file_paths.map(PlSmallStr::from),
        allow_missing_columns,
    };
    let lf = LazyFrame::scan_parquet(PlPath::new(&path), args).map_err(JsPolarsErr::from)?;
    Ok(lf.into())
}

#[napi(object)]
pub struct ScanIPCOptions {
    pub n_rows: Option<i64>,
    pub cache: Option<bool>,
    pub rechunk: Option<bool>,
    pub row_count: Option<JsRowCount>,
}

#[napi(catch_unwind)]
pub fn scan_ipc(path: String, options: ScanIPCOptions) -> napi::Result<JsLazyFrame> {
    let n_rows = options.n_rows.map(|i| i as usize);
    let cache = options.cache.unwrap_or(true);
    let rechunk = options.rechunk.unwrap_or(false);
    let row_index: Option<RowIndex> = options.row_count.map(|rc| rc.into());
    let options = IpcScanOptions;
    let lf = LazyFrame::scan_ipc(
        PlPath::new(&path),
        options,
        UnifiedScanArgs {
            pre_slice: n_rows.map(|len| Slice::Positive { offset: 0, len }),
            row_index,
            rechunk,
            cache,
            glob: true,
            ..Default::default()
        },
    )
    .map_err(JsPolarsErr::from)?;
    Ok(lf.into())
}

#[napi(object)]
pub struct JsonScanOptions {
    pub infer_schema_length: Option<i64>,
    pub batch_size: i64,
    pub n_threads: Option<i64>,
    pub num_rows: Option<i64>,
    pub skip_rows: Option<i64>,
    pub low_memory: Option<bool>,
    pub row_count: Option<JsRowCount>,
}

#[napi(catch_unwind)]
pub fn scan_json(path: String, options: JsonScanOptions) -> napi::Result<JsLazyFrame> {
    let batch_size = options.batch_size as usize;
    let batch_size = NonZeroUsize::new(batch_size);
    LazyJsonLineReader::new(PlPath::new(&path))
        .with_batch_size(batch_size)
        .low_memory(options.low_memory.unwrap_or(false))
        .with_row_index(options.row_count.map(|rc| rc.into()))
        .with_n_rows(options.num_rows.map(|i| i as usize))
        .finish()
        .map_err(|err| napi::Error::from_reason(err.to_string()))
        .map(|lf| lf.into())
}

pub struct AsyncFetch((LazyFrame, u32));

impl Task for AsyncFetch {
    type Output = DataFrame;
    type JsValue = JsDataFrame;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let (ldf, n_rows) = &self.0;
        let ldf = ldf.clone();
        let df = ldf.limit(*n_rows).collect().map_err(JsPolarsErr::from)?;
        Ok(df)
    }

    fn resolve(&mut self, _env: Env, df: DataFrame) -> napi::Result<Self::JsValue> {
        Ok(df.into())
    }
}
pub struct AsyncCollect(LazyFrame);

impl Task for AsyncCollect {
    type Output = DataFrame;
    type JsValue = JsDataFrame;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let ldf = self.0.clone();
        let df = ldf.collect().map_err(JsPolarsErr::from)?;
        Ok(df)
    }

    fn resolve(&mut self, _env: Env, df: DataFrame) -> napi::Result<Self::JsValue> {
        Ok(df.into())
    }
}
