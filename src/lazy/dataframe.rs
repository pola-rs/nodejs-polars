use super::dsl::*;
use crate::dataframe::JsDataFrame;
use crate::prelude::*;
use polars::prelude::{col, lit, ClosedWindow, JoinType};
use polars_io::cloud::CloudOptions;
use polars_io::RowIndex;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::PathBuf;

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

#[napi]
impl JsLazyFrame {
    #[napi(catch_unwind)]
    pub fn to_js(&self, env: Env) -> napi::Result<napi::JsUnknown> {
        env.to_js_value(&self.ldf.logical_plan)
    }

    #[napi(catch_unwind)]
    pub fn serialize(&self, format: String) -> napi::Result<Buffer> {
        let buf = match format.as_ref() {
            "bincode" => bincode::serialize(&self.ldf.logical_plan)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            "json" => serde_json::to_vec(&self.ldf.logical_plan)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
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
        let lp: LogicalPlan = match format.as_ref() {
            "bincode" => bincode::deserialize(&buf)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            "json" => serde_json::from_slice(&buf)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
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
    pub fn describe_plan(&self) -> String {
        self.ldf.describe_plan()
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
        _string_cache: Option<bool>,
        slice_pushdown: Option<bool>,
    ) -> JsLazyFrame {
        let type_coercion = type_coercion.unwrap_or(true);
        let predicate_pushdown = predicate_pushdown.unwrap_or(true);
        let projection_pushdown = projection_pushdown.unwrap_or(true);
        let simplify_expr = simplify_expr.unwrap_or(true);
        let slice_pushdown = slice_pushdown.unwrap_or(true);

        let ldf = self.ldf.clone();
        let ldf = ldf
            .with_type_coercion(type_coercion)
            .with_predicate_pushdown(predicate_pushdown)
            .with_simplify_expr(simplify_expr)
            .with_slice_pushdown(slice_pushdown)
            .with_projection_pushdown(projection_pushdown);
        ldf.into()
    }
    #[napi(catch_unwind)]
    pub fn sort(
        &self,
        by_column: String,
        reverse: bool,
        nulls_last: bool,
        multithreaded: bool,
        maintain_order: bool,
    ) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.sort(
            &by_column,
            SortOptions {
                descending: reverse,
                nulls_last,
                multithreaded,
                maintain_order,
            },
        )
        .into()
    }
    #[napi(catch_unwind)]
    pub fn sort_by_exprs(
        &self,
        by_column: Vec<&JsExpr>,
        reverse: Vec<bool>,
        nulls_last: bool,
        maintain_order: bool,
    ) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.sort_by_exprs(by_column.to_exprs(), reverse, nulls_last, maintain_order)
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
    pub fn fetch(&self, n_rows: i64) -> AsyncTask<AsyncFetch> {
        let ldf = self.ldf.clone();
        AsyncTask::new(AsyncFetch((ldf, n_rows as usize)))
    }

    #[napi(catch_unwind)]
    pub fn fetch_sync(&self, n_rows: i64) -> napi::Result<JsDataFrame> {
        let ldf = self.ldf.clone();
        let df = ldf.fetch(n_rows as usize).map_err(JsPolarsErr::from)?;
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
        check_sorted: bool,
    ) -> JsLazyGroupBy {
        let closed_window = closed.0;
        let ldf = self.ldf.clone();
        let by = by.to_exprs();
        let lazy_gb = ldf.group_by_rolling(
            index_column.inner.clone(),
            by,
            RollingGroupOptions {
                index_column: "".into(),
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                closed_window,
                check_sorted,
            },
        );

        JsLazyGroupBy { lgb: Some(lazy_gb) }
    }

    #[allow(clippy::too_many_arguments)]
    #[napi(catch_unwind)]
    pub fn groupby_dynamic(
        &mut self,
        index_column: &JsExpr,
        every: String,
        period: String,
        offset: String,
        include_boundaries: bool,
        closed: Wrap<ClosedWindow>,
        by: Vec<&JsExpr>,
        start_by: Wrap<StartBy>,
        check_sorted: bool,
    ) -> JsLazyGroupBy {
        let closed_window = closed.0;
        let by = by.to_exprs();
        let ldf = self.ldf.clone();
        let lazy_gb = ldf.group_by_dynamic(
            index_column.inner.clone(),
            by,
            DynamicGroupOptions {
                every: Duration::parse(&every),
                period: Duration::parse(&period),
                offset: Duration::parse(&offset),
                label: Label::DataPoint,
                include_boundaries,
                closed_window,
                start_by: start_by.0,
                check_sorted,
                ..Default::default()
            },
        );

        JsLazyGroupBy { lgb: Some(lazy_gb) }
    }
    #[allow(clippy::too_many_arguments)]
    #[napi(catch_unwind)]
    pub fn join_asof(
        &self,
        other: &JsLazyFrame,
        left_on: &JsExpr,
        right_on: &JsExpr,
        left_by: Option<Vec<&str>>,
        right_by: Option<Vec<&str>>,
        allow_parallel: bool,
        force_parallel: bool,
        suffix: String,
        strategy: String,
        tolerance: Option<Wrap<AnyValue<'_>>>,
        tolerance_str: Option<String>,
    ) -> JsLazyFrame {
        let strategy = match strategy.as_ref() {
            "forward" => AsofStrategy::Forward,
            "backward" => AsofStrategy::Backward,
            _ => panic!("expected one of {{'forward', 'backward'}}"),
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
            .how(JoinType::AsOf(AsOfOptions {
                strategy,
                left_by: left_by.map(strings_to_smartstrings),
                right_by: right_by.map(strings_to_smartstrings),
                tolerance: tolerance.map(|t| t.0.into_static().unwrap()),
                tolerance_str: tolerance_str.map(|s| s.into()),
            }))
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
    ) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        let other = other.ldf.clone();
        let left_on = left_on.to_exprs();
        let right_on = right_on.to_exprs();

        ldf.join_builder()
            .with(other)
            .left_on(left_on)
            .right_on(right_on)
            .allow_parallel(allow_parallel)
            .force_parallel(force_parallel)
            .how(how.0)
            .suffix(suffix)
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
        ldf.rename(existing, new_names).into()
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
        let out = ldf.min().map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn max(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.max().map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn sum(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.sum().map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn mean(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.mean().map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn std(&self, ddof: Option<u8>) -> napi::Result<JsLazyFrame> {
        let ddof = ddof.unwrap_or(1);
        let ldf = self.ldf.clone();
        let out = ldf.std(ddof).map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn var(&self, ddof: Option<u8>) -> napi::Result<JsLazyFrame> {
        let ddof = ddof.unwrap_or(1);
        let ldf = self.ldf.clone();
        let out = ldf.var(ddof).map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn median(&self) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf.median().map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> napi::Result<JsLazyFrame> {
        let ldf = self.ldf.clone();
        let out = ldf
            .quantile(lit(quantile), interpolation.0)
            .map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }

    #[napi(catch_unwind)]
    pub fn explode(&self, column: Vec<&JsExpr>) -> JsLazyFrame {
        let ldf = self.ldf.clone();

        ldf.explode(column.to_exprs()).into()
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
            true => ldf.unique_stable(subset, keep.0),
            false => ldf.unique(subset, keep.0),
        }
        .into()
    }
    #[napi(catch_unwind)]
    pub fn drop_nulls(&self, subset: Option<Vec<String>>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.drop_nulls(subset.map(|v| v.into_iter().map(|s| col(&s)).collect()))
            .into()
    }
    #[napi(catch_unwind)]
    pub fn slice(&self, offset: i64, len: u32) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.slice(offset, len).into()
    }
    #[napi(catch_unwind)]
    pub fn tail(&self, n: u32) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.tail(n).into()
    }
    #[napi(catch_unwind)]
    pub fn melt(
        &self,
        id_vars: Vec<&str>,
        value_vars: Vec<&str>,
        value_name: Option<&str>,
        variable_name: Option<&str>,
        streamable: Option<bool>,
    ) -> JsLazyFrame {
        let args = MeltArgs {
            id_vars: strings_to_smartstrings(id_vars),
            value_vars: strings_to_smartstrings(value_vars),
            value_name: value_name.map(|s| s.into()),
            variable_name: variable_name.map(|s| s.into()),
            streamable: streamable.unwrap_or(false),
        };
        let ldf = self.ldf.clone();
        ldf.melt(args).into()
    }

    #[napi(catch_unwind)]
    pub fn with_row_count(&self, name: String, offset: Option<u32>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.with_row_index(&name, offset).into()
    }

    #[napi(catch_unwind)]
    pub fn drop_columns(&self, colss: Vec<String>) -> JsLazyFrame {
        let ldf = self.ldf.clone();
        ldf.drop(colss).into()
    }
    #[napi(js_name = "clone", catch_unwind)]
    pub fn clone(&self) -> JsLazyFrame {
        self.ldf.clone().into()
    }

    #[napi(getter, js_name = "columns", catch_unwind)]
    pub fn columns(&self) -> napi::Result<Vec<String>> {
        Ok(self
            .ldf
            .schema()
            .map_err(JsPolarsErr::from)?
            .iter_names()
            .map(|s| s.as_str().into())
            .collect())
    }

    #[napi(catch_unwind)]
    pub fn unnest(&self, colss: Vec<String>) -> JsLazyFrame {
        self.ldf.clone().unnest(colss).into()
    }

    #[napi(catch_unwind)]
    pub fn sink_csv(&self, path: String, options: SinkCsvOptions) -> napi::Result<()> {
        let quote_style = QuoteStyle::default();
        let null_value = options
            .null_value
            .unwrap_or(SerializeOptions::default().null);
        let float_precision: Option<usize> = options.float_precision.map(|fp| fp as usize);
        let separator = options.separator.unwrap_or(",".to_owned()).as_bytes()[0];
        let line_terminator = options.line_terminator.unwrap_or("\n".to_string());
        let quote_char = options.quote_char.unwrap_or("\"".to_owned()).as_bytes()[0];
        let date_format = options.date_format;
        let time_format = options.time_format;
        let datetime_format = options.datetime_format;

        let serialize_options = SerializeOptions {
            date_format,
            time_format,
            datetime_format,
            float_precision,
            separator,
            quote_char,
            null: null_value,
            line_terminator,
            quote_style,
        };

        let batch_size = options.batch_size.map(|bs| bs).unwrap_or(1024) as usize;
        let batch_size = NonZeroUsize::new(batch_size).unwrap();
        let include_bom = options.include_bom.unwrap_or(false);
        let include_header = options.include_header.unwrap_or(true);
        let maintain_order = options.maintain_order;

        let options = CsvWriterOptions {
            include_bom,
            include_header,
            maintain_order,
            batch_size,
            serialize_options,
        };

        let path_buf: PathBuf = PathBuf::from(path);
        let ldf = self.ldf.clone().with_comm_subplan_elim(false);
        let _ = ldf.sink_csv(path_buf, options).map_err(JsPolarsErr::from);
        Ok(())
    }

    #[napi(catch_unwind)]
    pub fn sink_parquet(&self, path: String, options: SinkParquetOptions) -> napi::Result<()> {
        let compression_str = options.compression.unwrap_or("zstd".to_string());
        let compression = parse_parquet_compression(compression_str, options.compression_level)?;
        let statistics = options.statistics.unwrap_or(false);
        let row_group_size = options.row_group_size.map(|i| i as usize);
        let data_pagesize_limit = options.data_pagesize_limit.map(|i| i as usize);
        let maintain_order = options.maintain_order.unwrap_or(true);

        let options = ParquetWriteOptions {
            compression,
            statistics,
            row_group_size,
            data_pagesize_limit,
            maintain_order,
        };

        let path_buf: PathBuf = PathBuf::from(path);
        let ldf = self.ldf.clone().with_comm_subplan_elim(false);
        let _ = ldf
            .sink_parquet(path_buf, options)
            .map_err(JsPolarsErr::from);
        Ok(())
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
    pub eol_char: Option<u8>,
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
    let quote_char = if let Some(s) = options.quote_char {
        if s.is_empty() {
            None
        } else {
            Some(s.as_bytes()[0])
        }
    } else {
        None
    };

    let overwrite_dtype = options.overwrite_dtype.map(|overwrite_dtype| {
        overwrite_dtype
            .iter()
            .map(|(name, dtype)| {
                let dtype = dtype.0.clone();
                Field::new(name, dtype)
            })
            .collect::<Schema>()
    });

    let encoding = match options.encoding.as_ref() {
        "utf8" => CsvEncoding::Utf8,
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        e => return Err(JsPolarsErr::Other(format!("encoding not {} not implemented.", e)).into()),
    };

    let r = LazyCsvReader::new(path)
        .with_infer_schema_length(Some(options.infer_schema_length.unwrap_or(100) as usize))
        .with_separator(options.sep.unwrap_or(",".to_owned()).as_bytes()[0])
        .has_header(options.has_header.unwrap_or(true))
        .with_ignore_errors(options.ignore_errors)
        .with_skip_rows(options.skip_rows.unwrap_or(0) as usize)
        .with_n_rows(n_rows)
        .with_cache(options.cache.unwrap_or(true))
        .with_dtype_overwrite(overwrite_dtype.as_ref())
        .with_schema(options.schema.map(|schema| Arc::new(schema.0)))
        .low_memory(options.low_memory.unwrap_or(false))
        .with_comment_prefix(options.comment_prefix.as_deref())
        .with_quote_char(quote_char)
        .with_end_of_line_char(options.eol_char.unwrap_or(b'\n'))
        .with_rechunk(options.rechunk.unwrap_or(false))
        .with_skip_rows_after_header(options.skip_rows_after_header as usize)
        .with_encoding(encoding)
        .with_row_index(row_count)
        .with_try_parse_dates(options.parse_dates.unwrap_or(false))
        .with_null_values(options.null_values.map(|s| s.0))
        .with_missing_is_null(!missing_utf8_is_empty_string)
        .truncate_ragged_lines(options.truncate_ragged_lines.unwrap_or(false))
        .raise_if_empty(options.raise_if_empty.unwrap_or(true))
        .finish()
        .map_err(JsPolarsErr::from)?;
    Ok(r.into())
}

#[napi(object)]
pub struct ScanParquetOptions {
    pub n_rows: Option<i64>,
    pub cache: Option<bool>,
    pub parallel: Wrap<ParallelStrategy>,
    pub row_count: Option<JsRowCount>,
    pub rechunk: Option<bool>,
    pub row_count_name: Option<String>,
    pub row_count_offset: Option<u32>,
    pub low_memory: Option<bool>,
    pub use_statistics: Option<bool>,
    pub hive_partitioning: Option<bool>,
    pub cloud_options: Option<HashMap::<String, String>>,
    pub retries: Option<i64>,
}

#[napi(catch_unwind)]
pub fn scan_parquet(path: String, options: ScanParquetOptions) -> napi::Result<JsLazyFrame> {
    let n_rows = options.n_rows.map(|i| i as usize);
    let cache = options.cache.unwrap_or(true);
    let parallel = options.parallel;
    let row_index: Option<RowIndex> = options.row_count.map(|rc| rc.into());
    let rechunk = options.rechunk.unwrap_or(false);
    let low_memory = options.low_memory.unwrap_or(false);
    let use_statistics = options.use_statistics.unwrap_or(false);
    
    let mut cloud_options: Option<CloudOptions> = if let Some(o) = options.cloud_options {
        let co: Vec<(String, String)> = o.into_iter().map(|kv: (String, String)| kv).collect();
        Some(CloudOptions::from_untyped_config(&path, co).map_err(JsPolarsErr::from)?)
    } else {
        None
    };
    
    let retries = options.retries.unwrap_or_else(|| 2) as usize;
    if retries > 0 {
        cloud_options =
            cloud_options
                .or_else(|| Some(CloudOptions::default()))
                .map(|mut options| {
                    options.max_retries = retries;
                    options
                });
    }

    let hive_partitioning: bool = options.hive_partitioning.unwrap_or(false);
    let args = ScanArgsParquet {
        n_rows,
        cache,
        parallel: parallel.0,
        rechunk,
        row_index,
        low_memory,
        cloud_options,
        use_statistics,
        hive_partitioning,
    };
    let lf = LazyFrame::scan_parquet(path, args).map_err(JsPolarsErr::from)?;
    Ok(lf.into())
}

#[napi(object)]
pub struct ScanIPCOptions {
    pub n_rows: Option<i64>,
    pub cache: Option<bool>,
    pub rechunk: Option<bool>,
    pub row_count: Option<JsRowCount>,
    pub memmap: Option<bool>,
}

#[napi(catch_unwind)]
pub fn scan_ipc(path: String, options: ScanIPCOptions) -> napi::Result<JsLazyFrame> {
    let n_rows = options.n_rows.map(|i| i as usize);
    let cache = options.cache.unwrap_or(true);
    let rechunk = options.rechunk.unwrap_or(false);
    let memmap = options.memmap.unwrap_or(true);
    let row_index: Option<RowIndex> = options.row_count.map(|rc| rc.into());
    let args = ScanArgsIpc {
        n_rows,
        cache,
        rechunk,
        row_index,
        memmap,
    };
    let lf = LazyFrame::scan_ipc(path, args).map_err(JsPolarsErr::from)?;
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
    LazyJsonLineReader::new(path)
        .with_batch_size(batch_size)
        .low_memory(options.low_memory.unwrap_or(false))
        .with_row_index(options.row_count.map(|rc| rc.into()))
        .with_n_rows(options.num_rows.map(|i| i as usize))
        .finish()
        .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))
        .map(|lf| lf.into())
}

pub struct AsyncFetch((LazyFrame, usize));

impl Task for AsyncFetch {
    type Output = DataFrame;
    type JsValue = JsDataFrame;

    fn compute(&mut self) -> napi::Result<Self::Output> {
        let (ldf, n_rows) = &self.0;
        let ldf = ldf.clone();
        let df = ldf.fetch(*n_rows).map_err(JsPolarsErr::from)?;
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
