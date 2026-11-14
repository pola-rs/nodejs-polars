use crate::dataframe::JsDataFrame;
use crate::prelude::*;
use polars_core::series::ops::NullBehavior;
use polars_core::utils::CustomIterTools;
use polars_utils::aliases::PlFixedStateQuality;
use std::hash::BuildHasher;

#[napi(custom_finalize)]
#[derive(Clone)]
pub struct JsSeries {
    pub(crate) series: Series,
    /// Bytes reported to V8 for this series, or 0 if it was never reported.
    pub(crate) reported_size: i64,
}

impl JsSeries {
    pub(crate) fn new(series: Series) -> Self {
        JsSeries {
            series,
            reported_size: 0,
        }
    }

    /// Build a series and report its size to V8 in one step.
    ///
    /// Use this at every `#[napi]` boundary that hands a new series to JS, so
    /// the report is paired with the finalizer's withdrawal.
    pub(crate) fn reported(series: Series, env: &Env) -> Self {
        let mut s = JsSeries::new(series);
        s.report_to_v8(env);
        s
    }

    /// Report this series' size to V8. Called as the series crosses into JS.
    pub(crate) fn report_to_v8(&mut self, env: &Env) {
        if self.reported_size == 0 {
            let size = self.series.estimated_size() as i64;
            if crate::memory::report(env, size) {
                self.reported_size = size;
            }
        }
    }
}

impl ObjectFinalize for JsSeries {
    fn finalize(self, env: Env) -> napi::Result<()> {
        if self.reported_size != 0 {
            let current = self.series.estimated_size() as i64;
            crate::memory::withdraw(&env, self.reported_size);
            crate::memory::record_drift(self.reported_size, current);
        }
        Ok(())
    }
}
impl From<Series> for JsSeries {
    fn from(s: Series) -> Self {
        JsSeries::new(s)
    }
}

fn bin_config() -> bincode::config::Configuration {
    bincode::config::standard()
        .with_no_limit()
        .with_variable_int_encoding()
}

#[inline]
fn map_seed(seed: Option<Wrap<u64>>) -> Option<u64> {
    seed.map(|v| v.0)
}

#[inline]
fn parse_fill_char(fill_char: String) -> napi::Result<char> {
    fill_char.chars().next().ok_or_else(|| {
        napi::Error::from_reason("fill_char must contain at least one character".to_owned())
    })
}

#[napi]
impl JsSeries {
    #[napi(catch_unwind)]
    pub fn to_js(&'_ self, env: Env) -> napi::Result<napi::Unknown<'_>> {
        env.to_js_value(&self.series)
    }

    #[napi(catch_unwind)]
    pub fn serialize(&self, format: String) -> napi::Result<Buffer> {
        let buf = match format.as_ref() {
            "bincode" => bincode::serde::encode_to_vec(&self.series, bin_config())
                .map_err(|err| napi::Error::from_reason(err.to_string()))?,
            "json" => serde_json::to_vec(&self.series)
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
    pub fn deserialize(env: Env, buf: Buffer, format: String) -> napi::Result<JsSeries> {
        let series: Series = match format.as_ref() {
            "bincode" => {
                bincode::serde::decode_from_slice(&buf, bin_config())
                    .map_err(|err| napi::Error::from_reason(err.to_string()))?
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
        Ok(JsSeries::reported(series, &env))
    }
    //
    // FACTORIES
    //
    #[napi(factory, catch_unwind)]
    pub fn new_int_8_array(env: Env, name: String, arr: Int8Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_uint8_array(env: Env, name: String, arr: Uint8Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_uint8_clamped_array(env: Env, name: String, arr: Uint8ClampedArray) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_int16_array(env: Env, name: String, arr: Int16Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_uint16_array(env: Env, name: String, arr: Uint16Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_int32_array(env: Env, name: String, arr: Int32Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_uint32_array(env: Env, name: String, arr: Uint32Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_float32_array(env: Env, name: String, arr: Float32Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_float64_array(env: Env, name: String, arr: Float64Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_bigint64_array(env: Env, name: String, arr: BigInt64Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_biguint64_array(env: Env, name: String, arr: BigUint64Array) -> JsSeries {
        JsSeries::reported(Series::new(PlSmallStr::from_string(name), arr), &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_opt_str(env: Env, name: String, val: Wrap<StringChunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_opt_bool(env: Env, name: String, val: Wrap<BooleanChunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_opt_i32(env: Env, name: String, val: Wrap<Int32Chunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_opt_i64(env: Env, name: String, val: Wrap<Int64Chunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_opt_u64(env: Env, name: String, val: Wrap<UInt64Chunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_opt_u32(env: Env, name: String, val: Wrap<UInt32Chunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }
    #[napi(factory, catch_unwind)]
    pub fn new_opt_f32(env: Env, name: String, val: Wrap<Float32Chunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }

    #[napi(factory, catch_unwind)]
    pub fn new_opt_f64(env: Env, name: String, val: Wrap<Float64Chunked>) -> JsSeries {
        let mut s = val.0.into_series();
        s.rename(PlSmallStr::from_string(name));
        JsSeries::reported(s, &env)
    }

    #[napi(factory, catch_unwind)]
    pub fn new_opt_date(
        env: Env,
        name: String,
        values: Vec<napi::Unknown>,
        strict: Option<bool>,
    ) -> napi::Result<JsSeries> {
        let len = values.len();
        let mut builder =
            PrimitiveChunkedBuilder::<Int64Type>::new(PlSmallStr::from_string(name), len);
        for item in values.into_iter() {
            match item.get_type()? {
                ValueType::Object => {
                    let obj: &Object = unsafe { &item.cast()? };
                    if obj.is_date()? {
                        let d: &napi::JsDate = unsafe { &item.cast()? };
                        match d.value_of() {
                            Ok(v) => builder.append_value(v as i64),
                            Err(e) => {
                                if strict.unwrap_or(false) {
                                    return Err(e);
                                }
                                builder.append_null()
                            }
                        }
                    }
                }
                ValueType::Null | ValueType::Undefined => builder.append_null(),
                _ => {
                    return Err(JsPolarsErr::Other("Series must be of date type".to_owned()).into())
                }
            }
        }
        let ca: ChunkedArray<Int64Type> = builder.finish();
        Ok(JsSeries::reported(
            ca.into_datetime(TimeUnit::Milliseconds, None).into_series(),
            &env,
        ))
    }

    #[napi(factory, catch_unwind)]
    pub fn new_any_value(
        env: Env,
        name: String,
        values: Vec<Wrap<AnyValue>>,
        dtype: Wrap<DataType>,
    ) -> napi::Result<JsSeries> {
        let values = values.into_iter().map(|v| v.0).collect::<Vec<_>>();

        let s = Series::from_any_values_and_dtype(
            PlSmallStr::from_string(name),
            &values,
            &dtype.0,
            false,
        )
        .map_err(JsPolarsErr::from)?;

        Ok(JsSeries::reported(s, &env))
    }

    #[napi(factory, catch_unwind)]
    pub fn new_list(
        env: Env,
        name: String,
        values: Array,
        dtype: Wrap<DataType>,
    ) -> napi::Result<JsSeries> {
        use crate::list_construction::js_arr_to_list;
        let s = js_arr_to_list(&name, &values, &dtype.0)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(factory, catch_unwind)]
    pub fn repeat(
        env: Env,
        name: String,
        val: Wrap<AnyValue>,
        n: i64,
        dtype: Wrap<DataType>,
    ) -> napi::Result<JsSeries> {
        let mut s: JsSeries = match dtype.0 {
            DataType::String => {
                if let AnyValue::StringOwned(v) = val.0 {
                    let val = v.to_string();
                    let mut ca: StringChunked = (0..n).map(|_| val.clone()).collect_trusted();
                    ca.rename(PlSmallStr::from_string(name));
                    ca.into_series().into()
                } else {
                    return Err(napi::Error::from_reason(
                        "invalid primitive cast".to_owned(),
                    ));
                }
            }
            DataType::Int64 => {
                if let AnyValue::Int64(v) = val.0 {
                    let mut ca: NoNull<Int64Chunked> = (0..n).map(|_| v).collect_trusted();
                    ca.rename(PlSmallStr::from_string(name));
                    ca.into_inner().into_series().into()
                } else {
                    return Err(napi::Error::from_reason(
                        "invalid primitive cast".to_owned(),
                    ));
                }
            }
            DataType::UInt64 => {
                if let AnyValue::UInt64(v) = val.0 {
                    let mut ca: NoNull<UInt64Chunked> = (0..n).map(|_| v).collect_trusted();
                    ca.rename(PlSmallStr::from_string(name));

                    ca.into_inner().into_series().into()
                } else {
                    return Err(napi::Error::from_reason(
                        "invalid primitive cast".to_owned(),
                    ));
                }
            }
            DataType::Float64 => {
                if let AnyValue::Float64(v) = val.0 {
                    let mut ca: NoNull<Float64Chunked> = (0..n).map(|_| v).collect_trusted();
                    ca.rename(PlSmallStr::from_string(name));
                    ca.into_inner().into_series().into()
                } else {
                    return Err(napi::Error::from_reason(
                        "invalid primitive cast".to_owned(),
                    ));
                }
            }
            dt => {
                return Err(napi::Error::from_reason(format!(
                    "data type: {:?} is not supported as range",
                    dt
                )));
            }
        };
        s.report_to_v8(&env);
        Ok(s)
    }
    //
    // GETTERS
    //
    #[napi(getter, catch_unwind)]
    pub fn dtype(&self) -> Wrap<DataType> {
        Wrap(self.series.dtype().clone())
    }

    #[napi(getter, catch_unwind)]
    pub fn __inner__(&self) -> External<Series> {
        External::new(self.series.clone())
    }

    #[napi(getter, catch_unwind)]
    pub fn inner_dtype(&self) -> Option<JsDataType> {
        self.series
            .dtype()
            .inner_dtype()
            .and_then(|dt| JsDataType::try_from(dt).ok())
    }
    #[napi(getter, catch_unwind)]
    pub fn name(&self) -> String {
        self.series.name().to_string()
    }
    #[napi(catch_unwind)]
    pub fn to_string(&self) -> String {
        std::string::ToString::to_string(&self.series)
    }
    #[napi(catch_unwind)]
    pub fn get_fmt(&self, index: Wrap<usize>, str_lengths: Wrap<usize>) -> String {
        let val = format!("{}", self.series.get(index.0).unwrap());
        if let DataType::String | DataType::Categorical(..) = self.series.dtype() {
            let trunc_end = val
                .char_indices()
                .nth(str_lengths.0)
                .map(|(i, _)| i)
                .unwrap_or(val.len());
            let v_trunc = &val[..trunc_end];
            if val == v_trunc {
                val
            } else {
                format!("{v_trunc}…")
            }
        } else {
            val
        }
    }
    #[napi(catch_unwind)]
    pub fn estimated_size(&self) -> i64 {
        self.series.estimated_size() as i64
    }

    #[napi(catch_unwind)]
    pub fn rechunk(&mut self, env: Env, in_place: bool) -> Option<JsSeries> {
        let series = self.series.rechunk();
        if in_place {
            self.series = series;
            None
        } else {
            Some(JsSeries::reported(series, &env))
        }
    }
    #[napi(catch_unwind)]
    pub fn get_idx(&self, idx: i64) -> Wrap<AnyValue<'_>> {
        Wrap(self.series.get(idx as usize).unwrap())
    }
    #[napi(catch_unwind)]
    pub fn bitand(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let out = (&self.series & &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }
    #[napi(catch_unwind)]
    pub fn bitor(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let out = (&self.series | &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }
    #[napi(catch_unwind)]
    pub fn bitxor(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let out = (&self.series ^ &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }
    #[napi(catch_unwind)]
    pub fn cum_sum(&self, env: Env, reverse: Option<bool>) -> napi::Result<JsSeries> {
        let reverse = reverse.unwrap_or(false);
        let out = cum_sum(&self.series, reverse).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }
    #[napi(catch_unwind)]
    pub fn cum_max(&self, env: Env, reverse: Option<bool>) -> napi::Result<JsSeries> {
        let reverse = reverse.unwrap_or(false);
        let out = cum_max(&self.series, reverse).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }
    #[napi(catch_unwind)]
    pub fn cum_min(&self, env: Env, reverse: Option<bool>) -> napi::Result<JsSeries> {
        let reverse = reverse.unwrap_or(false);
        let out = cum_min(&self.series, reverse).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }
    #[napi(catch_unwind)]
    pub fn cum_prod(&self, env: Env, reverse: Option<bool>) -> napi::Result<JsSeries> {
        let reverse = reverse.unwrap_or(false);
        let out = cum_prod(&self.series, reverse).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }
    #[napi(catch_unwind)]
    pub fn product(&self) -> Result<JsAnyValue> {
        let scalar = self.series.product().map_err(JsPolarsErr::from)?;
        scalar.value().clone().try_into()
    }
    #[napi(catch_unwind)]
    pub fn chunk_lengths(&self) -> Vec<u32> {
        self.series.chunk_lengths().map(|i| i as u32).collect()
    }

    #[napi(catch_unwind)]
    pub fn rename(&mut self, name: String) {
        self.series.rename(PlSmallStr::from_string(name));
    }

    #[napi(catch_unwind)]
    pub fn mean(&self) -> Option<f64> {
        match self.series.dtype() {
            DataType::Boolean => self
                .series
                .cast(&DataType::UInt8)
                .ok()
                .and_then(|s| s.mean()),
            _ => self.series.mean(),
        }
    }
    #[napi(catch_unwind)]
    pub fn max(&self) -> napi::Result<Either3<Option<f64>, bool, Option<i64>>> {
        Ok(match self.series.dtype() {
            DataType::Float32 | DataType::Float64 => self.series.max::<f64>().map(Either3::A),
            DataType::Boolean => self
                .series
                .max::<u32>()
                .map(|v| v == Some(1))
                .map(Either3::B),
            _ => self.series.max::<i64>().map(Either3::C),
        }
        .map_err(JsPolarsErr::from)?)
    }
    #[napi(catch_unwind)]
    pub fn min(&self) -> napi::Result<Either3<Option<f64>, bool, Option<i64>>> {
        Ok(match self.series.dtype() {
            DataType::Float32 | DataType::Float64 => self.series.min::<f64>().map(Either3::A),
            DataType::Boolean => self
                .series
                .min::<u32>()
                .map(|v| v == Some(1))
                .map(Either3::B),
            _ => self.series.min::<i64>().map(Either3::C),
        }
        .map_err(JsPolarsErr::from)?)
    }
    #[napi(catch_unwind)]
    pub fn sum(&self) -> napi::Result<Either3<f64, bool, i64>> {
        Ok(match self.series.dtype() {
            DataType::Float32 | DataType::Float64 => self.series.sum::<f64>().map(Either3::A),
            DataType::Boolean => self.series.sum::<u32>().map(|v| v == 1).map(Either3::B),
            _ => self.series.sum::<i64>().map(Either3::C),
        }
        .map_err(JsPolarsErr::from)?)
    }
    #[napi(catch_unwind)]
    pub fn n_chunks(&self) -> u32 {
        self.series.n_chunks() as u32
    }
    #[napi(catch_unwind)]
    pub fn limit(&self, env: Env, num_elements: f64) -> JsSeries {
        JsSeries::reported(self.series.limit(num_elements as usize), &env)
    }
    #[napi(catch_unwind)]
    pub fn slice(&self, env: Env, offset: i64, length: f64) -> JsSeries {
        JsSeries::reported(self.series.slice(offset, length as usize), &env)
    }
    #[napi(catch_unwind)]
    pub fn append(&mut self, other: &JsSeries) -> napi::Result<()> {
        self.series
            .append(&other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn extend(&mut self, other: &JsSeries) -> napi::Result<()> {
        self.series
            .extend(&other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn filter(&self, env: Env, filter: &JsSeries) -> napi::Result<JsSeries> {
        let filter_series = &filter.series;
        if let Ok(ca) = filter_series.bool() {
            let series = self.series.filter(ca).map_err(JsPolarsErr::from)?;
            Ok(JsSeries::reported(series, &env))
        } else {
            let err = napi::Error::from_reason("Expected a boolean mask".to_owned());
            Err(err)
        }
    }
    #[napi(catch_unwind)]
    pub fn add(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let series = (&self.series + &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(series, &env))
    }
    #[napi(catch_unwind)]
    pub fn sub(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let series = (&self.series - &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(series, &env))
    }
    #[napi(catch_unwind)]
    pub fn mul(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let series = (&self.series * &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(series, &env))
    }
    #[napi(catch_unwind)]
    pub fn div(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let series = (&self.series / &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(series, &env))
    }
    #[napi(catch_unwind)]
    pub fn rem(&self, env: Env, other: &JsSeries) -> napi::Result<JsSeries> {
        let series = (&self.series % &other.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(series, &env))
    }
    #[napi(catch_unwind)]
    pub fn head(&self, env: Env, length: Option<i64>) -> JsSeries {
        JsSeries::reported(self.series.head(length.map(|l| l as usize)), &env)
    }
    #[napi(catch_unwind)]
    pub fn tail(&self, env: Env, length: Option<i64>) -> JsSeries {
        JsSeries::reported(self.series.tail(length.map(|l| l as usize)), &env)
    }

    #[napi(catch_unwind)]
    pub fn sort(&mut self, env: Env, descending: bool, nulls_last: bool) -> napi::Result<JsSeries> {
        let sorted: Series = self
            .series
            .sort(
                SortOptions::default()
                    .with_order_descending(descending)
                    .with_nulls_last(nulls_last),
            )
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(sorted, &env))
    }

    #[napi]
    pub fn argsort(
        &self,
        env: Env,
        descending: bool,
        nulls_last: bool,
        multithreaded: bool,
        maintain_order: bool,
    ) -> JsSeries {
        let series = self
            .series
            .arg_sort(SortOptions {
                descending,
                nulls_last,
                multithreaded,
                maintain_order,
                limit: None,
            })
            .into_series();
        JsSeries::reported(series, &env)
    }
    #[napi(catch_unwind)]
    pub fn unique(&self, env: Env) -> napi::Result<JsSeries> {
        let unique = self.series.unique().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(unique, &env))
    }
    #[napi(catch_unwind)]
    pub fn unique_stable(&self, env: Env) -> napi::Result<JsSeries> {
        let unique = self.series.unique_stable().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(unique, &env))
    }
    #[napi(catch_unwind)]
    pub fn value_counts(
        &self,
        env: Env,
        sort: bool,
        parallel: bool,
        name: String,
        normalize: bool,
    ) -> napi::Result<JsDataFrame> {
        let df = self
            .series
            .value_counts(sort, parallel, PlSmallStr::from_string(name), normalize)
            .map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::reported(df, &env))
    }

    #[napi(catch_unwind)]
    pub fn arg_unique(&self, env: Env) -> napi::Result<JsSeries> {
        let arg_unique = self.series.arg_unique().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(arg_unique.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn arg_min(&self) -> Option<i64> {
        self.series.arg_min().map(|v| v as i64)
    }
    #[napi(catch_unwind)]
    pub fn arg_max(&self) -> Option<i64> {
        self.series.arg_max().map(|v| v as i64)
    }
    #[napi(catch_unwind)]
    pub fn take(&self, env: Env, indices: Vec<u32>) -> napi::Result<JsSeries> {
        let indices = UInt32Chunked::from_vec(PlSmallStr::EMPTY, indices);
        let take = self.series.take(&indices).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(take, &env))
    }
    #[napi(catch_unwind)]
    pub fn take_with_series(&self, env: Env, indices: &JsSeries) -> napi::Result<JsSeries> {
        let idx = indices.series.u32().map_err(JsPolarsErr::from)?;
        let take = self.series.take(idx).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(take, &env))
    }

    #[napi(catch_unwind)]
    pub fn null_count(&self) -> napi::Result<i64> {
        Ok(self.series.null_count() as i64)
    }

    #[napi(catch_unwind)]
    pub fn has_nulls(&self) -> bool {
        self.series.has_nulls()
    }

    #[napi(catch_unwind)]
    pub fn is_null(&self, env: Env) -> JsSeries {
        JsSeries::reported(self.series.is_null().into_series(), &env)
    }

    #[napi(catch_unwind)]
    pub fn is_not_null(&self, env: Env) -> JsSeries {
        JsSeries::reported(self.series.is_not_null().into_series(), &env)
    }

    #[napi(catch_unwind)]
    pub fn is_not_nan(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.is_not_nan().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(ca.into_series(), &env))
    }

    #[napi(catch_unwind)]
    pub fn is_nan(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.is_nan().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(ca.into_series(), &env))
    }

    #[napi(catch_unwind)]
    pub fn is_finite(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.is_finite().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(ca.into_series(), &env))
    }

    #[napi(catch_unwind)]
    pub fn is_infinite(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.is_infinite().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(ca.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn implode(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.implode().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(ca.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn is_unique(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = is_unique(&self.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(ca.into_series(), &env))
    }

    #[napi(catch_unwind)]
    pub fn sample_n(
        &self,
        env: Env,
        n: u32,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<Wrap<u64>>,
    ) -> napi::Result<Self> {
        let seed = map_seed(seed);
        let s = self
            .series
            .sample_n(n as usize, with_replacement, Some(shuffle), seed)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn sample_frac(
        &self,
        env: Env,
        frac: f64,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<Wrap<u64>>,
    ) -> napi::Result<Self> {
        let seed = map_seed(seed);
        let s = self
            .series
            .sample_frac(frac, with_replacement, Some(shuffle), seed)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn is_duplicated(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = is_duplicated(&self.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(ca.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn explode(&self, env: Env) -> napi::Result<JsSeries> {
        let options = ExplodeOptions {
            empty_as_null: true,
            keep_nulls: true,
        };
        let s = self.series.explode(options).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn gather_every(&self, env: Env, n: i64, offset: i64) -> napi::Result<JsSeries> {
        let s = self
            .series
            .gather_every(n as usize, offset as usize)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn series_equal(&self, other: &JsSeries, null_equal: bool, strict: bool) -> bool {
        if strict {
            self.series.eq(&other.series)
        } else if null_equal {
            self.series.equals_missing(&other.series)
        } else {
            self.series.equals(&other.series)
        }
    }
    #[napi(catch_unwind)]
    pub fn eq(&self, env: Env, rhs: &JsSeries) -> napi::Result<JsSeries> {
        Ok(JsSeries::reported(
            self.series
                .equal(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
            &env,
        ))
    }

    #[napi(catch_unwind)]
    pub fn neq(&self, env: Env, rhs: &JsSeries) -> napi::Result<JsSeries> {
        Ok(JsSeries::reported(
            self.series
                .not_equal(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
            &env,
        ))
    }

    #[napi(catch_unwind)]
    pub fn gt(&self, env: Env, rhs: &JsSeries) -> napi::Result<JsSeries> {
        Ok(JsSeries::reported(
            self.series
                .gt(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
            &env,
        ))
    }

    #[napi(catch_unwind)]
    pub fn gt_eq(&self, env: Env, rhs: &JsSeries) -> napi::Result<JsSeries> {
        Ok(JsSeries::reported(
            self.series
                .gt_eq(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
            &env,
        ))
    }

    #[napi(catch_unwind)]
    pub fn lt(&self, env: Env, rhs: &JsSeries) -> napi::Result<JsSeries> {
        Ok(JsSeries::reported(
            self.series
                .lt(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
            &env,
        ))
    }

    #[napi(catch_unwind)]
    pub fn lt_eq(&self, env: Env, rhs: &JsSeries) -> napi::Result<JsSeries> {
        Ok(JsSeries::reported(
            self.series
                .lt_eq(&rhs.series)
                .map_err(JsPolarsErr::from)?
                .into_series(),
            &env,
        ))
    }

    #[napi(catch_unwind)]
    pub fn all(&self, ignore_nulls: bool) -> napi::Result<Option<bool>> {
        let bool = self.series.bool().map_err(JsPolarsErr::from)?;
        Ok(if ignore_nulls {
            Some(bool.all())
        } else {
            bool.all_kleene()
        })
    }
    #[napi(catch_unwind)]
    pub fn any(&self, ignore_nulls: bool) -> napi::Result<Option<bool>> {
        let bool = self.series.bool().map_err(JsPolarsErr::from)?;
        Ok(if ignore_nulls {
            Some(bool.any())
        } else {
            bool.any_kleene()
        })
    }
    #[napi(catch_unwind)]
    pub fn _not(&self, env: Env) -> napi::Result<JsSeries> {
        let bool = self.series.bool().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported((!bool).into_series(), &env))
    }

    #[napi(catch_unwind)]
    pub fn as_str(&self) -> napi::Result<String> {
        Ok(format!("{:?}", self.series))
    }
    #[napi(catch_unwind)]
    pub fn len(&self) -> i64 {
        self.series.len() as i64
    }
    #[napi(catch_unwind)]
    pub fn to_physical(&self, env: Env) -> JsSeries {
        let s = self.series.to_physical_repr().into_owned();
        JsSeries::reported(s, &env)
    }

    #[napi(catch_unwind)]
    pub fn to_typed_array(&self) -> TypedArrayBuffer {
        let series = &self.series;
        series.into()
    }
    #[napi(catch_unwind)]
    pub fn to_array(&self) -> Wrap<&Series> {
        Wrap(&self.series)
    }

    #[napi(catch_unwind)]
    pub fn median(&self) -> Option<f64> {
        match self.series.dtype() {
            DataType::Boolean => self
                .series
                .cast(&DataType::UInt8)
                .ok()
                .and_then(|s| s.median()),
            _ => self.series.median(),
        }
    }
    #[napi(catch_unwind)]
    pub fn quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileMethod>,
    ) -> Result<JsAnyValue> {
        let binding = self
            .series
            .quantile_reduce(quantile, interpolation.0)
            .map_err(JsPolarsErr::from)?;
        let v = binding.as_any_value();
        v.clone().try_into()
    }
    /// Rechunk and return a pointer to the start of the Series.
    /// Only implemented for numeric types
    pub fn as_single_ptr(&mut self) -> napi::Result<usize> {
        let ptr = self.series.as_single_ptr().map_err(JsPolarsErr::from)?;
        Ok(ptr)
    }

    #[napi(catch_unwind)]
    pub fn drop_nulls(&self, env: Env) -> JsSeries {
        JsSeries::reported(self.series.drop_nulls(), &env)
    }

    #[napi(catch_unwind)]
    pub fn fill_null(&self, env: Env, strategy: Wrap<FillNullStrategy>) -> napi::Result<JsSeries> {
        let series = self
            .series
            .fill_null(strategy.0)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(series, &env))
    }

    #[napi(catch_unwind)]
    pub fn is_in(&self, env: Env, other: &JsSeries, nulls_equal: bool) -> napi::Result<JsSeries> {
        let imploded = other.series.implode().map_err(JsPolarsErr::from)?;
        let series = is_in(&self.series, &imploded.into_series(), nulls_equal)
            .map(|ca| ca.into_series())
            .map_err(JsPolarsErr::from)?;

        Ok(JsSeries::reported(series, &env))
    }

    #[napi(catch_unwind)]
    pub fn clone(&self, env: Env) -> JsSeries {
        JsSeries::reported(self.series.clone(), &env)
    }

    #[napi(catch_unwind)]
    pub fn shift(&self, env: Env, periods: i64) -> JsSeries {
        let s = self.series.shift(periods);
        JsSeries::reported(s, &env)
    }
    #[napi(catch_unwind)]
    pub fn zip_with(&self, env: Env, mask: &JsSeries, other: &JsSeries) -> napi::Result<JsSeries> {
        let mask = mask.series.bool().map_err(JsPolarsErr::from)?;
        let s = self
            .series
            .zip_with(mask, &other.series)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    // Struct namespace
    #[napi(catch_unwind)]
    pub fn struct_to_frame(&self, env: Env) -> napi::Result<crate::dataframe::JsDataFrame> {
        let ca = self.series.struct_().map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::reported(ca.clone().unnest(), &env))
    }

    #[napi(catch_unwind)]
    pub fn struct_fields(&self) -> napi::Result<Vec<&str>> {
        let ca = self.series.struct_().map_err(JsPolarsErr::from)?;
        Ok(ca
            .struct_fields()
            .iter()
            .map(|s| s.name().as_str())
            .collect())
    }
    // String Namespace

    #[napi(catch_unwind)]
    pub fn str_lengths(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca.str_len_chars().into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi]
    pub fn str_contains(&self, env: Env, pat: String, strict: bool) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca
            .contains(&pat, strict)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn str_json_decode(
        &self,
        env: Env,
        dtype: Option<Wrap<DataType>>,
        infer_schema_len: Option<i64>,
    ) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let dt = dtype.map(|d| d.0);
        let infer_schema_len = infer_schema_len.map(|l| l as usize);
        let s = ca
            .json_decode(dt, infer_schema_len)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn str_json_path_match(
        &self,
        env: Env,
        pat: Wrap<StringChunked>,
    ) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca
            .json_path_match(&pat.0)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn str_replace(&self, env: Env, pat: String, val: String) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca
            .replace(&pat, &val)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn str_replace_all(&self, env: Env, pat: String, val: String) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca
            .replace_all(&pat, &val)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn str_to_uppercase(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca.to_uppercase().into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn str_to_lowercase(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca.to_lowercase().into_series();
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn str_hex_encode(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca.hex_encode().into_series();
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn str_hex_decode(&self, env: Env, strict: bool) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca
            .hex_decode(strict)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(s, &env))
    }
    #[napi]
    pub fn str_base64_encode(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca.base64_encode().into_series();
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn str_base64_decode(&self, env: Env, strict: bool) -> napi::Result<JsSeries> {
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca
            .base64_decode(strict)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn str_pad_start(
        &self,
        env: Env,
        length: Vec<i64>,
        fill_char: String,
    ) -> napi::Result<JsSeries> {
        let fill_char = parse_fill_char(fill_char)?;
        let vec_ulen = length.into_iter().map(|x| x as u64).collect();
        let chunked_len = UInt64Chunked::from_vec("str_pad_start_length".into(), vec_ulen);
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca.pad_start(&chunked_len, fill_char).into_series();
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn str_pad_end(
        &self,
        env: Env,
        length: Vec<i64>,
        fill_char: String,
    ) -> napi::Result<JsSeries> {
        let fill_char = parse_fill_char(fill_char)?;
        let vec_ulen = length.into_iter().map(|x| x as u64).collect();
        let chunked_len = UInt64Chunked::from_vec("str_pad_start_length".into(), vec_ulen);
        let ca = self.series.str().map_err(JsPolarsErr::from)?;
        let s = ca.pad_end(&chunked_len, fill_char).into_series();
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn strftime(&self, env: Env, fmt: String) -> napi::Result<JsSeries> {
        let s = self.series.strftime(&fmt).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn arr_lengths(&self, env: Env) -> napi::Result<JsSeries> {
        let ca = self.series.list().map_err(JsPolarsErr::from)?;
        let s = ca.lst_lengths().into_series();
        Ok(JsSeries::reported(s, &env))
    }
    // #[napi(catch_unwind)]
    // pub fn timestamp(&self, tu: Wrap<TimeUnit>) -> napi::Result<JsSeries> {
    //   let ca = self.series.timestamp(tu.0).map_err(JsPolarsErr::from)?;
    //   Ok(ca.into_series().into())
    // }
    #[napi(catch_unwind)]
    pub fn to_dummies(
        &self,
        env: Env,
        separator: Option<String>,
        drop_first: bool,
        drop_nulls: bool,
    ) -> napi::Result<JsDataFrame> {
        let df = self
            .series
            .to_dummies(separator.as_deref(), drop_first, drop_nulls)
            .map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::reported(df, &env))
    }
    #[napi(catch_unwind)]
    pub fn get_list(&self, env: Env, index: i64) -> Option<JsSeries> {
        if let Ok(ca) = &self.series.list() {
            Some(JsSeries::reported(ca.get_as_series(index as usize)?, &env))
        } else {
            None
        }
    }
    #[napi(catch_unwind)]
    pub fn year(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.year().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn month(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.month().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn weekday(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.weekday().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn week(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.week().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn day(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.day().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn ordinal_day(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.ordinal_day().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn hour(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.hour().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn minute(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.minute().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn second(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.second().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn nanosecond(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.nanosecond().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s.into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn dt_epoch_seconds(&self, env: Env) -> napi::Result<JsSeries> {
        let ms = self
            .series
            .timestamp(TimeUnit::Milliseconds)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported((ms / 1000).into_series(), &env))
    }
    #[napi(catch_unwind)]
    pub fn n_unique(&self) -> napi::Result<i64> {
        let n = self.series.n_unique().map_err(JsPolarsErr::from)?;
        Ok(n as i64)
    }

    #[napi(catch_unwind)]
    pub fn is_first_distinct(&self, env: Env) -> napi::Result<JsSeries> {
        let out = is_first_distinct(&self.series)
            .map_err(JsPolarsErr::from)?
            .into_series();
        Ok(JsSeries::reported(out, &env))
    }

    #[napi(catch_unwind)]
    pub fn round(&self, env: Env, decimals: u32, mode: Wrap<RoundMode>) -> napi::Result<JsSeries> {
        let s = self
            .series
            .round(decimals, mode.0)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn floor(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.floor().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn ceil(&self, env: Env) -> napi::Result<JsSeries> {
        let s = self.series.ceil().map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }
    #[napi(catch_unwind)]
    pub fn shrink_to_fit(&mut self) {
        self.series.shrink_to_fit();
    }

    #[napi(catch_unwind)]
    pub fn dot(&self, other: &JsSeries) -> napi::Result<f64> {
        let s = self.series.dot(&other.series).map_err(JsPolarsErr::from)?;
        Ok(s)
    }

    #[napi(catch_unwind)]
    pub fn hash(
        &self,
        env: Env,
        k0: Wrap<u64>,
        k1: Wrap<u64>,
        k2: Wrap<u64>,
        k3: Wrap<u64>,
    ) -> JsSeries {
        let seed = PlFixedStateQuality::default().hash_one((k0.0, k1.0, k2.0, k3.0));
        let hb = PlSeedableRandomStateQuality::seed_from_u64(seed);
        JsSeries::reported(self.series.hash(hb).into_series(), &env)
    }
    #[napi(catch_unwind)]
    pub fn reinterpret(&self, env: Env, signed: bool) -> napi::Result<JsSeries> {
        let dtype = if signed {
            DataType::Int64
        } else {
            DataType::UInt64
        };

        let s = reinterpret(&self.series, &dtype).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn mode(&self, env: Env) -> napi::Result<JsSeries> {
        let s = mode::mode(&self.series, false).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn rank(
        &self,
        env: Env,
        method: Wrap<RankMethod>,
        descending: Option<bool>,
        seed: Option<Wrap<u64>>,
    ) -> napi::Result<JsSeries> {
        let descending = descending.unwrap_or(false);
        let seed = map_seed(seed);
        let options = RankOptions {
            method: method.0,
            descending: descending,
        };
        Ok(JsSeries::reported(self.series.rank(options, seed), &env))
    }
    #[napi(catch_unwind)]
    pub fn diff(
        &self,
        env: Env,
        n: i64,
        null_behavior: Wrap<NullBehavior>,
    ) -> napi::Result<JsSeries> {
        let s = diff(&self.series, n, null_behavior.0).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn skew(&self, bias: bool) -> napi::Result<Option<f64>> {
        let out = self.series.skew(bias).map_err(JsPolarsErr::from)?;
        Ok(out)
    }

    #[napi(catch_unwind)]
    pub fn kurtosis(&self, fisher: bool, bias: bool) -> napi::Result<Option<f64>> {
        let out = self
            .series
            .kurtosis(fisher, bias)
            .map_err(JsPolarsErr::from)?;
        Ok(out)
    }

    #[napi(catch_unwind)]
    pub fn cast(
        &self,
        env: Env,
        dtype: Wrap<DataType>,
        strict: Option<bool>,
    ) -> napi::Result<JsSeries> {
        let strict = strict.unwrap_or(false);
        let dtype = dtype.0;
        let out = if strict {
            self.series.strict_cast(&dtype)
        } else {
            self.series.cast(&dtype)
        };
        let out = out.map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }

    #[napi(catch_unwind)]
    pub fn abs(&self, env: Env) -> napi::Result<JsSeries> {
        let s = abs(&self.series).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(s, &env))
    }

    #[napi(catch_unwind)]
    pub fn reshape(&self, env: Env, dims: Vec<i64>) -> napi::Result<JsSeries> {
        let dims = dims
            .into_iter()
            .map(ReshapeDimension::new)
            .collect::<Vec<_>>();
        let out = self.series.reshape_list(&dims).map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }

    #[napi(catch_unwind)]
    pub fn shuffle(&self, env: Env, seed: Wrap<u64>) -> JsSeries {
        JsSeries::reported(self.series.shuffle(Some(seed.0)), &env)
    }

    #[napi(catch_unwind)]
    pub fn extend_constant(
        &self,
        env: Env,
        value: Wrap<AnyValue>,
        n: i64,
    ) -> napi::Result<JsSeries> {
        let out = self
            .series
            .extend_constant(value.0, n as usize)
            .map_err(JsPolarsErr::from)?;
        Ok(JsSeries::reported(out, &env))
    }

    #[napi(catch_unwind)]
    pub fn time_unit(&self) -> Option<String> {
        if let DataType::Datetime(tu, _) | DataType::Duration(tu) = self.series.dtype() {
            Some(
                match tu {
                    TimeUnit::Nanoseconds => "ns",
                    TimeUnit::Microseconds => "us",
                    TimeUnit::Milliseconds => "ms",
                }
                .to_owned(),
            )
        } else {
            None
        }
    }
    #[napi(catch_unwind)]
    pub fn scatter(&mut self, idx: &JsSeries, values: &JsSeries) -> napi::Result<()> {
        // we take the value because we want a ref count
        // of 1 so that we can have mutable access
        let s = std::mem::take(&mut self.series);
        match crate::set::scatter(s, &idx.series, &values.series) {
            Ok(out) => {
                self.series = out;
                Ok(())
            }
            Err(e) => Err(napi::Error::from_reason(e.to_string())),
        }
    }
}

macro_rules! impl_set_with_mask_wrap {
    ($name:ident, $native:ty, $cast:ident) => {
        #[napi(catch_unwind)]
        pub fn $name(
            env: Env,
            series: &JsSeries,
            mask: &JsSeries,
            value: Option<Wrap<$native>>,
        ) -> napi::Result<JsSeries> {
            let value = value.map(|v| v.0);
            let mask = mask.series.bool().map_err(JsPolarsErr::from)?;
            let ca = series.series.$cast().map_err(JsPolarsErr::from)?;
            let new = ca
                .set(mask, value)
                .map_err(JsPolarsErr::from)?
                .into_series();
            Ok(JsSeries::reported(new, &env))
        }
    };
}

macro_rules! impl_set_with_mask {
    ($name:ident, $native:ty, $cast:ident) => {
        #[napi(catch_unwind)]
        pub fn $name(
            env: Env,
            series: &JsSeries,
            mask: &JsSeries,
            value: Option<$native>,
        ) -> napi::Result<JsSeries> {
            let mask = mask.series.bool().map_err(JsPolarsErr::from)?;
            let ca = series.series.$cast().map_err(JsPolarsErr::from)?;
            let new = ca
                .set(mask, value)
                .map_err(JsPolarsErr::from)?
                .into_series();
            Ok(JsSeries::reported(new, &env))
        }
    };
}

// impl_set_with_mask!(series_set_with_mask_str, String, str);
impl_set_with_mask!(series_set_with_mask_f64, f64, f64);
impl_set_with_mask_wrap!(series_set_with_mask_f32, f32, f32);
impl_set_with_mask_wrap!(series_set_with_mask_u8, u8, u8);
impl_set_with_mask_wrap!(series_set_with_mask_u16, u16, u16);
impl_set_with_mask!(series_set_with_mask_u32, u32, u32);
impl_set_with_mask_wrap!(series_set_with_mask_u64, u64, u64);
impl_set_with_mask_wrap!(series_set_with_mask_i8, i8, i8);
impl_set_with_mask_wrap!(series_set_with_mask_i16, i16, i16);
impl_set_with_mask!(series_set_with_mask_i32, i32, i32);
impl_set_with_mask!(series_set_with_mask_i64, i64, i64);

macro_rules! impl_get {
    ($name:ident, $series_variant:ident, $type:ty, $cast:ty) => {
        #[napi(catch_unwind)]
        pub fn $name(s: &JsSeries, index: i64) -> Option<$cast> {
            if let Ok(ca) = s.series.$series_variant() {
                let index = if index < 0 {
                    (ca.len() as i64 + index) as usize
                } else {
                    index as usize
                };
                ca.get(index).map(|s| s as $cast)
            } else {
                None
            }
        }
    };
}

impl_get!(series_get_f32, f32, f32, f64);
impl_get!(series_get_f64, f64, f64, f64);
impl_get!(series_get_u8, u8, u8, i32);
impl_get!(series_get_u16, u16, u16, u32);
impl_get!(series_get_u32, u32, u32, u32);
impl_get!(series_get_u64, u64, u64, i64);
impl_get!(series_get_i8, i8, i8, i32);
impl_get!(series_get_i16, i16, i16, i32);
impl_get!(series_get_i32, i32, i32, i32);
impl_get!(series_get_i64, i64, i64, i32);
impl_get!(series_get_str, str, utf8, &str);
// impl_get!(series_get_date, date, i32, i32);
// impl_get!(series_get_datetime, datetime, i64, i64);
// impl_get!(series_get_duration, duration, i64, i64);

macro_rules! impl_arithmetic {
  ($name:ident, $type:ty, $operand:tt) => {
      #[napi(catch_unwind)]
      pub fn $name(env: Env, s: &JsSeries, other: Wrap<AnyValue>) -> napi::Result<JsSeries> {
          let other: $type = other.try_into()?;
          Ok(JsSeries::reported(&s.series $operand other, &env))
      }
  };
}
impl_arithmetic!(series_add_u8, u8, +);
impl_arithmetic!(series_add_u16, u16, +);
impl_arithmetic!(series_add_u32, u32, +);
impl_arithmetic!(series_add_u64, u64, +);
impl_arithmetic!(series_add_i8, i8, +);
impl_arithmetic!(series_add_i16, i16, +);
impl_arithmetic!(series_add_i32, i32, +);
impl_arithmetic!(series_add_i64, i64, +);
impl_arithmetic!(series_add_datetime, i64, +);
impl_arithmetic!(series_add_duration, i64, +);
impl_arithmetic!(series_add_f32, f32, +);
impl_arithmetic!(series_add_f64, f64, +);
impl_arithmetic!(series_sub_u8, u8, -);
impl_arithmetic!(series_sub_u16, u16, -);
impl_arithmetic!(series_sub_u32, u32, -);
impl_arithmetic!(series_sub_u64, u64, -);
impl_arithmetic!(series_sub_i8, i8, -);
impl_arithmetic!(series_sub_i16, i16, -);
impl_arithmetic!(series_sub_i32, i32, -);
impl_arithmetic!(series_sub_i64, i64, -);
impl_arithmetic!(series_sub_datetime, i64, -);
impl_arithmetic!(series_sub_duration, i64, -);
impl_arithmetic!(series_sub_f32, f32, -);
impl_arithmetic!(series_sub_f64, f64, -);
impl_arithmetic!(series_div_u8, u8, /);
impl_arithmetic!(series_div_u16, u16, /);
impl_arithmetic!(series_div_u32, u32, /);
impl_arithmetic!(series_div_u64, u64, /);
impl_arithmetic!(series_div_i8, i8, /);
impl_arithmetic!(series_div_i16, i16, /);
impl_arithmetic!(series_div_i32, i32, /);
impl_arithmetic!(series_div_i64, i64, /);
impl_arithmetic!(series_div_f32, f32, /);
impl_arithmetic!(series_div_f64, f64, /);
impl_arithmetic!(series_mul_u8, u8, *);
impl_arithmetic!(series_mul_u16, u16, *);
impl_arithmetic!(series_mul_u32, u32, *);
impl_arithmetic!(series_mul_u64, u64, *);
impl_arithmetic!(series_mul_i8, i8, *);
impl_arithmetic!(series_mul_i16, i16, *);
impl_arithmetic!(series_mul_i32, i32, *);
impl_arithmetic!(series_mul_i64, i64, *);
impl_arithmetic!(series_mul_f32, f32, *);
impl_arithmetic!(series_mul_f64, f64, *);
impl_arithmetic!(series_rem_u8, u8, %);
impl_arithmetic!(series_rem_u16, u16, %);
impl_arithmetic!(series_rem_u32, u32, %);
impl_arithmetic!(series_rem_u64, u64, %);
impl_arithmetic!(series_rem_i8, i8, %);
impl_arithmetic!(series_rem_i16, i16, %);
impl_arithmetic!(series_rem_i32, i32, %);
impl_arithmetic!(series_rem_i64, i64, %);
impl_arithmetic!(series_rem_f32, f32, %);
impl_arithmetic!(series_rem_f64, f64, %);

macro_rules! impl_rhs_arithmetic {
    ($name:ident, $type:ty, $operand:ident) => {
        #[napi(catch_unwind)]

        pub fn $name(env: Env, s: &JsSeries, other: Wrap<AnyValue>) -> napi::Result<JsSeries> {
            let other: $type = other.try_into()?;
            Ok(JsSeries::reported(other.$operand(&s.series), &env))
        }
    };
}

impl_rhs_arithmetic!(series_add_u8_rhs, u8, add);
impl_rhs_arithmetic!(series_add_u16_rhs, u16, add);
impl_rhs_arithmetic!(series_add_u32_rhs, u32, add);
impl_rhs_arithmetic!(series_add_u64_rhs, u64, add);
impl_rhs_arithmetic!(series_add_i8_rhs, i8, add);
impl_rhs_arithmetic!(series_add_i16_rhs, i16, add);
impl_rhs_arithmetic!(series_add_i32_rhs, i32, add);
impl_rhs_arithmetic!(series_add_i64_rhs, i64, add);
impl_rhs_arithmetic!(series_add_f32_rhs, f32, add);
impl_rhs_arithmetic!(series_add_f64_rhs, f64, add);
impl_rhs_arithmetic!(series_sub_u8_rhs, u8, sub);
impl_rhs_arithmetic!(series_sub_u16_rhs, u16, sub);
impl_rhs_arithmetic!(series_sub_u32_rhs, u32, sub);
impl_rhs_arithmetic!(series_sub_u64_rhs, u64, sub);
impl_rhs_arithmetic!(series_sub_i8_rhs, i8, sub);
impl_rhs_arithmetic!(series_sub_i16_rhs, i16, sub);
impl_rhs_arithmetic!(series_sub_i32_rhs, i32, sub);
impl_rhs_arithmetic!(series_sub_i64_rhs, i64, sub);
impl_rhs_arithmetic!(series_sub_f32_rhs, f32, sub);
impl_rhs_arithmetic!(series_sub_f64_rhs, f64, sub);
impl_rhs_arithmetic!(series_div_u8_rhs, u8, div);
impl_rhs_arithmetic!(series_div_u16_rhs, u16, div);
impl_rhs_arithmetic!(series_div_u32_rhs, u32, div);
impl_rhs_arithmetic!(series_div_u64_rhs, u64, div);
impl_rhs_arithmetic!(series_div_i8_rhs, i8, div);
impl_rhs_arithmetic!(series_div_i16_rhs, i16, div);
impl_rhs_arithmetic!(series_div_i32_rhs, i32, div);
impl_rhs_arithmetic!(series_div_i64_rhs, i64, div);
impl_rhs_arithmetic!(series_div_f32_rhs, f32, div);
impl_rhs_arithmetic!(series_div_f64_rhs, f64, div);
impl_rhs_arithmetic!(series_mul_u8_rhs, u8, mul);
impl_rhs_arithmetic!(series_mul_u16_rhs, u16, mul);
impl_rhs_arithmetic!(series_mul_u32_rhs, u32, mul);
impl_rhs_arithmetic!(series_mul_u64_rhs, u64, mul);
impl_rhs_arithmetic!(series_mul_i8_rhs, i8, mul);
impl_rhs_arithmetic!(series_mul_i16_rhs, i16, mul);
impl_rhs_arithmetic!(series_mul_i32_rhs, i32, mul);
impl_rhs_arithmetic!(series_mul_i64_rhs, i64, mul);
impl_rhs_arithmetic!(series_mul_f32_rhs, f32, mul);
impl_rhs_arithmetic!(series_mul_f64_rhs, f64, mul);
impl_rhs_arithmetic!(series_rem_u8_rhs, u8, rem);
impl_rhs_arithmetic!(series_rem_u16_rhs, u16, rem);
impl_rhs_arithmetic!(series_rem_u32_rhs, u32, rem);
impl_rhs_arithmetic!(series_rem_u64_rhs, u64, rem);
impl_rhs_arithmetic!(series_rem_i8_rhs, i8, rem);
impl_rhs_arithmetic!(series_rem_i16_rhs, i16, rem);
impl_rhs_arithmetic!(series_rem_i32_rhs, i32, rem);
impl_rhs_arithmetic!(series_rem_i64_rhs, i64, rem);
impl_rhs_arithmetic!(series_rem_f32_rhs, f32, rem);
impl_rhs_arithmetic!(series_rem_f64_rhs, f64, rem);

macro_rules! impl_eq_num {
    ($name:ident, $type:ty) => {
        #[napi(catch_unwind)]
        pub fn $name(env: Env, s: &JsSeries, rhs: Wrap<AnyValue>) -> napi::Result<JsSeries> {
            let rhs: $type = rhs.try_into()?;
            Ok(JsSeries::reported(
                s.series
                    .equal(rhs)
                    .map_err(JsPolarsErr::from)?
                    .into_series(),
                &env,
            ))
        }
    };
}

impl_eq_num!(series_eq_u8, u8);
impl_eq_num!(series_eq_u16, u16);
impl_eq_num!(series_eq_u32, u32);
impl_eq_num!(series_eq_u64, u64);
impl_eq_num!(series_eq_i8, i8);
impl_eq_num!(series_eq_i16, i16);
impl_eq_num!(series_eq_i32, i32);
impl_eq_num!(series_eq_i64, i64);
impl_eq_num!(series_eq_f32, f32);
impl_eq_num!(series_eq_f64, f64);
impl_eq_num!(series_eq_str, &str);

macro_rules! impl_neq_num {
    ($name:ident, $type:ty) => {
        #[napi(catch_unwind)]
        pub fn $name(env: Env, s: &JsSeries, rhs: Wrap<AnyValue>) -> napi::Result<JsSeries> {
            let rhs: $type = rhs.try_into()?;
            Ok(JsSeries::reported(
                s.series
                    .not_equal(rhs)
                    .map_err(JsPolarsErr::from)?
                    .into_series(),
                &env,
            ))
        }
    };
}
impl_neq_num!(series_neq_u8, u8);
impl_neq_num!(series_neq_u16, u16);
impl_neq_num!(series_neq_u32, u32);
impl_neq_num!(series_neq_u64, u64);
impl_neq_num!(series_neq_i8, i8);
impl_neq_num!(series_neq_i16, i16);
impl_neq_num!(series_neq_i32, i32);
impl_neq_num!(series_neq_i64, i64);
impl_neq_num!(series_neq_f32, f32);
impl_neq_num!(series_neq_f64, f64);
impl_neq_num!(series_neq_str, &str);

macro_rules! impl_gt_num {
    ($name:ident, $type:ty) => {
        #[napi(catch_unwind)]
        pub fn $name(env: Env, s: &JsSeries, rhs: Wrap<AnyValue>) -> napi::Result<JsSeries> {
            let rhs: $type = rhs.try_into()?;
            Ok(JsSeries::reported(
                s.series.gt(rhs).map_err(JsPolarsErr::from)?.into_series(),
                &env,
            ))
        }
    };
}
impl_gt_num!(series_gt_u8, u8);
impl_gt_num!(series_gt_u16, u16);
impl_gt_num!(series_gt_u32, u32);
impl_gt_num!(series_gt_u64, u64);
impl_gt_num!(series_gt_i8, i8);
impl_gt_num!(series_gt_i16, i16);
impl_gt_num!(series_gt_i32, i32);
impl_gt_num!(series_gt_i64, i64);
impl_gt_num!(series_gt_f32, f32);
impl_gt_num!(series_gt_f64, f64);
impl_gt_num!(series_gt_str, &str);

macro_rules! impl_gt_eq_num {
    ($name:ident, $type:ty) => {
        #[napi(catch_unwind)]
        pub fn $name(env: Env, s: &JsSeries, rhs: Wrap<AnyValue>) -> napi::Result<JsSeries> {
            let rhs: $type = rhs.try_into()?;
            Ok(JsSeries::reported(
                s.series
                    .gt_eq(rhs)
                    .map_err(JsPolarsErr::from)?
                    .into_series(),
                &env,
            ))
        }
    };
}
impl_gt_eq_num!(series_gt_eq_u8, u8);
impl_gt_eq_num!(series_gt_eq_u16, u16);
impl_gt_eq_num!(series_gt_eq_u32, u32);
impl_gt_eq_num!(series_gt_eq_u64, u64);
impl_gt_eq_num!(series_gt_eq_i8, i8);
impl_gt_eq_num!(series_gt_eq_i16, i16);
impl_gt_eq_num!(series_gt_eq_i32, i32);
impl_gt_eq_num!(series_gt_eq_i64, i64);
impl_gt_eq_num!(series_gt_eq_f32, f32);
impl_gt_eq_num!(series_gt_eq_f64, f64);
impl_gt_eq_num!(series_gt_eq_str, &str);

macro_rules! impl_lt_num {
    ($name:ident, $type:ty) => {
        #[napi(catch_unwind)]
        pub fn $name(env: Env, s: &JsSeries, rhs: Wrap<AnyValue>) -> napi::Result<JsSeries> {
            let rhs: $type = rhs.try_into()?;
            Ok(JsSeries::reported(
                s.series.lt(rhs).map_err(JsPolarsErr::from)?.into_series(),
                &env,
            ))
        }
    };
}
impl_lt_num!(series_lt_u8, u8);
impl_lt_num!(series_lt_u16, u16);
impl_lt_num!(series_lt_u32, u32);
impl_lt_num!(series_lt_u64, u64);
impl_lt_num!(series_lt_i8, i8);
impl_lt_num!(series_lt_i16, i16);
impl_lt_num!(series_lt_i32, i32);
impl_lt_num!(series_lt_i64, i64);
impl_lt_num!(series_lt_f32, f32);
impl_lt_num!(series_lt_f64, f64);
impl_lt_num!(series_lt_str, &str);

macro_rules! impl_lt_eq_num {
    ($name:ident, $type:ty) => {
        #[napi(catch_unwind)]
        pub fn $name(env: Env, s: &JsSeries, rhs: Wrap<AnyValue>) -> napi::Result<JsSeries> {
            let rhs: $type = rhs.try_into()?;
            Ok(JsSeries::reported(
                s.series
                    .lt_eq(rhs)
                    .map_err(JsPolarsErr::from)?
                    .into_series(),
                &env,
            ))
        }
    };
}
impl_lt_eq_num!(series_lt_eq_u8, u8);
impl_lt_eq_num!(series_lt_eq_u16, u16);
impl_lt_eq_num!(series_lt_eq_u32, u32);
impl_lt_eq_num!(series_lt_eq_u64, u64);
impl_lt_eq_num!(series_lt_eq_i8, i8);
impl_lt_eq_num!(series_lt_eq_i16, i16);
impl_lt_eq_num!(series_lt_eq_i32, i32);
impl_lt_eq_num!(series_lt_eq_i64, i64);
impl_lt_eq_num!(series_lt_eq_f32, f32);
impl_lt_eq_num!(series_lt_eq_f64, f64);
impl_lt_eq_num!(series_lt_eq_str, &str);
