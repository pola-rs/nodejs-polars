use crate::lazy::dsl::JsExpr;
use crate::prelude::*;
use napi::bindgen_prelude::*;
use polars::prelude::NullStrategy;
use polars::prelude::*;
use polars_core::series::ops::NullBehavior;
use polars_io::cloud::CloudOptions;
use polars_io::utils::sync_on_close::SyncOnCloseType;
use polars_io::RowIndex;
use std::collections::HashMap;
use std::num::NonZero;

#[derive(Debug)]
pub struct Wrap<T: ?Sized>(pub T);

impl<T> Clone for Wrap<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Wrap(self.0.clone())
    }
}
impl<T> From<T> for Wrap<T> {
    fn from(t: T) -> Self {
        Wrap(t)
    }
}
impl TypeName for Wrap<StringChunked> {
    fn type_name() -> &'static str {
        "StringChunked"
    }

    fn value_type() -> ValueType {
        ValueType::Object
    }
}
/// Safety.
/// it is up to the consumer to make sure the item is valid
pub(crate) trait ToSeries {
    fn to_series(&self, name: &str) -> Series;
}

impl ToSeries for Array<'_> {
    fn to_series(&self, name: &str) -> Series {
        let len = self.len();
        let mut v: Vec<AnyValue> = Vec::with_capacity(len as usize);
        for i in 0..len {
            let av: Wrap<AnyValue> = self.get(i).unwrap().unwrap_or(Wrap(AnyValue::Null));
            v.push(av.0);
        }
        Series::new(name.into(), v)
    }
}

impl ToSeries for Unknown<'_> {
    fn to_series(&self, name: &str) -> Series {
        let obj = unsafe { self.cast::<Object>() };
        let len = obj.as_ref().unwrap().get_array_length_unchecked().unwrap();
        let mut v: Vec<AnyValue> = Vec::with_capacity(len as usize);
        for i in 0..len {
            let unknown: Unknown = obj.as_ref().unwrap().clone().get_element(i).unwrap();
            let av = AnyValue::from_js(unknown).unwrap();
            v.push(av);
        }
        Series::new(name.into(), v)
    }
}

impl ToNapiValue for Wrap<&Series> {
    unsafe fn to_napi_value(napi_env: sys::napi_env, val: Self) -> napi::Result<sys::napi_value> {
        let s = val.0.rechunk();
        let len = s.len();
        let dtype = s.dtype();
        let env = Env::from_raw(napi_env);

        match dtype {
            DataType::Struct(_) => {
                let ca = s.struct_().map_err(JsPolarsErr::from)?;
                let df: DataFrame = ca.clone().unnest();

                let (height, _) = df.shape();
                let mut rows = env.create_array(height as u32)?;

                for idx in 0..height {
                    let mut row = Object::new(&env)?;
                    for col in df.get_columns() {
                        let key = col.name();
                        let val = col.get(idx);
                        row.set(key, Wrap(val.unwrap()))?;
                    }
                    rows.set(idx as u32, row)?;
                }
                Array::to_napi_value(napi_env, rows)
            }
            _ => {
                let mut arr = env.create_array(len as u32)?;

                for (idx, val) in s.iter().enumerate() {
                    arr.set(idx as u32, Wrap(val))?;
                }
                Array::to_napi_value(napi_env, arr)
            }
        }
    }
}

impl<'a> ToNapiValue for Wrap<AnyValue<'a>> {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        match val.0 {
            AnyValue::Null => napi::bindgen_prelude::Null::to_napi_value(env, napi::bindgen_prelude::Null),
            AnyValue::Boolean(b) => bool::to_napi_value(env, b),
            AnyValue::Int8(n) => i32::to_napi_value(env, n as i32),
            AnyValue::Int16(n) => i32::to_napi_value(env, n as i32),
            AnyValue::Int32(n) => i32::to_napi_value(env, n),
            AnyValue::Int64(n) => i64::to_napi_value(env, n),
            AnyValue::Int128(n) => i128::to_napi_value(env, n),
            AnyValue::UInt8(n) => u32::to_napi_value(env, n as u32),
            AnyValue::UInt16(n) => u32::to_napi_value(env, n as u32),
            AnyValue::UInt32(n) => u32::to_napi_value(env, n),
            AnyValue::UInt64(n) => u64::to_napi_value(env, n),
            AnyValue::Float32(n) => f64::to_napi_value(env, n as f64),
            AnyValue::Float64(n) => f64::to_napi_value(env, n),
            AnyValue::String(s) => String::to_napi_value(env, s.to_owned()),
            AnyValue::StringOwned(s) => String::to_napi_value(env, s.to_string()),
            AnyValue::Date(v) => {
                let mut ptr = std::ptr::null_mut();

                // Multiple days to get to Epoch time
                let epoch_time: f64 = (v as f64) * 86400000.0;

                check_status!(
                    napi::sys::napi_create_date(env, epoch_time, &mut ptr),
                    "Failed to convert rust type `AnyValue::Date` into napi value",
                )?;
                Ok(ptr)
            }
            AnyValue::Datetime(v, _, _) => {
                let mut js_value = std::ptr::null_mut();
                napi::sys::napi_create_date(env, v as f64, &mut js_value);

                Ok(js_value)
            }
            AnyValue::Categorical(cat, &ref lmap) | AnyValue::CategoricalOwned(cat, ref lmap) => {
                let s = unsafe { lmap.cat_to_str_unchecked(cat) };
                let ptr = String::to_napi_value(env, s.to_string());
                Ok(ptr.unwrap())
            },
            AnyValue::Duration(v, _) => i64::to_napi_value(env, v),
            AnyValue::Time(v) => i64::to_napi_value(env, v),
            AnyValue::List(ser) => Wrap::<&Series>::to_napi_value(env, Wrap(&ser)),
            ref av @ AnyValue::Struct(_, _, flds) => struct_dict(env, av._iter_struct_av(), flds),
            AnyValue::Array(ser, _) => Wrap::<&Series>::to_napi_value(env, Wrap(&ser)),
            AnyValue::Enum(_, _) => Err(napi::Error::from_reason("Enum is not a supported, please convert to string or number before collecting to js")),
            AnyValue::Object(_) => Err(napi::Error::from_reason("Object is not a supported, please convert to string or number before collecting to js")),
            AnyValue::ObjectOwned(_) => Err(napi::Error::from_reason("ObjectOwned is not a supported, please convert to string or number before collecting to js")),
            AnyValue::StructOwned(_) => Err(napi::Error::from_reason("StructOwned is not a supported, please convert to string or number before collecting to js")),
            AnyValue::Binary(_) => Err(napi::Error::from_reason("Binary is not a supported, please convert to string or number before collecting to js")),
            AnyValue::BinaryOwned(_) => Err(napi::Error::from_reason("BinaryOwned is not a supported, please convert to string or number before collecting to js")),
            AnyValue::Decimal(_, _) => Err(napi::Error::from_reason("Decimal is not a supported type in javascript, please convert to string or number before collecting to js")),
            AnyValue::DatetimeOwned(_,_,_) => Err(napi::Error::from_reason("DatetimeOwned is not a supported, please convert to string or number before collecting to js")),
            AnyValue::EnumOwned(_,_) => Err(napi::Error::from_reason("EnumOwned is not a supported, please convert to string or number before collecting to js")),
        }
    }
}

impl FromNapiValue for Wrap<StringChunked> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let arr = Array::from_napi_value(env, napi_val)?;
        let len = arr.len() as usize;
        let mut builder = StringChunkedBuilder::new(PlSmallStr::EMPTY, len);
        for i in 0..len {
            match arr.get::<String>(i as u32) {
                Ok(val) => match val {
                    Some(str_val) => builder.append_value(str_val),
                    None => builder.append_null(),
                },
                Err(_) => builder.append_null(),
            }
        }

        Ok(Wrap(builder.finish()))
    }
}
impl FromNapiValue for Wrap<BooleanChunked> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let arr = Array::from_napi_value(env, napi_val)?;
        let len = arr.len() as usize;
        let mut builder = BooleanChunkedBuilder::new(PlSmallStr::EMPTY, len);
        for i in 0..len {
            match arr.get::<bool>(i as u32) {
                Ok(val) => match val {
                    Some(str_val) => builder.append_value(str_val),
                    None => builder.append_null(),
                },
                Err(_) => builder.append_null(),
            }
        }

        Ok(Wrap(builder.finish()))
    }
}

impl FromNapiValue for Wrap<Float32Chunked> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let arr = Array::from_napi_value(env, napi_val)?;
        let len = arr.len() as usize;
        let mut builder = PrimitiveChunkedBuilder::<Float32Type>::new(PlSmallStr::EMPTY, len);
        for i in 0..len {
            match arr.get::<f64>(i as u32) {
                Ok(val) => match val {
                    Some(v) => builder.append_value(v as f32),
                    None => builder.append_null(),
                },
                Err(_) => builder.append_null(),
            }
        }

        Ok(Wrap(builder.finish()))
    }
}
macro_rules! impl_chunked {
    ($type:ty, $native:ty) => {
        impl FromNapiValue for Wrap<ChunkedArray<$type>> {
            unsafe fn from_napi_value(
                env: sys::napi_env,
                napi_val: sys::napi_value,
            ) -> JsResult<Self> {
                let arr = Array::from_napi_value(env, napi_val)?;
                let len = arr.len() as usize;
                let mut builder = PrimitiveChunkedBuilder::<$type>::new(PlSmallStr::EMPTY, len);
                for i in 0..len {
                    match arr.get::<$native>(i as u32) {
                        Ok(val) => match val {
                            Some(v) => builder.append_value(v),
                            None => builder.append_null(),
                        },
                        Err(_) => builder.append_null(),
                    }
                }
                Ok(Wrap(builder.finish()))
            }
        }
    };
}
impl_chunked!(Float64Type, f64);
impl_chunked!(Int32Type, i32);
impl_chunked!(UInt32Type, u32);
impl_chunked!(Int64Type, i64);

impl FromNapiValue for Wrap<ChunkedArray<UInt64Type>> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let arr = Array::from_napi_value(env, napi_val)?;
        let len = arr.len() as usize;
        let mut builder = PrimitiveChunkedBuilder::<UInt64Type>::new(PlSmallStr::EMPTY, len);
        for i in 0..len {
            match arr.get::<BigInt>(i as u32) {
                Ok(val) => match val {
                    Some(v) => {
                        let (_, v, _) = v.get_u64();
                        builder.append_value(v)
                    }
                    None => builder.append_null(),
                },
                Err(_) => builder.append_null(),
            }
        }
        Ok(Wrap(builder.finish()))
    }
}

impl FromNapiValue for Wrap<Expr> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let obj = Object::from_napi_value(env, napi_val)?;
        let expr: &JsExpr = obj
            .get("_expr")?
            .expect(&format!("field {} should exist", "_expr"));
        Ok(Wrap(expr.inner.clone()))
    }
}

impl FromNapiValue for Wrap<JsExpr> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let obj = Object::from_napi_value(env, napi_val)?;
        let expr: &JsExpr = obj
            .get("_expr")?
            .expect(&format!("field {} should exist", "_expr"));
        Ok(Wrap(expr.clone()))
    }
}

impl TypeName for Wrap<QuantileMethod> {
    fn type_name() -> &'static str {
        "QuantileMethod"
    }

    fn value_type() -> ValueType {
        ValueType::Object
    }
}

impl FromNapiValue for Wrap<QuantileMethod> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let interpolation = String::from_napi_value(env, napi_val)?;
        let interpol = match interpolation.as_ref() {
            "nearest" => QuantileMethod::Nearest,
            "lower" => QuantileMethod::Lower,
            "higher" => QuantileMethod::Higher,
            "midpoint" => QuantileMethod::Midpoint,
            "linear" => QuantileMethod::Linear,
            _ => return Err(napi::Error::from_reason("not supported".to_owned())),
        };
        Ok(Wrap(interpol))
    }
}

impl FromNapiValue for Wrap<StartBy> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let start = String::from_napi_value(env, napi_val)?;
        let parsed = match start.as_ref() {
            "window" => StartBy::WindowBound,
            "datapoint" => StartBy::DataPoint,
            "monday" => StartBy::Monday,
            v => {
                return Err(napi::Error::from_reason(format!(
                    "closed must be one of {{'window', 'datapoint', 'monday'}}, got {v}",
                )))
            }
        };
        Ok(Wrap(parsed))
    }
}

impl FromNapiValue for Wrap<Label> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let start = String::from_napi_value(env, napi_val)?;
        let parsed = match start.as_ref() {
            "left" => Label::Left,
            "right" => Label::Right,
            "datapoint" => Label::DataPoint,
            v => {
                return Err(napi::Error::from_reason(format!(
                    "`label` must be one of {{'left', 'right', 'datapoint'}}, got {v}",
                )));
            }
        };
        Ok(Wrap(parsed))
    }
}

impl FromNapiValue for Wrap<ClosedWindow> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let s = String::from_napi_value(env, napi_val)?;
        let cw = match s.as_ref() {
            "none" => ClosedWindow::None,
            "both" => ClosedWindow::Both,
            "left" => ClosedWindow::Left,
            "right" => ClosedWindow::Right,
            _ => {
                return Err(napi::Error::from_reason(
                    "closed should be any of {'none', 'left', 'right', 'both'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(cw))
    }
}

impl FromNapiValue for Wrap<RankMethod> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let method = String::from_napi_value(env, napi_val)?;
        let method = match method.as_ref() {
            "min" => RankMethod::Min,
            "max" => RankMethod::Max,
            "average" => RankMethod::Average,
            "dense" => RankMethod::Dense,
            "ordinal" => RankMethod::Ordinal,
            "random" => RankMethod::Random,
            _ => {
                return Err(napi::Error::from_reason(
                    "use one of {'average', 'min', 'max', 'dense', 'ordinal', 'random'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(method))
    }
}

impl FromNapiValue for Wrap<ParquetCompression> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let compression = String::from_napi_value(env, napi_val)?;
        let compression = match compression.as_ref() {
            "snappy" => ParquetCompression::Snappy,
            "gzip" => ParquetCompression::Gzip(None),
            "lzo" => ParquetCompression::Lzo,
            "brotli" => ParquetCompression::Brotli(None),
            "lz4" => ParquetCompression::Lz4Raw,
            "zstd" => ParquetCompression::Zstd(None),
            _ => ParquetCompression::Uncompressed,
        };
        Ok(Wrap(compression))
    }
}

impl FromNapiValue for Wrap<Option<IpcCompression>> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let compression = String::from_napi_value(env, napi_val)?;
        let compression = match compression.as_ref() {
            "lz4" => Some(IpcCompression::LZ4),
            "zstd" => Some(IpcCompression::ZSTD),
            _ => None,
        };
        Ok(Wrap(compression))
    }
}

impl ToNapiValue for Wrap<Option<IpcCompression>> {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        let s = match val.0.unwrap() {
            IpcCompression::LZ4 => "lz4",
            IpcCompression::ZSTD => "zstd",
        };
        String::to_napi_value(env, s.to_owned())
    }
}

impl FromNapiValue for Wrap<UniqueKeepStrategy> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let method = String::from_napi_value(env, napi_val)?;
        let method = match method.as_ref() {
            "first" => UniqueKeepStrategy::First,
            "last" => UniqueKeepStrategy::Last,
            _ => {
                return Err(napi::Error::from_reason(
                    "use one of {'first', 'last'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(method))
    }
}

impl FromNapiValue for Wrap<NullStrategy> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let method = String::from_napi_value(env, napi_val)?;
        let method = match method.as_ref() {
            "ignore" => NullStrategy::Ignore,
            "propagate" => NullStrategy::Propagate,
            _ => {
                return Err(napi::Error::from_reason(
                    "use one of {'ignore', 'propagate'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(method))
    }
}

impl FromNapiValue for Wrap<NullBehavior> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let method = String::from_napi_value(env, napi_val)?;
        let method = match method.as_ref() {
            "drop" => NullBehavior::Drop,
            "ignore" => NullBehavior::Ignore,
            _ => {
                return Err(napi::Error::from_reason(
                    "use one of {'drop', 'ignore'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(method))
    }
}

impl FromNapiValue for Wrap<FillNullStrategy> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let method = String::from_napi_value(env, napi_val)?;
        let method = match method.as_ref() {
            "backward" => FillNullStrategy::Backward(None),
            "forward" => FillNullStrategy::Forward(None),
            "min" => FillNullStrategy::Min,
            "max" => FillNullStrategy::Max,
            "mean" => FillNullStrategy::Mean,
            "zero" => FillNullStrategy::Zero,
            "one" => FillNullStrategy::One,
            _ => {
                return Err(napi::Error::from_reason(
                    "Strategy not supported".to_owned(),
                ))
            }
        };
        Ok(Wrap(method))
    }
}

impl FromNapiValue for Wrap<u8> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let n = u32::from_napi_value(env, napi_val)?;
        Ok(Wrap(n as u8))
    }
}
impl FromNapiValue for Wrap<u16> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let n = u32::from_napi_value(env, napi_val)?;
        Ok(Wrap(n as u16))
    }
}
impl FromNapiValue for Wrap<i8> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let n = u32::from_napi_value(env, napi_val)?;
        Ok(Wrap(n as i8))
    }
}
impl FromNapiValue for Wrap<i16> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let n = u32::from_napi_value(env, napi_val)?;
        Ok(Wrap(n as i16))
    }
}
impl FromNapiValue for Wrap<f32> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let n = f64::from_napi_value(env, napi_val)?;
        Ok(Wrap(n as f32))
    }
}
impl FromNapiValue for Wrap<u64> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let big = BigInt::from_napi_value(env, napi_val)?;
        let (_, value, _) = big.get_u64();
        Ok(Wrap(value))
    }
}

impl FromNapiValue for Wrap<RoundMode> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let method = String::from_napi_value(env, napi_val)?;
        let method = match method.as_ref() {
            "halftoeven" => RoundMode::HalfToEven,
            "halfawayfromzero" => RoundMode::HalfAwayFromZero,
            _ => {
                return Err(napi::Error::from_reason(
                    "use one of {'halftoeven', 'halfawayfromzero'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(method))
    }
}

#[napi(object)]
pub struct JsRollingOptions {
    pub window_size: i16,
    pub weights: Option<Vec<f64>>,
    pub min_periods: i64,
    pub center: bool,
    pub ddof: Option<u8>,
}

impl From<JsRollingOptions> for RollingOptionsFixedWindow {
    fn from(o: JsRollingOptions) -> Self {
        RollingOptionsFixedWindow {
            window_size: o.window_size as usize,
            weights: o.weights,
            min_periods: o.min_periods as usize,
            center: o.center,
            fn_params: Some(RollingFnParams::Var(RollingVarParams {
                ddof: o.ddof.unwrap_or(1),
            })),
            ..Default::default()
        }
    }
}

impl FromNapiValue for Wrap<SyncOnCloseType> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let soct = String::from_napi_value(env, napi_val)?;
        let soct = match soct.as_ref() {
            "none" => SyncOnCloseType::None,
            "data" => SyncOnCloseType::Data,
            "all" => SyncOnCloseType::All,
            _ => return Err(napi::Error::from_reason("not supported".to_owned())),
        };
        Ok(Wrap(soct))
    }
}
impl ToNapiValue for Wrap<SyncOnCloseType> {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        let s = match val.0 {
            SyncOnCloseType::None => "none",
            SyncOnCloseType::Data => "data",
            SyncOnCloseType::All => "all",
        };
        String::to_napi_value(env, s.to_owned())
    }
}

#[napi(object)]
pub struct JsRowCount {
    pub name: String,
    pub offset: u32,
}

impl From<JsRowCount> for RowIndex {
    fn from(o: JsRowCount) -> Self {
        RowIndex {
            name: o.name.into(),
            offset: o.offset,
        }
    }
}

#[napi(object)]
pub struct JsSinkOptions {
    /// Call sync when closing the file.
    pub sync_on_close: Wrap<SyncOnCloseType>,

    /// The output file needs to maintain order of the data that comes in.
    pub maintain_order: bool,

    /// Recursively create all the directories in the path.
    pub mkdir: bool,
}
impl From<JsSinkOptions> for SinkOptions {
    fn from(o: JsSinkOptions) -> Self {
        SinkOptions {
            sync_on_close: o.sync_on_close.0,
            maintain_order: o.maintain_order,
            mkdir: o.mkdir,
        }
    }
}
impl From<SinkOptions> for JsSinkOptions {
    fn from(o: SinkOptions) -> Self {
        JsSinkOptions {
            sync_on_close: Wrap(o.sync_on_close),
            maintain_order: o.maintain_order,
            mkdir: o.mkdir,
        }
    }
}

#[napi(object)]
pub struct SinkParquetOptions {
    pub compression: Option<String>,
    pub compression_level: Option<i32>,
    pub statistics: Option<bool>,
    pub row_group_size: Option<i16>,
    pub data_pagesize_limit: Option<i64>,
    pub maintain_order: Option<bool>,
    pub type_coercion: Option<bool>,
    pub predicate_pushdown: Option<bool>,
    pub projection_pushdown: Option<bool>,
    pub simplify_expression: Option<bool>,
    pub slice_pushdown: Option<bool>,
    pub no_optimization: Option<bool>,
    pub cloud_options: Option<HashMap<String, String>>,
    pub retries: Option<u32>,
    pub sink_options: JsSinkOptions,
}

#[napi(object)]
pub struct SinkJsonOptions {
    pub maintain_order: Option<bool>,
    pub cloud_options: Option<HashMap<String, String>>,
    pub retries: Option<u32>,
    pub sync_on_close: Wrap<SyncOnCloseType>,
    pub mkdir: Option<bool>,
}

#[napi(object)]
pub struct SinkIpcOptions {
    pub compat_level: Option<String>,
    pub compression: Wrap<Option<IpcCompression>>,
    pub maintain_order: Option<bool>,
    pub cloud_options: Option<HashMap<String, String>>,
    pub retries: Option<u32>,
    pub sync_on_close: Wrap<SyncOnCloseType>,
    pub mkdir: Option<bool>,
}

#[napi(object)]
pub struct ScanParquetOptions {
    pub n_rows: Option<i64>,
    pub row_index_name: Option<String>,
    pub row_index_offset: Option<u32>,
    pub cache: Option<bool>,
    pub parallel: Wrap<ParallelStrategy>,
    pub glob: Option<bool>,
    pub hive_partitioning: Option<bool>,
    pub hive_schema: Option<Wrap<Schema>>,
    pub try_parse_hive_dates: Option<bool>,
    pub rechunk: Option<bool>,
    pub schema: Option<Wrap<Schema>>,
    pub low_memory: Option<bool>,
    pub use_statistics: Option<bool>,
    pub cloud_options: Option<HashMap<String, String>>,
    pub retries: Option<u32>,
    pub include_file_paths: Option<String>,
    pub allow_missing_columns: Option<bool>,
}

#[napi(object)]
pub struct Shape {
    pub height: i64,
    pub width: i64,
}

impl From<(usize, usize)> for Shape {
    fn from(s: (usize, usize)) -> Self {
        let (height, width) = s;
        Shape {
            height: height as i64,
            width: width as i64,
        }
    }
}

impl FromNapiValue for Wrap<TimeUnit> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let tu = String::from_napi_value(env, napi_val)?;
        let tu = match tu.as_ref() {
            "ns" => TimeUnit::Nanoseconds,
            "us" => TimeUnit::Microseconds,
            "ms" => TimeUnit::Milliseconds,
            _ => panic!("not a valid timeunit"),
        };

        Ok(Wrap(tu))
    }
}
impl FromNapiValue for Wrap<DataType> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let ty = type_of!(env, napi_val)?;
        match ty {
            ValueType::Object => {
                let obj = Object::from_napi_value(env, napi_val)?;
                let variant = obj.get::<String>("variant")?.map_or("".into(), |v| v);

                let dtype = match variant.as_ref() {
                    "Int8" => DataType::Int8,
                    "Int16" => DataType::Int16,
                    "Int32" => DataType::Int32,
                    "Int64" => DataType::Int64,
                    "UInt8" => DataType::UInt8,
                    "UInt16" => DataType::UInt16,
                    "UInt32" => DataType::UInt32,
                    "UInt64" => DataType::UInt64,
                    "Float32" => DataType::Float32,
                    "Float64" => DataType::Float64,
                    "Bool" => DataType::Boolean,
                    "Utf8" => DataType::String,
                    "String" => DataType::String,
                    "List" => {
                        let inner = obj.get::<Array>("inner")?.unwrap();
                        let inner_dtype: Object = inner.get::<Object>(0)?.unwrap();
                        let napi_dt = Object::to_napi_value(env, inner_dtype).unwrap();

                        let dt = Wrap::<DataType>::from_napi_value(env, napi_dt)?;
                        DataType::List(Box::new(dt.0))
                    }
                    "FixedSizeList" => {
                        let inner = obj.get::<Array>("inner")?.unwrap();
                        let inner_dtype: Object = inner.get::<Object>(0)?.unwrap();
                        let napi_dt = Object::to_napi_value(env, inner_dtype).unwrap();

                        let dt = Wrap::<DataType>::from_napi_value(env, napi_dt)?;

                        let size = inner.get::<i32>(1)?.unwrap();

                        DataType::Array(Box::new(dt.0), size as usize)
                    }

                    "Date" => DataType::Date,
                    "Datetime" => {
                        let tu = obj.get::<Wrap<TimeUnit>>("timeUnit")?.unwrap();
                        DataType::Datetime(tu.0, None)
                    }
                    "Time" => DataType::Time,
                    "Object" => DataType::Object("object"),
                    "Categorical" => {
                        let categories = Categories::new(
                            PlSmallStr::EMPTY,
                            PlSmallStr::EMPTY,
                            CategoricalPhysical::U32,
                        );
                        DataType::Categorical(categories.clone(), categories.clone().mapping())
                    }
                    "Struct" => {
                        let inner = obj.get::<Array>("fields")?.unwrap();
                        let mut fldvec: Vec<Field> = Vec::with_capacity(inner.len() as usize);
                        for i in 0..inner.len() {
                            let inner_dtype: Object = inner.get::<Object>(i)?.unwrap();
                            let napi_dt = Object::to_napi_value(env, inner_dtype).unwrap();
                            let obj = Object::from_napi_value(env, napi_dt)?;
                            let name = obj.get::<String>("name")?.unwrap();
                            let dt = obj.get::<Wrap<DataType>>("dtype")?.unwrap();
                            let fld = Field::new(name.into(), dt.0);
                            fldvec.push(fld);
                        }
                        DataType::Struct(fldvec)
                    }
                    "Decimal" => {
                        let inner = obj.get::<Array>("inner")?.unwrap(); // [precision, scale]
                        let precision = inner.get::<Option<i32>>(0)?.unwrap().map(|x| x as usize);
                        let scale = inner.get::<Option<i32>>(1)?.unwrap().map(|x| x as usize);
                        DataType::Decimal(precision, scale)
                    }
                    tp => panic!("Type {} not implemented in str_to_polarstype", tp),
                };
                Ok(Wrap(dtype))
            }
            _ => Err(Error::new(
                Status::InvalidArg,
                "not a valid conversion to 'DataType'".to_owned(),
            )),
        }
    }
}

impl FromNapiValue for Wrap<Schema> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let ty = type_of!(env, napi_val)?;
        match ty {
            ValueType::Object => {
                let obj = Object::from_napi_value(env, napi_val)?;
                let keys = Object::keys(&obj)?;
                Ok(Wrap(
                    keys.iter()
                        .map(|key| {
                            let value = obj.get::<Object>(&key)?.unwrap();
                            let napi_val = Object::to_napi_value(env, value)?;
                            let dtype = Wrap::<DataType>::from_napi_value(env, napi_val)?;

                            Ok(Field::new(key.into(), dtype.0))
                        })
                        .collect::<Result<Schema>>()?,
                ))
            }
            _ => Err(Error::new(
                Status::InvalidArg,
                "not a valid conversion to 'Schema'".to_owned(),
            )),
        }
    }
}
impl ToNapiValue for Wrap<Schema> {
    unsafe fn to_napi_value(napi_env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        let env = Env::from_raw(napi_env);
        let mut schema = Object::new(&env)?;

        for (name, dtype) in val.0.iter() {
            schema.set(name, Wrap(dtype.clone()))?;
        }
        Object::to_napi_value(napi_env, schema)
    }
}

impl ToNapiValue for Wrap<ParallelStrategy> {
    unsafe fn to_napi_value(napi_env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        let env = Env::from_raw(napi_env);
        let s = val.0;
        let mut strategy = Object::new(&env)?;

        let unit = match s {
            ParallelStrategy::Auto => "auto",
            ParallelStrategy::Columns => "columns",
            ParallelStrategy::RowGroups => "row_groups",
            ParallelStrategy::None => "none",
            ParallelStrategy::Prefiltered => "prefiltered",
        };
        let _ = strategy.set("strategy", unit);
        Object::to_napi_value(napi_env, strategy)
    }
}
impl FromNapiValue for Wrap<ParallelStrategy> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let s = String::from_napi_value(env, napi_val)?;

        let unit = match s.as_ref() {
            "auto" => ParallelStrategy::Auto,
            "columns" => ParallelStrategy::Columns,
            "row_groups" => ParallelStrategy::RowGroups,
            "none" => ParallelStrategy::None,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "expected one of {'auto', 'columns', 'row_groups', 'none'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(unit))
    }
}
impl FromNapiValue for Wrap<InterpolationMethod> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let s = String::from_napi_value(env, napi_val)?;

        let unit = match s.as_ref() {
            "linear" => InterpolationMethod::Linear,
            "nearest" => InterpolationMethod::Nearest,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "expected one of {'linear', 'nearest'}".to_owned(),
                ))
            }
        };
        Ok(Wrap(unit))
    }
}
impl FromNapiValue for Wrap<SortOptions> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let obj = Object::from_napi_value(env, napi_val)?;
        let descending = obj.get::<bool>("descending")?.unwrap_or(false);
        let nulls_last = obj
            .get::<bool>("nulls_last")?
            .or_else(|| obj.get::<bool>("nullsLast").expect("expect nullsLast"))
            .unwrap_or(false);
        let multithreaded = obj.get::<bool>("multithreaded")?.unwrap_or(false);
        let maintain_order: bool = obj.get::<bool>("maintainOrder")?.unwrap_or(true);
        let limit = obj.get::<_>("limit")?.unwrap();
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded,
            maintain_order,
            limit,
        };
        Ok(Wrap(options))
    }
}
impl FromNapiValue for Wrap<QuoteStyle> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let quote_style_str = String::from_napi_value(env, napi_val)?;

        let parsed = match quote_style_str.as_str() {
            "always" => QuoteStyle::Always,
            "necessary" => QuoteStyle::Necessary,
            "non_numeric" => QuoteStyle::NonNumeric,
            "never" => QuoteStyle::Never,
            _ => return Err(Error::new(Status::InvalidArg,
                format!("`quote_style` must be one of {{'always', 'necessary', 'non_numeric', 'never'}}, got '{}'", quote_style_str),
                )),
        };
        Ok(Wrap(parsed))
    }
}
impl FromNapiValue for Wrap<CsvWriterOptions> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let obj = Object::from_napi_value(env, napi_val)?;
        let include_bom = obj.get::<bool>("includeBom")?.unwrap_or(false);
        let include_header = obj.get::<bool>("includeHeader")?.unwrap_or(true);
        let batch_size = NonZero::new(obj.get::<i64>("batchSize")?.unwrap_or(1024) as usize)
            .ok_or_else(|| napi::Error::from_reason("Invalid batch size"))?;
        let date_format = obj.get::<String>("dateFormat")?;
        let time_format = obj.get::<String>("timeFormat")?;
        let datetime_format = obj.get::<String>("datetimeFormat")?;
        let float_scientific = obj.get::<bool>("floatScientific")?;
        let float_precision = obj.get::<i32>("floatPrecision")?.map(|x| x as usize);
        let separator = obj
            .get::<String>("separator")?
            .unwrap_or(",".to_owned())
            .as_bytes()[0];
        let quote_char = obj
            .get::<String>("quoteChar")?
            .unwrap_or("\"".to_owned())
            .as_bytes()[0];
        let null_value = obj
            .get::<String>("nullValue")?
            .unwrap_or(SerializeOptions::default().null);
        let line_terminator = obj
            .get::<String>("lineTerminator")?
            .unwrap_or("\n".to_owned());
        let quote_style = obj
            .get::<Wrap<QuoteStyle>>("quoteStyle")?
            .map_or(QuoteStyle::default(), |wrap| wrap.0);

        let serialize_options = SerializeOptions {
            date_format,
            time_format,
            datetime_format,
            float_scientific,
            float_precision,
            separator,
            quote_char,
            null: null_value,
            line_terminator,
            quote_style,
            decimal_comma: false,
        };

        let options = CsvWriterOptions {
            include_bom,
            include_header,
            batch_size,
            serialize_options,
        };
        Ok(Wrap(options))
    }
}

impl FromNapiValue for Wrap<(i64, usize)> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let big = BigInt::from_napi_value(env, napi_val)?;
        let (value, _) = big.get_i64();
        Ok(Wrap((value, value as usize)))
    }
}

impl FromNapiValue for Wrap<usize> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let i = i64::from_napi_value(env, napi_val)?;
        Ok(Wrap(i as usize))
    }
}

impl FromNapiValue for Wrap<JoinType> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> napi::Result<Self> {
        let s = String::from_napi_value(env, napi_val)?;
        let parsed = match s.as_ref() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "full" => JoinType::Full,
            "semi" => JoinType::Semi,
            "anti" => JoinType::Anti,
            "cross" => JoinType::Cross,
            v =>
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("how must be one of {{'inner', 'left', 'full', 'semi', 'anti', 'cross'}}, got {v}")
                ))
        };
        Ok(Wrap(parsed))
    }
}

pub enum TypedArrayBuffer {
    Int8(Int8Array),
    Int16(Int16Array),
    Int32(Int32Array),
    Int64(BigInt64Array),
    UInt8(Uint8Array),
    UInt16(Uint16Array),
    UInt32(Uint32Array),
    UInt64(BigUint64Array),
    Float32(Float32Array),
    Float64(Float64Array),
}

impl From<&Series> for TypedArrayBuffer {
    fn from(series: &Series) -> Self {
        let dt = series.dtype();
        match dt {
            DataType::Int8 => TypedArrayBuffer::Int8(Int8Array::with_data_copied(
                series.i8().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::Int16 => TypedArrayBuffer::Int16(Int16Array::with_data_copied(
                series.i16().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::Int32 => TypedArrayBuffer::Int32(Int32Array::with_data_copied(
                series.i32().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::Int64 => TypedArrayBuffer::Int64(BigInt64Array::with_data_copied(
                series.i64().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::UInt8 => TypedArrayBuffer::UInt8(Uint8Array::with_data_copied(
                series.u8().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::UInt16 => TypedArrayBuffer::UInt16(Uint16Array::with_data_copied(
                series.u16().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::UInt32 => TypedArrayBuffer::UInt32(Uint32Array::with_data_copied(
                series.u32().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::UInt64 => TypedArrayBuffer::UInt64(BigUint64Array::with_data_copied(
                series.u64().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::Float32 => TypedArrayBuffer::Float32(Float32Array::with_data_copied(
                series.f32().unwrap().rechunk().cont_slice().unwrap(),
            )),
            DataType::Float64 => TypedArrayBuffer::Float64(Float64Array::with_data_copied(
                series.f64().unwrap().rechunk().cont_slice().unwrap(),
            )),
            dt => panic!("to_list() not implemented for {:?}", dt),
        }
    }
}

impl ToNapiValue for TypedArrayBuffer {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> napi::Result<sys::napi_value> {
        match val {
            TypedArrayBuffer::Int8(v) => Int8Array::to_napi_value(env, v),
            TypedArrayBuffer::Int16(v) => Int16Array::to_napi_value(env, v),
            TypedArrayBuffer::Int32(v) => Int32Array::to_napi_value(env, v),
            TypedArrayBuffer::Int64(v) => BigInt64Array::to_napi_value(env, v),
            TypedArrayBuffer::UInt8(v) => Uint8Array::to_napi_value(env, v),
            TypedArrayBuffer::UInt16(v) => Uint16Array::to_napi_value(env, v),
            TypedArrayBuffer::UInt32(v) => Uint32Array::to_napi_value(env, v),
            TypedArrayBuffer::UInt64(v) => BigUint64Array::to_napi_value(env, v),
            TypedArrayBuffer::Float32(v) => Float32Array::to_napi_value(env, v),
            TypedArrayBuffer::Float64(v) => Float64Array::to_napi_value(env, v),
        }
    }
}
impl ToNapiValue for Wrap<DataType> {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> napi::Result<sys::napi_value> {
        match val.0 {
            DataType::Int8 => String::to_napi_value(env, "Int8".to_owned()),
            DataType::Int16 => String::to_napi_value(env, "Int16".to_owned()),
            DataType::Int32 => String::to_napi_value(env, "Int32".to_owned()),
            DataType::Int64 => String::to_napi_value(env, "Int64".to_owned()),
            DataType::UInt8 => String::to_napi_value(env, "UInt8".to_owned()),
            DataType::UInt16 => String::to_napi_value(env, "UInt16".to_owned()),
            DataType::UInt32 => String::to_napi_value(env, "UInt32".to_owned()),
            DataType::UInt64 => String::to_napi_value(env, "UInt64".to_owned()),
            DataType::Float32 => String::to_napi_value(env, "Float32".to_owned()),
            DataType::Float64 => String::to_napi_value(env, "Float64".to_owned()),
            DataType::Boolean => String::to_napi_value(env, "Bool".to_owned()),
            DataType::String => String::to_napi_value(env, "String".to_owned()),
            DataType::List(inner) => {
                let env_ctx = Env::from_raw(env);
                let mut obj = Object::new(&env_ctx)?;
                let wrapped = Wrap(*inner);

                obj.set("variant", "List")?;
                obj.set("inner", vec![wrapped])?;
                Object::to_napi_value(env, obj)
            }
            DataType::Date => String::to_napi_value(env, "Date".to_owned()),
            DataType::Datetime(tu, tz) => {
                let env_ctx = Env::from_raw(env);
                let mut obj = Object::new(&env_ctx)?;
                let mut inner_arr = env_ctx.create_array(2)?;

                inner_arr.set(0, tu.to_ascii())?;
                inner_arr.set(1, tz.as_ref().map_or("", |s| s.as_str()))?;
                obj.set("variant", "Datetime")?;
                obj.set("inner", inner_arr)?;
                Object::to_napi_value(env, obj)
            }
            DataType::Null => String::to_napi_value(env, "Null".to_owned()),
            DataType::Time => String::to_napi_value(env, "Time".to_owned()),
            DataType::Object(..) => String::to_napi_value(env, "Object".to_owned()),
            DataType::Categorical(..) => String::to_napi_value(env, "Categorical".to_owned()),
            DataType::Struct(flds) => {
                let env_ctx = Env::from_raw(env);

                let mut obj = Object::new(&env_ctx)?;
                let mut js_flds = env_ctx.create_array(flds.len() as u32)?;
                for (idx, fld) in flds.iter().enumerate() {
                    let name = fld.name().clone();
                    let dtype = Wrap(fld.dtype().clone());
                    let mut fld_obj = Object::new(&env_ctx)?;
                    fld_obj.set("name", name.to_string())?;
                    fld_obj.set("dtype", dtype)?;
                    js_flds.set(idx as u32, fld_obj)?;
                }
                obj.set("variant", "Struct")?;
                obj.set("inner", vec![js_flds])?;

                Object::to_napi_value(env, obj)
            }
            DataType::Array(dtype, size) => {
                let env_ctx = Env::from_raw(env);
                let mut obj = Object::new(&env_ctx)?;
                let wrapped = Wrap(*dtype);
                let mut inner_arr = env_ctx.create_array(2)?;
                inner_arr.set(0, wrapped)?;
                inner_arr.set(1, size as u32)?;
                obj.set("variant", "FixedSizeList")?;
                obj.set("inner", inner_arr)?;
                Object::to_napi_value(env, obj)
            }
            DataType::Decimal(precision, scale) => {
                let env_ctx = Env::from_raw(env);
                let mut obj = Object::new(&env_ctx)?;
                let mut inner_arr = env_ctx.create_array(2)?;
                inner_arr.set(0, precision.map(|p| p as u32))?;
                inner_arr.set(1, scale.map(|s| s as u32))?;
                obj.set("variant", "Decimal")?;
                obj.set("inner", inner_arr)?;
                Object::to_napi_value(env, obj)
            }
            _ => {
                todo!()
            }
        }
    }
}

impl FromNapiValue for Wrap<NullValues> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        if let Ok(s) = String::from_napi_value(env, napi_val) {
            Ok(Wrap(NullValues::AllColumnsSingle(s.into())))
        } else if let Ok(s) = Vec::<String>::from_napi_value(env, napi_val) {
            Ok(Wrap(NullValues::AllColumns(
                s.into_iter().map(PlSmallStr::from_string).collect(),
            )))
        } else if let Ok(s) = HashMap::<String, String>::from_napi_value(env, napi_val) {
            let null_values = s
                .into_iter()
                .map(|a| (PlSmallStr::from_string(a.0), PlSmallStr::from_string(a.1)))
                .collect::<Vec<(PlSmallStr, PlSmallStr)>>();
            Ok(Wrap(NullValues::Named(null_values)))
        } else {
            Err(
                JsPolarsErr::Other("could not extract value from null_values argument".into())
                    .into(),
            )
        }
    }
}

impl ToNapiValue for Wrap<NullValues> {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> napi::Result<sys::napi_value> {
        match val.0 {
            NullValues::AllColumnsSingle(s) => String::to_napi_value(env, s.to_string()),
            NullValues::AllColumns(arr) => {
                Vec::<String>::to_napi_value(env, arr.iter().map(|x| x.to_string()).collect())
            }
            NullValues::Named(obj) => {
                let o: HashMap<String, String> = obj
                    .into_iter()
                    .map(|s| (s.0.to_string(), s.1.to_string()))
                    .collect::<HashMap<String, String>>();
                HashMap::<String, String>::to_napi_value(env, o)
            }
        }
    }
}

pub trait FromJsUnknown: Sized + Send {
    fn from_js(obj: Unknown) -> Result<Self>;
}

impl FromJsUnknown for String {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: String = unsafe { val.cast()? };
        Ok(s)
    }
}

impl FromJsUnknown for AnyValue<'_> {
    fn from_js(val: Unknown) -> Result<Self> {
        match val.get_type()? {
            ValueType::Undefined | ValueType::Null => Ok(AnyValue::Null),
            ValueType::Boolean => bool::from_js(val).map(AnyValue::Boolean),
            ValueType::Number => f64::from_js(val).map(AnyValue::Float64),
            ValueType::String => String::from_js(val).map(|s| AnyValue::StringOwned(s.into())),
            ValueType::BigInt => u64::from_js(val).map(AnyValue::UInt64),
            ValueType::Object => {
                if val.is_date()? {
                    let d: Date = unsafe { val.cast()? };
                    let d = d.value_of()?;
                    let d = d as i64;
                    Ok(AnyValue::Datetime(d, TimeUnit::Milliseconds, None))
                } else {
                    Err(JsPolarsErr::Other("Unsupported Data type".to_owned()).into())
                }
            }
            _ => panic!("not supported"),
        }
    }
}

impl FromJsUnknown for DataType {
    fn from_js(val: Unknown) -> Result<Self> {
        match val.get_type()? {
            ValueType::Undefined | ValueType::Null => Ok(DataType::Null),
            ValueType::Boolean => Ok(DataType::Boolean),
            ValueType::Number => Ok(DataType::Float64),
            ValueType::String => Ok(DataType::String),
            ValueType::BigInt => Ok(DataType::UInt64),
            ValueType::Object => {
                if val.is_date()? {
                    Ok(DataType::Datetime(TimeUnit::Milliseconds, None))
                } else {
                    Ok(DataType::String)
                }
            }
            _ => panic!("not supported"),
        }
    }
}

impl FromJsUnknown for bool {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: bool = unsafe { val.cast()? };
        Ok(s)
    }
}

impl FromJsUnknown for f64 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: f64 = unsafe { val.cast()? };
        Ok(s)
    }
}

impl FromJsUnknown for i64 {
    fn from_js(val: Unknown) -> Result<Self> {
        match val.get_type()? {
            ValueType::BigInt => {
                let big: BigInt = unsafe { val.cast()? };
                Ok(big.get_i64().0)
            }
            ValueType::Number => {
                let s: BigInt = unsafe { val.cast()? };
                Ok(s.get_i64().0)
            }
            dt => Err(JsPolarsErr::Other(format!("cannot cast {} to i64", dt)).into()),
        }
    }
}

impl FromJsUnknown for u64 {
    fn from_js(val: Unknown) -> Result<Self> {
        match val.get_type()? {
            ValueType::BigInt => {
                let big: BigInt = unsafe { val.cast()? };
                Ok(big.get_u64().1)
            }
            ValueType::Number => {
                let s: BigInt = unsafe { val.cast()? };
                Ok(s.get_u64().1)
            }
            dt => Err(JsPolarsErr::Other(format!("cannot cast {} to u64", dt)).into()),
        }
    }
}
impl FromJsUnknown for u32 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: u32 = unsafe { val.cast() }?;
        Ok(s)
    }
}
impl FromJsUnknown for f32 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: f64 = unsafe { val.cast() }?;
        Ok(s as f32)
    }
}

impl FromJsUnknown for usize {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: u32 = unsafe { val.cast() }?;
        Ok(s as usize)
    }
}
impl FromJsUnknown for u8 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: u8 = unsafe { val.cast() }?;
        Ok(s)
    }
}
impl FromJsUnknown for u16 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: u16 = unsafe { val.cast() }?;
        Ok(s)
    }
}
impl FromJsUnknown for i8 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: i8 = unsafe { val.cast() }?;
        Ok(s)
    }
}
impl FromJsUnknown for i16 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: i16 = unsafe { val.cast() }?;
        Ok(s)
    }
}

impl FromJsUnknown for i32 {
    fn from_js(val: Unknown) -> Result<Self> {
        let s: i32 = unsafe { val.cast() }?;
        Ok(s)
    }
}

impl<V> FromJsUnknown for Option<V>
where
    V: FromJsUnknown,
{
    fn from_js(val: Unknown) -> Result<Self> {
        let v = V::from_js(val);
        match v {
            Ok(v) => Ok(Some(v)),
            Err(_) => Ok(None),
        }
    }
}

unsafe fn struct_dict<'a>(
    env_raw: sys::napi_env,
    vals: impl Iterator<Item = AnyValue<'a>>,
    flds: &[Field],
) -> Result<sys::napi_value> {
    let env = Env::from_raw(env_raw);
    let mut obj = Object::new(&env)?;
    for (val, fld) in vals.zip(flds) {
        let key = fld.name();
        let val = Wrap(val);
        obj.set(key, val)?;
    }
    Object::to_napi_value(env_raw, obj)
}
pub(crate) fn parse_fill_null_strategy(
    strategy: &str,
    limit: FillNullLimit,
) -> JsResult<FillNullStrategy> {
    let parsed = match strategy {
        "forward" => FillNullStrategy::Forward(limit),
        "backward" => FillNullStrategy::Backward(limit),
        "min" => FillNullStrategy::Min,
        "max" => FillNullStrategy::Max,
        "mean" => FillNullStrategy::Mean,
        "zero" => FillNullStrategy::Zero,
        "one" => FillNullStrategy::One,
        e => {
            return Err(napi::Error::from_reason(
                format!("Strategy {e} not supported").to_owned(),
            ))
        }
    };
    Ok(parsed)
}

pub(crate) fn strings_to_pl_smallstr<I, S>(container: I) -> Vec<PlSmallStr>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    container
        .into_iter()
        .map(|s| PlSmallStr::from_str(s.as_ref()))
        .collect()
}

pub(crate) fn strings_to_selector<I, S>(container: I) -> Selector
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    Selector::ByName {
        names: container
            .into_iter()
            .map(|s| PlSmallStr::from_str(s.as_ref()))
            .collect(),
        strict: true,
    }
}

pub(crate) fn parse_parquet_compression(
    compression: String,
    compression_level: Option<i32>,
) -> JsResult<ParquetCompression> {
    let parsed = match compression.as_ref() {
        "uncompressed" => ParquetCompression::Uncompressed,
        "snappy" => ParquetCompression::Snappy,
        "gzip" => ParquetCompression::Gzip(
            compression_level
                .map(|lvl| {
                    GzipLevel::try_new(lvl as u8)
                        .map_err(|e| napi::Error::from_reason(format!("{:?}", e)))
                })
                .transpose()?,
        ),
        "lzo" => ParquetCompression::Lzo,
        "brotli" => ParquetCompression::Brotli(
            compression_level
                .map(|lvl| {
                    BrotliLevel::try_new(lvl as u32)
                        .map_err(|e| napi::Error::from_reason(format!("{e:?}")))
                })
                .transpose()?,
        ),
        "lz4" => ParquetCompression::Lz4Raw,
        "zstd" => ParquetCompression::Zstd(
            compression_level
                .map(|lvl| {
                    ZstdLevel::try_new(lvl)
                        .map_err(|e| napi::Error::from_reason(format!("{e:?}")))
                })
                .transpose()?,
        ),
        e => {
            return Err(napi::Error::from_reason(format!(
                "parquet `compression` must be one of {{'uncompressed', 'snappy', 'gzip', 'lzo', 'brotli', 'lz4', 'zstd'}}, got {e}",
            )))
        }
    };
    Ok(parsed)
}

pub(crate) fn parse_cloud_options(
    uri: &str,
    kv: Option<HashMap<String, String>>,
    max_retries: Option<u32>,
) -> Option<CloudOptions> {
    let mut cloud_options: Option<CloudOptions> = if let Some(o) = kv {
        let co: Vec<(String, String)> = o.into_iter().map(|kv: (String, String)| kv).collect();
        Some(
            CloudOptions::from_untyped_config(&uri, co)
                .map_err(JsPolarsErr::from)
                .unwrap(),
        )
    } else {
        None
    };

    let max_retries = max_retries.unwrap_or_else(|| 2) as usize;
    if max_retries > 0 {
        cloud_options =
            cloud_options
                .or_else(|| Some(CloudOptions::default()))
                .map(|mut options| {
                    options.max_retries = max_retries;
                    options
                });
    }
    cloud_options
}
