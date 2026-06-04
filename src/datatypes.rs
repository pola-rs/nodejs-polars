use crate::prelude::*;
use crate::series::JsSeries;
use napi::bindgen_prelude::TypedArrayType;

const JS_ANYVALUE_ERROR: &str = "Unknown JS variables cannot be represented as a JsAnyValue";

fn invalid_js_anyvalue_error() -> Error {
    Error::new(Status::InvalidArg, JS_ANYVALUE_ERROR.to_owned())
}

fn unsupported_conversion_error(from: &str, to: &str, value: &str) -> Error {
    Error::new(
        Status::InvalidArg,
        format!("Unsupported conversion from {from} to {to}: {value}"),
    )
}

#[napi(js_name = "DataType")]
pub enum JsDataType {
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Bool,
    Utf8,
    String,
    List,
    Date,
    Datetime,
    Time,
    Duration,
    Object,
    Categorical,
    Struct,
}
impl JsDataType {
    pub fn from_str(s: &str) -> JsResult<Self> {
        s.try_into()
    }
}

impl TryFrom<&str> for JsDataType {
    type Error = napi::Error;

    fn try_from(s: &str) -> napi::Result<Self> {
        match s {
            "Int8" => Ok(JsDataType::Int8),
            "Int16" => Ok(JsDataType::Int16),
            "Int32" => Ok(JsDataType::Int32),
            "Int64" => Ok(JsDataType::Int64),
            "UInt8" => Ok(JsDataType::UInt8),
            "UInt16" => Ok(JsDataType::UInt16),
            "UInt32" => Ok(JsDataType::UInt32),
            "UInt64" => Ok(JsDataType::UInt64),
            "Float32" => Ok(JsDataType::Float32),
            "Float64" => Ok(JsDataType::Float64),
            "Bool" => Ok(JsDataType::Bool),
            "Utf8" => Ok(JsDataType::Utf8),
            "String" => Ok(JsDataType::String),
            "List" => Ok(JsDataType::List),
            "Date" => Ok(JsDataType::Date),
            "Datetime" => Ok(JsDataType::Datetime),
            "Time" => Ok(JsDataType::Time),
            "Duration" => Ok(JsDataType::Duration),
            "Object" => Ok(JsDataType::Object),
            "Categorical" => Ok(JsDataType::Categorical),
            "Struct" => Ok(JsDataType::Struct),
            _ => Err(unsupported_conversion_error("&str", "JsDataType", s)),
        }
    }
}

impl TryFrom<&DataType> for JsDataType {
    type Error = napi::Error;

    fn try_from(dt: &DataType) -> napi::Result<Self> {
        use JsDataType::*;
        match dt {
            DataType::Int8 => Ok(Int8),
            DataType::Int16 => Ok(Int16),
            DataType::Int32 => Ok(Int32),
            DataType::Int64 => Ok(Int64),
            DataType::UInt8 => Ok(UInt8),
            DataType::UInt16 => Ok(UInt16),
            DataType::UInt32 => Ok(UInt32),
            DataType::UInt64 => Ok(UInt64),
            DataType::Float32 => Ok(Float32),
            DataType::Float64 => Ok(Float64),
            DataType::Boolean => Ok(Bool),
            DataType::String => Ok(Utf8),
            DataType::List(_) => Ok(List),
            DataType::Date => Ok(Date),
            DataType::Datetime(_, _) => Ok(Datetime),
            DataType::Time => Ok(Time),
            DataType::Duration(_) => Ok(Duration),
            DataType::Object(..) => Ok(Object),
            DataType::Categorical(..) => Ok(Categorical),
            DataType::Struct(_) => Ok(Struct),
            other => Err(unsupported_conversion_error(
                "DataType",
                "JsDataType",
                &format!("{other:?}"),
            )),
        }
    }
}

impl TryFrom<TypedArrayType> for JsDataType {
    type Error = napi::Error;

    fn try_from(dt: TypedArrayType) -> napi::Result<Self> {
        use napi::bindgen_prelude::TypedArrayType::*;
        match dt {
            Int8 => Ok(JsDataType::Int8),
            Uint8 => Ok(JsDataType::UInt8),
            Uint8Clamped => Ok(JsDataType::UInt8),
            Int16 => Ok(JsDataType::Int16),
            Uint16 => Ok(JsDataType::UInt16),
            Int32 => Ok(JsDataType::Int32),
            Uint32 => Ok(JsDataType::UInt32),
            Float32 => Ok(JsDataType::Float32),
            Float64 => Ok(JsDataType::Float64),
            BigInt64 => Ok(JsDataType::Int64),
            BigUint64 => Ok(JsDataType::UInt64),
            _ => Err(unsupported_conversion_error(
                "TypedArrayType",
                "JsDataType",
                &format!("{dt:?}"),
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JsAnyValue {
    Null,
    Boolean(bool),
    Utf8(String),
    String(String),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Date(i32),
    Datetime(i64, TimeUnit, Option<TimeZone>),
    Duration(i64, TimeUnit),
    Time(i64),
    List(Series),
    Struct(Vec<JsAnyValue>),
}

impl<'a> FromNapiValue for JsAnyValue {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let ty = type_of!(env, napi_val)?;
        let val = match ty {
            ValueType::Boolean => JsAnyValue::Boolean(bool::from_napi_value(env, napi_val)?),
            ValueType::Number => JsAnyValue::Float64(f64::from_napi_value(env, napi_val)?),
            ValueType::String => JsAnyValue::Utf8(String::from_napi_value(env, napi_val)?),
            ValueType::BigInt => JsAnyValue::UInt64(Wrap::<u64>::from_napi_value(env, napi_val)?.0),
            ValueType::Object => {
                if let Ok(s) = <&JsSeries>::from_napi_value(env, napi_val) {
                    JsAnyValue::List(s.series.clone())
                } else if let Ok(d) = napi::JsDate::from_napi_value(env, napi_val) {
                    let d = d.value_of()?;
                    let dt = d as i64;
                    JsAnyValue::Datetime(dt, TimeUnit::Milliseconds, None)
                } else {
                    return Err(invalid_js_anyvalue_error());
                }
            }
            ValueType::Null | ValueType::Undefined => JsAnyValue::Null,
            _ => return Err(invalid_js_anyvalue_error()),
        };
        Ok(val)
    }
}

impl<'a> FromNapiValue for Wrap<AnyValue<'a>> {
    unsafe fn from_napi_value(env: sys::napi_env, napi_val: sys::napi_value) -> JsResult<Self> {
        let ty = type_of!(env, napi_val)?;
        let val = match ty {
            ValueType::Boolean => AnyValue::Boolean(bool::from_napi_value(env, napi_val)?),
            ValueType::Number => AnyValue::Float64(f64::from_napi_value(env, napi_val)?),
            ValueType::String => {
                let s = String::from_napi_value(env, napi_val)?;
                AnyValue::StringOwned(s.into())
            }
            ValueType::BigInt => AnyValue::UInt64(Wrap::<u64>::from_napi_value(env, napi_val)?.0),
            ValueType::Object => {
                if let Ok(vals) = Vec::<Wrap<AnyValue>>::from_napi_value(env, napi_val) {
                    let vals = vals.into_iter().map(|wrap| wrap.0).collect::<Vec<AnyValue>>();
                    let s = Series::new(PlSmallStr::EMPTY, vals);
                    AnyValue::List(s)
                } else if let Ok(s) = <&JsSeries>::from_napi_value(env, napi_val) {
                    AnyValue::List(s.series.clone())
                } else if let Ok(d) = napi::JsDate::from_napi_value(env, napi_val) {
                    let d = d.value_of()?;
                    let dt = d as i64;
                    AnyValue::Datetime(dt, TimeUnit::Milliseconds, None)
                } else {
                    return Err(invalid_js_anyvalue_error());
                }
            }
            ValueType::Null | ValueType::Undefined => AnyValue::Null,
            _ => return Err(invalid_js_anyvalue_error()),
        };

        Ok(val.into())
    }
}

impl From<napi::ValueType> for Wrap<DataType> {
    fn from(dt: napi::ValueType) -> Self {
        use napi::ValueType::*;
        match dt {
            Undefined | Null | Unknown => Wrap(DataType::Null),
            Boolean => Wrap(DataType::Boolean),
            Number => Wrap(DataType::Float64),
            BigInt => Wrap(DataType::UInt64),
            _ => Wrap(DataType::String),
        }
    }
}

impl ToNapiValue for JsAnyValue {
    unsafe fn to_napi_value(env: sys::napi_env, val: Self) -> Result<sys::napi_value> {
        match val {
            JsAnyValue::Null => Null::to_napi_value(env, Null),
            JsAnyValue::Boolean(b) => bool::to_napi_value(env, b),
            JsAnyValue::Int8(n) => i32::to_napi_value(env, n as i32),
            JsAnyValue::Int16(n) => i32::to_napi_value(env, n as i32),
            JsAnyValue::Int32(n) => i32::to_napi_value(env, n),
            JsAnyValue::Int64(n) => i64::to_napi_value(env, n),
            JsAnyValue::UInt8(n) => u32::to_napi_value(env, n as u32),
            JsAnyValue::UInt16(n) => u32::to_napi_value(env, n as u32),
            JsAnyValue::UInt32(n) => u32::to_napi_value(env, n),
            JsAnyValue::UInt64(n) => u64::to_napi_value(env, n),
            JsAnyValue::Float32(n) => f64::to_napi_value(env, n as f64),
            JsAnyValue::Float64(n) => f64::to_napi_value(env, n),
            JsAnyValue::Utf8(s) => String::to_napi_value(env, s),
            JsAnyValue::String(s) => String::to_napi_value(env, s),
            JsAnyValue::Date(v) => {
                let mut ptr = std::ptr::null_mut();
                let epoch_time: f64 = (v as f64) * 86400000.0;
                check_status!(
                    napi::sys::napi_create_date(env, epoch_time, &mut ptr),
                    "Failed to convert rust type `AnyValue::Date` into napi value",
                )?;
                Ok(ptr)
            }
            JsAnyValue::Datetime(v, time_unit, _) => {
                let mut ptr = std::ptr::null_mut();
                let epoch_time: f64 = match time_unit {
                    TimeUnit::Milliseconds => v as f64,
                    TimeUnit::Microseconds => (v / 1000) as f64,
                    TimeUnit::Nanoseconds => (v / 1_000_000) as f64,
                };
                check_status!(
                    napi::sys::napi_create_date(env, epoch_time, &mut ptr),
                    "Failed to convert rust type `AnyValue::Date` into napi value",
                )?;
                Ok(ptr)
            }
            JsAnyValue::Duration(v, _) => i64::to_napi_value(env, v),
            JsAnyValue::Time(v) => i64::to_napi_value(env, v),
            JsAnyValue::List(ser) => JsSeries::to_napi_value(env, ser.into()),
            JsAnyValue::Struct(vals) => Vec::<JsAnyValue>::to_napi_value(env, vals),
        }
    }
}

impl TryFrom<JsAnyValue> for AnyValue<'static> {
    type Error = napi::Error;

    fn try_from(av: JsAnyValue) -> napi::Result<Self> {
        match av {
            JsAnyValue::Null => Ok(AnyValue::Null),
            JsAnyValue::Boolean(v) => Ok(AnyValue::Boolean(v)),
            JsAnyValue::Utf8(v) => Ok(AnyValue::StringOwned(v.into())),
            JsAnyValue::String(v) => Ok(AnyValue::StringOwned(v.into())),
            JsAnyValue::UInt8(v) => Ok(AnyValue::UInt8(v)),
            JsAnyValue::UInt16(v) => Ok(AnyValue::UInt16(v)),
            JsAnyValue::UInt32(v) => Ok(AnyValue::UInt32(v)),
            JsAnyValue::UInt64(v) => Ok(AnyValue::UInt64(v)),
            JsAnyValue::Int8(v) => Ok(AnyValue::Int8(v)),
            JsAnyValue::Int16(v) => Ok(AnyValue::Int16(v)),
            JsAnyValue::Int32(v) => Ok(AnyValue::Int32(v)),
            JsAnyValue::Int64(v) => Ok(AnyValue::Int64(v)),
            JsAnyValue::Float32(v) => Ok(AnyValue::Float32(v)),
            JsAnyValue::Float64(v) => Ok(AnyValue::Float64(v)),
            JsAnyValue::Date(v) => Ok(AnyValue::Date(v)),
            JsAnyValue::Datetime(v, w, _) => Ok(AnyValue::Datetime(v, w, None)),
            JsAnyValue::Duration(v, w) => Ok(AnyValue::Duration(v, w)),
            JsAnyValue::Time(v) => Ok(AnyValue::Time(v)),
            JsAnyValue::List(v) => Ok(AnyValue::List(v)),
            other => Err(unsupported_conversion_error(
                "JsAnyValue",
                "AnyValue",
                &format!("{other:?}"),
            )),
        }
    }
}

impl TryFrom<AnyValue<'_>> for JsAnyValue {
    type Error = napi::Error;

    fn try_from(av: AnyValue) -> napi::Result<Self> {
        match av {
            AnyValue::Null => Ok(JsAnyValue::Null),
            AnyValue::Boolean(v) => Ok(JsAnyValue::Boolean(v)),
            AnyValue::String(v) => Ok(JsAnyValue::Utf8(v.to_owned())),
            AnyValue::StringOwned(v) => Ok(JsAnyValue::Utf8(v.to_string())),
            AnyValue::UInt8(v) => Ok(JsAnyValue::UInt8(v)),
            AnyValue::UInt16(v) => Ok(JsAnyValue::UInt16(v)),
            AnyValue::UInt32(v) => Ok(JsAnyValue::UInt32(v)),
            AnyValue::UInt64(v) => Ok(JsAnyValue::UInt64(v)),
            AnyValue::Int8(v) => Ok(JsAnyValue::Int8(v)),
            AnyValue::Int16(v) => Ok(JsAnyValue::Int16(v)),
            AnyValue::Int32(v) => Ok(JsAnyValue::Int32(v)),
            AnyValue::Int64(v) => Ok(JsAnyValue::Int64(v)),
            AnyValue::Float32(v) => Ok(JsAnyValue::Float32(v)),
            AnyValue::Float64(v) => Ok(JsAnyValue::Float64(v)),
            AnyValue::Date(v) => Ok(JsAnyValue::Date(v)),
            AnyValue::Datetime(v, w, _) => Ok(JsAnyValue::Datetime(v, w, None)),
            AnyValue::DatetimeOwned(v, w, _) => Ok(JsAnyValue::Datetime(v, w, None)),
            AnyValue::Duration(v, w) => Ok(JsAnyValue::Duration(v, w)),
            AnyValue::Time(v) => Ok(JsAnyValue::Time(v)),
            AnyValue::List(v) => Ok(JsAnyValue::List(v)),
            other => Err(unsupported_conversion_error(
                "AnyValue",
                "JsAnyValue",
                &format!("{other:?}"),
            )),
        }
    }
}

impl TryFrom<&JsAnyValue> for DataType {
    type Error = napi::Error;

    fn try_from(av: &JsAnyValue) -> napi::Result<Self> {
        match av {
            JsAnyValue::Null => Ok(DataType::Null),
            JsAnyValue::Boolean(_) => Ok(DataType::Boolean),
            JsAnyValue::Utf8(_) => Ok(DataType::String),
            JsAnyValue::String(_) => Ok(DataType::String),
            JsAnyValue::UInt8(_) => Ok(DataType::UInt8),
            JsAnyValue::UInt16(_) => Ok(DataType::UInt16),
            JsAnyValue::UInt32(_) => Ok(DataType::UInt32),
            JsAnyValue::UInt64(_) => Ok(DataType::UInt64),
            JsAnyValue::Int8(_) => Ok(DataType::Int8),
            JsAnyValue::Int16(_) => Ok(DataType::Int16),
            JsAnyValue::Int32(_) => Ok(DataType::Int32),
            JsAnyValue::Int64(_) => Ok(DataType::Int64),
            JsAnyValue::Float32(_) => Ok(DataType::Float32),
            JsAnyValue::Float64(_) => Ok(DataType::Float64),
            JsAnyValue::Date(_) => Ok(DataType::Date),
            JsAnyValue::Datetime(_, _, _) => Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
            JsAnyValue::Time(_) => Ok(DataType::Time),
            JsAnyValue::Duration(_, _) => Ok(DataType::Duration(TimeUnit::Milliseconds)),
            other => Err(unsupported_conversion_error(
                "JsAnyValue",
                "DataType",
                &format!("{other:?}"),
            )),
        }
    }
}

macro_rules! impl_av_into {
    ($type:ty, $pattern:pat => $extracted_value:expr) => {
        impl TryInto<$type> for Wrap<AnyValue<'_>> {
            type Error = napi::Error;
            fn try_into(self) -> napi::Result<$type> {
                match self.0 {
                    $pattern => $extracted_value,
                    _ => Err(napi::Error::from_reason(
                        "invalid primitive cast".to_owned(),
                    )),
                }
            }
        }
    };
}
impl<'a> TryInto<&'a str> for Wrap<AnyValue<'a>> {
    type Error = napi::Error;
    fn try_into(self) -> napi::Result<&'a str> {
        match self.0 {
            AnyValue::String(v) => Ok(v),
            _ => Err(napi::Error::from_reason(
                "invalid primitive cast".to_owned(),
            )),
        }
    }
}

impl_av_into!(String, AnyValue::String(v) => Ok(v.to_string()));
impl_av_into!(bool, AnyValue::Boolean(v) => Ok(v));
impl_av_into!(u8,  AnyValue::UInt8(v) => Ok(v));
impl_av_into!(u16, AnyValue::UInt16(v) => Ok(v));
impl_av_into!(u32, AnyValue::UInt32(v) => Ok(v));
impl_av_into!(u64, AnyValue::UInt64(v) => Ok(v));
impl_av_into!(i8,  AnyValue::Int8(v) => Ok(v));
impl_av_into!(i16, AnyValue::Int16(v) => Ok(v));
impl_av_into!(i32, AnyValue::Int32(v) => Ok(v));
impl_av_into!(i64, AnyValue::Int64(v) => Ok(v));
impl_av_into!(f32, AnyValue::Float32(v) => Ok(v));
impl_av_into!(f64, AnyValue::Float64(v) => Ok(v));

#[allow(clippy::from_over_into)]
impl Into<DataType> for JsDataType {
    fn into(self) -> DataType {
        use DataType::*;
        match self {
            JsDataType::Int8 => Int8,
            JsDataType::Int16 => Int16,
            JsDataType::Int32 => Int32,
            JsDataType::Int64 => Int64,
            JsDataType::UInt8 => UInt8,
            JsDataType::UInt16 => UInt16,
            JsDataType::UInt32 => UInt32,
            JsDataType::UInt64 => UInt64,
            JsDataType::Float32 => Float32,
            JsDataType::Float64 => Float64,
            JsDataType::Bool => Boolean,
            JsDataType::Utf8 => String,
            JsDataType::String => String,
            JsDataType::List => List(DataType::Null.into()),
            JsDataType::Date => Date,
            JsDataType::Datetime => Datetime(TimeUnit::Milliseconds, None),
            JsDataType::Time => Time,
            JsDataType::Duration => DataType::Duration(TimeUnit::Microseconds),
            JsDataType::Object => Object("object"),
            JsDataType::Categorical => {
                let categories = Categories::new(
                    PlSmallStr::EMPTY,
                    PlSmallStr::EMPTY,
                    CategoricalPhysical::U32,
                );
                DataType::Categorical(categories.clone(), categories.clone().mapping())
            }
            JsDataType::Struct => Struct(vec![]),
        }
    }
}
