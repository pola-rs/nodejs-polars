use crate::prelude::*;
use napi::bindgen_prelude::*;

macro_rules! typed_to_chunked {
    ($arr:expr, $type:ty, $pl_type:ty) => {{
        let v: &[$type] = $arr.as_ref();
        let mut buffer = Vec::<$type>::new();
        buffer.extend_from_slice(v);
        ChunkedArray::<$pl_type>::from_vec(PlSmallStr::EMPTY, buffer)
    }};
}

macro_rules! typed_option_or_null {
    ($name:expr, $arr:expr, $type:ty, $dtype:expr, $pl_type:ty, $arry_type:ty) => {{
        let len = $arr.len();
        let mut builder = ListPrimitiveChunkedBuilder::<$pl_type>::new(
            $name,
            len as usize,
            (len as usize) * 5,
            $dtype,
        );
        for idx in 0..len {
            let obj: napi::Unknown = $arr.get(idx)?.unwrap();
            if obj.is_typedarray()? {
                let buff: $arry_type = unsafe { obj.cast()? };
                let ca = typed_to_chunked!(buff, $type, $pl_type);
                builder.append_iter(ca.into_iter())
            } else {
                let values: Either<Array, Null> = $arr.get(idx)?.unwrap();
                match values {
                    Either::A(inner_arr) => {
                        let inner_len = inner_arr.len();
                        let mut inner_builder =
                            PrimitiveChunkedBuilder::<$pl_type>::new($name, inner_len as usize);
                        for inner_idx in 0..inner_len {
                            let item: Option<$type> = inner_arr.get(inner_idx)?.unwrap();
                            match item {
                                Some(v) => inner_builder.append_value(v),
                                None => inner_builder.append_null(),
                            }
                        }
                        let ser = inner_builder.finish().into_series();
                        let _ = builder.append_series(&ser);
                    }
                    Either::B(_) => builder.append_null(),
                }
            }
        }
        builder.finish().into_series()
    }};
}
macro_rules! build_list_with_downcast {
    ($name:expr, $arr:expr, $type:ty, $dtype:expr, $pl_type:ty, $arry_type:ty) => {{
        let len = $arr.len();
        let mut builder = ListPrimitiveChunkedBuilder::<$pl_type>::new(
            $name,
            len as usize,
            (len as usize) * 5,
            $dtype,
        );
        for idx in 0..len {
            let obj: napi::Unknown = $arr.get(idx)?.unwrap();
            if obj.is_typedarray()? {
                let buff: $arry_type = unsafe { obj.cast()? };
                let ca = typed_to_chunked!(buff, $type, $pl_type);
                builder.append_iter(ca.into_iter())
            } else {
                let values: Either<Array, Null> = $arr.get(idx)?.unwrap();
                match values {
                    Either::A(inner_arr) => {
                        let inner_len = inner_arr.len();
                        let mut inner_builder =
                            PrimitiveChunkedBuilder::<$pl_type>::new($name, inner_len as usize);
                        for inner_idx in 0..inner_len {
                            let item: Option<Wrap<$type>> = inner_arr.get(inner_idx)?.unwrap();
                            match item {
                                Some(v) => inner_builder.append_value(v.0),
                                None => inner_builder.append_null(),
                            }
                        }
                        let ser = inner_builder.finish().into_series();
                        let _ = builder.append_series(&ser);
                    }
                    Either::B(_) => builder.append_null(),
                }
            }
        }
        builder.finish().into_series()
    }};
}

pub fn js_arr_to_list(name: &str, arr: &Array, dtype: &DataType) -> napi::Result<Series> {
    let len = arr.len();

    let s = match dtype {
        DataType::Int8 => build_list_with_downcast!(
            PlSmallStr::from_str(name),
            arr,
            i8,
            DataType::Int8,
            Int8Type,
            Int8Array
        ),
        DataType::UInt8 => build_list_with_downcast!(
            PlSmallStr::from_str(name),
            arr,
            u8,
            DataType::UInt8,
            UInt8Type,
            Uint8Array
        ),
        DataType::Int16 => build_list_with_downcast!(
            PlSmallStr::from_str(name),
            arr,
            i16,
            DataType::Int16,
            Int16Type,
            Int16Array
        ),
        DataType::UInt16 => build_list_with_downcast!(
            PlSmallStr::from_str(name),
            arr,
            u16,
            DataType::UInt16,
            UInt16Type,
            Uint16Array
        ),
        DataType::Int32 => typed_option_or_null!(
            PlSmallStr::from_str(name),
            arr,
            i32,
            DataType::Int32,
            Int32Type,
            Int32Array
        ),
        DataType::UInt32 => typed_option_or_null!(
            PlSmallStr::from_str(name),
            arr,
            u32,
            DataType::UInt32,
            UInt32Type,
            Uint32Array
        ),
        DataType::Float32 => {
            build_list_with_downcast!(
                PlSmallStr::from_str(name),
                arr,
                f32,
                DataType::Float32,
                Float32Type,
                Float32Array
            )
        }
        DataType::Int64 => typed_option_or_null!(
            PlSmallStr::from_str(name),
            arr,
            i64,
            DataType::Int64,
            Int64Type,
            BigInt64Array
        ),
        DataType::Float64 => typed_option_or_null!(
            PlSmallStr::from_str(name),
            arr,
            f64,
            DataType::Float64,
            Float64Type,
            Float64Array
        ),
        DataType::UInt64 => build_list_with_downcast!(
            PlSmallStr::from_str(name),
            arr,
            u64,
            DataType::UInt64,
            UInt64Type,
            BigUint64Array
        ),
        DataType::String => arr.to_series(name),
        DataType::Boolean => {
            let mut builder = ListBooleanChunkedBuilder::new(
                PlSmallStr::from_str(name),
                len as usize,
                (len as usize) * 5,
            );
            for idx in 0..len {
                let values: Either<Vec<Option<bool>>, Null> = arr.get(idx)?.unwrap();

                match values {
                    Either::A(inner_arr) => builder.append_iter(inner_arr.into_iter()),
                    Either::B(_) => builder.append_null(),
                }
            }
            builder.finish().into_series()
        }
        DataType::Datetime(_, _) => {
            let mut builder = ListPrimitiveChunkedBuilder::<Int64Type>::new(
                PlSmallStr::from_str(name),
                len as usize,
                (len as usize) * 5,
                DataType::Datetime(TimeUnit::Milliseconds, None),
            );
            for idx in 0..len {
                let values: Either<Array, Null> = arr.get(idx)?.unwrap();
                match values {
                    Either::A(inner_arr) => {
                        let inner_len = inner_arr.len();
                        let mut inner_builder = PrimitiveChunkedBuilder::<Int64Type>::new(
                            PlSmallStr::from_str(name),
                            inner_len as usize,
                        );
                        for inner_idx in 0..inner_len {
                            let item: Either<napi::JsDate, Null> =
                                inner_arr.get(inner_idx)?.unwrap();
                            match item {
                                Either::A(d) => match d.value_of() {
                                    Ok(v) => inner_builder.append_value(v as i64),
                                    Err(_) => inner_builder.append_null(),
                                },
                                Either::B(_) => builder.append_null(),
                            }
                        }
                        let dt_series = inner_builder
                            .finish()
                            .into_series()
                            .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                            .map_err(JsPolarsErr::from)?;
                        let _ = builder.append_series(&dt_series);
                    }
                    Either::B(_) => builder.append_null(),
                }
            }
            builder.finish().into_series()
        }
        dt => {
            panic!("cannot create list array from {:?}", dt);
        }
    };
    Ok(s)
}

/*
pub fn from_typed_array(arr: &napi::JsTypedArrayValue) -> JsResult<Series> {
    let dtype: JsDataType = arr.typedarray_type.into();
    let series = match dtype {
        JsDataType::Int8 => typed_to_chunked!(arr, i8, Int8Type).into(),
        JsDataType::UInt8 => typed_to_chunked!(arr, u8, UInt8Type).into(),
        JsDataType::Int16 => typed_to_chunked!(arr, i16, Int16Type).into(),
        JsDataType::UInt16 => typed_to_chunked!(arr, u16, UInt16Type).into(),
        JsDataType::Int32 => typed_to_chunked!(arr, i32, Int32Type).into(),
        JsDataType::UInt32 => typed_to_chunked!(arr, u32, UInt32Type).into(),
        JsDataType::Float32 => typed_to_chunked!(arr, f32, Float32Type).into(),
        JsDataType::Float64 => typed_to_chunked!(arr, f64, Float64Type).into(),
        JsDataType::Int64 => typed_to_chunked!(arr, i64, Int64Type).into(),
        JsDataType::UInt64 => typed_to_chunked!(arr, u64, UInt64Type).into(),
        _ => panic!("cannot create series from"),
    };

    Ok(series)
} */
