//! Bridges Polars lazy `map_batches` to a JavaScript callback via N-API threadsafe functions.
//!
//! The input [`Series`] is moved into a `JsSeries` on the JS thread. The callback returns
//! another `JsSeries`, and only the `Arc` around the column buffers is cloned back. Batches
//! are not serialized.

use std::sync::{Arc, OnceLock};

use napi::bindgen_prelude::{validate_type_tag, FromNapiValue, Function, Status, TypeTag};
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use polars::prelude::*;
use polars_plan::dsl::anonymous::{new_column_udf, AnonymousColumnsUdf, ColumnsUdf};
use polars_plan::prelude::{FunctionFlags, FunctionOptions};

use crate::conversion::Wrap;
use crate::series::JsSeries;

type MapBatchesTsfn = ThreadsafeFunction<Series, ReturnedSeries, JsSeries, Status, false, true, 0>;

/// Series returned by the JavaScript callback.
///
/// `FromNapiValue` clones the `Series` (`Arc` clone of the buffers) out of the
/// live `JsSeries` so the JS wrapper can be collected later without freeing data
/// the engine still holds.
pub struct ReturnedSeries(Series);

impl FromNapiValue for ReturnedSeries {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        napi_val: napi::sys::napi_value,
    ) -> napi::Result<Self> {
        let mut wrapped = std::ptr::null_mut();
        napi::check_status!(
            unsafe { napi::sys::napi_unwrap(env, napi_val, &mut wrapped) },
            "mapBatches callback must return a polars Series"
        )?;
        unsafe { validate_type_tag(env, napi_val, &JsSeries::type_tag(), "JsSeries")? };
        if wrapped.is_null() {
            return Err(napi::Error::from_reason(
                "mapBatches callback must return a polars Series".to_owned(),
            ));
        }
        let series = unsafe { (*wrapped.cast::<JsSeries>()).series.clone() };
        Ok(Self(series))
    }
}

fn align_output_dtype(out: Series, field: &Field) -> PolarsResult<Series> {
    let must_cast = out.dtype().matches_schema_type(field.dtype()).map_err(|_| {
        polars_err!(
            SchemaMismatch: "mapBatches expected output type '{:?}', got '{:?}'; set `returnType` to the proper datatype",
            field.dtype(),
            out.dtype(),
        )
    })?;
    if must_cast {
        out.cast(field.dtype())
    } else {
        Ok(out)
    }
}

#[napi(object)]
pub struct JsMapBatchesOptions {
    pub return_type: Option<Wrap<DataType>>,
    pub is_elementwise: Option<bool>,
    pub returns_scalar: Option<bool>,
}

pub struct JsMapBatchesUdf {
    tsfn: Arc<MapBatchesTsfn>,
    return_type: Option<DataType>,
    is_elementwise: bool,
    returns_scalar: bool,
    materialized_field: OnceLock<Field>,
}

impl JsMapBatchesUdf {
    pub fn new(
        callback: Function<'_, (JsSeries,), ReturnedSeries>,
        return_type: Option<DataType>,
        is_elementwise: bool,
        returns_scalar: bool,
    ) -> napi::Result<Self> {
        let tsfn = callback
            .build_threadsafe_function::<Series>()
            // Leave the event loop free to exit once the query promise settles.
            .weak::<true>()
            .build_callback(|ctx: ThreadsafeCallContext<Series>| {
                Ok(JsSeries::reported(ctx.value, &ctx.env))
            })?;
        Ok(Self {
            tsfn: Arc::new(tsfn),
            return_type,
            is_elementwise,
            returns_scalar,
            materialized_field: OnceLock::new(),
        })
    }

    pub fn into_expr(self, input: Expr) -> Expr {
        let mut flags = FunctionFlags::default() | FunctionFlags::OPTIONAL_RE_ENTRANT;
        if self.is_elementwise {
            flags.set_elementwise();
        }
        if self.returns_scalar {
            flags |= FunctionFlags::RETURNS_SCALAR;
        }

        Expr::AnonymousFunction {
            input: vec![input],
            function: new_column_udf(self),
            options: FunctionOptions {
                flags,
                ..Default::default()
            },
            fmt_str: Box::new("map_batches".into()),
        }
    }

    fn materialized_output_field(&self) -> PolarsResult<&Field> {
        self.materialized_field.get().ok_or_else(|| {
            polars_err!(
                ComputeError: "mapBatches output field was not materialized before execution"
            )
        })
    }

    fn invoke_js(&self, input: Series) -> PolarsResult<Series> {
        let (tx, rx) = std::sync::mpsc::sync_channel::<PolarsResult<Series>>(1);

        let status = self.tsfn.call_with_return_value(
            input,
            ThreadsafeFunctionCallMode::Blocking,
            move |ret, _env| {
                let out = ret
                    .map(|returned| returned.0)
                    .map_err(|e| polars_err!(ComputeError: "{}", e));
                let _ = tx.send(out);
                Ok(())
            },
        );

        if status != Status::Ok {
            polars_bail!(
                ComputeError: "mapBatches threadsafe function call failed with status {:?}",
                status
            );
        }

        rx.recv().map_err(
            |_| polars_err!(ComputeError: "mapBatches callback channel closed unexpectedly"),
        )?
    }
}

impl ColumnsUdf for JsMapBatchesUdf {
    fn call_udf(&self, s: &mut [Column]) -> PolarsResult<Column> {
        let field = self.materialized_output_field()?;

        let input_col = s
            .first_mut()
            .ok_or_else(|| polars_err!(ComputeError: "mapBatches expects one input column"))?;
        let input_col = std::mem::take(input_col);
        let out = self.invoke_js(input_col.take_materialized_series())?;
        let out = align_output_dtype(out, field)?;
        Ok(out.into_column())
    }
}

impl AnonymousColumnsUdf for JsMapBatchesUdf {
    fn as_column_udf(self: Arc<Self>) -> Arc<dyn ColumnsUdf> {
        self as _
    }

    fn deep_clone(self: Arc<Self>) -> Arc<dyn AnonymousColumnsUdf> {
        Arc::new(Self {
            tsfn: Arc::clone(&self.tsfn),
            return_type: self.return_type.clone(),
            is_elementwise: self.is_elementwise,
            returns_scalar: self.returns_scalar,
            // Output field is recomputed when the cloned plan is optimized.
            materialized_field: OnceLock::new(),
        }) as _
    }

    fn get_field(&self, _input_schema: &Schema, fields: &[Field]) -> PolarsResult<Field> {
        let input = fields
            .first()
            .ok_or_else(|| polars_err!(ComputeError: "mapBatches expects one input column"))?;

        Ok(self
            .materialized_field
            .get_or_init(|| {
                let dtype = self
                    .return_type
                    .clone()
                    .unwrap_or_else(|| input.dtype().clone());
                Field::new(input.name().clone(), dtype)
            })
            .clone())
    }
}
