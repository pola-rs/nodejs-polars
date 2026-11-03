//! Bridges Polars lazy `map_batches` to a JavaScript callback via N-API threadsafe functions.
//!
//! Each batch is serialized with bincode, executed on the JS thread, decoded back into a
//! `Series`, then cast to the materialized output field when needed.

use std::sync::{Arc, OnceLock};

use napi::bindgen_prelude::{Buffer, Function, Status};
use napi::threadsafe_function::{
    ThreadsafeCallContext, ThreadsafeFunction, ThreadsafeFunctionCallMode,
};
use polars::prelude::*;
use polars_plan::dsl::anonymous::{new_column_udf, AnonymousColumnsUdf, ColumnsUdf};
use polars_plan::prelude::{FunctionFlags, FunctionOptions};

use crate::conversion::Wrap;

type MapBatchesTsfn = ThreadsafeFunction<
    Buffer,
    Buffer,
    Vec<Buffer>,
    Status,
    false,
    false,
    0,
>;

fn bin_config() -> bincode::config::Configuration {
    bincode::config::standard()
        .with_no_limit()
        .with_variable_int_encoding()
}

fn series_to_bytes(s: &Series) -> PolarsResult<Vec<u8>> {
    bincode::serde::encode_to_vec(s, bin_config()).map_err(|e| {
        polars_err!(ComputeError: "mapBatches failed to encode Series: {}", e)
    })
}

fn series_from_bytes(buf: &[u8]) -> PolarsResult<Series> {
    bincode::serde::decode_from_slice(buf, bin_config())
        .map(|(s, _)| s)
        .map_err(|e| polars_err!(ComputeError: "mapBatches failed to decode Series: {}", e))
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
        callback: Function<'_, (Buffer,), Buffer>,
        return_type: Option<DataType>,
        is_elementwise: bool,
        returns_scalar: bool,
    ) -> napi::Result<Self> {
        let tsfn = callback
            .build_threadsafe_function::<Buffer>()
            .build_callback(|ctx: ThreadsafeCallContext<Buffer>| Ok(vec![ctx.value]))?;
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

    fn invoke_js(&self, input_bytes: Vec<u8>) -> PolarsResult<Series> {
        if input_bytes.is_empty() {
            polars_bail!(ComputeError: "mapBatches encoded empty batch");
        }

        let (tx, rx) = std::sync::mpsc::sync_channel::<PolarsResult<Series>>(1);

        let status = self.tsfn.call_with_return_value(
            Buffer::from(input_bytes),
            ThreadsafeFunctionCallMode::Blocking,
            move |ret, _env| {
                let out: PolarsResult<Series> = ret
                    .map_err(|e| polars_err!(ComputeError: "{}", e))
                    .and_then(|buf| series_from_bytes(buf.as_ref()));
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

        rx.recv()
            .map_err(|_| {
                polars_err!(ComputeError: "mapBatches callback channel closed unexpectedly")
            })?
    }
}

impl ColumnsUdf for JsMapBatchesUdf {
    fn call_udf(&self, s: &mut [Column]) -> PolarsResult<Column> {
        let field = self.materialized_output_field()?;

        let input_col = s
            .first_mut()
            .ok_or_else(|| polars_err!(ComputeError: "mapBatches expects one input column"))?;
        let input_col = std::mem::take(input_col);
        let input_bytes = series_to_bytes(&input_col.take_materialized_series())?;
        let out = self.invoke_js(input_bytes)?;
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
        let input = fields.first().ok_or_else(|| {
            polars_err!(ComputeError: "mapBatches expects one input column")
        })?;

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
