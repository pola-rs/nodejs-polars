use crate::conversion::{parse_fill_null_strategy, Wrap};
use crate::prelude::*;
use crate::utils::reinterpret;
use polars::lazy::dsl;
use polars::lazy::dsl::Expr;
use polars::lazy::dsl::Operator;
use polars_core::series::ops::NullBehavior;
use std::any::Any;
use std::borrow::Cow;

#[napi]
#[repr(transparent)]
#[derive(Clone)]
pub struct JsExpr {
    pub(crate) inner: dsl::Expr,
}

pub(crate) trait ToExprs {
    fn to_exprs(self) -> Vec<Expr>;
}
impl JsExpr {
    pub(crate) fn new(inner: dsl::Expr) -> JsExpr {
        JsExpr { inner }
    }
}
impl From<dsl::Expr> for JsExpr {
    fn from(s: dsl::Expr) -> JsExpr {
        JsExpr::new(s)
    }
}
impl ToExprs for Vec<JsExpr> {
    fn to_exprs(self) -> Vec<Expr> {
        // Safety
        // repr is transparent
        // and has only got one inner field`
        unsafe { std::mem::transmute(self) }
    }
}

impl ToExprs for Vec<&JsExpr> {
    fn to_exprs(self) -> Vec<Expr> {
        self.into_iter()
            .map(|e| e.inner.clone())
            .collect::<Vec<Expr>>()
    }
}

#[napi]
impl JsExpr {
    #[napi(catch_unwind)]
    pub fn to_js(&self, env: Env) -> napi::Result<napi::JsUnknown> {
        env.to_js_value(&self.inner)
    }
    #[napi(catch_unwind)]
    pub fn serialize(&self, format: String) -> napi::Result<Buffer> {
        let buf = match format.as_ref() {
            "bincode" => bincode::serialize(&self.inner)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            "json" => serde_json::to_vec(&self.inner)
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
    pub fn deserialize(buf: Buffer, format: String) -> napi::Result<JsExpr> {
        // Safety
        // we skipped the serializing/deserializing of the static in lifetime in `DataType`
        // so we actually don't have a lifetime at all when serializing.

        // &[u8] still has a lifetime. But its ok, because we drop it immediately
        // in this scope
        let bytes: &[u8] = &buf;
        let bytes = unsafe { std::mem::transmute::<&'_ [u8], &'static [u8]>(bytes) };
        let expr: Expr = match format.as_ref() {
            "bincode" => bincode::deserialize(bytes)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            "json" => serde_json::from_slice(bytes)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            _ => {
                return Err(napi::Error::from_reason(
                    "unexpected format. \n supported options are 'json', 'bincode'".to_owned(),
                ))
            }
        };
        Ok(expr.into())
    }
    #[napi(catch_unwind)]
    pub fn __add__(&self, rhs: &JsExpr) -> napi::Result<JsExpr> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Plus, rhs.inner.clone()).into())
    }

    #[napi(catch_unwind)]
    pub fn __sub__(&self, rhs: &JsExpr) -> napi::Result<JsExpr> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Minus, rhs.inner.clone()).into())
    }

    #[napi(catch_unwind)]
    pub fn __mul__(&self, rhs: &JsExpr) -> napi::Result<JsExpr> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Multiply, rhs.inner.clone()).into())
    }

    #[napi(catch_unwind)]
    pub fn __truediv__(&self, rhs: &JsExpr) -> napi::Result<JsExpr> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::TrueDivide, rhs.inner.clone()).into())
    }

    #[napi(catch_unwind)]
    pub fn __mod__(&self, rhs: &JsExpr) -> napi::Result<JsExpr> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Modulus, rhs.inner.clone()).into())
    }

    #[napi(catch_unwind)]
    pub fn __floordiv__(&self, rhs: &JsExpr) -> napi::Result<JsExpr> {
        Ok(dsl::binary_expr(self.inner.clone(), Operator::Divide, rhs.inner.clone()).into())
    }

    #[napi(catch_unwind)]
    pub fn to_string(&self) -> String {
        format!("{:?}", self.inner)
    }

    #[napi(catch_unwind)]
    pub fn eq(&self, other: &JsExpr) -> JsExpr {
        self.clone().inner.eq(other.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn neq(&self, other: &JsExpr) -> JsExpr {
        self.clone().inner.neq(other.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn gt(&self, other: &JsExpr) -> JsExpr {
        self.clone().inner.gt(other.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn gt_eq(&self, other: &JsExpr) -> JsExpr {
        self.clone().inner.gt_eq(other.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn lt_eq(&self, other: &JsExpr) -> JsExpr {
        self.clone().inner.lt_eq(other.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn lt(&self, other: &JsExpr) -> JsExpr {
        self.clone().inner.lt(other.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn alias(&self, name: String) -> JsExpr {
        self.clone().inner.alias(&name).into()
    }

    #[napi(catch_unwind)]
    pub fn is_not(&self) -> JsExpr {
        self.clone().inner.not().into()
    }

    #[napi(catch_unwind)]
    pub fn is_null(&self) -> JsExpr {
        self.clone().inner.is_null().into()
    }

    #[napi(catch_unwind)]
    pub fn is_not_null(&self) -> JsExpr {
        self.clone().inner.is_not_null().into()
    }

    #[napi(catch_unwind)]
    pub fn is_infinite(&self) -> JsExpr {
        self.clone().inner.is_infinite().into()
    }

    #[napi(catch_unwind)]
    pub fn is_finite(&self) -> JsExpr {
        self.clone().inner.is_finite().into()
    }

    #[napi(catch_unwind)]
    pub fn is_nan(&self) -> JsExpr {
        self.clone().inner.is_nan().into()
    }

    #[napi(catch_unwind)]
    pub fn is_not_nan(&self) -> JsExpr {
        self.clone().inner.is_not_nan().into()
    }

    #[napi(catch_unwind)]
    pub fn min(&self) -> JsExpr {
        self.clone().inner.min().into()
    }

    #[napi(catch_unwind)]
    pub fn max(&self) -> JsExpr {
        self.clone().inner.max().into()
    }

    #[napi(catch_unwind)]
    pub fn mean(&self) -> JsExpr {
        self.clone().inner.mean().into()
    }

    #[napi(catch_unwind)]
    pub fn median(&self) -> JsExpr {
        self.clone().inner.median().into()
    }

    #[napi(catch_unwind)]
    pub fn sum(&self) -> JsExpr {
        self.clone().inner.sum().into()
    }

    #[napi(catch_unwind)]
    pub fn n_unique(&self) -> JsExpr {
        self.clone().inner.n_unique().into()
    }

    #[napi(catch_unwind)]
    pub fn arg_unique(&self) -> JsExpr {
        self.clone().inner.arg_unique().into()
    }

    #[napi(catch_unwind)]
    pub fn unique(&self) -> JsExpr {
        self.clone().inner.unique().into()
    }

    #[napi(catch_unwind)]
    pub fn unique_stable(&self) -> JsExpr {
        self.clone().inner.unique_stable().into()
    }

    #[napi(catch_unwind)]
    pub fn first(&self) -> JsExpr {
        self.clone().inner.first().into()
    }

    #[napi(catch_unwind)]
    pub fn last(&self) -> JsExpr {
        self.clone().inner.last().into()
    }

    #[napi(catch_unwind)]
    pub fn list(&self) -> JsExpr {
        self.clone().inner.into()
    }

    #[napi(catch_unwind)]
    pub fn quantile(
        &self,
        quantile: &JsExpr,
        interpolation: Wrap<QuantileInterpolOptions>,
    ) -> JsExpr {
        self.clone()
            .inner
            .quantile(quantile.inner.clone(), interpolation.0)
            .into()
    }

    #[napi(catch_unwind)]
    pub fn agg_groups(&self) -> JsExpr {
        self.clone().inner.agg_groups().into()
    }

    #[napi(catch_unwind)]
    pub fn count(&self) -> JsExpr {
        self.clone().inner.count().into()
    }

    #[napi(catch_unwind)]
    pub fn value_counts(&self, multithreaded: bool, sorted: bool) -> JsExpr {
        self.inner
            .clone()
            .value_counts(multithreaded, sorted)
            .into()
    }

    #[napi(catch_unwind)]
    pub fn unique_counts(&self) -> JsExpr {
        self.inner.clone().unique_counts().into()
    }

    #[napi(catch_unwind)]
    pub fn cast(&self, data_type: Wrap<DataType>, strict: bool) -> JsExpr {
        let dt = data_type.0;
        let expr = if strict {
            self.inner.clone().strict_cast(dt)
        } else {
            self.inner.clone().cast(dt)
        };
        expr.into()
    }

    #[napi(catch_unwind)]
    pub fn sort_with(
        &self,
        descending: bool,
        nulls_last: bool,
        multithreaded: bool,
        maintain_order: bool,
    ) -> JsExpr {
        self.clone()
            .inner
            .sort_with(SortOptions {
                descending,
                nulls_last,
                multithreaded,
                maintain_order,
            })
            .into()
    }

    #[napi(catch_unwind)]
    pub fn arg_sort(&self, reverse: bool, multithreaded: bool, maintain_order: bool) -> JsExpr {
        self.clone()
            .inner
            .arg_sort(SortOptions {
                descending: reverse,
                nulls_last: true,
                multithreaded,
                maintain_order,
            })
            .into()
    }
    #[napi(catch_unwind)]
    pub fn arg_max(&self) -> JsExpr {
        self.clone().inner.arg_max().into()
    }
    #[napi(catch_unwind)]
    pub fn arg_min(&self) -> JsExpr {
        self.clone().inner.arg_min().into()
    }
    #[napi(catch_unwind)]
    pub fn take(&self, idx: &JsExpr) -> JsExpr {
        self.clone().inner.take(idx.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn sort_by(&self, by: Vec<&JsExpr>, reverse: Vec<bool>) -> JsExpr {
        let by = by.to_exprs();
        self.clone().inner.sort_by(by, reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn backward_fill(&self) -> JsExpr {
        self.clone().inner.backward_fill(None).into()
    }

    #[napi(catch_unwind)]
    pub fn forward_fill(&self) -> JsExpr {
        self.clone().inner.forward_fill(None).into()
    }

    #[napi(catch_unwind)]
    pub fn shift(&self, periods: i64) -> JsExpr {
        self.clone().inner.shift(periods).into()
    }

    #[napi(catch_unwind)]
    pub fn shift_and_fill(&self, periods: i64, fill_value: i64) -> JsExpr {
        self.clone()
            .inner
            .shift_and_fill(periods, fill_value)
            .into()
    }

    #[napi(catch_unwind)]
    pub fn fill_null(&self, expr: &JsExpr) -> JsExpr {
        self.clone().inner.fill_null(expr.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn fill_null_with_strategy(
        &self,
        strategy: String,
        limit: FillNullLimit,
    ) -> JsResult<JsExpr> {
        let strat = parse_fill_null_strategy(&strategy, limit)?;
        Ok(self
            .inner
            .clone()
            .apply(
                move |s| s.fill_null(strat).map(Some),
                GetOutput::same_type(),
            )
            .with_fmt("fill_null_with_strategy")
            .into())
    }
    #[napi(catch_unwind)]
    pub fn fill_nan(&self, expr: &JsExpr) -> JsExpr {
        self.inner.clone().fill_nan(expr.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn drop_nulls(&self) -> JsExpr {
        self.inner.clone().drop_nulls().into()
    }

    #[napi(catch_unwind)]
    pub fn drop_nans(&self) -> JsExpr {
        self.inner.clone().drop_nans().into()
    }

    #[napi(catch_unwind)]
    pub fn filter(&self, predicate: &JsExpr) -> JsExpr {
        self.clone().inner.filter(predicate.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn reverse(&self) -> JsExpr {
        self.clone().inner.reverse().into()
    }

    #[napi(catch_unwind)]
    pub fn std(&self, ddof: Option<u8>) -> JsExpr {
        let ddof = ddof.unwrap_or(1);
        self.clone().inner.std(ddof).into()
    }

    #[napi(catch_unwind)]
    pub fn var(&self, ddof: Option<u8>) -> JsExpr {
        let ddof = ddof.unwrap_or(1);
        self.clone().inner.var(ddof).into()
    }
    #[napi(catch_unwind)]
    pub fn is_unique(&self) -> JsExpr {
        self.clone().inner.is_unique().into()
    }

    #[napi(catch_unwind)]
    pub fn is_first_distinct(&self) -> JsExpr {
        self.clone().inner.is_first_distinct().into()
    }

    #[napi(catch_unwind)]
    pub fn explode(&self) -> JsExpr {
        self.clone().inner.explode().into()
    }
    #[napi(catch_unwind)]
    pub fn take_every(&self, n: i64) -> JsExpr {
        self.clone()
            .inner
            .map(
                move |s: Series| Ok(Some(s.take_every(n as usize))),
                GetOutput::same_type(),
            )
            .with_fmt("take_every")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn tail(&self, n: Option<i64>) -> JsExpr {
        let n = n.map(|v| v as usize);
        self.clone().inner.tail(n).into()
    }

    #[napi(catch_unwind)]
    pub fn head(&self, n: Option<i64>) -> JsExpr {
        let n = n.map(|v| v as usize);
        self.clone().inner.head(n).into()
    }
    #[napi(catch_unwind)]
    pub fn slice(&self, offset: &JsExpr, length: &JsExpr) -> JsExpr {
        self.inner
            .clone()
            .slice(offset.inner.clone(), length.inner.clone())
            .into()
    }
    #[napi(catch_unwind)]
    pub fn round(&self, decimals: u32) -> JsExpr {
        self.clone().inner.round(decimals).into()
    }

    #[napi(catch_unwind)]
    pub fn floor(&self) -> JsExpr {
        self.clone().inner.floor().into()
    }

    #[napi(catch_unwind)]
    pub fn ceil(&self) -> JsExpr {
        self.clone().inner.ceil().into()
    }
    #[napi(catch_unwind)]
    pub fn clip(&self, min: &JsExpr, max: &JsExpr) -> JsExpr {
        self.clone().inner.clip(min.inner.clone(), max.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn abs(&self) -> JsExpr {
        self.clone().inner.abs().into()
    }
    #[napi(catch_unwind)]
    pub fn is_duplicated(&self) -> JsExpr {
        self.clone().inner.is_duplicated().into()
    }
    #[napi(catch_unwind)]
    pub fn over(&self, partition_by: Vec<&JsExpr>) -> JsExpr {
        self.clone().inner.over(partition_by.to_exprs()).into()
    }
    #[napi(catch_unwind)]
    pub fn _and(&self, expr: &JsExpr) -> JsExpr {
        self.clone().inner.and(expr.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn not(&self) -> JsExpr {
        self.clone().inner.not().into()
    }
    #[napi(catch_unwind)]
    pub fn _xor(&self, expr: &JsExpr) -> JsExpr {
        self.clone().inner.xor(expr.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn _or(&self, expr: &JsExpr) -> JsExpr {
        self.clone().inner.or(expr.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn is_in(&self, expr: &JsExpr) -> JsExpr {
        self.clone().inner.is_in(expr.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn repeat_by(&self, by: &JsExpr) -> JsExpr {
        self.clone().inner.repeat_by(by.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn pow(&self, exponent: f64) -> JsExpr {
        self.clone().inner.pow(exponent).into()
    }
    #[napi(catch_unwind)]
    pub fn cumsum(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cumsum(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn cummax(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cummax(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn cummin(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cummin(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn cumprod(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cumprod(reverse).into()
    }

    #[napi(catch_unwind)]
    pub fn product(&self) -> JsExpr {
        self.clone().inner.product().into()
    }

    #[napi(catch_unwind)]
    pub fn str_to_date(
        &self,
        format: Option<String>,
        strict: bool,
        exact: bool,
        cache: bool,
    ) -> JsExpr {
        let options = StrptimeOptions {
            format,
            strict,
            exact,
            cache,
        };
        self.inner.clone().str().to_date(options).into()
    }

    #[napi(catch_unwind)]
    #[allow(clippy::too_many_arguments)]
    pub fn str_to_datetime(
        &self,
        format: Option<String>,
        time_unit: Option<Wrap<TimeUnit>>,
        time_zone: Option<TimeZone>,
        strict: bool,
        exact: bool,
        cache: bool,
        ambiguous: Option<Wrap<Expr>>,
    ) -> JsExpr {
        let options = StrptimeOptions {
            format,
            strict,
            exact,
            cache,
        };
        let ambiguous = ambiguous
            .map(|e| e.0)
            .unwrap_or(dsl::lit(String::from("raise")));
        self.inner
            .clone()
            .str()
            .to_datetime(time_unit.map(|tu| tu.0), time_zone, options, ambiguous)
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_strip(&self) -> JsExpr {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.apply(|s| Some(Cow::Borrowed(s?.trim()))).into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::same_type())
            .with_fmt("str.strip")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_rstrip(&self) -> JsExpr {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(
                ca.apply(|s| Some(Cow::Borrowed(s?.trim_end()))).into_series(),
            ))
        };
        self.clone()
            .inner
            .map(function, GetOutput::same_type())
            .with_fmt("str.rstrip")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_lstrip(&self) -> JsExpr {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(
                ca.apply(|s| Some(Cow::Borrowed(s?.trim_start()))).into_series(),
            ))
        };
        self.clone()
            .inner
            .map(function, GetOutput::same_type())
            .with_fmt("str.lstrip")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_pad_start(&self, length: i64, fill_char: String) -> JsExpr {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(
                ca.rjust(length as usize, fill_char.chars().nth(0).unwrap())
                    .into_series(),
            ))
        };

        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Utf8))
            .with_fmt("str.pad_start")
            .into()
    }

    
    #[napi(catch_unwind)]
    pub fn str_pad_end(&self, length: i64, fill_char: String) -> JsExpr {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(
                ca.ljust(length as usize, fill_char.chars().nth(0).unwrap())
                    .into_series(),
            ))
        };

        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Utf8))
            .with_fmt("str.pad_end")
            .into()
    }
    
    #[napi(catch_unwind)]
    pub fn str_z_fill(&self, width: i64) -> JsExpr {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.zfill(width as usize).into_series()))
        };

        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Utf8))
            .with_fmt("str.z_fill")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_to_uppercase(&self) -> JsExpr {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.to_uppercase().into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.to_uppercase")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_slice(&self, start: i64, length: Option<i64>) -> JsExpr {
        let function = move |s: Series| {
            let length = length.map(|l| l as u64);
            let ca = s.utf8()?;
            Ok(Some(ca.str_slice(start, length).into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Utf8))
            .with_fmt("str.slice")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_to_lowercase(&self) -> JsExpr {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.to_lowercase().into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.to_lowercase")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_lengths(&self) -> JsExpr {
        let function = |s: Series| {
            let ca = s.utf8()?;
            Ok(Some(ca.str_len_chars().into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.len")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_replace(&self, pat: String, val: String) -> JsExpr {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            match ca.replace(&pat, &val) {
                Ok(ca) => Ok(Some(ca.into_series())),
                Err(e) => Err(PolarsError::ComputeError(format!("{:?}", e).into())),
            }
        };
        self.clone()
            .inner
            .map(function, GetOutput::same_type())
            .with_fmt("str.replace")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_replace_all(&self, pat: String, val: String) -> JsExpr {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            match ca.replace_all(&pat, &val) {
                Ok(ca) => Ok(Some(ca.into_series())),
                Err(e) => Err(PolarsError::ComputeError(format!("{:?}", e).into())),
            }
        };
        self.clone()
            .inner
            .map(function, GetOutput::same_type())
            .with_fmt("str.replace_all")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_contains(&self, pat: String, strict: bool) -> JsExpr {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            match ca.contains(&pat, strict) {
                Ok(ca) => Ok(Some(ca.into_series())),
                Err(e) => Err(PolarsError::ComputeError(format!("{:?}", e).into())),
            }
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Boolean))
            .with_fmt("str.contains")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_hex_encode(&self) -> JsExpr {
        self.clone()
            .inner
            .map(
                move |s| s.utf8().map(|s| Some(s.hex_encode().into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("str.hex_encode")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_hex_decode(&self, strict: bool) -> JsExpr {
        self.clone()
            .inner
            .map(
                move |s| s.utf8()?.hex_decode(strict).map(|s| Some(s.into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("str.hex_decode")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_base64_encode(&self) -> JsExpr {
        self.clone()
            .inner
            .map(
                move |s| s.utf8().map(|s| Some(s.base64_encode().into_series())),
                GetOutput::same_type(),
            )
            .with_fmt("str.base64_encode")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_base64_decode(&self, strict: bool) -> JsExpr {
        self.clone()
            .inner
            .map(
                move |s| {
                    s.utf8()?
                        .base64_decode(strict)
                        .map(|s| Some(s.into_series()))
                },
                GetOutput::same_type(),
            )
            .with_fmt("str.base64_decode")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_json_extract(
        &self,
        dtype: Option<Wrap<DataType>>,
        infer_schema_len: Option<i64>,
    ) -> JsExpr {
        let dt = dtype.clone().map(|d| d.0 as DataType);
        let infer_schema_len = infer_schema_len.map(|l| l as usize);
        self.inner
            .clone()
            .str()
            .json_extract(dt, infer_schema_len)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_json_path_match(&self, pat: String) -> JsExpr {
        let function = move |s: Series| {
            let ca = s.utf8()?;
            match ca.json_path_match(&pat) {
                Ok(ca) => Ok(Some(ca.into_series())),
                Err(e) => Err(PolarsError::ComputeError(format!("{:?}", e).into())),
            }
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::Boolean))
            .with_fmt("str.json_path_match")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_extract(&self, pat: String, group_index: i64) -> JsExpr {
        self.inner
            .clone()
            .str()
            .extract(&pat, group_index as usize)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn strftime(&self, fmt: String) -> JsExpr {
        self.inner.clone().dt().strftime(&fmt).into()
    }
    #[napi(catch_unwind)]
    pub fn str_split(&self, by: &JsExpr) -> JsExpr {
        self.inner.clone().str().split(by.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn str_split_inclusive(&self, by: Wrap<Expr>) -> JsExpr {
        self.inner.clone().str().split_inclusive(by.0).into()
    }
    #[napi(catch_unwind)]
    pub fn str_split_exact(&self, by: Wrap<Expr>, n: i64) -> JsExpr {
        self.inner.clone().str().split_exact(by.0, n as usize).into()
    }
    #[napi(catch_unwind)]
    pub fn str_split_exact_inclusive(&self, by: Wrap<Expr>, n: i64) -> JsExpr {
        self.inner
            .clone()
            .str()
            .split_exact_inclusive(by.0, n as usize)
            .into()
    }

    #[napi(catch_unwind)]
    pub fn year(&self) -> JsExpr {
        self.clone().inner.dt().year().into()
    }
    #[napi(catch_unwind)]
    pub fn month(&self) -> JsExpr {
        self.clone().inner.dt().month().into()
    }
    #[napi(catch_unwind)]
    pub fn week(&self) -> JsExpr {
        self.clone().inner.dt().week().into()
    }
    #[napi(catch_unwind)]
    pub fn weekday(&self) -> JsExpr {
        self.clone().inner.dt().weekday().into()
    }
    #[napi(catch_unwind)]
    pub fn day(&self) -> JsExpr {
        self.clone().inner.dt().day().into()
    }
    #[napi(catch_unwind)]
    pub fn ordinal_day(&self) -> JsExpr {
        self.clone().inner.dt().ordinal_day().into()
    }
    #[napi(catch_unwind)]
    pub fn hour(&self) -> JsExpr {
        self.clone().inner.dt().hour().into()
    }
    #[napi(catch_unwind)]
    pub fn minute(&self) -> JsExpr {
        self.clone().inner.dt().minute().into()
    }
    #[napi(catch_unwind)]
    pub fn second(&self) -> JsExpr {
        self.clone().inner.dt().second().into()
    }
    #[napi(catch_unwind)]
    pub fn nanosecond(&self) -> JsExpr {
        self.clone().inner.dt().nanosecond().into()
    }
    #[napi(catch_unwind)]
    pub fn duration_days(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.days().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_hours(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.hours().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_seconds(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.seconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_nanoseconds(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.nanoseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_milliseconds(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.milliseconds().into_series())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn timestamp(&self) -> JsExpr {
        self.inner
            .clone()
            .dt()
            .timestamp(TimeUnit::Milliseconds)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn dt_epoch_seconds(&self) -> JsExpr {
        self.clone()
            .inner
            .map(
                |s| {
                    s.timestamp(TimeUnit::Milliseconds)
                        .map(|ca| Some((ca / 1000).into_series()))
                },
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn dot(&self, other: &JsExpr) -> JsExpr {
        self.inner.clone().dot(other.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn hash(&self, k0: Wrap<u64>, k1: Wrap<u64>, k2: Wrap<u64>, k3: Wrap<u64>) -> JsExpr {
        let function = move |s: Series| {
            let hb = polars::export::ahash::RandomState::with_seeds(k0.0, k1.0, k2.0, k3.0);
            Ok(Some(s.hash(hb).into_series()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt64))
            .into()
    }

    #[napi(catch_unwind)]
    pub fn reinterpret(&self, signed: bool) -> JsExpr {
        let function = move |s: Series| reinterpret(&s, signed).map(Some);
        let dt = if signed {
            DataType::Int64
        } else {
            DataType::UInt64
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(dt))
            .into()
    }
    #[napi(catch_unwind)]
    pub fn mode(&self) -> JsExpr {
        self.inner.clone().mode().into()
    }
    #[napi(catch_unwind)]
    pub fn keep_name(&self) -> JsExpr {
        self.inner.clone().keep_name().into()
    }
    #[napi(catch_unwind)]
    pub fn prefix(&self, prefix: String) -> JsExpr {
        self.inner.clone().prefix(&prefix).into()
    }
    #[napi(catch_unwind)]
    pub fn suffix(&self, suffix: String) -> JsExpr {
        self.inner.clone().suffix(&suffix).into()
    }

    #[napi(catch_unwind)]
    pub fn exclude(&self, columns: Vec<String>) -> JsExpr {
        self.inner.clone().exclude(&columns).into()
    }

    #[napi(catch_unwind)]
    pub fn exclude_dtype(&self, dtypes: Vec<Wrap<DataType>>) -> JsExpr {
        // Safety:
        // Wrap is transparent.
        let dtypes: Vec<DataType> = unsafe { std::mem::transmute(dtypes) };
        self.inner.clone().exclude_dtype(&dtypes).into()
    }
    #[napi(catch_unwind)]
    pub fn interpolate(&self, method: Wrap<InterpolationMethod>) -> JsExpr {
        self.inner.clone().interpolate(method.0).into()
    }
    #[napi(catch_unwind)]
    pub fn peak_min(&self) -> JsExpr {
        self.inner.clone().peak_min().into()
    }
    #[napi(catch_unwind)]
    pub fn peak_max(&self) -> JsExpr {
        self.inner.clone().peak_max().into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_sum(&self, options: JsRollingOptions) -> JsExpr {
        self.inner.clone().rolling_sum(options.into()).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_min(&self, options: JsRollingOptions) -> JsExpr {
        self.inner.clone().rolling_min(options.into()).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_max(&self, options: JsRollingOptions) -> JsExpr {
        self.inner.clone().rolling_max(options.into()).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_mean(&self, options: JsRollingOptions) -> JsExpr {
        self.inner.clone().rolling_mean(options.into()).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_std(&self, options: JsRollingOptions) -> JsExpr {
        self.inner.clone().rolling_std(options.into()).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_var(&self, options: JsRollingOptions) -> JsExpr {
        self.inner.clone().rolling_var(options.into()).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_median(&self, options: JsRollingOptions) -> JsExpr {
        self.inner.clone().rolling_median(options.into()).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_quantile(
        &self,
        quantile: f64,
        interpolation: Wrap<QuantileInterpolOptions>,
        window_size: String,
        weights: Option<Vec<f64>>,
        min_periods: i64,
        center: bool,
        by: Option<String>,
        closed: Option<Wrap<ClosedWindow>>,
    ) -> JsExpr {
        let options = RollingOptions {
            window_size: Duration::parse(&window_size),
            min_periods: min_periods as usize,
            weights,
            center,
            by,
            closed_window: closed.map(|c| c.0),
            fn_params: Some(Arc::new(RollingQuantileParams {
                prob: quantile,
                interpol: interpolation.0,
            }) as Arc<dyn Any + Send + Sync>),
        };
        self.inner.clone().rolling_quantile(options).into()
    }
    #[napi(catch_unwind)]
    pub fn rolling_skew(&self, window_size: i64, bias: bool) -> JsExpr {
        self.inner
            .clone()
            .rolling_map_float(window_size as usize, move |ca| {
                ca.clone().into_series().skew(bias).unwrap()
            })
            .into()
    }
    #[napi(catch_unwind)]
    pub fn lower_bound(&self) -> JsExpr {
        self.inner.clone().lower_bound().into()
    }
    
    #[napi(catch_unwind)]
    pub fn upper_bound(&self) -> JsExpr {
        self.inner.clone().upper_bound().into()
    }

    #[napi(catch_unwind)]
    pub fn list_max(&self) -> JsExpr {
        self.inner.clone().list().max().into()
    }
    #[napi(catch_unwind)]
    pub fn list_min(&self) -> JsExpr {
        self.inner.clone().list().min().into()
    }

    #[napi(catch_unwind)]
    pub fn list_sum(&self) -> JsExpr {
        self.inner.clone().list().sum().with_fmt("arr.sum").into()
    }

    #[napi(catch_unwind)]
    pub fn list_mean(&self) -> JsExpr {
        self.inner.clone().list().mean().with_fmt("arr.mean").into()
    }

    #[napi(catch_unwind)]
    pub fn list_sort(&self, descending: bool) -> JsExpr {
        self.inner
            .clone()
            .list()
            .sort(SortOptions {
                descending,
                ..Default::default()
            })
            .with_fmt("arr.sort")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn list_reverse(&self) -> JsExpr {
        self.inner.clone().list().reverse().into()
    }

    #[napi(catch_unwind)]
    pub fn list_unique(&self) -> JsExpr {
        self.inner.clone().list().unique().into()
    }
    #[napi(catch_unwind)]
    pub fn list_lengths(&self) -> JsExpr {
        self.inner.clone().list().len().into()
    }
    #[napi(catch_unwind)]
    pub fn list_get(&self, index: &JsExpr) -> JsExpr {
        self.inner.clone().list().get(index.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn list_join(&self, separator: &JsExpr) -> JsExpr {
        self.inner.clone().list().join(separator.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn list_arg_min(&self) -> JsExpr {
        self.inner.clone().list().arg_min().into()
    }

    #[napi(catch_unwind)]
    pub fn list_arg_max(&self) -> JsExpr {
        self.inner.clone().list().arg_max().into()
    }
    #[napi(catch_unwind)]
    pub fn list_diff(&self, n: i64, null_behavior: Wrap<NullBehavior>) -> JsExpr {
        self.inner.clone().list().diff(n, null_behavior.0).into()
    }

    #[napi(catch_unwind)]
    pub fn list_shift(&self, periods: Wrap<Expr>) -> JsExpr {
        self.inner.clone().list().shift(periods.0).into()
    }
    #[napi(catch_unwind)]
    pub fn list_slice(&self, offset: &JsExpr, length: Option<&JsExpr>) -> JsExpr {
        let length = match length {
            Some(i) => i.inner.clone(),
            None => dsl::lit(i64::MAX),
        };
        self.inner
            .clone()
            .list()
            .slice(offset.inner.clone(), length)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn list_eval(&self, expr: &JsExpr, parallel: bool) -> JsExpr {
        self.inner
            .clone()
            .list()
            .eval(expr.inner.clone(), parallel)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn rank(&self, method: Wrap<RankMethod>, reverse: bool, seed: Option<Wrap<u64>>) -> JsExpr {
        // Safety:
        // Wrap is transparent.
        let seed: Option<u64> = unsafe { std::mem::transmute(seed) };
        let options = RankOptions {
            method: method.0,
            descending: reverse,
        };
        self.inner.clone().rank(options, seed).into()
    }
    #[napi(catch_unwind)]
    pub fn diff(&self, n: i64, null_behavior: Wrap<NullBehavior>) -> JsExpr {
        self.inner.clone().diff(n, null_behavior.0).into()
    }
    #[napi(catch_unwind)]
    pub fn pct_change(&self, n: Wrap<Expr>) -> JsExpr {
        self.inner.clone().pct_change(n.0).into()
    }

    #[napi(catch_unwind)]
    pub fn skew(&self, bias: bool) -> JsExpr {
        self.inner.clone().skew(bias).into()
    }
    #[napi(catch_unwind)]
    pub fn kurtosis(&self, fisher: bool, bias: bool) -> JsExpr {
        self.inner.clone().kurtosis(fisher, bias).into()
    }
    #[napi(catch_unwind)]
    pub fn str_concat(&self, delimiter: String) -> JsExpr {
        self.inner.clone().str().concat(&delimiter).into()
    }
    #[napi(catch_unwind)]
    pub fn cat_set_ordering(&self, ordering: String) -> JsExpr {
        let ordering = match ordering.as_ref() {
            "physical" => CategoricalOrdering::Physical,
            "lexical" => CategoricalOrdering::Lexical,
            _ => panic!("expected one of {{'physical', 'lexical'}}"),
        };

        self.inner.clone().cat().set_ordering(ordering).into()
    }
    #[napi(catch_unwind)]
    pub fn reshape(&self, dims: Vec<i64>) -> JsExpr {
        self.inner.clone().reshape(&dims).into()
    }
    #[napi(catch_unwind)]
    pub fn cumcount(&self, reverse: bool) -> JsExpr {
        self.inner.clone().cumcount(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn to_physical(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.to_physical_repr().into_owned())),
                GetOutput::map_dtype(|dt| dt.to_physical()),
            )
            .with_fmt("to_physical")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn shuffle(&self, seed: Wrap<u64>) -> JsExpr {
        self.inner.clone().shuffle(Some(seed.0)).into()
    }

    #[napi(catch_unwind)]
    pub fn sample_frac(
        &self,
        frac: Wrap<Expr>,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<i64>,
    ) -> JsExpr {
        let seed = seed.map(|s| s as u64);
        self.inner
            .clone()
            .sample_frac(frac.0, with_replacement, shuffle, seed)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn ewm_mean(
        &self,
        alpha: f64,
        adjust: bool,
        min_periods: i64,
        bias: bool,
        ignore_nulls: bool,
    ) -> JsExpr {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods: min_periods as usize,
            ignore_nulls,
        };
        self.inner.clone().ewm_mean(options).into()
    }
    #[napi(catch_unwind)]
    pub fn ewm_std(
        &self,
        alpha: f64,
        adjust: bool,
        min_periods: i64,
        bias: bool,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods: min_periods as usize,
            ignore_nulls,
        };
        self.inner.clone().ewm_std(options).into()
    }
    #[napi(catch_unwind)]
    pub fn ewm_var(
        &self,
        alpha: f64,
        adjust: bool,
        min_periods: i64,
        bias: bool,
        ignore_nulls: bool,
    ) -> Self {
        let options = EWMOptions {
            alpha,
            adjust,
            bias,
            min_periods: min_periods as usize,
            ignore_nulls,
        };
        self.inner.clone().ewm_var(options).into()
    }
    #[napi(catch_unwind)]
    pub fn extend_constant(&self, value: Option<JsAnyValue>, n: i64) -> Self {
        self.inner
            .clone()
            .apply(
                move |s| Ok(Some(s.extend_constant(value.clone().into(), n as usize)?)),
                GetOutput::same_type(),
            )
            .with_fmt("extend")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn any(&self, drop_nulls: bool) -> JsExpr {
        self.inner.clone().any(drop_nulls).into()
    }

    #[napi(catch_unwind)]
    pub fn all(&self, drop_nulls: bool) -> JsExpr {
        self.inner.clone().all(drop_nulls).into()
    }
    #[napi(catch_unwind)]
    pub fn struct_field_by_name(&self, name: String) -> JsExpr {
        self.inner.clone().struct_().field_by_name(&name).into()
    }
    #[napi(catch_unwind)]
    pub fn struct_rename_fields(&self, names: Vec<String>) -> JsExpr {
        self.inner.clone().struct_().rename_fields(names).into()
    }
    #[napi(catch_unwind)]
    pub fn log(&self, base: f64) -> JsExpr {
        self.inner.clone().log(base).into()
    }
    #[napi(catch_unwind)]
    pub fn entropy(&self, base: f64, normalize: bool) -> JsExpr {
        self.inner.clone().entropy(base, normalize).into()
    }
    #[napi(catch_unwind)]
    pub fn add(&self, rhs: &JsExpr) -> JsExpr {
        dsl::binary_expr(self.inner.clone(), Operator::Plus, rhs.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn sub(&self, rhs: &JsExpr) -> JsExpr {
        dsl::binary_expr(self.inner.clone(), Operator::Minus, rhs.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn mul(&self, rhs: &JsExpr) -> JsExpr {
        dsl::binary_expr(self.inner.clone(), Operator::Multiply, rhs.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn true_div(&self, rhs: &JsExpr) -> JsExpr {
        dsl::binary_expr(self.inner.clone(), Operator::TrueDivide, rhs.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn rem(&self, rhs: &JsExpr) -> JsExpr {
        dsl::binary_expr(self.inner.clone(), Operator::Modulus, rhs.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn div(&self, rhs: &JsExpr) -> JsExpr {
        dsl::binary_expr(self.inner.clone(), Operator::Divide, rhs.inner.clone()).into()
    }
}

#[napi(catch_unwind)]
pub fn when(condition: &JsExpr) -> JsWhen {
    JsWhen {
        inner: dsl::when(condition.inner.clone()),
    }
}

#[napi]
#[derive(Clone)]
pub struct JsWhen {
    inner: dsl::When,
}

#[napi]
#[derive(Clone)]
pub struct JsThen {
    inner: dsl::Then,
}

#[napi]
#[derive(Clone)]
pub struct JsChainedWhen {
    inner: dsl::ChainedWhen,
}

#[napi]
#[derive(Clone)]
pub struct JsChainedThen {
    inner: dsl::ChainedThen,
}

#[napi]
impl JsWhen {
    #[napi(catch_unwind)]
    pub fn then(&self, statement: &JsExpr) -> JsThen {
        JsThen {
            inner: self.inner.clone().then(statement.inner.clone()),
        }
    }
}

#[napi]
impl JsThen {
    #[napi(catch_unwind)]
    pub fn when(&self, condition: &JsExpr) -> JsChainedWhen {
        JsChainedWhen {
            inner: self.inner.clone().when(condition.inner.clone()),
        }
    }
    #[napi(catch_unwind)]
    pub fn otherwise(&self, statement: &JsExpr) -> JsExpr {
        self.inner.clone().otherwise(statement.inner.clone()).into()
    }
}

#[napi]
impl JsChainedWhen {
    #[napi(catch_unwind)]
    pub fn then(&self, statement: &JsExpr) -> JsChainedThen {
        JsChainedThen {
            inner: self.inner.clone().then(statement.inner.clone()),
        }
    }
}

#[napi]
impl JsChainedThen {
    #[napi(catch_unwind)]
    pub fn when(&self, condition: &JsExpr) -> JsChainedWhen {
        JsChainedWhen {
            inner: self.inner.clone().when(condition.inner.clone()),
        }
    }
    #[napi(catch_unwind)]

    pub fn otherwise(&self, statement: &JsExpr) -> JsExpr {
        self.inner.clone().otherwise(statement.inner.clone()).into()
    }
}
#[napi(catch_unwind)]
pub fn col(name: String) -> JsExpr {
    dsl::col(&name).into()
}

#[napi(catch_unwind)]
pub fn count() -> JsExpr {
    dsl::count().into()
}

#[napi(catch_unwind)]
pub fn first() -> JsExpr {
    dsl::first().into()
}

#[napi(catch_unwind)]
pub fn last() -> JsExpr {
    dsl::last().into()
}

#[napi(catch_unwind)]
pub fn cols(names: Vec<String>) -> JsExpr {
    dsl::cols(names).into()
}

#[napi(catch_unwind)]
pub fn dtype_cols(dtypes: Vec<Wrap<DataType>>) -> crate::lazy::dsl::JsExpr {
    // Safety
    // Wrap is transparent
    let dtypes: Vec<DataType> = unsafe { std::mem::transmute(dtypes) };
    dsl::dtype_cols(dtypes).into()
}

#[napi(catch_unwind)]
pub fn int_range(
    start: Wrap<Expr>,
    end: Wrap<Expr>,
    step: i64,
    dtype: Option<Wrap<DataType>>,
) -> JsExpr {
    let dtype = dtype.map(|d| d.0 as DataType);

    let mut result = dsl::int_range(start.0, end.0, step);

    if dtype.is_some() && dtype.clone().unwrap() != DataType::Int64 {
        result = result.cast(dtype.clone().unwrap());
    }

    result.into()
}

#[napi(catch_unwind)]
pub fn int_ranges(
    start: Wrap<Expr>,
    end: Wrap<Expr>,
    step: i64,
    dtype: Option<Wrap<DataType>>,
) -> JsExpr {
    let dtype = dtype.map(|d| d.0 as DataType);

    let mut result = dsl::int_ranges(start.0, end.0, step);

    if dtype.is_some() && dtype.clone().unwrap() != DataType::Int64 {
        result = result.cast(DataType::List(Box::new(dtype.clone().unwrap())));
    }

    result.into()
}

#[napi(catch_unwind)]
pub fn pearson_corr(a: Wrap<Expr>, b: Wrap<Expr>, ddof: Option<u8>) -> JsExpr {
    let ddof = ddof.unwrap_or(1);
    polars::lazy::dsl::pearson_corr(a.0, b.0, ddof).into()
}

#[napi(catch_unwind)]
pub fn spearman_rank_corr(
    a: Wrap<Expr>,
    b: Wrap<Expr>,
    ddof: Option<u8>,
    propagate_nans: bool,
) -> JsExpr {
    let ddof = ddof.unwrap_or(1);
    polars::lazy::dsl::spearman_rank_corr(a.0, b.0, ddof, propagate_nans).into()
}

#[napi(catch_unwind)]
pub fn cov(a: Wrap<Expr>, b: Wrap<Expr>) -> JsExpr {
    polars::lazy::dsl::cov(a.0, b.0).into()
}

#[napi(catch_unwind)]
#[cfg(feature = "range")]
pub fn arg_sort_by(by: Vec<&JsExpr>, descending: Vec<bool>) -> JsExpr {
    let by = by.to_exprs();
    polars::lazy::dsl::arg_sort_by(by, &descending).into()
}

#[napi(catch_unwind)]
pub fn lit(value: Wrap<AnyValue>) -> JsResult<JsExpr> {
    let lit: LiteralValue = value.0.try_into().map_err(JsPolarsErr::from)?;
    Ok(dsl::lit(lit).into())
}

#[napi(catch_unwind)]
pub fn range(low: Wrap<Expr>, high: Wrap<Expr>, dtype: Wrap<DataType>) -> JsExpr {
    int_range(low, high, 1, Some(dtype)).into()
}

#[napi(catch_unwind)]
pub fn concat_lst(s: Vec<&JsExpr>) -> JsResult<JsExpr> {
    let s = s.to_exprs();
    let expr = polars::lazy::dsl::concat_list(s).map_err(JsPolarsErr::from)?;
    Ok(expr.into())
}

#[napi(catch_unwind)]
pub fn concat_str(s: Vec<&JsExpr>, sep: String) -> JsExpr {
    let s = s.to_exprs();
    dsl::concat_str(s, &sep).into()
}

#[napi(catch_unwind)]
pub fn as_struct(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    polars::lazy::dsl::as_struct(exprs).into()
}

#[napi(catch_unwind)]
pub fn all_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::all_horizontal(exprs).into()
}

#[napi(catch_unwind)]
pub fn any_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::any_horizontal(exprs).into()
}
#[napi(catch_unwind)]
pub fn min_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::min_horizontal(exprs).into()
}

#[napi(catch_unwind)]
pub fn max_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::max_horizontal(exprs).into()
}
#[napi(catch_unwind)]
pub fn sum_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::sum_horizontal(exprs).into()
}
