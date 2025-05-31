use crate::conversion::Wrap;
use crate::prelude::*;
use crate::utils::reinterpret;
use polars::lazy::dsl;
use polars::lazy::dsl::Expr;
use polars_compute::rolling::RollingQuantileParams;
use polars_core::series::ops::NullBehavior;
use polars_utils::aliases::PlFixedStateQuality;
use std::borrow::Cow;
use std::hash::BuildHasher;

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
    pub fn quantile(&self, quantile: &JsExpr, interpolation: Wrap<QuantileMethod>) -> JsExpr {
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
    pub fn value_counts(
        &self,
        sort: bool,
        parallel: bool,
        name: String,
        normalize: bool,
    ) -> JsExpr {
        self.inner
            .clone()
            .value_counts(sort, parallel, &name, normalize)
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
    pub fn sort_with(&self, descending: bool, nulls_last: bool, maintain_order: bool) -> JsExpr {
        self.clone()
            .inner
            .sort(
                SortOptions::default()
                    .with_order_descending(descending)
                    .with_nulls_last(nulls_last)
                    .with_maintain_order(maintain_order),
            )
            .into()
    }

    #[napi(catch_unwind)]
    pub fn arg_sort(&self, descending: bool) -> JsExpr {
        self.clone()
            .inner
            .arg_sort(SortOptions::default().with_order_descending(descending))
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
    pub fn gather(&self, idx: &JsExpr) -> JsExpr {
        self.clone()
            .inner
            .gather(idx.inner.clone().cast(DataType::Int64))
            .into()
    }
    #[napi(catch_unwind)]
    pub fn sort_by(&self, by: Vec<&JsExpr>, reverse: Vec<bool>) -> JsExpr {
        self.clone()
            .inner
            .sort_by(
                by.to_exprs(),
                SortMultipleOptions::default().with_order_descending_multi(reverse),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn backward_fill(&self) -> JsExpr {
        self.clone()
            .inner
            .fill_null_with_strategy(FillNullStrategy::Backward(None))
            .into()
    }

    #[napi(catch_unwind)]
    pub fn forward_fill(&self) -> JsExpr {
        self.clone()
            .inner
            .fill_null_with_strategy(FillNullStrategy::Forward(None))
            .into()
    }

    #[napi(catch_unwind)]
    pub fn shift(&self, periods: &JsExpr) -> JsExpr {
        self.clone().inner.shift(periods.inner.clone()).into()
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
    pub fn implode(&self) -> JsExpr {
        self.clone().inner.implode().into()
    }
    #[napi(catch_unwind)]
    pub fn gather_every(&self, n: i64, offset: i64) -> JsExpr {
        self.clone()
            .inner
            .map(
                move |s: Column| {
                    Ok(Some(
                        s.gather_every(n as usize, offset as usize).expect("REASON"),
                    ))
                },
                GetOutput::same_type(),
            )
            .with_fmt("gather_every")
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
            .slice(
                offset.inner.clone().cast(DataType::Int64),
                length.inner.clone().cast(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn round(&self, decimals: u32, mode: Wrap<RoundMode>) -> JsExpr {
        self.clone().inner.round(decimals, mode.0).into()
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
        self.clone()
            .inner
            .clip(min.inner.clone(), max.inner.clone())
            .into()
    }
    #[napi(catch_unwind)]
    pub fn abs(&self) -> JsExpr {
        self.clone().inner.abs().into()
    }
    #[napi(catch_unwind)]
    pub fn sin(&self) -> Self {
        self.inner.clone().sin().into()
    }
    #[napi(catch_unwind)]
    pub fn cos(&self) -> Self {
        self.inner.clone().cos().into()
    }
    #[napi(catch_unwind)]
    pub fn tan(&self) -> Self {
        self.inner.clone().tan().into()
    }
    #[napi(catch_unwind)]
    pub fn cot(&self) -> Self {
        self.inner.clone().cot().into()
    }
    #[napi(catch_unwind)]
    pub fn arcsin(&self) -> Self {
        self.inner.clone().arcsin().into()
    }
    #[napi(catch_unwind)]
    pub fn arccos(&self) -> Self {
        self.inner.clone().arccos().into()
    }
    #[napi(catch_unwind)]
    pub fn arctan(&self) -> Self {
        self.inner.clone().arctan().into()
    }
    #[napi(catch_unwind)]
    pub fn arctan2(&self, y: &JsExpr) -> Self {
        self.inner.clone().arctan2(y.inner.clone()).into()
    }
    #[napi(catch_unwind)]
    pub fn sinh(&self) -> Self {
        self.inner.clone().sinh().into()
    }
    #[napi(catch_unwind)]
    pub fn cosh(&self) -> Self {
        self.inner.clone().cosh().into()
    }
    #[napi(catch_unwind)]
    pub fn tanh(&self) -> Self {
        self.inner.clone().tanh().into()
    }
    #[napi(catch_unwind)]
    pub fn arcsinh(&self) -> Self {
        self.inner.clone().arcsinh().into()
    }
    #[napi(catch_unwind)]
    pub fn arccosh(&self) -> Self {
        self.inner.clone().arccosh().into()
    }
    #[napi(catch_unwind)]
    pub fn arctanh(&self) -> Self {
        self.inner.clone().arctanh().into()
    }
    #[napi(catch_unwind)]
    pub fn degrees(&self) -> Self {
        self.inner.clone().degrees().into()
    }
    #[napi(catch_unwind)]
    pub fn radians(&self) -> Self {
        self.inner.clone().radians().into()
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
    pub fn is_in(&self, expr: &JsExpr, nulls_equal: bool) -> JsExpr {
        self.clone()
            .inner
            .is_in(expr.inner.clone(), nulls_equal)
            .into()
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
    pub fn cum_sum(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cum_sum(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn cum_max(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cum_max(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn cum_min(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cum_min(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn cum_prod(&self, reverse: bool) -> JsExpr {
        self.clone().inner.cum_prod(reverse).into()
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
            format: format.map_or(None, |s| Some(PlSmallStr::from_string(s))),
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
        time_zone: Option<String>,
        strict: bool,
        exact: bool,
        cache: bool,
        ambiguous: Option<Wrap<Expr>>,
    ) -> JsExpr {
        let options = StrptimeOptions {
            format: format.map_or(None, |s| Some(PlSmallStr::from_string(s))),
            strict,
            exact,
            cache,
        };
        let ambiguous = ambiguous
            .map(|e| e.0)
            .unwrap_or(dsl::lit(String::from("raise")));
        let time_zone = time_zone.map_or(None, |x| Some(PlSmallStr::from_string(x)));
        self.inner
            .clone()
            .str()
            .to_datetime(time_unit.map(|tu| tu.0), time_zone, options, ambiguous)
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_strip(&self) -> JsExpr {
        let function = |s: Column| {
            let ca = s.str()?;
            Ok(Some(
                ca.apply(|s| Some(Cow::Borrowed(s?.trim()))).into_column(),
            ))
        };
        self.clone()
            .inner
            .map(function, GetOutput::same_type())
            .with_fmt("str.strip")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_strip_chars(&self, pattern: &JsExpr, start: bool, end: bool) -> JsExpr {
        match (start, end) {
            (false, true) => self
                .inner
                .clone()
                .str()
                .strip_chars_end(pattern.inner.clone())
                .into(),
            (true, false) => self
                .inner
                .clone()
                .str()
                .strip_chars_start(pattern.inner.clone())
                .into(),
            (true, true) => self
                .inner
                .clone()
                .str()
                .strip_chars(pattern.inner.clone())
                .into(),
            _ => self.inner.clone().into(),
        }
    }

    #[napi(catch_unwind)]
    pub fn str_rstrip(&self) -> JsExpr {
        let function = |s: Column| {
            let ca = s.str()?;
            Ok(Some(
                ca.apply(|s| Some(Cow::Borrowed(s?.trim_end())))
                    .into_column(),
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
        let function = |s: Column| {
            let ca = s.str()?;
            Ok(Some(
                ca.apply(|s| Some(Cow::Borrowed(s?.trim_start())))
                    .into_column(),
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
        let function = move |s: Column| {
            let ca = s.str()?;
            Ok(Some(
                ca.pad_start(length as usize, fill_char.chars().nth(0).unwrap())
                    .into_column(),
            ))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::String))
            .with_fmt("str.pad_start")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_pad_end(&self, length: i64, fill_char: String) -> JsExpr {
        let function = move |s: Column| {
            let ca = s.str()?;
            Ok(Some(
                ca.pad_end(length as usize, fill_char.chars().nth(0).unwrap())
                    .into_column(),
            ))
        };

        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::String))
            .with_fmt("str.pad_end")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn zfill(&self, length: &JsExpr) -> Self {
        self.inner.clone().str().zfill(length.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn str_to_uppercase(&self) -> JsExpr {
        let function = |s: Column| {
            let ca = s.str()?;
            Ok(Some(ca.to_uppercase().into_column()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.to_uppercase")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_slice(&self, offset: &JsExpr, length: &JsExpr) -> JsExpr {
        self.inner
            .clone()
            .str()
            .slice(offset.inner.clone(), length.inner.clone())
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_starts_with(&self, sub: &JsExpr) -> JsExpr {
        self.inner
            .clone()
            .str()
            .starts_with(sub.inner.clone())
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_ends_with(&self, sub: &JsExpr) -> JsExpr {
        self.inner.clone().str().ends_with(sub.inner.clone()).into()
    }

    #[napi(catch_unwind)]
    pub fn str_to_lowercase(&self) -> JsExpr {
        let function = |s: Column| {
            let ca = s.str()?;
            Ok(Some(ca.to_lowercase().into_column()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.to_lowercase")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_lengths(&self) -> JsExpr {
        let function = |s: Column| {
            let ca = s.str()?;
            Ok(Some(ca.str_len_chars().into_column()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt32))
            .with_fmt("str.len")
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_replace(&self, pat: &JsExpr, val: &JsExpr, literal: bool, n: i64) -> JsExpr {
        match literal {
            true => self
                .inner
                .clone()
                .str()
                .replace_n(pat.inner.clone(), val.inner.clone(), literal, n)
                .into(),
            _ => self
                .inner
                .clone()
                .str()
                .replace(pat.inner.clone(), val.inner.clone(), false)
                .into(),
        }
    }

    #[napi(catch_unwind)]
    pub fn str_replace_all(&self, pat: &JsExpr, val: &JsExpr, literal: bool) -> JsExpr {
        self.inner
            .clone()
            .str()
            .replace_all(pat.inner.clone(), val.inner.clone(), literal)
            .into()
    }

    #[napi(catch_unwind)]
    pub fn str_contains(&self, pat: &JsExpr, literal: bool, strict: bool) -> JsExpr {
        match literal {
            true => self
                .inner
                .clone()
                .str()
                .contains_literal(pat.inner.clone())
                .into(),
            _ => self
                .inner
                .clone()
                .str()
                .contains(pat.inner.clone(), strict)
                .into(),
        }
    }
    #[napi(catch_unwind)]
    pub fn str_hex_encode(&self) -> JsExpr {
        self.clone()
            .inner
            .map(
                move |s| s.str().map(|s| Some(s.hex_encode().into_column())),
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
                move |s| s.str()?.hex_decode(strict).map(|s| Some(s.into_column())),
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
                move |s| s.str().map(|s| Some(s.base64_encode().into_column())),
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
                    s.str()?
                        .base64_decode(strict)
                        .map(|s| Some(s.into_column()))
                },
                GetOutput::same_type(),
            )
            .with_fmt("str.base64_decode")
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_json_decode(
        &self,
        dtype: Option<Wrap<DataType>>,
        infer_schema_len: Option<i64>,
    ) -> JsExpr {
        let dt = dtype.clone().map(|d| d.0 as DataType);
        let infer_schema_len = infer_schema_len.map(|l| l as usize);
        self.inner
            .clone()
            .str()
            .json_decode(dt, infer_schema_len)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn str_json_path_match(&self, pat: Wrap<ChunkedArray<StringType>>) -> JsExpr {
        let function = move |s: Column| {
            let ca = s.str()?;
            match ca.json_path_match(&pat.0) {
                Ok(ca) => Ok(Some(ca.into_column())),
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
    pub fn str_extract(&self, pat: &JsExpr, group_index: i64) -> JsExpr {
        self.inner
            .clone()
            .str()
            .extract(pat.inner.clone(), group_index as usize)
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
        self.inner
            .clone()
            .str()
            .split_exact(by.0, n as usize)
            .into()
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
    pub fn replace(&self, old: &JsExpr, new: &JsExpr) -> JsExpr {
        self.inner
            .clone()
            .replace(old.inner.clone(), new.inner.clone())
            .into()
    }
    #[napi(catch_unwind)]
    pub fn replace_strict(
        &self,
        old: &JsExpr,
        new: &JsExpr,
        default: Option<&JsExpr>,
        return_dtype: Option<Wrap<DataType>>,
    ) -> JsExpr {
        self.inner
            .clone()
            .replace_strict(
                old.inner.clone(),
                new.inner.clone(),
                default.map(|e| e.inner.clone()),
                return_dtype.map(|dt| dt.0),
            )
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
                |s| Ok(Some(s.duration()?.days().into_column())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_hours(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.hours().into_column())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_seconds(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.seconds().into_column())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_nanoseconds(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.nanoseconds().into_column())),
                GetOutput::from_type(DataType::Int64),
            )
            .into()
    }
    #[napi(catch_unwind)]
    pub fn duration_milliseconds(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.duration()?.milliseconds().into_column())),
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
                |s: Column| {
                    s.take_materialized_series()
                        .timestamp(TimeUnit::Milliseconds)
                        .map(|ca| Some((ca / 1000).into_column()))
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
        let function = move |s: Column| {
            let seed = PlFixedStateQuality::default().hash_one((k0.0, k1.0, k2.0, k3.0));
            let hb = PlSeedableRandomStateQuality::seed_from_u64(seed);
            Ok(Some(s.as_materialized_series().hash(hb).into_column()))
        };
        self.clone()
            .inner
            .map(function, GetOutput::from_type(DataType::UInt64))
            .into()
    }

    #[napi(catch_unwind)]
    pub fn reinterpret(&self, signed: bool) -> JsExpr {
        let function = move |s: Column| reinterpret(&s, signed).map(Some);
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
        self.inner.clone().name().keep().into()
    }
    #[napi(catch_unwind)]
    pub fn prefix(&self, prefix: String) -> JsExpr {
        self.inner.clone().name().prefix(&prefix).into()
    }
    #[napi(catch_unwind)]
    pub fn suffix(&self, suffix: String) -> JsExpr {
        self.inner.clone().name().suffix(&suffix).into()
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
        method: Wrap<QuantileMethod>,
        window_size: i16,
        weights: Option<Vec<f64>>,
        min_periods: i64,
        center: bool,
    ) -> JsExpr {
        let options = RollingOptionsFixedWindow {
            window_size: window_size as usize,
            min_periods: min_periods as usize,
            weights,
            center,
            fn_params: Some(RollingFnParams::Quantile(RollingQuantileParams {
                prob: quantile,
                method: method.0,
            })),
        };
        self.inner
            .clone()
            .rolling_quantile(method.0, quantile, options)
            .into()
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
    pub fn list_get(&self, index: &JsExpr, null_on_oob: bool) -> JsExpr {
        self.inner
            .clone()
            .list()
            .get(index.inner.clone(), null_on_oob)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn list_join(&self, separator: &JsExpr, ignore_nulls: bool) -> JsExpr {
        self.inner
            .clone()
            .list()
            .join(separator.inner.clone(), ignore_nulls)
            .into()
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
    pub fn list_shift(&self, periods: &JsExpr) -> JsExpr {
        self.inner
            .clone()
            .list()
            .shift(periods.inner.clone())
            .into()
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
    pub fn list_contains(&self, expr: &JsExpr) -> JsExpr {
        self.inner
            .clone()
            .list()
            .contains(expr.inner.clone())
            .into()
    }
    #[napi(catch_unwind)]
    pub fn rank(
        &self,
        method: Wrap<RankMethod>,
        descending: bool,
        seed: Option<Wrap<u64>>,
    ) -> JsExpr {
        // Safety:
        // Wrap is transparent.
        let seed: Option<u64> = unsafe { std::mem::transmute(seed) };
        let options = RankOptions {
            method: method.0,
            descending,
        };
        self.inner.clone().rank(options, seed).into()
    }
    #[napi(catch_unwind)]
    pub fn diff(&self, n: Wrap<Expr>, null_behavior: Wrap<NullBehavior>) -> JsExpr {
        self.inner.clone().diff(n.0, null_behavior.0).into()
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
    pub fn str_concat(&self, separator: String, ignore_nulls: bool) -> JsExpr {
        self.inner
            .clone()
            .str()
            .join(&separator, ignore_nulls)
            .into()
    }
    #[napi(catch_unwind)]
    pub fn cat_set_ordering(&self, ordering: String) -> JsExpr {
        let ordering = match ordering.as_ref() {
            "physical" => CategoricalOrdering::Physical,
            "lexical" => CategoricalOrdering::Lexical,
            _ => panic!("expected one of {{'physical', 'lexical'}}"),
        };

        self.inner
            .clone()
            .cast(DataType::Categorical(None, ordering))
            .into()
    }
    #[napi(catch_unwind)]
    pub fn reshape(&self, dims: Vec<i64>) -> JsExpr {
        self.inner.clone().reshape(&dims).into()
    }
    #[napi(catch_unwind)]
    pub fn cum_count(&self, reverse: bool) -> JsExpr {
        self.inner.clone().cum_count(reverse).into()
    }
    #[napi(catch_unwind)]
    pub fn to_physical(&self) -> JsExpr {
        self.inner
            .clone()
            .map(
                |s| Ok(Some(s.to_physical_repr().to_owned())),
                GetOutput::map_dtype(|dt| Ok(dt.to_physical())),
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
    pub fn struct_field_by_index(&self, index: i64) -> JsExpr {
        self.inner.clone().struct_().field_by_index(index).into()
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
    pub fn struct_with_fields(&self, fields: Vec<&JsExpr>) -> JsExpr {
        self.inner
            .clone()
            .struct_()
            .with_fields(fields.to_exprs())
            .map_err(JsPolarsErr::from)
            .unwrap()
            .into()
    }
    #[napi(catch_unwind)]
    pub fn log(&self, base: f64) -> JsExpr {
        self.inner.clone().log(base).into()
    }
    #[napi(catch_unwind)]
    pub fn log1p(&self) -> JsExpr {
        self.inner.clone().log1p().into()
    }
    #[napi(catch_unwind)]
    pub fn exp(&self) -> JsExpr {
        self.inner.clone().exp().into()
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
pub fn first() -> JsExpr {
    dsl::first().into()
}

#[napi(catch_unwind)]
pub fn last() -> JsExpr {
    dsl::last().into()
}

#[napi(catch_unwind)]
pub fn nth(n: i64) -> JsExpr {
    Expr::Nth(n).into()
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
pub fn int_range(start: Wrap<Expr>, end: Wrap<Expr>, step: i64, dtype: Wrap<DataType>) -> JsExpr {
    dsl::int_range(start.0, end.0, step, dtype.0).into()
}

#[napi(catch_unwind)]
pub fn int_ranges(
    start: Wrap<Expr>,
    end: Wrap<Expr>,
    step: Wrap<Expr>,
    dtype: Option<Wrap<DataType>>,
) -> JsExpr {
    let dtype = dtype.map(|d| d.0 as DataType);

    let mut result = dsl::int_ranges(start.0, end.0, step.0);

    if dtype.is_some() && dtype.clone().unwrap() != DataType::Int64 {
        result = result.cast(DataType::List(Box::new(dtype.clone().unwrap())));
    }

    result.into()
}

#[napi(catch_unwind)]
pub fn pearson_corr(a: Wrap<Expr>, b: Wrap<Expr>) -> JsExpr {
    polars::lazy::dsl::pearson_corr(a.0, b.0).into()
}

#[napi(catch_unwind)]
pub fn spearman_rank_corr(a: Wrap<Expr>, b: Wrap<Expr>, propagate_nans: bool) -> JsExpr {
    polars::lazy::dsl::spearman_rank_corr(a.0, b.0, propagate_nans).into()
}

#[napi(catch_unwind)]
pub fn len() -> JsExpr {
    polars::lazy::dsl::len().into()
}

#[napi(catch_unwind)]
pub fn cov(a: Wrap<Expr>, b: Wrap<Expr>, ddof: u8) -> JsExpr {
    polars::lazy::dsl::cov(a.0, b.0, ddof).into()
}

#[napi(catch_unwind)]
#[cfg(feature = "range")]
pub fn arg_sort_by(by: Vec<&JsExpr>, descending: Vec<bool>) -> JsExpr {
    let by = by.to_exprs();
    polars::lazy::dsl::arg_sort_by(
        by,
        SortMultipleOptions::default().with_order_descending_multi(descending),
    )
    .into()
}

#[napi(catch_unwind)]
pub fn lit(value: Wrap<AnyValue>) -> JsResult<JsExpr> {
    let lit: LiteralValue = value
        .0
        .try_into()
        .map_err(|e| napi::Error::from_reason(format!("{e:?}")))?;
    Ok(dsl::lit(lit).into())
}

#[napi(catch_unwind)]
pub fn range(low: Wrap<Expr>, high: Wrap<Expr>, dtype: Wrap<DataType>) -> JsExpr {
    int_range(low, high, 1, dtype).into()
}

#[napi(catch_unwind)]
pub fn concat_lst(s: Vec<&JsExpr>) -> JsResult<JsExpr> {
    let s = s.to_exprs();
    let expr = polars::lazy::dsl::concat_list(s).map_err(JsPolarsErr::from)?;
    Ok(expr.into())
}

#[napi(catch_unwind)]
pub fn concat_str(s: Vec<&JsExpr>, separator: String, ignore_nulls: bool) -> JsExpr {
    let s = s.into_iter().map(|e| e.inner.clone()).collect::<Vec<_>>();
    dsl::concat_str(s, &separator, ignore_nulls).into()
}

#[napi(catch_unwind)]
pub fn as_struct(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    polars::lazy::dsl::as_struct(exprs).into()
}

#[napi(catch_unwind)]
pub fn all_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::all_horizontal(exprs)
        .map_err(JsPolarsErr::from)
        .unwrap()
        .into()
}

#[napi(catch_unwind)]
pub fn any_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::any_horizontal(exprs)
        .map_err(JsPolarsErr::from)
        .unwrap()
        .into()
}
#[napi(catch_unwind)]
pub fn min_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::min_horizontal(exprs)
        .map_err(JsPolarsErr::from)
        .unwrap()
        .into()
}

#[napi(catch_unwind)]
pub fn max_horizontal(exprs: Vec<&JsExpr>) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::max_horizontal(exprs)
        .map_err(JsPolarsErr::from)
        .unwrap()
        .into()
}
#[napi(catch_unwind)]
pub fn sum_horizontal(exprs: Vec<&JsExpr>, ignore_nulls: bool) -> JsExpr {
    let exprs = exprs.to_exprs();
    dsl::sum_horizontal(exprs, ignore_nulls)
        .map_err(JsPolarsErr::from)
        .unwrap()
        .into()
}
