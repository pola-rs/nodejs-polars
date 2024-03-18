use crate::{export::JsLazyFrame, prelude::*};
use polars::sql::SQLContext;

#[napi(js_name = "SqlContext")]
#[repr(transparent)]
#[derive(Clone)]
pub struct JsSQLContext {
    context: SQLContext,
}

#[napi]
impl JsSQLContext {
    #[napi(constructor)]
    #[allow(clippy::new_without_default)]
    pub fn new() -> JsSQLContext {
        JsSQLContext {
            context: SQLContext::new(),
        }
    }

    #[napi(catch_unwind)]
    pub fn execute(&mut self, query: String) -> JsResult<JsLazyFrame> {
        Ok(self
            .context
            .execute(&query)
            .map_err(JsPolarsErr::from)?
            .into())
    }

    #[napi]
    pub fn get_tables(&self) -> Vec<String> {
        self.context.get_tables()
    }

    #[napi]
    pub fn register(&mut self, name: String, lf: &JsLazyFrame) {
        self.context.register(&name, lf.clone().ldf)
    }

    #[napi]
    pub fn unregister(&mut self, name: String) {
        self.context.unregister(&name)
    }
}
