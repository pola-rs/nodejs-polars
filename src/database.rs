use connectorx::prelude::*;
use std::convert::TryFrom;

#[napi]
pub fn read_postgres(query: &str, connection_string: &str) -> Result<JsDataFrame> {
    let mut source_conn =
        SourceConn::try_from(format!("{}?cxprotocol=binary", connection_string))
            .expect("parse conn str failed");
    let queries = &[
        CXQuery::from("SELECT id FROM entity.review_group_questions WHERE company_id = 3445"),
    ];
    let destination = get_arrow(&source_conn, None, queries).expect("run failed");

    let data = destination.polars();
    return JsDataFrame::from(data);
}
