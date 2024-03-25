use polars_core::prelude::DataFrame;
use polars_io::{parquet::ParquetReader, SerReader};
use polars_lazy::{dsl::UnionArgs, frame::{IntoLazy, LazyFrame}};
use polars::prelude::concat;
use deltalake::{DeltaTableBuilder, DeltaTableError};
use crate::dataframe::ReadDeltaOptions;

pub async fn read_delta_table(path: &str, 
    options: ReadDeltaOptions,
    ) -> Result<LazyFrame, DeltaTableError> {
    let mut db = DeltaTableBuilder::from_uri(path)
                .with_allow_http(false);

    // if version specified, add it
    if options.version.is_some() {
        db = db.with_version(options.version.unwrap());
    }

    let dt = db.load().await?;

    // show all active files in the table
    let files: Vec<_> = dt.get_file_uris()?.collect();

    let mut df_collection: Vec<DataFrame> = vec![];

    for file in files.into_iter() {
        let base = std::path::Path::new(path);
        let file_path = std::path::Path::new(&file);
        let full_path = base.join(file_path);
        let mut file = std::fs::File::open(full_path).unwrap();

        let columns = options.columns.clone();
        let parallel = options.parallel.0;

        let df = ParquetReader::new(&mut file)
            .with_columns(columns)
            .read_parallel(parallel)
            .finish().unwrap();

        df_collection.push(df);
    }

    let empty_head = df_collection[0].clone().lazy().limit(0);

    Ok(df_collection.into_iter().fold(empty_head, |acc, df| concat([acc, df.lazy()], 
            UnionArgs {
                rechunk: false,
                parallel: false,
                ..Default::default()
            }).unwrap()))

}