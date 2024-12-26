use crate::file::*;
use crate::prelude::*;
use crate::series::JsSeries;
use napi::JsUnknown;
use polars::frame::row::{infer_schema, Row};
use polars_core::utils::arrow::array::{ PrimitiveArray, Utf8ViewArray, NullArray, BooleanArray };
use polars_core::utils::arrow::array::Array as ArrowArray;
use polars_io::csv::write::CsvWriterOptions;
use polars_io::mmap::MmapBytesReader;
use polars_io::RowIndex;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor};
use std::num::NonZeroUsize;

#[napi]
#[repr(transparent)]
#[derive(Clone)]
pub struct JsDataFrame {
    pub(crate) df: DataFrame,
}

impl JsDataFrame {
    pub(crate) fn new(df: DataFrame) -> JsDataFrame {
        JsDataFrame { df }
    }
}
impl From<DataFrame> for JsDataFrame {
    fn from(s: DataFrame) -> JsDataFrame {
        JsDataFrame::new(s)
    }
}

pub(crate) fn to_series_collection(ps: Array) -> Vec<Column> {
    let len = ps.len();
    (0..len)
        .map(|idx| {
            let item: &JsSeries = ps.get(idx).unwrap().unwrap();
            item.series.clone().into()
        })
        .collect()
}
pub(crate) fn to_jsseries_collection(s: Vec<Series>) -> Vec<JsSeries> {
    let mut s = std::mem::ManuallyDrop::new(s);

    let p = s.as_mut_ptr() as *mut JsSeries;
    let len = s.len();
    let cap = s.capacity();

    unsafe { Vec::from_raw_parts(p, len, cap) }
}

#[napi(object)]
pub struct ReadCsvOptions {
    /// Stop reading from the csv after this number of rows is reached
    pub n_rows: Option<u32>,
    // used by error ignore logic
    pub infer_schema_length: Option<u32>,
    pub skip_rows: u32,
    /// Optional indexes of the columns to project
    pub projection: Option<Vec<u32>>,
    /// Optional column names to project/ select.
    pub columns: Option<Vec<String>>,
    pub sep: Option<String>,
    pub schema: Option<Wrap<Schema>>,
    pub encoding: String,
    pub num_threads: Option<u32>,
    pub path: Option<String>,
    pub dtypes: Option<HashMap<String, Wrap<DataType>>>,
    pub chunk_size: u32,
    pub comment_char: Option<String>,
    pub null_values: Option<Wrap<NullValues>>,
    pub quote_char: Option<String>,
    pub skip_rows_after_header: u32,
    pub try_parse_dates: bool,
    pub row_count: Option<JsRowCount>,

    /// Aggregates chunk afterwards to a single chunk.
    pub rechunk: bool,
    pub raise_if_empty: bool,
    pub truncate_ragged_lines: bool,
    pub missing_is_null: bool,
    pub low_memory: bool,
    pub has_header: bool,
    pub ignore_errors: bool,
    pub eol_char: String,
}

fn mmap_reader_to_df<'a>(
    csv: impl MmapBytesReader + 'a,
    options: ReadCsvOptions,
) -> napi::Result<JsDataFrame> {
    let null_values = options.null_values.map(|w| w.0);
    let row_count = options.row_count.map(RowIndex::from);
    let projection = options
        .projection
        .map(|p: Vec<u32>| p.into_iter().map(|p| p as usize).collect());

    let quote_char = options.quote_char.map_or(None, |q| {
        if q.is_empty() {
            None
        } else {
            Some(q.as_bytes()[0])
        }
    });

    let encoding = match options.encoding.as_ref() {
        "utf8" => CsvEncoding::Utf8,
        "utf8-lossy" => CsvEncoding::LossyUtf8,
        e => return Err(JsPolarsErr::Other(format!("encoding not {} not implemented.", e)).into()),
    };

    let overwrite_dtype = options.dtypes.map(|overwrite_dtype| {
        overwrite_dtype
            .iter()
            .map(|(name, dtype)| {
                let dtype = dtype.0.clone();
                Field::new((&**name).into(), dtype)
            })
            .collect::<Schema>()
    });

    let df = CsvReadOptions::default()
        .with_infer_schema_length(Some(options.infer_schema_length.unwrap_or(100) as usize))
        .with_projection(projection.map(Arc::new))
        .with_has_header(options.has_header)
        .with_n_rows(options.n_rows.map(|i| i as usize))
        .with_skip_rows(options.skip_rows as usize)
        .with_ignore_errors(options.ignore_errors)
        .with_rechunk(options.rechunk)
        .with_chunk_size(options.chunk_size as usize)
        .with_columns(
            options
                .columns
                .map(|x| x.into_iter().map(PlSmallStr::from_string).collect()),
        )
        .with_n_threads(options.num_threads.map(|i| i as usize))
        .with_schema_overwrite(overwrite_dtype.map(Arc::new))
        .with_schema(options.schema.map(|schema| Arc::new(schema.0)))
        .with_low_memory(options.low_memory)
        .with_row_index(row_count)
        .with_skip_rows_after_header(options.skip_rows_after_header as usize)
        .with_raise_if_empty(options.raise_if_empty)
        .with_parse_options(
            CsvParseOptions::default()
                .with_separator(options.sep.unwrap_or(",".to_owned()).as_bytes()[0])
                .with_encoding(encoding)
                .with_missing_is_null(options.missing_is_null)
                .with_comment_prefix(options.comment_char.as_deref())
                .with_null_values(null_values)
                .with_try_parse_dates(options.try_parse_dates)
                .with_quote_char(quote_char)
                .with_eol_char(options.eol_char.as_bytes()[0])
                .with_truncate_ragged_lines(options.truncate_ragged_lines),
        )
        .into_reader_with_file_handle(csv)
        .finish()
        .map_err(JsPolarsErr::from)?;

    Ok(df.into())
}

#[napi(catch_unwind)]
pub fn read_csv(
    path_or_buffer: Either<String, Buffer>,
    options: ReadCsvOptions,
) -> napi::Result<JsDataFrame> {
    match path_or_buffer {
        Either::A(path) => mmap_reader_to_df(std::fs::File::open(path)?, options),
        Either::B(buffer) => mmap_reader_to_df(Cursor::new(buffer.as_ref()), options),
    }
}

#[napi(object)]
pub struct ReadJsonOptions {
    pub infer_schema_length: Option<u32>,
    pub batch_size: Option<u32>,
    pub format: Option<String>,
}

#[napi(object)]
pub struct WriteJsonOptions {
    pub format: String,
}

#[napi(catch_unwind)]
pub fn read_json_lines(
    path_or_buffer: Either<String, Buffer>,
    options: ReadJsonOptions,
) -> napi::Result<JsDataFrame> {
    let infer_schema_length =
        NonZeroUsize::new(options.infer_schema_length.unwrap_or(100) as usize);
    let batch_size = options
        .batch_size
        .map(|b| NonZeroUsize::try_from(b as usize).unwrap());

    let df = match path_or_buffer {
        Either::A(path) => JsonLineReader::from_path(path)
            .expect("unable to read file")
            .infer_schema_len(infer_schema_length)
            .with_chunk_size(batch_size)
            .finish()
            .map_err(JsPolarsErr::from)?,
        Either::B(buf) => {
            let cursor = Cursor::new(buf.as_ref());
            JsonLineReader::new(cursor)
                .infer_schema_len(infer_schema_length)
                .with_chunk_size(batch_size)
                .finish()
                .map_err(JsPolarsErr::from)?
        }
    };
    Ok(df.into())
}
#[napi(catch_unwind)]
pub fn read_json(
    path_or_buffer: Either<String, Buffer>,
    options: ReadJsonOptions,
) -> napi::Result<JsDataFrame> {
    let infer_schema_length =
        NonZeroUsize::new(options.infer_schema_length.unwrap_or(100) as usize);
    let batch_size = options.batch_size.unwrap_or(10000) as usize;
    let batch_size = NonZeroUsize::new(batch_size).unwrap();
    let format: JsonFormat = options
        .format
        .map(|s| match s.as_ref() {
            "lines" => Ok(JsonFormat::JsonLines),
            "json" => Ok(JsonFormat::Json),
            _ => Err(napi::Error::from_reason(
                "format must be 'json' or `lines'".to_owned(),
            )),
        })
        .unwrap()?;
    let df = match path_or_buffer {
        Either::A(path) => {
            let f = File::open(&path)?;
            let reader = BufReader::new(f);
            JsonReader::new(reader)
                .infer_schema_len(infer_schema_length)
                .with_batch_size(batch_size)
                .with_json_format(format)
                .finish()
                .map_err(JsPolarsErr::from)?
        }
        Either::B(buf) => {
            let cursor = Cursor::new(buf.as_ref());
            JsonReader::new(cursor)
                .infer_schema_len(infer_schema_length)
                .with_batch_size(batch_size)
                .with_json_format(format)
                .finish()
                .map_err(JsPolarsErr::from)?
        }
    };
    Ok(df.into())
}

#[napi(object)]
pub struct ReadParquetOptions {
    pub columns: Option<Vec<String>>,
    pub projection: Option<Vec<i64>>,
    pub n_rows: Option<i64>,
    pub row_count: Option<JsRowCount>,
}

#[napi(catch_unwind)]
pub fn read_parquet(
    path_or_buffer: Either<String, Buffer>,
    options: ReadParquetOptions,
    parallel: Wrap<ParallelStrategy>,
) -> napi::Result<JsDataFrame> {
    let columns = options.columns;

    let projection = options
        .projection
        .map(|projection| projection.into_iter().map(|p| p as usize).collect());
    let row_count = options.row_count.map(|rc| rc.into());
    let n_rows = options.n_rows.map(|nr| nr as usize);

    let result = match path_or_buffer {
        Either::A(path) => {
            let f = File::open(&path)?;
            let reader = BufReader::new(f);
            ParquetReader::new(reader)
                .with_projection(projection)
                .with_columns(columns)
                .read_parallel(parallel.0)
                .with_slice(n_rows.map(|x| (0, x)))
                .with_row_index(row_count)
                .finish()
        }
        Either::B(buf) => {
            let cursor = Cursor::new(buf.as_ref());
            ParquetReader::new(cursor)
                .with_projection(projection)
                .with_columns(columns)
                .read_parallel(parallel.0)
                .with_slice(n_rows.map(|x| (0, x)))
                .with_row_index(row_count)
                .finish()
        }
    };
    let df = result.map_err(JsPolarsErr::from)?;
    Ok(JsDataFrame::new(df))
}

#[napi(object)]
pub struct ReadIpcOptions {
    pub columns: Option<Vec<String>>,
    pub projection: Option<Vec<i64>>,
    pub n_rows: Option<i64>,
    pub row_count: Option<JsRowCount>,
}

#[napi(catch_unwind)]
pub fn read_ipc(
    path_or_buffer: Either<String, Buffer>,
    options: ReadIpcOptions,
) -> napi::Result<JsDataFrame> {
    let columns = options.columns;
    let projection = options
        .projection
        .map(|projection| projection.into_iter().map(|p| p as usize).collect());
    let row_count = options.row_count.map(|rc| rc.into());
    let n_rows = options.n_rows.map(|nr| nr as usize);

    let result = match path_or_buffer {
        Either::A(path) => {
            let f = File::open(&path)?;
            let reader = BufReader::new(f);
            IpcReader::new(reader)
                .with_projection(projection)
                .with_columns(columns)
                .with_n_rows(n_rows)
                .with_row_index(row_count)
                .finish()
        }
        Either::B(buf) => {
            let cursor = Cursor::new(buf.as_ref());
            IpcReader::new(cursor)
                .with_projection(projection)
                .with_columns(columns)
                .with_n_rows(n_rows)
                .with_row_index(row_count)
                .finish()
        }
    };
    let df = result.map_err(JsPolarsErr::from)?;
    Ok(JsDataFrame::new(df))
}

#[napi(catch_unwind)]
pub fn read_ipc_stream(
    path_or_buffer: Either<String, Buffer>,
    options: ReadIpcOptions,
) -> napi::Result<JsDataFrame> {
    let columns = options.columns;
    let projection = options
        .projection
        .map(|projection| projection.into_iter().map(|p| p as usize).collect());
    let row_count = options.row_count.map(|rc| rc.into());
    let n_rows = options.n_rows.map(|nr| nr as usize);

    let result = match path_or_buffer {
        Either::A(path) => {
            let f = File::open(&path)?;
            let reader = BufReader::new(f);
            IpcStreamReader::new(reader)
                .with_projection(projection)
                .with_columns(columns)
                .with_n_rows(n_rows)
                .with_row_index(row_count)
                .finish()
        }
        Either::B(buf) => {
            let cursor = Cursor::new(buf.as_ref());
            IpcStreamReader::new(cursor)
                .with_projection(projection)
                .with_columns(columns)
                .with_n_rows(n_rows)
                .with_row_index(row_count)
                .finish()
        }
    };
    let df = result.map_err(JsPolarsErr::from)?;
    Ok(JsDataFrame::new(df))
}

#[napi(object)]
pub struct ReadAvroOptions {
    pub columns: Option<Vec<String>>,
    pub projection: Option<Vec<i64>>,
    pub n_rows: Option<i64>,
}

#[napi(catch_unwind)]
pub fn read_avro(
    path_or_buffer: Either<String, Buffer>,
    options: ReadAvroOptions,
) -> napi::Result<JsDataFrame> {
    use polars::io::avro::AvroReader;
    let columns = options.columns;
    let projection = options
        .projection
        .map(|projection| projection.into_iter().map(|p| p as usize).collect());
    let n_rows = options.n_rows.map(|nr| nr as usize);

    let result = match path_or_buffer {
        Either::A(path) => {
            let f = File::open(&path)?;
            let reader = BufReader::new(f);
            AvroReader::new(reader)
                .with_projection(projection)
                .with_columns(columns)
                .with_n_rows(n_rows)
                .finish()
        }
        Either::B(buf) => {
            let cursor = Cursor::new(buf.as_ref());
            AvroReader::new(cursor)
                .with_projection(projection)
                .with_columns(columns)
                .with_n_rows(n_rows)
                .finish()
        }
    };
    let df = result.map_err(JsPolarsErr::from)?;
    Ok(JsDataFrame::new(df))
}

#[napi(catch_unwind)]
pub fn from_rows(
    rows: Array,
    schema: Option<Wrap<Schema>>,
    infer_schema_length: Option<u32>,
    env: Env,
) -> napi::Result<JsDataFrame> {
    let schema = match schema {
        Some(s) => s.0,
        None => {
            let infer_schema_length = infer_schema_length.unwrap_or(100) as usize;
            let pairs = obj_to_pairs(&rows, infer_schema_length);
            infer_schema(pairs, infer_schema_length)
        }
    };
    let len = rows.len();
    let it: Vec<Row> = (0..len)
        .into_iter()
        .map(|idx| {
            let obj = rows
                .get::<Object>(idx as u32)
                .unwrap_or(None)
                .unwrap_or_else(|| env.create_object().unwrap());
            Row(schema
                .iter_fields()
                .map(|fld| {
                    let dtype = fld.dtype();
                    let key = fld.name();
                    if let Ok(unknown) = obj.get(key) {
                        let _av = match unknown {
                            Some(unknown) => unsafe {
                                coerce_js_anyvalue(unknown, &dtype).unwrap_or(AnyValue::Null)
                            },
                            None => AnyValue::Null,
                        };
                        // todo: return av instead of null
                        // av
                        AnyValue::Null
                    } else {
                        AnyValue::Null
                    }
                })
                .collect())
        })
        .collect();
    let df = DataFrame::from_rows_and_schema(&it, &schema).map_err(JsPolarsErr::from)?;
    Ok(df.into())
}

#[napi]
impl JsDataFrame {
    #[napi(catch_unwind)]
    pub fn to_js(&self, env: Env) -> napi::Result<napi::JsUnknown> {
        env.to_js_value(&self.df)
    }

    #[napi(catch_unwind)]
    pub fn serialize(&self, format: String) -> napi::Result<Buffer> {
        let buf = match format.as_ref() {
            "bincode" => bincode::serialize(&self.df)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            "json" => serde_json::to_vec(&self.df)
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
    pub fn deserialize(buf: Buffer, format: String) -> napi::Result<JsDataFrame> {
        let df: DataFrame = match format.as_ref() {
            "bincode" => bincode::deserialize(&buf)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            "json" => serde_json::from_slice(&buf)
                .map_err(|err| napi::Error::from_reason(format!("{:?}", err)))?,
            _ => {
                return Err(napi::Error::from_reason(
                    "unexpected format. \n supported options are 'json', 'bincode'".to_owned(),
                ))
            }
        };
        Ok(df.into())
    }
    #[napi(constructor)]
    pub fn from_columns(columns: Array) -> napi::Result<JsDataFrame> {
        let len = columns.len();
        let cols: Vec<Column> = (0..len)
            .map(|idx| {
                let item: &JsSeries = columns.get(idx).unwrap().unwrap();
                item.series.clone().into()
            })
            .collect();

        let df = DataFrame::new(cols).map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }

    #[napi(catch_unwind)]
    pub fn estimated_size(&self) -> u32 {
        self.df.estimated_size() as u32
    }

    #[napi(catch_unwind)]
    pub fn to_string(&self) -> String {
        format!("{:?}", self.df)
    }

    #[napi(catch_unwind)]
    pub fn add(&self, s: &JsSeries) -> napi::Result<JsDataFrame> {
        let df = (&self.df + &s.series).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn sub(&self, s: &JsSeries) -> napi::Result<JsDataFrame> {
        let df = (&self.df - &s.series).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn div(&self, s: &JsSeries) -> napi::Result<JsDataFrame> {
        let df = (&self.df / &s.series).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn mul(&self, s: &JsSeries) -> napi::Result<JsDataFrame> {
        let df = (&self.df * &s.series).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn rem(&self, s: &JsSeries) -> napi::Result<JsDataFrame> {
        let df = (&self.df % &s.series).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn add_df(&self, s: &JsDataFrame) -> napi::Result<JsDataFrame> {
        let df = (&self.df + &s.df).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn sub_df(&self, s: &JsDataFrame) -> napi::Result<JsDataFrame> {
        let df = (&self.df - &s.df).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn div_df(&self, s: &JsDataFrame) -> napi::Result<JsDataFrame> {
        let df = (&self.df / &s.df).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn mul_df(&self, s: &JsDataFrame) -> napi::Result<JsDataFrame> {
        let df = (&self.df * &s.df).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn rem_df(&self, s: &JsDataFrame) -> napi::Result<JsDataFrame> {
        let df = (&self.df % &s.df).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn rechunk(&self) -> JsDataFrame {
        let mut df = self.df.clone();
        df.as_single_chunk_par();
        df.into()
    }
    #[napi(catch_unwind)]
    pub fn fill_null(&self, strategy: Wrap<FillNullStrategy>) -> napi::Result<JsDataFrame> {
        let df = self.df.fill_null(strategy.0).map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }
    #[napi(catch_unwind)]
    pub fn join(
        &self,
        other: &JsDataFrame,
        left_on: Vec<&str>,
        right_on: Vec<&str>,
        how: String,
        suffix: Option<String>,
    ) -> napi::Result<JsDataFrame> {
        let how = match how.as_ref() {
            "left" => JoinType::Left,
            "inner" => JoinType::Inner,
            "full" => JoinType::Full,
            "semi" => JoinType::Semi,
            "anti" => JoinType::Anti,
            "asof" => JoinType::AsOf(AsOfOptions {
                strategy: AsofStrategy::Backward,
                left_by: None,
                right_by: None,
                tolerance: None,
                tolerance_str: None,
            }),
            "cross" => JoinType::Cross,
            _ => panic!("not supported"),
        };

        let df = self
            .df
            .join(
                &other.df,
                left_on,
                right_on,
                JoinArgs {
                    how: how,
                    suffix: suffix.map_or(None, |s| Some(PlSmallStr::from_string(s))),
                    ..Default::default()
                },
            )
            .map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }

    #[napi(catch_unwind)]
    pub fn get_columns(&self) -> Vec<JsSeries> {
        let cols: Vec<Series> = self
            .df
            .get_columns()
            .iter()
            .map(Column::as_materialized_series)
            .cloned()
            .collect();
        to_jsseries_collection(cols.to_vec())
    }

    /// Get column names
    #[napi(getter, catch_unwind)]
    pub fn columns(&self) -> Vec<&str> {
        self.df.get_column_names_str()
    }

    #[napi(setter, js_name = "columns", catch_unwind)]
    pub fn set_columns(&mut self, names: Vec<&str>) -> napi::Result<()> {
        self.df.set_column_names(names).map_err(JsPolarsErr::from)?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub fn with_column(&mut self, s: &JsSeries) -> napi::Result<JsDataFrame> {
        let mut df = self.df.clone();
        df.with_column(s.series.clone())
            .map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    /// Get datatypes
    #[napi(catch_unwind)]
    pub fn dtypes(&self) -> Vec<Wrap<DataType>> {
        self.df.iter().map(|s| Wrap(s.dtype().clone())).collect()
    }
    #[napi(catch_unwind)]
    pub fn n_chunks(&self) -> napi::Result<u32> {
        let n = self.df.first_col_n_chunks();
        Ok(n as u32)
    }

    #[napi(getter, catch_unwind)]
    pub fn shape(&self) -> Shape {
        self.df.shape().into()
    }
    #[napi(getter, catch_unwind)]
    pub fn height(&self) -> i64 {
        self.df.height() as i64
    }
    #[napi(getter, catch_unwind)]
    pub fn width(&self) -> i64 {
        self.df.width() as i64
    }
    #[napi(getter, catch_unwind)]
    pub fn schema(&self) -> Wrap<Schema> {
        self.df.schema().into()
    }
    #[napi(catch_unwind)]
    pub fn hstack_mut(&mut self, columns: Array) -> napi::Result<()> {
        let columns = to_series_collection(columns);
        self.df.hstack_mut(&columns).map_err(JsPolarsErr::from)?;
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn hstack(&self, columns: Array) -> napi::Result<JsDataFrame> {
        let columns = to_series_collection(columns);
        let df = self.df.hstack(&columns).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }
    #[napi(catch_unwind)]
    pub fn extend(&mut self, df: &JsDataFrame) -> napi::Result<()> {
        self.df.extend(&df.df).map_err(JsPolarsErr::from)?;
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn vstack_mut(&mut self, df: &JsDataFrame) -> napi::Result<()> {
        self.df.vstack_mut(&df.df).map_err(JsPolarsErr::from)?;
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn vstack(&mut self, df: &JsDataFrame) -> napi::Result<JsDataFrame> {
        let df = self.df.vstack(&df.df).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }
    #[napi(catch_unwind)]
    pub fn drop_in_place(&mut self, name: String) -> napi::Result<JsSeries> {
        let s = self.df.drop_in_place(&name).map_err(JsPolarsErr::from)?;
        Ok(JsSeries {
            series: s.take_materialized_series(),
        })
    }
    #[napi(catch_unwind)]
    pub fn drop_nulls(&self, subset: Option<Vec<String>>) -> napi::Result<JsDataFrame> {
        let df = self
            .df
            .drop_nulls(subset.as_ref().map(|s| s.as_ref()))
            .map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn drop(&self, name: String) -> napi::Result<JsDataFrame> {
        let df = self.df.drop(&name).map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }
    #[napi(catch_unwind)]
    pub fn select_at_idx(&self, idx: i64) -> Option<JsSeries> {
        self.df
            .select_at_idx(idx as usize)
            .map(|s| JsSeries::new(s.clone().take_materialized_series()))
    }

    #[napi(catch_unwind)]
    pub fn find_idx_by_name(&self, name: String) -> Option<i64> {
        self.df.get_column_index(&name).map(|i| i as i64)
    }
    #[napi(catch_unwind)]
    pub fn column(&self, name: String) -> napi::Result<JsSeries> {
        let series = self
            .df
            .column(&name)
            .map(|s| JsSeries::new(s.clone().take_materialized_series()))
            .map_err(JsPolarsErr::from)?;
        Ok(series)
    }
    #[napi(catch_unwind)]
    pub fn select(&self, selection: Vec<&str>) -> napi::Result<JsDataFrame> {
        let df = self.df.select(selection).map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }
    #[napi(catch_unwind)]
    pub fn filter(&self, mask: &JsSeries) -> napi::Result<JsDataFrame> {
        let filter_series = &mask.series;
        if let Ok(ca) = filter_series.bool() {
            let df = self.df.filter(ca).map_err(JsPolarsErr::from)?;
            Ok(JsDataFrame::new(df))
        } else {
            Err(napi::Error::from_reason(
                "Expected a boolean mask".to_owned(),
            ))
        }
    }
    #[napi(catch_unwind)]
    pub fn take(&self, indices: Vec<u32>) -> napi::Result<JsDataFrame> {
        let indices = UInt32Chunked::from_vec(PlSmallStr::EMPTY, indices);
        let df = self.df.take(&indices).map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }
    #[napi(catch_unwind)]
    pub fn take_with_series(&self, indices: &JsSeries) -> napi::Result<JsDataFrame> {
        let idx = indices.series.u32().map_err(JsPolarsErr::from)?;
        let df = self.df.take(idx).map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }
    #[napi(catch_unwind)]
    pub fn sort(
        &self,
        by_column: String,
        descending: bool,
        nulls_last: bool,
        maintain_order: bool,
    ) -> napi::Result<JsDataFrame> {
        let df = self
            .df
            .sort(
                [&by_column],
                SortMultipleOptions::default()
                    .with_order_descending(descending)
                    .with_nulls_last(nulls_last)
                    .with_maintain_order(maintain_order),
            )
            .map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }
    #[napi(catch_unwind)]
    pub fn sort_in_place(
        &mut self,
        by_column: String,
        descending: bool,
        maintain_order: bool,
    ) -> napi::Result<()> {
        self.df
            .sort_in_place(
                [&by_column],
                SortMultipleOptions::default()
                    .with_order_descending(descending)
                    .with_maintain_order(maintain_order),
            )
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn replace(&mut self, column: String, new_col: &JsSeries) -> napi::Result<()> {
        self.df
            .replace(&column, new_col.series.clone())
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub fn rename(&mut self, column: String, new_col: String) -> napi::Result<()> {
        self.df
            .rename(&column, PlSmallStr::from_string(new_col))
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub fn replace_at_idx(&mut self, index: f64, new_col: &JsSeries) -> napi::Result<()> {
        self.df
            .replace_column(index as usize, new_col.series.clone())
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub fn insert_at_idx(&mut self, index: f64, new_col: &JsSeries) -> napi::Result<()> {
        self.df
            .insert_column(index as usize, new_col.series.clone())
            .map_err(JsPolarsErr::from)?;
        Ok(())
    }

    #[napi(catch_unwind)]
    pub fn slice(&self, offset: i64, length: i64) -> JsDataFrame {
        let df = self.df.slice(offset as i64, length as usize);
        df.into()
    }

    #[napi(catch_unwind)]
    pub fn head(&self, length: Option<i64>) -> JsDataFrame {
        let length = length.map(|l| l as usize);
        let df = self.df.head(length);
        JsDataFrame::new(df)
    }
    #[napi(catch_unwind)]
    pub fn tail(&self, length: Option<i64>) -> JsDataFrame {
        let length = length.map(|l| l as usize);
        let df = self.df.tail(length);
        JsDataFrame::new(df)
    }
    #[napi(catch_unwind)]
    pub fn is_unique(&self) -> napi::Result<JsSeries> {
        let mask = self.df.is_unique().map_err(JsPolarsErr::from)?;
        Ok(mask.into_series().into())
    }
    #[napi(catch_unwind)]
    pub fn is_duplicated(&self) -> napi::Result<JsSeries> {
        let mask = self.df.is_duplicated().map_err(JsPolarsErr::from)?;
        Ok(mask.into_series().into())
    }
    #[napi(catch_unwind)]
    pub fn frame_equal(&self, other: &JsDataFrame, null_equal: bool) -> bool {
        if null_equal {
            self.df.equals_missing(&other.df)
        } else {
            self.df.equals(&other.df)
        }
    }
    #[napi(catch_unwind)]
    pub fn with_row_count(&self, name: String, offset: Option<u32>) -> napi::Result<JsDataFrame> {
        let df = self
            .df
            .with_row_index(PlSmallStr::from_string(name), offset)
            .map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }
    #[napi(catch_unwind)]
    pub fn groupby(
        &self,
        by: Vec<&str>,
        select: Option<Vec<String>>,
        agg: String,
    ) -> napi::Result<JsDataFrame> {
        let gb = self.df.group_by(by).map_err(JsPolarsErr::from)?;
        let selection = match select.as_ref() {
            Some(s) => gb.select(s),
            None => gb,
        };
        finish_groupby(selection, &agg)
    }

    #[napi(catch_unwind)]
    pub fn pivot_expr(
        &self,
        values: Vec<String>,
        on: Vec<String>,
        index: Vec<String>,
        aggregate_expr: Option<Wrap<polars::prelude::Expr>>,
        maintain_order: bool,
        sort_columns: bool,
        separator: Option<&str>,
    ) -> napi::Result<JsDataFrame> {
        let fun = match maintain_order {
            true => polars::prelude::pivot::pivot_stable,
            false => polars::prelude::pivot::pivot,
        };
        fun(
            &self.df,
            on,
            Some(index),
            Some(values),
            sort_columns,
            aggregate_expr.map(|e| e.0 as Expr),
            separator,
        )
        .map(|df| df.into())
        .map_err(|e| napi::Error::from_reason(format!("Could not pivot: {}", e)))
    }
    #[napi(catch_unwind)]
    pub fn clone(&self) -> JsDataFrame {
        JsDataFrame::new(self.df.clone())
    }
    #[napi(catch_unwind)]
    pub fn unpivot(
        &self,
        id_vars: Vec<String>,
        value_vars: Vec<String>,
        variable_name: Option<String>,
        value_name: Option<String>,
    ) -> napi::Result<JsDataFrame> {
        let args = UnpivotArgsIR {
            on: strings_to_pl_smallstr(value_vars),
            index: strings_to_pl_smallstr(id_vars),
            variable_name: variable_name.map(|s| s.into()),
            value_name: value_name.map(|s| s.into()),
        };

        let df = self.df.unpivot2(args).map_err(JsPolarsErr::from)?;
        Ok(JsDataFrame::new(df))
    }

    #[napi(catch_unwind)]
    pub fn partition_by(
        &self,
        groups: Vec<String>,
        stable: bool,
        include_key: bool,
    ) -> napi::Result<Vec<JsDataFrame>> {
        let out = if stable {
            self.df.partition_by_stable(groups, include_key)
        } else {
            self.df.partition_by(groups, include_key)
        }
        .map_err(JsPolarsErr::from)?;
        // Safety:
        // Repr mem layout
        Ok(unsafe { std::mem::transmute::<Vec<DataFrame>, Vec<JsDataFrame>>(out) })
    }

    #[napi(catch_unwind)]
    pub fn shift(&self, periods: i64) -> JsDataFrame {
        self.df.shift(periods).into()
    }
    #[napi(catch_unwind)]
    pub fn unique(
        &self,
        maintain_order: bool,
        subset: Option<Vec<String>>,
        keep: Wrap<UniqueKeepStrategy>,
        slice: Option<Wrap<(i64, usize)>>,
    ) -> napi::Result<JsDataFrame> {
        let subset = subset.map(|v| v.iter().map(|x| PlSmallStr::from_str(x.as_str())).collect());
        let df = self
            .df
            .unique_impl(
                maintain_order,
                subset,
                keep.0,
                slice.map(|s| s.0 as (i64, usize)),
            )
            .map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn lazy(&self) -> crate::lazy::dataframe::JsLazyFrame {
        self.df.clone().lazy().into()
    }

    #[napi(catch_unwind)]
    pub fn hmean(&self, null_strategy: Wrap<NullStrategy>) -> napi::Result<Option<JsSeries>> {
        let s = self
            .df
            .mean_horizontal(null_strategy.0)
            .map_err(JsPolarsErr::from)?;
        Ok(s.map(|s| s.take_materialized_series().into()))
    }
    #[napi(catch_unwind)]
    pub fn hmax(&self) -> napi::Result<Option<JsSeries>> {
        let s = self.df.max_horizontal().map_err(JsPolarsErr::from)?;
        Ok(s.map(|s| s.take_materialized_series().into()))
    }

    #[napi(catch_unwind)]
    pub fn hmin(&self) -> napi::Result<Option<JsSeries>> {
        let s = self.df.min_horizontal().map_err(JsPolarsErr::from)?;
        Ok(s.map(|s| s.take_materialized_series().into()))
    }

    #[napi(catch_unwind)]
    pub fn hsum(&self, null_strategy: Wrap<NullStrategy>) -> napi::Result<Option<JsSeries>> {
        let s = self
            .df
            .sum_horizontal(null_strategy.0)
            .map_err(JsPolarsErr::from)?;
        Ok(s.map(|s| s.take_materialized_series().into()))
    }
    #[napi(catch_unwind)]
    pub fn to_dummies(
        &self,
        separator: Option<&str>,
        drop_first: bool,
    ) -> napi::Result<JsDataFrame> {
        let df = self
            .df
            .to_dummies(separator, drop_first)
            .map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn null_count(&self) -> JsDataFrame {
        let df = self.df.null_count();
        df.into()
    }
    #[napi(catch_unwind)]
    pub fn shrink_to_fit(&mut self) {
        self.df.shrink_to_fit();
    }
    #[napi(catch_unwind)]
    pub fn hash_rows(
        &mut self,
        k0: Wrap<u64>,
        k1: Wrap<u64>,
        k2: Wrap<u64>,
        k3: Wrap<u64>,
    ) -> napi::Result<JsSeries> {
        let hb = PlRandomState::with_seeds(k0.0, k1.0, k2.0, k3.0);
        let hash = self.df.hash_rows(Some(hb)).map_err(JsPolarsErr::from)?;
        Ok(hash.into_series().into())
    }

    #[napi(catch_unwind)]
    pub unsafe fn transpose(
        &mut self,
        keep_names_as: Option<String>,
        names: Option<Either<String, Vec<String>>>,
    ) -> napi::Result<JsDataFrame> {
        let names = names.map(|e| match e {
            Either::A(s) => either::Either::Left(s),
            Either::B(v) => either::Either::Right(v),
        });
        Ok(self
            .df
            .transpose(keep_names_as.as_deref(), names)
            .map_err(JsPolarsErr::from)?
            .into())
    }

    #[napi(catch_unwind)]
    pub fn sample_n(
        &self,
        n: &JsSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<i64>,
    ) -> napi::Result<JsDataFrame> {
        let df = self
            .df
            .sample_n(&n.series, with_replacement, shuffle, seed.map(|s| s as u64))
            .map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn sample_frac(
        &self,
        frac: &JsSeries,
        with_replacement: bool,
        shuffle: bool,
        seed: Option<i64>,
    ) -> napi::Result<JsDataFrame> {
        let df = self
            .df
            .sample_frac(
                &frac.series,
                with_replacement,
                shuffle,
                seed.map(|s| s as u64),
            )
            .map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }

    #[napi(catch_unwind)]
    pub fn upsample(
        &self,
        by: Vec<String>,
        index_column: String,
        every: String,
        stable: bool,
    ) -> napi::Result<JsDataFrame> {
        let out = if stable {
            self.df
                .upsample_stable(by, &index_column, Duration::parse(&every))
        } else {
            self.df.upsample(by, &index_column, Duration::parse(&every))
        };
        let out = out.map_err(JsPolarsErr::from)?;
        Ok(out.into())
    }
    #[napi(catch_unwind)]
    pub fn to_struct(&self, name: String) -> JsSeries {
        let s = self.df.clone().into_struct(PlSmallStr::from_string(name));
        s.into_series().into()
    }
    #[napi(catch_unwind)]
    pub fn unnest(&self, names: Vec<String>) -> napi::Result<JsDataFrame> {
        let df = self.df.unnest(names).map_err(JsPolarsErr::from)?;
        Ok(df.into())
    }
    #[napi(catch_unwind)]
    pub fn to_row(&self, idx: f64, env: Env) -> napi::Result<Array> {
        let idx = idx as i64;

        let idx = if idx < 0 {
            (self.df.height() as i64 + idx) as usize
        } else {
            idx as usize
        };

        let width = self.df.width();
        let mut row = env.create_array(width as u32)?;

        for (i, col) in self.df.get_columns().iter().enumerate() {
            let val = col.get(idx);
            row.set(i as u32, Wrap(val.unwrap()))?;
        }
        Ok(row)
    }

    #[napi(catch_unwind)]
    pub fn to_rows(&self, env: Env) -> napi::Result<Array> {
        let (height, width) = self.df.shape();

        let mut rows = env.create_array(height as u32)?;
        for idx in 0..height {
            let mut row = env.create_array(width as u32)?;
            for (i, col) in self.df.get_columns().iter().enumerate() {
                let val = col.get(idx);
                row.set(i as u32, Wrap(val.unwrap()))?;
            }
            rows.set(idx as u32, row)?;
        }
        Ok(rows)
    }
    // #[napi]
    // pub fn to_rows_cb(&self, callback: napi::JsFunction, env: Env) -> napi::Result<()> {
    //     panic!("not implemented");
    // use napi::threadsafe_function::*;
    // use polars_core::utils::rayon::prelude::*;
    // let (height, _) = self.df.shape();
    // let tsfn: ThreadsafeFunction<
    //     Either<Vec<JsAnyValue>, napi::JsNull>,
    //     ErrorStrategy::CalleeHandled,
    // > = callback.create_threadsafe_function(
    //     0,
    //     |ctx: ThreadSafeCallContext<Either<Vec<JsAnyValue>, napi::JsNull>>| Ok(vec![ctx.value]),
    // )?;

    // polars_core::POOL.install(|| {
    //     (0..height).into_par_iter().for_each(|idx| {
    //         let tsfn = tsfn.clone();
    //         let values = self
    //             .df
    //             .get_columns()
    //             .iter()
    //             .map(|s| {
    //                 let av: JsAnyValue = s.get(idx).into();
    //                 av
    //             })
    //             .collect::<Vec<_>>();

    //         tsfn.call(
    //             Ok(Either::A(values)),
    //             ThreadsafeFunctionCallMode::NonBlocking,
    //         );
    //     });
    // });
    // tsfn.call(
    //     Ok(Either::B(env.get_null().unwrap())),
    //     ThreadsafeFunctionCallMode::NonBlocking,
    // );

    // Ok(())
    // }
    #[napi]
    pub fn to_row_obj(&self, idx: Either<i64, f64>, env: Env) -> napi::Result<Object> {
        let idx = match idx {
            Either::A(a) => a,
            Either::B(b) => b as i64,
        };

        let idx = if idx < 0 {
            (self.df.height() as i64 + idx) as usize
        } else {
            idx as usize
        };

        let mut row = env.create_object()?;

        for col in self.df.get_columns() {
            let key = col.name();
            let val = col.get(idx);
            row.set(key, Wrap(val.unwrap()))?;
        }
        Ok(row)
    }
    #[napi(catch_unwind)]
    pub fn to_objects(&self, env: Env) -> napi::Result<Array> {
        let (height, _) = self.df.shape();

        let mut rows = env.create_array(height as u32)?;
        for idx in 0..height {
            let mut row = env.create_object()?;
            for col in self.df.get_columns() {
                let key = col.name();
                let val = col.get(idx);
                row.set(key, Wrap(val.unwrap()))?;
            }
            rows.set(idx as u32, row)?;
        }
        Ok(rows)
    }

    // #[napi]
    // pub fn to_objects_cb(&self, callback: napi::JsFunction, env: Env) -> napi::Result<()> {
    //     panic!("not implemented");
    // use napi::threadsafe_function::*;
    // use polars_core::utils::rayon::prelude::*;
    // use std::collections::HashMap;
    // let (height, _) = self.df.shape();
    // let tsfn: ThreadsafeFunction<
    //     Either<HashMap<String, JsAnyValue>, napi::JsNull>,
    //     ErrorStrategy::CalleeHandled,
    // > = callback.create_threadsafe_function(
    //     0,
    //     |ctx: ThreadSafeCallContext<Either<HashMap<String, JsAnyValue>, napi::JsNull>>| {
    //         Ok(vec![ctx.value])
    //     },
    // )?;

    // polars_core::POOL.install(|| {
    //     (0..height).into_par_iter().for_each(|idx| {
    //         let tsfn = tsfn.clone();
    //         let values = self
    //             .df
    //             .get_columns()
    //             .iter()
    //             .map(|s| {
    //                 let key = s.name().to_owned();
    //                 let av: JsAnyValue = s.get(idx).into();
    //                 (key, av)
    //             })
    //             .collect::<HashMap<_, _>>();

    //         tsfn.call(
    //             Ok(Either::A(values)),
    //             ThreadsafeFunctionCallMode::NonBlocking,
    //         );
    //     });
    // });
    // tsfn.call(
    //     Ok(Either::B(env.get_null().unwrap())),
    //     ThreadsafeFunctionCallMode::NonBlocking,
    // );

    // Ok(())
    // }

    #[napi(catch_unwind)]
    pub fn write_csv(
        &mut self,
        path_or_buffer: JsUnknown,
        options: Wrap<CsvWriterOptions>,
        env: Env,
    ) -> napi::Result<()> {
        match path_or_buffer.get_type()? {
            ValueType::String => {
                let path: napi::JsString = unsafe { path_or_buffer.cast() };
                let path = path.into_utf8()?.into_owned()?;

                let f = std::fs::File::create(path).unwrap();
                let f = BufWriter::new(f);
                CsvWriter::new(f)
                    .include_bom(options.0.include_bom)
                    .include_header(options.0.include_header)
                    .with_separator(options.0.serialize_options.separator)
                    .with_line_terminator(options.0.serialize_options.line_terminator)
                    .with_batch_size(options.0.batch_size)
                    .with_datetime_format(options.0.serialize_options.datetime_format)
                    .with_date_format(options.0.serialize_options.date_format)
                    .with_time_format(options.0.serialize_options.time_format)
                    .with_float_precision(options.0.serialize_options.float_precision)
                    .with_null_value(options.0.serialize_options.null)
                    .with_quote_char(options.0.serialize_options.quote_char)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            ValueType::Object => {
                let inner: napi::JsObject = unsafe { path_or_buffer.cast() };
                let writeable = JsWriteStream { inner, env: &env };

                CsvWriter::new(writeable)
                    .include_bom(options.0.include_bom)
                    .include_header(options.0.include_header)
                    .with_separator(options.0.serialize_options.separator)
                    .with_line_terminator(options.0.serialize_options.line_terminator)
                    .with_batch_size(options.0.batch_size)
                    .with_datetime_format(options.0.serialize_options.datetime_format)
                    .with_date_format(options.0.serialize_options.date_format)
                    .with_time_format(options.0.serialize_options.time_format)
                    .with_float_precision(options.0.serialize_options.float_precision)
                    .with_null_value(options.0.serialize_options.null)
                    .with_quote_char(options.0.serialize_options.quote_char)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            _ => panic!(),
        };
        Ok(())
    }

    #[napi(catch_unwind)]
    pub fn write_parquet(
        &mut self,
        path_or_buffer: JsUnknown,
        compression: Wrap<ParquetCompression>,
        env: Env,
    ) -> napi::Result<()> {
        match path_or_buffer.get_type()? {
            ValueType::String => {
                let path: napi::JsString = unsafe { path_or_buffer.cast() };
                let path = path.into_utf8()?.into_owned()?;

                let f = std::fs::File::create(path).unwrap();
                let f = BufWriter::new(f);
                ParquetWriter::new(f)
                    .with_compression(compression.0)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            ValueType::Object => {
                let inner: napi::JsObject = unsafe { path_or_buffer.cast() };
                let writeable = JsWriteStream { inner, env: &env };

                ParquetWriter::new(writeable)
                    .with_compression(compression.0)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            _ => panic!(),
        };
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn write_ipc(
        &mut self,
        path_or_buffer: JsUnknown,
        compression: Wrap<Option<IpcCompression>>,
        env: Env,
    ) -> napi::Result<()> {
        match path_or_buffer.get_type()? {
            ValueType::String => {
                let path: napi::JsString = unsafe { path_or_buffer.cast() };
                let path = path.into_utf8()?.into_owned()?;
                let f = std::fs::File::create(path).unwrap();
                let f = BufWriter::new(f);
                IpcWriter::new(f)
                    .with_compression(compression.0)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            ValueType::Object => {
                let inner: napi::JsObject = unsafe { path_or_buffer.cast() };
                let writeable = JsWriteStream { inner, env: &env };
                IpcWriter::new(writeable)
                    .with_compression(compression.0)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            _ => panic!(),
        };
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn write_ipc_stream(
        &mut self,
        path_or_buffer: JsUnknown,
        compression: Wrap<Option<IpcCompression>>,
        env: Env,
    ) -> napi::Result<()> {
        match path_or_buffer.get_type()? {
            ValueType::String => {
                let path: napi::JsString = unsafe { path_or_buffer.cast() };
                let path = path.into_utf8()?.into_owned()?;
                let f = std::fs::File::create(path).unwrap();
                let f = BufWriter::new(f);
                IpcStreamWriter::new(f)
                    .with_compression(compression.0)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            ValueType::Object => {
                let inner: napi::JsObject = unsafe { path_or_buffer.cast() };
                let writeable = JsWriteStream { inner, env: &env };
                IpcStreamWriter::new(writeable)
                    .with_compression(compression.0)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            _ => panic!(),
        };
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn write_json(
        &mut self,
        path_or_buffer: JsUnknown,
        options: WriteJsonOptions,
        env: Env,
    ) -> napi::Result<()> {
        let json_format = options.format;
        let json_format = match json_format.as_ref() {
            "json" => JsonFormat::Json,
            "lines" => JsonFormat::JsonLines,
            _ => {
                return Err(napi::Error::from_reason(
                    "format must be 'json' or `lines'".to_owned(),
                ))
            }
        };

        match path_or_buffer.get_type()? {
            ValueType::String => {
                let path: napi::JsString = unsafe { path_or_buffer.cast() };
                let path = path.into_utf8()?.into_owned()?;
                let f = std::fs::File::create(path).unwrap();
                let f = BufWriter::new(f);
                JsonWriter::new(f)
                    .with_json_format(json_format)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            ValueType::Object => {
                let inner: napi::JsObject = unsafe { path_or_buffer.cast() };
                let writeable = JsWriteStream { inner, env: &env };
                JsonWriter::new(writeable)
                    .with_json_format(json_format)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)
                    .unwrap()
            }
            _ => panic!(),
        };
        Ok(())
    }
    #[napi(catch_unwind)]
    pub fn write_avro(
        &mut self,
        path_or_buffer: JsUnknown,
        compression: String,
        env: Env,
    ) -> napi::Result<()> {
        use polars::io::avro::{AvroCompression, AvroWriter};
        let compression = match compression.as_ref() {
            "uncompressed" => None,
            "snappy" => Some(AvroCompression::Snappy),
            "deflate" => Some(AvroCompression::Deflate),
            s => return Err(JsPolarsErr::Other(format!("compression {} not supported", s)).into()),
        };

        match path_or_buffer.get_type()? {
            ValueType::String => {
                let path: napi::JsString = unsafe { path_or_buffer.cast() };
                let path = path.into_utf8()?.into_owned()?;
                let f = std::fs::File::create(path).unwrap();
                let f = BufWriter::new(f);
                AvroWriter::new(f)
                    .with_compression(compression)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            ValueType::Object => {
                let inner: napi::JsObject = unsafe { path_or_buffer.cast() };
                let writeable = JsWriteStream { inner, env: &env };

                AvroWriter::new(writeable)
                    .with_compression(compression)
                    .finish(&mut self.df)
                    .map_err(JsPolarsErr::from)?;
            }
            _ => panic!(),
        };
        Ok(())
    }
}

#[allow(deprecated)]
fn finish_groupby(gb: GroupBy, agg: &str) -> napi::Result<JsDataFrame> {
    let df = match agg {
        "min" => gb.min(),
        "max" => gb.max(),
        "mean" => gb.mean(),
        "first" => gb.first(),
        "last" => gb.last(),
        "sum" => gb.sum(),
        "count" => gb.count(),
        "n_unique" => gb.n_unique(),
        "median" => gb.median(),
        "agg_list" => gb.agg_list(),
        "groups" => gb.groups(),
        a => Err(PolarsError::ComputeError(
            format!("agg fn {} does not exists", a).into(),
        )),
    };

    let df = df.map_err(JsPolarsErr::from)?;
    Ok(JsDataFrame::new(df))
}

fn coerce_data_type<A: Borrow<DataType>>(datatypes: &[A]) -> DataType {
    use DataType::*;

    let are_all_equal = datatypes.windows(2).all(|w| w[0].borrow() == w[1].borrow());

    if are_all_equal {
        return datatypes[0].borrow().clone();
    }

    let (lhs, rhs) = (datatypes[0].borrow(), datatypes[1].borrow());

    return match (lhs, rhs) {
        (lhs, rhs) if lhs == rhs => lhs.clone(),
        (List(lhs), List(rhs)) => {
            let inner = coerce_data_type(&[lhs.as_ref(), rhs.as_ref()]);
            List(Box::new(inner))
        }
        (scalar, List(list)) => {
            let inner = coerce_data_type(&[scalar, list.as_ref()]);
            List(Box::new(inner))
        }
        (List(list), scalar) => {
            let inner = coerce_data_type(&[scalar, list.as_ref()]);
            List(Box::new(inner))
        }
        (Float64, UInt64) => Float64,
        (UInt64, Float64) => Float64,
        (UInt64, Boolean) => UInt64,
        (Boolean, UInt64) => UInt64,
        (_, _) => String,
    };
}

fn obj_to_pairs(rows: &Array, len: usize) -> impl '_ + Iterator<Item = Vec<(String, DataType)>> {
    let len = std::cmp::min(len, rows.len() as usize);
    (0..len).map(move |idx| {
        let obj = rows.get::<Object>(idx as u32).unwrap().unwrap();
        let keys = Object::keys(&obj).unwrap();
        keys.iter()
            .map(|key| {
                let value = obj.get::<_, napi::JsUnknown>(&key).unwrap_or(None);
                let dtype = match value {
                    Some(val) => {
                        let ty = val.get_type().unwrap();
                        match ty {
                            ValueType::Boolean => DataType::Boolean,
                            ValueType::Number => DataType::Float64,
                            ValueType::BigInt => DataType::UInt64,
                            ValueType::String => DataType::String,
                            ValueType::Object => {
                                if val.is_array().unwrap() {
                                    let arr: napi::JsObject = unsafe { val.cast() };
                                    let len = arr.get_array_length().unwrap();
                                    if len == 0 {
                                        DataType::List(DataType::Null.into())
                                    } else {
                                        // dont compare too many items, as it could be expensive
                                        let max_take = std::cmp::min(len as usize, 10);
                                        let mut dtypes: Vec<DataType> =
                                            Vec::with_capacity(len as usize);

                                        for idx in 0..max_take {
                                            let item: napi::JsUnknown =
                                                arr.get_element(idx as u32).unwrap();
                                            let ty = item.get_type().unwrap();
                                            let dt: Wrap<DataType> = ty.into();
                                            dtypes.push(dt.0)
                                        }
                                        let dtype = coerce_data_type(&dtypes);

                                        DataType::List(dtype.into())
                                    }
                                } else if val.is_date().unwrap() {
                                    DataType::Datetime(TimeUnit::Milliseconds, None)
                                } else {
                                    let inner_val: napi::JsObject = unsafe { val.cast() };
                                    let inner_keys = Object::keys(&inner_val).unwrap();
                                    let mut fldvec: Vec<Field> = Vec::with_capacity(inner_keys.len() as usize);

                                    inner_keys.iter().for_each(|key| {
                                        let inner_val = &inner_val.get::<_, napi::JsUnknown>(&key).unwrap();
                                        let dtype = match inner_val.as_ref().unwrap().get_type().unwrap() {
                                            ValueType::Boolean => DataType::Boolean,
                                            ValueType::Number => DataType::Float64,
                                            ValueType::BigInt => DataType::UInt64,
                                            ValueType::String => DataType::String,
                                            ValueType::Object => DataType::Struct(vec![]),
                                            _ => DataType::Null
                                        };
                        
                                        let fld = Field::new(key.into(), dtype);
                                        fldvec.push(fld);
                                    });
                                    DataType::Struct(fldvec)
                                }
                            }
                            _ => DataType::Null,
                        }
                    }
                    None => DataType::Null
                };
                (key.to_owned(), dtype)
            })
            .collect()
    })
}

unsafe fn coerce_js_anyvalue<'a>(val: JsUnknown, dtype: &'a DataType) -> JsResult<AnyValue<'a>> {
    use DataType::*;
    let vtype = val.get_type().unwrap();
    match (vtype, dtype) {
        (ValueType::Null | ValueType::Undefined | ValueType::Unknown, _) => Ok(AnyValue::Null),
        (ValueType::String, String) => AnyValue::from_js(val),
        (_, String) => {
            let s = val.coerce_to_string()?.into_unknown();
            AnyValue::from_js(s)
        }
        (ValueType::Boolean, Boolean) => bool::from_js(val).map(AnyValue::Boolean),
        (_, Boolean) => val.coerce_to_bool().map(|b| {
            let b: bool = b.try_into().unwrap();
            AnyValue::Boolean(b)
        }),
        (ValueType::BigInt | ValueType::Number, UInt64) => u64::from_js(val).map(AnyValue::UInt64),
        (_, UInt64) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_int64().unwrap();
            AnyValue::UInt64(n as u64)
        }),
        (ValueType::BigInt | ValueType::Number, Int64) => i64::from_js(val).map(AnyValue::Int64),
        (_, Int64) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_int64().unwrap();
            AnyValue::Int64(n)
        }),
        (ValueType::Number, Float64) => f64::from_js(val).map(AnyValue::Float64),
        (_, Float64) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_double().unwrap();
            AnyValue::Float64(n)
        }),
        (ValueType::Number, Float32) => f32::from_js(val).map(AnyValue::Float32),
        (_, Float32) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_double().unwrap();
            AnyValue::Float32(n as f32)
        }),
        (ValueType::Number, Int32) => i32::from_js(val).map(AnyValue::Int32),
        (_, Int32) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_int32().unwrap();
            AnyValue::Int32(n)
        }),
        (ValueType::Number, UInt32) => u32::from_js(val).map(AnyValue::UInt32),
        (_, UInt32) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_uint32().unwrap();
            AnyValue::UInt32(n)
        }),
        (ValueType::Number, Int16) => i16::from_js(val).map(AnyValue::Int16),
        (_, Int16) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_int32().unwrap();
            AnyValue::Int16(n as i16)
        }),
        (ValueType::Number, UInt16) => u16::from_js(val).map(AnyValue::UInt16),
        (_, UInt16) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_uint32().unwrap();
            AnyValue::UInt16(n as u16)
        }),
        (ValueType::Number, Int8) => i8::from_js(val).map(AnyValue::Int8),
        (_, Int8) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_int32().unwrap();
            AnyValue::Int8(n as i8)
        }),
        (ValueType::Number, UInt8) => u8::from_js(val).map(AnyValue::UInt8),
        (_, UInt8) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_uint32().unwrap();
            AnyValue::UInt8(n as u8)
        }),
        (ValueType::Number, Date) => i32::from_js(val).map(AnyValue::Date),
        (_, Date) => val.coerce_to_number().map(|js_num| {
            let n = js_num.get_int32().unwrap();
            AnyValue::Date(n)
        }),
        (ValueType::BigInt | ValueType::Number, Datetime(_, _)) => {
            i64::from_js(val).map(|d| AnyValue::Datetime(d, TimeUnit::Milliseconds, None))
        }
        (ValueType::Object, DataType::Datetime(_, _)) => {
            if val.is_date()? {
                let d: napi::JsDate = val.cast();
                let d = d.value_of()?;
                Ok(AnyValue::Datetime(d as i64, TimeUnit::Milliseconds, None))
            } else {
                Ok(AnyValue::Null)
            }
        }
        (ValueType::Object, DataType::List(_)) => {
            let s = val.to_series();
            Ok(AnyValue::List(s))
        }
        (ValueType::Object, DataType::Struct(fields)) => {
            let number_of_fields: i8 = fields.len().try_into().map_err(
                |e| napi::Error::from_reason(format!("the number of `fields` cannot be larger than i8::MAX {e:?}"))
            )?;

            let inner_val: napi::JsObject = val.cast();
            let arrow_dtype = dtype.to_physical().to_arrow(CompatLevel::newest());

            let mut val_vec = Vec::with_capacity(number_of_fields as usize);
            fields.iter().for_each(|fld| {
                let single_val = inner_val.get::<_, napi::JsUnknown>(&fld.name).unwrap().unwrap();
                let vv = match fld.dtype {
                    DataType::Boolean =>
                    {
                        let bl = single_val.coerce_to_bool().unwrap().get_value().unwrap();
                        BooleanArray::from_slice([bl]).boxed()
                    },
                    DataType::String =>
                    {
                        let ut = single_val.coerce_to_string().unwrap().into_utf8().unwrap();
                        let s = ut.as_str().unwrap();
                        Utf8ViewArray::from_slice_values([s]).boxed()
                    },
                    DataType::Int32 => {
                        let js_num = single_val.coerce_to_number().unwrap().get_int32().unwrap();
                        PrimitiveArray::<i32>::from(vec![Some(js_num)]).boxed()
                    },
                    DataType::Int64 => {
                        let js_num = single_val.coerce_to_number().unwrap().get_int64().unwrap();
                        PrimitiveArray::<i64>::from(vec![Some(js_num)]).boxed()
                    },
                    DataType::Float64 => {
                        let js_num = single_val.coerce_to_number().unwrap().get_double().unwrap();
                        PrimitiveArray::<f64>::from(vec![Some(js_num)]).boxed()
                    },
                    _ => NullArray::new(ArrowDataType::Null, 1).boxed()
                };
                val_vec.push(vv);
            });

            let array = StructArray::new(arrow_dtype.clone(),1,val_vec,None);
            let array = &*(&array as *const dyn ArrowArray as *const StructArray);
            Ok(AnyValue::Struct(number_of_fields as usize, &array, &fields))
        }
        _ => Ok(AnyValue::Null),
    }
}
