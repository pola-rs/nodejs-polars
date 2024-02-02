import { DataType } from "./datatypes";
import pli from "./internals/polars_internal";
import { DataFrame, _DataFrame } from "./dataframe";
import { isPath } from "./utils";
import { LazyDataFrame, _LazyDataFrame } from "./lazy/dataframe";
import { Readable, Stream } from "stream";
import { concat } from "./functions";

export function readPostgres(
  query: string,
  connectionString: string,
): DataFrame {
  return _DataFrame(
    pli.readDatabase(query, string),
  );
}
