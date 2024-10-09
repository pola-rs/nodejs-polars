import fs from "node:fs";
import path from "node:path";
import { Stream } from "node:stream";
import pl from "@polars";
// eslint-disable-next-line no-undef
const csvpath = path.resolve(__dirname, "./examples/datasets/foods1.csv");
// eslint-disable-next-line no-undef
const tsvpath = path.resolve(__dirname, "./examples/datasets/data.tsv");
// eslint-disable-next-line no-undef
const emptycsvpath = path.resolve(__dirname, "./examples/datasets/empty.csv");
// eslint-disable-next-line no-undef
const parquetpath = path.resolve(__dirname, "./examples/foods.parquet");
// eslint-disable-next-line no-undef
const avropath = path.resolve(__dirname, "./examples/foods.avro");
// eslint-disable-next-line no-undef
const ipcpath = path.resolve(__dirname, "./examples/foods.ipc");
// eslint-disable-next-line no-undef
const jsonpath = path.resolve(__dirname, "./examples/foods.json");
// eslint-disable-next-line no-undef
const singlejsonpath = path.resolve(__dirname, "./examples/single_foods.json");
describe("read:csv", () => {
  it("can read from a csv file", () => {
    const df = pl.readCSV(csvpath);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  it("can read from a csv file with inferSchemaLength = 0 option", () => {
    let df = pl.readCSV(csvpath, { inferSchemaLength: 0 });
    const expected = `shape: (1, 4)
┌────────────┬──────────┬────────┬──────────┐
│ category   ┆ calories ┆ fats_g ┆ sugars_g │
│ ---        ┆ ---      ┆ ---    ┆ ---      │
│ str        ┆ str      ┆ str    ┆ str      │
╞════════════╪══════════╪════════╪══════════╡
│ vegetables ┆ 45       ┆ 0.5    ┆ 2        │
└────────────┴──────────┴────────┴──────────┘`;
    expect(df.head(1).toString()).toEqual(expected);
    df = pl.readCSV(csvpath, { inferSchemaLength: null });
    expect(df.head(1).toString()).toEqual(expected);
  });
  it("can read from a csv file with options", () => {
    const df = pl.readCSV(csvpath, { hasHeader: false, skipRows: 1, nRows: 4 });
    expect(df.shape).toEqual({ height: 4, width: 4 });
  });
  it("can read from a tsv file", () => {
    const df = pl.readCSV(tsvpath, { sep: "\t" });
    expect(df.shape).toEqual({ height: 2, width: 3 });
  });
  it("can read from a csv string", () => {
    const csvString = "foo,bar,baz\n1,2,3\n4,5,6\n";
    const df = pl.readCSV(csvString);
    expect(df.writeCSV().toString().slice(0, 22)).toEqual(
      csvString.slice(0, 22),
    );
  });
  it("can read from a csv buffer", () => {
    const csvBuffer = Buffer.from("foo,bar,baz\n1,2,3\n4,5,6\n", "utf-8");
    const df = pl.readCSV(csvBuffer);
    expect(df.writeCSV().toString("utf-8").slice(0, 22)).toEqual(
      csvBuffer.toString("utf-8").slice(0, 22),
    );
  });
  it("can read from a csv buffer quoted", () => {
    const csvBuffer = Buffer.from('a,b,c,d\n1,test,"a,b,c",another test');
    const df = pl.readCSV(csvBuffer, { quoteChar: '"' });
    const expected = `shape: (1, 4)
┌─────┬──────┬───────┬──────────────┐
│ a   ┆ b    ┆ c     ┆ d            │
│ --- ┆ ---  ┆ ---   ┆ ---          │
│ i64 ┆ str  ┆ str   ┆ str          │
╞═════╪══════╪═══════╪══════════════╡
│ 1   ┆ test ┆ a,b,c ┆ another test │
└─────┴──────┴───────┴──────────────┘`;
    expect(df.toString()).toEqual(expected);
  });
  it("can read from a csv buffer with options", () => {
    const csvBuffer = Buffer.from("foo,bar,baz\n1,2,3\n4,5,6\n", "utf-8");
    const df = pl.readCSV(csvBuffer, { hasHeader: true, chunkSize: 10 });
    // the newline characters are confusing jest
    expect(df.writeCSV().toString("utf-8").slice(0, 22)).toEqual(
      csvBuffer.toString("utf-8").slice(0, 22),
    );
  });
  it("can read csv with ragged lines", () => {
    const csvBuffer = Buffer.from("A\nB\nC,ragged\n", "utf-8");
    let df = pl.readCSV(csvBuffer);
    const expected = `shape: (2, 1)
┌─────┐
│ A   │
│ --- │
│ str │
╞═════╡
│ B   │
│ C   │
└─────┘`;
    expect(df.toString()).toEqual(expected);
    const f = () => {
      df = pl.readCSV(csvBuffer, { truncateRaggedLines: false });
    };
    expect(f).toThrow();
  });
  it("can load empty csv", () => {
    const df = pl.readCSV(emptycsvpath, { raiseIfEmpty: false });
    expect(df.shape).toEqual({ height: 0, width: 0 });
  });
  it("can parse datetimes", () => {
    const csv = `timestamp,open,high
2021-01-01 00:00:00,0.00305500,0.00306000
2021-01-01 00:15:00,0.00298800,0.00300400
2021-01-01 00:30:00,0.00298300,0.00300100
2021-01-01 00:45:00,0.00299400,0.00304000`;
    const df = pl.readCSV(csv, { tryParseDates: true });
    expect(df.dtypes.map((dt) => dt.toJSON())).toEqual([
      pl.Datetime("us").toJSON(),
      pl.Float64.toJSON(),
      pl.Float64.toJSON(),
    ]);
  });
  it.each`
    csv                         | nullValues
    ${"a,b,c\nna,b,c\na,na,c"}  | ${"na"}
    ${"a,b,c\nna,b,c\na,n/a,c"} | ${["na", "n/a"]}
    ${"a,b,c\nna,b,c\na,n/a,c"} | ${{ a: "na", b: "n/a" }}
  `("can handle null values", ({ csv, nullValues }) => {
    const df = pl.readCSV(csv, { nullValues });
    expect(df.getColumn("a")[0]).toBeNull();
    expect(df.getColumn("b")[1]).toBeNull();
  });
  test("csv with rowcount", () => {
    const df = pl.readCSV(csvpath, { rowCount: { name: "rc", offset: 11 } });
    const expectedMaxRowCount = df.height + 10;

    const maxRowCount = df.getColumn("rc").max();
    expect(expectedMaxRowCount).toStrictEqual(maxRowCount);
  });
  test("csv files with dtypes", () => {
    const df = pl.readCSV(csvpath, { dtypes: { calories: pl.Utf8 } });
    expect(df.dtypes[1].equals(pl.String)).toBeTruthy();
    const df2 = pl.readCSV(csvpath);
    expect(df2.dtypes[1].equals(pl.Int64)).toBeTruthy();
  });
  test("csv buffer with dtypes", () => {
    const csv = `a,b,c
1,2,x
4,5,y`;
    const df = pl.readCSV(csv);
    expect(df.dtypes[0].equals(pl.Int64)).toBeTruthy();
    const df2 = pl.readCSV(csv, { dtypes: { a: pl.Utf8 } });
    expect(df2.dtypes[0].equals(pl.String)).toBeTruthy();
  });
  it.todo("can read from a stream");
});

describe("read:json", () => {
  it("can read from a json file", () => {
    const df = pl.readJSON(jsonpath);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  it("can specify read options", () => {
    let df = pl.readJSON(jsonpath, { batchSize: 10, inferSchemaLength: 100 });
    expect(df.shape).toEqual({ height: 27, width: 4 });
    df = pl.readJSON(jsonpath, { batchSize: 10, inferSchemaLength: null });
    expect(df.shape).toEqual({ height: 27, width: 4 });
    df = pl.readJSON(jsonpath, { batchSize: 10, inferSchemaLength: 0 });
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  it("can read from a json buffer", () => {
    const json = [
      JSON.stringify({ bar: "1", foo: 1 }),
      JSON.stringify({ bar: "1", foo: 2 }),
      "",
    ].join("\n");
    const df = pl.readJSON(Buffer.from(json), { format: "lines" });

    expect(df.writeJSON({ format: "lines" }).toString().slice(0, 30)).toEqual(
      json.slice(0, 30),
    );
  });
  it("can read null json from buffer", () => {
    const json = [
      JSON.stringify({ bar: 1, foo: "a", nul: null }),
      JSON.stringify({ bar: 2, foo: "b", nul: null }),
      "",
    ].join("\n");
    const df = pl.readJSON(Buffer.from(json), { format: "lines" });
    const actualCols = df.getColumns().map((x) => x.dtype);
    expect(actualCols).toEqual([pl.Int64, pl.Utf8, pl.Null]);
  });
});

describe("scan", () => {
  it("can lazy load (scan) from a csv file", () => {
    const df = pl.scanCSV(csvpath).collectSync();
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  it("can lazy load (scan) from a json file", () => {
    const df = pl.scanJson(singlejsonpath).collectSync();
    expect(df.shape).toEqual({ height: 1, width: 4 });
  });
  it("can lazy load (scan) from a csv file with options", () => {
    const df = pl
      .scanCSV(csvpath, {
        hasHeader: false,
        skipRows: 1,
        nRows: 4,
      })
      .collectSync();

    expect(df.shape).toEqual({ height: 4, width: 4 });
  });
  it("can lazy load (scan) from a ipc file", () => {
    const df = pl.scanCSV(csvpath).collectSync();
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  it("can lazy load (scan) from a csv file with options", () => {
    let df = pl
      .scanCSV(csvpath, {
        hasHeader: false,
        skipRows: 2,
        nRows: 4,
      })
      .collectSync();

    expect(df.shape).toEqual({ height: 4, width: 4 });

    df = pl
      .scanCSV(csvpath, {
        hasHeader: true,
        skipRows: 2,
        nRows: 4,
      })
      .collectSync();

    expect(df.shape).toEqual({ height: 4, width: 4 });
  });

  it("can lazy load empty csv", () => {
    const df = pl.scanCSV(emptycsvpath, { raiseIfEmpty: false }).collectSync();
    expect(df.shape).toEqual({ height: 0, width: 0 });
  });

  it("can lazy load (scan) from a parquet file with options", () => {
    pl.readCSV(csvpath, {
      hasHeader: false,
      skipRows: 2,
      nRows: 4,
    }).writeParquet(parquetpath);

    const df = pl.scanParquet(parquetpath).collectSync();

    expect(df.shape).toEqual({ height: 4, width: 4 });
  });
});

describe("parquet", () => {
  beforeEach(() => {
    pl.readCSV(csvpath).writeParquet(parquetpath);
  });
  afterEach(() => {
    fs.rmSync(parquetpath);
  });

  test("read", () => {
    const df = pl.readParquet(parquetpath);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  test("read:buffer", () => {
    const buff = fs.readFileSync(parquetpath);
    const df = pl.readParquet(buff);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });

  test("read:compressed", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeParquet(parquetpath, { compression: "lz4" });
    const df = pl.readParquet(parquetpath);
    expect(df).toFrameEqual(csvDF);
  });

  test("read:options", () => {
    const df = pl.readParquet(parquetpath, { numRows: 4 });
    expect(df.shape).toEqual({ height: 4, width: 4 });
  });

  test("scan", () => {
    const df = pl.scanParquet(parquetpath).collectSync();
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });

  test("scan:options", () => {
    const df = pl.scanParquet(parquetpath, { nRows: 4 }).collectSync();
    expect(df.shape).toEqual({ height: 4, width: 4 });
  });

  test("writeParquet with decimals", async () => {
    const df = pl.DataFrame([
      pl.Series("decimal", [1n, 2n, 3n], pl.Decimal()),
      pl.Series("u32", [1, 2, 3], pl.UInt32),
      pl.Series("str", ["a", "b", "c"]),
    ]);

    const buf = df.writeParquet();
    const newDF = pl.readParquet(buf);
    expect(newDF).toFrameEqual(df);
  });
});

describe("ipc", () => {
  beforeEach(() => {
    pl.readCSV(csvpath).writeIPC(ipcpath);
  });
  afterEach(() => {
    fs.rmSync(ipcpath);
  });

  test("read", () => {
    const df = pl.readIPC(ipcpath);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  test("read/write:buffer", () => {
    const buff = pl.readCSV(csvpath).writeIPC();
    const df = pl.readIPC(buff);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  test("read:compressed", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPC(ipcpath, { compression: "lz4" });
    const ipcDF = pl.readIPC(ipcpath);
    expect(ipcDF).toFrameEqual(csvDF);
  });

  test("read:options", () => {
    const df = pl.readIPC(ipcpath, { nRows: 4 });
    expect(df.shape).toEqual({ height: 4, width: 4 });
  });

  test("scan", () => {
    const df = pl.scanIPC(ipcpath).collectSync();
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });

  test("scan:options", () => {
    const df = pl.scanIPC(ipcpath, { nRows: 4 }).collectSync();
    expect(df.shape).toEqual({ height: 4, width: 4 });
  });

  test("writeIPC", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPC(ipcpath);
    const ipcDF = pl.readIPC(ipcpath);
    expect(ipcDF).toFrameEqual(csvDF);
  });
});
describe("ipc stream", () => {
  beforeEach(() => {
    pl.readCSV(csvpath).writeIPCStream(ipcpath);
  });
  afterEach(() => {
    fs.rmSync(ipcpath);
  });

  test("read", () => {
    const df = pl.readIPCStream(ipcpath);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  test("read/write:buffer", () => {
    const buff = pl.readCSV(csvpath).writeIPCStream();
    const df = pl.readIPCStream(buff);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  test("read:compressed", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPCStream(ipcpath, { compression: "lz4" });
    const ipcDF = pl.readIPCStream(ipcpath);
    expect(ipcDF).toFrameEqual(csvDF);
  });

  test("read:options", () => {
    const df = pl.readIPCStream(ipcpath, { nRows: 4 });
    expect(df.shape).toEqual({ height: 4, width: 4 });
  });

  test("writeIPCStream", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPCStream(ipcpath);
    const ipcDF = pl.readIPCStream(ipcpath);
    expect(ipcDF).toFrameEqual(csvDF);
  });
});

describe("avro", () => {
  beforeEach(() => {
    pl.readCSV(csvpath).writeAvro(avropath);
  });
  afterEach(() => {
    fs.rmSync(avropath);
  });

  test("round trip", () => {
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: ["a", "b", "c"],
    });
    const buf = expected.writeAvro();
    const actual = pl.readAvro(buf);
    expect(actual).toFrameEqual(expected);
  });
  test("read", () => {
    const df = pl.readAvro(avropath);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });
  test("read:buffer", () => {
    const buff = fs.readFileSync(avropath);
    const df = pl.readAvro(buff);
    expect(df.shape).toEqual({ height: 27, width: 4 });
  });

  test("read:compressed", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeAvro(avropath, { compression: "snappy" });
    const df = pl.readAvro(avropath);
    expect(df).toFrameEqual(csvDF);
  });

  test("read:options", () => {
    const df = pl.readAvro(avropath, { nRows: 4 });
    expect(df.shape).toEqual({ height: 4, width: 4 });
  });
});

describe("stream", () => {
  test("readCSV", async () => {
    const readStream = new Stream.Readable({ read() {} });
    readStream.push("a,b\n");
    readStream.push("1,2\n");
    readStream.push("2,2\n");
    readStream.push("3,2\n");
    readStream.push("4,2\n");
    readStream.push(null);
    const expected = pl.DataFrame({
      a: pl.Series("a", [1, 2, 3, 4], pl.Int64),
      b: pl.Series("b", [2, 2, 2, 2], pl.Int64),
    });
    const df = await pl.readCSVStream(readStream, { chunkSize: 2 });
    expect(df).toFrameEqual(expected);
  });

  test("readCSV:schema mismatch", async () => {
    const readStream = new Stream.Readable({ read() {} });
    readStream.push("a,b,c\n");
    readStream.push("1,2\n");
    readStream.push("2,2\n");
    readStream.push("3,2\n");
    readStream.push("11,1,2,3,4,5,1\n");
    readStream.push("null");
    readStream.push(null);

    const promise = pl.readCSVStream(readStream, {
      inferSchemaLength: 2,
      ignoreErrors: false,
      truncateRaggedLines: false,
    });
    await expect(promise).rejects.toBeDefined();
  });

  test("readJSON", async () => {
    const readStream = new Stream.Readable({ read() {} });
    readStream.push(
      `${JSON.stringify([
        { a: 1, b: 2 },
        { a: 2, b: 2 },
        { a: 3, b: 2 },
        { a: 4, b: 2 },
      ])}`,
    );
    readStream.push(null);

    const expected = pl.DataFrame({
      a: pl.Series("a", [1, 2, 3, 4], pl.Int64),
      b: pl.Series("b", [2, 2, 2, 2], pl.Int64),
    });
    const actual = await pl.readJSONStream(readStream);
    expect(actual).toFrameEqual(expected);
  });

  test("readJSON:lines", async () => {
    const readStream = new Stream.Readable({ read() {} });
    readStream.push(`${JSON.stringify({ a: 1, b: 2 })} \n`);
    readStream.push(`${JSON.stringify({ a: 2, b: 2 })} \n`);
    readStream.push(`${JSON.stringify({ a: 3, b: 2 })} \n`);
    readStream.push(`${JSON.stringify({ a: 4, b: 2 })} \n`);
    readStream.push(null);
    const actual = await pl.readJSONStream(readStream, { format: "lines" });
    const expected = pl.DataFrame({
      a: pl.Series("a", [1, 2, 3, 4], pl.Int64),
      b: pl.Series("b", [2, 2, 2, 2], pl.Int64),
    });
    expect(actual).toFrameEqual(expected);
  });

  test("readJSON:error", async () => {
    const readStream = new Stream.Readable({ read() {} });
    readStream.push(`${JSON.stringify({ a: 1, b: 2 })} \n`);
    readStream.push(`${JSON.stringify({ a: 2, b: 2 })} \n`);
    readStream.push(`${JSON.stringify({ a: 3, b: 2 })} \n`);
    readStream.push("not parseable json ");
    readStream.push(null);
    await expect(
      pl.readJSONStream(readStream, { format: "lines" }),
    ).rejects.toBeDefined();
  });
});
