import fs from "node:fs";
import path from "node:path";
import { Stream } from "node:stream";
import pl from "../polars";

// eslint-disable-next-line no-undef
const csvpath = path.resolve(__dirname, "./examples/datasets/foods1.csv");
// eslint-disable-next-line no-undef
const tsvpath = path.resolve(__dirname, "./examples/datasets/data.tsv");
// eslint-disable-next-line no-undef
const emptycsvpath = path.resolve(__dirname, "./examples/datasets/empty.csv");
// eslint-disable-next-line no-undef
const pipecsvpath = path.resolve(__dirname, "./examples/datasets/pipe-eol.csv");
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
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
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
    assert.deepStrictEqual(df.head(1).toString(), expected);
    df = pl.readCSV(csvpath, { inferSchemaLength: null });
    assert.deepStrictEqual(df.head(1).toString(), expected);
  });
  it("can read from a csv file with options", () => {
    const df = pl.readCSV(csvpath, { hasHeader: false, skipRows: 1, nRows: 4 });
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });
  it("can read from a tsv file", () => {
    const df = pl.readCSV(tsvpath, { sep: "\t" });
    assert.deepStrictEqual(df.shape, { height: 2, width: 3 });
  });
  it("can read from a csv string", () => {
    const csvString = "foo,bar,baz\n1,2,3\n4,5,6\n";
    const df = pl.readCSV(csvString);
    assert.deepStrictEqual(
      df.writeCSV().toString().slice(0, 22),
      csvString.slice(0, 22),
    );
  });
  it("can read from a csv file with eolChar", async () => {
    const actual = pl.readCSV(pipecsvpath, { eolChar: "|" });
    const expected = `shape: (2, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i64 ┆ str │
╞═════╪═════╡
│ 1   ┆ foo │
│ 2   ┆ boo │
└─────┴─────┘`;
    assert.deepStrictEqual(actual.toString(), expected);
  });
  it("can read from a csv buffer with newline in the header", () => {
    const csvBuffer = Buffer.from(
      '"name\na","height\nb"\n"John",172.23\n"Anna",1653.34',
    );
    const df = pl.readCSV(csvBuffer, {
      sep: ",",
      hasHeader: false,
      skipRows: 1,
    });
    assert.deepStrictEqual(df.toRecords(), [
      { column_1: "John", column_2: 172.23 },
      { column_1: "Anna", column_2: 1653.34 },
    ]);
  });
  it("can read from a csv buffer", () => {
    const csvBuffer = Buffer.from("foo,bar,baz\n1,2,3\n4,5,6\n", "utf-8");
    const df = pl.readCSV(csvBuffer);
    assert.deepStrictEqual(
      df.writeCSV().toString("utf-8").slice(0, 22),
      csvBuffer.toString("utf-8").slice(0, 22),
    );
  });
  it("can read from a csv buffer quoted", () => {
    let csvBuffer = Buffer.from('a,b,c,d\n1,test,"a,b,c",another test');
    let df = pl.readCSV(csvBuffer);
    const expected = `shape: (1, 4)
┌─────┬──────┬───────┬──────────────┐
│ a   ┆ b    ┆ c     ┆ d            │
│ --- ┆ ---  ┆ ---   ┆ ---          │
│ i64 ┆ str  ┆ str   ┆ str          │
╞═════╪══════╪═══════╪══════════════╡
│ 1   ┆ test ┆ a,b,c ┆ another test │
└─────┴──────┴───────┴──────────────┘`;
    assert.deepStrictEqual(df.toString(), expected);
    csvBuffer = Buffer.from("a,b,c,d\n1,test,|a,b,c|,another test");
    df = pl.readCSV(csvBuffer, { quoteChar: "|" });
    assert.deepStrictEqual(df.toString(), expected);
  });
  it("can read from a csv buffer with options", () => {
    const csvBuffer = Buffer.from("foo,bar,baz\n1,2,3\n4,5,6\n", "utf-8");
    const df = pl.readCSV(csvBuffer, { hasHeader: true, chunkSize: 10 });
    // the newline characters are confusing jest
    assert.deepStrictEqual(
      df.writeCSV().toString("utf-8").slice(0, 22),
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
    assert.deepStrictEqual(df.toString(), expected);
    const f = () => {
      df = pl.readCSV(csvBuffer, { truncateRaggedLines: false });
    };
    assert.throws(f);
  });
  it("can load empty csv", () => {
    const df = pl.readCSV(emptycsvpath, { raiseIfEmpty: false });
    assert.deepStrictEqual(df.shape, { height: 0, width: 0 });
  });
  it("can parse datetimes", () => {
    const csv = `timestamp,open,high
2021-01-01 00:00:00,0.00305500,0.00306000
2021-01-01 00:15:00,0.00298800,0.00300400
2021-01-01 00:30:00,0.00298300,0.00300100
2021-01-01 00:45:00,0.00299400,0.00304000`;
    const df = pl.readCSV(csv, { tryParseDates: true });
    assert.deepStrictEqual(
      df.dtypes.map((dt) => dt.toJSON()),
      [pl.Datetime("us").toJSON(), pl.Float64.toJSON(), pl.Float64.toJSON()],
    );
  });
  it.each`
    csv                         | nullValues
    ${"a,b,c\nna,b,c\na,na,c"}  | ${"na"}
    ${"a,b,c\nna,b,c\na,n/a,c"} | ${["na", "n/a"]}
    ${"a,b,c\nna,b,c\na,n/a,c"} | ${{ a: "na", b: "n/a" }}
  `("can handle null values", ({ csv, nullValues }) => {
    const df = pl.readCSV(csv, { nullValues });
    assert.strictEqual(df.getColumn("a")[0], null);
    assert.strictEqual(df.getColumn("b")[1], null);
  });
  test("csv with rowcount", () => {
    const df = pl.readCSV(csvpath, { rowCount: { name: "rc", offset: 11 } });
    const expectedMaxRowCount = df.height + 10;

    const maxRowCount = df.getColumn("rc").max();
    assert.deepStrictEqual(expectedMaxRowCount, maxRowCount);
  });
  test("csv files with dtypes", () => {
    const df = pl.readCSV(csvpath, { dtypes: { calories: pl.Utf8 } });
    assert.ok(df.dtypes[1].equals(pl.String));
    const df2 = pl.readCSV(csvpath);
    assert.ok(df2.dtypes[1].equals(pl.Int64));
  });
  test("csv buffer with dtypes", () => {
    const csv = `a,b,c
1,2,x
4,5,y`;
    const df = pl.readCSV(csv);
    assert.ok(df.dtypes[0].equals(pl.Int64));
    const df2 = pl.readCSV(csv, { dtypes: { a: pl.Utf8 } });
    assert.ok(df2.dtypes[0].equals(pl.String));
  });
  test("csv with commentPrefix", () => {
    const df = pl.readCSV(csvpath, { commentPrefix: "vegetables" });
    assert.deepStrictEqual(df.shape, { height: 20, width: 4 });
  });
});
describe("read:json", () => {
  it("can read from a json file", () => {
    const df = pl.readJSON(jsonpath);
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  it("can specify read options", () => {
    let df = pl.readJSON(jsonpath, { batchSize: 10, inferSchemaLength: 100 });
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
    df = pl.readJSON(jsonpath, { batchSize: 10, inferSchemaLength: null });
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
    df = pl.readJSON(jsonpath, { batchSize: 10, inferSchemaLength: 0 });
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  it("can read from a json buffer", () => {
    const json = [
      JSON.stringify({ bar: "1", foo: 1 }),
      JSON.stringify({ bar: "1", foo: 2 }),
      "",
    ].join("\n");
    const df = pl.readJSON(Buffer.from(json), { format: "lines" });

    assert.deepStrictEqual(
      df.writeJSON({ format: "lines" }).toString().slice(0, 30),
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
    assert.deepStrictEqual(actualCols, [pl.Int64, pl.String, pl.Null]);
  });

  it("can read from an inline json string body", () => {
    const body = JSON.stringify([
      { foo: 1, bar: "x" },
      { foo: 2, bar: "y" },
    ]);
    const df = pl.readJSON(body);

    assert.deepStrictEqual(df.shape, { height: 2, width: 2 });
    assert.deepStrictEqual(df.getColumn("foo").toArray(), [1, 2]);
  });
});

describe("read:records", () => {
  it("can read records with a provided schema", () => {
    const rows = [{ a: "1", b: "2" }];
    const df = pl.readRecords(rows, { schema: { a: pl.Int64, b: pl.Int64 } });

    assert.deepStrictEqual(
      df.dtypes.map((dt) => dt.toJSON()),
      [pl.Int64.toJSON(), pl.Int64.toJSON()],
    );
    assert.deepStrictEqual(df.toRecords(), [{ a: 1, b: 2 }]);
  });

  it("can read records with inferSchemaLength", () => {
    const rows = [
      { a: "1", b: "x" },
      { a: "2", b: "y" },
    ];
    const df = pl.readRecords(rows, { inferSchemaLength: 1 });

    assert.deepStrictEqual(df.shape, { height: 2, width: 2 });
  });
});

describe("scan", () => {
  it("can lazy load (scan) from a csv file", () => {
    const df = pl.scanCSV(csvpath).collectSync();
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  it("can lazy load (scan) from a json file", () => {
    const df = pl.scanJson(singlejsonpath).collectSync();
    assert.deepStrictEqual(df.shape, { height: 1, width: 4 });
  });
  it("can lazy load (scan) from a csv file with options", () => {
    const df = pl
      .scanCSV(csvpath, {
        hasHeader: false,
        skipRows: 1,
        nRows: 4,
      })
      .collectSync({ engine: "streaming" });

    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });
  it("can lazy load (scan) from a ipc file", () => {
    const df = pl.scanCSV(csvpath).collectSync();
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  it("can lazy load (scan) from a csv file with options", () => {
    let df = pl
      .scanCSV(csvpath, {
        hasHeader: false,
        skipRows: 2,
        nRows: 4,
      })
      .collectSync({ engine: "auto" });

    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });

    df = pl
      .scanCSV(csvpath, {
        hasHeader: true,
        skipRows: 2,
        nRows: 4,
      })
      .collectSync();

    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });

  it("can lazy load empty csv", () => {
    const df = pl.scanCSV(emptycsvpath, { raiseIfEmpty: false }).collectSync();
    assert.deepStrictEqual(df.shape, { height: 0, width: 0 });
  });

  it("can lazy load (scan) from a parquet file with options", () => {
    pl.readCSV(csvpath, {
      hasHeader: false,
      skipRows: 2,
      nRows: 4,
    }).writeParquet(parquetpath);

    const df = pl.scanParquet(parquetpath).collectSync();

    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });
  it("can lazy load (scan) from a csv file with eolChar", async () => {
    const actual = pl.scanCSV(pipecsvpath, { eolChar: "|" }).collectSync();
    const expected = `shape: (2, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i64 ┆ str │
╞═════╪═════╡
│ 1   ┆ foo │
│ 2   ┆ boo │
└─────┴─────┘`;
    assert.deepStrictEqual(actual.toString(), expected);
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
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  test("read:buffer", () => {
    const buff = fs.readFileSync(parquetpath);
    const df = pl.readParquet(buff);
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });

  test("read:compressed", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeParquet(parquetpath, { compression: "lz4" });
    const df = pl.readParquet(parquetpath);
    assertFrameEqual(df, csvDF);
  });

  test("read:options", () => {
    const df = pl.readParquet(parquetpath, { numRows: 4 });
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });

  test("read:options:projection by numeric indices", () => {
    const df = pl.readParquet(parquetpath, { columns: [0, 2] });
    assert.deepStrictEqual(df.shape, { height: 27, width: 2 });
  });

  test("scan", () => {
    const df = pl.scanParquet(parquetpath).collectSync();
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });

  test("scan:options", () => {
    const df = pl.scanParquet(parquetpath, { nRows: 4 }).collectSync();
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });

  test("writeParquet with decimals", async () => {
    const df = pl.DataFrame([
      pl.Series("decimal", [1n, 2n, 3n], pl.Decimal(2, 0)),
      pl.Series("u32", [1, 2, 3], pl.UInt32),
      pl.Series("str", ["a", "b", "c"]),
    ]);
    const buf = df.writeParquet();
    const newDF = pl.readParquet(buf);
    assertFrameEqual(newDF, df);
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
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  test("read/write:buffer", () => {
    const buff = pl.readCSV(csvpath).writeIPC();
    const df = pl.readIPC(buff);
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  test("read:compressed", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPC(ipcpath, { compression: "lz4" });
    const ipcDF = pl.readIPC(ipcpath);
    assertFrameEqual(ipcDF, csvDF);
  });

  test("read:options", () => {
    const df = pl.readIPC(ipcpath, { nRows: 4 });
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });

  test("scan", () => {
    const df = pl.scanIPC(ipcpath).collectSync();
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });

  test("scan:options", () => {
    const df = pl.scanIPC(ipcpath, { nRows: 4 }).collectSync();
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });

  test("writeIPC", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPC(ipcpath);
    const ipcDF = pl.readIPC(ipcpath);
    assertFrameEqual(ipcDF, csvDF);
  });

  test("readIPC:datetime:microseconds", () => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const path = require("node:path");
    const tmpPath = path.resolve(__dirname, "./test_datetime_us.ipc");

    try {
      const dt1 = new Date("2024-02-28T14:53:00.000Z");
      const dt2 = new Date("2025-01-03T09:30:00.000Z");
      const dt3 = new Date("2024-12-31T23:59:59.999Z");

      const df = pl.DataFrame({
        id: [1, 2, 3],
        datetime_us: pl.Series(
          "datetime_us",
          [dt1, dt2, dt3],
          pl.Datetime("us"),
        ),
        date_field: pl.Series("date_field", [dt1, dt2, dt3], pl.Date),
      });

      df.writeIPC(tmpPath);
      const dfRead = pl.readIPC(tmpPath);

      const variants = dfRead.dtypes.map((dt) => dt.variant);
      assert.match(variants[0], /Int64|Float64/);
      assert.strictEqual(variants[1], "Datetime");
      assert.strictEqual(variants[2], "Date");

      const records = dfRead.toRecords() as unknown as Array<{
        id: number;
        datetime_us: Date;
        date_field: Date;
      }>;

      assert.strictEqual(records.length, 3);

      const date1 = records[0].datetime_us;
      const date2 = records[1].datetime_us;
      const date3 = records[2].datetime_us;

      assert.ok(Math.abs(date1.getTime() - dt1.getTime()) < 1000);
      assert.ok(Math.abs(date2.getTime() - dt2.getTime()) < 1000);
      assert.ok(Math.abs(date3.getTime() - dt3.getTime()) < 1000);
    } finally {
      if (fs.existsSync(tmpPath)) {
        fs.rmSync(tmpPath);
      }
    }
  });
  test("readIPC:date", () => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const path = require("node:path");
    const tmpPath = path.resolve(__dirname, "./test_date.ipc");

    try {
      const dt1 = new Date("2024-02-28T14:53:00.000Z");
      const dt2 = new Date("2025-01-03T09:30:00.000Z");
      const dt3 = new Date("2024-12-31T23:59:59.999Z");

      const df = pl.DataFrame({
        id: [1, 2, 3],
        date_field: pl.Series("date_field", [dt1, dt2, dt3], pl.Date),
      });

      df.writeIPC(tmpPath);
      const dfRead = pl.readIPC(tmpPath);

      const variants = dfRead.dtypes.map((dt) => dt.variant);
      assert.match(variants[0], /Int64|Float64/);
      assert.strictEqual(variants[1], "Date");

      const records = dfRead.toRecords() as unknown as Array<{
        id: number;
        date_field: Date;
      }>;

      assert.strictEqual(records.length, 3);

      const date1 = records[0].date_field;
      const date2 = records[1].date_field;
      const date3 = records[2].date_field;

      const expected1 = Date.UTC(
        dt1.getUTCFullYear(),
        dt1.getUTCMonth(),
        dt1.getUTCDate(),
      );
      const expected2 = Date.UTC(
        dt2.getUTCFullYear(),
        dt2.getUTCMonth(),
        dt2.getUTCDate(),
      );
      const expected3 = Date.UTC(
        dt3.getUTCFullYear(),
        dt3.getUTCMonth(),
        dt3.getUTCDate(),
      );

      assert.strictEqual(date1.getTime(), expected1);
      assert.strictEqual(date2.getTime(), expected2);
      assert.strictEqual(date3.getTime(), expected3);
    } finally {
      if (fs.existsSync(tmpPath)) {
        fs.rmSync(tmpPath);
      }
    }
  });
  test("readIPC:datetime:microseconds:fromPythonIPC", () => {
    const path = require("node:path");
    const pythonIpcPath = path.resolve(
      __dirname,
      "./examples/datasets/test_datetime_us_python.ipc",
    );

    if (!fs.existsSync(pythonIpcPath)) {
      console.warn(
        `Skipping test: ${pythonIpcPath} not found. Run create_test_datetime.py first.`,
      );
      return;
    }

    const dfRead = pl.readIPC(pythonIpcPath);

    const variants = dfRead.dtypes.map((dt) => dt.variant);
    assert.match(variants[0], /Int64|Float64/);
    assert.strictEqual(variants[1], "Datetime");
    assert.strictEqual(variants[2], "Date");

    const records = dfRead.toRecords() as unknown as Array<{
      id: number;
      datetime_us: Date;
      date_field: Date;
    }>;

    assert.strictEqual(records.length, 3);

    const date1 = records[0].datetime_us;
    const date2 = records[1].datetime_us;
    const date3 = records[2].datetime_us;

    const expected1 = new Date("2024-02-28T14:53:00.000Z").getTime();
    const expected2 = new Date("2025-01-03T09:30:00.000Z").getTime();
    const expected3 = new Date("2024-12-31T23:59:59.999Z").getTime();

    assert.ok(Math.abs(date1.getTime() - expected1) < 1000);
    assert.ok(Math.abs(date2.getTime() - expected2) < 1000);
    assert.ok(Math.abs(date3.getTime() - expected3) < 1000);
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
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  test("read/write:buffer", () => {
    const buff = pl.readCSV(csvpath).writeIPCStream();
    const df = pl.readIPCStream(buff);
    assert.deepStrictEqual(df.shape, { height: 27, width: 4 });
  });
  test("read:compressed", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPCStream(ipcpath, { compression: "lz4" });
    const ipcDF = pl.readIPCStream(ipcpath);
    assertFrameEqual(ipcDF, csvDF);
  });

  test("read:options", () => {
    const df = pl.readIPCStream(ipcpath, { nRows: 4 });
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });

  test("writeIPCStream", () => {
    const csvDF = pl.readCSV(csvpath);
    csvDF.writeIPCStream(ipcpath);
    const ipcDF = pl.readIPCStream(ipcpath);
    assertFrameEqual(ipcDF, csvDF);
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
    assertFrameEqual(actual, expected);
  });
  test("read:avro", () => {
    const df = pl.readAvro(avropath, { nRows: 4 });
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });
  test("read:avro:buffer", () => {
    const buff = fs.readFileSync(avropath);
    const df = pl.readAvro(buff, { nRows: 4 });
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
  });

  test("read:avro:compressed", () => {
    const csvDF = pl.readCSV(csvpath, { nRows: 4 });
    csvDF.writeAvro(avropath, { compression: "snappy" });
    const df = pl.readAvro(avropath, { nRows: 4 });
    assertFrameEqual(df, csvDF);
  });

  test("read:options", () => {
    const df = pl.readAvro(avropath, { nRows: 4 });
    assert.deepStrictEqual(df.shape, { height: 4, width: 4 });
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
    assertFrameEqual(df, expected);
  });

  test("readCSV:endRows early-aborts after first emitted batch", async () => {
    const readStream = new Stream.Readable({ read() {} });
    readStream.push("a,b\n1,2\n2,3\n3,4\n4,5\n");
    readStream.push(null);

    const df = await pl.readCSVStream(readStream, {
      batchSize: 2,
      endRows: 0,
    });

    assert.deepStrictEqual(df.shape, { height: 1, width: 2 });
    assert.deepStrictEqual(df.toRecords(), [{ a: 1, b: 2 }]);
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
    await assert.rejects(promise);
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
    assertFrameEqual(actual, expected);
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
    assertFrameEqual(actual, expected);
  });

  test("readJSON:error", async () => {
    const readStream = new Stream.Readable({ read() {} });
    readStream.push(`${JSON.stringify({ a: 1, b: 2 })} \n`);
    readStream.push(`${JSON.stringify({ a: 2, b: 2 })} \n`);
    readStream.push(`${JSON.stringify({ a: 3, b: 2 })} \n`);
    readStream.push("not parseable json ");
    readStream.push(null);
    await assert.rejects(pl.readJSONStream(readStream, { format: "lines" }));
  });
});

describe("io input validation", () => {
  test("readCSV throws for non string/buffer input", () => {
    assert.throws(
      () => pl.readCSV(123 as unknown as string),
      /must supply either a path or body/,
    );
  });

  test("readJSON throws for non string/buffer input", () => {
    assert.throws(
      () => pl.readJSON(123 as unknown as string),
      /must supply either a path or body/,
    );
  });

  test("readParquet throws for non string/buffer input", () => {
    assert.throws(
      () => pl.readParquet(123 as unknown as string),
      /must supply either a path or body/,
    );
  });

  test("readAvro throws for non string/buffer input", () => {
    assert.throws(
      () => pl.readAvro(123 as unknown as string),
      /must supply either a path or body/,
    );
  });

  test("readIPC throws for non string/buffer input", () => {
    assert.throws(
      () => pl.readIPC(123 as unknown as string),
      /must supply either a path or body/,
    );
  });

  test("readIPCStream throws for non string/buffer input", () => {
    assert.throws(
      () => pl.readIPCStream(123 as unknown as string),
      /must supply either a path or body/,
    );
  });

  test("inline binary-format readers attempt to parse string bodies", () => {
    assert.throws(() => pl.readParquet("not parquet content"));
    assert.throws(() => pl.readAvro("not avro content"));
    assert.throws(() => pl.readIPC("not ipc content"));
    assert.throws(() => pl.readIPCStream("not ipc stream content"));
  });
});
