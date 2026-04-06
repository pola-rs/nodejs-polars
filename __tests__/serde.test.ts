import path from "node:path";
import pl from "../polars";

// eslint-disable-next-line no-undef
const csvpath = path.resolve(__dirname, "./examples/datasets/foods1.csv");
describe("serde", () => {
  test("lazyframe:json", () => {
    const df = pl.scanCSV(csvpath);
    const buf = df.serialize("json");
    const deserde = pl.LazyDataFrame.deserialize(buf, "json");
    const expected = df.collectSync();
    const actual = deserde.collectSync();
    assertFrameEqual(actual, expected);
  });

  test("lazyframe:bincode", () => {
    const df = pl.scanCSV(csvpath);
    const buf = df.serialize("bincode");
    const deserde = pl.LazyDataFrame.deserialize(buf, "bincode");
    const expected = df.collectSync();
    const actual = deserde.collectSync();
    assertFrameEqual(actual, expected);
  });
  test("expr:json", () => {
    const expr = pl.cols("foo", "bar").sortBy("other");

    const buf = expr.serialize("json");
    const actual = pl.Expr.deserialize(buf, "json");

    assert.deepStrictEqual(actual.toString(), expr.toString());
  });
  test("expr:bincode", () => {
    const expr = pl.cols("foo", "bar").sortBy("other");
    const buf = expr.serialize("bincode");
    const actual = pl.Expr.deserialize(buf, "bincode");

    assert.deepStrictEqual(actual.toString(), expr.toString());
  });
  test("dataframe:json", () => {
    const df = pl.DataFrame({
      foo: [1, 2],
      bar: [2, 3],
    });
    const buf = df.serialize("json");
    const expected = pl.DataFrame.deserialize(buf, "json");
    assertFrameEqual(df, expected);
  });
  test("dataframe:bincode", () => {
    const df = pl.DataFrame({
      foo: [1, 2],
      bar: [2, 3],
    });
    const buf = df.serialize("bincode");
    const expected = pl.DataFrame.deserialize(buf, "bincode");
    assertFrameEqual(df, expected);
  });

  test("dataframe:unsupported", () => {
    const df = pl.DataFrame({
      foo: [1, 2],
      bar: [2, 3],
    });
    const ser = () => df.serialize("yaml" as any);
    const buf = df.serialize("bincode");
    const de = () => pl.DataFrame.deserialize(buf, "yaml" as any);
    const mismatch = () => pl.DataFrame.deserialize(buf, "json");
    assert.throws(ser);
    assert.throws(de);
    assert.throws(mismatch);
  });
  test("series:json", () => {
    const s = pl.Series("foo", [1, 2, 3]);

    const buf = s.serialize("json");
    const expected = pl.Series.deserialize(buf, "json");
    assertSeriesEqual(s, expected);
  });
  test("series:bincode", () => {
    const s = pl.Series("foo", [1, 2, 3]);

    const buf = s.serialize("bincode");
    const expected = pl.Series.deserialize(buf, "bincode");
    assertSeriesEqual(s, expected);
  });

  test("series:unsupported", () => {
    const s = pl.Series("foo", [1, 2, 3]);
    const ser = () => s.serialize("yaml" as any);
    const buf = s.serialize("bincode");
    const de = () => pl.Series.deserialize(buf, "yaml" as any);
    const mismatch = () => pl.Series.deserialize(buf, "json");
    assert.throws(ser);
    assert.throws(de);
    assert.throws(mismatch);
  });
});
