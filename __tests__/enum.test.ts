import pl, { DataType } from "../polars";

describe("Enum dtype", () => {
  const levels = pl.Enum(["low", "medium", "high"]);

  it("constructs Enum with readable categories", () => {
    assert.deepStrictEqual(levels.categories, ["low", "medium", "high"]);
    assert.strictEqual(levels.variant, "Enum");
  });

  it("casts and reports dtype through dtypes", () => {
    const df = pl
      .DataFrame({ level: ["high", "low", "medium"] })
      .withColumns(pl.col("level").cast(levels));

    const dtype = df.dtypes[0];
    assert.strictEqual(dtype.variant, "Enum");
    assert.deepStrictEqual((dtype as import("../polars").Enum).categories, [
      "low",
      "medium",
      "high",
    ]);
  });

  it("sorts by category order, not lexical order", () => {
    const df = pl
      .DataFrame({ level: ["high", "low", "medium"] })
      .withColumns(pl.col("level").cast(levels))
      .sort("level");

    assert.deepStrictEqual(df.getColumn("level").toArray(), [
      "low",
      "medium",
      "high",
    ]);
  });

  it("round-trips through Parquet", () => {
    const df = pl
      .DataFrame({ level: ["high", "low", "medium"] })
      .withColumns(pl.col("level").cast(levels));

    const restored = pl.readParquet(df.writeParquet());
    const dtype = restored.dtypes[0];
    assert.strictEqual(dtype.variant, "Enum");
    assert.deepStrictEqual((dtype as import("../polars").Enum).categories, [
      "low",
      "medium",
      "high",
    ]);
  });

  it("round-trips through IPC", () => {
    const df = pl
      .DataFrame({ level: ["high", "low", "medium"] })
      .withColumns(pl.col("level").cast(levels));

    const buff = df.writeIPC();
    const restored = pl.readIPC(buff);
    const dtype = restored.dtypes[0];
    assert.strictEqual(dtype.variant, "Enum");
    assert.deepStrictEqual((dtype as import("../polars").Enum).categories, [
      "low",
      "medium",
      "high",
    ]);
    assert.deepStrictEqual(restored.getColumn("level").toArray(), [
      "high",
      "low",
      "medium",
    ]);
  });

  it("deserializes from native dtype objects", () => {
    const df = pl
      .DataFrame({ level: ["high", "low", "medium"] })
      .withColumns(pl.col("level").cast(levels));
    const raw = (df as any)._df.dtypes()[0];
    const deserialized = DataType.deserialize(raw);
    assert(deserialized.equals(levels));
  });
});
