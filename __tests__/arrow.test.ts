import { Table, tableFromArrays, Vector, vectorFromArray } from "apache-arrow";
import pl from "../polars";

describe("apache-arrow integration", () => {
  describe("DataFrame.toArrow", () => {
    it("should convert a DataFrame to an Apache Arrow Table", () => {
      const df = pl.DataFrame({ a: [1, 2, 3], b: ["x", "y", "z"] });
      const table = df.toArrow();
      assert.ok(table instanceof Table);
      assert.strictEqual(table.numRows, 3);
      assert.deepStrictEqual(
        table.schema.fields.map((f) => f.name),
        ["a", "b"],
      );
    });

    it("should preserve numeric column values", () => {
      const df = pl.DataFrame({ nums: [10, 20, 30] });
      const table = df.toArrow();
      const col = table.getChild("nums");
      assert.deepStrictEqual([...(col ?? [])], [10, 20, 30]);
    });

    it("should preserve string column values", () => {
      const df = pl.DataFrame({ names: ["alice", "bob", "charlie"] });
      const table = df.toArrow();
      const col = table.getChild("names");
      assert.deepStrictEqual([...(col ?? [])], ["alice", "bob", "charlie"]);
    });

    it("should handle multiple dtypes", () => {
      const df = pl.DataFrame({
        ints: [1, 2, 3],
        floats: [1.1, 2.2, 3.3],
        strs: ["a", "b", "c"],
        bools: [true, false, true],
      });
      const table = df.toArrow();
      assert.strictEqual(table.numCols, 4);
      assert.strictEqual(table.numRows, 3);
    });

    it("should round-trip through fromArrow", () => {
      const original = pl.DataFrame({ a: [1, 2, 3], b: ["x", "y", "z"] });
      const table = original.toArrow();
      const roundTripped = pl.fromArrow(table);
      assert.deepStrictEqual(roundTripped.shape, original.shape);
      assert.deepStrictEqual(roundTripped.columns, original.columns);
    });
  });

  describe("pl.fromArrow", () => {
    it("should create a DataFrame from an Apache Arrow Table", () => {
      const table = tableFromArrays({
        col1: [1, 2, 3],
        col2: ["a", "b", "c"],
      });
      const df = pl.fromArrow(table);
      assert.deepStrictEqual(df.shape, { height: 3, width: 2 });
      assert.deepStrictEqual(df.columns, ["col1", "col2"]);
    });

    it("should preserve values from Arrow Table", () => {
      const table = tableFromArrays({
        nums: new Float64Array([10, 20, 30]),
      });
      const df = pl.fromArrow(table);
      assert.deepStrictEqual(df.getColumn("nums").toArray(), [10, 20, 30]);
    });

    it("should handle Arrow Table created from IPC buffer", () => {
      const original = tableFromArrays({ x: [1, 2], y: ["a", "b"] });
      const tableFromBuffer = pl.fromArrow(original);
      assert.deepStrictEqual(tableFromBuffer.shape, { height: 2, width: 2 });
    });
  });

  describe("Series.toArrow", () => {
    it("should convert a numeric Series to an Apache Arrow Vector", () => {
      const s = pl.Series("nums", [1, 2, 3]);
      const vec = s.toArrow();
      assert.ok(vec instanceof Vector);
      assert.deepStrictEqual([...vec], [1, 2, 3]);
    });

    it("should convert a string Series to an Apache Arrow Vector", () => {
      const s = pl.Series("strs", ["a", "b", "c"]);
      const vec = s.toArrow();
      assert.ok(vec instanceof Vector);
      assert.deepStrictEqual([...vec], ["a", "b", "c"]);
    });

    it("should round-trip through Series.fromArrow", () => {
      const original = pl.Series("test", [10.0, 20.0, 30.0]);
      const vec = original.toArrow();
      const roundTripped = pl.Series.fromArrow("test", vec);
      assert.deepStrictEqual(roundTripped.toArray(), original.toArray());
    });
  });

  describe("Series.fromArrow", () => {
    it("should create a Series from an Apache Arrow Vector with name", () => {
      const vec = vectorFromArray([4.0, 5.0, 6.0]);
      const s = pl.Series.fromArrow("myCol", vec);
      assert.strictEqual(s.name, "myCol");
      assert.deepStrictEqual(s.toArray(), [4, 5, 6]);
    });

    it("should create a Series from an Apache Arrow Vector without name (defaults to 'value')", () => {
      const vec = vectorFromArray([7.0, 8.0, 9.0]);
      const s = pl.Series.fromArrow(vec);
      assert.strictEqual(s.name, "value");
      assert.deepStrictEqual(s.toArray(), [7, 8, 9]);
    });

    it("should handle string vectors", () => {
      const vec = vectorFromArray(["x", "y", "z"]);
      const s = pl.Series.fromArrow("letters", vec);
      assert.strictEqual(s.name, "letters");
      assert.deepStrictEqual(s.toArray(), ["x", "y", "z"]);
    });
  });
});
