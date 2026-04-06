import { DataType, Field } from "../polars/datatypes";

describe("DataType variants", () => {
  describe("Simple types", () => {
    it("Null should have variant 'Null'", () => {
      const dt = DataType.Null;
      assert.strictEqual(dt.variant, "Null");
    });

    it("Bool should have variant 'Bool'", () => {
      const dt = DataType.Bool;
      assert.strictEqual(dt.variant, "Bool");
    });

    it("Int8 should have variant 'Int8'", () => {
      const dt = DataType.Int8;
      assert.strictEqual(dt.variant, "Int8");
    });

    it("Int16 should have variant 'Int16'", () => {
      const dt = DataType.Int16;
      assert.strictEqual(dt.variant, "Int16");
    });

    it("Int32 should have variant 'Int32'", () => {
      const dt = DataType.Int32;
      assert.strictEqual(dt.variant, "Int32");
    });

    it("Int64 should have variant 'Int64'", () => {
      const dt = DataType.Int64;
      assert.strictEqual(dt.variant, "Int64");
    });

    it("UInt8 should have variant 'UInt8'", () => {
      const dt = DataType.UInt8;
      assert.strictEqual(dt.variant, "UInt8");
    });

    it("UInt16 should have variant 'UInt16'", () => {
      const dt = DataType.UInt16;
      assert.strictEqual(dt.variant, "UInt16");
    });

    it("UInt32 should have variant 'UInt32'", () => {
      const dt = DataType.UInt32;
      assert.strictEqual(dt.variant, "UInt32");
    });

    it("UInt64 should have variant 'UInt64'", () => {
      const dt = DataType.UInt64;
      assert.strictEqual(dt.variant, "UInt64");
    });

    it("Float32 should have variant 'Float32'", () => {
      const dt = DataType.Float32;
      assert.strictEqual(dt.variant, "Float32");
    });

    it("Float64 should have variant 'Float64'", () => {
      const dt = DataType.Float64;
      assert.strictEqual(dt.variant, "Float64");
    });

    it("Date should have variant 'Date'", () => {
      const dt = DataType.Date;
      assert.strictEqual(dt.variant, "Date");
    });

    it("Time should have variant 'Time'", () => {
      const dt = DataType.Time;
      assert.strictEqual(dt.variant, "Time");
    });

    it("Object should have variant 'Object'", () => {
      const dt = DataType.Object;
      assert.strictEqual(dt.variant, "Object");
    });

    it("Utf8 should have variant 'Utf8'", () => {
      const dt = DataType.Utf8;
      assert.strictEqual(dt.variant, "Utf8");
    });

    it("String should have variant 'String'", () => {
      const dt = DataType.String;
      assert.strictEqual(dt.variant, "String");
    });

    it("Categorical should have variant 'Categorical'", () => {
      const dt = DataType.Categorical;
      assert.strictEqual(dt.variant, "Categorical");
    });
  });

  describe("Complex types", () => {
    it("Decimal should have variant 'Decimal'", () => {
      const dt = DataType.Decimal();
      assert.strictEqual(dt.variant, "Decimal");
    });

    it("Decimal with precision and scale should have variant 'Decimal'", () => {
      const dt = DataType.Decimal(10, 2);
      assert.strictEqual(dt.variant, "Decimal");
    });

    it("Datetime should have variant 'Datetime'", () => {
      const dt = DataType.Datetime();
      assert.strictEqual(dt.variant, "Datetime");
    });

    it("Datetime with timeUnit should have variant 'Datetime'", () => {
      const dt = DataType.Datetime("ms");
      assert.strictEqual(dt.variant, "Datetime");
    });

    it("Datetime with timeUnit and timeZone should have variant 'Datetime'", () => {
      const dt = DataType.Datetime("ms", "America/New_York");
      assert.strictEqual(dt.variant, "Datetime");
    });

    it("List should have variant 'List'", () => {
      const dt = DataType.List(DataType.Int32);
      assert.strictEqual(dt.variant, "List");
    });

    it("FixedSizeList should have variant 'FixedSizeList'", () => {
      const dt = DataType.FixedSizeList(DataType.Float64, 5);
      assert.strictEqual(dt.variant, "FixedSizeList");
    });

    it("Struct should have variant 'Struct'", () => {
      const dt = DataType.Struct({
        a: DataType.Int32,
        b: DataType.Utf8,
      });
      assert.strictEqual(dt.variant, "Struct");
    });
  });

  describe("Variant usage in equals", () => {
    it("should use variant for type comparison", () => {
      const dt1 = DataType.Int32;
      const dt2 = DataType.Int32;
      const dt3 = DataType.Int64;

      assert.strictEqual(dt1.equals(dt2), true);
      assert.strictEqual(dt1.equals(dt3), false);
    });

    it("should use variant for complex type comparison", () => {
      const dt1 = DataType.List(DataType.Int32);
      const dt2 = DataType.List(DataType.Int32);
      const dt3 = DataType.List(DataType.Float64);

      assert.strictEqual(dt1.equals(dt2), true);
      assert.strictEqual(dt1.equals(dt3), false);
    });
  });
});

describe("DataType behavior", () => {
  it("toString and inspect include nested details", () => {
    const dt = DataType.List(DataType.Int32);

    assert.strictEqual(dt.toString(), "DataType(List(DataType(Int32)))");
    assert.strictEqual(
      dt[Symbol.for("nodejs.util.inspect.custom")](),
      "DataType(List(DataType(Int32)))",
    );
  });

  it("toJSON serializes simple and nested dtypes", () => {
    const simple = DataType.Int32;
    const decimal = DataType.Decimal(10, 2);
    const fixed = DataType.FixedSizeList(DataType.Int16, 3);

    assert.deepStrictEqual(simple.toJSON(), { DataType: "Int32" });
    assert.deepStrictEqual(decimal.toJSON(), {
      DataType: { Decimal: { precision: 10, scale: 2 } },
    });
    assert.deepStrictEqual(fixed.toJSON(), {
      DataType: {
        FixedSizeList: {
          type: { DataType: "Int16" },
          size: 3,
        },
      },
    });
  });

  it("asFixedSizeList returns fixed list or null", () => {
    const fixed = DataType.FixedSizeList(DataType.UInt8, 4);
    const list = DataType.List(DataType.UInt8);

    assert.strictEqual(fixed.asFixedSizeList(), fixed);
    assert.strictEqual(list.asFixedSizeList(), null);
  });

  it("Struct accepts object and Field[] constructors equivalently", () => {
    const fromObject = DataType.Struct({
      a: DataType.Int32,
      b: DataType.Utf8,
    });
    const fromFields = DataType.Struct([
      new Field("a", DataType.Int32),
      new Field("b", DataType.Utf8),
    ]);

    assert.strictEqual(fromObject.equals(fromFields), true);
    const actual = fromFields.toJSON() as any;
    assert.strictEqual(actual.DataType.Struct.length, 2);
    assert.deepStrictEqual(
      actual.DataType.Struct[0],
      new Field("a", DataType.Int32),
    );
    assert.deepStrictEqual(
      actual.DataType.Struct[1],
      new Field("b", DataType.Utf8),
    );
  });

  it("deserialize handles string/simple and complex nested variants", () => {
    assert.deepStrictEqual(DataType.deserialize("Int32"), DataType.Int32);

    const datetime = DataType.deserialize({
      variant: "Datetime",
      inner: ["us", "UTC"],
    });
    assert.strictEqual(datetime.equals(DataType.Datetime("us", "UTC")), true);

    const list = DataType.deserialize({
      variant: "List",
      inner: ["Int64"],
    });
    assert.strictEqual(list.equals(DataType.List(DataType.Int64)), true);

    const fixed = DataType.deserialize({
      variant: "FixedSizeList",
      inner: ["Float32", 2],
    });
    assert.strictEqual(
      fixed.equals(DataType.FixedSizeList(DataType.Float32, 2)),
      true,
    );

    const struct = DataType.deserialize({
      variant: "Struct",
      inner: [
        [
          { name: "id", dtype: "UInt32" },
          { name: "label", dtype: "Utf8" },
        ],
      ],
    });
    assert.strictEqual(
      struct.equals(
        DataType.Struct({
          id: DataType.UInt32,
          label: DataType.Utf8,
        }),
      ),
      true,
    );
  });
});
