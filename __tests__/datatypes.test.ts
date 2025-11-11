import { DataType } from "@polars/datatypes";

describe("DataType variants", () => {
  describe("Simple types", () => {
    it("Null should have variant 'Null'", () => {
      const dt = DataType.Null;
      expect(dt.variant).toBe("Null");
    });

    it("Bool should have variant 'Bool'", () => {
      const dt = DataType.Bool;
      expect(dt.variant).toBe("Bool");
    });

    it("Int8 should have variant 'Int8'", () => {
      const dt = DataType.Int8;
      expect(dt.variant).toBe("Int8");
    });

    it("Int16 should have variant 'Int16'", () => {
      const dt = DataType.Int16;
      expect(dt.variant).toBe("Int16");
    });

    it("Int32 should have variant 'Int32'", () => {
      const dt = DataType.Int32;
      expect(dt.variant).toBe("Int32");
    });

    it("Int64 should have variant 'Int64'", () => {
      const dt = DataType.Int64;
      expect(dt.variant).toBe("Int64");
    });

    it("UInt8 should have variant 'UInt8'", () => {
      const dt = DataType.UInt8;
      expect(dt.variant).toBe("UInt8");
    });

    it("UInt16 should have variant 'UInt16'", () => {
      const dt = DataType.UInt16;
      expect(dt.variant).toBe("UInt16");
    });

    it("UInt32 should have variant 'UInt32'", () => {
      const dt = DataType.UInt32;
      expect(dt.variant).toBe("UInt32");
    });

    it("UInt64 should have variant 'UInt64'", () => {
      const dt = DataType.UInt64;
      expect(dt.variant).toBe("UInt64");
    });

    it("Float32 should have variant 'Float32'", () => {
      const dt = DataType.Float32;
      expect(dt.variant).toBe("Float32");
    });

    it("Float64 should have variant 'Float64'", () => {
      const dt = DataType.Float64;
      expect(dt.variant).toBe("Float64");
    });

    it("Date should have variant 'Date'", () => {
      const dt = DataType.Date;
      expect(dt.variant).toBe("Date");
    });

    it("Time should have variant 'Time'", () => {
      const dt = DataType.Time;
      expect(dt.variant).toBe("Time");
    });

    it("Object should have variant 'Object'", () => {
      const dt = DataType.Object;
      expect(dt.variant).toBe("Object");
    });

    it("Utf8 should have variant 'Utf8'", () => {
      const dt = DataType.Utf8;
      expect(dt.variant).toBe("Utf8");
    });

    it("String should have variant 'String'", () => {
      const dt = DataType.String;
      expect(dt.variant).toBe("String");
    });

    it("Categorical should have variant 'Categorical'", () => {
      const dt = DataType.Categorical;
      expect(dt.variant).toBe("Categorical");
    });
  });

  describe("Complex types", () => {
    it("Decimal should have variant 'Decimal'", () => {
      const dt = DataType.Decimal();
      expect(dt.variant).toBe("Decimal");
    });

    it("Decimal with precision and scale should have variant 'Decimal'", () => {
      const dt = DataType.Decimal(10, 2);
      expect(dt.variant).toBe("Decimal");
    });

    it("Datetime should have variant 'Datetime'", () => {
      const dt = DataType.Datetime();
      expect(dt.variant).toBe("Datetime");
    });

    it("Datetime with timeUnit should have variant 'Datetime'", () => {
      const dt = DataType.Datetime("ms");
      expect(dt.variant).toBe("Datetime");
    });

    it("Datetime with timeUnit and timeZone should have variant 'Datetime'", () => {
      const dt = DataType.Datetime("ms", "America/New_York");
      expect(dt.variant).toBe("Datetime");
    });

    it("List should have variant 'List'", () => {
      const dt = DataType.List(DataType.Int32);
      expect(dt.variant).toBe("List");
    });

    it("FixedSizeList should have variant 'FixedSizeList'", () => {
      const dt = DataType.FixedSizeList(DataType.Float64, 5);
      expect(dt.variant).toBe("FixedSizeList");
    });

    it("Struct should have variant 'Struct'", () => {
      const dt = DataType.Struct({
        a: DataType.Int32,
        b: DataType.Utf8,
      });
      expect(dt.variant).toBe("Struct");
    });
  });

  describe("Variant usage in equals", () => {
    it("should use variant for type comparison", () => {
      const dt1 = DataType.Int32;
      const dt2 = DataType.Int32;
      const dt3 = DataType.Int64;

      expect(dt1.equals(dt2)).toBe(true);
      expect(dt1.equals(dt3)).toBe(false);
    });

    it("should use variant for complex type comparison", () => {
      const dt1 = DataType.List(DataType.Int32);
      const dt2 = DataType.List(DataType.Int32);
      const dt3 = DataType.List(DataType.Float64);

      expect(dt1.equals(dt2)).toBe(true);
      expect(dt1.equals(dt3)).toBe(false);
    });
  });
});
