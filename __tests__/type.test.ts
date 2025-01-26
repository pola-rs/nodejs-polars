import { expectType } from "ts-expect";

import pl from "@polars/index";

describe("type tests", () => {
  it("types the DataFrame from the input", () => {
    const df = pl.DataFrame({
      bools: [false, true, false],
      bools_nulls: [null, true, false],
      int: [1, 2, 3],
      int_nulls: [1, null, 3],
      bigint: [1n, 2n, 3n],
      bigint_nulls: [1n, null, 3n],
      floats: [1.0, 2.0, 3.0],
      floats_nulls: [1.0, null, 3.0],
      strings: ["foo", "bar", "ham"],
      strings_nulls: ["foo", null, "ham"],
      date: [new Date(), new Date(), new Date()],
      datetime: [13241324, 12341256, 12341234],
    });

    expectType<
      pl.DataFrame<{
        bools: pl.Bool;
        bools_nulls: pl.Bool;
        int: pl.Float64;
        int_nulls: pl.Float64;
        bigint: pl.UInt64;
        bigint_nulls: pl.UInt64;
        floats: pl.Float64;
        floats_nulls: pl.Float64;
        strings: pl.String;
        strings_nulls: pl.String;
        date: pl.Datetime;
        datetime: pl.Float64;
      }>
    >(df);
  });

  it("produces the expected types", () => {
    const a = pl.DataFrame({
      id: [1n, 2n],
      age: [18n, 19n],
      name: ["A", "B"],
    });
    const b = pl.DataFrame({
      id: [1n, 2n],
      age: [18n, 19n],
      fl: [1.3, 3.4],
    });
    expectType<pl.Series<pl.UInt64, "age">>(a.getColumn("age"));
    expectType<
      (
        | pl.Series<pl.UInt64, "id">
        | pl.Series<pl.UInt64, "age">
        | pl.Series<pl.String, "name">
      )[]
    >(a.getColumns());
    expectType<pl.DataFrame<{ id: pl.UInt64; age: pl.UInt64 }>>(a.drop("name"));
    expectType<pl.DataFrame<{ id: pl.UInt64 }>>(a.drop(["name", "age"]));
    expectType<pl.DataFrame<{ id: pl.UInt64 }>>(a.drop("name", "age"));
    expectType<
      pl.DataFrame<{
        id: pl.UInt64;
        age: pl.UInt64;
        age_right: pl.UInt64;
        name: pl.String;
        fl: pl.Float64;
      }>
    >(a.join(b, { on: ["id"] }));
    expectType<
      pl.DataFrame<{
        id: pl.UInt64;
        age: pl.UInt64;
        ageRight: pl.UInt64;
        name: pl.String;
        fl: pl.Float64;
      }>
    >(a.join(b, { on: ["id"], suffix: "Right" }));
    expectType<
      pl.DataFrame<{
        id: pl.UInt64;
        id_right: pl.UInt64;
        age: pl.UInt64;
        name: pl.String;
        fl: pl.Float64;
      }>
    >(a.join(b, { leftOn: "id", rightOn: ["age"] }));
    expectType<
      pl.DataFrame<{
        id: pl.UInt64;
        id_right: pl.UInt64;
        age: pl.UInt64;
        age_right: pl.UInt64;
        name: pl.String;
        fl: pl.Float64;
      }>
    >(a.join(b, { how: "cross" }));
  });

  it("folds", () => {
    const df = pl.DataFrame({
      a: [2, 1, 3],
      b: [1, 2, 3],
      c: [1.0, 2.0, 3.0],
    });
    expectType<pl.Series<pl.Float64, string>>(df.fold((s1, s2) => s1.plus(s2)));
  });
});
