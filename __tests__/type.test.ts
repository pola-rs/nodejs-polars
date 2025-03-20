import { expectType } from "ts-expect";

import {
  DataFrame,
  type Float64,
  type Int64,
  type String as PlString,
  type Series,
} from "../polars";

describe("type tests", () => {
  it("is cleaned up later", () => {
    const a = null as unknown as DataFrame<{
      id: Int64;
      age: Int64;
      name: PlString;
    }>;
    const b = null as unknown as DataFrame<{
      id: Int64;
      age: Int64;
      fl: Float64;
    }>;
    expectType<Series<Int64, "age">>(a.getColumn("age"));
    expectType<
      (Series<Int64, "id"> | Series<Int64, "age"> | Series<PlString, "name">)[]
    >(a.getColumns());
    expectType<DataFrame<{ id: Int64; age: Int64 }>>(a.drop("name"));
    expectType<DataFrame<{ id: Int64 }>>(a.drop(["name", "age"]));
    expectType<DataFrame<{ id: Int64 }>>(a.drop("name", "age"));
    // expectType<Series<Int64, "age">>(a.age);
    expectType<
      DataFrame<{
        id: Int64;
        age: Int64;
        age_right: Int64;
        name: PlString;
        fl: Float64;
      }>
    >(a.join(b, { on: ["id"] }));
    expectType<
      DataFrame<{
        id: Int64;
        age: Int64;
        ageRight: Int64;
        name: PlString;
        fl: Float64;
      }>
    >(a.join(b, { on: ["id"], suffix: "Right" }));
    expectType<
      DataFrame<{
        id: Int64;
        id_right: Int64;
        age: Int64;
        name: PlString;
        fl: Float64;
      }>
    >(a.join(b, { leftOn: "id", rightOn: ["age"] }));
    expectType<
      DataFrame<{
        id: Int64;
        id_right: Int64;
        age: Int64;
        age_right: Int64;
        name: PlString;
        fl: Float64;
      }>
    >(a.join(b, { how: "cross" }));
  });

  it("folds", () => {
    const df = DataFrame({
      a: [2, 1, 3],
      b: [1, 2, 3],
      c: [1.0, 2.0, 3.0],
    });
    expectType<Series<Float64, string>>(df.fold((s1, s2) => s1.plus(s2)));
  });
});
