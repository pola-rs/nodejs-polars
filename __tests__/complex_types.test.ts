import pl from "@polars";

describe("complex types", () => {
  test("nested arrays round trip", () => {
    const arr = [["foo"], [], null];
    const s = pl.Series("", arr);
    const actual = s.toArray();
    expect(actual).toEqual(arr);
  });
  test("struct arrays round trip", () => {
    const arr = [
      { foo: "a", bar: 1 },
      { foo: "b", bar: 2 },
    ];
    const s = pl.Series("", arr);
    const actual = s.toArray();
    expect(actual).toEqual(arr);
  });
});
