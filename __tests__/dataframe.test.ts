import fs from "node:fs";
import { Stream } from "node:stream";
/* eslint-disable newline-per-chained-call */
import pl from "@polars";

describe("dataframe", () => {
  const df = pl.DataFrame([
    pl.Series("foo", [1, 2, 9], pl.Int16),
    pl.Series("bar", [6, 2, 8], pl.Int16),
  ]);
  test("dtypes", () => {
    const expected = [pl.Float64, pl.String];
    const actual = pl.DataFrame({ a: [1, 2, 3], b: ["a", "b", "c"] }).dtypes;
    expect(actual).toStrictEqual(expected);
  });
  test("height", () => {
    const expected = 3;
    const actual = pl.DataFrame({ a: [1, 2, 3], b: ["a", "b", "c"] }).height;
    expect(actual).toEqual(expected);
  });
  test("width", () => {
    const expected = 2;
    const actual = pl.DataFrame({ a: [1, 2, 3], b: ["a", "b", "c"] }).width;
    expect(actual).toEqual(expected);
  });
  test("shape", () => {
    const expected = { height: 3, width: 2 };
    const actual = pl.DataFrame({ a: [1, 2, 3], b: ["a", "b", "c"] }).shape;
    expect(actual).toEqual(expected);
  });
  test("get columns", () => {
    const expected = ["a", "b"];
    const actual = pl.DataFrame({ a: [1, 2, 3], b: ["a", "b", "c"] }).columns;
    expect(actual).toEqual(expected);
  });
  test("set columns", () => {
    const expected = ["d", "e"];
    const df = pl.DataFrame({ a: [1, 2, 3], b: ["a", "b", "c"] });
    df.columns = expected;
    expect(df.columns).toEqual(expected);
  });
  test("clone", () => {
    const expected = pl.DataFrame({ a: [1, 2, 3], b: ["a", "b", "c"] });
    const actual = expected.clone();
    expect(actual).toFrameEqual(expected);
  });
  test("describe", () => {
    const actual = pl
      .DataFrame({
        a: [1, 2, 3],
        b: ["a", "b", "c"],
        c: [true, true, false],
      })
      .describe()
      .withColumn(pl.col("c").round(6).as("c"));
    const expected = pl.DataFrame({
      describe: ["mean", "std", "min", "max", "median"],
      a: [2, 1, 1, 3, 2],
      b: [null, null, "a", "c", null],
      c: [0.666667, 0.57735, 0, 1, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("drop", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
      apple: ["a", "b", "c"],
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const actual = df.drop("apple");
    expect(actual).toFrameEqual(expected);
  });
  test("drop: array", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
      apple: ["a", "b", "c"],
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
    });
    const actual = df.drop(["apple", "ham"]);
    expect(actual).toFrameEqual(expected);
  });
  test("drop: ...rest", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
      apple: ["a", "b", "c"],
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
    });
    const actual = df.drop("apple", "ham");
    expect(actual).toFrameEqual(expected);
  });
  test("unique", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 2, 3],
        bar: [1, 2, 2, 4],
        ham: ["a", "d", "d", "c"],
      })
      .unique();
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [1, 2, 4],
      ham: ["a", "d", "c"],
    });
    expect(actual).toFrameEqualIgnoringOrder(expected);
  });
  test("unique:subset", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 2, 2],
        bar: [1, 2, 2, 2],
        ham: ["a", "b", "c", "c"],
      })
      .unique({ subset: ["foo", "ham"] });
    const expected = pl.DataFrame({
      foo: [1, 2, 2],
      bar: [1, 2, 2],
      ham: ["a", "b", "c"],
    });
    expect(actual).toFrameEqualIgnoringOrder(expected);
  });
  // run this test 100 times to make sure it is deterministic.
  test("unique:maintainOrder", () => {
    for (const _x of Array.from({ length: 100 })) {
      const actual = pl
        .DataFrame({
          foo: [0, 1, 2, 2, 2],
          bar: [0, 1, 2, 2, 2],
          ham: ["0", "a", "b", "b", "b"],
        })
        .unique({ maintainOrder: true });

      const expected = pl.DataFrame({
        foo: [0, 1, 2],
        bar: [0, 1, 2],
        ham: ["0", "a", "b"],
      });
      expect(actual).toFrameEqual(expected);
    }
  });
  // run this test 100 times to make sure it is deterministic.
  test("unique:maintainOrder:single subset", () => {
    for (const _x of Array.from({ length: 100 })) {
      const actual = pl
        .DataFrame({
          foo: [0, 1, 2, 2, 2],
          bar: [0, 1, 2, 2, 2],
          ham: ["0", "a", "b", "c", "d"],
        })
        .unique({ maintainOrder: true, subset: "foo" });

      const expected = pl.DataFrame({
        foo: [0, 1, 2],
        bar: [0, 1, 2],
        ham: ["0", "a", "b"],
      });
      expect(actual).toFrameEqual(expected);
    }
  });
  // run this test 100 times to make sure it is deterministic.
  test("unique:maintainOrder:multi subset", () => {
    for (const _x of Array.from({ length: 100 })) {
      const actual = pl
        .DataFrame({
          foo: [0, 1, 2, 2, 2],
          bar: [0, 1, 2, 2, 2],
          ham: ["0", "a", "b", "c", "c"],
        })
        .unique({ maintainOrder: true, subset: ["foo", "ham"] });

      const expected = pl.DataFrame({
        foo: [0, 1, 2, 2],
        bar: [0, 1, 2, 2],
        ham: ["0", "a", "b", "c"],
      });
      expect(actual).toFrameEqual(expected);
    }
  });
  test("unnest", () => {
    const expected = pl.DataFrame({
      int: [1, 2],
      str: ["a", "b"],
      bool: [true, null],
      list: [[1, 2], [3]],
    });
    const actual = expected.toStruct("my_struct").toFrame().unnest("my_struct");
    expect(actual).toFrameEqual(expected);
  });
  test("DF with nulls", () => {
    const actual = pl.DataFrame([
      { foo: 1, bar: 6.0, ham: "a" },
      { foo: null, bar: 0.5, ham: "b" },
      { foo: 3, bar: 7.0, ham: "c" },
    ]);
    const expected = pl.DataFrame({
      foo: [1, null, 3],
      bar: [6.0, 0.5, 7.0],
      ham: ["a", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("dropNulls", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, 7.0, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .dropNulls();
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("dropNulls subset", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, 7.0, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .dropNulls("foo");
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("explode", () => {
    const actual = pl
      .DataFrame({
        letters: ["c", "a"],
        nrs: [
          [1, 2],
          [1, 3],
        ],
      })
      .explode("nrs");

    const expected = pl.DataFrame({
      letters: ["c", "c", "a", "a"],
      nrs: [1, 2, 1, 3],
    });

    expect(actual).toFrameEqual(expected);
  });
  test("fillNull:zero", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, 7.0, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .fillNull("zero");
    const expected = pl.DataFrame({
      foo: [1, 0, 2, 3],
      bar: [6.0, 0.5, 7.0, 8.0],
      ham: ["a", "d", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("fillNull:one", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, 7.0, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .fillNull("one");
    const expected = pl.DataFrame({
      foo: [1, 1, 2, 3],
      bar: [6.0, 0.5, 7.0, 8.0],
      ham: ["a", "d", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("filter", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
    });
    // Filter on one condition
    let actual = df.filter(pl.col("foo").lt(3));
    let expected = pl.DataFrame({
      foo: [1, 2],
      bar: [6, 7],
      ham: ["a", "b"],
    });
    expect(actual).toFrameEqual(expected);

    // Filter on multiple conditions
    actual = df.filter(
      pl
        .col("foo")
        .lt(3)
        .and(pl.col("ham").eq(pl.lit("a"))),
    );
    expected = pl.DataFrame({
      foo: [1],
      bar: [6],
      ham: ["a"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("findIdxByName", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .findIdxByName("ham");
    const expected = 2;
    expect(actual).toEqual(expected);
  });
  test("fold:single column", () => {
    const expected = pl.Series([1, 2, 3]);
    const df = pl.DataFrame([expected]);
    const actual = df.fold((a, b) => a.concat(b));
    expect(actual).toSeriesEqual(expected);
  });
  it.each`
    name                     | actual                                                 | expected
    ${"fold:lessThan"}       | ${df.fold((a, b) => a.lessThan(b)).alias("foo")}       | ${pl.Series("foo", [true, false, false])}
    ${"fold:lt"}             | ${df.fold((a, b) => a.lt(b)).alias("foo")}             | ${pl.Series("foo", [true, false, false])}
    ${"fold:lessThanEquals"} | ${df.fold((a, b) => a.lessThanEquals(b)).alias("foo")} | ${pl.Series("foo", [true, true, false])}
    ${"fold:ltEq"}           | ${df.fold((a, b) => a.ltEq(b)).alias("foo")}           | ${pl.Series("foo", [true, true, false])}
    ${"fold:neq"}            | ${df.fold((a, b) => a.neq(b)).alias("foo")}            | ${pl.Series("foo", [true, false, true])}
    ${"fold:plus"}           | ${df.fold((a, b) => a.plus(b)).alias("foo")}           | ${pl.Series("foo", [7, 4, 17])}
    ${"fold:minus"}          | ${df.fold((a, b) => a.minus(b)).alias("foo")}          | ${pl.Series("foo", [-5, 0, 1])}
    ${"fold:mul"}            | ${df.fold((a, b) => a.mul(b)).alias("foo")}            | ${pl.Series("foo", [6, 4, 72])}
  `("$# $name expected matches actual", ({ expected, actual }) => {
    expect(expected).toSeriesEqual(actual);
  });
  test("fold:lt", () => {
    const s1 = pl.Series([1, 2, 3]);
    const s2 = pl.Series([4, 5, 6]);
    const s3 = pl.Series([7, 8, 1]);
    const df = pl.DataFrame([s1, s2, s3]);
    const expected = pl.Series("foo", [true, true, false]);
    let actual = df.fold((a, b) => a.lessThan(b)).alias("foo");
    expect(actual).toSeriesEqual(expected);
    actual = df.fold((a, b) => a.lt(b)).alias("foo");
    expect(actual).toSeriesEqual(expected);
  });
  test("frameEqual:true", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
    });
    const other = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
    });
    const actual = df.frameEqual(other);
    expect(actual).toStrictEqual(true);
  });
  test("frameEqual:false", () => {
    const df = pl.DataFrame({
      foo: [3, 2, 22],
      baz: [0, 7, 8],
    });
    const other = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
    });
    const actual = df.frameEqual(other);
    expect(actual).toStrictEqual(false);
  });
  test("frameEqual:nullEq:false", () => {
    const df = pl.DataFrame({
      foo: [1, 2, null],
      bar: [6, 7, 8],
    });
    const other = pl.DataFrame({
      foo: [1, 2, null],
      bar: [6, 7, 8],
    });
    const actual = df.frameEqual(other, false);
    expect(actual).toStrictEqual(false);
  });
  test("frameEqual:nullEq:true", () => {
    const df = pl.DataFrame({
      foo: [1, 2, null],
      bar: [6, 7, 8],
    });
    const other = pl.DataFrame({
      foo: [1, 2, null],
      bar: [6, 7, 8],
    });
    const actual = df.frameEqual(other, true);
    expect(actual).toStrictEqual(true);
  });
  test("getColumn", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .getColumn("ham");
    const expected = pl.Series("ham", ["a", "b", "c"]);
    expect(actual).toSeriesEqual(expected);
  });
  test("getColumns", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .getColumns();
    const expected = [
      pl.Series("foo", [1, 2, 3]),
      pl.Series("ham", ["a", "b", "c"]),
    ];
    for (const [idx, a] of actual.entries()) {
      expect(a).toSeriesEqual(expected[idx]);
    }
  });
  test("groupBy", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .groupBy("foo");
    expect(actual.toString()).toEqual("GroupBy");
  });
  test("hashRows", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .hashRows();
    expect(actual.dtype).toStrictEqual(pl.UInt64);
  });
  test.each([[1], [1, 2], [1, 2, 3], [1, 2, 3, 4]])(
    "hashRows:positional",
    (...args: any[]) => {
      const actual = pl
        .DataFrame({
          foo: [1, 2, 3],
          ham: ["a", "b", "c"],
        })
        .hashRows(...args);
      expect(actual.dtype).toStrictEqual(pl.UInt64);
    },
  );
  test.each([
    [{ k0: 1 }],
    [{ k0: 1, k1: 2 }],
    [{ k0: 1, k1: 2, k2: 3 }],
    [{ k0: 1, k1: 2, k2: 3, k3: 4 }],
  ])("hashRows:named", (opts) => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .hashRows(opts);
    expect(actual.dtype).toStrictEqual(pl.UInt64);
  });
  test("head", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .head(1);
    const expected = pl
      .DataFrame({
        foo: [1],
        ham: ["a"],
      })
      .head(1);
    expect(actual).toFrameEqual(expected);
  });
  test("hstack:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .hstack([pl.Series("apple", [10, 20, 30])]);
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
      apple: [10, 20, 30],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("hstack:df", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .hstack(pl.DataFrame([pl.Series("apple", [10, 20, 30])]));
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
      apple: [10, 20, 30],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("hstack:df", () => {
    const actual = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
    });
    actual.insertAtIdx(0, pl.Series("apple", [10, 20, 30]));
    const expected = pl.DataFrame({
      apple: [10, 20, 30],
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("interpolate", () => {
    const df = pl.DataFrame({
      a: [1, null, 3],
    });
    const expected = pl.DataFrame({
      a: [1, 2, 3],
    });
    const actual = df.interpolate();
    expect(actual).toFrameEqual(expected);
  });
  test("isDuplicated", () => {
    const df = pl.DataFrame({
      a: [1, 2, 2],
      b: [1, 2, 2],
    });
    const expected = pl.Series([false, true, true]);
    const actual = df.isDuplicated();
    expect(actual).toSeriesEqual(expected);
  });
  test("isEmpty", () => {
    const df = pl.DataFrame({});
    expect(df.isEmpty()).toEqual(true);
  });
  test("isUnique", () => {
    const df = pl.DataFrame({
      a: [1, 2, 2],
      b: [1, 2, 2],
    });
    const expected = pl.Series([true, false, false]);
    const actual = df.isUnique();
    expect(actual).toSeriesEqual(expected);
  });

  test("lazy", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      })
      .lazy()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("limit", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .limit(1);
    const expected = pl.DataFrame({
      foo: [1],
      ham: ["a"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("max:axis:0", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .max();
    expect(actual.row(0)).toEqual([3, 8, "c"]);
  });
  test("max:axis:1", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .max(1);
    const expected = pl.Series("foo", [6, 2, 9]);
    expect(actual).toSeriesEqual(expected);
  });
  test("mean:axis:0", () => {
    const actual = pl
      .DataFrame({
        foo: [4, 4, 4],
        bar: [1, 1, 10],
        ham: ["a", "b", "a"],
      })
      .mean();
    expect(actual.row(0)).toEqual([4, 4, null]);
  });
  test("mean:axis:1", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 6],
        bar: [6, 2, 8],
      })
      .mean(1, "ignore");
    const expected = pl.Series("foo", [3.5, 2, 7]);
    expect(actual).toSeriesEqual(expected);
  });
  test("median", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .median();
    expect(actual.row(0)).toEqual([2, 7, null]);
  });
  test("unpivot", () => {
    const df = pl.DataFrame({
      id: [1],
      asset_key_1: ["123"],
      asset_key_2: ["456"],
      asset_key_3: ["abc"],
    });
    const actual = df.unpivot("id", [
      "asset_key_1",
      "asset_key_2",
      "asset_key_3",
    ]);
    const expected = pl.DataFrame({
      id: [1, 1, 1],
      variable: ["asset_key_1", "asset_key_2", "asset_key_3"],
      value: ["123", "456", "abc"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("unpivot renamed", () => {
    const df = pl.DataFrame({
      id: [1],
      asset_key_1: ["123"],
      asset_key_2: ["456"],
      asset_key_3: ["abc"],
    });
    const actual = df.unpivot(
      "id",
      ["asset_key_1", "asset_key_2", "asset_key_3"],
      {
        variableName: "foo",
        valueName: "bar",
      },
    );
    const expected = pl.DataFrame({
      id: [1, 1, 1],
      foo: ["asset_key_1", "asset_key_2", "asset_key_3"],
      bar: ["123", "456", "abc"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("min:axis:0", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .min();
    expect(actual.row(0)).toEqual([1, 6, "a"]);
  });
  test("min:axis:1", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .min(1);
    const expected = pl.Series("foo", [1, 2, 8]);
    expect(actual).toSeriesEqual(expected);
  });
  test("nChunks", () => {
    const actual = pl.DataFrame({
      foo: [1, 2, 9],
      bar: [6, 2, 8],
    });
    expect(actual.nChunks()).toEqual(1);
  });
  test("nth", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, null],
        bar: [6, 2, 8],
        apple: [6, 2, 8],
        pizza: [null, null, 8],
      })
      .select(pl.nth(2));
    expect(actual.columns).toEqual(["apple"]);
  });
  test("nth:multiple", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, null],
        bar: [6, 2, 8],
        apple: [6, 2, 8],
        pizza: [null, null, 8],
      })
      .select(pl.nth(2), pl.nth(3));
    expect(actual.columns).toEqual(["apple", "pizza"]);
  });
  test("nullCount", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, null],
        bar: [6, 2, 8],
        apple: [6, 2, 8],
        pizza: [null, null, 8],
      })
      .nullCount();
    expect(actual.row(0)).toEqual([1, 0, 0, 2]);
  });
  test.todo("pipe");
  test("quantile", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .quantile(0.5);
    expect(actual.row(0)).toEqual([2, 7, null]);
  });
  test("rename", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .rename({
        foo: "foo_new",
        bar: "bar_new",
        ham: "ham_new",
      });
    expect(actual.columns).toEqual(["foo_new", "bar_new", "ham_new"]);
  });
  test("replaceAtIdx", () => {
    const actual: pl.DataFrame = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
    });
    const s = pl.Series("new_foo", [0.12, 2.0, 9.99]);
    actual.replaceAtIdx(0, s);
    expect(actual.getColumn("new_foo")).toSeriesEqual(s);
    expect(actual.findIdxByName("new_foo")).toEqual(0);
  });
  test("row", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .row(1);
    expect(actual).toEqual([2, 7, "b"]);
  });
  test("rows", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .rows();
    expect(actual).toEqual([
      [1, 6, "a"],
      [2, 7, "b"],
      [3, 8, "c"],
    ]);
  });
  test("rows:categorical", () => {
    const actual = pl
      .DataFrame({
        foo: ["one", "two", "one", "two", "three"],
        flt: [1, 2, 1, 2, 3],
      })
      .withColumns(
        pl.col("foo").cast(pl.Categorical).alias("cat"),
        pl.col("flt").cast(pl.Int32).alias("int"),
      );

    const expected = [
      ["one", 1.0, "one", 1],
      ["two", 2.0, "two", 2],
      ["one", 1.0, "one", 1],
      ["two", 2.0, "two", 2],
      ["three", 3.0, "three", 3],
    ];

    expect(actual.rows()).toEqual(expected);
    const byType = actual.select(
      pl.col(pl.Utf8),
      pl.col(pl.Float64),
      pl.col(pl.Categorical),
      pl.col(pl.Int32),
    );
    expect(byType.rows()).toEqual(expected);
  });
  test("sample:n", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .sample(2);
    expect(actual.height).toStrictEqual(2);
  });
  test("sample:default", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .sample();
    expect(actual.height).toStrictEqual(1);
  });
  test("sample:frac", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
        ham: ["a", "b", "c", null],
      })
      .sample({ frac: 0.5 });
    expect(actual.height).toStrictEqual(2);
  });
  test("sample:frac", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
        ham: ["a", "b", "c", null],
      })
      .sample({ frac: 0.75 });
    expect(actual.height).toStrictEqual(3);
  });
  test("sample:invalid", () => {
    const fn = () =>
      pl
        .DataFrame({
          foo: [1, 2, 3, 1],
          bar: [6, 7, 8, 1],
          ham: ["a", "b", "c", null],
        })
        .sample({} as any);
    expect(fn).toThrow(TypeError);
  });
  test("select:strings", () => {
    const columns = ["ham", "foo"];
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
        ham: ["a", "b", "c", null],
      })
      .select(...columns);
    const foo = pl.Series("foo", [1, 2, 3, 1]);
    const ham = pl.Series("ham", ["a", "b", "c", null]);
    expect(actual.width).toStrictEqual(2);
    expect(actual.getColumn("foo")).toSeriesEqual(foo);
    expect(actual.getColumn("ham")).toSeriesEqual(ham);
  });
  test("select:expr", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
        ham: ["a", "b", "c", null],
      })
      .select(pl.col("foo"), "ham");
    const foo = pl.Series("foo", [1, 2, 3, 1]);
    const ham = pl.Series("ham", ["a", "b", "c", null]);
    expect(actual.width).toStrictEqual(2);
    expect(actual.getColumn("foo")).toSeriesEqual(foo);
    expect(actual.getColumn("ham")).toSeriesEqual(ham);
  });
  test("shift:pos", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .shift(1);
    const expected = pl.DataFrame({
      foo: [null, 1, 2, 3],
      bar: [null, 6, 7, 8],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("shift:neg", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .shift(-1);
    const expected = pl.DataFrame({
      foo: [2, 3, 1, null],
      bar: [7, 8, 1, null],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("shiftAndFill:positional", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .shiftAndFill(-1, 99);
    const expected = pl.DataFrame({
      foo: [2, 3, 1, 99],
      bar: [7, 8, 1, 99],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("shiftAndFill:named", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .shiftAndFill({ n: -1, fillValue: 99 });
    const expected = pl.DataFrame({
      foo: [2, 3, 1, 99],
      bar: [7, 8, 1, 99],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("shrinkToFit:inPlace", () => {
    const actual = pl.DataFrame({
      foo: [1, 2, 3, 1],
      bar: [6, 7, 8, 1],
    });
    actual.shrinkToFit(true);
    const expected = pl.DataFrame({
      foo: [1, 2, 3, 1],
      bar: [6, 7, 8, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("shrinkToFit", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .shrinkToFit();
    const expected = pl.DataFrame({
      foo: [1, 2, 3, 1],
      bar: [6, 7, 8, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("slice:positional", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .slice(0, 2);
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: [6, 7],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("slice:named", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .slice({ offset: 0, length: 2 });
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: [6, 7],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("sort:positional", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .sort("bar");
    const expected = pl.DataFrame({
      foo: [1, 1, 2, 3],
      bar: [1, 6, 7, 8],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("sort:named", () => {
    let actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .sort({ by: "bar", descending: true });
    const expected = pl.DataFrame({
      foo: [3, 2, 1, 1],
      bar: [8, 7, 6, 1],
    });
    expect(actual).toFrameEqual(expected);
    actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .sort({ by: "bar", reverse: true }); // deprecated
    expect(actual).toFrameEqual(expected);
  });
  test("sort:multi-args", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, -1],
        bar: [6, 7, 8, 2],
        baz: ["a", "b", "d", "A"],
      })
      .sort({
        by: [pl.col("baz"), pl.col("bar")],
      });
    const expected = pl.DataFrame({
      foo: [-1, 1, 2, 3],
      bar: [2, 6, 7, 8],
      baz: ["A", "a", "b", "d"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("sort:nullsLast:false", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
      })
      .sort({ by: "foo", nullsLast: false });
    const expected = pl.DataFrame({
      foo: [null, 1, 2, 3],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("sort:nullsLast:true", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
      })
      .sort({ by: "foo", nullsLast: true });
    const expected = pl.DataFrame({
      foo: [1, 2, 3, null],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("std", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .std();
    const expected = pl.DataFrame([
      pl.Series("foo", [1]),
      pl.Series("bar", [1]),
      pl.Series("ham", [null], pl.Utf8),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("sum:axis:0", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .sum();
    expect(actual.row(0)).toEqual([6, 21, null]);
  });
  test("sum:axis:1", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .sum(1)
      .rename("sum");
    const expected = pl.Series("sum", [7, 4, 17]);
    expect(actual).toSeriesEqual(expected);
  });
  test("tail", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .tail(1)
      .row(0);
    const expected = [9, 8];
    expect(actual).toEqual(expected);
  });
  test("transpose", () => {
    const expected = pl.DataFrame({
      column_0: [1, 1],
      column_1: [2, 2],
      column_2: [3, 3],
    });
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: [1, 2, 3],
    });
    const actual = df.transpose();
    expect(actual).toFrameEqual(expected);
  });
  test("transpose:includeHeader", () => {
    const expected = pl.DataFrame({
      column: ["a", "b"],
      column_0: [1, 1],
      column_1: [2, 2],
      column_2: [3, 3],
    });
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: [1, 2, 3],
    });
    const actual = df.transpose({ includeHeader: true });
    expect(actual).toFrameEqual(expected);
  });
  test("transpose:columnNames", () => {
    const expected = pl.DataFrame({
      a: [1, 1],
      b: [2, 2],
      c: [3, 3],
    });
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: [1, 2, 3],
    });
    const actual = df.transpose({ includeHeader: false, columnNames: "abc" });
    expect(actual).toFrameEqual(expected);
  });
  test("transpose:columnNames:array", () => {
    const expected = pl.DataFrame({
      a: [1, 1],
      b: [2, 2],
      c: [3, 3],
    });
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: [1, 2, 3],
    });
    const actual = df.transpose({
      includeHeader: false,
      columnNames: ["a", "b", "c"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("transpose:columnNames:generator", () => {
    const expected = pl.DataFrame({
      col_0: [1, 1],
      col_1: [2, 2],
      col_2: [3, 3],
    });
    function* namesGenerator() {
      const baseName = "col_";
      let count = 0;
      while (true) {
        const name = `${baseName}${count}`;
        yield name;
        count++;
      }
    }
    const df = pl.DataFrame({
      a: [1, 2, 3],
      b: [1, 2, 3],
    });
    const actual = df.transpose({
      includeHeader: false,
      columnNames: namesGenerator(),
    });
    expect(actual).toFrameEqual(expected);
  });
  test("var", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .var();
    const expected = pl.DataFrame([
      pl.Series("foo", [1]),
      pl.Series("bar", [1]),
      pl.Series("ham", [null], pl.Utf8),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("vstack", () => {
    const df1 = pl.DataFrame({
      foo: [1, 2],
      bar: [6, 7],
      ham: ["a", "b"],
    });
    const df2 = pl.DataFrame({
      foo: [3, 4],
      bar: [8, 9],
      ham: ["c", "d"],
    });

    const actual = df1.vstack(df2);
    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 3, 4]),
      pl.Series("bar", [6, 7, 8, 9]),
      pl.Series("ham", ["a", "b", "c", "d"]),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("withColumn:series", () => {
    const actual = df
      .clone()
      .withColumn(pl.Series("col_a", ["a", "a", "a"], pl.Utf8));
    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("withColumn:expr", () => {
    const actual = df.clone().withColumn(pl.lit("a").alias("col_a"));

    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("withColumns:series", () => {
    const actual = df
      .clone()
      .withColumns(
        pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
        pl.Series("col_b", ["b", "b", "b"], pl.Utf8),
      );
    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
      pl.Series("col_b", ["b", "b", "b"], pl.Utf8),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("withColumns:expr", () => {
    const actual = df
      .clone()
      .withColumns(pl.lit("a").alias("col_a"), pl.lit("b").alias("col_b"));
    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
      pl.Series("col_b", ["b", "b", "b"], pl.Utf8),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("withColumnRenamed:positional", () => {
    const actual = df.clone().withColumnRenamed("foo", "apple");

    const expected = pl.DataFrame([
      pl.Series("apple", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("withColumnRenamed:named", () => {
    const actual = df
      .clone()
      .withColumnRenamed({ existing: "foo", replacement: "apple" });

    const expected = pl.DataFrame([
      pl.Series("apple", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("withRowCount", () => {
    const actual = df.clone().withRowCount();

    const expected = pl.DataFrame([
      pl.Series("row_nr", [0, 1, 2], pl.UInt32),
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
    ]);
    expect(actual).toFrameEqual(expected);
  });
  test("df from JSON with multiple struct", () => {
    const rows = [
      {
        id: 1,
        name: "one",
        attributes: {
          b: false,
          bb: true,
          s: "one",
          x: 1,
          att2: { s: "two", y: 2, att3: { s: "three", y: 3 } },
        },
      },
    ];

    const actual = pl.DataFrame(rows);
    const expected = `shape: (1,)
Series: 'attributes' [struct[5]]
[
	{false,true,"one",1.0,{"two",2.0,{"three",3.0}}}
]`;
    expect(actual.select("attributes").toSeries().toString()).toEqual(expected);
  });
  test("df from JSON with struct", () => {
    const rows = [
      {
        id: 1,
        name: "one",
        attributes: { b: false, bb: true, s: "one", x: 1 },
      },
      {
        id: 2,
        name: "two",
        attributes: { b: false, bb: true, s: "two", x: 2 },
      },
      {
        id: 3,
        name: "three",
        attributes: { b: false, bb: true, s: "three", x: 3 },
      },
    ];

    let actual = pl.DataFrame(rows);
    expect(actual.schema).toStrictEqual({
      id: pl.Float64,
      name: pl.String,
      attributes: pl.Struct([
        new pl.Field("b", pl.Bool),
        new pl.Field("bb", pl.Bool),
        new pl.Field("s", pl.String),
        new pl.Field("x", pl.Float64),
      ]),
    });

    let expected = `shape: (3, 3)
┌─────┬───────┬──────────────────────────┐
│ id  ┆ name  ┆ attributes               │
│ --- ┆ ---   ┆ ---                      │
│ f64 ┆ str   ┆ struct[4]                │
╞═════╪═══════╪══════════════════════════╡
│ 1.0 ┆ one   ┆ {false,true,"one",1.0}   │
│ 2.0 ┆ two   ┆ {false,true,"two",2.0}   │
│ 3.0 ┆ three ┆ {false,true,"three",3.0} │
└─────┴───────┴──────────────────────────┘`;
    expect(actual.toString()).toStrictEqual(expected);

    const schema = {
      id: pl.Int32,
      name: pl.String,
      attributes: pl.Struct([
        new pl.Field("b", pl.Bool),
        new pl.Field("bb", pl.Bool),
        new pl.Field("s", pl.String),
        new pl.Field("x", pl.Int16),
      ]),
    };
    actual = pl.DataFrame(rows, { schema: schema });
    expected = `shape: (3, 3)
┌─────┬───────┬────────────────────────┐
│ id  ┆ name  ┆ attributes             │
│ --- ┆ ---   ┆ ---                    │
│ i32 ┆ str   ┆ struct[4]              │
╞═════╪═══════╪════════════════════════╡
│ 1   ┆ one   ┆ {false,true,"one",1}   │
│ 2   ┆ two   ┆ {false,true,"two",2}   │
│ 3   ┆ three ┆ {false,true,"three",3} │
└─────┴───────┴────────────────────────┘`;
    expect(actual.toString()).toStrictEqual(expected);
    expect(actual.getColumn("name").toArray()).toEqual(
      rows.map((e) => e["name"]),
    );
    expect(actual.getColumn("attributes").toArray()).toMatchObject(
      rows.map((e) => e["attributes"]),
    );
  });
  test("pivot:values-with-options", () => {
    {
      const df = pl.DataFrame({
        a: pl.Series([1, 2, 3]).cast(pl.Int32),
        b: pl
          .Series([
            [1, 1],
            [2, 2],
            [3, 3],
          ])
          .cast(pl.List(pl.Int32)),
      });

      const expected = pl
        .DataFrame({
          a: pl.Series([1, 2, 3]).cast(pl.Int32),
          "1": pl.Series([[1, 1], null, null]).cast(pl.List(pl.Int32)),
          "2": pl.Series([null, [2, 2], null]).cast(pl.List(pl.Int32)),
          "3": pl.Series([null, null, [3, 3]]).cast(pl.List(pl.Int32)),
        })
        .select("a", "1", "2", "3");

      const actual = df.pivot("b", {
        index: "a",
        on: "a",
        aggregateFunc: "first",
        sortColumns: true,
      });

      expect(actual).toFrameEqual(expected, true);
    }

    {
      const df = pl.DataFrame({
        a: ["beep", "bop"],
        b: ["a", "b"],
        c: ["s", "f"],
        d: [7, 8],
        e: ["x", "y"],
      });
      const actual = df.pivot(["a", "e"], {
        index: "b",
        on: ["b"],
        aggregateFunc: "first",
        separator: "|",
        maintainOrder: true,
      });

      const expected = pl.DataFrame({
        b: ["a", "b"],
        "a|a": ["beep", null],
        "a|b": [null, "bop"],
        "e|a": ["x", null],
        "e|b": [null, "y"],
      });
      expect(actual).toFrameEqual(expected, true);
    }
    {
      const df = pl.DataFrame({
        foo: ["A", "A", "B", "B", "C"],
        N: [1, 2, 2, 4, 2],
        bar: ["k", "l", "m", "n", "o"],
      });
      const actual = df.pivot(["N"], {
        index: "foo",
        on: "bar",
        aggregateFunc: "first",
      });

      const expected = pl.DataFrame({
        foo: ["A", "B", "C"],
        k: [1, null, null],
        l: [2, null, null],
        m: [null, 2, null],
        n: [null, 4, null],
        o: [null, null, 2],
      });

      expect(actual).toFrameEqual(expected, true);
    }
    {
      const df = pl.DataFrame({
        ix: [1, 1, 2, 2, 1, 2],
        col: ["a", "a", "a", "a", "b", "b"],
        foo: [0, 1, 2, 2, 7, 1],
        bar: [0, 2, 0, 0, 9, 4],
      });

      const actual = df.pivot(["foo", "bar"], {
        index: "ix",
        on: "col",
        aggregateFunc: "sum",
        separator: "/",
      });

      const expected = pl.DataFrame({
        ix: [1, 2],
        "foo/a": [1, 4],
        "foo/b": [7, 1],
        "bar/a": [2, 0],
        "bar/b": [9, 4],
      });
      expect(actual).toFrameEqual(expected, true);
    }
  });
  test("pivot:options-only", () => {
    {
      const df = pl.DataFrame({
        a: pl.Series([1, 2, 3]).cast(pl.Int32),
        b: pl
          .Series([
            [1, 1],
            [2, 2],
            [3, 3],
          ])
          .cast(pl.List(pl.Int32)),
      });

      const expected = pl
        .DataFrame({
          a: pl.Series([1, 2, 3]).cast(pl.Int32),
          "1": pl.Series([[1, 1], null, null]).cast(pl.List(pl.Int32)),
          "2": pl.Series([null, [2, 2], null]).cast(pl.List(pl.Int32)),
          "3": pl.Series([null, null, [3, 3]]).cast(pl.List(pl.Int32)),
        })
        .select("a", "1", "2", "3");

      const actual = df.pivot({
        values: "b",
        index: "a",
        on: "a",
        aggregateFunc: "first",
        sortColumns: true,
      });

      expect(actual).toFrameEqual(expected, true);
    }

    {
      const df = pl.DataFrame({
        a: ["beep", "bop"],
        b: ["a", "b"],
        c: ["s", "f"],
        d: [7, 8],
        e: ["x", "y"],
      });
      const actual = df.pivot({
        values: ["a", "e"],
        index: "b",
        on: ["b"],
        aggregateFunc: "first",
        separator: "|",
        maintainOrder: true,
      });

      const expected = pl.DataFrame({
        b: ["a", "b"],
        "a|a": ["beep", null],
        "a|b": [null, "bop"],
        "e|a": ["x", null],
        "e|b": [null, "y"],
      });
      expect(actual).toFrameEqual(expected, true);
    }
    {
      const df = pl.DataFrame({
        foo: ["A", "A", "B", "B", "C"],
        N: [1, 2, 2, 4, 2],
        bar: ["k", "l", "m", "n", "o"],
      });
      const actual = df.pivot({
        values: ["N"],
        index: "foo",
        on: "bar",
        aggregateFunc: "first",
      });

      const expected = pl.DataFrame({
        foo: ["A", "B", "C"],
        k: [1, null, null],
        l: [2, null, null],
        m: [null, 2, null],
        n: [null, 4, null],
        o: [null, null, 2],
      });

      expect(actual).toFrameEqual(expected, true);
    }
    {
      const df = pl.DataFrame({
        ix: [1, 1, 2, 2, 1, 2],
        col: ["a", "a", "a", "a", "b", "b"],
        foo: [0, 1, 2, 2, 7, 1],
        bar: [0, 2, 0, 0, 9, 4],
      });

      const actual = df.pivot({
        values: ["foo", "bar"],
        index: "ix",
        on: "col",
        aggregateFunc: "sum",
        separator: "/",
      });

      const expected = pl.DataFrame({
        ix: [1, 2],
        "foo/a": [1, 4],
        "foo/b": [7, 1],
        "bar/a": [2, 0],
        "bar/b": [9, 4],
      });
      expect(actual).toFrameEqual(expected, true);
    }
  });
});
describe("join", () => {
  test("on", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
    });
    const actual = df.join(otherDF, { on: "ham" });

    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: [6.0, 7.0],
      ham: ["a", "b"],
      apple: ["x", "y"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("on:multiple-columns", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo: [1, 10, 11],
    });
    const actual = df.join(otherDF, { on: ["ham", "foo"] });

    const expected = pl.DataFrame({
      foo: [1],
      bar: [6.0],
      ham: ["a"],
      apple: ["x"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("on:left&right", () => {
    const df = pl.DataFrame({
      foo_left: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo_right: [1, 10, 11],
    });
    const actual = df.join(otherDF, {
      leftOn: ["foo_left", "ham"],
      rightOn: ["foo_right", "ham"],
    });

    const expected = pl.DataFrame({
      foo_left: [1],
      bar: [6.0],
      ham: ["a"],
      apple: ["x"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("on:left&right", () => {
    const df = pl.DataFrame({
      foo_left: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo_right: [1, 10, 11],
    });
    const actual = df.join(otherDF, {
      leftOn: ["foo_left", "ham"],
      rightOn: ["foo_right", "ham"],
    });

    const expected = pl.DataFrame({
      foo_left: [1],
      bar: [6.0],
      ham: ["a"],
      apple: ["x"],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("on throws error if only 'leftOn' is specified", () => {
    const df = pl.DataFrame({
      foo_left: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo_right: [1, 10, 11],
    });
    const f = () =>
      df.join(otherDF, {
        leftOn: ["foo_left", "ham"],
      } as any);
    expect(f).toThrow(TypeError);
  });
  test("on throws error if only 'rightOn' is specified", () => {
    const df = pl.DataFrame({
      foo_left: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo_right: [1, 10, 11],
    });
    const f = () =>
      df.join(otherDF, {
        rightOn: ["foo_right", "ham"],
      } as any);
    expect(f).toThrow(TypeError);
  });
  test("on takes precedence over left&right", () => {
    const df = pl.DataFrame({
      foo_left: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo_right: [1, 10, 11],
    });
    const actual = df.join(otherDF, {
      on: "ham",
      leftOn: ["foo_left", "ham"],
      rightOn: ["foo_right", "ham"],
    } as any);
    const expected = pl.DataFrame({
      foo_left: [1, 2],
      bar: [6.0, 7.0],
      ham: ["a", "b"],
      apple: ["x", "y"],
      foo_right: [1, 10],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("how:left", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo: [1, 10, 11],
    });
    const actual = df.join(otherDF, {
      on: "ham",
      how: "left",
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
      apple: ["x", "y", null],
      foo_right: [1, 10, null],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("how:full", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y"],
      ham: ["a", "d"],
      foo: [1, 10],
    });
    const actual = df.join(otherDF, {
      on: "ham",
      how: "full",
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3, null],
      bar: [6, 7, 8, null],
      ham: ["a", "b", "c", null],
      apple: ["x", null, null, "y"],
      ham_right: ["a", null, null, "d"],
      foo_right: [1, null, null, 10],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("suffix", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl.DataFrame({
      apple: ["x", "y", "z"],
      ham: ["a", "b", "d"],
      foo: [1, 10, 11],
    });
    const actual = df.join(otherDF, {
      on: "ham",
      how: "left",
      suffix: "_other",
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6, 7, 8],
      ham: ["a", "b", "c"],
      apple: ["x", "y", null],
      foo_other: [1, 10, null],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("asof_cross_join", () => {
    const left = pl
      .DataFrame({ a: [-10, 5, 10], left_val: ["a", "b", "c"] })
      .sort("a");
    const right = pl
      .DataFrame({
        a: [1, 2, 3, 6, 7],
        right_val: [1, 2, 3, 6, 7],
      })
      .sort("a");

    //  only test dispatch of asof join
    let out = left.joinAsof(right, { on: "a" });
    expect(out.shape).toEqual({ height: 3, width: 3 });

    out = left.lazy().joinAsof(right.lazy(), { on: "a" }).collectSync();
    expect(out.shape).toEqual({ height: 3, width: 3 });

    // only test dispatch of cross join
    out = left.join(right, { how: "cross" });
    expect(out.shape).toEqual({ height: 15, width: 4 });

    left.lazy().join(right.lazy(), { how: "cross" }).collectSync();
    expect(out.shape).toEqual({ height: 15, width: 4 });
  });
});
describe("io", () => {
  const df = pl.DataFrame([
    pl.Series("foo", [1, 2, 9], pl.Int16),
    pl.Series("bar", [6, 2, 8], pl.Int16),
  ]);
  test("writeCSV:string", () => {
    const actual = df.clone().writeCSV().toString();
    const expected = "foo,bar\n1,6\n2,2\n9,8\n";
    expect(actual).toEqual(expected);
  });
  test("writeCSV:string:sep", () => {
    const actual = df.clone().writeCSV({ separator: "X" }).toString();
    const expected = "fooXbar\n1X6\n2X2\n9X8\n";
    expect(actual).toEqual(expected);
  });
  test("writeCSV:string:quote", () => {
    const df = pl.DataFrame({
      bar: ["a,b,c", "d,e,f", "g,h,i"],
      foo: [1, 2, 3],
    });
    const actual = df.writeCSV({ quoteChar: "^" }).toString();
    const expected = "bar,foo\n^a,b,c^,1.0\n^d,e,f^,2.0\n^g,h,i^,3.0\n";
    expect(actual).toEqual(expected);
  });
  test("writeCSV:string:header", () => {
    const actual = df
      .clone()
      .writeCSV({ separator: "X", includeHeader: false, lineTerminator: "|" })
      .toString();
    const expected = "1X6|2X2|9X8|";
    expect(actual).toEqual(expected);
  });
  test("writeCSV:stream", (done) => {
    const df = pl.DataFrame([
      pl.Series("foo", [1, 2, 3], pl.UInt32),
      pl.Series("bar", ["a", "b", "c"]),
    ]);
    let body = "";
    const writeStream = new Stream.Writable({
      write(chunk, _encoding, callback) {
        body += chunk;
        callback(null);
      },
    });
    df.writeCSV(writeStream);
    const newDF = pl.readCSV(body);
    expect(newDF).toFrameEqual(df);
    done();
  });
  test("writeCSV:path", (done) => {
    const df = pl.DataFrame([
      pl.Series("foo", [1, 2, 3], pl.UInt32),
      pl.Series("bar", ["a", "b", "c"]),
    ]);
    df.writeCSV("./test.csv");
    const newDF = pl.readCSV("./test.csv");
    expect(newDF).toFrameEqual(df);
    fs.rmSync("./test.csv");
    done();
  });
  test("toRecords", () => {
    const df = pl.DataFrame({
      foo: [1],
      bar: ["a"],
    });
    const expected = [{ foo: 1.0, bar: "a" }];
    const actual = df.toRecords();
    expect(actual).toEqual(expected);
  });
  test("toRecords:date", () => {
    const df = pl
      .DataFrame({
        date: [new Date()],
      })
      .withColumn(pl.col("date").cast(pl.Date).alias("date"));
    const dt = new Date();
    const expected = [
      {
        date: new Date(
          Date.UTC(dt.getFullYear(), dt.getMonth(), dt.getDate(), 0, 0, 0, 0),
        ),
      },
    ];
    const actual = df.toRecords();
    expect(JSON.stringify(actual)).toEqual(JSON.stringify(expected));
  });
  test("toRecords:quoteChar:emptyHeader", () => {
    const csv = `|name|,||
|John|,|green|
|Anna|,|red|
`;
    const df = pl.readCSV(csv, { quoteChar: "|" });
    const actual = df.toRecords();
    const expected = [
      { name: "John", "": "green" },
      { name: "Anna", "": "red" },
    ];
    expect(actual).toEqual(expected);
  });
  test("toObject", () => {
    const expected = {
      foo: [1],
      bar: ["a"],
    };
    const df = pl.DataFrame(expected);

    const actual = df.toObject();
    expect(actual).toEqual(expected);
  });
  test("writeJSON:lines", () => {
    const rows = [{ foo: 1.1 }, { foo: 3.1 }, { foo: 3.1 }];
    const actual = pl.DataFrame(rows).writeJSON({ format: "lines" }).toString();
    const expected = rows
      .map((r) => JSON.stringify(r))
      .join("\n")
      .concat("\n");
    expect(actual).toEqual(expected);
  });
  test("writeJSON:stream", (done) => {
    const df = pl.DataFrame([
      pl.Series("foo", [1, 2, 3], pl.UInt32),
      pl.Series("bar", ["a", "b", "c"]),
    ]);

    let body = "";
    const writeStream = new Stream.Writable({
      write(chunk, _encoding, callback) {
        body += chunk;
        callback(null);
      },
    });
    df.writeJSON(writeStream, { format: "json" });
    const newDF = pl.readJSON(body).select("foo", "bar");
    expect(newDF).toFrameEqual(df);
    done();
  });
  test("writeJSON:path", (done) => {
    const df = pl.DataFrame([
      pl.Series("foo", [1, 2, 3], pl.UInt32),
      pl.Series("bar", ["a", "b", "c"]),
    ]);
    df.writeJSON("./test.json", { format: "json" });
    const newDF = pl.readJSON("./test.json").select("foo", "bar");
    expect(newDF).toFrameEqual(df);
    fs.rmSync("./test.json");
    done();
  });

  test("writeJSON:rows", () => {
    const rows = [{ foo: 1.1 }, { foo: 3.1 }, { foo: 3.1 }];
    const expected = JSON.stringify(rows);
    const actual = pl
      .readRecords(rows)
      .writeJSON({ format: "json" })
      .toString();
    expect(actual).toEqual(expected);
  });
  test("toSeries", () => {
    const s = pl.Series([1, 2, 3]);
    const actual = s.clone().toFrame().toSeries();
    expect(actual).toSeriesEqual(s);
  });
});
describe("create", () => {
  test("from empty", () => {
    const df = pl.DataFrame();
    expect(df.isEmpty()).toStrictEqual(true);
  });
  test("from empty-object", () => {
    const df = pl.DataFrame({});
    expect(df.isEmpty()).toStrictEqual(true);
  });
  test("all supported types", () => {
    const df = pl.DataFrame({
      bool: [true, null],
      date: pl.Series("", [new Date(), new Date()], pl.Date),
      date_nulls: pl.Series("", [null, new Date()], pl.Date),
      datetime: pl.Series("", [new Date(), new Date()]),
      datetime_nulls: pl.Series("", [null, new Date()]),
      string: ["a", "b"],
      string_nulls: [null, "a"],
      categorical: pl.Series("", ["one", "two"], pl.Categorical),
      categorical_nulls: pl.Series("", ["apple", null], pl.Categorical),
      list: [[1], [2, 3]],
      float_64: [1, 2],
      float_64_nulls: [1, null],
      uint_64: [1n, 2n],
      uint_64_null: [null, 2n],
      int_8_typed: Int8Array.from([1, 2]),
      int_16_typed: Int16Array.from([1, 2]),
      int_32_typed: Int32Array.from([1, 2]),
      int_64_typed: BigInt64Array.from([1n, 2n]),
      uint_8_typed: Uint8Array.from([1, 2]),
      uint_16_typed: Uint16Array.from([1, 2]),
      uint_32_typed: Uint32Array.from([1, 2]),
      uint_64_typed: BigUint64Array.from([1n, 2n]),
      float_32_typed: Float32Array.from([1.1, 2.2]),
      float_64_typed: Float64Array.from([1.1, 2.2]),
    });
    const expectedSchema = {
      bool: pl.Bool,
      date: pl.Date,
      date_nulls: pl.Date,
      datetime: pl.Datetime("ms", ""),
      datetime_nulls: pl.Datetime("ms", ""),
      string: pl.String,
      string_nulls: pl.String,
      categorical: pl.Categorical,
      categorical_nulls: pl.Categorical,
      list: pl.List(pl.Float64),
      float_64: pl.Float64,
      float_64_nulls: pl.Float64,
      uint_64: pl.UInt64,
      uint_64_null: pl.UInt64,
      int_8_typed: pl.Int8,
      int_16_typed: pl.Int16,
      int_32_typed: pl.Int32,
      int_64_typed: pl.Int64,
      uint_8_typed: pl.UInt8,
      uint_16_typed: pl.UInt16,
      uint_32_typed: pl.UInt32,
      uint_64_typed: pl.UInt64,
      float_32_typed: pl.Float32,
      float_64_typed: pl.Float64,
    };
    const actual = df.schema;
    expect(actual).toStrictEqual(expectedSchema);
  });
  test("from series-array", () => {
    const s1 = pl.Series("num", [1, 2, 3]);
    const s2 = pl.Series(
      "date",
      [null, Date.now(), Date.now()],
      pl.Datetime("ms"),
    );
    const df = pl.DataFrame([s1, s2]);
    expect(df.getColumn("num")).toSeriesEqual(s1);
    expect(df.getColumn("date")).toSeriesEqual(s2);
  });
  test("from arrays", () => {
    const columns = [
      [1, 2, 3],
      [1, 2, 2],
    ];

    const df = pl.DataFrame(columns);

    expect(df.getColumns()[0].toArray()).toEqual(columns[0]);
    expect(df.getColumns()[1].toArray()).toEqual(columns[1]);
  });
  test("from arrays: orient=col", () => {
    const columns = [
      [1, 2, 3],
      [1, 2, 2],
    ];

    const df = pl.DataFrame(columns, { orient: "col" });

    expect(df.getColumns()[0].toArray()).toEqual(columns[0]);
    expect(df.getColumns()[1].toArray()).toEqual(columns[1]);
  });
  test("from arrays: orient=row", () => {
    const rows = [
      [1, 2, 3],
      [1, 2, 2],
    ];

    const df = pl.readRecords(rows);

    expect(df.row(0).sort()).toEqual(rows[0]);
    expect(df.row(1).sort()).toEqual(rows[1]);
  });
  test("from arrays with column names: orient=col", () => {
    const columns = [
      [1, 2, 3],
      [1, 2, 2],
    ];

    const expectedColumnNames = ["a", "b"];
    const df = pl.DataFrame(columns, {
      columns: expectedColumnNames,
      orient: "col",
    });

    expect(df.getColumns()[0].toArray()).toEqual(columns[0]);
    expect(df.getColumns()[1].toArray()).toEqual(columns[1]);
    expect(df.columns).toEqual(expectedColumnNames);
  });
  test("from arrays: invalid ", () => {
    const columns = [
      [1, 2, 3],
      [1, 2, 2],
    ];

    const fn = () => pl.DataFrame(columns, { columns: ["a", "b", "c", "d"] });
    expect(fn).toThrow();
  });
  test("from arrays with columns, orient=row", () => {
    const rows = [
      [1, 2, 3],
      [1, 2, 2],
    ];
    const expectedColumns = ["a", "b", "c"];
    const df = pl.DataFrame(rows, { columns: expectedColumns, orient: "row" });

    expect(df.row(0).sort()).toEqual(rows[0].sort());
    expect(df.row(1).sort()).toEqual(rows[1].sort());
    expect(df.columns).toEqual(expectedColumns);
  });
  test("from row objects, inferred schema", () => {
    const rows = [
      { num: 1, date: new Date(Date.now()), string: "foo1" },
      { num: 1, date: new Date(Date.now()), string: 1 },
    ];

    const expected = [
      rows[0],
      { num: 1, date: rows[1].date, string: rows[1].string.toString() },
    ];

    const df = pl.readRecords(rows, { inferSchemaLength: 1 });
    expect(df.toRecords()).toEqual(expected);
    expect(df.schema).toStrictEqual({
      num: pl.Float64,
      date: pl.Datetime("ms", ""),
      string: pl.String,
    });
  });
  test("from row objects, inferred schema, empty array", () => {
    const df = pl.readRecords([
      { a: [], b: 0 },
      { a: [""], b: 0 },
    ]);
    expect(df.schema).toStrictEqual({
      a: pl.List(pl.String),
      b: pl.Float64,
    });
  });
  test("from row objects, with schema", () => {
    const rows = [
      { num: 1, date: "foo", string: "foo1" },
      { num: 1, date: "foo" },
    ];

    const expected = [
      { num: 1, date: rows[0].date.toString(), string: "foo1" },
      { num: 1, date: rows[1].date.toString(), string: null },
    ];

    const schema = {
      num: pl.Int32,
      date: pl.String,
      string: pl.String,
    };
    const df = pl.readRecords(rows, { schema });
    expect(df.toRecords()).toEqual(expected);
    expect(df.schema).toStrictEqual(schema);
  });

  test("from nulls", () => {
    const df = pl.DataFrame({ nulls: [null, null, null] });
    const expected = pl.DataFrame([
      pl.Series("nulls", [null, null, null], pl.Float64),
    ]);
    expect(df).toFrameStrictEqual(expected);
  });
  test("from list types", () => {
    const int8List = [
      Int8Array.from([1, 2, 3]),
      Int8Array.from([2]),
      Int8Array.from([1, 1, 1]),
    ];
    const expected: any = {
      num_list: [[1, 2], [], [3, null]],
      bool_list: [[true, null], [], [false]],
      str_list: [["a", null], ["b", "c"], []],
      bigint_list: [[1n], [2n, 3n], []],
      int8_list: int8List,
    };
    expected.int8_list = int8List.map((i) => [...i]);
    const df = pl.DataFrame(expected);

    expect(df.toObject()).toEqual(expected);
  });
  test("with schema", () => {
    const df = pl.DataFrame(
      {
        a: [1, 2, 3],
        b: ["1", "2", "3"],
      },
      {
        schema: {
          x: pl.Int32,
          y: pl.String,
        },
      },
    );
    expect(df.schema).toStrictEqual({ x: pl.Int32, y: pl.String });
  });
  test("with schema", () => {
    const df = pl.DataFrame(
      {
        a: [1, 2, 3],
        b: ["1", "2", "3"],
      },
      {
        schema: {
          x: pl.Int32,
          y: pl.String,
        },
      },
    );
    expect(df.schema).toStrictEqual({ x: pl.Int32, y: pl.String });
  });
  test("with schema overrides", () => {
    const df = pl.DataFrame(
      {
        a: [1, 2, 3],
        b: ["1", "2", "3"],
      },
      {
        schemaOverrides: {
          a: pl.Int32,
        },
      },
    );
    expect(df.schema).toStrictEqual({ a: pl.Int32, b: pl.String });
  });
  test("errors if schemaOverrides and schema are both specified", () => {
    const fn = () =>
      pl.DataFrame(
        {
          a: [1, 2, 3],
          b: ["1", "2", "3"],
        },
        {
          schema: {
            x: pl.Int32,
            y: pl.String,
          },
          schemaOverrides: {
            a: pl.Int32,
          },
        },
      );
    expect(fn).toThrow();
  });
  test("errors if schema mismatch", () => {
    const fn = () => {
      pl.DataFrame(
        {
          a: [1, 2, 3],
          b: ["1", "2", "3"],
        },
        {
          schema: {
            a: pl.Int32,
            b: pl.String,
            c: pl.Int32,
          },
        },
      );
    };
    expect(fn).toThrow();
  });
});
describe("arithmetic", () => {
  test("add", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .add(1);
    const expected = pl.DataFrame({
      foo: [2, 3, 4],
      bar: [5, 6, 7],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("sub", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .sub(1);
    const expected = pl.DataFrame({
      foo: [0, 1, 2],
      bar: [3, 4, 5],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("div", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 4, 6],
        bar: [2, 2, 2],
      })
      .div(2);
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [1, 1, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("mul", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .mul(2);
    const expected = pl.DataFrame({
      foo: [2, 4, 6],
      bar: [6, 4, 2],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("rem", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .rem(2);
    const expected = pl.DataFrame({
      foo: [1, 0, 1],
      bar: [1, 0, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("plus", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .plus(1);
    const expected = pl.DataFrame({
      foo: [2, 3, 4],
      bar: [5, 6, 7],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("minus", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .minus(1);
    const expected = pl.DataFrame({
      foo: [0, 1, 2],
      bar: [3, 4, 5],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("divideBy", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 4, 6],
        bar: [2, 2, 2],
      })
      .divideBy(2);
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [1, 1, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("multiplyBy", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .multiplyBy(2);
    const expected = pl.DataFrame({
      foo: [2, 4, 6],
      bar: [6, 4, 2],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("modulo", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .modulo(2);
    const expected = pl.DataFrame({
      foo: [1, 0, 1],
      bar: [1, 0, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("add:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .add(pl.Series([3, 2, 1]));
    const expected = pl.DataFrame({
      foo: [4, 4, 4],
      bar: [7, 7, 7],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("sub:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .sub(pl.Series([1, 2, 3]));
    const expected = pl.DataFrame({
      foo: [0, 0, 0],
      bar: [3, 3, 3],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("div:series", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 4, 6],
        bar: [2, 2, 2],
      })
      .div(pl.Series([2, 2, 1]));
    const expected = pl.DataFrame({
      foo: [1, 2, 6],
      bar: [1, 1, 2],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("mul:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .mul(pl.Series([2, 3, 1]));
    const expected = pl.DataFrame({
      foo: [2, 6, 3],
      bar: [6, 6, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("rem:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .rem(pl.Series([1, 1, 3]));
    const expected = pl.DataFrame({
      foo: [0, 0, 0],
      bar: [0, 0, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("plus:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .plus(pl.Series([3, 2, 1]));
    const expected = pl.DataFrame({
      foo: [4, 4, 4],
      bar: [7, 7, 7],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("minus:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [4, 5, 6],
      })
      .minus(pl.Series([1, 2, 3]));
    const expected = pl.DataFrame({
      foo: [0, 0, 0],
      bar: [3, 3, 3],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("divideBy:series", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 4, 6],
        bar: [2, 2, 2],
      })
      .divideBy(pl.Series([2, 2, 1]));
    const expected = pl.DataFrame({
      foo: [1, 2, 6],
      bar: [1, 1, 2],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("multiplyBy:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .multiplyBy(pl.Series([2, 3, 1]));
    const expected = pl.DataFrame({
      foo: [2, 6, 3],
      bar: [6, 6, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
  test("modulo:series", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [3, 2, 1],
      })
      .modulo(pl.Series([1, 1, 3]));
    const expected = pl.DataFrame({
      foo: [0, 0, 0],
      bar: [0, 0, 1],
    });
    expect(actual).toFrameEqual(expected);
  });
});

describe("meta", () => {
  test("array destructuring", () => {
    const df = pl.DataFrame({
      os: ["apple", "linux"],
      version: [10.12, 18.04],
    });
    const [col0] = df;
    expect(col0).toSeriesEqual(df.getColumn("os"));
    const [, version] = df;
    expect(version).toSeriesEqual(df.getColumn("version"));
    const [[row0Index0], [, row1Index1]] = df;
    expect(row0Index0).toStrictEqual("apple");
    expect(row1Index1).toStrictEqual(18.04);
  });
  test("object destructuring", () => {
    const df = pl.DataFrame({
      os: ["apple", "linux"],
      version: [10.12, 18.04],
    });
    const { os, version } = <any>df;
    expect(os).toSeriesEqual(df.getColumn("os"));
    expect(version).toSeriesEqual(df.getColumn("version"));
    const df2 = pl.DataFrame({
      fruits: ["apple", "orange"],
      cars: ["ford", "honda"],
    });
    const df3 = pl.DataFrame({ ...df, ...df2 });
    const expected = df.hstack(df2);
    expect(df3).toFrameEqual(expected);
  });
  test("object bracket notation", () => {
    const df = pl.DataFrame({
      os: ["apple", "linux"],
      version: [10.12, 18.04],
    });

    expect(df["os"]).toSeriesEqual(df.getColumn("os"));
    expect(df["os"][1]).toStrictEqual("linux");

    df["os"] = pl.Series(["mac", "ubuntu"]);
    expect(df["os"][0]).toStrictEqual("mac");
  });
  test("object.keys shows column names", () => {
    const df = pl.DataFrame({
      os: ["apple", "linux"],
      version: [10.12, 18.04],
    });
    const keys = Object.keys(df);
    expect(keys).toEqual(df.columns);
  });
  test("object.values shows column values", () => {
    const df = pl.DataFrame({
      os: ["apple", "linux"],
      version: [10.12, 18.04],
    });
    const values = Object.values(df);
    expect(values[0]).toSeriesEqual(df["os"]);
    expect(values[1]).toSeriesEqual(df["version"]);
  });
  test("df rows", () => {
    const df = pl.DataFrame({
      os: ["apple", "linux"],
      version: [10.12, 18.04],
    });
    const actual = df[0][0];
    expect(actual).toStrictEqual(df.getColumn("os").get(0));
  });

  test("proxy:has", () => {
    const df = pl.DataFrame({
      os: ["apple", "linux"],
      version: [10.12, 18.04],
    });
    expect("os" in df).toBe(true);
  });
  test("inspect & toString", () => {
    const df = pl.DataFrame({
      a: [1],
    });
    const expected = `shape: (1, 1)
┌─────┐
│ a   │
│ --- │
│ f64 │
╞═════╡
│ 1.0 │
└─────┘`;
    const actualInspect = df[Symbol.for("nodejs.util.inspect.custom")]();
    const dfString = df.toString();
    expect(actualInspect).toStrictEqual(expected);
    expect(dfString).toStrictEqual(expected);
  });
});
test("Jupyter.display", () => {
  const df = pl.DataFrame({
    os: ["apple", "linux"],
    version: [10.12, 18.04],
  });
  expect(Symbol.for("Jupyter.display") in df).toBe(true);

  const actual = df[Symbol.for("Jupyter.display")]();

  expect(actual).toBeInstanceOf(Object);

  const dataResource = actual["application/vnd.dataresource+json"];
  expect(dataResource).toBeInstanceOf(Object);
  expect(dataResource).toHaveProperty("schema");
  expect(dataResource).toHaveProperty("data");
  expect(dataResource).toHaveProperty("data", [
    { os: "apple", version: 10.12 },
    { os: "linux", version: 18.04 },
  ]);

  const html = actual["text/html"];
  expect(html).toContain("apple");
  expect(html).toContain("linux");
  expect(html).toContain("10.12");
  expect(html).toContain("18.04");
});

describe("additional", () => {
  test("partitionBy", () => {
    const df = pl.DataFrame({
      label: ["a", "a", "b", "b"],
      value: [1, 2, 3, 4],
    });
    const dfs = df
      .partitionBy(["label"], true, true)
      .map((df) => df.toObject());
    const expected = [
      {
        label: ["a", "a"],
        value: [1, 2],
      },
      {
        label: ["b", "b"],
        value: [3, 4],
      },
    ];

    expect(dfs).toEqual(expected);
  });
  test("partitionBy with callback", () => {
    const df = pl.DataFrame({
      label: ["a", "a", "b", "b"],
      value: [1, 2, 3, 4],
    });
    const dfs = df.partitionBy(["label"], true, true, (df) => df.toObject());

    const expected = [
      {
        label: ["a", "a"],
        value: [1, 2],
      },
      {
        label: ["b", "b"],
        value: [3, 4],
      },
    ];

    expect(dfs).toEqual(expected);
  });
  test("df from rows with schema", () => {
    const rows = [{ a: 1, b: 2, c: null, d: "foo" }];

    const df = pl.DataFrame(rows, {
      schema: { a: pl.Int32, b: pl.Int32, c: pl.Utf8, d: pl.String },
      orient: "row",
    });
    const actual = df.toRecords();
    expect(actual).toEqual(rows);
  });
  test("upsample", () => {
    const df = pl
      .DataFrame({
        date: [
          new Date(2024, 1, 1),
          new Date(2024, 3, 1),
          new Date(2024, 4, 1),
          new Date(2024, 5, 1),
        ],
        groups: ["A", "B", "A", "B"],
        values: [0, 1, 2, 3],
      })
      .withColumn(pl.col("date").cast(pl.Date).alias("date"))
      .sort("date");

    let actual = df
      .upsample("date", "1mo", "groups", true)
      .select(pl.col("*").forwardFill());

    let expected = pl
      .DataFrame({
        date: [
          new Date(2024, 1, 1),
          new Date(2024, 2, 1),
          new Date(2024, 3, 1),
          new Date(2024, 4, 1),
          new Date(2024, 3, 1),
          new Date(2024, 4, 1),
          new Date(2024, 5, 1),
        ],
        groups: ["A", "A", "A", "A", "B", "B", "B"],
        values: [0.0, 0.0, 0.0, 2.0, 1.0, 1.0, 3.0],
      })
      .withColumn(pl.col("date").cast(pl.Date).alias("date"));

    expect(actual).toFrameEqual(expected);

    actual = df
      .upsample({
        timeColumn: "date",
        every: "1mo",
        by: "groups",
        maintainOrder: true,
      })
      .select(pl.col("*").forwardFill());

    expect(actual).toFrameEqual(expected);

    actual = df
      .upsample({ timeColumn: "date", every: "1mo" })
      .select(pl.col("*").forwardFill());

    expected = pl
      .DataFrame({
        date: [
          new Date(2024, 1, 1),
          new Date(2024, 2, 1),
          new Date(2024, 3, 1),
          new Date(2024, 4, 1),
          new Date(2024, 5, 1),
        ],
        groups: ["A", "A", "B", "A", "B"],
        values: [0.0, 0.0, 1.0, 2.0, 3.0],
      })
      .withColumn(pl.col("date").cast(pl.Date).alias("date"));

    expect(actual).toFrameEqual(expected);

    actual = df
      .upsample({ timeColumn: "date", every: "1m" })
      .select(pl.col("*").forwardFill());

    expect(actual.shape).toEqual({ height: 174_241, width: 3 });
  });
});
