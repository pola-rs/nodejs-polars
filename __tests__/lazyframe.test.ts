import fs from "node:fs";
import pl from "../polars";

describe("lazyframe", () => {
  test("columns", () => {
    const df = pl
      .DataFrame({
        foo: [1, 2],
        bar: ["a", "b"],
      })
      .lazy();
    const actual = df.columns;
    assert.deepStrictEqual(actual, ["foo", "bar"]);
  });
  test("collectSync", () => {
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const actual = expected.lazy().collectSync();
    assertFrameEqual(actual, expected);
  });
  test("collect", async () => {
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const actual = await expected.lazy().collect();
    assertFrameEqual(actual, expected);
  });
  test("collect:streaming", async () => {
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const actual = await expected.lazy().collect({ streaming: true });
    assertFrameEqual(actual, expected);
  });
  test("describeOptimizedPlan", () => {
    const df = pl
      .DataFrame({
        foo: [1, 2],
        bar: ["a", "b"],
      })
      .lazy();
    let actual = df.describeOptimizedPlan().replace(/\s+/g, " ");
    const expected = `DF ["foo", "bar"]; PROJECT */2 COLUMNS`;
    assert.deepStrictEqual(actual, expected);
    actual = df.describePlan().replace(/\s+/g, " ");
    assert.deepStrictEqual(actual, expected);
  });
  test("cache", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
    });
    let actual = df.lazy().cache().collectSync();
    assertFrameEqual(actual, expected);
    actual = df.lazy().clone().collectSync();
    assertFrameEqual(actual, expected);
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
    const actual = df.lazy().drop("apple").collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("drop:array", () => {
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
    const actual = df.lazy().drop(["apple", "ham"]).collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("drop:rest", () => {
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
    const actual = df.lazy().drop("apple", "ham").collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("unique", () => {
    const ldf = pl
      .DataFrame({
        foo: [1, 2, 2, 3],
        bar: [1, 2, 2, 4],
        ham: ["a", "d", "d", "c"],
      })
      .lazy();
    let actual = ldf.unique().collectSync();
    let expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [1, 2, 4],
      ham: ["a", "d", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
    actual = ldf.unique("foo", "first", true).collectSync();
    assertFrameEqual(actual, expected);
    actual = ldf.unique("foo").collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
    actual = ldf.unique(["foo"]).collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
    actual = ldf.unique(["foo", "ham"], "first", true).collectSync();
    assertFrameEqual(actual, expected);
    actual = ldf.unique(["foo", "ham"]).collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
    actual = ldf.unique(["foo", "ham"], "none", true).collectSync();
    expected = pl.DataFrame({
      foo: [1, 3],
      bar: [1, 4],
      ham: ["a", "c"],
    });
    assertFrameEqual(actual, expected);
  });
  test("unique:subset", () => {
    const ldf = pl
      .DataFrame({
        foo: [1, 2, 2, 2],
        bar: [1, 2, 2, 3],
        ham: ["a", "b", "c", "c"],
      })
      .lazy();
    let actual = ldf.unique({ subset: ["foo", "ham"] }).collectSync();
    let expected = pl.DataFrame({
      foo: [1, 2, 2],
      bar: [1, 2, 2],
      ham: ["a", "b", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
    actual = ldf.unique({ subset: ["ham"] }).collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
    actual = ldf
      .unique({ subset: ["ham"], keep: "first", maintainOrder: true })
      .collectSync();
    assertFrameEqualIgnoringOrder(actual, expected);
    actual = ldf.unique({ subset: ["ham"], keep: "last" }).collectSync();
    expected = pl.DataFrame({
      foo: [1, 2, 2],
      bar: [1, 2, 3],
      ham: ["a", "b", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
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
        .lazy()
        .unique({ maintainOrder: true })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [0, 1, 2],
        bar: [0, 1, 2],
        ham: ["0", "a", "b"],
      });
      assertFrameEqual(actual, expected);
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
        .lazy()
        .unique({ maintainOrder: true, subset: "foo" })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [0, 1, 2],
        bar: [0, 1, 2],
        ham: ["0", "a", "b"],
      });
      assertFrameEqual(actual, expected);
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
        .lazy()
        .unique({ maintainOrder: true, subset: ["foo", "ham"] })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [0, 1, 2, 2],
        bar: [0, 1, 2, 2],
        ham: ["0", "a", "b", "c"],
      });
      assertFrameEqual(actual, expected);
    }
  });
  test("dropNulls", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, 7.0, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .lazy()
      .dropNulls()
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("dropNulls:array", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, null, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .lazy()
      .dropNulls(["foo"])
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, null, 8.0],
      ham: ["a", "b", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("dropNulls:string", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, null, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .lazy()
      .dropNulls("foo")
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, null, 8.0],
      ham: ["a", "b", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("dropNulls:rest", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, null, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .lazy()
      .dropNulls("foo", "bar")
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 3],
      bar: [6.0, 8.0],
      ham: ["a", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("explode", () => {
    const actual = pl
      .DataFrame({
        letters: ["c", "a"],
        list_1: [
          [1, 2],
          [1, 3],
        ],
      })
      .lazy()
      .explode("list_1")
      .collectSync();
    const expected = pl.DataFrame({
      letters: ["c", "c", "a", "a"],
      list_1: [1, 2, 1, 3],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("explode:all-columns", () => {
    const actual = (
      pl
        .DataFrame({
          list_1: [
            [1, 2],
            [3, 4],
          ],
          list_2: [
            ["a", "b"],
            ["c", "d"],
          ],
        })
        .lazy() as any
    )
      .explode()
      .collectSync();

    const expected = pl.DataFrame({
      list_1: [1, 2, 3, 4],
      list_2: ["a", "b", "c", "d"],
    });
    assertFrameEqual(actual, expected);
  });
  test("explode:array", () => {
    const actual = pl
      .DataFrame({
        id: [1, 2],
        list_1: [
          [1, 2],
          [3, 4],
        ],
        list_2: [
          ["a", "b"],
          ["c", "d"],
        ],
      })
      .lazy()
      .explode(["list_1", "list_2"])
      .collectSync();

    const expected = pl.DataFrame({
      id: [1, 1, 2, 2],
      list_1: [1, 2, 3, 4],
      list_2: ["a", "b", "c", "d"],
    });
    assertFrameEqual(actual, expected);
  });
  test("fetch", async () => {
    const df = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const expected = pl.DataFrame({
      foo: [1],
      bar: ["a"],
    });
    const actual = await df
      .lazy()
      .select("*")
      .fetch(1, { noOptimization: true });
    assertFrameEqual(actual, expected);
  });
  test("fetchSync", () => {
    const df = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const expected = pl.DataFrame({
      foo: [1],
      bar: ["a"],
    });
    let actual = df.lazy().select("*").fetchSync(1);
    assertFrameEqual(actual, expected);
    actual = df.lazy().select("*").fetchSync(1, { noOptimization: true });
    assertFrameEqual(actual, expected);
  });
  test("first", () => {
    const df = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const expected = pl.DataFrame({
      foo: [1],
      bar: ["a"],
    });
    const actual: pl.DataFrame = df.lazy().first();
    assertFrameEqual(actual, expected);
  });
  test("fillNull:zero", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, 7.0, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .lazy()
      .fillNull(0)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 0, 2, 3],
      bar: [6.0, 0.5, 7.0, 8.0],
      ham: ["a", "d", "b", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("fillNull:expr", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
        bar: [6.0, 0.5, 7.0, 8.0],
        ham: ["a", "d", "b", "c"],
      })
      .lazy()
      .fillNull(pl.lit(1))
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 1, 2, 3],
      bar: [6.0, 0.5, 7.0, 8.0],
      ham: ["a", "d", "b", "c"],
    });
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("filter", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .filter(pl.col("foo").eq(2))
      .collectSync();
    const expected = pl.DataFrame({
      foo: [2, 2],
      bar: [2, 3],
    });
    assertFrameEqual(actual, expected);
  });
  test("filter:lit", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .filter(pl.col("foo").eq(pl.lit(2)))
      .collectSync();
    const expected = pl.DataFrame({
      foo: [2, 2],
      bar: [2, 3],
    });
    assertFrameEqual(actual, expected);
  });
  describe("groupby", () => {
    test("groupBy", () => {
      const ldf = pl
        .DataFrame({
          foo: [1, 2, 3, 4],
          ham: ["a", "a", "b", "b"],
        })
        .lazy();

      let actual = ldf.groupBy("ham").agg(pl.col("foo").sum()).collectSync();
      let expected = pl.DataFrame({
        ham: ["a", "b"],
        foo: [3, 7],
      });
      assertFrameEqual(actual, expected);

      actual = ldf.groupBy("ham", true).agg(pl.col("foo").sum()).collectSync();
      assertFrameEqual(actual, expected);

      actual = ldf
        .groupBy("ham", { maintainOrder: true })
        .agg(pl.col("foo").sum())
        .collectSync();
      assertFrameEqual(actual, expected);

      actual = ldf
        .groupBy("ham", { maintainOrder: true })
        .head(1)
        .collectSync();
      expected = pl.DataFrame({
        ham: ["a", "b"],
        foo: [1, 3],
      });
      assertFrameEqual(actual, expected);

      actual = ldf
        .groupBy("ham", { maintainOrder: true })
        .tail(1)
        .collectSync();
      expected = pl.DataFrame({
        ham: ["a", "b"],
        foo: [2, 4],
      });
      assertFrameEqual(actual, expected);
      actual = ldf.groupBy("ham").len().collectSync();
      expected = pl.DataFrame(
        {
          ham: ["a", "b"],
          len: [2, 2],
        },
        { schema: { ham: pl.String, len: pl.UInt32 } },
      );
      assertFrameEqual(actual, expected);
      actual = ldf.groupBy("ham").len("foo").collectSync();
      expected = pl.DataFrame(
        {
          ham: ["a", "b"],
          foo: [2, 2],
        },
        { schema: { ham: pl.String, foo: pl.UInt32 } },
      );
      assertFrameEqual(actual, expected);
    });
  });
  test("head", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .lazy()
      .head(1)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1],
      ham: ["a"],
    });
    assertFrameEqual(actual, expected);
  });
  describe("join", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
      bar: [6.0, 7.0, 8.0],
      ham: ["a", "b", "c"],
    });
    const otherDF = pl
      .DataFrame({
        apple: ["x", "y", "z"],
        ham: ["a", "b", "d"],
        foo: [1, 10, 11],
      })
      .lazy();
    test("on", () => {
      const actual = df.lazy().join(otherDF, { on: "ham" }).collectSync();
      const expected = pl.DataFrame({
        foo: [1, 2],
        bar: [6.0, 7.0],
        ham: ["a", "b"],
        apple: ["x", "y"],
        fooright: [1, 10],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("on:multiple-columns", () => {
      const actual = df
        .lazy()
        .join(otherDF, { on: ["ham", "foo"] })
        .collectSync();

      const expected = pl.DataFrame({
        foo: [1],
        bar: [6.0],
        ham: ["a"],
        apple: ["x"],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("on:left&right", () => {
      const df = pl.DataFrame({
        foo_left: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      });
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y", "z"],
          ham: ["a", "b", "d"],
          foo_right: [1, 10, 11],
        })
        .lazy();

      const actual = df
        .lazy()
        .join(otherDF, {
          leftOn: ["foo_left", "ham"],
          rightOn: ["foo_right", "ham"],
        })
        .collectSync();

      const expected = pl.DataFrame({
        foo_left: [1],
        bar: [6.0],
        ham: ["a"],
        apple: ["x"],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("on:left&right", () => {
      const df = pl.DataFrame({
        foo_left: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      });
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y", "z"],
          ham: ["a", "b", "d"],
          foo_right: [1, 10, 11],
        })
        .lazy();

      const actual = df
        .lazy()
        .join(otherDF, {
          leftOn: ["foo_left", "ham"],
          rightOn: ["foo_right", "ham"],
        })
        .collectSync();

      const expected = pl.DataFrame({
        foo_left: [1],
        bar: [6.0],
        ham: ["a"],
        apple: ["x"],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("on throws error if only 'leftOn' is specified", () => {
      const df = pl.DataFrame({
        foo_left: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      });
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y", "z"],
          ham: ["a", "b", "d"],
          foo_right: [1, 10, 11],
        })
        .lazy();

      const f = () =>
        df.lazy().join(otherDF, {
          leftOn: ["foo_left", "ham"],
        } as any);
      assert.throws(f, TypeError);
    });
    test("on throws error if only 'rightOn' is specified", () => {
      const df = pl.DataFrame({
        foo_left: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      });
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y", "z"],
          ham: ["a", "b", "d"],
          foo_right: [1, 10, 11],
        })
        .lazy();

      const f = () =>
        df
          .lazy()
          .join(otherDF, {
            rightOn: ["foo_right", "ham"],
          } as any)
          .collectSync();
      assert.throws(f, TypeError);
    });
    test("on takes precedence over left&right", () => {
      const df = pl.DataFrame({
        foo_left: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      });
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y", "z"],
          ham: ["a", "b", "d"],
          foo_right: [1, 10, 11],
        })
        .lazy();

      const actual = df
        .lazy()
        .join(otherDF, {
          on: "ham",
          leftOn: ["foo_left", "ham"],
          rightOn: ["foo_right", "ham"],
        } as any)
        .collectSync();
      const expected = pl.DataFrame({
        foo_left: [1, 2],
        bar: [6.0, 7.0],
        ham: ["a", "b"],
        apple: ["x", "y"],
        foo_right: [1, 10],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("how:left", () => {
      const actual = df
        .lazy()
        .join(otherDF, {
          on: "ham",
          how: "left",
        })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
        apple: ["x", "y", null],
        fooright: [1, 10, null],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("how:full", () => {
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y"],
          ham: ["a", "d"],
          foo: [1, 10],
        })
        .lazy();

      let actual = df
        .lazy()
        .join(otherDF, {
          on: "ham",
          how: "full",
          coalesce: false,
        })
        .collectSync();
      let expected = pl.DataFrame({
        foo: [1, null, 2, 3],
        bar: [6, null, 7, 8],
        ham: ["a", null, "b", "c"],
        apple: ["x", "y", null, null],
        hamright: ["a", "d", null, null],
        fooright: [1, 10, null, null],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
      actual = df
        .lazy()
        .join(otherDF, {
          on: "ham",
          how: "full",
          coalesce: true,
        })
        .collectSync();
      expected = pl.DataFrame({
        foo: [1, null, 2, 3],
        bar: [6, null, 7, 8],
        ham: ["a", "d", "b", "c"],
        apple: ["x", "y", null, null],
        fooright: [1, 10, null, null],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("suffix", () => {
      const actual = df
        .lazy()
        .join(otherDF, {
          on: "ham",
          how: "left",
          suffix: "_other",
        })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
        apple: ["x", "y", null],
        foo_other: [1, 10, null],
      });
      assertFrameEqualIgnoringOrder(actual, expected);
    });
    test("coalesce:false", () => {
      const actual = df
        .lazy()
        .join(otherDF, {
          on: "ham",
          how: "left",
          suffix: "_other",
          coalesce: false,
        })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
        apple: ["x", "y", null],
        ham_other: ["a", "b", null],
        foo_other: [1, 10, null],
      });
      assertFrameEqual(actual, expected);
    });
    test("joinAsof", () => {
      let actual = df.lazy().joinAsof(otherDF, { on: "ham" }).collectSync();
      assert.deepStrictEqual(actual.shape, { height: 3, width: 5 });
      actual = df
        .lazy()
        .joinAsof(otherDF, { leftOn: "ham", rightOn: "ham" })
        .collectSync();
      assert.deepStrictEqual(actual.shape, { height: 3, width: 5 });
      let fn = () =>
        df.lazy().joinAsof(otherDF, { leftOn: "ham" }).collectSync();
      assert.throws(fn);
      fn = () =>
        df
          .lazy()
          .joinAsof(otherDF, { leftOn: "ham", rightOn: "ham", byLeft: "ham" })
          .collectSync();
      assert.throws(fn);
      fn = () =>
        df
          .lazy()
          .joinAsof(otherDF, { byLeft: "ham", byRight: "ham" })
          .collectSync();
      assert.throws(fn);
      actual = df
        .lazy()
        .joinAsof(otherDF, {
          leftOn: "ham",
          rightOn: "ham",
          byLeft: "ham",
          byRight: "ham",
        })
        .collectSync();
      assert.deepStrictEqual(actual.shape, { height: 3, width: 5 });
      actual = df
        .lazy()
        .joinAsof(otherDF, {
          leftOn: "ham",
          rightOn: "ham",
          byLeft: ["ham"],
          byRight: ["ham"],
        })
        .collectSync();
      assert.deepStrictEqual(actual.shape, { height: 3, width: 5 });
      actual = df
        .lazy()
        .joinAsof(otherDF, { leftOn: "ham", rightOn: "ham", by: ["ham"] })
        .collectSync();
      assert.deepStrictEqual(actual.shape, { height: 3, width: 5 });
    });
  });
  test("last", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .lazy()
      .last()
      .collectSync();
    const expected = pl.DataFrame({
      foo: [3],
      ham: ["c"],
    });
    assertFrameEqual(actual, expected);
  });
  test("limit", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .lazy()
      .limit(1)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1],
      ham: ["a"],
    });
    assertFrameEqual(actual, expected);
  });
  test("max", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 11],
      })
      .lazy()
      .max()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [4],
      bar: [11],
    });
    assertFrameEqual(actual, expected);
  });
  test("mean", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .mean()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [2.75],
      bar: [1.75],
    });
    assertFrameEqual(actual, expected);
  });
  test("median", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .median()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [2.5],
      bar: [1.5],
    });
    assertFrameEqual(actual, expected);
  });
  test("min", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 11],
      })
      .lazy()
      .min()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [2],
      bar: [1],
    });
    assertFrameEqual(actual, expected);
  });
  test("quantile", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .lazy()
      .quantile(0.5)
      .collectSync();
    assert.deepStrictEqual(actual.row(0), [2, 7, null]);
  });
  test("rename", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        bar: [6, 7, 8],
        ham: ["a", "b", "c"],
      })
      .lazy()
      .rename({
        foo: "foo_new",
        bar: "bar_new",
        ham: "ham_new",
      })
      .collectSync();
    assert.deepStrictEqual(actual.columns, ["foo_new", "bar_new", "ham_new"]);
  });
  test("reverse", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .reverse()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [4, 3, 2, 2],
      bar: [1, 1, 3, 2],
    });
    assertFrameEqual(actual, expected);
  });
  test("tail", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3],
        ham: ["a", "b", "c"],
      })
      .lazy()
      .tail(1)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [3],
      ham: ["c"],
    });
    assertFrameEqual(actual, expected);
  });
  test("select:single", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
        ham: ["a", "b", "c", null],
      })
      .lazy()
      .select("ham")
      .collectSync();
    const ham = pl.Series("ham", ["a", "b", "c", null]);
    assert.deepStrictEqual(actual.width, 1);
    assert.throws(() => actual.getColumn("foo"));
    assertSeriesEqual(actual.getColumn("ham"), ham);
  });
  test("select:strings", () => {
    const columns = ["ham", "foo"];
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
        ham: ["a", "b", "c", null],
      })
      .lazy()
      .select(...columns)
      .collectSync();
    const foo = pl.Series("foo", [1, 2, 3, 1]);
    const ham = pl.Series("ham", ["a", "b", "c", null]);
    assert.deepStrictEqual(actual.width, 2);
    assertSeriesEqual(actual.getColumn("foo"), foo);
    assertSeriesEqual(actual.getColumn("ham"), ham);
  });
  test("select:expr", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
        ham: ["a", "b", "c", null],
      })
      .lazy()
      .select(pl.col("foo"), "ham")
      .collectSync();
    const foo = pl.Series("foo", [1, 2, 3, 1]);
    const ham = pl.Series("ham", ["a", "b", "c", null]);
    assert.deepStrictEqual(actual.width, 2);
    assertSeriesEqual(actual.getColumn("foo"), foo);
    assertSeriesEqual(actual.getColumn("ham"), ham);
  });
  test("shift:pos", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .shift(1)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [null, 1, 2, 3],
      bar: [null, 6, 7, 8],
    });
    assertFrameEqual(actual, expected);
  });
  test("shift:neg", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .shift(-1)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [2, 3, 1, null],
      bar: [7, 8, 1, null],
    });
    assertFrameEqual(actual, expected);
  });
  test("shiftAndFill:positional", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .shiftAndFill(-1, 99)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [2, 3, 1, 99],
      bar: [7, 8, 1, 99],
    });
    assertFrameEqual(actual, expected);
  });
  test("shiftAndFill:named", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .shiftAndFill({ n: -1, fillValue: 99 })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [2, 3, 1, 99],
      bar: [7, 8, 1, 99],
    });
    assertFrameEqual(actual, expected);
  });
  test("shiftAndFill:expr", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .shiftAndFill({ n: -1, fillValue: 99 })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [2, 3, 1, 99],
      bar: [7, 8, 1, 99],
    });
    assertFrameEqual(actual, expected);
  });
  test("slice:positional", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .slice(0, 2)
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: [6, 7],
    });
    assertFrameEqual(actual, expected);
  });
  test("slice:named", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .slice({ offset: 0, length: 2 })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: [6, 7],
    });
    assertFrameEqual(actual, expected);
  });
  test("sort:positional", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .sort("bar")
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 1, 2, 3],
      bar: [1, 6, 7, 8],
    });
    assertFrameEqual(actual, expected);
  });
  test("sort:named", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, 1],
        bar: [6, 7, 8, 1],
      })
      .lazy()
      .sort({ by: "bar", descending: true })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [3, 2, 1, 1],
      bar: [8, 7, 6, 1],
    });
    assertFrameEqual(actual, expected);
  });
  test("sort:multi-args", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 3, -1],
        bar: [6, 7, 8, 2],
        baz: ["a", "b", "d", "A"],
      })
      .lazy()
      .sort({
        by: [pl.col("baz"), pl.col("bar")],
      })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [-1, 1, 2, 3],
      bar: [2, 6, 7, 8],
      baz: ["A", "a", "b", "d"],
    });
    assertFrameEqual(actual, expected);
  });
  test("sort:nulls_last:false", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
      })
      .lazy()
      .sort({ by: "foo", nullsLast: false })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [null, 1, 2, 3],
    });
    assertFrameEqual(actual, expected);
  });
  test("sort:nulls_last:true", () => {
    const actual = pl
      .DataFrame({
        foo: [1, null, 2, 3],
      })
      .lazy()
      .sort({ by: "foo", nullsLast: true })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [1, 2, 3, null],
    });
    assertFrameEqual(actual, expected);
  });
  test("sum", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 11],
      })
      .lazy()
      .sum()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [11],
      bar: [17],
    });
    assertFrameEqual(actual, expected);
  });
  test("std", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .std()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [0.9574271077563381],
      bar: [0.9574271077563381],
    });
    assertFrameEqual(actual, expected);
  });
  test("var", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .var()
      .collectSync();

    const expected = pl.DataFrame({
      foo: [0.9166666666666666],
      bar: [0.9166666666666666],
    });
    assertFrameEqual(actual, expected);
  });
  test("withColumn", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy()
      .withColumn(pl.lit("a").alias("col_a"))
      .collectSync();

    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
    ]);
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("withColumn:series", async () => {
    const actual: pl.DataFrame = pl
      .DataFrame()
      .lazy()
      .withColumn(pl.Series("series1", [1, 2, 3, 4], pl.Int16))
      .collectSync();
    const expected: pl.DataFrame = pl.DataFrame([
      pl.Series("series1", [1, 2, 3, 4], pl.Int16),
    ]);
    assertFrameEqual(actual, expected);
  });
  test("withColumns:series", async () => {
    const actual: pl.DataFrame = pl
      .DataFrame()
      .lazy()
      .withColumns(
        pl.Series("series1", [1, 2, 3, 4], pl.Int16),
        pl.Series("series2", [1, 2, 3, 4], pl.Int32),
      )
      .collectSync();
    const expected: pl.DataFrame = pl.DataFrame([
      pl.Series("series1", [1, 2, 3, 4], pl.Int16),
      pl.Series("series2", [1, 2, 3, 4], pl.Int32),
    ]);
    assertFrameEqual(actual, expected);
  });
  test("select:series", async () => {
    let actual: pl.DataFrame = pl
      .DataFrame()
      .lazy()
      .select(
        pl.Series("series1", [1, 2, 3, 4], pl.Int16),
        pl.Series("series2", [1, 2, 3, 4], pl.Int32),
      )
      .collectSync();
    let expected: pl.DataFrame = pl.DataFrame([
      pl.Series("series1", [1, 2, 3, 4], pl.Int16),
      pl.Series("series2", [1, 2, 3, 4], pl.Int32),
    ]);
    assertFrameEqual(actual, expected);
    actual = pl
      .DataFrame({ text: ["hello"] })
      .lazy()
      .select(pl.Series("series", [1, 2, 3]))
      .collectSync();

    expected = pl.DataFrame([pl.Series("series", [1, 2, 3])]);
    assertFrameEqual(actual, expected);

    actual = pl
      .DataFrame({ text: ["hello"] })
      .lazy()
      .select("text", pl.Series("series", [1]))
      .collectSync();
    expected = pl.DataFrame({ text: ["hello"], series: [1] });
    assertFrameEqual(actual, expected);
  });
  test("select:lit", () => {
    const df = pl.DataFrame({ a: [1] }, { schema: { a: pl.Float32 } });
    let actual = df.lazy().select(pl.col("a"), pl.lit(1)).collectSync();
    const expected = pl.DataFrame({
      a: [1],
      literal: [1],
    });
    assertFrameEqual(actual, expected);
    actual = df
      .lazy()
      .select(pl.col("a").mul(2).alias("b"), pl.lit(2))
      .collectSync();
    const expected2 = pl.DataFrame({
      b: [2],
      literal: [2],
    });
    assertFrameEqual(actual, expected2);
  });
  test("inspect", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy();
    assert.notStrictEqual(actual, undefined);
  });
  test("LazyDataFrame.isLazyDataFrame", () => {
    const ldf = pl.DataFrame({ a: [1, 2] }).lazy();
    assert.strictEqual(pl.LazyDataFrame.isLazyDataFrame(ldf), true);
    assert.strictEqual(pl.LazyDataFrame.isLazyDataFrame({}), false);
  });
  test("LazyDataFrame.deserialize", () => {
    const ldf = pl.DataFrame({ foo: [1, 2], bar: [3, 4] }).lazy();

    const json = ldf.serialize("json");
    const actualJson = pl.LazyDataFrame.deserialize(json, "json").collectSync();
    assertFrameEqual(actualJson, ldf.collectSync());

    const bincode = ldf.serialize("bincode");
    const actualBincode = pl.LazyDataFrame.deserialize(
      bincode,
      "bincode",
    ).collectSync();
    assertFrameEqual(actualBincode, ldf.collectSync());
  });
  test("LazyDataFrame.fromExternal", () => {
    const ldf = pl.DataFrame({ foo: [1, 2], bar: ["a", "b"] }).lazy();
    const external = ldf.inner();

    const cloned = pl.LazyDataFrame.fromExternal(external);

    assert.strictEqual(pl.LazyDataFrame.isLazyDataFrame(cloned), true);
    assertFrameEqual(cloned.collectSync(), ldf.collectSync());
  });
  test("toJSON", () => {
    const ldf = pl
      .DataFrame({
        foo: [1, 2],
        bar: ["a", "b"],
      })
      .lazy();

    const directJson = ldf.toJSON();
    const rootJson = JSON.parse(JSON.stringify(ldf));
    const nestedJson = JSON.parse(JSON.stringify({ ldf }));

    assert.strictEqual(typeof directJson, "string");
    assert.notStrictEqual(JSON.parse(directJson), undefined);
    assert.strictEqual(typeof rootJson, "object");
    assert.strictEqual(typeof nestedJson.ldf, "string");
    assert.notStrictEqual(JSON.parse(nestedJson.ldf), undefined);
  });
  test("withColumns", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy()
      .withColumns(pl.lit("a").alias("col_a"), pl.lit("b").alias("col_b"))
      .collectSync();
    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
      pl.Series("col_b", ["b", "b", "b"], pl.Utf8),
    ]);
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("withColumns:array", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy()
      .withColumns(pl.lit("a").alias("col_a"), pl.lit("b").alias("col_b"))
      .collectSync();
    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
      pl.Series("col_b", ["b", "b", "b"], pl.Utf8),
    ]);
    assertFrameEqualIgnoringOrder(actual, expected);
  });
  test("withColumnRenamed", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy()
      .withColumnRenamed("foo", "apple")
      .collectSync();

    const expected = pl.DataFrame([
      pl.Series("apple", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
    ]);
    assertFrameEqual(actual, expected);
  });
  test("withRowCount", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy()
      .withRowCount()
      .collectSync();

    const expected = pl.DataFrame([
      pl.Series("row_nr", [0, 1, 2], pl.UInt32),
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
    ]);
    assertFrameEqual(actual, expected);
  });
  test("withRowIndex", () => {
    const ldf = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy();

    let actual = ldf.withRowIndex().collectSync();
    let expected = ldf.collectSync();
    expected.insertAtIdx(0, pl.Series("index", [0, 1, 2], pl.Int16));
    assertFrameEqual(actual, expected);
    actual = ldf.withRowIndex("idx", 100).collectSync();
    expected = ldf.collectSync();
    expected.insertAtIdx(0, pl.Series("idx", [100, 101, 102], pl.Int16));
    assertFrameEqual(actual, expected);
  });
  test("tail", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .tail(1)
      .collectSync();

    const expected = pl.DataFrame({
      foo: [4],
      bar: [1],
    });
    assertFrameEqual(actual, expected);
  });
  test("head", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .head(1)
      .collectSync();

    const expected = pl.DataFrame({
      foo: [2],
      bar: [2],
    });
    assertFrameEqual(actual, expected);
  });
  test("limit", () => {
    const actual = pl
      .DataFrame({
        foo: [2, 2, 3, 4],
        bar: [2, 3, 1, 1],
      })
      .lazy()
      .limit(1)
      .collectSync();

    const expected = pl.DataFrame({
      foo: [2],
      bar: [2],
    });
    assertFrameEqual(actual, expected);
  });
  test("str:padStart", () => {
    const actual = pl
      .DataFrame({
        ham: ["a", "b", "c"],
      })
      .lazy()
      .withColumn(pl.col("ham").str.padStart(3, "-"))
      .collectSync();
    const expected = pl.DataFrame({
      ham: ["--a", "--b", "--c"],
    });
    assertFrameEqual(actual, expected);
  });
  test("str:padEnd", () => {
    const actual = pl
      .DataFrame({
        ham: ["a", "b", "c"],
      })
      .lazy()
      .withColumn(pl.col("ham").str.padEnd(3, "-"))
      .collectSync();
    const expected = pl.DataFrame({
      ham: ["a--", "b--", "c--"],
    });
    assertFrameEqual(actual, expected);
  });
  test("str:zFill", () => {
    const actual = pl
      .DataFrame({
        ham: ["a", "b", "c"],
      })
      .lazy()
      .withColumn(pl.col("ham").str.zFill(3))
      .collectSync();
    const expected = pl.DataFrame({
      ham: ["00a", "00b", "00c"],
    });
    assertFrameEqual(actual, expected);
  });
  test("json:extract", () => {
    const expected = pl.DataFrame({
      json: [
        { a: 1, b: true },
        { a: null, b: null },
        { a: 2, b: false },
      ],
    });
    const ldf = pl
      .DataFrame({
        json: [
          '{"a": 1, "b": true}',
          '{"a": null, "b": null }',
          '{"a": 2, "b": false}',
        ],
      })
      .lazy();
    const dtype = pl.Struct([
      new pl.Field("a", pl.Int64),
      new pl.Field("b", pl.Bool),
    ]);
    const actual = ldf
      .select(pl.col("json").str.jsonDecode(dtype))
      .collectSync();
    assertFrameEqual(actual, expected);
  });
  test("sinkCSV:path", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("foo", [1, 2, 3], pl.Int64),
        pl.Series("bar", ["a", "b", "c"]),
      ])
      .lazy();
    await ldf.sinkCSV("./test.csv").collect();
    const newDF: pl.DataFrame = pl.readCSV("./test.csv");
    const actualDf: pl.DataFrame = await ldf.collect({
      streaming: true,
      noOptimization: true,
    });
    assertFrameEqual(newDF.sort("foo"), actualDf);
    fs.rmSync("./test.csv");
  });
  test("sinkCSV:noHeader", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("column_1", [1, 2, 3], pl.Int64),
        pl.Series("column_2", ["a", "b", "c"]),
      ])
      .lazy();
    await ldf.sinkCSV("./test.csv", { includeHeader: false }).collect();
    const newDF: pl.DataFrame = pl.readCSV("./test.csv", { hasHeader: false });
    const actualDf: pl.DataFrame = await ldf.collect();
    assertFrameEqual(newDF.sort("column_1"), actualDf);
    fs.rmSync("./test.csv");
  });
  test("sinkCSV:separator", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("foo", [1, 2, 3], pl.Int64),
        pl.Series("bar", ["a", "b", "c"]),
      ])
      .lazy();
    await ldf.sinkCSV("./test.csv", { separator: "|" }).collect();
    const newDF: pl.DataFrame = pl.readCSV("./test.csv", { sep: "|" });
    const actualDf: pl.DataFrame = await ldf.collect();
    assertFrameEqual(newDF.sort("foo"), actualDf);
    fs.rmSync("./test.csv");
  });
  test("sinkCSV:nullValue", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("foo", [1, 2, 3], pl.Int64),
        pl.Series("bar", ["a", "b", null]),
      ])
      .lazy();
    await ldf.sinkCSV("./test.csv", { nullValue: "BOOM" }).collect();
    const newDF: pl.DataFrame = pl.readCSV("./test.csv", { sep: "," });
    const actualDf: pl.DataFrame = await (await ldf.collect()).withColumn(
      pl.col("bar").fillNull("BOOM"),
    );
    assertFrameEqual(newDF.sort("foo"), actualDf);
    fs.rmSync("./test.csv");
  });
  test("sinkParquet:path", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("foo", [1, 2, 3], pl.Int64),
        pl.Series("bar", ["a", "b", "c"]),
      ])
      .lazy();
    await ldf.sinkParquet("./test.parquet").collect();
    const newDF: pl.DataFrame = pl.readParquet("./test.parquet");
    const actualDf: pl.DataFrame = await ldf.collect();
    assertFrameEqual(newDF.sort("foo"), actualDf);
    fs.rmSync("./test.parquet");
  });
  test("sinkParquet:compression:gzip", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("foo", [1, 2, 3], pl.Int64),
        pl.Series("bar", ["a", "b", "c"]),
      ])
      .lazy();
    await ldf.sinkParquet("./test.parquet", { compression: "gzip" }).collect();
    const newDF: pl.DataFrame = pl.readParquet("./test.parquet");
    const actualDf: pl.DataFrame = await ldf.collect();
    assertFrameEqual(newDF.sort("foo"), actualDf);
    fs.rmSync("./test.parquet");
  });
  test("sinkNdJson:path", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("foo", [1, 2, 3], pl.Int64),
        pl.Series("bar", ["a", "b", "c"]),
      ])
      .lazy();
    await ldf.sinkNdJson("./test.ndjson").collect();
    let df = pl.scanJson("./test.ndjson").collectSync();
    assert.deepStrictEqual(df.shape, { height: 3, width: 2 });

    await ldf
      .sinkNdJson("./test.ndjson", {
        retries: 1,
        syncOnClose: "all",
        maintainOrder: false,
      })
      .collect();
    df = pl.scanJson("./test.ndjson").collectSync();
    assert.deepStrictEqual(df.shape, { height: 3, width: 2 });

    fs.rmSync("./test.ndjson");
  });
  test("sinkIpc:path", async () => {
    const ldf = pl
      .DataFrame([
        pl.Series("foo", [1, 2, 3], pl.Int64),
        pl.Series("bar", ["a", "b", "c"]),
      ])
      .lazy();
    await ldf.sinkIpc("./test.ipc").collect();
    let df = pl.scanIPC("./test.ipc").collectSync();
    assert.deepStrictEqual(df.shape, { height: 3, width: 2 });

    await ldf
      .sinkIpc("./test.ipc", {
        retries: 1,
        syncOnClose: "all",
        maintainOrder: false,
        compression: "lz4",
      })
      .collect();
    df = pl.scanIPC("./test.ipc").collectSync();
    assert.deepStrictEqual(df.shape, { height: 3, width: 2 });
    fs.rmSync("./test.ipc");
  });
  test("unpivot renamed", () => {
    const ldf = pl
      .DataFrame({
        id: [1],
        asset_key_1: ["123"],
        asset_key_2: ["456"],
        asset_key_3: ["abc"],
      })
      .lazy();
    const actual = ldf.unpivot(
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
    assertFrameEqual(actual.collectSync(), expected);
  });
  test("unique:invalid argument", () => {
    const ldf = pl.DataFrame({ foo: [1, 1, 2] }).lazy();
    const fn = () => ldf.unique(123 as any);
    assert.throws(fn, TypeError);
  });
});
