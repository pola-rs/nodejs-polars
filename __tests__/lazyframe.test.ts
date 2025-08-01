import fs from "node:fs";
import pl from "@polars";

describe("lazyframe", () => {
  test("columns", () => {
    const df = pl
      .DataFrame({
        foo: [1, 2],
        bar: ["a", "b"],
      })
      .lazy();
    const actual = df.columns;
    expect(actual).toEqual(["foo", "bar"]);
  });
  test("collectSync", () => {
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const actual = expected.lazy().collectSync();
    expect(actual).toFrameEqual(expected);
  });
  test("collect", async () => {
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const actual = await expected.lazy().collect();
    expect(actual).toFrameEqual(expected);
  });
  test("collect:streaming", async () => {
    const expected = pl.DataFrame({
      foo: [1, 2],
      bar: ["a", "b"],
    });
    const actual = await expected.lazy().collect({ streaming: true });
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toEqual(expected);
    actual = df.describePlan().replace(/\s+/g, " ");
    expect(actual).toEqual(expected);
  });
  test("cache", () => {
    const df = pl.DataFrame({
      foo: [1, 2, 3],
    });
    const expected = pl.DataFrame({
      foo: [1, 2, 3],
    });
    let actual = df.lazy().cache().collectSync();
    expect(actual).toFrameEqual(expected);
    actual = df.lazy().clone().collectSync();
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
    const actual = df.lazy().drop("apple").collectSync();
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
  });
  test("unique", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 2, 3],
        bar: [1, 2, 2, 4],
        ham: ["a", "d", "d", "c"],
      })
      .lazy()
      .unique()
      .collectSync();
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
      .lazy()
      .unique({ subset: ["foo", "ham"] })
      .collectSync();
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
        .lazy()
        .unique({ maintainOrder: true })
        .collectSync();
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
        .lazy()
        .unique({ maintainOrder: true, subset: "foo" })
        .collectSync();
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
        .lazy()
        .unique({ maintainOrder: true, subset: ["foo", "ham"] })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [0, 1, 2, 2],
        bar: [0, 1, 2, 2],
        ham: ["0", "a", "b", "c"],
      });
      expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    const actual = await df.lazy().select("*").fetch(1);
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
    actual = df.lazy().select("*").fetchSync(1, { noOptimization: true });
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
  });
  describe("groupby", () => {});
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
    expect(actual).toFrameEqual(expected);
  });
  describe("join", () => {
    test("on", () => {
      const df = pl.DataFrame({
        foo: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      });
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y", "z"],
          ham: ["a", "b", "d"],
        })
        .lazy();
      const actual = df.lazy().join(otherDF, { on: "ham" }).collectSync();

      const expected = pl.DataFrame({
        foo: [1, 2],
        bar: [6.0, 7.0],
        ham: ["a", "b"],
        apple: ["x", "y"],
      });
      expect(actual).toFrameEqualIgnoringOrder(expected);
    });
    test("on:multiple-columns", () => {
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
      expect(actual).toFrameEqualIgnoringOrder(expected);
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
      expect(actual).toFrameEqualIgnoringOrder(expected);
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
      expect(actual).toFrameEqualIgnoringOrder(expected);
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
      expect(f).toThrow(TypeError);
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
      expect(f).toThrow(TypeError);
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
      expect(actual).toFrameEqualIgnoringOrder(expected);
    });
    test("how:left", () => {
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
      expect(actual).toFrameEqualIgnoringOrder(expected);
    });
    test("how:full", () => {
      const df = pl.DataFrame({
        foo: [1, 2, 3],
        bar: [6.0, 7.0, 8.0],
        ham: ["a", "b", "c"],
      });
      const otherDF = pl
        .DataFrame({
          apple: ["x", "y"],
          ham: ["a", "d"],
          foo: [1, 10],
        })
        .lazy();

      const actual = df
        .lazy()
        .join(otherDF, {
          on: "ham",
          how: "full",
        })
        .collectSync();
      const expected = pl.DataFrame({
        foo: [1, null, 2, 3],
        bar: [6, null, 7, 8],
        ham: ["a", null, "b", "c"],
        apple: ["x", "y", null, null],
        hamright: ["a", "d", null, null],
        fooright: [1, 10, null, null],
      });
      expect(actual).toFrameEqualIgnoringOrder(expected);
    });
    test("suffix", () => {
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
      expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual.row(0)).toEqual([2, 7, null]);
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
    expect(actual.columns).toEqual(["foo_new", "bar_new", "ham_new"]);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual.width).toStrictEqual(1);
    expect(() => actual.getColumn("foo")).toThrow();
    expect(actual.getColumn("ham")).toSeriesEqual(ham);
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
      .lazy()
      .select(pl.col("foo"), "ham")
      .collectSync();
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
      .lazy()
      .shift(1)
      .collectSync();
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
      .lazy()
      .shift(-1)
      .collectSync();
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
      .lazy()
      .shiftAndFill(-1, 99)
      .collectSync();
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
      .lazy()
      .shiftAndFill({ n: -1, fillValue: 99 })
      .collectSync();
    const expected = pl.DataFrame({
      foo: [2, 3, 1, 99],
      bar: [7, 8, 1, 99],
    });
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqualIgnoringOrder(expected);
  });
  test("withColumns:array", () => {
    const actual = pl
      .DataFrame({
        foo: [1, 2, 9],
        bar: [6, 2, 8],
      })
      .lazy()
      .withColumns([pl.lit("a").alias("col_a"), pl.lit("b").alias("col_b")])
      .collectSync();
    const expected = pl.DataFrame([
      pl.Series("foo", [1, 2, 9], pl.Int16),
      pl.Series("bar", [6, 2, 8], pl.Int16),
      pl.Series("col_a", ["a", "a", "a"], pl.Utf8),
      pl.Series("col_b", ["b", "b", "b"], pl.Utf8),
    ]);
    expect(actual).toFrameEqualIgnoringOrder(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
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
    expect(actual).toFrameEqual(expected);
  });
  test("json:extract", () => {
    const expected = pl.DataFrame({
      json: [
        { a: 1, b: true },
        { a: null, b: null },
        { a: 2, b: false },
      ],
    });
    const actual = pl
      .DataFrame({
        json: [
          '{"a": 1, "b": true}',
          '{"a": null, "b": null }',
          '{"a": 2, "b": false}',
        ],
      })
      .lazy()
      .select(pl.col("json").str.jsonDecode())
      .collectSync();
    expect(actual).toFrameEqual(expected);
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
    const actualDf: pl.DataFrame = await ldf.collect({ streaming: true });
    expect(newDF.sort("foo")).toFrameEqual(actualDf);
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
    expect(newDF.sort("column_1")).toFrameEqual(actualDf);
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
    expect(newDF.sort("foo")).toFrameEqual(actualDf);
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
    expect(newDF.sort("foo")).toFrameEqual(actualDf);
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
    expect(newDF.sort("foo")).toFrameEqual(actualDf);
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
    expect(newDF.sort("foo")).toFrameEqual(actualDf);
    fs.rmSync("./test.parquet");
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
    expect(actual.collectSync()).toFrameEqual(expected);
  });
});
