import { DataFrame } from "@polars/dataframe";
import { Series } from "@polars/series";

declare global {
  type DoneCallback = (error?: unknown) => void;
  type CompatTestFn = {
    (name: string, fn: any): void;
    each(
      cases: readonly unknown[] | TemplateStringsArray,
      ...values: unknown[]
    ): (name: string, fn: any) => void;
  };

  const describe: typeof import("node:test").describe;
  const it: CompatTestFn;
  const test: CompatTestFn;
  const beforeAll: typeof import("node:test").before;
  const beforeEach: typeof import("node:test").beforeEach;
  const afterAll: typeof import("node:test").after;
  const afterEach: typeof import("node:test").afterEach;
  const assert: typeof import("node:assert/strict");
  function assertSeriesEqual(actual: Series<any>, expected: Series<any>): void;
  function assertSeriesStrictEqual(
    actual: Series<any>,
    expected: Series<any>,
  ): void;
  function assertFrameEqual(
    actual: DataFrame,
    expected: DataFrame,
    nullEqual?: boolean,
  ): void;
  function assertFrameStrictEqual(actual: DataFrame, expected: DataFrame): void;
  function assertFrameEqualIgnoringOrder(
    actual: DataFrame,
    expected: DataFrame,
  ): void;
}

export {};
