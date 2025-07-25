import { DataFrame, type LazyDataFrame, _LazyDataFrame } from ".";
import pli from "./internals/polars_internal";
const INSPECT = Symbol.for("nodejs.util.inspect.custom");

/**
 * Run SQL queries against DataFrame/LazyFrame data.
 *
 * @experimental This functionality is considered **unstable**, although it is close to being
 * considered stable. It may be changed at any point without it being considered a breaking change.
 */
export interface SQLContext {
  /**
   * Parse the given SQL query and execute it against the registered frame data.
   *
   * @param query - A valid string SQL query.
   * @param eager - Apply the query eagerly, returning `DataFrame` instead of `LazyFrame`.
   *                If unset, the value of the init-time parameter "eager_execution" will be
   *                used. (Note that the query itself is always executed in lazy-mode; this
   *                parameter only impacts the type of the returned frame).
   *
   * @example
   * Declare frame data and register with a SQLContext:
   *
   * ```ts
   * const df = pl.DataFrame({
   *   data: [
   *     ("The Godfather", 1972, 6_000_000, 134_821_952, 9.2),
   *     ("The Dark Knight", 2008, 185_000_000, 533_316_061, 9.0),
   *     ("Schindler's List", 1993, 22_000_000, 96_067_179, 8.9),
   *     ("Pulp Fiction", 1994, 8_000_000, 107_930_000, 8.9),
   *     ("The Shawshank Redemption", 1994, 25_000_000, 28_341_469, 9.3),
   *   ],
   *   schema: ["title", "release_year", "budget", "gross", "imdb_score"],
   * });
   * const ctx = pl.SQLContext({ films: df });
   * ```
   *
   * Execute a SQL query against the registered frame data:
   *
   * ```ts
   * const result = ctx.execute(`
   *   SELECT title, release_year, imdb_score
   *   FROM films
   *   WHERE release_year > 1990
   *   ORDER BY imdb_score DESC
   * `, { eager: true });
   * console.log(result);
   * // shape: (4, 3)
   * // ┌──────────────────────────┬──────────────┬────────────┐
   * // │ title                    ┆ release_year ┆ imdb_score │
   * // │ ---                      ┆ ---          ┆ ---        │
   * // ╞══════════════════════════╪══════════════╪════════════╡
   * // │ The Shawshank Redemption ┆ 1994         ┆ 9.3        │
   * // │ The Dark Knight          ┆ 2008         ┆ 9.0        │
   * // │ Schindler's List         ┆ 1993         ┆ 8.9        │
   * // │ Pulp Fiction             ┆ 1994         ┆ 8.9        │
   * // └──────────────────────────┴──────────────┴────────────┘
   * ```
   *
   * Execute a GROUP BY query:
   *
   * ```ts
   * ctx.execute(`
   *   SELECT
   *       MAX(release_year / 10) * 10 AS decade,
   *       SUM(gross) AS total_gross,
   *       COUNT(title) AS n_films,
   *   FROM films
   *   GROUP BY (release_year / 10) -- decade
   *   ORDER BY total_gross DESC
   * `, { eager: true });
   * // shape: (3, 3)
   * // ┌────────┬─────────────┬─────────┐
   * // │ decade ┆ total_gross ┆ n_films │
   * // │ ---    ┆ ---         ┆ ---     │
   * // ╞════════╪═════════════╪═════════╡
   * // │ 2000   ┆ 533316061   ┆ 1       │
   * // │ 1990   ┆ 232338648   ┆ 3       │
   * // │ 1970   ┆ 134821952   ┆ 1       │
   * // └────────┴─────────────┴─────────┘
   * ```
   */
  execute(query: string, { eager }: { eager: true }): DataFrame;
  execute(query: string, { eager }: { eager: false }): LazyDataFrame;
  execute(query: string): LazyDataFrame;
  /**
   * Register a single frame as a table, using the given name.
   *
   * Parameters
   * ----------
   * name : string
   *     Name of the table.
   * frame : DataFrame | LazyFrame | null
   *     Eager/lazy frame to associate with this table name.
   *
   * See Also
   * --------
   * register_globals
   * register_many
   * unregister
   *
   * Examples
   * --------
   * const df = pl.DataFrame({"hello": ["world"]});
   * const ctx = pl.SQLContext();
   * ctx.register("frame_data", df).execute("SELECT * FROM frame_data").collect();
   * returns: shape: (1, 1)
   * ┌───────┐
   * │ hello │
   * │ ---   │
   * │ str   │
   * ╞═══════╡
   * │ world │
   * └───────┘
   */
  register(name: string, frame: DataFrame | LazyDataFrame | null): SQLContext;

  /**
   * Register multiple DataFrames as tables, using the associated names.
   *
   * @param {Object} frames An `{name: df, ...}` mapping.
   *
   * @returns {SQLContext} The SQLContext with registered DataFrames.
   *
   * @see register
   * @see unregister
   *
   * @example
   * const lf1 = pl.DataFrame({"a": [1, 2, 3], "b": ["m", "n", "o"]});
   * const lf2 = pl.DataFrame({"a": [2, 3, 4], "c": ["p", "q", "r"]});
   *
   * // Register multiple DataFrames at once
   * const ctx = pl.SQLContext().registerMany({"tbl1": lf1, "tbl2": lf2});
   * console.log(ctx.tables());
   * // Output: ['tbl1', 'tbl2']
   */
  registerMany(frames: Record<string, DataFrame | LazyDataFrame>): SQLContext;
  /**
   * Unregister one or more eager/lazy frames by name.
   *
   * @param names - Names of the tables to unregister.
   *
   * @remarks
   * You can also control table registration lifetime by using `SQLContext` as a
   * context manager; this can often be more useful when such control is wanted.
   *
   * Frames registered in-scope are automatically unregistered on scope-exit. Note
   * that frames registered on construction will persist through subsequent scopes.
   *
   * @example
   * ```ts
   * const df0 = pl.DataFrame({"colx": [0, 1, 2]});
   * const df1 = pl.DataFrame({"colx": [1, 2, 3]});
   * const df2 = pl.DataFrame({"colx": [2, 3, 4]});
   *
   * // Register one frame at construction time, and the other two in-scope
   * const ctx = pl.SQLContext({ tbl0: df0 });
   * ctx.register("tbl1", df1);
   * ctx.register("tbl2", df2);
   * console.log(ctx.tables()); // Output: ['tbl0', 'tbl1', 'tbl2']
   *
   * // After scope exit, none of the tables registered in-scope remain
   * ```
   *
   * @see register
   * @see register_globals
   * @see register_many
   *
   * @example
   * ```ts
   * const df0 = pl.DataFrame({"ints": [9, 8, 7, 6, 5]});
   * const lf1 = pl.LazyDataFrame({"text": ["a", "b", "c"]});
   * const lf2 = pl.LazyDataFrame({"misc": ["testing1234"]});
   *
   * // Register with a SQLContext object
   * const ctx = pl.SQLContext({ test1: df0, test2: lf1, test3: lf2 });
   * console.log(ctx.tables()); // Output: ['test1', 'test2', 'test3']
   *
   * // Unregister one or more of the tables
   * ctx.unregister(["test1", "test3"]);
   * console.log(ctx.tables()); // Output: ['test2']
   * ctx.unregister("test2");
   * console.log(ctx.tables()); // Output: []
   * ```
   */
  unregister(names: string | string[]): SQLContext;

  /**
   * Returns a list of the registered table names.
   *
   * @remarks
   * The `tables` method will return the same values as the "SHOW TABLES" SQL statement, but as a list instead of a frame.
   *
   * Executing as SQL:
   * ```ts
   * const frame_data = pl.DataFrame({"hello": ["world"]});
   * const ctx = pl.SQLContext({ hello_world: frame_data });
   * console.log(ctx.execute("SHOW TABLES", { eager: true }));
   * // shape: (1, 1)
   * // ┌─────────────┐
   * // │ name        │
   * // │ ---         │
   * // │ str         │
   * // ╞═════════════╡
   * // │ hello_world │
   * // └─────────────┘
   * ```
   *
   * Calling the method:
   * ```ts
   * console.log(ctx.tables());
   * // ['hello_world']
   * ```
   *
   * @example
   * ```ts
   * const df1 = pl.DataFrame({"hello": ["world"]});
   * const df2 = pl.DataFrame({"foo": ["bar", "baz"]});
   * const ctx = pl.SQLContext({ hello_data: df1, foo_bar: df2 });
   * console.log(ctx.tables());
   * // ['foo_bar', 'hello_data']
   * ```
   *
   * @returns {string[]} An array of the registered table names.
   */
  tables(): string[];
}

export class SQLContext implements SQLContext {
  #ctx: any; // native SQLContext
  [INSPECT](): string {
    return `SQLContext: {${this.#ctx.getTables().join(", ")}}`;
  }

  constructor(frames?: Record<string, DataFrame | LazyDataFrame>) {
    this.#ctx = new pli.SqlContext();
    for (const [name, frame] of Object.entries(frames ?? {})) {
      if (DataFrame.isDataFrame(frame)) {
        this.#ctx.register(name, frame._df.lazy());
      } else {
        this.#ctx.register(name, frame._ldf);
      }
    }
  }

  execute(query: string): LazyDataFrame;
  execute(query: string, { eager }: { eager: true }): DataFrame;
  execute(query: string, { eager }: { eager: false }): LazyDataFrame;
  execute(
    this: SQLContext,
    query: string,
    { eager } = { eager: false },
  ): LazyDataFrame | DataFrame {
    const lf_ = this.#ctx.execute(query);
    const lf = _LazyDataFrame(lf_);
    if (eager) {
      return lf.collectSync();
    }

    return lf;
  }

  register(
    this: SQLContext,
    name: string,
    frame: DataFrame | LazyDataFrame | null,
  ): SQLContext {
    if (frame == null) {
      frame = DataFrame().lazy();
    } else if (DataFrame.isDataFrame(frame)) {
      frame = frame.lazy();
    }
    this.#ctx.register(name, frame._ldf);
    return this;
  }

  registerMany(
    this: SQLContext,
    frames: Record<string, DataFrame | LazyDataFrame>,
  ): SQLContext {
    for (const [name, frame] of Object.entries(frames)) {
      this.register(name, frame);
    }
    return this;
  }

  unregister(names: string | string[]): SQLContext {
    if (typeof names === "string") {
      names = [names];
    }
    for (const name of names) {
      this.#ctx.unregister(name);
    }
    return this;
  }

  tables(): string[] {
    return this.#ctx.getTables();
  }
}
