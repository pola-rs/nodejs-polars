# Polars

Polars: Blazingly fast DataFrames in Rust, Python, Node.js, R and SQL

[![rust docs](https://github.com/pola-rs/nodejs-polars/actions/workflows/docs.yaml/badge.svg)](https://github.com/pola-rs/nodejs-polars/actions/workflows/docs.yaml/)
[![Build and test](https://github.com/pola-rs/nodejs-polars/actions/workflows/test-js.yaml/badge.svg)](https://github.com/pola-rs/nodejs-polars/actions/workflows/test-js.yaml)
[![](https://img.shields.io/crates/v/polars.svg)](https://crates.io/crates/polars)
[![PyPI Latest Release](https://img.shields.io/pypi/v/polars.svg)](https://pypi.org/project/polars/)
[![NPM Latest Release](https://img.shields.io/npm/v/nodejs-polars.svg)](https://www.npmjs.com/package/nodejs-polars)

Documentation: [Node.js](https://pola-rs.github.io/nodejs-polars/index.html)
-[ Rust](https://docs.rs/polars/latest/polars/)
-[ Python](https://pola-rs.github.io/polars/py-polars/html/reference/index.html)
-[ R](https://rpolars.github.io/index.html)
|StackOverflow: [ Node.js](https://stackoverflow.com/questions/tagged/nodejs-polars)
-[ Rust](https://stackoverflow.com/questions/tagged/rust-polars)
-[ Python](https://stackoverflow.com/questions/tagged/python-polars)
| [User Guide](https://pola-rs.github.io/polars/)
| [Discord](https://discord.gg/4UfP5cfBE7)

#### Note: This library is intended to work only with server side JS/TS (Node, Bun, Deno). For browser please see [js-polars](https://github.com/pola-rs/js-polars)

## Usage

### Type-safe API (TypeScript)

Polars for Node provides a strongly typed public API so dtypes and column names flow through your code.

Key features:
- Schema-aware builder: `df.$.col<K>(name: K)` returns `Expr<S[K], K>` typed to the DataFrame’s schema.
- withColumns overloads:
  - Record: `df.withColumns({ newCol: df.$.col("a").cast(pl.Int64), ... })`
  - Builder-callback: `df.withColumns(b => ({ x: b.col("a"), y: b.col("b").cast(pl.Int64) }))`
- groupBy.agg overloads:
  - Array builder: `.agg(g => [ g.col("x").sum().alias("x_sum"), ... ])`
  - Record builder: `.agg(g => ({ x_sum: g.col("x").sum(), ... }))`
  Both infer output names and dtypes.
- Expr typing: arithmetic/comparison/boolean/math/string/list/datetime/struct ops preserve dtype/name (e.g. `alias`, `cast`).

Example:

```ts
import pl, { DataType } from "nodejs-polars";

const df = pl.DataFrame({
  strings: ["a", "a", "b"],
  ints: [1, 2, 3],
  bools: [true, false, true],
})
  .withColumns(b => ({
    newColumns: b.col("bools").cast(DataType.Int64),
    someOtherNewColumns: b.col("ints").cast(DataType.Int64),
  }))
  .groupBy("strings", "bools")
  .agg(g => ({
    newColumns_sum: g.col("alnewColumnsps").sum(),
    someOtherNewColumns_sum: g.col("someOtherNewColumns").sum(),
  }));

// df: DataFrame<{ strings: String; bools: Bool; alps_sum: Int64; sara_sum: Int64 }>
```

### Importing

```js
// esm
import pl from 'nodejs-polars';

// require
const pl = require('nodejs-polars'); 
```

### Series

```js
> const fooSeries = pl.Series("foo", [1, 2, 3])
> fooSeries.sum()
6

// a lot operations support both positional and named arguments
// you can see the full specs in the docs or the type definitions
> fooSeries.sort(true)
> fooSeries.sort({descending: true})
shape: (3,)
Series: 'foo' [f64]
[
        3
        2
        1
]
> fooSeries.toArray()
[1, 2, 3]

// Series are 'Iterables' so you can use javascript iterable syntax on them
> [...fooSeries]
[1, 2, 3]

> fooSeries[0]
1

```

### DataFrame

```js
>const df = pl.DataFrame(
...   {
...     A: [1, 2, 3, 4, 5],
...     fruits: ["banana", "banana", "apple", "apple", "banana"],
...     B: [5, 4, 3, 2, 1],
...     cars: ["beetle", "audi", "beetle", "beetle", "beetle"],
...   }
... )
> df.sort("fruits").select(
...     "fruits",
...     "cars",
...     pl.lit("fruits").alias("literal_string_fruits"),
...     pl.col("B").filter(pl.col("cars").eq(pl.lit("beetle"))).sum(),
...     pl.col("A").filter(pl.col("B").gt(2)).sum().over("cars").alias("sum_A_by_cars"),
...     pl.col("A").sum().over("fruits").alias("sum_A_by_fruits"),
...     pl.col("A").reverse().over("fruits").flatten().alias("rev_A_by_fruits")
...   )
shape: (5, 8)
┌──────────┬──────────┬──────────────┬─────┬─────────────┬─────────────┬─────────────┐
│ fruits   ┆ cars     ┆ literal_stri ┆ B   ┆ sum_A_by_ca ┆ sum_A_by_fr ┆ rev_A_by_fr │
│ ---      ┆ ---      ┆ ng_fruits    ┆ --- ┆ rs          ┆ uits        ┆ uits        │
│ str      ┆ str      ┆ ---          ┆ i64 ┆ ---         ┆ ---         ┆ ---         │
│          ┆          ┆ str          ┆     ┆ i64         ┆ i64         ┆ i64         │
╞══════════╪══════════╪══════════════╪═════╪═════════════╪═════════════╪═════════════╡
│ "apple"  ┆ "beetle" ┆ "fruits"     ┆ 11  ┆ 4           ┆ 7           ┆ 4           │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ "apple"  ┆ "beetle" ┆ "fruits"     ┆ 11  ┆ 4           ┆ 7           ┆ 3           │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ "banana" ┆ "beetle" ┆ "fruits"     ┆ 11  ┆ 4           ┆ 8           ┆ 5           │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ "banana" ┆ "audi"   ┆ "fruits"     ┆ 11  ┆ 2           ┆ 8           ┆ 2           │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ "banana" ┆ "beetle" ┆ "fruits"     ┆ 11  ┆ 4           ┆ 8           ┆ 1           │
└──────────┴──────────┴──────────────┴─────┴─────────────┴─────────────┴─────────────┘
```

```js
> df["cars"] // or df.getColumn("cars")
shape: (5,)
Series: 'cars' [str]
[
        "beetle"
        "beetle"
        "beetle"
        "audi"
        "beetle"
]
```

## Node setup

Install the latest polars version with:

```sh
$ yarn add nodejs-polars # yarn
$ npm i -s nodejs-polars # npm
$ bun i -D nodejs-polars # Bun
```

Releases happen quite often (weekly / every few days) at the moment, so updating polars regularly to get the latest bugfixes / features might not be a bad idea.

### Minimum Requirements
- Node version `>=18`
- Rust version `>=1.86` - *Only needed for development*


## Deno

In Deno modules you can import polars straight from `npm`:

```typescript
import pl from "npm:nodejs-polars";
```

With Deno 1.37, you can use the `display` function to display a `DataFrame` in the notebook:

```typescript
import pl from "npm:nodejs-polars";
import { display } from "https://deno.land/x/display@v1.1.2/mod.ts";

let response = await fetch(
  "https://cdn.jsdelivr.net/npm/world-atlas@1/world/110m.tsv",
);
let data = await response.text();
let df = pl.readCSV(data, { sep: "\t" });
await display(df)
```

With Deno 1.38, you only have to make the dataframe be the last expression in the cell:

```typescript
import pl from "npm:nodejs-polars";
let response = await fetch(
  "https://cdn.jsdelivr.net/npm/world-atlas@1/world/110m.tsv",
);
let data = await response.text();
let df = pl.readCSV(data, { sep: "\t" });
df
```

<img width="510" alt="image" src="https://github.com/pola-rs/nodejs-polars/assets/836375/90cf7bf4-7478-4919-b297-f8eb6a16196f">


___

## Documentation

Want to know about all the features Polars supports? Read the [docs](https://docs.pola.rs)!

#### Python

- Installation guide: `$ pip3 install polars`
- [Python documentation](https://pola-rs.github.io/polars/py-polars/html/reference/index.html)
- [User guide](https://docs.pola.rs)

#### Rust

- [Rust documentation](https://docs.rs/polars/latest/polars/)

#### Node

  * Installation guide: `$ yarn add nodejs-polars`
  * [Node documentation](https://pola-rs.github.io/nodejs-polars/)

## Contribution

Want to contribute? Read our [contribution guideline](https://github.com/pola-rs/polars/blob/master/CONTRIBUTING.md).

## \[Node\]: compile polars from source

If you want a bleeding edge release or maximal performance you should compile **polars** from source.

1. Install the latest [Rust compiler](https://www.rust-lang.org/tools/install)
2. Run `npm|yarn install`
3. Choose any of:
   - Fastest binary, very long compile times:
     ```bash
     $ cd nodejs-polars && yarn build && yarn build:ts # this will generate a /bin directory with the compiles TS code, as well as the rust binary
     ```
   - Debugging, fastest compile times but slow & large binary:
     ```bash
     $ cd nodejs-polars && yarn build:debug && yarn build:ts # this will generate a /bin directory with the compiles TS code, as well as the rust binary
     ```

## Webpack configuration
To use `nodejs-polars` with [Webpack](https://webpack.js.org) please use [node-loader](https://github.com/webpack-contrib/node-loader) and `webpack.config.js`

## Sponsors

[<img src="https://www.jetbrains.com/company/brand/img/jetbrains_logo.png" height="50" alt="JetBrains logo" />](https://www.jetbrains.com)
