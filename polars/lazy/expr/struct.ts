import { selectionToExprList } from "../../utils";
import { _Expr, type Expr } from "../expr";

/**
 * Struct functions for Lazy datatframes
 */
export interface ExprStruct {
  /**
   * Access a field by name
   * @param name - name of the field
   * @example
   * pl.DataFrame({
         objs: [
           { a: 1, b: 2.0, c: "abc" },
           { a: 10, b: 20.0, c: "def" },
         ],
       }).select(
          col("objs").struct.field("b"),
          col("objs").struct.field("c").as("last")
        );
    >>shape: (2, 2)
    ┌──────┬──────┐
    │ b    ┆ last │
    │ ---  ┆ ---  │
    │ f64  ┆ str  │
    ╞══════╪══════╡
    │ 2.0  ┆ abc  │
    │ 20.0 ┆ def  │
    └──────┴──────┘
   */
  field(name: string): Expr;
  /**
   * Access a field by index (zero based index)
   * @param index - index of the field (starts at 0)
   * 
   * @example
   * pl.DataFrame({
         objs: [
           { a: 1, b: 2.0, c: "abc" },
           { a: 10, b: 20.0, c: "def" },
         ],
       }).select(
          col("objs").struct.nth(1),
          col("objs").struct.nth(2).as("last"),
        );
    >>shape: (2, 2)
    ┌──────┬──────┐
    │ b    ┆ last │
    │ ---  ┆ ---  │
    │ f64  ┆ str  │
    ╞══════╪══════╡
    │ 2.0  ┆ abc  │
    │ 20.0 ┆ def  │
    └──────┴──────┘
   */
  nth(index: number): Expr;
  /**
   * Rename the fields of a struct
   * @param names - new names of the fields
   * 
   * @example
   * pl.DataFrame({
         objs: [
           { a: 1, b: 2.0, c: "abc" },
           { a: 10, b: 20.0, c: "def" },
         ]}).select(col("objs").struct.renameFields(["a", "b", "c"]));
    >>shape: (2, 1)
    ┌───────────────────┐
    │ objs              │
    │ ---               │
    │ struct[3]         │
    ╞═══════════════════╡
    │ {1.0,2.0,"abc"}   │
    │ {10.0,20.0,"def"} │
    └───────────────────┘
   */
  renameFields(names: string[]): Expr;
  /**
   * Add/replace fields in a struct
   * @param fields - array of expressions for new fields
   * 
   * @example
   * pl.DataFrame({
         objs: [
           { a: 1, b: 2.0, c: "abc" },
           { a: 10, b: 20.0, c: "def" },
         ],
         more: ["text1", "text2"],
         final: [100, null],
       }).select(
             col("objs").struct.withFields([
               pl.lit(null).alias("d"),
               pl.lit("text").alias("e"),
             ]),
             col("objs")
               .struct.withFields([col("more"), col("final")])
               .alias("new"),
           );
    shape: (2, 2)
    ┌───────────────────────────────┬────────────────────────────────┐
    │ objs                          ┆ new                            │
    │ ---                           ┆ ---                            │
    │ struct[5]                     ┆ struct[5]                      │
    ╞═══════════════════════════════╪════════════════════════════════╡
    │ {1.0,2.0,"abc",null,"text"}   ┆ {1.0,2.0,"abc","text1",100.0}  │
    │ {10.0,20.0,"def",null,"text"} ┆ {10.0,20.0,"def","text2",null} │
    └───────────────────────────────┴────────────────────────────────┘
   */
  withFields(fields: Expr[]): Expr;
  /**
   * Convert this struct to a string column with json values.
   * 
   * @example
   * pl.DataFrame( {"a": [{"a": [1, 2], "b": [45]}, {"a": [9, 1, 3], "b": null}]}
                 ).withColumn(pl.col("a").struct.jsonEncode().alias("encoded"))
    shape: (2, 2)
    ┌──────────────────┬────────────────────────┐
    │ a                ┆ encoded                │
    │ ---              ┆ ---                    │
    │ struct[2]        ┆ str                    │
    ╞══════════════════╪════════════════════════╡
    │ {[1, 2],[45]}    ┆ {"a":[1,2],"b":[45]}   │
    │ {[9, 1, 3],null} ┆ {"a":[9,1,3],"b":null} │
    └──────────────────┴────────────────────────┘
   */
  jsonEncode(): Expr;
}

export const ExprStructFunctions = (_expr: any): ExprStruct => {
  return {
    field(name) {
      return _Expr(_expr.structFieldByName(name));
    },
    nth(index) {
      return _Expr(_expr.structFieldByIndex(index));
    },
    renameFields(names) {
      return _Expr(_expr.structRenameFields(names));
    },
    withFields(fields: Expr[]) {
      fields = selectionToExprList(fields, false);
      return _Expr(_expr.structWithFields(fields));
    },
    jsonEncode() {
      return _Expr(_expr.structJsonEncode());
    },
  };
};
