import { selectionToExprList } from "../../utils";
import { type Expr, _Expr } from "../expr";

/**
 * Struct functions for Lazy datatframes
 */
export interface ExprStruct {
  /**
   * Access a field by name
   * @param name - name of the field
   */
  field(name: string): Expr;
  /**
   * Access a field by index (zero based index)
   * @param index - index of the field (starts at 0)
   */
  nth(index: number): Expr;
  /**
   * Rename the fields of a struct
   * @param names - new names of the fields
   */
  renameFields(names: string[]): Expr;
  /**
   * Add/replace fields in a struct
   * @param fields - array of expressions for new fields
   */
  withFields(fields: Expr[]): Expr;
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
  };
};
