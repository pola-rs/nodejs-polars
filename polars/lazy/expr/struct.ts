import type { DataType } from "../../datatypes";
import { selectionToExprList } from "../../utils";
import { _Expr, type Expr } from "../expr";

/**
 * Struct functions for Lazy datatframes
 */
export interface ExprStruct<Name extends string | undefined = undefined> {
  /**
   * Access a field by name
   * @param name - name of the field
   */
  field(name: string): Expr<DataType, Name>;
  /**
   * Access a field by index (zero based index)
   * @param index - index of the field (starts at 0)
   */
  nth(index: number): Expr<DataType, Name>;
  /**
   * Rename the fields of a struct
   * @param names - new names of the fields
   */
  renameFields(names: string[]): Expr<DataType, Name>;
  /**
   * Add/replace fields in a struct
   * @param fields - array of expressions for new fields
   */
  withFields(fields: Expr[]): Expr<DataType, Name>;
}

export const ExprStructFunctions = (_expr: any): ExprStruct => {
  return {
    field(name) {
      return _Expr(_expr.structFieldByName(name)) as any;
    },
    nth(index) {
      return _Expr(_expr.structFieldByIndex(index)) as any;
    },
    renameFields(names) {
      return _Expr(_expr.structRenameFields(names)) as any;
    },
    withFields(fields: Expr[]) {
      fields = selectionToExprList(fields, false);
      return _Expr(_expr.structWithFields(fields)) as any;
    },
  };
};
