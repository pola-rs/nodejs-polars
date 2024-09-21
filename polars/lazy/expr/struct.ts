import { type Expr, _Expr } from "../expr";

/**
 * Struct functions
 */
export interface ExprStruct {
  /**
   * Access a field by name
   * @param name - name of the field
   */
  field(name: string): Expr;
  /**
   * Rename the fields of a struct
   * @param names - new names of the fields
   */
  renameFields(names: string[]): Expr;
}

export const ExprStructFunctions = (_expr: any): ExprStruct => {
  return {
    field(name) {
      return _Expr(_expr.structFieldByName(name));
    },
    renameFields(names) {
      return _Expr(_expr.structRenameFields(names));
    },
  };
};
