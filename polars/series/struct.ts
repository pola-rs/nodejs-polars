import { _DataFrame, DataFrame } from "../dataframe";
import pli from "../internals/polars_internal";
import { _Expr } from "../lazy/expr";
import type { Series } from ".";

/**
 * Struct Functions for Series
 */
export interface SeriesStructFunctions {
  /**
   * List of fields of the struct
   * @return Array of field names
   */
  fields: string[];
  /**
   * Convert series of struct into dataframe
   * @return A new dataframe
   */
  toFrame(): DataFrame;
  /**
   * Access a field by name
   * @param name - name of the field
   * @return A new series containing the values of struct's field
   */
  field(name: string): Series;
  /**
   * Rename the fields of a struct
   * @param names - new names of the fields
   * @return A new series containing the struct with new field names
   */
  renameFields(names: string[]): Series;
  /**
   * Access a field by index (zero based index)
   * @param index - index of the field (starts at 0)
   * @return A new series containing the values of struct's field index
   */
  nth(index: number): Series;
}

export const SeriesStructFunctions = (_s: any): SeriesStructFunctions => {
  return {
    get fields() {
      return _s.structFields();
    },
    toFrame() {
      return _DataFrame(_s.structToFrame());
    },
    field(name) {
      return DataFrame({})
        .select(_Expr(pli.lit(_s).structFieldByName(name)))
        .toSeries();
    },
    renameFields(names) {
      return DataFrame({})
        .select(_Expr(pli.lit(_s).structRenameFields(names)))
        .toSeries();
    },
    nth(index) {
      return DataFrame({})
        .select(_Expr(pli.lit(_s).structFieldByIndex(index)))
        .toSeries();
    },
  };
};
