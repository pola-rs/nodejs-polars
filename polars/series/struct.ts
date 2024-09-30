import { type Series, _Series } from ".";
import { DataFrame, _DataFrame } from "../dataframe";
import pli from "../internals/polars_internal";
import { _Expr } from "../lazy/expr";

export interface SeriesStructFunctions {
  fields: string[];
  toFrame(): DataFrame;
  field(name: string): Series;
  renameFields(names: string[]): Series;
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
  };
};
