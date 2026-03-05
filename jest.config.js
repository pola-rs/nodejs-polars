import { pathsToModuleNameMapper } from "ts-jest";
import ts from "typescript";
const { config } = ts.readConfigFile("./tsconfig.json", ts.sys.readFile);

export default {
  preset: "ts-jest",
  testEnvironment: "node",
  clearMocks: true,
  collectCoverage: true,
  moduleDirectories: ["node_modules", "./polars"],
  moduleFileExtensions: ["js", "ts"],
  setupFilesAfterEnv : ["<rootDir>/__tests__/setup.ts"],
  moduleNameMapper: pathsToModuleNameMapper(config.compilerOptions.paths, { prefix: "<rootDir>/polars" }),
  testPathIgnorePatterns: ["<rootDir>/__tests__/setup.ts"],
  transform: {
    '^.+\\.{ts|tsx}?$': ['ts-jest', {
      tsConfig: 'tsconfig.json',
    }],
  },
};
