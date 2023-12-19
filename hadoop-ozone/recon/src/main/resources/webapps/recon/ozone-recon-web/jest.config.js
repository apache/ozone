// For a detailed explanation regarding each configuration property, visit:
// https://jestjs.io/docs/en/configuration.html

module.exports = {
  preset: "ts-jest",
  globals: {
    "ts-jest": {
      packageJson: "package.json"
    }
  },
  transformIgnorePatterns: ['<rootDir>/node_modules/'],
  // The root of your source code, typically /src
  // `<rootDir>` is a token Jest substitutes
  roots: ["<rootDir>/src"],

  // Jest transformations -- this adds support for TypeScript
  // using ts-jest
  transform: {
    "^.+\\.tsx?$": "ts-jest"
  },

  // Runs special logic, such as cleaning up components
  // when using React Testing Library and adds special
  // extended assertions to Jest
  setupFilesAfterEnv: [
    // "<rootDir>/src/tests/setupTests.js",
    "@testing-library/jest-dom/extend-expect"
  ],

  // Test spec file resolution pattern
  // Matches parent folder `__tests__` and filename
  // should contain `test` or `spec`.
  testRegex: "(/tests/.*|(\\.|/)(test|spec))\\.tsx?$",
  modulePathIgnorePatterns: ["__mocks__"],

  // Module file extensions for importing
  moduleFileExtensions: ["ts", "tsx", "js", "jsx", "json", "node"],

  // Automatically clear mock calls and instances between every test
  clearMocks: true
};
