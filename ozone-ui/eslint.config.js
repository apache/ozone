/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/* eslint-disable @typescript-eslint/no-require-imports */
const { FlatCompat } = require('@eslint/eslintrc');

const compat = new FlatCompat({
  baseDirectory: __dirname,
});

module.exports = [
  {
    ignores: ['dist', 'build', 'node_modules'],
  },
  ...compat.config({
    env: {
      browser: true,
      node: true,
      es2020: true,
    },
    extends: [
      'plugin:react/recommended',
      'plugin:jsx-a11y/recommended',
      'plugin:@typescript-eslint/recommended',
      'plugin:prettier/recommended',
    ],
    parser: '@typescript-eslint/parser',
    parserOptions: {
      ecmaVersion: 2020,
      sourceType: 'module',
      ecmaFeatures: {
        jsx: true,
      },
    },
    plugins: ['jsx-a11y', 'react-hooks'],
    settings: {
      react: {
        pragma: 'React',
        version: '18.3.1',
      },
    },
    rules: {
      'no-duplicate-imports': 'warn',
      'new-cap': 0,
      'no-console': 0,
      'no-extra-boolean-cast': 0,
      'no-invalid-this': 0,
      'no-lonely-if': 2,
      'no-throw-literal': 0,
      'no-unused-vars': 0,
      '@typescript-eslint/no-unused-vars': [
        'error',
        {
          vars: 'all',
          args: 'after-used',
          varsIgnorePattern: '^_',
          argsIgnorePattern: '^_',
          ignoreRestSiblings: true,
        },
      ],
      'no-useless-constructor': 2,
      'no-var': 1,
      'no-undef': 2,
      'one-var': 0,
      'prefer-arrow-callback': 2,
      'prefer-const': ['warn', { destructuring: 'all' }],
      'require-jsdoc': 0,
      strict: 0,
      'valid-jsdoc': 0,
      'eol-last': ['error', 'always'],
      'react/display-name': 0,
      curly: [2, 'all'],
      'jsx-a11y/no-autofocus': 0,
      'jsx-a11y/label-has-associated-control': [
        1,
        {
          labelComponents: ['label'],
          labelAttributes: ['htmlFor'],
          controlComponents: ['input'],
          assert: 'both',
        },
      ],
      'jsx-a11y/anchor-is-valid': 1,
      'jsx-a11y/anchor-has-content': 1,
      'jsx-a11y/click-events-have-key-events': 1,
      'jsx-a11y/no-noninteractive-element-interactions': 1,
      'jsx-a11y/interactive-supports-focus': 1,
      'jsx-a11y/no-static-element-interactions': 1,
      'jsx-a11y/media-has-caption': 1,
      'react-hooks/rules-of-hooks': 'error',
      'react-hooks/exhaustive-deps': 'error',
      'react/jsx-no-useless-fragment': [
        'warn',
        {
          // TS complains if we return a string from a component unless we wrap it
          // in a fragment, so we should allow that case
          allowExpressions: true,
        },
      ],
      'react/react-in-jsx-scope': 'off',
      '@typescript-eslint/indent': 0,
      '@typescript-eslint/explicit-function-return-type': 0,
      '@typescript-eslint/no-use-before-define': 0,
      '@typescript-eslint/no-explicit-any': 0,
      '@typescript-eslint/explicit-member-accessibility': 0,
      '@typescript-eslint/no-non-null-assertion': 0,
      '@typescript-eslint/no-empty-interface': 0,
      '@typescript-eslint/camelcase': 0,
      '@typescript-eslint/no-var-requires': 0,
      '@typescript-eslint/no-empty-function': 0,
      '@typescript-eslint/ban-ts-ignore': 0,
      '@typescript-eslint/no-this-alias': 0,
      'prettier/prettier': [
        'error',
        {
          printWidth: 100,
          singleQuote: true,
          trailingComma: 'es5',
          semi: true,
          tabWidth: 2,
          endOfLine: 'lf',
          arrowParens: 'always',
          bracketSpacing: true,
          htmlWhitespaceSensitivity: 'css',
          quoteProps: 'as-needed',
          bracketSameLine: false,
          insertPragma: false,
          requirePragma: false,
        },
      ],
    },
    overrides: [
      {
        files: ['*.ts', '*.tsx', '*.cts', '*.mts'],
        rules: {
          // this is handled by TS instead, otherwise we'll get a lot of false
          // positives.
          // See: https://github.com/typescript-eslint/typescript-eslint/blob/master/docs/getting-started/linting/FAQ.md no-undef section
          'no-undef': 'off',
          // prop types only really useful in non TS code
          'react/prop-types': 'off',
        },
      },
    ],
  }),
];
