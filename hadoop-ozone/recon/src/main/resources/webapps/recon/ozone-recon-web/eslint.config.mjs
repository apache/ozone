/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';
import reactPlugin from 'eslint-plugin-react';
import importPlugin from 'eslint-plugin-import';
import pluginPromise from 'eslint-plugin-promise';
import stylistic from '@stylistic/eslint-plugin';
import eslintConfigPrettier from 'eslint-config-prettier/flat';
import globals from 'globals';

export default tseslint.config(
  {
    ignores: [
      '**/node_modules/**',
      'build/**',
      'dist/**',
      'api/**',
      'vite.config.ts',
      'vite-env.d.ts',
    ],
  },
  eslint.configs.recommended,
  ...tseslint.configs.recommended,
  reactPlugin.configs.flat.recommended,
  reactPlugin.configs.flat['jsx-runtime'],
  importPlugin.flatConfigs.recommended,
  importPlugin.flatConfigs.typescript,
  eslintConfigPrettier,
  {
    files: ['src/**/*.{ts,tsx}'],
    languageOptions: {
      ecmaVersion: 2020,
      sourceType: 'module',
      globals: {
        ...globals.browser,
        ...globals.node,
      },
      parserOptions: {
        ecmaFeatures: { jsx: true },
      },
    },
    plugins: {
      '@stylistic': stylistic,
      promise: pluginPromise,
    },
    settings: {
      react: { version: 'detect' },
      'import/parsers': {
        '@typescript-eslint/parser': ['.ts', '.tsx'],
      },
      'import/resolver': {
        typescript: { alwaysTryTypes: true },
        node: {
          extensions: ['.js', '.jsx', '.ts', '.tsx'],
        },
      },
    },
    rules: {
      'promise/prefer-await-to-then': 'warn',
      'camelcase': 'off',
      '@stylistic/space-infix-ops': 'warn',
      '@stylistic/quotes': ['warn', 'single', { avoidEscape: true, allowTemplateLiterals: true }],
      'no-unused-vars': 'off',
      '@stylistic/object-curly-spacing': ['warn', 'always'],
      '@stylistic/object-property-newline': 'warn',
      'no-return-assign': 'off',
      '@stylistic/indent': ['warn', 2, { SwitchCase: 1 }],
      'constructor-super': 'warn',
      'import/no-unassigned-import': 'off',
      'import/no-unused-modules': ['warn', { unusedExports: true }],
      'import/no-extraneous-dependencies': ['error', {
        devDependencies: true,
        optionalDependencies: true,
        peerDependencies: true,
      }],
      'react/state-in-constructor': 'off',
      'react/require-default-props': 'off',
      'react/default-props-match-prop-types': 'off',
      'react/no-array-index-key': 'off',
      '@typescript-eslint/explicit-function-return-type': 'off',
      '@typescript-eslint/prefer-readonly-parameter-types': 'off',
      '@typescript-eslint/no-unused-vars': ['warn', {
        argsIgnorePattern: '^_\\w*',
        varsIgnorePattern: '^_\\w*',
      }],
      '@typescript-eslint/no-non-null-assertion': 'off',
      '@typescript-eslint/no-explicit-any': 'warn',
      '@typescript-eslint/no-unused-expressions': 'off',
      '@typescript-eslint/no-empty-function': 'error',
      '@typescript-eslint/no-inferrable-types': 'error',
    },
  },
);
