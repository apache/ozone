/*
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

/// <reference types="vitest" />
/// <reference types="vite/client" />

import { defineConfig, splitVendorChunkPlugin } from 'vite';
import { resolve } from 'path';
import react from '@vitejs/plugin-react-swc';

function pathResolve(dir: string) {
  return resolve(__dirname, '.', dir)
}

// https://vitejs.dev/config/
export default defineConfig({
  // This "base" path determines the import locations for other files. By default it is '/' which causes all files to be
  // fetched from a root URL. However in case of proxy we need to get this value relative to a base, hence it is set to empty string
  base: "",
  plugins: [
    react({
      devTarget: "es2015" //SWC by default bypasses the build target, set dev target explicitly
    }),
    splitVendorChunkPlugin()
  ],
  build: {
    target: "es2015",
    outDir: 'build',
    rollupOptions: {
      output: {
        chunkFileNames: 'static/js/[name]-[hash].js',
        entryFileNames: 'static/js/[name]-[hash].js',
        assetFileNames: (assetInfo) => {
          const extName = assetInfo.name!.split(".")[1];
          if (/css/.test(extName)){
            return `static/css/[name]-[hash].${extName}`
          }
          else {
            return `static/media/[name]-[hash].${extName}`
          }
        }
      }
    }
  },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:9888"
      }
    }
  },
  resolve: {
    alias: {
      "@": pathResolve('src'),
      "@tests": pathResolve('src/__tests__')
    }
  },
  css: {
    preprocessorOptions: {
      less: {
        javascriptEnabled: true,
        math: "always",
        relativeUrls: true,
        modifyVars: {
          '@primary-color': '#1DA57A'
        }
      }
    }
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: 'src/__tests__/vitest.setup.ts',
    include: ["src/__tests__/**/*.test.tsx"],
    reporters: ['verbose']
  }
});
