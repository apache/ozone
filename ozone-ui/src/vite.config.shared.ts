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
import { UserConfig } from 'vite';

/**
 * Common vendor chunking configuration for all packages
 * This replaces the deprecated splitVendorChunk() function from Vite 4
 */
export const getVendorChunks = () => ({
  // React ecosystem
  'react-vendor': ['react', 'react-dom'],
  // Ant Design ecosystem  
  'antd-vendor': ['antd', '@ant-design/icons'],
  // Router
  'router-vendor': ['react-router-dom'],
  // HTTP client
  'axios-vendor': ['axios'],
  // Other utilities
  'utils-vendor': ['@fontsource/roboto', 'less']
});

/**
 * Common optimizeDeps configuration
 * This helps prevent "outdated optimize deps" issues
 */
export const getOptimizeDepsConfig = () => ({
  include: [
    'react',
    'react-dom',
    'antd',
    '@ant-design/icons',
    'react-router-dom',
    'axios'
  ],
  force: false // Set to true temporarily if you need to force re-optimization
});

/**
 * Common build configuration
 */
export const getBuildConfig = (outDir: string) => ({
  target: "es2015",
  outDir,
  rollupOptions: {
    output: {
      chunkFileNames: 'static/js/[name]-[hash].js',
      entryFileNames: 'static/js/[name]-[hash].js',
      assetFileNames: (assetInfo: any) => {
        const extName = assetInfo.name!.split(".")[1];
        if (/css/.test(extName)) {
          return `static/css/[name]-[hash].${extName}`;
        } else {
          return `static/media/[name]-[hash].${extName}`;
        }
      },
      manualChunks: getVendorChunks()
    }
  }
}); 