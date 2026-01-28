/// <reference types="vitest" />
/// <reference types="vite/client" />

import { defineConfig } from 'vite';
import { resolve } from 'path';
import react from '@vitejs/plugin-react-swc';

function pathResolve(dir: string) {
  return resolve(__dirname, '.', dir)
}

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    react({
      devTarget: "es2015" //SWC by default bypasses the build target, set dev target explicitly
    }),
  ],
  build: {
    target: "es2015",
    outDir: '../../../build/om',
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
        },
        // Manual chunking for vendor libraries
        manualChunks: {
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
        }
      }
    }
  },
  // Optimize dependencies to prevent outdated cache issues
  optimizeDeps: {
    include: [
      'react',
      'react-dom',
      'antd',
      '@ant-design/icons',
      'react-router-dom',
      'axios'
    ],
    force: false // Set to true temporarily if you need to force re-optimization
  },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:9862"
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