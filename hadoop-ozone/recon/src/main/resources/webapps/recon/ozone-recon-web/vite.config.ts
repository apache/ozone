import { defineConfig, splitVendorChunkPlugin } from 'vite';
import { resolve } from 'path';
import react from '@vitejs/plugin-react';

function pathResolve(dir: string) {
  return resolve(__dirname, '.', dir)
}

// https://vitejs.dev/config/
export default defineConfig({
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
  plugins: [react(), splitVendorChunkPlugin()],
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:9888"
      }
    }
  },
  resolve: {
    alias: {
      "@": pathResolve('./src')
    }
  },
  css: {
    preprocessorOptions: {
      less: {
        javascriptEnabled: true,
        math: "always",
        relativeUrls: true
      }
    }
  },
  test: {
    globals: true,
    setupFiles: './src/setupTests.ts',
    css: true,
    reporters: ['verbose'],
    coverage: {
      reporter: ['text', 'json', 'html'],
      include: ['src/**/*'],
      exclude: []
    }
  }
})