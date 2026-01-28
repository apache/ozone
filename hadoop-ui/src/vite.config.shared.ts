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