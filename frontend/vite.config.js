import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  server: {
    host: '0.0.0.0',
    port: 3000,
    proxy: {
      '/api/server1': {
        target: 'http://file-server-1:8081',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/server1/, '/api')
      },
      '/api/server2': {
        target: 'http://file-server-2:8082',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/server2/, '/api')
      },
      '/api/server3': {
        target: 'http://file-server-3:8083',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api\/server3/, '/api')
      }
    }
  }
})
