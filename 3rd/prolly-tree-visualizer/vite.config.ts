import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const repositoryRoot = decodeURIComponent(new URL('../..', import.meta.url).pathname).replace(/\/$/, '');

export default defineConfig({
  plugins: [react()],
  server: {
    fs: {
      allow: [repositoryRoot],
    },
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
  preview: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
});
