import tailwindcss from '@tailwindcss/vite'
import react from '@vitejs/plugin-react'
import telefunc from 'telefunc/vite'
import vike from 'vike/plugin'
import type { UserConfig } from 'vite'

export default {
  plugins: [react(), vike(), telefunc(), tailwindcss()],
  // Pre-bundle the heavy client deps at dev-server start. PixiJS is only imported by the match
  // view, so without this Vite discovers it on first entry into a match and triggers a full page
  // reload ("optimized dependencies changed") — which would drop players out of a live match.
  optimizeDeps: {
    include: ['pixi.js', 'zustand', 'react', 'react-dom', 'react-dom/client'],
  },
  build: {
    // The PixiJS/WebGL renderer makes the match bundle exceed Vite's 500 kB advisory; a game
    // client is expected to be large, so lift the warning rather than over-split a hot path.
    chunkSizeWarningLimit: 2000,
  },
} satisfies UserConfig
