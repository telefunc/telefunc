import telefunc from 'telefunc/vite'

export default {
  plugins: [telefunc()],
  builder: {}, // `vite build` also builds the server, which `vite preview` runs
  environments: { ssr: { build: { outDir: 'dist/server', rollupOptions: { input: 'Lobby.telefunc.ts' } } } },
}
