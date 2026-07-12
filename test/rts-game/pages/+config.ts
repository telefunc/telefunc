import vikeReact from 'vike-react/config'
import type { Config } from 'vike/types'

export default {
  extends: [vikeReact],
  title: 'Telefront — 10v10 RTS',
  // The match is a PixiJS/WebGL client app driven entirely by live Room data; there is nothing to
  // server-render, and PixiJS is browser-only. Render client-side to keep the engine off the server.
  ssr: false,
} satisfies Config
