import type { Config } from 'vike/types'

// Client-only: the live() client owns a browser-side subscription, and SSR would run the live queries
// once on the server for a render the browser then discards — extra live handles that muddy the fetch
// counts this test asserts on.
export default {
  ssr: false,
} satisfies Config
