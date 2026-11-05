// Global typing for `getContext()` in *.telefunc.ts files (see /getContext docs).

import type { SessionUser } from './identity'

declare global {
  namespace Telefunc {
    interface Context {
      user: SessionUser | null
    }
  }
}

export {}
