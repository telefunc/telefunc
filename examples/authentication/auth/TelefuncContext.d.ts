import type { User } from '#app/db'

declare global {
  namespace Telefunc {
    interface Context {
      user: null | User
    }
  }
}
