import 'telefunc'
import type { User } from '#app/db'

declare module 'telefunc' {
  namespace Telefunc {
    interface Context {
      user: null | User
    }
  }
}
