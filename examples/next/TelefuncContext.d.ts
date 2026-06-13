import type { User } from './auth/getUser'

declare global {
  namespace Telefunc {
    interface Context {
      user: null | User
    }
  }
}
