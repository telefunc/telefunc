import 'telefunc'
import type { User } from './auth/getUser'

declare module 'telefunc' {
  namespace Telefunc {
    interface Context {
      user: null | User
    }
  }
}
