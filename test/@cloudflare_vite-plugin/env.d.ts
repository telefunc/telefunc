// The app's bindings, as its code reaches them: Wrangler's generated Env (`worker-configuration.d.ts`) types a Durable
// Object namespace without its class where `main` is a virtual module (`vike:server`), so the app names it here.
import type { TodoListDurableObject } from './database/todoItems'

declare global {
  interface Env {
    TO_DO_LIST_DURABLE_OBJECTS: DurableObjectNamespace<TodoListDurableObject>
  }
  namespace Cloudflare {
    interface Env {
      TO_DO_LIST_DURABLE_OBJECTS: DurableObjectNamespace<TodoListDurableObject>
    }
  }
}
