export { handleAll as default }
export { TodoListDurableObject } from '../database/todoItems'
export { $TelefuncDurableObject } from './telefunc'

import { handleSsr } from './ssr'
import { handleTelefunc } from './telefunc'

const handleAll = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)

    const resp = await handleTelefunc(request, env, ctx)
    if (resp) return resp

    return await handleSsr(url)
  },
} satisfies ExportedHandler<Env>
