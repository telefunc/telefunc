export { handleAll as default }
export { TodoListDurableObject } from '../database/todoItems'

import { telefunc } from 'telefunc/cloudflare'
import { handleSsr } from './ssr'

const tf = telefunc({ shards: 5 })
export const TelefuncDurableObject = tf.TelefuncDurableObject

const handleAll = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url)

    const resp = await tf.serve({ request, env, ctx })
    if (resp) return resp

    return await handleSsr(url)
  },
} satisfies ExportedHandler<Env>
