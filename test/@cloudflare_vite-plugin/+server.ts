import 'telefunc/async_hooks'

import vike from 'vike/fetch'
import { Telefunc } from 'telefunc/cloudflare'

const tf = new Telefunc({ scale: 5 })

// Cloudflare requires Durable Object classes to be named exports of the worker entry.
// `wrangler.jsonc`'s `main: "vike:server-entry"` re-exports everything from this file,
// so these reach Cloudflare's binding resolver intact.
const TelefuncDurableObjectBase = tf.TelefuncDurableObject
export class TelefuncDurableObject extends TelefuncDurableObjectBase {
  async telefuncRoomAsyncContextProbe(): Promise<string> {
    try {
      await (
        this as unknown as {
          telefuncRoomDeliver(request: {
            roomId: string
            inc: string
            laneKey: string
            subscriberDoId: string
            leaseId: string
            generationToken: string
            frame: ArrayBuffer
            seq: number
            timestamp: number
          }): Promise<void>
        }
      ).telefuncRoomDeliver({
        roomId: 'recipe-probe',
        inc: 'recipe-probe',
        laneKey: 'recipe-probe',
        subscriberDoId: 'recipe-probe',
        leaseId: 'recipe-probe',
        generationToken: 'recipe-probe',
        frame: new ArrayBuffer(0),
        seq: 0,
        timestamp: 0,
      })
      return 'unexpected delivery success'
    } catch (error) {
      return error instanceof Error ? error.message : String(error)
    }
  }
}
export const TelefuncRoomDurableObject = tf.TelefuncRoomDurableObject
export { TodoListDurableObject } from './database/todoItems'

// vike's docs example uses `export default vike` directly — meaning when vike is the
// worker entry's default, Cloudflare invokes vike.fetch with `(request, env, ctx)`.
// Type-wise vike/fetch is generic-typed for universal-middleware, but at runtime it
// IS a Cloudflare handler. Cast for the wrapper call.
const vikeAsCloudflareHandler = vike as unknown as ExportedHandler<Env>

export default {
  async fetch(request, env, ctx): Promise<Response> {
    if (new URL(request.url).pathname === '/__telefunc-room-async-context-probe') {
      const id = env.TelefuncDurableObject.idFromName('telefunc-room-recipe-probe')
      const session = env.TelefuncDurableObject.get(id) as unknown as {
        telefuncRoomAsyncContextProbe(): Promise<string>
      }
      const result = await session.telefuncRoomAsyncContextProbe()
      return result.includes('delivery addressed the wrong session shard')
        ? new Response(null, { status: 204 })
        : new Response(result, { status: 500 })
    }
    const resp = await tf.serve({ request, env, ctx })
    return resp ?? vikeAsCloudflareHandler.fetch!(request, env, ctx)
  },
} satisfies ExportedHandler<Env>
