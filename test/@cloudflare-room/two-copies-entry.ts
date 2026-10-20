/// <reference types="@cloudflare/workers-types" />
import { Telefunc as FirstTelefunc } from '../../packages/telefunc/serve/cloudflare.js'
import { getChannelMux as firstChannelMux } from '../../packages/telefunc/wire-protocol/server/mux.js'
import { stringify } from '@brillout/json-serializer/stringify'
import { Broadcast, Telefunc, decorateTelefunction, getChannelMux } from './second-copy.js'

new FirstTelefunc({ bindingName: 'TELEFUNC' })
// The app's Durable Object comes from the second copy, which reuses the backend the first copy installed.
const { TelefuncDurableObject } = new Telefunc({ bindingName: 'TELEFUNC' })
const PROBE_FILE = '/probe.telefunc.ts'
let outcome: { subscribed: string; sameMux: boolean } | undefined

decorateTelefunction(
  async () => {
    let subscribed = 'subscribed'
    try {
      Broadcast.subscribe('two-copies', () => {})()
    } catch (error) {
      subscribed = String(error)
    }
    outcome = { subscribed, sameMux: firstChannelMux() === getChannelMux() }
  },
  'onProbe',
  PROBE_FILE,
  '/',
)

export class TwoCopiesDurableObject extends TelefuncDurableObject {
  async probe(): Promise<unknown> {
    const response = await this.fetch!(
      new Request('https://room.test/_telefunc', {
        method: 'POST',
        headers: { 'x-telefunc-broadcast-bucket': 'weur' },
        body: stringify({ file: PROBE_FILE, name: 'onProbe', args: [] }),
      }),
    )
    return { status: response.status, ...outcome }
  }
}

export default {
  async fetch(_request: Request, env: Cloudflare.Env): Promise<Response> {
    const session = env.TELEFUNC.get(env.TELEFUNC.idFromName('two-copies')) as unknown as TwoCopiesDurableObject
    return Response.json(await session.probe())
  },
}
