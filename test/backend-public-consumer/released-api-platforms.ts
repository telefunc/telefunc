// Runtime-specific entry points from the released API baseline. The consumer supplies
// the same host ambient types an application on each runtime supplies.
import { Telefunc as CloudflareTelefunc, type CloudflareOptions } from 'telefunc/cloudflare'
import { Telefunc as NodeTelefunc } from 'telefunc/node'
import { Telefunc as BunTelefunc } from 'telefunc/bun'
import { Telefunc as DenoTelefunc } from 'telefunc/deno'

type Assert<T extends true> = T
type Extends<Actual, Baseline> = [Actual] extends [Baseline] ? true : false
type HasKeys<Actual, Keys extends PropertyKey> = Exclude<Keys, keyof Actual> extends never ? true : false
type _cloudflareOptionKeys = Assert<
  HasKeys<
    CloudflareOptions,
    'bindingName' | 'kvBindingName' | 'instanceName' | 'context' | 'scale' | 'locationFallback' | 'jurisdiction'
  >
>
type ReleasedCloudflareOptions = {
  bindingName?: string
  kvBindingName?: string
  instanceName?: string
  context?: (request: Request, env: Cloudflare.Env) => unknown | Promise<unknown>
  scale?: number | Partial<Record<DurableObjectLocationHint, number>>
  locationFallback?: DurableObjectLocationHint
  jurisdiction?: DurableObjectJurisdiction
}
type _cloudflareOptionShape = Assert<Extends<CloudflareOptions, ReleasedCloudflareOptions>>

declare const context: NonNullable<CloudflareOptions['context']>
const _cloudflareOptions: CloudflareOptions = {
  bindingName: 'TelefuncDurableObject',
  kvBindingName: 'TelefuncKV',
  instanceName: 'telefunc',
  context,
  scale: 1,
  locationFallback: 'weur',
  jurisdiction: 'eu',
}

const cloudflare = new CloudflareTelefunc(_cloudflareOptions)
const node = new NodeTelefunc()
const bun = new BunTelefunc()
const deno = new DenoTelefunc()

void [
  cloudflare.serve,
  cloudflare.TelefuncDurableObject,
  node.installWebSocket,
  node.serve,
  bun.websocket,
  bun.serve,
  deno.serve,
]
