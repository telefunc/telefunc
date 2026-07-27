// Runtime-specific entry points from the released API baseline. The consumer supplies
// the same host ambient types an application on each runtime supplies.
import { Telefunc as CloudflareTelefunc, type CloudflareOptions } from 'telefunc/cloudflare'
import { Telefunc as NodeTelefunc } from 'telefunc/node'
import { Telefunc as BunTelefunc } from 'telefunc/bun'
import { Telefunc as DenoTelefunc } from 'telefunc/deno'

const _cloudflareOptions: CloudflareOptions = {
  bindingName: 'TelefuncDurableObject',
  kvBindingName: 'TelefuncKV',
  instanceName: 'telefunc',
  scale: 1,
  locationFallback: 'weur',
  jurisdiction: 'eu',
}

void [new CloudflareTelefunc(_cloudflareOptions), new NodeTelefunc(), new BunTelefunc(), new DenoTelefunc()]
