// Runtime entry points, against the host ambient types an application on each runtime supplies.
import * as cloudflare from 'telefunc/cloudflare'
import * as node from 'telefunc/node'
import * as bun from 'telefunc/bun'
import * as deno from 'telefunc/deno'

void [cloudflare, node, bun, deno]
