// Another server on the same store, as the store specs run one: a process of its own, on the built packages.
export { atAnotherServer, joinedByAnotherServer, hostingLease }

import { execFile } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import type { Redis } from 'ioredis'

/** What `work` prints, run on room `id` by another server: a process of its own, on the built packages, which then
 *  stops. */
async function atAnotherServer(host: string, id: string, work: string): Promise<string[]> {
  const server = `
import { Redis } from 'ioredis'
import { Room } from '@telefunc/room'
import { installRedis } from '@telefunc/redis'
installRedis(new Redis({ host: process.argv[1], username: 'telefunc', password: 'pw' }))
const id = process.argv[2]
${work}
process.exit(0)
`
  const cwd = fileURLToPath(new URL('..', import.meta.url))
  const { stdout } = await promisify(execFile)(process.execPath, ['--input-type=module', '-e', server, host, id], {
    cwd,
  })
  return stdout.trim().split('\n')
}

/** A participant that another server joined to room `id` with `options` under its own lease, which it then left
 *  behind, as a server that stopped does: the participant's id. */
async function joinedByAnotherServer(
  host: string,
  id: string,
  options: { identity?: string; hidden?: boolean } = {},
): Promise<string> {
  const printed = await atAnotherServer(host, id, `console.log((await Room.join(id, ${JSON.stringify(options)})).id)`)
  const participant = printed.at(-1)
  if (participant === undefined || participant === '') throw new Error(`the other server joined no one: ${printed}`)
  return participant
}

/** The incarnation's cells of room `id` and the lease that hosts `member`, as the store holds them. */
async function hostingLease(admin: Redis, id: string, member: string): Promise<{ values: string; lease: string }> {
  const [values] = await admin.keys(`tf:0:room:{${id}}:g:*:values`)
  if (values === undefined) throw new Error(`room ${id} has no cells`)
  const mark = (await admin.hkeys(values)).find((key) => key.startsWith('sm:') && key.endsWith(`:${member}`))
  const lease = mark?.split(':')[1]
  if (lease === undefined) throw new Error(`no lease hosts ${member}`)
  return { values, lease }
}
