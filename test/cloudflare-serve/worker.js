// A Worker with neither `telefunc/cloudflare` nor a backend: Broadcast and Room run in memory, in one isolate.
import { Broadcast } from 'telefunc'
import { Room } from '@telefunc/room'

let published = 0
let answers = 0
let frames = 0
let member = null

Broadcast.subscribe('published', () => published++)
Broadcast.subscribe('ask', () => void Broadcast.publish('first answer', 'x'))
Broadcast.subscribe('first answer', () => void Broadcast.publish('second answer', 'x'))
Broadcast.subscribe('second answer', () => answers++)

const routes = {
  // A listener's answer to an answer waits for a turn, which this request may not live to see.
  '/answer': async () => {
    await Broadcast.publish('ask', 'x')
    return answers
  },
  '/answers': () => answers,
  // Crosses the count of publishes between turns.
  '/publishes': () => {
    for (let i = 0; i < 16_400; i++) void Broadcast.publish('published', 'x')
    return published
  },
  '/publish': async () => {
    await Broadcast.publish('published', 'x')
    return published
  },
  '/join': async () => {
    const room = await Room.create(crypto.randomUUID())
    room.subscribe(() => frames++)
    member = await room.join({ meta: { name: 'Alice' } })
    return frames
  },
  '/commit': async () => {
    await member.publish('x')
    return frames
  },
  '/frames': () => frames,
}

export default {
  async fetch(request) {
    const route = routes[new URL(request.url).pathname]
    return new Response(route ? String(await route()) : 'Not found', { status: route ? 200 : 404 })
  },
}
