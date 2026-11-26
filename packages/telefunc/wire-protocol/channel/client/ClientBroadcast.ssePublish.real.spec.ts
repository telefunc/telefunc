import { expect, test } from 'vitest'
import { WIRE_GATHER_BYTES } from '../../constants.js'
import { SseEvents } from '../../wire/SseEvents.js'
import { isOneFrame, messageFrames } from '../../wire/frame.js'
import { TAG, isPublishBody } from '../../wire/tags.js'
import { getChannelMux } from '../server/ChannelMux.js'
import { Broadcast, ServerBroadcast } from '../server/ServerBroadcast.js'
import { ATTACHABLE_NOW } from '../server/ServerChannel.js'
import { ClientBroadcast } from './ClientBroadcast.js'
import { until, useRealServer, world } from './realServer.fixture.js'

useRealServer(['sse'])

function publishEvents(): (number | undefined)[][] {
  const publishes: (number | undefined)[][] = []
  const events = new SseEvents()
  world.hooks = [
    {
      kind: 'open',
      stream: (body) =>
        body.pipeThrough(
          new TransformStream<Uint8Array, Uint8Array>({
            transform(chunk, controller) {
              events.push(chunk, (event) => {
                const tag = event[0]
                if (tag === undefined || (tag !== TAG.PUBLISH_TO && !isPublishBody(tag))) return
                publishes.push(isOneFrame(event) ? [tag] : Array.from(messageFrames(event), (frame) => frame[0]))
              })
              controller.enqueue(chunk)
            },
          }),
        ),
    },
  ]
  return publishes
}

test.each([
  {
    name: 'an idle SSE wire sends each fitting publish in one event',
    textValue: 'first',
    payload: Uint8Array.of(7, 8),
    events: [
      [TAG.PUBLISH_TO, TAG.PUBLISH],
      [TAG.PUBLISH_TO, TAG.PUBLISH_BINARY],
    ],
  },
  {
    name: 'an SSE event carrying a single frame still delivers an oversized publish',
    textValue: 'x'.repeat(WIRE_GATHER_BYTES),
    payload: new Uint8Array(WIRE_GATHER_BYTES).fill(9),
    events: [[TAG.PUBLISH_TO], [TAG.PUBLISH], [TAG.PUBLISH_TO], [TAG.PUBLISH_BINARY]],
  },
])('$name and the page reads both values in order', async ({ textValue, payload, events }) => {
  const seenEvents = publishEvents()
  const server = new ServerBroadcast<string>({ key: crypto.randomUUID() })
  getChannelMux().registerChannel(server, ATTACHABLE_NOW)
  const page = new ClientBroadcast<string>({
    channelId: server.id,
    connection: {
      telefuncUrl: world.telefuncUrl,
      transports: ['sse'],
      idleTimeout: 0,
      connectionKey: crypto.randomUUID(),
    },
  })
  const values: (string | Uint8Array)[] = []
  const text = page.subscribe((value) => void values.push(value))
  const binary = page.subscribeBinary((value) => void values.push(value))
  try {
    await Promise.all([text.opened, binary.opened])
    await Broadcast.publish(server.key, textValue)
    await until(() => values.length === 1)
    await Broadcast.publishBinary(server.key, payload)
    await until(() => values.length === 2)
    expect(values).toEqual([textValue, payload])
    expect(seenEvents).toEqual(events)
  } finally {
    page.abort()
    server.abort()
  }
})
