export { onStartupBytes, onStartupOrder, onStartupOverload, onThrowWithCauseGetter, onBugReports }

import { Broadcast, config, onBug } from 'telefunc'

async function onStartupBytes() {
  const { send, open } = installStartupTransport()
  const seen: { bytes: number[]; seq: number; timestamp: number }[] = []
  const off = Broadcast.subscribeBinary('startup-bytes', (bytes, { seq, timestamp }) => {
    seen.push({ bytes: [...bytes], seq, timestamp })
  })
  try {
    send(Uint8Array.of(1, 11))
    send(Uint8Array.of(2, 22))
    open()
    await off.opened
    return seen
  } finally {
    off()
  }
}

async function onStartupOrder() {
  const key = 'startup-order'
  const { send, open } = installStartupTransport()
  const seen: number[] = []
  const off = Broadcast.subscribeBinary(key, (bytes) => {
    const value = bytes[0]!
    seen.push(value)
    if (value === 1) void Broadcast.publishBinary(key, Uint8Array.of(3))
  })
  try {
    send(Uint8Array.of(1))
    send(Uint8Array.of(2))
    open()
    await off.opened
    return seen
  } finally {
    off()
  }
}

async function onStartupOverload() {
  const key = 'startup-overload'
  const { send, open, stopped } = installStartupTransport()
  const frame = new Uint8Array(640 * 1024)
  const seen: number[] = []
  const record = (bytes: Uint8Array) => seen.push(bytes[0]!)
  const stalled = Broadcast.subscribeBinary(key, record)
  try {
    for (const value of [1, 2, 3]) {
      frame[0] = value
      send(frame)
    }
    // Opening would mask a stalled subscription. Check the refusal without opening it.
    const overloaded = await Promise.race([
      stalled.opened.then(
        () => 'opened',
        (err: Error) => err.cause instanceof Error && err.cause.name,
      ),
      new Promise<string>((resolve) => setImmediate(() => resolve('still waiting'))),
    ])
    const stoppedAfterRefusal = stopped()
    stalled()
    const next = Broadcast.subscribeBinary(key, record)
    try {
      for (const value of [4, 5]) {
        frame[0] = value
        send(frame)
      }
      open()
      await next.opened
      return { overloaded, stoppedAfterRefusal, seen }
    } finally {
      next()
    }
  } finally {
    stalled()
  }
}

/** A broker that acknowledges on demand and uses the same receive buffer and ordering record for consecutive messages. */
function installStartupTransport() {
  type Sink = Parameters<NonNullable<typeof config.broadcast.transport>['listenBinary']>[1]
  let listener: { receive: Sink; open: () => void } | undefined
  let receiveBuffer: Uint8Array | undefined
  const ordering = { seq: 0, timestamp: 0 }
  let unsubscribed = 0
  const send = (data: Uint8Array) => {
    if (receiveBuffer?.byteLength !== data.byteLength) receiveBuffer = new Uint8Array(data.byteLength)
    receiveBuffer.set(data)
    ordering.seq++
    ordering.timestamp += 10
    listener?.receive(receiveBuffer, ordering)
    return ordering
  }
  config.broadcast = {
    transport: {
      send() {
        throw new Error('This fixture publishes binary messages')
      },
      listen() {
        throw new Error('This fixture listens for binary messages')
      },
      sendBinary: (_key, data) => send(data),
      listenBinary(_key, receive) {
        const { promise, resolve } = Promise.withResolvers<void>()
        listener = { receive, open: resolve }
        return {
          opened: promise,
          unsubscribe() {
            listener = undefined
            unsubscribed++
          },
        }
      },
    },
  }
  return { send, open: () => listener?.open(), stopped: () => unsubscribed }
}

const reported: string[] = []
const testedError = 'broadcast-listener-bug: telefunction error'

function withUnreadableCause(message: string): Error {
  return Object.defineProperty(new Error(message), 'cause', {
    get() {
      throw new Error('Cause is unavailable')
    },
  })
}

async function onThrowWithCauseGetter() {
  throw withUnreadableCause(testedError)
}

async function onBugReports() {
  return reported.splice(0)
}

onBug((err) => {
  if (err instanceof Error && err.message === testedError) {
    throw withUnreadableCause('broadcast-listener-bug: monitor threw')
  }
})
onBug(async (err) => {
  if (err instanceof Error && err.message === testedError) {
    throw Object.defineProperty(new Error('monitor rejected'), 'stack', {
      get() {
        throw new Error('Stack is unavailable')
      },
    })
  }
})
onBug((err) => {
  if (err instanceof Error && err.message === testedError) reported.push(err.message)
})
