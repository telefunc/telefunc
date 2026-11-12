export { prepareIteratorAck, sendIteratorAck }
export type { IteratorAckOptions }

import type { ChannelBase } from 'telefunc'

type ProbeChannel = ChannelBase<string, string>

type IteratorAckOptions = { binary: boolean; mode: 'abort' | 'return'; answer: boolean }

function prepareIteratorAck(channel: ProbeChannel, { binary, mode, answer }: IteratorAckOptions) {
  let enter!: () => void
  let release!: () => void
  const entered = new Promise<void>((resolve) => {
    enter = resolve
  })
  const held = new Promise<void>((resolve) => {
    release = resolve
  })
  const responder = new AbortController()
  const listen = binary ? channel.listenBinary.bind(channel) : channel.listen.bind(channel)
  const isPrime = (data: string | Uint8Array) => (data instanceof Uint8Array ? data[0] === 0 : data === 'prime')
  listen(async (data: string | Uint8Array) => {
    if (isPrime(data)) return
    responder.abort()
    enter()
    await held
  })
  const controller = new AbortController()
  const iterator = channel.messages({ signal: controller.signal })
  const read = iterator.next()
  void read.catch(() => {})
  let answers = 0
  if (answer)
    listen(
      (data: string | Uint8Array) => {
        if (isPrime(data)) return
        answers++
        return 'answer'
      },
      { signal: responder.signal },
    )
  return {
    async primed() {
      await read
    },
    async cancel() {
      await entered
      if (mode === 'abort') {
        controller.abort('stop')
        await read.catch((err) => {
          if (err !== 'stop') throw err
        })
      } else {
        await iterator.return?.()
      }
      release()
    },
    answers: () => answers,
  }
}

function sendIteratorAck(channel: ProbeChannel, { binary, prime }: { binary: boolean; prime: boolean }) {
  if (binary) {
    if (prime) return channel.sendBinary(new Uint8Array([0]))
    return channel.sendBinary(new Uint8Array([1]), { ack: true })
  }
  if (prime) return channel.send('prime')
  return channel.send('request', { ack: true })
}
