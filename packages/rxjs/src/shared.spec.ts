import { describe, expect, test } from 'vitest'
import { Subject } from 'rxjs'
import {
  MSG,
  OBS_MSG,
  wireProxyObservable,
  wireSourceObservable,
  wireSubject,
  type ObservableMessage,
  type SubjectMessage,
  type WireChannel,
} from './shared.js'

/** One end of a channel whose peer is the test: `deliver` hands its listener a message, and its `close()` ends once
 *  the test says the peer acknowledged it (`end()`), as a graceful close waits for its acknowledgement. */
class FakeChannel<T> implements WireChannel<T> {
  sent: T[] = []
  private listener: Parameters<WireChannel<T>['listen']>[0] | null = null
  private closeCallbacks: Parameters<WireChannel<T>['onClose']>[0][] = []
  private ended = false
  private acknowledge: (() => void) | null = null

  get isClosed(): boolean {
    return this.ended
  }
  async send(data: T): Promise<void> {
    this.sent.push(data)
  }
  listen(callback: Parameters<WireChannel<T>['listen']>[0]): void {
    this.listener = callback
  }
  onClose(callback: Parameters<WireChannel<T>['onClose']>[0]): void {
    this.closeCallbacks.push(callback)
  }
  close(): Promise<0> {
    return new Promise((resolve) => {
      this.acknowledge = () => resolve(0)
    })
  }
  abort(): void {
    this.end()
  }
  deliver(message: T): void {
    this.listener?.(message)
  }
  /** The peer acknowledged the close: the channel ends, and its close callbacks run. */
  end(): void {
    this.ended = true
    this.acknowledge?.()
    for (const callback of this.closeCallbacks) callback()
  }
}

describe('after `close()` was called, until the close ends', () => {
  test('an observable a telefunction returned gives its subscribers no further value, then completes them', async () => {
    const channel = new FakeChannel<ObservableMessage>()
    const wire = wireProxyObservable(channel)
    const got: unknown[] = []
    let completed = false
    wire.observable.subscribe({ next: (v) => got.push(v), complete: () => (completed = true) })
    channel.deliver({ t: OBS_MSG.NEXT, id: 0, v: 1 })
    channel.deliver({ t: OBS_MSG.NEXT, id: 0, v: 2 })
    const closing = wire.close()
    channel.deliver({ t: OBS_MSG.NEXT, id: 0, v: 3 })
    expect({ got, completed }).toEqual({ got: [1, 2], completed: false })
    channel.end()
    await closing
    expect({ got, completed }).toEqual({ got: [1, 2], completed: true })
  })

  test("an observable's end from the other end still reaches its subscribers", () => {
    const channel = new FakeChannel<ObservableMessage>()
    const wire = wireProxyObservable(channel)
    let completed = false
    wire.observable.subscribe({ complete: () => (completed = true) })
    void wire.close()
    channel.deliver({ t: OBS_MSG.COMPLETE, id: 0 })
    expect(completed).toBe(true)
  })

  test('a Subject gives its subscribers no further value, and forwards none of its own', () => {
    const channel = new FakeChannel<SubjectMessage>()
    const subject = new Subject<unknown>()
    const wire = wireSubject(subject, channel)
    const got: unknown[] = []
    subject.subscribe((v) => got.push(v))
    channel.deliver({ t: MSG.NEXT, v: 1 })
    void wire.close()
    channel.deliver({ t: MSG.NEXT, v: 2 })
    channel.deliver({ t: MSG.SUBSCRIBE })
    subject.next('local')
    expect(got).toEqual([1, 'local'])
    expect(channel.sent).toEqual([{ t: MSG.SUBSCRIBE }])
  })

  test('an observable the app passed sends no further value, and still sends its end', () => {
    const channel = new FakeChannel<ObservableMessage>()
    const source = new Subject<unknown>()
    const wire = wireSourceObservable(source, channel)
    channel.deliver({ t: OBS_MSG.SUBSCRIBE, id: 0 })
    source.next(1)
    void wire.close()
    source.next(2)
    channel.deliver({ t: OBS_MSG.SUBSCRIBE, id: 1 })
    source.complete()
    expect(channel.sent).toEqual([
      { t: OBS_MSG.NEXT, id: 0, v: 1 },
      { t: OBS_MSG.COMPLETE, id: 0 },
    ])
  })
})
