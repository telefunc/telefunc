export {
  SERIALIZER_PREFIX_OBSERVABLE,
  SERIALIZER_PREFIX_SUBJECT,
  MSG,
  OBS_MSG,
  wireSubject,
  wireSourceObservable,
  wireProxyObservable,
  swallowedErrors,
  markSwallowed,
}
export type {
  ObservableMetadata,
  SubjectMetadata,
  ObservableContract,
  SubjectContract,
  SubjectMessage,
  ObservableMessage,
}

import type { TypeContract, ChannelBase } from 'telefunc'
import { Abort } from 'telefunc/client'
import type { Subject, Subscriber } from 'rxjs'
import { Observable, type Subscription } from 'rxjs'
import { isObject } from './isObject.js'
import { assert } from './assert.js'

/** WeakSet of errors that `onUnhandledError` handlers may use to suppress
 *  telefunc-originated errors. Populated by markSwallowed() before any call
 *  to subject.error()/sub.error() in the wire layer. */
const swallowedErrors = new WeakSet<object>()
function markSwallowed(err: unknown) {
  assert(isObject(err))
  swallowedErrors.add(err)
}

const SERIALIZER_PREFIX_OBSERVABLE = '!TelefuncObservable:'
const SERIALIZER_PREFIX_SUBJECT = '!TelefuncSubject:'

type ObservableMetadata = {
  channelId: string
}

type SubjectMetadata = {
  channelId: string
}

type ObservableContract = TypeContract<Observable<unknown>, Observable<unknown>, ObservableMetadata>
type SubjectContract = TypeContract<Subject<unknown>, Subject<unknown>, SubjectMetadata>

// === Subject protocol ===

const MSG = {
  SUBSCRIBE: 's',
  UNSUBSCRIBE: 'u',
  NEXT: 'n',
  ERROR: 'e',
  COMPLETE: 'c',
} as const

type SubjectMessage =
  | { t: typeof MSG.SUBSCRIBE }
  | { t: typeof MSG.UNSUBSCRIBE }
  | { t: typeof MSG.NEXT; v: unknown }
  | { t: typeof MSG.ERROR }
  | { t: typeof MSG.COMPLETE }

// === Observable protocol ===

const OBS_MSG = {
  SUBSCRIBE: 's',
  UNSUBSCRIBE: 'u',
  NEXT: 'n',
  ERROR: 'e',
  COMPLETE: 'c',
} as const

type ObservableMessage =
  | { t: typeof OBS_MSG.SUBSCRIBE; id: number }
  | { t: typeof OBS_MSG.UNSUBSCRIBE; id: number }
  | { t: typeof OBS_MSG.NEXT; id: number; v: unknown }
  | { t: typeof OBS_MSG.ERROR; id: number }
  | { t: typeof OBS_MSG.COMPLETE; id: number }

// === Subject wiring ===

/**
 * Wire a Subject to a channel. Symmetric — both sides use the same function.
 *
 * - Overrides `_subscribe` to send MSG.SUBSCRIBE/MSG.UNSUBSCRIBE when local
 *   code subscribes/unsubscribes.
 * - On MSG.SUBSCRIBE from remote: creates a real `subject.subscribe()` that
 *   forwards values to the channel.
 * - On MSG.NEXT/ERROR/COMPLETE from remote: calls `subject.next/error/complete`
 *   directly. The re-entrancy guard prevents the forwarding subscription from
 *   echoing back to the originating channel.
 * - On channel close: just unsubscribes. Does NOT error the Subject — that's
 *   the caller's responsibility (reviver/replacer) because only they know
 *   whether the Subject is safe to terminate (per-request vs potentially shared).
 */
function wireSubject(subject: Subject<unknown>, channel: ChannelBase<SubjectMessage, SubjectMessage>) {
  let isProcessingRemote = false
  let remoteSub: Subscription | null = null
  let localSubscriberCount = 0

  const remoteSubscribe = () => {
    if (remoteSub) return
    remoteSub = subject.subscribe({
      next(v) {
        if (channel.isClosed || isProcessingRemote) return
        channel.send({ t: MSG.NEXT, v })
      },
      error(err) {
        if (channel.isClosed || isProcessingRemote) return
        if (err instanceof Abort) {
          channel.abort(err.abortValue, err.message)
          return
        }
        console.error('[telefunc:rxjs] Subject error:', err)
        channel.send({ t: MSG.ERROR })
        void channel.close()
      },
      complete() {
        if (channel.isClosed || isProcessingRemote) return
        channel.send({ t: MSG.COMPLETE })
        void channel.close()
      },
    })
  }

  const remoteUnsubscribe = () => {
    remoteSub?.unsubscribe()
    remoteSub = null
  }

  const origInternalSubscribe = (subject as any)._subscribe.bind(subject)
  ;(subject as any)._subscribe = function (subscriber: any) {
    const subscription = origInternalSubscribe(subscriber)
    if (subscription.closed) return subscription

    localSubscriberCount++
    if (localSubscriberCount === 1 && !channel.isClosed) channel.send({ t: MSG.SUBSCRIBE })

    subscription.add(() => {
      localSubscriberCount--
      if (localSubscriberCount === 0 && !channel.isClosed) channel.send({ t: MSG.UNSUBSCRIBE })
    })

    return subscription
  }

  // If the Subject already has observers (subscribed before wiring), notify
  // the remote so it creates a remoteSub and starts forwarding values.
  if (subject.observed && !channel.isClosed) {
    localSubscriberCount = 1
    channel.send({ t: MSG.SUBSCRIBE })
  }

  channel.listen((msg) => {
    isProcessingRemote = true
    try {
      switch (msg.t) {
        case MSG.SUBSCRIBE:
          remoteSubscribe()
          break
        case MSG.UNSUBSCRIBE:
          remoteUnsubscribe()
          break
        case MSG.NEXT:
          subject.next(msg.v)
          break
        case MSG.ERROR: {
          const err = new Error('Internal error — see logs')
          markSwallowed(err)
          subject.error(err)
          void channel.close()
          break
        }
        case MSG.COMPLETE:
          subject.complete()
          void channel.close()
          break
      }
    } finally {
      isProcessingRemote = false
    }
  })

  channel.onClose(() => {
    remoteUnsubscribe()
  })

  return {
    async close() {
      remoteUnsubscribe()
      await channel.close()
    },
    abort(abortValue?: unknown) {
      channel.abort(abortValue)
    },
  }
}

// === Observable wiring ===

/**
 * Wire the SOURCE Observable (replacer side — owns the original Observable).
 *
 * Listens for multiplexed subscription requests. Each OBS_MSG.SUBSCRIBE
 * creates a fresh observable.subscribe() — matching rxjs per-subscription semantics.
 */
function wireSourceObservable(
  observable: Observable<unknown>,
  channel: ChannelBase<ObservableMessage, ObservableMessage>,
) {
  const subscriptions = new Map<number, Subscription>()

  const unsubAll = () => {
    for (const sub of subscriptions.values()) sub.unsubscribe()
    subscriptions.clear()
  }

  channel.listen((msg) => {
    switch (msg.t) {
      case OBS_MSG.SUBSCRIBE: {
        const id = msg.id
        const sub = observable.subscribe({
          next(v) {
            if (channel.isClosed) return
            channel.send({ t: OBS_MSG.NEXT, id, v })
          },
          error(v) {
            subscriptions.delete(id)
            if (channel.isClosed) return
            if (v instanceof Abort) {
              channel.abort(v.abortValue, v.message)
              return
            }
            console.error('[telefunc:rxjs] Observable error:', v)
            channel.send({ t: OBS_MSG.ERROR, id })
          },
          complete() {
            subscriptions.delete(id)
            if (channel.isClosed) return
            channel.send({ t: OBS_MSG.COMPLETE, id })
          },
        })
        subscriptions.set(id, sub)
        break
      }
      case OBS_MSG.UNSUBSCRIBE:
        subscriptions.get(msg.id)?.unsubscribe()
        subscriptions.delete(msg.id)
        break
    }
  })

  channel.onClose(() => {
    unsubAll()
  })

  return {
    async close() {
      await channel.close()
    },
    abort(abortValue?: unknown) {
      channel.abort(abortValue)
    },
  }
}

/**
 * Wire the PROXY Observable (reviver side — reconstructs an Observable from a channel).
 *
 * Each subscribe() sends OBS_MSG.SUBSCRIBE with a unique id, triggering a fresh
 * server-side subscription. Messages are routed by id.
 */
function wireProxyObservable(channel: ChannelBase<ObservableMessage, ObservableMessage>) {
  const subscribers = new Map<number, Subscriber<unknown>>()
  let nextId = 0

  channel.listen((msg) => {
    const sub = subscribers.get(msg.id)
    if (!sub) return
    switch (msg.t) {
      case OBS_MSG.NEXT:
        sub.next(msg.v)
        break
      case OBS_MSG.ERROR: {
        const err = new Error('Internal error — see logs')
        markSwallowed(err)
        sub.error(err)
        subscribers.delete(msg.id)
        break
      }
      case OBS_MSG.COMPLETE:
        sub.complete()
        subscribers.delete(msg.id)
        break
    }
  })

  channel.onClose((err) => {
    if (subscribers.size === 0) return
    for (const sub of subscribers.values()) {
      if (err) {
        markSwallowed(err)
        sub.error(err)
      } else {
        sub.complete()
      }
    }
    subscribers.clear()
  })

  const observable = new Observable<unknown>((subscriber) => {
    const id = nextId++
    subscribers.set(id, subscriber)
    if (!channel.isClosed) channel.send({ t: OBS_MSG.SUBSCRIBE, id })
    return () => {
      subscribers.delete(id)
      if (!channel.isClosed) channel.send({ t: OBS_MSG.UNSUBSCRIBE, id })
    }
  })

  return {
    observable,
    async close() {
      await channel.close()
    },
    abort(abortValue?: unknown) {
      channel.abort(abortValue)
    },
  }
}
