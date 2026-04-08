export {
  SERIALIZER_PREFIX_OBSERVABLE,
  SERIALIZER_PREFIX_SUBJECT,
  MSG,
  OBS_MSG,
  wireSourceSubject,
  wireProxySubject,
  wireSourceObservable,
  wireProxyObservable,
}
export type {
  ObservableMetadata,
  SubjectMetadata,
  SubjectVariant,
  ObservableContract,
  SubjectContract,
  SubjectMessage,
  ObservableMessage,
}

import type { TypeContract, ChannelBase } from 'telefunc'
import type { Subject, Subscriber } from 'rxjs'
import { Observable, type Subscription } from 'rxjs'

const SERIALIZER_PREFIX_OBSERVABLE = '!TelefuncObservable:'
const SERIALIZER_PREFIX_SUBJECT = '!TelefuncSubject:'

type SubjectVariant = 'Subject' | 'BehaviorSubject' | 'ReplaySubject'

type ObservableMetadata = {
  channelId: string
}

type SubjectMetadata = {
  channelId: string
  variant: SubjectVariant
  initialValue?: unknown
  bufferSize?: number
  replayBuffer?: unknown[]
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
  | { t: typeof MSG.ERROR; v: unknown }
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
  | { t: typeof OBS_MSG.ERROR; id: number; v: unknown }
  | { t: typeof OBS_MSG.COMPLETE; id: number }

// === Subject wiring ===

/**
 * Wire the SOURCE Subject (replacer side — owns the original Subject).
 *
 * Intercepts next/error/complete to forward to remote.
 * No _subscribe override — prevents interference with Observable replacer on same Subject.
 *
 * @param alwaysForward true for BehaviorSubject/ReplaySubject (state must stay in sync),
 *                      false for plain Subject (lazy — only forward when remote has subscribers)
 */
function wireSourceSubject(
  subject: Subject<unknown>,
  channel: ChannelBase<SubjectMessage, SubjectMessage>,
  alwaysForward: boolean,
) {
  let remoteSubscribed = false

  const origNext = subject.next.bind(subject)
  const origError = subject.error.bind(subject)
  const origComplete = subject.complete.bind(subject)

  subject.next = (v: unknown) => {
    origNext(v)
    if (channel.isClosed) return
    if (alwaysForward || remoteSubscribed) channel.send({ t: MSG.NEXT, v })
  }

  subject.error = (err: unknown) => {
    origError(err)
    if (channel.isClosed) return
    channel.send({ t: MSG.ERROR, v: err })
  }

  subject.complete = () => {
    origComplete()
    if (channel.isClosed) return
    channel.send({ t: MSG.COMPLETE })
  }

  channel.listen((msg) => {
    switch (msg.t) {
      case MSG.SUBSCRIBE:
        remoteSubscribed = true
        break
      case MSG.UNSUBSCRIBE:
        remoteSubscribed = false
        break
      case MSG.NEXT:
        origNext(msg.v)
        break
      case MSG.ERROR:
        origError(msg.v)
        break
      case MSG.COMPLETE:
        origComplete()
        break
    }
  })

  // Source side: always complete the local subject when the transport goes away.
  // Transport errors (timeouts, disconnects) aren't actionable for local subscribers
  // — they just need the completion signal so resources (intervals, subscriptions)
  // get cleaned up. Erroring would also surface as an unhandled error in subscribers
  // that only registered a `complete` handler, which can crash the host process.
  channel.onClose(() => {
    origComplete()
  })

  return {
    async close() {
      origComplete()
      await channel.close()
    },
    abort(abortValue?: unknown) {
      channel.abort(abortValue)
    },
  }
}

/**
 * Wire the PROXY Subject (reviver side — the reconstructed Subject).
 *
 * Intercepts next/error/complete to forward to remote.
 * For plain Subject (alwaysForward=false): overrides _subscribe to detect all subscription
 * paths (direct, pipe, operators) and signal remote via MSG.SUBSCRIBE/MSG.UNSUBSCRIBE.
 * For BehaviorSubject/ReplaySubject (alwaysForward=true): no _subscribe override needed.
 *
 * @param alwaysForward true for BehaviorSubject/ReplaySubject, false for plain Subject
 */
function wireProxySubject(
  subject: Subject<unknown>,
  channel: ChannelBase<SubjectMessage, SubjectMessage>,
  alwaysForward: boolean,
) {
  let localSubscriberCount = 0

  const origNext = subject.next.bind(subject)
  const origError = subject.error.bind(subject)
  const origComplete = subject.complete.bind(subject)

  subject.next = (v: unknown) => {
    origNext(v)
    if (channel.isClosed) return
    if (alwaysForward || localSubscriberCount > 0) channel.send({ t: MSG.NEXT, v })
  }

  subject.error = (err: unknown) => {
    origError(err)
    if (channel.isClosed) return
    channel.send({ t: MSG.ERROR, v: err })
  }

  subject.complete = () => {
    origComplete()
    if (channel.isClosed) return
    channel.send({ t: MSG.COMPLETE })
  }

  if (!alwaysForward) {
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
  }

  channel.listen((msg) => {
    switch (msg.t) {
      case MSG.NEXT:
        origNext(msg.v)
        break
      case MSG.ERROR:
        origError(msg.v)
        break
      case MSG.COMPLETE:
        origComplete()
        break
    }
  })

  channel.onClose((err) => {
    if (err) origError(err)
    else origComplete()
  })

  return {
    async close() {
      origComplete()
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
            channel.send({ t: OBS_MSG.ERROR, id, v })
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
      case OBS_MSG.ERROR:
        sub.error(msg.v)
        subscribers.delete(msg.id)
        break
      case OBS_MSG.COMPLETE:
        sub.complete()
        subscribers.delete(msg.id)
        break
    }
  })

  channel.onClose((err) => {
    if (subscribers.size === 0) return
    for (const sub of subscribers.values()) {
      if (err) sub.error(err)
      else sub.complete()
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
