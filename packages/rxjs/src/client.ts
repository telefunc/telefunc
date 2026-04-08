import { config } from 'telefunc/client'
import type {
  TelefuncClientExtension,
  ReplacerType,
  ReviverType,
  ClientReplacerContext,
  ClientReviverContext,
} from 'telefunc/client'
import { Observable, Subject, BehaviorSubject, ReplaySubject } from 'rxjs'
import {
  SERIALIZER_PREFIX_OBSERVABLE,
  SERIALIZER_PREFIX_SUBJECT,
  wireSourceSubject,
  wireProxySubject,
  wireSourceObservable,
  wireProxyObservable,
  type ObservableContract,
  type SubjectContract,
  type ObservableMessage,
  type SubjectMessage,
  type SubjectVariant,
  type SubjectMetadata,
  type ObservableMetadata,
} from './shared.js'

// === Server→client revivers ===

const subjectReviver: ReviverType<SubjectContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_SUBJECT,
  createValue(metadata, context) {
    const channel = context.createChannel<SubjectMessage, SubjectMessage>({ channelId: metadata.channelId })

    let subject: Subject<unknown>
    switch (metadata.variant) {
      case 'BehaviorSubject':
        subject = new BehaviorSubject(metadata.initialValue)
        break
      case 'ReplaySubject': {
        const rs = new ReplaySubject(metadata.bufferSize)
        if (metadata.replayBuffer) {
          for (const v of metadata.replayBuffer) rs.next(v)
        }
        subject = rs
        break
      }
      default:
        subject = new Subject()
    }

    const alwaysForward = metadata.variant === 'BehaviorSubject' || metadata.variant === 'ReplaySubject'
    const wire = wireProxySubject(subject, channel, alwaysForward)

    return {
      value: subject,
      async close() {
        await wire.close()
      },
      abort() {
        wire.abort()
      },
    }
  },
}

const observableReviver: ReviverType<ObservableContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_OBSERVABLE,
  createValue(metadata, context) {
    const channel = context.createChannel<ObservableMessage, ObservableMessage>({ channelId: metadata.channelId })
    const wire = wireProxyObservable(channel)

    return {
      value: wire.observable,
      async close() {
        await wire.close()
      },
      abort() {
        wire.abort()
      },
    }
  },
}

// === Client→server replacers ===

const subjectReplacer: ReplacerType<SubjectContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_SUBJECT,
  detect(value): value is Subject<unknown> {
    return value instanceof Subject
  },
  getMetadata(subject, context) {
    let variant: SubjectVariant = 'Subject'
    const metadata: SubjectMetadata = { channelId: '', variant }

    if (subject instanceof BehaviorSubject) {
      variant = 'BehaviorSubject'
      metadata.variant = variant
      metadata.initialValue = subject.getValue()
    } else if (subject instanceof ReplaySubject) {
      variant = 'ReplaySubject'
      metadata.variant = variant
      metadata.bufferSize = (subject as any)._bufferSize
      const buffer: unknown[] = []
      const sub = subject.subscribe({
        next(v) {
          buffer.push(v)
        },
      })
      sub.unsubscribe()
      metadata.replayBuffer = buffer
    }

    const channel = context.createChannel<SubjectMessage, SubjectMessage>({ ack: false })
    metadata.channelId = channel.id

    const alwaysForward = subject instanceof BehaviorSubject || subject instanceof ReplaySubject
    const wire = wireSourceSubject(subject, channel, alwaysForward)

    return {
      metadata,
      async close() {
        await wire.close()
      },
      abort() {
        wire.abort()
      },
    }
  },
}

const observableReplacer: ReplacerType<ObservableContract, ClientReplacerContext> = {
  prefix: SERIALIZER_PREFIX_OBSERVABLE,
  detect(value): value is Observable<unknown> {
    return value instanceof Observable
  },
  getMetadata(observable, context) {
    const channel = context.createChannel<ObservableMessage, ObservableMessage>({ ack: false })
    const wire = wireSourceObservable(observable, channel)

    const metadata: ObservableMetadata = { channelId: channel.id }
    return {
      metadata,
      async close() {
        await wire.close()
      },
      abort() {
        wire.abort()
      },
    }
  },
}

// === Extension registration ===

export const rxjsExtension = {
  name: '@telefunc/rxjs',
  responseTypes: [subjectReviver, observableReviver],
  requestTypes: [subjectReplacer, observableReplacer],
} satisfies TelefuncClientExtension

config.extensions.push(rxjsExtension)
