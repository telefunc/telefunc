import { config } from 'telefunc'
import type {
  TelefuncServerExtension,
  ReplacerType,
  ReviverType,
  ServerReplacerContext,
  ServerReviverContext,
} from 'telefunc'
import { Observable, Subject, config as rxjsConfig } from 'rxjs'

import {
  SERIALIZER_PREFIX_OBSERVABLE,
  SERIALIZER_PREFIX_SUBJECT,
  wireSubject,
  wireSourceObservable,
  wireProxyObservable,
  type ObservableContract,
  type SubjectContract,
  type ObservableMessage,
  type SubjectMessage,
  type SubjectMetadata,
  type ObservableMetadata,
} from './shared.js'

// Server-side: swallow ALL unhandled rxjs errors to prevent process crash.
// Still log them so bugs are visible in server logs.
rxjsConfig.onUnhandledError = (err) => {
  console.error('[telefunc:rxjs] Unhandled rxjs error:', err)
}

// === Server→client replacers ===
// Detection order: Subject before Observable (Subject extends Observable)

const subjectReplacer: ReplacerType<SubjectContract, ServerReplacerContext> = {
  prefix: SERIALIZER_PREFIX_SUBJECT,
  detect(value): value is Subject<unknown> {
    return value instanceof Subject
  },
  getMetadata(subject, context) {
    const channel = context.createChannel<SubjectMessage, SubjectMessage>({ ack: false })
    const metadata: SubjectMetadata = { channelId: channel.id }

    const wire = wireSubject(subject, channel)

    return {
      metadata,
      async close() {
        await wire.close()
      },
      abort(abortError) {
        wire.abort(abortError.abortValue)
      },
    }
  },
}

const observableReplacer: ReplacerType<ObservableContract, ServerReplacerContext> = {
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
      abort(abortError) {
        wire.abort(abortError.abortValue)
      },
    }
  },
}

// === Client→server revivers ===

const subjectReviver: ReviverType<SubjectContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_SUBJECT,
  createValue(metadata, context) {
    const channel = context.createChannel<SubjectMessage, SubjectMessage>({ id: metadata.channelId })

    const subject = new Subject<unknown>()
    const wire = wireSubject(subject, channel)

    // Proxy side: Subject is per-request (created here). Propagate abort errors
    // to local subscribers so the server function sees the error.
    channel.onClose((err) => {
      if (err) subject.error(err)
    })

    return {
      value: subject,
      async close() {
        await wire.close()
      },
      abort(abortError) {
        wire.abort(abortError.abortValue)
      },
    }
  },
}

const observableReviver: ReviverType<ObservableContract, ServerReviverContext> = {
  prefix: SERIALIZER_PREFIX_OBSERVABLE,
  createValue(metadata, context) {
    const channel = context.createChannel<ObservableMessage, ObservableMessage>({ id: metadata.channelId })
    const wire = wireProxyObservable(channel)

    return {
      value: wire.observable,
      async close() {
        await wire.close()
      },
      abort(abortError) {
        wire.abort(abortError.abortValue)
      },
    }
  },
}

// === Extension registration ===
declare global {
  namespace Telefunc {
    interface ShieldTypeMap {
      observable: Observable<any>
      subject: Subject<any>
    }
  }
}
export const rxjsExtension = {
  name: '@telefunc/rxjs',
  responseTypes: [subjectReplacer, observableReplacer],
  requestTypes: [subjectReviver, observableReviver],
  shieldTypes: {
    observable: (input: unknown) => input instanceof Observable,
    subject: (input: unknown) => input instanceof Subject,
  },
} satisfies TelefuncServerExtension

config.extensions.push(rxjsExtension)
