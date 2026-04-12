import { config } from 'telefunc/client'
import type {
  TelefuncClientExtension,
  ReplacerType,
  ReviverType,
  ClientReplacerContext,
  ClientReviverContext,
} from 'telefunc/client'
import { Observable, Subject, config as rxjsConfig } from 'rxjs'
import {
  SERIALIZER_PREFIX_OBSERVABLE,
  SERIALIZER_PREFIX_SUBJECT,
  wireSubject,
  wireSourceObservable,
  wireProxyObservable,
  swallowedErrors,
  markSwallowed,
  type ObservableContract,
  type SubjectContract,
  type ObservableMessage,
  type SubjectMessage,
  type SubjectMetadata,
  type ObservableMetadata,
} from './shared.js'
import { isObject } from './isObject.js'

// Client-side: only swallow telefunc-originated errors. Non-telefunc errors
// fall through to rxjs's default (throw), preserving the user's own rxjs
// error-handling semantics in the browser.
const prevOnUnhandledError = rxjsConfig.onUnhandledError
rxjsConfig.onUnhandledError = (err) => {
  if (isObject(err) && swallowedErrors.has(err)) {
    swallowedErrors.delete(err)
    return
  }
  if (prevOnUnhandledError) prevOnUnhandledError(err)
  else throw err
}

// === Server→client revivers ===

const subjectReviver: ReviverType<SubjectContract, ClientReviverContext> = {
  prefix: SERIALIZER_PREFIX_SUBJECT,
  createValue(metadata, context) {
    const channel = context.createChannel<SubjectMessage, SubjectMessage>({ channelId: metadata.channelId })
    const subject = new Subject<unknown>()
    const wire = wireSubject(subject, channel)

    // Proxy side: Subject is per-request (created here). Propagate abort errors
    // to local subscribers so the user's `subscribe({ error })` fires.
    channel.onClose((err) => {
      if (err) {
        markSwallowed(err)
        subject.error(err)
      }
    })

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
    const channel = context.createChannel<SubjectMessage, SubjectMessage>({ ack: false })
    const metadata: SubjectMetadata = { channelId: channel.id }

    const wire = wireSubject(subject, channel)

    // Source side on client: the Subject is owned by this call. If the server
    // aborts, terminate the Subject so local subscribers see the error.
    channel.onClose((err) => {
      if (err) {
        markSwallowed(err)
        subject.error(err)
      }
    })

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
