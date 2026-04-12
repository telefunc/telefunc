export {
  onObservable,
  onObservableComplete,
  onObservableError,
  onSubject,
  onSharedSubject,
  onSubjectServerComplete,
  onObservableFromClient,
  onSubjectEcho,
  onSubjectMultiSubscribe,
  onObservableServerError,
  onSubjectServerError,
  onSubjectArgNoErrorHandler,
  onSharedSubjectOneErrors,
  onSubjectNoHandler,
}

import { Observable, Subject, interval } from 'rxjs'
import { map, take } from 'rxjs/operators'
import { getContext } from 'telefunc'
import { cleanupState } from '../../cleanup-state'

// === Observable: server → client ===

async function onObservable() {
  return interval(500).pipe(
    map((i) => `tick-${i}`),
    take(5),
  )
}

async function onObservableComplete() {
  return new Observable<string>((subscriber) => {
    subscriber.next('a')
    subscriber.next('b')
    subscriber.complete()
  })
}

async function onObservableError() {
  return new Observable<string>((subscriber) => {
    subscriber.next('ok')
    subscriber.error('test-error')
  })
}

// === Subject: bidirectional ===

async function onSubject() {
  const subject = new Subject<string>()
  const sub = interval(500)
    .pipe(map((i) => `server-${i + 1}`))
    .subscribe((v) => subject.next(v))
  const { onClose } = getContext()
  onClose(() => {
    sub.unsubscribe()
    cleanupState.subjectCleanedUp = 'true'
  })
  return subject
}

// Shared subject: multiple clients should multicast
const sharedSubject = new Subject<string>()
async function onSharedSubject() {
  return sharedSubject
}

// Server-initiated complete
async function onSubjectServerComplete() {
  const subject = new Subject<string>()
  // Emit after the wire is established (client needs time to subscribe)
  setTimeout(() => subject.next('before-complete'), 200)
  setTimeout(() => subject.complete(), 700)
  return subject
}

// === Observable: client → server ===

async function onObservableFromClient(input$: Observable<string>) {
  const received: string[] = []
  return new Promise<string[]>((resolve) => {
    input$.subscribe({
      next(v) {
        received.push(v)
      },
      complete() {
        cleanupState.clientObsCompleted = 'true'
        resolve(received)
      },
    })
  })
}

// === Subject: echo (server subscribes + sends back) ===
//
// Uses setTimeout to defer the echo call. The wire's re-entrancy guard blocks
// sends on the SAME channel while processing an incoming MSG.NEXT — so a
// synchronous `subject.next('echo:' + v)` inside the subscribe callback would
// be silently dropped (still in the outer handler's sync stack). Deferring to
// a fresh task breaks out of the re-entrant window and the send goes through.
async function onSubjectEcho() {
  const subject = new Subject<string>()
  subject.subscribe((v) => {
    if (!v.startsWith('echo:')) {
      setTimeout(() => subject.next(`echo:${v}`), 0)
    }
  })
  return subject
}

// === Subject: multiple subscriptions on same observable ===

async function onSubjectMultiSubscribe() {
  const subject = new Subject<number>()
  // Delay start so the wire's MSG.SUBSCRIBE roundtrip completes first
  setTimeout(() => {
    interval(300)
      .pipe(take(3))
      .subscribe({
        next: (i) => subject.next(i + 1),
        complete: () => subject.complete(),
      })
  }, 200)
  return subject
}

// === Server-initiated error propagation ===
//
// Note: `throw Abort()` only works synchronously before the telefunction returns.
// Once a Subject/Observable has been returned, the way to signal failure from
// the server is `subject.error()` / `subscriber.error()`. These tests cover
// that path — the rxjs equivalent of server-side error signaling.

// Server returns an Observable that emits one value, then errors itself.
async function onObservableServerError() {
  return new Observable<string>((subscriber) => {
    subscriber.next('before-error')
    setTimeout(() => subscriber.error(new Error('server-side-error')), 200)
  })
}

// Server returns a Subject, emits one value, then errors it.
async function onSubjectServerError() {
  const subject = new Subject<string>()
  setTimeout(() => subject.next('before-error'), 200)
  setTimeout(() => subject.error(new Error('server-side-error')), 500)
  return subject
}

// Accepts a Subject argument. Server subscribes WITHOUT an error handler.
// When the client errors the Subject, the server side must NOT crash —
// onUnhandledError swallows all rxjs errors on the server.
async function onSubjectArgNoErrorHandler(input$: Subject<string>) {
  input$.subscribe((v) => {
    cleanupState.subjectArgReceived = v
  })
  const { onClose } = getContext()
  onClose(() => {
    cleanupState.subjectArgClosed = 'true'
  })
}

// Shared Subject across clients: client A errors its proxy — transparent
// behavior means the shared server-side Subject dies for all clients.
const sharedErrorSubject = new Subject<string>()
async function onSharedSubjectOneErrors() {
  return sharedErrorSubject
}

// Server returns a Subject, subscribes to it with NO error handler, then
// errors it shortly after. Server must not crash (onUnhandledError swallows).
async function onSubjectNoHandler() {
  const subject = new Subject<string>()
  subject.subscribe((v) => {
    cleanupState.noHandlerReceived = v
  })
  const { onClose } = getContext()
  onClose(() => {
    cleanupState.noHandlerClosed = 'true'
  })
  setTimeout(() => subject.error(new Error('no-handler-error')), 600)
  return subject
}
