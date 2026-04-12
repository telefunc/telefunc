export { RxJSDemo }

import React, { useState, useEffect } from 'react'
import {
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
} from './RxJS.telefunc'
import { Subject as RxSubject, Observable as RxObservable } from 'rxjs'
import { close } from 'telefunc/client'

function RxJSDemo() {
  const [result, setResult] = useState('')
  const [hydrated, setHydrated] = useState(false)
  useEffect(() => setHydrated(true), [])
  return (
    <div className="max-w-3xl mx-auto px-8 py-10 space-y-4">
      <h1 className="text-2xl font-bold">RxJS Playground</h1>
      {hydrated && <span id="hydrated" />}

      {/* Observable: server emits 5 ticks then completes */}
      <button
        id="test-obs-ticks"
        onClick={async () => {
          setResult('')
          const obs = await onObservable()
          const values: string[] = []
          obs.subscribe({
            next(v) {
              values.push(v)
              setResult(JSON.stringify({ values: [...values], done: false }))
            },
            complete() {
              setResult(JSON.stringify({ values, done: true }))
            },
            error(err) {
              setResult(JSON.stringify({ values, error: String(err) }))
            },
          })
        }}
      >
        Observable ticks
      </button>

      {/* Observable: synchronous complete */}
      <button
        id="test-obs-complete"
        onClick={async () => {
          setResult('')
          const obs = await onObservableComplete()
          const values: string[] = []
          obs.subscribe({
            next(v) {
              values.push(v)
            },
            complete() {
              setResult(JSON.stringify({ values, done: true }))
            },
          })
        }}
      >
        Observable sync complete
      </button>

      {/* Observable: error */}
      <button
        id="test-obs-error"
        onClick={async () => {
          setResult('')
          const obs = await onObservableError()
          const values: string[] = []
          obs.subscribe({
            next(v) {
              values.push(v)
            },
            error(err) {
              setResult(JSON.stringify({ values, error: String(err) }))
            },
          })
        }}
      >
        Observable error
      </button>

      {/* Observable: close() cancellation */}
      <button
        id="test-obs-close"
        onClick={async () => {
          setResult('')
          const obs = await onObservable()
          const values: string[] = []
          let completed = false
          let errored = false
          obs.subscribe({
            next(v) {
              values.push(v)
              if (values.length === 2) {
                close(obs)
              }
            },
            complete() {
              completed = true
            },
            error() {
              errored = true
            },
          })
          setTimeout(() => {
            setResult(JSON.stringify({ values, completed, errored, closedAfter: 2 }))
          }, 3000)
        }}
      >
        Observable close() after 2
      </button>

      {/* Subject: bidirectional */}
      <button
        id="test-subject-bidir"
        onClick={async () => {
          setResult('')
          const subject = await onSubject()
          const received: string[] = []
          subject.subscribe((v) => {
            received.push(v)
            setResult(JSON.stringify({ received: [...received] }))
          })
          // Send from client after a short delay
          setTimeout(() => subject.next('client-hello'), 800)
        }}
      >
        Subject bidirectional
      </button>

      {/* Subject: close from client */}
      <button
        id="test-subject-close"
        onClick={async () => {
          setResult('')
          const subject = await onSubject()
          const received: string[] = []
          subject.subscribe((v) => {
            received.push(v)
          })
          setTimeout(async () => {
            try {
              await close(subject)
              setResult(JSON.stringify({ received, closed: true }))
            } catch (err: any) {
              setResult(JSON.stringify({ received, closed: false, error: String(err?.message ?? err) }))
            }
          }, 1200)
        }}
      >
        Subject close()
      </button>

      {/* Subject: echo (server subscribes and echoes back) */}
      <button
        id="test-subject-echo"
        onClick={async () => {
          setResult('')
          const subject = await onSubjectEcho()
          const received: string[] = []
          subject.subscribe((v) => {
            received.push(v)
            setResult(JSON.stringify({ received: [...received] }))
          })
          // Delay: both sides need the MSG.SUBSCRIBE roundtrip to complete
          // before values flow bidirectionally.
          setTimeout(() => subject.next('ping'), 200)
        }}
      >
        Subject echo
      </button>

      {/* Subject: server completes */}
      <button
        id="test-subject-server-complete"
        onClick={async () => {
          setResult('')
          const subject = await onSubjectServerComplete()
          const received: string[] = []
          let completed = false
          subject.subscribe({
            next(v) {
              received.push(v)
            },
            complete() {
              completed = true
              setResult(JSON.stringify({ received, completed }))
            },
          })
          // Also report if not completed after timeout
          setTimeout(() => {
            if (!completed) setResult(JSON.stringify({ received, completed }))
          }, 2000)
        }}
      >
        Subject server complete
      </button>

      {/* Shared subject: multicast */}
      <button
        id="test-shared-subject"
        onClick={async () => {
          setResult('')
          // Two "clients" subscribing to same shared subject
          const s1 = await onSharedSubject()
          const s2 = await onSharedSubject()
          const received1: string[] = []
          const received2: string[] = []
          s1.subscribe((v) => {
            received1.push(v)
            setResult(JSON.stringify({ received1: [...received1], received2: [...received2] }))
          })
          s2.subscribe((v) => {
            received2.push(v)
            setResult(JSON.stringify({ received1: [...received1], received2: [...received2] }))
          })
          // Send from s1 — s2 should receive it too
          setTimeout(() => s1.next('from-s1'), 500)
          // Send from s2 — s1 should receive it too
          setTimeout(() => s2.next('from-s2'), 1000)
        }}
      >
        Shared subject multicast
      </button>

      {/* Observable from client: client → server */}
      <button
        id="test-obs-from-client"
        onClick={async () => {
          setResult('')
          const input$ = new RxObservable<string>((subscriber) => {
            subscriber.next('a')
            subscriber.next('b')
            subscriber.next('c')
            subscriber.complete()
          })
          const serverReceived = await onObservableFromClient(input$)
          setResult(JSON.stringify({ serverReceived }))
        }}
      >
        Observable client→server
      </button>

      {/* Subject: multiple subscriptions (unicast check) */}
      <button
        id="test-subject-multi-sub"
        onClick={async () => {
          setResult('')
          const subject = await onSubjectMultiSubscribe()
          const sub1: number[] = []
          const sub2: number[] = []
          let completed1 = false
          let completed2 = false
          subject.subscribe({
            next(v) {
              sub1.push(v)
            },
            complete() {
              completed1 = true
              if (completed2) setResult(JSON.stringify({ sub1, sub2, completed1, completed2 }))
            },
          })
          subject.subscribe({
            next(v) {
              sub2.push(v)
            },
            complete() {
              completed2 = true
              if (completed1) setResult(JSON.stringify({ sub1, sub2, completed1, completed2 }))
            },
          })
        }}
      >
        Subject multi-subscribe
      </button>

      {/* Observable: server-side error propagates to client */}
      <button
        id="test-obs-server-error"
        onClick={async () => {
          setResult('')
          const obs = await onObservableServerError()
          const values: string[] = []
          obs.subscribe({
            next(v) {
              values.push(v)
            },
            error(err) {
              setResult(JSON.stringify({ values, error: String(err?.message ?? err) }))
            },
          })
        }}
      >
        Observable server-error
      </button>

      {/* Subject: server-side error propagates to client */}
      <button
        id="test-subject-server-error"
        onClick={async () => {
          setResult('')
          const subject = await onSubjectServerError()
          const received: string[] = []
          subject.subscribe({
            next(v) {
              received.push(v)
            },
            error(err) {
              setResult(JSON.stringify({ received, error: String(err?.message ?? err) }))
            },
          })
        }}
      >
        Subject server-error
      </button>

      {/* Subject passed as arg; server has no error handler; client errors its subject */}
      <button
        id="test-subject-arg-no-handler"
        onClick={async () => {
          setResult('')
          const input$ = new RxSubject<string>()
          await onSubjectArgNoErrorHandler(input$)
          // Wait for the wire's MSG.SUBSCRIBE roundtrip, then send a value + error
          setTimeout(() => {
            input$.next('hello')
            setTimeout(() => {
              input$.error(new Error('client-triggered-error'))
            }, 100)
          }, 200)
        }}
      >
        Subject arg no-handler server-survive
      </button>

      {/* Shared subject: client A errors locally, client B keeps receiving */}
      <button
        id="test-shared-error-one"
        onClick={async () => {
          setResult('')
          const a = await onSharedSubjectOneErrors()
          const b = await onSharedSubjectOneErrors()
          const received_a: string[] = []
          const received_b: string[] = []
          let a_errored = false
          let b_errored = false
          a.subscribe({
            next(v) {
              received_a.push(v)
            },
            error() {
              a_errored = true
              render()
            },
          })
          b.subscribe({
            next(v) {
              received_b.push(v)
              render()
            },
            error() {
              b_errored = true
              render()
            },
          })
          const render = () =>
            setResult(
              JSON.stringify({ received_a: [...received_a], received_b: [...received_b], a_errored, b_errored }),
            )

          // Error A — transparent: kills the shared server Subject, B should also error
          setTimeout(() => {
            a.error(new Error('client-A-aborted'))
            // Give time for error to propagate through server to B
            setTimeout(render, 500)
          }, 200)
        }}
      >
        Shared Subject one-aborts
      </button>

      {/* Server subscribes without error handler — server should not crash */}
      <button
        id="test-subject-no-handler"
        onClick={async () => {
          setResult('')
          const subject = await onSubjectNoHandler()
          const received: string[] = []
          subject.subscribe({
            next(v) {
              received.push(v)
            },
            error(err) {
              setResult(JSON.stringify({ received, error: String(err?.message ?? err) }))
            },
          })
          setTimeout(() => subject.next('hello-from-client'), 200)
        }}
      >
        Subject no-handler (server survives)
      </button>

      <pre id="rxjs-result" className="mt-4 p-2 bg-gray-100 text-sm font-mono min-h-[2em]">
        {result}
      </pre>
    </div>
  )
}
