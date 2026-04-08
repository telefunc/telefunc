export { RxJSDemo }

import React, { useState, useEffect, useRef } from 'react'
import { onObservable, onSubject, onBehaviorSubject, onReplaySubject, onObservableFromClient } from './RxJS.telefunc'
import { Subject as RxSubject } from 'rxjs'

function RxJSDemo() {
  return (
    <div className="max-w-3xl mx-auto px-8 py-10 space-y-8">
      <h1 className="text-2xl font-bold">RxJS Playground</h1>
      <ObservableDemo />
      <SubjectDemo />
      <BehaviorSubjectDemo />
      <ReplaySubjectDemo />
      <ObservableToServerDemo />
    </div>
  )
}

function ObservableDemo() {
  const [ticks, setTicks] = useState<{ tick: number; time: number }[]>([])
  const [status, setStatus] = useState<string>('idle')

  const start = async () => {
    setStatus('subscribing...')
    setTicks([])
    const ticker$ = await onObservable()
    setStatus('subscribed')
    ticker$.subscribe({
      next: (v) => setTicks((prev) => [...prev, v]),
      complete: () => setStatus('completed'),
      error: (err) => setStatus(`error: ${err}`),
    })
  }

  return (
    <section>
      <h2 className="text-xl font-semibold">Observable (server → client)</h2>
      <p className="text-sm text-gray-600 mb-2">Server emits ticks via interval(1000), client subscribes.</p>
      <button onClick={start} className="px-3 py-1 bg-blue-600 text-white rounded mr-2">
        Subscribe
      </button>
      <span className="text-sm" data-testid="observable-status">
        {status}
      </span>
      <ul className="mt-2 text-sm font-mono" data-testid="observable-ticks">
        {ticks.map((t, i) => (
          <li key={i}>tick #{t.tick}</li>
        ))}
      </ul>
    </section>
  )
}

function SubjectDemo() {
  const [messages, setMessages] = useState<string[]>([])
  const [input, setInput] = useState('')
  const subjectRef = useRef<Awaited<ReturnType<typeof onSubject>> | null>(null)

  const connect = async () => {
    const subject = await onSubject()
    subjectRef.current = subject
    subject.subscribe((v) => setMessages((prev) => [...prev, v]))
  }

  const send = () => {
    if (subjectRef.current && input) {
      subjectRef.current.next(`client-${input}`)
      setInput('')
    }
  }

  return (
    <section>
      <h2 className="text-xl font-semibold">Subject (bidirectional)</h2>
      <p className="text-sm text-gray-600 mb-2">Server pushes every second. Client can push too.</p>
      <button onClick={connect} className="px-3 py-1 bg-blue-600 text-white rounded mr-2">
        Connect
      </button>
      <input
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={(e) => e.key === 'Enter' && send()}
        placeholder="type and enter"
        className="border px-2 py-1 rounded mr-2"
      />
      <button onClick={send} className="px-3 py-1 bg-green-600 text-white rounded">
        Send
      </button>
      <ul className="mt-2 text-sm font-mono max-h-40 overflow-y-auto" data-testid="subject-messages">
        {messages.map((m, i) => (
          <li key={i}>{m}</li>
        ))}
      </ul>
    </section>
  )
}

function BehaviorSubjectDemo() {
  const [theme, setTheme] = useState<string>('(not connected)')
  const subjectRef = useRef<Awaited<ReturnType<typeof onBehaviorSubject>> | null>(null)

  const connect = async () => {
    const bs = await onBehaviorSubject()
    subjectRef.current = bs
    bs.subscribe((v) => setTheme(v))
  }

  const toggle = () => {
    if (subjectRef.current) {
      const current = subjectRef.current.getValue()
      subjectRef.current.next(current === 'light' ? 'dark' : 'light')
    }
  }

  return (
    <section>
      <h2 className="text-xl font-semibold">BehaviorSubject</h2>
      <p className="text-sm text-gray-600 mb-2">
        Initial value &quot;light&quot;. Server switches to &quot;dark&quot; after 3s. Client can toggle.
      </p>
      <button onClick={connect} className="px-3 py-1 bg-blue-600 text-white rounded mr-2">
        Connect
      </button>
      <button onClick={toggle} className="px-3 py-1 bg-purple-600 text-white rounded mr-2">
        Toggle
      </button>
      <span className="text-sm font-mono" data-testid="behavior-theme">
        theme: {theme}
      </span>
    </section>
  )
}

function ReplaySubjectDemo() {
  const [events, setEvents] = useState<string[]>([])
  const [status, setStatus] = useState('idle')

  const connect = async () => {
    setStatus('connecting...')
    setEvents([])
    const log = await onReplaySubject()
    setStatus('connected')
    log.subscribe({
      next: (v) => setEvents((prev) => [...prev, v]),
      complete: () => setStatus('completed'),
    })
  }

  return (
    <section>
      <h2 className="text-xl font-semibold">ReplaySubject</h2>
      <p className="text-sm text-gray-600 mb-2">
        Server pre-fills 3 events. On subscribe, replays buffer + live events every 2s.
      </p>
      <button onClick={connect} className="px-3 py-1 bg-blue-600 text-white rounded mr-2">
        Connect
      </button>
      <span className="text-sm" data-testid="replay-status">
        {status}
      </span>
      <ul className="mt-2 text-sm font-mono max-h-40 overflow-y-auto" data-testid="replay-events">
        {events.map((e, i) => (
          <li key={i}>{e}</li>
        ))}
      </ul>
    </section>
  )
}

function ObservableToServerDemo() {
  const [status, setStatus] = useState('idle')
  const subjectRef = useRef<RxSubject<{ x: number; y: number }> | null>(null)

  const start = async () => {
    const clicks$ = new RxSubject<{ x: number; y: number }>()
    subjectRef.current = clicks$
    await onObservableFromClient(clicks$)
    setStatus('connected — click anywhere')
  }

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      subjectRef.current?.next({ x: e.clientX, y: e.clientY })
    }
    document.addEventListener('click', handler)
    return () => document.removeEventListener('click', handler)
  }, [])

  return (
    <section>
      <h2 className="text-xl font-semibold">Observable (client → server)</h2>
      <p className="text-sm text-gray-600 mb-2">
        Client sends click coordinates to server via Subject passed as Observable argument.
      </p>
      <button onClick={start} className="px-3 py-1 bg-blue-600 text-white rounded mr-2">
        Start
      </button>
      <span className="text-sm" data-testid="obs-to-server-status">
        {status}
      </span>
    </section>
  )
}
