export { onObservable, onSubject, onBehaviorSubject, onReplaySubject, onObservableFromClient }

import { Observable, Subject, BehaviorSubject, ReplaySubject, interval } from 'rxjs'
import { map, take } from 'rxjs/operators'

async function onObservable() {
  const ticker$ = interval(1000).pipe(
    map((i) => ({ tick: i + 1, time: Date.now() })),
    take(10),
  )
  return ticker$
}

async function onSubject() {
  const subject = new Subject<string>()

  // Server pushes a value every second
  let count = 0
  const id = setInterval(() => {
    subject.next(`server-${++count}`)
  }, 1000)

  // Server also subscribes to see client pushes
  subject.subscribe((v) => console.log('[server] subject received:', v))

  // Clean up on complete
  subject.subscribe({ complete: () => clearInterval(id) })

  return subject
}

async function onBehaviorSubject() {
  const theme = new BehaviorSubject<string>('light')

  // Simulate server-side theme change after 3 seconds
  setTimeout(() => theme.next('dark'), 3000)

  return theme
}

async function onReplaySubject() {
  const log = new ReplaySubject<string>(5)

  // Pre-fill some history
  log.next('event-1')
  log.next('event-2')
  log.next('event-3')

  // Keep adding events
  let count = 3
  const id = setInterval(() => {
    log.next(`event-${++count}`)
  }, 2000)

  log.subscribe({ complete: () => clearInterval(id) })

  return log
}

async function onObservableFromClient(clicks$: Observable<{ x: number; y: number }>) {
  clicks$.subscribe((click) => console.log('[server] click received:', click))
}
