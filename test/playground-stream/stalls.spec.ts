import { readFileSync } from 'node:fs'
import { expect, test } from 'vitest'
import { isVoid, stoppedFor, type Stall, type Stop } from './stalls'

const SLACK_MS = 100

/** A stall of `who` from `startedAt` for `ms`. */
function stall(who: Stall['who'], startedAt: number, ms: number): Stall {
  return { who, endedAt: startedAt + ms, ms }
}

/** A stop of the sleeper from `startedAt` for `ms`. */
function stop(startedAt: number, ms: number): Stop {
  return { endedAt: startedAt + ms, ms }
}

test('a run whose stalls the sleeper stopped across, as the machine stopped them all, is void', () => {
  // A run of the lossy card in which the host stopped the machine again and again; its stops are stall-witness's, a
  // tick added, as it reads a stop's lateness and the sleeper its whole wake.
  const replayed: { slackMs: number; stalls: Stall[]; stops: Stop[] } = JSON.parse(
    readFileSync(new URL('./stalls.spec.replay.json', import.meta.url), 'utf8'),
  )
  expect(isVoid(replayed.slackMs, replayed.stalls, replayed.stops)).toBe(true)
})

test('a stop of the page and the server together that the sleeper ran through is not void', () => {
  expect(isVoid(SLACK_MS, [stall('page', 1000, 300), stall('server', 1002, 298)], [])).toBe(false)
})

test('a server that computes while the page and the sleeper run is not void', () => {
  expect(isVoid(SLACK_MS, [stall('server', 1000, 300)], [])).toBe(false)
})

test("a stall the sleeper's stops cover only in part, by less than the stall less the slack, is not void", () => {
  expect(isVoid(SLACK_MS, [stall('server', 1000, 500)], [stop(1000, 300)])).toBe(false)
})

test("a stall the sleeper's stops cover all of but less than the slack is void", () => {
  expect(isVoid(SLACK_MS, [stall('page', 1000, 500)], [stop(1000, 250), stop(1290, 200)])).toBe(true)
})

test('a failed run with no stall of the slack is not void', () => {
  expect(isVoid(SLACK_MS, [stall('page', 1000, 60)], [stop(1000, 60)])).toBe(false)
})

test('a run the machine stopped for the slack could have been passed by the stop', () => {
  expect(stoppedFor(SLACK_MS, [stop(1000, 120)])).toBe(true)
  expect(stoppedFor(SLACK_MS, [stop(1000, 90)])).toBe(false)
})
