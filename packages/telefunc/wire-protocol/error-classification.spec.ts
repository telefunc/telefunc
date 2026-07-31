import { expect, it } from 'vitest'
import { isRoomError } from './room/errors.js'
import { classifyTelefuncError } from './error-classification.js'

const brands = Object.fromEntries(
  ['Abort', 'RoomError', 'ShieldValidationError'].map((name) => [name, Symbol.for(`telefunc.${name}`)]),
) as Record<string, symbol>
const branded = (...names: string[]) =>
  Object.assign(
    new Error('branded'),
    Object.fromEntries(names.map((name) => [brands[name], true])),
    names.includes('Abort') ? { abortValue: undefined } : {},
  )

it.each([
  [['Abort', 'RoomError'], 'abort'],
  [['Abort', 'ShieldValidationError'], 'abort'],
  [['RoomError', 'ShieldValidationError'], 'expected'],
  [['Abort', 'RoomError', 'ShieldValidationError'], 'abort'],
] as const)('pins classification precedence for %j', (names, expected) => {
  expect(classifyTelefuncError(branded(...names), isRoomError).kind).toBe(expected)
})

it.each([
  ['Abort', false],
  ['RoomError', 1],
  ['ShieldValidationError', 'true'],
] as const)('rejects a malformed %s brand', (name, value) => {
  expect(classifyTelefuncError(Object.assign(new Error(), { [brands[name]]: value }), isRoomError).kind).toBe('bug')
})
