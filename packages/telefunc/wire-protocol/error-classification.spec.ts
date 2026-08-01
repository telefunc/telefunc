import { expect, it } from 'vitest'
import { classifyTelefuncError } from './error-classification.js'

const brands = Object.fromEntries(
  ['Abort', 'Expected', 'ShieldValidationError'].map((name) => [name, Symbol.for(`telefunc.${name}`)]),
) as Record<string, symbol>
const isExpected = (error: unknown): error is Error =>
  error instanceof Error && Object.getOwnPropertyDescriptor(error, brands.Expected!)?.value === true
const branded = (...names: string[]) =>
  Object.assign(
    new Error('branded'),
    Object.fromEntries(names.map((name) => [brands[name], true])),
    names.includes('Abort') ? { abortValue: undefined } : {},
  )

it.each([
  [['Abort', 'Expected'], 'abort'],
  [['Abort', 'ShieldValidationError'], 'abort'],
  [['Expected', 'ShieldValidationError'], 'expected'],
  [['Abort', 'Expected', 'ShieldValidationError'], 'abort'],
] as const)('pins classification precedence for %j', (names, expected) => {
  expect(classifyTelefuncError(branded(...names), isExpected).kind).toBe(expected)
})

it.each([
  ['Abort', false],
  ['Expected', 1],
  ['ShieldValidationError', 'true'],
] as const)('rejects a malformed %s brand', (name, value) => {
  expect(classifyTelefuncError(Object.assign(new Error(), { [brands[name]]: value }), isExpected).kind).toBe('bug')
})
