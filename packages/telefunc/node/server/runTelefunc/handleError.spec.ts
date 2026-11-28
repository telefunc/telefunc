import { expect, it, vi } from 'vitest'
import { handleError } from './handleError.js'

it('reports an error with a cyclic cause once', () => {
  const logged = vi.spyOn(console, 'error').mockImplementation(() => {})
  try {
    const error = new Error('cyclic cause')
    error.cause = error
    handleError(error)

    expect(
      logged.mock.calls
        .flat()
        .join(' ')
        .match(/Error: cyclic cause/g),
    ).toHaveLength(1)
  } finally {
    logged.mockRestore()
  }
})
