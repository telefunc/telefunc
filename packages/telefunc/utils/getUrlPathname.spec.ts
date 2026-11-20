import { expect, test } from 'vitest'
import { readRequestUrl } from './getUrlPathname.js'

test.each([
  ['names credentials', 'http://user:secret@example.com/_telefunc'],
  ['names a user alone', '//user@example.com/_telefunc'],
  ['is no URL, even against a base', '//'],
])("a request's URL that %s is read as none", (_, url) => {
  expect(readRequestUrl(url)).toBe(null)
})

test("a request's URL that is a path, or a whole URL, is read against no host of its own", () => {
  expect(readRequestUrl('/_telefunc?wire=1')?.pathname).toBe('/_telefunc')
  expect(readRequestUrl('http://other.example/_telefunc')?.pathname).toBe('/_telefunc')
})
