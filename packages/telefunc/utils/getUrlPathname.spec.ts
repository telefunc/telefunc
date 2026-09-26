import { expect, test } from 'vitest'
import { getRequestPathname } from './getUrlPathname.js'

test("reads a raw request path's pathname, and none from a path that isn't a URL", () => {
  expect(getRequestPathname('/_telefunc?_telefunc=txt&session=s')).toBe('/_telefunc')
  for (const path of ['//', '//?x', '//a:b']) expect(getRequestPathname(path)).toBeNull()
})
