export { assertIsNotBrowser }

import { assert } from './assert.js'

/** Ensure we don't bloat the client-side */
function assertIsNotBrowser() {
  // Using `typeof window !== 'undefined'` alone is not enough because some users use https://www.npmjs.com/package/ssr-window
  assert(!(typeof window !== 'undefined' && typeof window.scrollY === 'number'))
}
