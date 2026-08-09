export { onLoad }

import { assertIsNotBrowser } from '../../utils/assertIsNotBrowser.js'
import { version } from 'vite'
import { assertViteVersion } from '../../utils/assertViteVersion.js'

function onLoad() {
  assertIsNotBrowser()
  assertViteVersion(version)
}
