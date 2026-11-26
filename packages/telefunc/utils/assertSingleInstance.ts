export { assertSingleInstance }

import { assertWarning } from './assert.js'
import { projectInfo } from './projectInfo.js'
import pc from '@brillout/picocolors'

function assertSingleInstance(globalObject: { version?: string }) {
  const { projectVersion } = projectInfo
  if (globalObject.version === projectVersion) return
  // `version` is missing if the Telefunc version loaded first is too old to record its version
  const versionFirst = globalObject.version
    ? `telefunc@${pc.bold(globalObject.version)}`
    : 'an older version of Telefunc'
  assertWarning(
    false,
    `${versionFirst} and telefunc@${pc.bold(projectVersion)} are both loaded, which is highly discouraged: use a single version of Telefunc, see ${pc.underline('https://telefunc.com/warning/version-mismatch')}`,
    { onlyOnce: 'version-mismatch' },
  )
}
