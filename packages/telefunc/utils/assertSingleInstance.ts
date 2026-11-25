export { assertSingleInstance }

import { assertWarning } from './assert.js'
import { projectInfo } from './projectInfo.js'
import pc from '@brillout/picocolors'

function assertSingleInstance(globalObject: { version?: string }) {
  // `version` is missing if a Telefunc version that doesn't record its version (0.3.1 or older) was loaded first
  const versionFirst = globalObject.version ?? '<=0.3.1'
  const { projectVersion } = projectInfo
  if (versionFirst === projectVersion) return
  assertWarning(
    false,
    `telefunc@${pc.bold(versionFirst)} and telefunc@${pc.bold(projectVersion)} are both loaded, which is highly discouraged: use a single version of Telefunc, see ${pc.underline('https://telefunc.com/warning/version-mismatch')}`,
    { onlyOnce: 'version-mismatch' },
  )
}
