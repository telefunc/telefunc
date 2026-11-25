export { assertSingleInstance }

import { assertWarning } from './assert.js'
import { projectInfo } from './projectInfo.js'
import pc from '@brillout/picocolors'

function assertSingleInstance(globalObject: { versions?: string[] }) {
  // `versions` is missing if a Telefunc version that doesn't record its version (0.3.1 or older) was loaded first
  const versions = (globalObject.versions ??= ['<=0.3.1'])
  const { projectVersion } = projectInfo
  if (!versions.includes(projectVersion)) versions.push(projectVersion)
  const versionOther = versions.find((version) => version !== projectVersion)
  if (versionOther === undefined) return
  assertWarning(
    false,
    `telefunc@${pc.bold(versionOther)} and telefunc@${pc.bold(projectVersion)} are both loaded, which is highly discouraged: use a single version of Telefunc, see ${pc.underline('https://telefunc.com/warning/version-mismatch')}`,
    { onlyOnce: 'version-mismatch' },
  )
}
