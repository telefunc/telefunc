import { toPosixPath } from '../../utils/path.js'
import { transformTelefuncFileClientSide } from '../shared/transformer/transformTelefuncFileClientSide.js'
import { transformTelefuncFileServerSide } from '../shared/transformer/transformTelefuncFileServerSide.js'
import { getExtensionImports } from '../shared/discoverExtensions.js'
import type { Loader } from './types.js'
import { getInfo } from './getInfo.js'

// Compute extension imports once per side, lazily on first use, then reuse.
let serverExtensionImports: string[] | undefined
let clientExtensionImports: string[] | undefined

export default async function (this: Loader, input: string): Promise<void> {
  this.async()
  const { id, root, isClientSide, isDev } = getInfo(this)
  const posixRoot = toPosixPath(root)

  if (isClientSide) {
    clientExtensionImports ??= getExtensionImports(posixRoot, 'client')
    const { code, map } = await transformTelefuncFileClientSide(
      input,
      toPosixPath(id),
      posixRoot,
      clientExtensionImports,
    )
    this.callback(null, code, map)
  } else {
    serverExtensionImports ??= getExtensionImports(posixRoot, 'server')
    const res = await transformTelefuncFileServerSide(input, toPosixPath(id), posixRoot, isDev, serverExtensionImports)
    if (!res) return
    this.callback(null, res.code, res.map)
  }
}
