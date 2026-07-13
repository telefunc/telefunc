import { transformTelefuncFileServerSide } from './transformTelefuncFileServerSide.js'
import { expect, describe, it } from 'vitest'

const appRootDir = '/root'
const id = '/root/server/foo.telefunc.js'

async function transform(src: string) {
  const { code } = await transformTelefuncFileServerSide(src, id, appRootDir, true, [])
  return code
}

describe('transformTelefuncFileServerSide', () => {
  it('local exports', async () => {
    const src = ['export function onFoo() {}', 'const sendBar = () => {}', 'export { sendBar }'].join('\n')
    expect(await transform(src)).toMatchInlineSnapshot(`
      "export function onFoo() {}
      const sendBar = () => {}
      export { sendBar }

      import { __decorateTelefunction } from "telefunc";
      __decorateTelefunction(onFoo, "onFoo", "/server/foo.telefunc.js", "/root");
      __decorateTelefunction(sendBar, "sendBar", "/server/foo.telefunc.js", "/root");"
    `)
  })

  it("re-exports — `export { onFoo } from './x.js'` has no local binding to reference => import it", async () => {
    const src = "export { onFoo, sendBar } from '@some/pkg'"
    expect(await transform(src)).toMatchInlineSnapshot(`
      "export { onFoo, sendBar } from '@some/pkg'

      import { __decorateTelefunction } from "telefunc";
      import { onFoo as __telefunc_reExport_onFoo } from "@some/pkg";
      __decorateTelefunction(__telefunc_reExport_onFoo, "onFoo", "/server/foo.telefunc.js", "/root");
      import { sendBar as __telefunc_reExport_sendBar } from "@some/pkg";
      __decorateTelefunction(__telefunc_reExport_sendBar, "sendBar", "/server/foo.telefunc.js", "/root");"
    `)
  })

  it('aliased re-export — import the source-side name', async () => {
    const src = "export { origName as onFoo } from './y.js'"
    expect(await transform(src)).toMatchInlineSnapshot(`
      "export { origName as onFoo } from './y.js'

      import { __decorateTelefunction } from "telefunc";
      import { origName as __telefunc_reExport_onFoo } from "./y.js";
      __decorateTelefunction(__telefunc_reExport_onFoo, "onFoo", "/server/foo.telefunc.js", "/root");"
    `)
  })

  it('mixed local exports and re-exports', async () => {
    const src = ["export { onFoo } from './x.js'", 'export function onBar() {}'].join('\n')
    expect(await transform(src)).toMatchInlineSnapshot(`
      "export { onFoo } from './x.js'
      export function onBar() {}

      import { __decorateTelefunction } from "telefunc";
      import { onFoo as __telefunc_reExport_onFoo } from "./x.js";
      __decorateTelefunction(__telefunc_reExport_onFoo, "onFoo", "/server/foo.telefunc.js", "/root");
      __decorateTelefunction(onBar, "onBar", "/server/foo.telefunc.js", "/root");"
    `)
  })
})
