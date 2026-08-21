import { getExportList } from './getExportList.js'
import { expect, describe, it } from 'vitest'

const filePath = '/root/server/foo.telefunc.js'

describe('getExportList()', () => {
  it('returns the export list', async () => {
    const src = [
      "import { db } from './db.js'",
      'export function onFoo() {}',
      'const sendBar = () => {}',
      'export { sendBar }',
      'export const onBaz = async () => db.query()',
    ].join('\n')
    expect(await getExportList(src, filePath)).toEqual([
      { exportName: 'onFoo', localName: 'onFoo' },
      { exportName: 'sendBar', localName: 'sendBar' },
      { exportName: 'onBaz', localName: 'onBaz' },
    ])
  })

  it("re-exports are forbidden — there's no local binding the generated code could reference", async () => {
    await expect(getExportList("export { onFoo } from './x.js'", filePath)).rejects.toThrowError(
      "The telefunc file /root/server/foo.telefunc.js contains `export { onFoo } from './x.js'` which isn't supported",
    )
  })

  it('all re-export forms are caught', async () => {
    const srcs = [
      "export { origName as onFoo } from './x.js'",
      "export * from './x.js'",
      "export * as ns from './x.js'",
      "export{onFoo}from'./x.js'",
    ]
    for (const src of srcs) {
      await expect(getExportList(src, filePath), src).rejects.toThrowError("which isn't supported")
    }
  })

  it("type-only re-exports are fine — they're erased at compile time", async () => {
    const src = ["export type { SomeType } from './x.js'", 'export function onFoo() {}'].join('\n')
    expect(await getExportList(src, filePath)).toEqual([{ exportName: 'onFoo', localName: 'onFoo' }])
  })
})
