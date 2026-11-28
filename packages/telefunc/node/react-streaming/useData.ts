export { useData }

import { useData as useData_ } from '../../shared/react-streaming/index.js'
import { restoreContext, isAsyncMode } from '../server/context/context.js'
import { PROVIDED_CONTEXT } from '../server/context/getContext.js'

import { useTelefuncContext } from './useTelefuncContext.js'

const useData: typeof useData_ = (...args) => {
  const context = useTelefuncContext()
  // Without `<TelefuncSSR context>`, async mode reads what `provideTelefuncContext()` gave the render.
  if (context === null && isAsyncMode()) return useData_(...args)
  return restoreContext({ [PROVIDED_CONTEXT]: context }, () => useData_(...args))
}
