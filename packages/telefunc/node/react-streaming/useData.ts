export { useData }

import { useData as useData_ } from '../../shared/react-streaming/index.js'
import { restoreContext, isAsyncMode } from '../server/context/context.js'
import { PROVIDED_CONTEXT } from '../server/context/getContext.js'

import { useTelefuncContext } from './useTelefuncContext.js'

const useData: typeof useData_ = (...args) => {
  if (!isAsyncMode()) {
    const context = useTelefuncContext()
    return restoreContext({ [PROVIDED_CONTEXT]: context }, () => useData_(...args))
  }
  return useData_(...args)
}
