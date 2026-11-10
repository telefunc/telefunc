export { Page }

import React from 'react'
import { usePageContext } from 'vike-react/usePageContext'
import { LiveOrm } from './LiveOrm'
import type { Orm } from './databases'

function Page() {
  return <LiveOrm orm={usePageContext().routeParams.orm as Orm} />
}
