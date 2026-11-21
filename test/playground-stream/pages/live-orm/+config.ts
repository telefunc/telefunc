import vikeReactQuery from 'vike-react-query/config'
import type { Config } from 'vike/types'

export default {
  extends: [vikeReactQuery],
  // The playground's server answers with a Web Response.
  stream: 'web',
} satisfies Config
