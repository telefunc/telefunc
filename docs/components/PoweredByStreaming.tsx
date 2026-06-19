export { PoweredByStreaming }

import React from 'react'
import { Link } from '@brillout/docpress'

function PoweredByStreaming({ pkg }: { pkg: string }) {
  return (
    <blockquote>
      <p>
        The <code>@telefunc/{pkg}</code> integration is powered by{' '}
        <Link href="/stream">Telefunc's streaming primitives</Link>.
      </p>
    </blockquote>
  )
}
