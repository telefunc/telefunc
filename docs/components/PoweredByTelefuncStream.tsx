export { PoweredByTelefuncStream }

import React from 'react'
import { Link } from '@brillout/docpress'

function PoweredByTelefuncStream({ pkg }: { pkg: string }) {
  return (
    <blockquote>
      <p>
        The <code>@telefunc/{pkg}</code> integration is powered by <Link href="/stream">Telefunc Stream</Link>.
      </p>
    </blockquote>
  )
}
