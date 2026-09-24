export { TelefuncStreamBeta }

import React from 'react'
import { Link } from '@brillout/docpress'

function TelefuncStreamBeta({ noLink }: { noLink?: boolean }) {
  return (
    <blockquote>
      <p>
        <strong>Beta</strong> — {noLink ? 'Telefunc Stream' : <Link href="/stream">Telefunc Stream</Link>} is in beta:
        breaking changes may occur in any version update.
      </p>
    </blockquote>
  )
}
