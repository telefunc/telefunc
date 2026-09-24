export { EventBasedRecommendation }

import React from 'react'
import { Link } from '@brillout/docpress'

function EventBasedRecommendation({ samePage }: { samePage?: boolean }) {
  return (
    <>
      <blockquote>
        <p>
          Opting out of the naming convention is perfectly fine, though we recommend having a clear reason for doing so.
        </p>
        <p>
          We recommend reading{' '}
          {samePage ? <Link href="#example">the example above</Link> : <Link href="/best-practices" />} before opting
          out. It explains why event-based telefunctions lead to increased:
        </p>
        <ul>
          <li>Development speed </li>
          <li>Security </li>
          <li>Performance </li>
        </ul>
        <p>
          <a href="https://github.com/telefunc/telefunc/issues/new">Feel free to reach out</a> if you have questions.
        </p>
      </blockquote>
    </>
  )
}
