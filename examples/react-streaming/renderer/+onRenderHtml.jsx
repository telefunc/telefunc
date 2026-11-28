export { onRenderHtml }

import React from 'react'
import { escapeInject } from 'vike/server'
import { renderToStream } from 'react-streaming/server'
import { PageLayout } from './PageLayout'
import { provideTelefuncContext } from 'telefunc'
import { TelefuncSSR } from 'telefunc/react-streaming/server'

async function onRenderHtml(pageContext) {
  const { Page, pageProps } = pageContext
  const showProfile = process.env.TEST_SSR_CONTEXT === '1'
  const props = showProfile ? { ...pageProps, showProfile } : pageProps
  const page = (
    <PageLayout>
      <Page {...props} />
    </PageLayout>
  )
  const render = (content) => renderToStream(content, { disable: false })
  const content =
    showProfile && pageContext.urlParsed.search.scope === 'provider' ? (
      <TelefuncSSR context={{ name: 'Nora' }}>{page}</TelefuncSSR>
    ) : (
      page
    )
  const stream = showProfile
    ? await provideTelefuncContext({ name: 'Ada' }, () => render(content))
    : await render(content)

  const documentHtml = escapeInject`<!DOCTYPE html>
    <html>
      <body>
        <div id="page-view">${stream}</div>
      </body>
    </html>`
  return { documentHtml, pageContext: { pageProps: props } }
}
