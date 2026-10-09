import React from 'react'
import './CodeComparison.css'

export { CodeComparison }

function CodeComparison({
  captions,
  headings,
  children,
}: {
  captions?: [string, string]
  headings?: [string, string]
  children: [React.ReactNode, React.ReactNode]
}) {
  return (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        flexDirection: 'column',
      }}
    >
      <div
        id="code-preview-wrapper"
        className={[
          /*
          'debug-layout',
          //*/
        ].join(' ')}
      >
        <div className="code-preview_code-block">
          <Caption>{headings?.[0]}</Caption>
          {children[0]}
          <Caption>{captions?.[0]}</Caption>
        </div>
        <div className="code-preview_code-block code-preview_code-block_right">
          <Caption>{headings?.[1]}</Caption>
          {children[1]}
          <Caption>{captions?.[1]}</Caption>
        </div>
      </div>
    </div>
  )
}

function Caption({ children }: { children: string | undefined }) {
  if (!children) {
    return null
  }

  return (
    <div className="code-preview_code-block_description">
      <em>{children}</em>
    </div>
  )
}
