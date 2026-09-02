import{n as e}from"../chunks/chunk-QTnfLwEv.js";import{N as t,i as n,v as r}from"../chunks/chunk-CINPQgwY.js";import{n as i,t as a}from"../chunks/chunk-B8X3DK4-.js";var o=e({default:()=>u,pageSectionsExport:()=>c}),s=t(),c=[{pageSectionId:`basic-example`,pageSectionLevel:2,pageSectionTitle:`Basic example`},{pageSectionId:`orm-sql`,pageSectionLevel:2,pageSectionTitle:`ORM & SQL`},{pageSectionId:`how-it-works`,pageSectionLevel:2,pageSectionTitle:`How it works`},{pageSectionId:`security`,pageSectionLevel:2,pageSectionTitle:`Security`},{pageSectionId:`throw-abort`,pageSectionLevel:2,pageSectionTitle:"`throw Abort()`"},{pageSectionId:`shield`,pageSectionLevel:2,pageSectionTitle:"`shield()`"},{pageSectionId:`random-telefunction-calls`,pageSectionLevel:2,pageSectionTitle:`Random telefunction calls`},{pageSectionId:`performance`,pageSectionLevel:2,pageSectionTitle:`Performance`}];function l(e){let t={a:`a`,blockquote:`blockquote`,code:`code`,figure:`figure`,li:`li`,ol:`ol`,p:`p`,pre:`pre`,span:`span`,strong:`strong`,ul:`ul`,...n(),...e.components};return(0,s.jsxs)(s.Fragment,{children:[(0,s.jsxs)(t.p,{children:[`Telefunc's approach — seamlessly calling remote functions — is known as `,(0,s.jsxs)(t.a,{href:`https://en.wikipedia.org/wiki/Remote_procedure_call`,children:[`RPC (`,(0,s.jsx)(t.strong,{children:`R`}),`emote `,(0,s.jsx)(t.strong,{children:`P`}),`rocedure `,(0,s.jsx)(t.strong,{children:`C`}),`all)`]}),`.`]}),`
`,(0,s.jsx)(t.p,{children:`This page explains:`}),`
`,(0,s.jsxs)(t.ul,{children:[`
`,(0,s.jsx)(t.li,{children:`What is RPC?`}),`
`,(0,s.jsx)(t.li,{children:`How does RPC work?`}),`
`,(0,s.jsx)(t.li,{children:`How to enforce RPC security?`}),`
`,(0,s.jsx)(t.li,{children:`How to maximize RPC performance?`}),`
`]}),`
`,(0,s.jsx)(`h2`,{id:`basic-example`,children:`Basic example`}),`
`,(0,s.jsx)(t.p,{children:`Telefunc enables functions defined on the server-side to be called remotely from the browser-side.`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// hello.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { hello }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// hello() always runs on the server-side`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` hello`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ `}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`name`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` }) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` message`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'Welcome '`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` +`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` name`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { message }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`html`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`html`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`<!-- index.html -->`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`<!-- Environment: client -->`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`<`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`html`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`body`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`script`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` type`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`"module"`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`      // This import doesn't actually load the hello.telefunc.js file: Telefunc transforms`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`      // hello.telefunc.js into a thin HTTP client.`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`      import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { hello } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` './hello.telefunc.js'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`      // This thin HTTP client makes an HTTP request when the hello() function is called.`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`      const`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:`message`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` hello`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ name: `}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`'Eva'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` })`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`      console.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`log`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(message) `}),(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Prints 'Welcome Eva'`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    </`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`script`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  </`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`body`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`</`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`html`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`The central aspect here is that `,(0,s.jsx)(t.code,{children:`hello()`}),` is always executed on the server side, which enables `,(0,s.jsx)(t.code,{children:`hello()`}),` to use server-side utilities such as SQL and ORMs.`]}),`
`,(0,s.jsx)(`h2`,{id:`orm-sql`,children:`ORM & SQL`}),`
`,(0,s.jsx)(t.p,{children:`As seen in the previous section, telefunctions always run on the server-side and can therefore use server utilities such as SQL and ORMs.`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// TodoList.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { onLoad }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onLoad`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`() {`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // ORM`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` todoItems`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` Task.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`findMany`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ select: `}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`'text'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` })`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // SQL`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` todoItems`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` execute`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`"SELECT text FROM todo_items;"`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`)`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` todoItems`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsxs)(t.p,{children:[(0,s.jsx)(t.code,{children:`.telefunc.js`}),` files are guaranteed to be loaded only on the server-side. You can therefore save secret information, such as the database passwords, in `,(0,s.jsx)(t.code,{children:`.telefunc.js`}),` files.`]}),`
`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`jsx`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`jsx`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// TodoList.jsx`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: client`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// This doesn't actually load CreateTodo.telefunc.js which we'll explain in the next section`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { onLoad } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` './TodoList.telefunc.js'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` TodoList`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`() {`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // The frontend uses the telefunction onLoad() to retrieve data by executing a SQL/ORM query`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` todoItems`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onLoad`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`ul`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`      {todoItems.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`map`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`((`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`item`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=>`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`        <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`li`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>{item.text}</`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`li`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`      ))}`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    </`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`ul`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  )`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsxs)(t.p,{children:[(0,s.jsx)(t.code,{children:`TodoList.telefunc.js`}),` is named and co-located next to `,(0,s.jsx)(t.code,{children:`TodoList.jsx`}),` — a practice explained at `,(0,s.jsx)(r,{href:`/event-based`}),`.`]}),`
`]}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsx)(t.p,{children:`While the examples here use JSX, Telefunc works with any UI framework (React, Vue, Svelte, Solid, ...).`}),`
`]}),`
`,(0,s.jsx)(t.p,{children:`Telefunctions can also be used to mutate data:`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// CreateTodo.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { onNewTodo }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { shield } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// We'll explain shield() later`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`shield`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(onNewTodo, [shield.type.string])`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onNewTodo`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`text`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // ORM`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` todoItem`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` new`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` Task`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ text })`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` todoItem.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`save`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // SQL`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  await`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` execute`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`'INSERT INTO todo_items VALUES (:text)'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`, { text })`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`jsx`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`jsx`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// CreateTodo.jsx`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: client`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { onNewTodo } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` './CreateTodo.telefunc.js'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onClick`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`form`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` text`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` form.input.value`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  await`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onNewTodo`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(text)`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` CreateTodo`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`() {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`form`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`      <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`input`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` input`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`"text"`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`></`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`input`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`      <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`button`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onClick`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`{onClick}>Add To-Do</`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`button`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    </`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`form`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  )`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsx)(t.p,{children:`RPC enables your frontend to tap directly into the full power of the server such as SQL and ORMs. For most use cases it's simpler, more flexible, and more performant than REST and GraphQL.`}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsx)(t.p,{children:`GraphQL and RESTful can be better than RPC if:`}),`
`,(0,s.jsxs)(t.ul,{children:[`
`,(0,s.jsx)(t.li,{children:`you want to give third parties generic access to your data, or`}),`
`,(0,s.jsx)(t.li,{children:`you are a very large company with highly complex databases.`}),`
`]}),`
`,(0,s.jsxs)(t.p,{children:[`See `,(0,s.jsx)(r,{href:`/RPC-vs-GraphQL-REST`}),`.`]}),`
`]}),`
`,(0,s.jsx)(`h2`,{id:`how-it-works`,children:`How it works`}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsx)(t.p,{children:`Understanding the basic mechanics of Telefunc is paramount in order to proficiently use it.`}),`
`]}),`
`,(0,s.jsx)(t.p,{children:`Let's see what happens when a telefunction is called.`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// hello.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { hello }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` hello`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ `}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`name`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` }) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` message`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'Welcome '`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` +`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` name`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { message }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: client`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { hello } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` './hello.telefunc.js'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`const`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:`message`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` hello`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`'Eva'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`)`})]})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`The `,(0,s.jsx)(t.code,{children:`hello.telefunc.js`}),` file is never loaded in the browser: instead Telefunc transforms `,(0,s.jsx)(t.code,{children:`hello.telefunc.js`}),` into the following:`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// hello.telefunc.js (after Telefunc transformation)`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: Browser`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { __remoteTelefunctionCall } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc/client'`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` const`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` hello`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`...`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`args`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=>`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` __remoteTelefunctionCall`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`'/hello.telefunc.js'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`, `}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`'hello'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`, args)`})]})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`When `,(0,s.jsx)(t.code,{children:`hello('Eva')`}),` is called in the browser-side, the following happens:`]}),`
`,(0,s.jsxs)(t.ol,{children:[`
`,(0,s.jsxs)(t.li,{children:[`On the browser-side, the `,(0,s.jsx)(t.code,{children:`__remoteTelefunctionCall()`}),` function makes an HTTP request to the server.`,`
`,(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{children:`POST /_telefunc HTTP/1.1
{
  "path": "/hello.telefunc.js:hello",
  "args": [{"name": "Eva"}]
}
`})}),`
`]}),`
`,(0,s.jsxs)(t.li,{children:[`On the server-side, the Telefunc middleware:`,`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// server.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Server (Hono, Express, Fastify, ...)`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { Telefunc } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc/node'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` telefunc`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` new`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` Telefunc`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Telefunc middleware`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`app.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`all`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`'/_telefunc'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`, `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`c`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=>`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` telefunc.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`serve`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ request: c.req.raw })`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`})`})})]})})}),`
`,`Replies following HTTP response:`,`
`,(0,s.jsx)(t.pre,{children:(0,s.jsx)(t.code,{children:`HTTP/1.1 200 OK
{
  "return": {
    "message": "Welcome Eva"
  }
}
`})}),`
`]}),`
`]}),`
`,(0,s.jsxs)(t.p,{children:[`In other words,
the `,(0,s.jsx)(t.code,{children:`hello()`}),` function is always executed on the server-side
while the browser-side can remotely call it in a seamless fashion.`]}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsx)(t.p,{children:`You can also call telefunctions from the server-side, in which case the telefunction is directly called (without making an HTTP request).`}),`
`]}),`
`,(0,s.jsx)(`h2`,{id:`security`,children:`Security`}),`
`,(0,s.jsx)(t.p,{children:`Anyone — not just your frontend — can remotely call your telefunctions.`}),`
`,(0,s.jsxs)(t.p,{children:[`For example, anyone can call the `,(0,s.jsx)(t.code,{children:`hello()`}),` telefunction we've seen in the previous section by opening a Linux terminal and making this HTTP request:`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`bash`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`bash`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`curl`}),(0,s.jsxs)(t.span,{style:{color:`#032F62`},children:[` `,(0,s.jsx)(t.a,{href:`https://your-website.com/_telefunc`,children:`https://your-website.com/_telefunc`})]}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` --data`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` '{`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`   "path": "/hello.telefunc.js:hello",`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`   "args": [{"name": "Elisabeth"}]`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` }'`})})]})})}),`
`,(0,s.jsx)(t.p,{children:`Thus, such telefunction is problematic:`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// sql.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// run() is public: it can be called by anyone`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { run }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` run`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`sql`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` database.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`execute`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(sql)`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`This `,(0,s.jsx)(t.code,{children:`run()`}),` telefunction essentially exposes the entire database to the world as
anyone can make this HTTP request:`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`bash`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`bash`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`curl`}),(0,s.jsxs)(t.span,{style:{color:`#032F62`},children:[` `,(0,s.jsx)(t.a,{href:`https://your-website.com/_telefunc`,children:`https://your-website.com/_telefunc`})]}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` --data`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` '{`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`    "path": "/run.telefunc.js:run",`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`    "args": ["SELECT login, password FROM users;"]`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#032F62`},children:`  }'`})})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`Always keep in mind that your `,(0,s.jsx)(t.strong,{children:`telefunctions are public`}),` and you must `,(0,s.jsx)(t.strong,{children:`always protect your telefunctions`}),`.`]}),`
`,(0,s.jsxs)(t.p,{children:[`In the sections below `,(0,s.jsx)(r,{href:`#throw-abort`}),` and `,(0,s.jsx)(r,{href:`#shield`}),` we explain how to achieve that.`]}),`
`,(0,s.jsx)(`h2`,{id:`throw-abort`,children:(0,s.jsx)(`code`,{children:`throw Abort()`})}),`
`,(0,s.jsx)(t.p,{children:`As explained in the previous section above,
the following telefunction isn't safe.`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// run.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// run() is public: it can be called by anyone`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { run }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` run`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`sql`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` database.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`execute`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(sql)`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`But you can use `,(0,s.jsx)(t.code,{children:`throw Abort()`}),` to protect it:`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// run.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// run() is public: it can be called by anyone`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { run }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { Abort, getContext } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` run`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`sql`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:`user`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` getContext`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // Only admins are allowed to run this telefunction`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  if`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (user.isAdmin `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`!==`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` true`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`throw`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` Abort`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` database.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`execute`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(sql)`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsxs)(t.p,{children:[`Telefunctions can access contextual information by using `,(0,s.jsx)(r,{href:`/getContext`,children:(0,s.jsx)(t.code,{children:`getContext()`})}),`.`]}),`
`]}),`
`,(0,s.jsxs)(t.p,{children:[`You can use `,(0,s.jsx)(t.code,{children:`throw Abort()`}),` to avoid any forbidden telefunction call.`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// TodoList.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { onLoad }`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { Abort, getContext } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onLoad`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`() {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:`user`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` getContext`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // Forbid onLoad() to be called by a user that isn't logged-in`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  if`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`!`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`user) `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`throw`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` Abort`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` todoList`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` Task.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`findMany`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ authorId: user.id })`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` todoList`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsxs)(t.p,{children:[`Use `,(0,s.jsx)(t.code,{children:`throw Abort()`}),` to implement permission: only a logged-in user is allowed to fetch its to-do items.
More about permissions at `,(0,s.jsx)(r,{href:`/permissions`}),`.`]}),`
`]}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsxs)(t.p,{children:[`Don't use `,(0,s.jsx)(t.code,{children:`throw new Error()`}),` instead of `,(0,s.jsx)(t.code,{children:`throw Abort()`}),`. While `,(0,s.jsx)(t.code,{children:`throw new Error()`}),` also interrupts the telefunction call, Telefunc interprets it as a bug, see `,(0,s.jsx)(r,{href:`/abort-vs-error`}),`.`]}),`
`]}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsxs)(t.p,{children:[`If, upon aborting a telefunction call, you want to pass information to the frontend then use `,(0,s.jsx)(t.code,{children:`return someValue`}),` or `,(0,s.jsx)(t.code,{children:`throw Abort(someValue)`}),`, see `,(0,s.jsx)(r,{href:`/permissions`}),`.`]}),`
`]}),`
`,(0,s.jsx)(`h2`,{id:`shield`,children:(0,s.jsx)(`code`,{children:`shield()`})}),`
`,(0,s.jsxs)(t.p,{children:[`Since telefunctions are public and can be called by anyone, you cannot assume anything about arguments. You can use `,(0,s.jsx)(t.code,{children:`throw Abort()`}),` to ensure the type of telefunction arguments:`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// CreateTodo.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onNewTodo`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`text`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // ❌ This may throw:`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:"  // ```"})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // Uncaught TypeError: Cannot read properties of undefined (reading 'toUpperCase')`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:"  // ```"})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:"  // While the frontend may always call onNewTodo(text) with `typeof text === 'string'`,"})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // because onNewTodo() is public, anyone can call onNewTodo(undefined) instead.`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  text `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` text.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`toUpperCase`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:"  // ✅ Ensures `text` is a string"})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  if`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`typeof`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` text `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`!==`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'string'`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`    throw`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` Abort`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  }`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  text `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` text.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`toUpperCase`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`For more convenience, you can use `,(0,s.jsx)(t.code,{children:`shield()`}),` instead:`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// CreateTodo.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { shield } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` t`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` shield.type`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`shield`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(onNewTodo, [t.string])`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onNewTodo`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`text`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // ...`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// CreateTodo.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { shield } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` t`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` shield.type`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`shield`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(onNewTodo, [{ text: t.string, isCompleted: t.boolean }])`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onNewTodo`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ `}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`text`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`, `}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`isCompleted`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` }) {`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // ...`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`Not only does `,(0,s.jsx)(t.code,{children:`shield()`}),` call `,(0,s.jsx)(t.code,{children:`throw Abort()`}),` on your behalf, but it also infers the type of the arguments for TypeScript and IntelliSense.`]}),`
`,(0,s.jsxs)(r,{href:`/shield#typescript-automatic`,children:[`When using TypeScript, Telefunc can automatically generate `,(0,s.jsx)(t.code,{children:`shield()`}),` for each telefunction.`]}),`
`,(0,s.jsx)(`h2`,{id:`random-telefunction-calls`,children:`Random telefunction calls`}),`
`,(0,s.jsx)(t.p,{children:`Any of your telefunctions can be called by anyone, at any time, and with any arguments. One way to think about it is that any random telefunction call can happen at any time.`}),`
`,(0,s.jsx)(t.p,{children:`You should always protect your telefunctions, even when your frontend calls a telefunction only in a certain way. For example:`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`jsx`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`jsx`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Comment.jsx`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: client`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { onDelete } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` './Comment.telefunc.js'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` Comment`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ `}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`id`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`, `}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`text`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` }) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` deleteButton`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` =`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`    // The delete button is only shown to admins`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`    !`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`user.isAdmin `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`?`}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:` null`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` :`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`button`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onClick`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`{() `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=>`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onDelete`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(id)}>Delete</`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`button`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    <>`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`      <`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`p`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>{text}</`}),(0,s.jsx)(t.span,{style:{color:`#22863A`},children:`p`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`>`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`      {deleteButton}`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`    </>`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`  )`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.p,{children:[`Because the frontend shows the delete button only to admins, you can assume the user to be an admin whenever the frontend calls `,(0,s.jsx)(t.code,{children:`onDelete()`}),`.
But you still need to use `,(0,s.jsx)(t.code,{children:`throw Abort()`}),` in order to protect the telefunction against calls that don't originate from the frontend.`]}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Comment.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { getContext, Abort, shield } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`shield`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(onDelete, [shield.type.number])`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onDelete`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`id`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:`user`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` getContext`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // onDelete() is public and anyone can call it without being an admin.`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // Abort if that happens.`})}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  if`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` (`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`!`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`user.isAdmin) `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`throw`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` Abort`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // ...`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsx)(`h2`,{id:`performance`,children:`Performance`}),`
`,(0,s.jsx)(t.p,{children:`When used correctly, RPC is highly efficient.`}),`
`,(0,s.jsx)(t.p,{children:`Since telefunctions are tailored to your frontend, you can make them send only the strict minimum amount of data.`}),`
`,(0,s.jsx)(t.p,{children:`For example, the following doesn't send a single byte of superfluous information:`}),`
`,(0,s.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,s.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,s.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// TodoList.telefunc.js`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { getContext } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,s.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`shield`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(onTodoComplete, [shield.type.number])`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` async`}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` onTodoComplete`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`(`}),(0,s.jsx)(t.span,{style:{color:`#E36209`},children:`id`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`) {`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:`user`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:` getContext`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,s.jsxs)(t.span,{"data-line":``,children:[(0,s.jsx)(t.span,{style:{color:`#D73A49`},children:`  await`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` Task.`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`update`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ completed: `}),(0,s.jsx)(t.span,{style:{color:`#005CC5`},children:`true`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:` }).`}),(0,s.jsx)(t.span,{style:{color:`#6F42C1`},children:`where`}),(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`({ id, author: user.id })`})]}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#6A737D`},children:`  // No data is returned: the HTTP response status code 200 is enough information for the client`})}),`
`,(0,s.jsx)(t.span,{"data-line":``,children:(0,s.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsx)(t.p,{children:`With REST, because API endpoints are generic you often end up returning superfluous data.`}),`
`]}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsx)(t.p,{children:`Even with GraphQL, the client must specify and send the list of fields that the server needs to return. This isn't needed with RPC — that information is implicitly defined by the telefunction name (i.e. the information is compressed).`}),`
`]}),`
`,(0,s.jsxs)(t.p,{children:[`For efficient telefunctions, we recommend creating tailored telefunctions instead of generic ones — see `,(0,s.jsx)(r,{href:`/event-based`}),`.`]}),`
`,(0,s.jsxs)(t.blockquote,{children:[`
`,(0,s.jsx)(t.p,{children:`Telefunc newcomers often make the mistake of creating generic telefunctions — a habit that comes from using REST or GraphQL.`}),`
`]})]})}function u(e={}){let{wrapper:t}={...n(),...e.components};return t?(0,s.jsx)(t,{...e,children:(0,s.jsx)(l,{...e})}):l(e)}var d={hasServerOnlyHook:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:!1}},isClientRuntimeLoaded:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:!0}},onBeforeRenderEnv:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:null}},dataEnv:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:null}},guardEnv:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:null}},onRenderClient:{type:`standard`,definedAtData:{filePathToShowToUser:`@brillout/docpress/renderer/onRenderClient`,fileExportPathToShowToUser:[]},valueSerialized:{type:`pointer-import`,value:i}},onCreatePageContext:{type:`cumulative`,definedAtData:[{filePathToShowToUser:`@brillout/docpress/renderer/onCreatePageContext`,fileExportPathToShowToUser:[]}],valueSerialized:[{type:`pointer-import`,value:a}]},Page:{type:`standard`,definedAtData:{filePathToShowToUser:`/pages/RPC/+Page.mdx`,fileExportPathToShowToUser:[]},valueSerialized:{type:`plus-file`,exportValues:o}},hydrationCanBeAborted:{type:`standard`,definedAtData:{filePathToShowToUser:`@brillout/docpress/config`,fileExportPathToShowToUser:[`default`,`hydrationCanBeAborted`]},valueSerialized:{type:`js-serialized`,value:!0}}};export{d as configValuesSerialized};