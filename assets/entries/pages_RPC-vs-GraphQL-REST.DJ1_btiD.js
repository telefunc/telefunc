import{n as e}from"../chunks/chunk-QTnfLwEv.js";import{N as t,h as n,i as r,m as i,v as a}from"../chunks/chunk-CINPQgwY.js";import{n as o,t as s}from"../chunks/chunk-B8X3DK4-.js";var c=e({default:()=>f,pageSectionsExport:()=>u}),l=t(),u=[{pageSectionId:`third-parties`,pageSectionLevel:2,pageSectionTitle:`Third Parties`},{pageSectionId:`decoupled-frontend-backend`,pageSectionLevel:2,pageSectionTitle:`Decoupled frontend-backend`},{pageSectionId:`telefunc-server`,pageSectionLevel:2,pageSectionTitle:`Telefunc Server`},{pageSectionId:`multiple-frontends`,pageSectionLevel:2,pageSectionTitle:`Multiple Frontends`},{pageSectionId:`complex-databases`,pageSectionLevel:2,pageSectionTitle:`Complex Databases`},{pageSectionId:`which-one-to-choose`,pageSectionLevel:2,pageSectionTitle:`Which one to choose?`}];function d(e){let t={blockquote:`blockquote`,code:`code`,em:`em`,figure:`figure`,li:`li`,p:`p`,pre:`pre`,span:`span`,ul:`ul`,...r(),...e.components};return(0,l.jsxs)(l.Fragment,{children:[(0,l.jsxs)(t.blockquote,{children:[`
`,(0,l.jsxs)(t.p,{children:[`See `,(0,l.jsx)(a,{href:`/RPC`}),` if you don't know what RPC is.`]}),`
`]}),`
`,(0,l.jsxs)(n,{children:[(0,l.jsx)(`b`,{children:`TL;DR`}),(0,l.jsx)(t.p,{children:`A GraphQL/RESTful API is only needed if:`}),(0,l.jsxs)(t.ul,{children:[`
`,(0,l.jsx)(t.li,{children:`You want to give third parties access to your database.`}),`
`,(0,l.jsx)(t.li,{children:`You are a very large company with very complex databases.`}),`
`]}),(0,l.jsx)(t.p,{children:`Otherwise, you can use RPC for a significant increase in development speed.`}),(0,l.jsx)(t.p,{children:`Contrary to common belief, you can use RPC while having
a decoupled frontend-backend development.`})]}),`
`,(0,l.jsx)(`h2`,{id:`third-parties`,children:`Third Parties`}),`
`,(0,l.jsx)(t.p,{children:`The most eminent use case for REST and GraphQL is giving
third parties access to your database.`}),`
`,(0,l.jsx)(t.p,{children:`For example, Facebook's API is used by ~200k third parties.
It makes sense that Facebook uses (and invented) GraphQL,
as GraphQL enables any third-party developer to extensively access Facebook's database,
thus enabling all kinds of applications to be built on top of Facebook's data.`}),`
`,(0,l.jsx)(t.p,{children:`GraphQL/RESTful APIs are generic:
they are meant to serve frontends without knowing the frontends' data query/mutation requirements.`}),`
`,(0,l.jsxs)(t.p,{children:[`In contrast,
as discussed in `,(0,l.jsx)(a,{href:`/event-based`}),`,
telefunctions (RPC endpoints) are tailored to your frontend's UI components.
This means that your telefunctions are only useful for your frontend and
third parties cannot use them to build third-party frontends.`]}),`
`,(0,l.jsx)(i,{icon:(0,l.jsx)(t.span,{style:{fontSize:`1.1em`},children:`⚗️`}),children:(0,l.jsxs)(t.p,{children:[(0,l.jsx)(`b`,{children:`Research Area`}),`.
It's theoretically possible to also use RPC to give third parties access to your database,
but this hasn't been done so far.
Reach out to the Telefunc maintainers if you're interested in exploring this topic.`]})}),`
`,(0,l.jsx)(`h2`,{id:`decoupled-frontend-backend`,children:`Decoupled frontend-backend`}),`
`,(0,l.jsx)(t.p,{children:`A common misbelief is that GraphQL/REST is required to decouple the frontend development from the backend development.`}),`
`,(0,l.jsx)(t.p,{children:`It is true that GraphQL/REST induces a decoupling:
as seen in the last section,
a GraphQL/RESTful API is generic which
means that the frontend team can develop independently of the backend team.`}),`
`,(0,l.jsxs)(t.p,{children:[`But you can as well achieve a decoupled frontend-backend development with RPC by using what we call a `,(0,l.jsx)(t.em,{children:`Telefunc Server`}),`.`]}),`
`,(0,l.jsx)(`h2`,{id:`telefunc-server`,children:`Telefunc Server`}),`
`,(0,l.jsx)(t.p,{children:`You usually install Telefunc as a Node.js server middleware.
This induces a tight coupling between your frontend and your Node.js server.`}),`
`,(0,l.jsx)(t.p,{children:`For example, let's consider this telefunction:`}),`
`,(0,l.jsx)(t.figure,{"data-rehype-pretty-code-figure":``,children:(0,l.jsx)(t.pre,{tabIndex:`0`,"data-language":`js`,"data-theme":`github-light`,children:(0,l.jsxs)(t.code,{"data-language":`js`,"data-theme":`github-light`,style:{display:`grid`},children:[(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#6A737D`},children:`// TodoList.telefunc.js`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#6A737D`},children:`// Environment: server`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`import`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` { getContext } `}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`from`}),(0,l.jsx)(t.span,{style:{color:`#032F62`},children:` 'telefunc'`})]}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#6A737D`},children:`// This telefunction is tightly coupled to the frontend: it returns exactly and`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#6A737D`},children:`// only what the <TodoList /> component needs.`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:` `}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`export`}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:` async`}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:` function`}),(0,l.jsx)(t.span,{style:{color:`#6F42C1`},children:` getInitialData`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`() {`})]}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,l.jsx)(t.span,{style:{color:`#005CC5`},children:`user`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,l.jsx)(t.span,{style:{color:`#6F42C1`},children:` getContext`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`()`})]}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`  if`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` (`}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`!`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`user) {`})]}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`    return`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` { isNotLoggedIn: `}),(0,l.jsx)(t.span,{style:{color:`#005CC5`},children:`true`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` }`})]}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`  }`})}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,l.jsx)(t.span,{style:{color:`#005CC5`},children:` todoItems`}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:` =`}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:` await`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` database.`}),(0,l.jsx)(t.span,{style:{color:`#6F42C1`},children:`query`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`(`})]}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#032F62`},children:`    'SELECT id, text FROM todos WHERE authorId = :authorId'`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`,`})]}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`    { authorId: user.id }`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`  )`})}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`  const`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` { `}),(0,l.jsx)(t.span,{style:{color:`#005CC5`},children:`firstName`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` } `}),(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`=`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` user`})]}),`
`,(0,l.jsxs)(t.span,{"data-line":``,children:[(0,l.jsx)(t.span,{style:{color:`#D73A49`},children:`  return`}),(0,l.jsx)(t.span,{style:{color:`#24292E`},children:` {`})]}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`    user: { firstName },`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`    todoItems`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`  }`})}),`
`,(0,l.jsx)(t.span,{"data-line":``,children:(0,l.jsx)(t.span,{style:{color:`#24292E`},children:`}`})})]})})}),`
`,(0,l.jsxs)(t.p,{children:[`If you change your `,(0,l.jsx)(t.code,{children:`<TodoList>`}),` component to also show the to-do items' creation date,
then you need to change the SQL query of your `,(0,l.jsx)(t.code,{children:`getInitialData()`}),` telefunction from `,(0,l.jsx)(t.code,{children:`SELECT id, text`}),` to `,(0,l.jsx)(t.code,{children:`SELECT id, text, created_at`}),`.`]}),`
`,(0,l.jsx)(t.p,{children:`This means that the frontend developers need to make changes to the Node.js server and re-deploy it.`}),`
`,(0,l.jsx)(t.p,{children:`If you are a small team of full-stack developers, then such frontend-backend coupling is not a problem.
But, as you grow, you may want to have a frontend team that develops independently of a backend team.`}),`
`,(0,l.jsx)(t.p,{children:`You can achieve a decoupling by using a Telefunc Server: a dedicated Node.js server with the sole purpose of serving telefunctions.`}),`
`,(0,l.jsx)(t.p,{children:`The frontend and the Telefunc server are developed & deployed hand-in-hand,
while the backend (another Node.js server, Ruby on Rails, ...) can be developed & deployed independently.`}),`
`,(0,l.jsx)(`h2`,{id:`multiple-frontends`,children:`Multiple Frontends`}),`
`,(0,l.jsx)(t.p,{children:`Another common misbelief is that GraphQL/REST is required to develop several frontends.`}),`
`,(0,l.jsxs)(t.p,{children:[`You can develop multiple frontends as well with RPC by having one `,(0,l.jsx)(a,{href:`#telefunc-server`,children:`Telefunc Server`}),` per frontend.`]}),`
`,(0,l.jsxs)(t.blockquote,{children:[`
`,(0,l.jsx)(t.p,{children:`The rise of edge computing,
such as Cloudflare Workers,
makes Telefunc Servers
very performant,
cheap (with generous free tiers),
and easy to set up.`}),`
`]}),`
`,(0,l.jsx)(`h2`,{id:`complex-databases`,children:`Complex Databases`}),`
`,(0,l.jsx)(t.p,{children:`A less common but nonetheless widespread use case is the usage of GraphQL in very large companies.`}),`
`,(0,l.jsx)(t.p,{children:`The databases of large companies can become too complex for
the frontend team.`}),`
`,(0,l.jsx)(t.p,{children:`Instead of the frontend team directly accessing the databases with Telefunc & SQL/ORM queries,
you create a GraphQL API that is simpler to use.
You essentially use GraphQL to abstract away the complexities of your databases.`}),`
`,(0,l.jsx)(t.p,{children:`For performance-critical apps deployed at large scale,
such as Twitter or Facebook,
it is common to use several database technologies at once.
You can then even use GraphQL to simplify the life of
not only the frontend developers but also the backend developers:
the backend developers then also use the GraphQL API instead of directly accessing your databases.`}),`
`,(0,l.jsx)(t.p,{children:`A GraphQL API enables an independent database development
which can become a crucial strategy at scale.`}),`
`,(0,l.jsxs)(t.blockquote,{children:[`
`,(0,l.jsx)(t.p,{children:`GraphQL is the state-of-the-art for this use case;
a RESTful API would be too limiting.`}),`
`]}),`
`,(0,l.jsx)(`h2`,{id:`which-one-to-choose`,children:`Which one to choose?`}),`
`,(0,l.jsx)(t.p,{children:`RPC enables your frontend to directly use ORM/SQL queries which is not only a fundamentally simpler approach,
but also more powerful (you can achieve more with ORM/SQL queries than with GraphQL/RESTful queries).
So you should use RPC whenever you can for a significant increase in development speed.`}),`
`,(0,l.jsx)(t.p,{children:`Also, RPC is a natural fit for the increasingly ubiquitous practice of full-stack development with frameworks such as Next.js.`}),`
`,(0,l.jsx)(t.p,{children:`On the other hand, you need GraphQL/REST if you need to
give third parties access to your database.`}),`
`,(0,l.jsx)(t.p,{children:`Also, using a GraphQL API can be a crucial strategy for very large companies with very complex databases.`}),`
`,(0,l.jsx)(t.p,{children:`In general, a sensible default is to start with RPC and use GraphQL/REST only when the need arises.`})]})}function f(e={}){let{wrapper:t}={...r(),...e.components};return t?(0,l.jsx)(t,{...e,children:(0,l.jsx)(d,{...e})}):d(e)}var p={hasServerOnlyHook:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:!1}},isClientRuntimeLoaded:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:!0}},onBeforeRenderEnv:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:null}},dataEnv:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:null}},guardEnv:{type:`computed`,definedAtData:null,valueSerialized:{type:`js-serialized`,value:null}},onRenderClient:{type:`standard`,definedAtData:{filePathToShowToUser:`@brillout/docpress/renderer/onRenderClient`,fileExportPathToShowToUser:[]},valueSerialized:{type:`pointer-import`,value:o}},onCreatePageContext:{type:`cumulative`,definedAtData:[{filePathToShowToUser:`@brillout/docpress/renderer/onCreatePageContext`,fileExportPathToShowToUser:[]}],valueSerialized:[{type:`pointer-import`,value:s}]},Page:{type:`standard`,definedAtData:{filePathToShowToUser:`/pages/RPC-vs-GraphQL-REST/+Page.mdx`,fileExportPathToShowToUser:[]},valueSerialized:{type:`plus-file`,exportValues:c}},hydrationCanBeAborted:{type:`standard`,definedAtData:{filePathToShowToUser:`@brillout/docpress/config`,fileExportPathToShowToUser:[`default`,`hydrationCanBeAborted`]},valueSerialized:{type:`js-serialized`,value:!0}}};export{p as configValuesSerialized};