// Exports-map check: every public entry point resolves and type-checks under Node16, NodeNext and Bundler.
import * as telefunc from 'telefunc'
import * as asyncHooks from 'telefunc/async_hooks'
import * as client from 'telefunc/client'
import * as react from 'telefunc/react'
import * as reactStreaming from 'telefunc/react-streaming'
import * as reactStreamingServer from 'telefunc/react-streaming/server'
import * as vite from 'telefunc/vite'
import * as webpackLoader from 'telefunc/webpack/loader'
import * as next from 'telefunc/next'
import * as nuxt from 'telefunc/nuxt'
import * as babel from 'telefunc/babel'
import * as node from 'telefunc/node'
import * as internal from 'telefunc/__internal'
import * as redis from '@telefunc/redis'

void [telefunc, asyncHooks, client, react, reactStreaming, reactStreamingServer, vite, webpackLoader]
void [next, nuxt, babel, node, internal, redis]
