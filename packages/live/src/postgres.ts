// A Cloudflare Worker on Postgres (Hyperdrive) imports this: there @telefunc/live doesn't import Postgres's engine itself.
import { usePostgres } from './database.js'
import * as engine from './postgres/engine.js'

usePostgres(engine)
