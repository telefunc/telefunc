import { config } from 'telefunc/client'
config.transport = import.meta.env.PUBLIC_ENV__TRANSPORT || 'ws'
