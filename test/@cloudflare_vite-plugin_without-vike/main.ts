import { config } from 'telefunc/client'
import { onHello } from './hello.telefunc'

config.telefuncUrl = '/api/tf'

document.querySelector('#greeting')!.textContent = await onHello('world')
