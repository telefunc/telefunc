import { onHello } from './hello.telefunc'

document.querySelector('#greeting')!.textContent = await onHello('world')
