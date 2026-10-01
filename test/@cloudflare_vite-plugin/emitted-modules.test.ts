import { runCommandThatTerminates } from '@brillout/test-e2e'

await runCommandThatTerminates('pnpm run test:emitted-modules')
