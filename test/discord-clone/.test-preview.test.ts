import { testRun } from './.testRun'
process.env.DISCORD_CLONE_DB = ':memory:' // fresh database per run — the first registrant owns the server
process.env.DISCORD_CLONE_PAGE_SIZE = '5' // small pages so the suite exercises "Load older"
testRun('pnpm preview')
