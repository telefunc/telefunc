import { setupDockerRun } from './.testRun-docker'
import {
  testRedisAcrossServers,
  testRedisOutages,
  testRedisRoomCrossInstance,
  testServerCrash,
} from './pages/room/e2e-cross-instance'

// testServerCrash() waits up to three minutes for a crashed server's member to leave.
if (setupDockerRun('pnpm test:docker', 3 * 60_000)) {
  testRedisRoomCrossInstance()
  testRedisAcrossServers()
  testRedisOutages()
  testServerCrash()
}
