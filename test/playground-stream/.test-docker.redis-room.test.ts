import { setupDockerRun } from './.testRun-docker'
import {
  testRedisAcrossServers,
  testRedisInstallAgain,
  testRedisOutages,
  testRedisRoomCrossInstance,
  testServerCrash,
} from './pages/room/e2e-cross-instance'
import { testRoomMetaReads } from './pages/room/e2e-test'

// testServerCrash() waits up to three minutes for a crashed server's member to leave.
if (setupDockerRun('pnpm test:docker', 3 * 60_000)) {
  testRedisRoomCrossInstance()
  testRedisAcrossServers()
  testRedisInstallAgain()
  // Before the tests that restart a container: one that starts changes the host's network, which aborts the page's
  // requests in flight (net::ERR_NETWORK_CHANGED).
  testRoomMetaReads()
  testRedisOutages()
  testServerCrash()
}
