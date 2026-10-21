import { setupDockerRun } from './.testRun-docker'
import { testRedisAcrossServers, testRedisOutages, testRedisRoomCrossInstance } from './pages/room/e2e-cross-instance'

if (setupDockerRun()) {
  testRedisRoomCrossInstance()
  testRedisAcrossServers()
  testRedisOutages()
}
