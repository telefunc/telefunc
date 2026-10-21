import { setupDockerRun } from './.testRun-docker'
import { testRedisOutages, testRedisRoomCrossInstance } from './pages/room/e2e-cross-instance'

if (setupDockerRun()) {
  testRedisRoomCrossInstance()
  testRedisOutages()
}
