import { setupDockerRun } from './.testRun-docker'
import { testRedisRoomClosedKeys, testRedisRoomCrossInstance } from './pages/room/e2e-cross-instance'

if (setupDockerRun()) {
  testRedisRoomCrossInstance()
  testRedisRoomClosedKeys()
}
