import { setupDockerRun } from './.testRun-docker'
import { testRedisBroadcastCrossInstance, testRedisRoomCrossInstance } from './pages/room/e2e-cross-instance'

if (setupDockerRun('pnpm test:docker:redis-cluster')) {
  testRedisRoomCrossInstance()
  testRedisBroadcastCrossInstance()
}
