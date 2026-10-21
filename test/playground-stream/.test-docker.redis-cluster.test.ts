import { setupDockerRun } from './.testRun-docker'
import {
  testRedisBroadcastCrossInstance,
  testRedisClusterReshard,
  testRedisRoomCrossInstance,
} from './pages/room/e2e-cross-instance'

// The playgrounds start once the stack's Redis Cluster has formed.
if (setupDockerRun('pnpm test:docker:redis-cluster', 5 * 60_000)) {
  testRedisRoomCrossInstance()
  testRedisBroadcastCrossInstance()
  testRedisClusterReshard()
}
