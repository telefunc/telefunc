import { setupDockerRun } from './.testRun-docker'
import { testRedisRoomCrossInstance } from './pages/room/e2e-cross-instance'

if (setupDockerRun()) testRedisRoomCrossInstance()
