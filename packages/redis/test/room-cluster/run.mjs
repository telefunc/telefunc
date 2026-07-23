// Launches one uniquely owned disposable WSL Redis Cluster, runs the full W4-R Vitest matrix through
// the Windows toolchain, and proves process/port/data cleanup in finally. It never addresses the
// machine's standalone Redis service and never issues FLUSHDB/FLUSHALL.

import { randomBytes } from 'node:crypto'
import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { spawnSync } from 'node:child_process'

const repoRoot = fileURLToPath(new URL('../../../../', import.meta.url))
const wsl = `${process.env.WINDIR ?? 'C:\\Windows'}\\System32\\wsl.exe`
const vitest = `${repoRoot}node_modules\\vitest\\vitest.mjs`
const tsc = `${repoRoot}node_modules\\typescript\\bin\\tsc`
const expectClusterSafe = !process.argv.includes('--expect-cluster-safe=false')
const focusedFile = process.argv.find((arg) => arg.startsWith('--file='))?.slice('--file='.length)
const focusedTest = process.argv.find((arg) => arg.startsWith('--test-name='))?.slice('--test-name='.length)
const watchdogMs = 20 * 60 * 1_000

if (process.platform !== 'win32') throw new Error('W4-R launcher currently requires Windows + WSL2')
if (!existsSync(wsl)) throw new Error(`WSL executable not found at ${wsl}`)
if (!existsSync(vitest)) throw new Error('Dependencies missing: run pnpm install --frozen-lockfile first')
if (!existsSync(tsc)) throw new Error('TypeScript missing: run pnpm install --frozen-lockfile first')

buildPublicPackage()

const token = `${process.pid}-${Date.now().toString(36)}-${randomBytes(4).toString('hex')}`
let owned
const startedAt = Date.now()
let passed = false
try {
  certifyPartialStartupCleanup(`${token}-startup-control`)
  owned = startCluster(token)
  const endpoints = owned.ports.map((port) => `127.0.0.1:${port}`).join(',')
  console.log(
    `[w4r-launcher] root=${owned.root} endpoints=${endpoints} redisServer=${owned.redisServerVersion} redisCli=${owned.redisCliVersion} masters=3`,
  )
  const vitestArgs = [vitest, 'run', '--config', 'packages/redis/vitest.cluster.config.ts', '--reporter=verbose']
  if (focusedFile !== undefined && focusedFile !== '') vitestArgs.push(focusedFile)
  if (focusedTest !== undefined && focusedTest !== '') {
    vitestArgs.push('--testNamePattern', focusedTest, '--testTimeout=30000', '--hookTimeout=30000')
  }
  const result = spawnSync(process.execPath, vitestArgs, {
    cwd: repoRoot,
    env: {
      ...process.env,
      TELEFUNC_TEST_REAL_REDIS: `redis://127.0.0.1:${owned.ports[0]}`,
      TELEFUNC_TEST_REDIS_CLUSTER_NODES: endpoints,
      TELEFUNC_EXPECT_CLUSTER_SAFE: String(expectClusterSafe),
      TELEFUNC_W4R_LAUNCHER_SENTINEL: `telefunc-w4r-${token}`,
    },
    stdio: 'inherit',
    timeout: watchdogMs,
    killSignal: 'SIGTERM',
  })
  if (result.error !== undefined) throw result.error
  if (result.signal !== null) throw new Error(`W4-R Vitest terminated by ${result.signal}`)
  if (result.status !== 0) throw new Error(`W4-R Vitest exited ${result.status}`)
  passed = true
} finally {
  if (owned !== undefined) {
    cleanupCluster(owned)
    console.log(
      `[w4r-launcher] ${passed ? 'PASS' : 'FAIL'} cleanup=zero-residue durationMs=${Date.now() - startedAt} expectedClusterSafe=${expectClusterSafe}`,
    )
  }
}

function startCluster(runToken) {
  for (let attempt = 0; attempt < 8; attempt++) {
    const base = 18_000 + ((process.pid + attempt * 977) % 3_000)
    const ports = [base, base + 1, base + 2]
    const root = `/tmp/telefunc-w4r-${runToken}-${attempt}`
    const script = `
set -eu
root='${root}'
case "$root" in /tmp/telefunc-w4r-*) ;; *) exit 91 ;; esac
umask 077
mkdir -p "$root"
for p in ${ports.join(' ')}; do
  bus=$((p + 10000))
  if ss -ltn 2>/dev/null | grep -Eq ":($p|$bus) "; then exit 92; fi
done
for p in ${ports.join(' ')}; do
  mkdir -p "$root/$p"
  redis-server --port "$p" --bind 127.0.0.1 --protected-mode no \\
    --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 \\
    --cluster-announce-ip 127.0.0.1 --cluster-announce-port "$p" \\
    --cluster-announce-bus-port "$((p + 10000))" --appendonly no --save "" \\
    --daemonize yes --pidfile "$root/$p/redis.pid" --logfile "$root/$p/redis.log" --dir "$root/$p"
done
for p in ${ports.join(' ')}; do
  i=0
  until redis-cli -h 127.0.0.1 -p "$p" ping >/dev/null 2>&1; do
    i=$((i + 1)); [ "$i" -lt 100 ] || exit 93; sleep 0.05
  done
done
redis-cli --cluster create ${ports.map((port) => `127.0.0.1:${port}`).join(' ')} --cluster-replicas 0 --cluster-yes
redis-cli -h 127.0.0.1 -p ${ports[0]} cluster nodes
redis-cli -h 127.0.0.1 -p ${ports[0]} cluster info
printf '__W4R_REDIS_SERVER_VERSION__=%s\n' "$(redis-server --version | head -n 1)"
printf '__W4R_REDIS_CLI_VERSION__=%s\n' "$(redis-cli --version | head -n 1)"
`
    const result = runWsl(script)
    if (result.status === 0) {
      const redisServerVersion = result.stdout.match(/__W4R_REDIS_SERVER_VERSION__=(.*)/)?.[1]?.trim()
      const redisCliVersion = result.stdout.match(/__W4R_REDIS_CLI_VERSION__=(.*)/)?.[1]?.trim()
      if (redisServerVersion === undefined || redisCliVersion === undefined) {
        cleanupCluster({ root, ports }, true)
        throw new Error(`Redis version discovery failed\n${result.stdout}\n${result.stderr}`)
      }
      return { root, ports, redisServerVersion, redisCliVersion }
    }
    cleanupCluster({ root, ports }, result.status !== 92)
    if (result.status !== 92) {
      throw new Error(`Redis Cluster startup failed (${result.status})\n${result.stdout}\n${result.stderr}`)
    }
  }
  throw new Error('Unable to reserve three unique Redis Cluster ports after eight attempts')
}

function certifyPartialStartupCleanup(runToken) {
  const base = 22_000 + (process.pid % 1_000)
  const ports = [base, base + 1, base + 2]
  const root = `/tmp/telefunc-w4r-${runToken}`
  const script = `
set -eu
root='${root}'
case "$root" in /tmp/telefunc-w4r-*) ;; *) exit 91 ;; esac
for p in ${ports.join(' ')}; do
  bus=$((p + 10000))
  if ss -ltn 2>/dev/null | grep -Eq ":($p|$bus) "; then exit 92; fi
done
umask 077
mkdir -p "$root/${ports[0]}"
redis-server --port "${ports[0]}" --bind 127.0.0.1 --protected-mode no \\
  --cluster-enabled yes --cluster-config-file nodes.conf --cluster-node-timeout 5000 \\
  --cluster-announce-ip 127.0.0.1 --cluster-announce-port "${ports[0]}" \\
  --cluster-announce-bus-port "${ports[0] + 10_000}" --appendonly no --save "" \\
  --daemonize yes --pidfile "$root/${ports[0]}/redis.pid" \\
  --logfile "$root/${ports[0]}/redis.log" --dir "$root/${ports[0]}"
i=0
until [ -s "$root/${ports[0]}/redis.pid" ] && redis-cli -h 127.0.0.1 -p "${ports[0]}" ping >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -lt 100 ] || exit 93; sleep 0.05
done
exit 77
`
  const result = runWsl(script)
  if (result.status === 92) return
  if (result.status !== 77) {
    cleanupCluster({ root, ports }, false)
    throw new Error(`Partial-start control failed to reach assertion boundary (${result.status})`)
  }
  cleanupCluster({ root, ports }, true)
  console.log(`[w4r-launcher] startup-assertion cleanup=zero-residue root=${root} ports=${ports.join(',')}`)
}

function cleanupCluster({ root, ports }, verifyPorts = true) {
  const script = `
set -u
root='${root}'
case "$root" in /tmp/telefunc-w4r-*) ;; *) exit 94 ;; esac
pids=''
failures=''
if [ -d "$root" ]; then
  for p in ${ports.join(' ')}; do
    pidfile="$root/$p/redis.pid"
    [ -f "$pidfile" ] || continue
    pid=$(cat "$pidfile")
    case "$pid" in ''|*[!0-9]*) failures="$failures invalid-pidfile:$pidfile:$pid"; continue ;; esac
    if ! kill -0 "$pid" 2>/dev/null; then
      pids="$pids $pid:$p:absent"
      continue
    fi
    cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)
    exe=$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)
    args=$(ps -p "$pid" -o args= 2>/dev/null || true)
    if [ "$cwd" != "$root/$p" ] || { [ "$exe" != "/usr/bin/redis-server" ] && [ "$exe" != "/usr/bin/redis-check-rdb" ]; }; then
      failures="$failures unverified-pid:$pid:$p:cwd=$cwd:exe=$exe:args=$args"
      continue
    fi
    case "$args" in *"redis-server 127.0.0.1:$p"*) ;; *) failures="$failures unexpected-args:$pid:$p:$args"; continue ;; esac
    pids="$pids $pid:$p:live"
  done
  for pair in $pids; do
    state=${'${pair##*:}'}; rest=${'${pair%:*}'}; pid=${'${rest%:*}'}
    [ "$state" = live ] || continue
    kill "$pid" 2>/dev/null || failures="$failures term-failed:$pid"
  done
  for pair in $pids; do
    state=${'${pair##*:}'}; rest=${'${pair%:*}'}; pid=${'${rest%:*}'}; p=${'${rest#*:}'}
    [ "$state" = live ] || continue
    i=0
    while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 100 ]; do i=$((i + 1)); sleep 0.05; done
    if kill -0 "$pid" 2>/dev/null; then
      cwd=$(readlink -f "/proc/$pid/cwd" 2>/dev/null || true)
      exe=$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)
      if [ "$cwd" = "$root/$p" ] && { [ "$exe" = "/usr/bin/redis-server" ] || [ "$exe" = "/usr/bin/redis-check-rdb" ]; }; then
        kill -KILL "$pid" 2>/dev/null || failures="$failures kill-failed:$pid"
        i=0; while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 100 ]; do i=$((i + 1)); sleep 0.05; done
      else
        failures="$failures reverify-failed:$pid:$p:cwd=$cwd:exe=$exe"
      fi
    fi
    kill -0 "$pid" 2>/dev/null && failures="$failures still-live:$pid:$p"
  done
  if [ -z "$failures" ]; then rm -rf -- "$root"; fi
fi
if [ -e "$root" ]; then failures="$failures root-remains:$root"; fi
if [ '${verifyPorts ? 'yes' : 'no'}' = yes ]; then
  for p in ${ports.join(' ')}; do
    bus=$((p + 10000))
    if ss -ltn 2>/dev/null | grep -Eq ":($p|$bus) "; then failures="$failures listener-remains:$p/$bus"; fi
  done
fi
if [ -n "$failures" ]; then echo "cleanup failures:$failures" >&2; exit 99; fi
`
  const result = runWsl(script)
  if (result.status !== 0) {
    throw new Error(`Redis Cluster cleanup failed (${result.status})\n${result.stdout}\n${result.stderr}`)
  }
}

function buildPublicPackage() {
  const result = spawnSync(process.execPath, [tsc, '--build', 'packages/redis'], {
    cwd: repoRoot,
    encoding: 'utf8',
    timeout: 120_000,
    windowsHide: true,
  })
  if (result.error !== undefined) throw result.error
  if (result.status !== 0)
    throw new Error(`Redis public build failed (${result.status})\n${result.stdout}\n${result.stderr}`)
  if (!existsSync(`${repoRoot}packages\\redis\\dist\\index.js`))
    throw new Error('Redis public build emitted no dist/index.js')
}

function runWsl(script) {
  return spawnSync(wsl, ['-e', 'sh', '-s'], {
    input: script,
    encoding: 'utf8',
    timeout: 60_000,
    windowsHide: true,
  })
}
