#!/usr/bin/env bash
# Runs a command (by default the Redis certification specs) against a standalone Redis and a three-master Cluster
# that it starts, with REDIS_URL and REDIS_CLUSTER_NODES set, and stops them on exit. The standalone listens on
# BASE_PORT (default 7000), the Cluster on the next three ports; WORK_DIR (default: a new temporary directory) holds
# their files.
set -euo pipefail
cd "$(dirname "$0")/../../.."

if ! command -v redis-server >/dev/null; then
  sudo apt-get update -q
  sudo apt-get install -y -q redis-server
fi

base=${BASE_PORT:-7000}
dir=${WORK_DIR:-$(mktemp -d)}
standalone=$base
cluster=($((base + 1)) $((base + 2)) $((base + 3)))

pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT

waitFor() {
  for _ in $(seq 100); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "Timed out waiting for: $*" >&2
  return 1
}
clusterOk() {
  redis-cli -p "$1" cluster info | grep -q '^cluster_state:ok'
}

for port in "$standalone" "${cluster[@]}"; do
  mkdir -p "$dir/$port"
  args=(--port "$port" --bind 127.0.0.1 --save '' --appendonly no --dir "$dir/$port")
  if [ "$port" != "$standalone" ]; then args+=(--cluster-enabled yes --cluster-config-file nodes.conf); fi
  redis-server "${args[@]}" >"$dir/$port/redis.log" 2>&1 &
  pids+=($!)
done
for port in "$standalone" "${cluster[@]}"; do waitFor redis-cli -p "$port" ping; done
redis-cli --cluster create "${cluster[@]/#/127.0.0.1:}" --cluster-yes >"$dir/cluster-create.log"
for port in "${cluster[@]}"; do waitFor clusterOk "$port"; done

export REDIS_URL="redis://127.0.0.1:$standalone"
REDIS_CLUSTER_NODES=$(printf '127.0.0.1:%s,' "${cluster[@]}")
export REDIS_CLUSTER_NODES=${REDIS_CLUSTER_NODES%,}
if [ $# -eq 0 ]; then set -- pnpm exec vitest run --config packages/redis/vitest.cluster.config.ts; fi
"$@"
