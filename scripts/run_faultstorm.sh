#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd "$(dirname "$0")/.." && pwd)
venv_dir="$repo_dir/venv"
feature=${1:-tests/faultstorm/features/maintenance_resetup.feature}

case "$feature" in
    tests/faultstorm/features/*.feature) ;;
    *) echo "Feature must be under tests/faultstorm/features/" >&2; exit 2 ;;
esac

test -f "$repo_dir/$feature" || { echo "Feature not found: $feature" >&2; exit 2; }
test -x "$venv_dir/bin/python3" || { echo "Missing project venv: $venv_dir" >&2; exit 1; }
export PATH="$venv_dir/bin:$PATH"

docker info >/dev/null
echo "Docker context: $(docker context show)"

expected_containers=$(printf '%s\n' pgconsul_faultstorm_1 pgconsul_postgresql1_1 pgconsul_postgresql2_1 pgconsul_postgresql3_1 pgconsul_zookeeper1_1 pgconsul_zookeeper2_1 pgconsul_zookeeper3_1 | sort)
existing_containers=$(docker ps -a --format '{{.Names}}' | grep '^pgconsul_' | sort || true)
if [ -n "$existing_containers" ] && [ "$existing_containers" != "$expected_containers" ]; then
    echo "Refusing to replace a non-FaultStorm pgconsul stack." >&2
    exit 1
fi

cd "$repo_dir"
make faultstorm_build
FAULTSTORM_FEATURE="$feature" make faultstorm_behave
