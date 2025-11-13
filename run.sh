#!/bin/bash

# Usage: ./run.sh <num_nodes> <num_peers_per_node>
N=${1:-4}
M=${2:-2}
PORT=8000
NETWORK="extrema_propagation_net"

echo "Starting $N nodes with $M peers each..."

# Build the image
docker build -t node:latest .

# Create network if not exists
if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
  echo "Creating network $NETWORK..."
  docker network create "$NETWORK"
fi

# Clean up old containers
for i in $(seq 1 $N); do
  docker rm -f node_$i >/dev/null 2>&1 || true
done

# Build base data
for i in $(seq 1 $N); do
  HOSTS[$i]="node_$i"
done

# ---------------- Connected symmetric peer generation ----------------
declare -A PEERS

add_edge() {
  local a=$1 b=$2
  [[ $a -eq $b ]] && return
  [[ ,${PEERS[$a]}, == *",$b,"* ]] && return

  # add both directions
  [[ -n ${PEERS[$a]} ]] && PEERS[$a]+=","
  [[ -n ${PEERS[$b]} ]] && PEERS[$b]+=","
  PEERS[$a]+=$b
  PEERS[$b]+=$a
}

# ---- Step 1: build a spanning tree to ensure connectivity ----
# Connect each node to one previous random node
for i in $(seq 2 $N); do
  p=$((RANDOM % (i-1) + 1))
  add_edge "$i" "$p"
done

# ---- Step 2: add random edges until target avg degree M ----
TARGET_EDGES=$((M * N / 2))

# Count existing edges
count_edges() {
  local edges=0
  for v in "${PEERS[@]}"; do
    [[ -z "$v" ]] && continue
    local c
    c=$(awk -F, '{print NF}' <<< "$v")
    edges=$((edges + c))
  done
  echo $((edges / 2))
}

CURRENT_EDGES=$(count_edges)

# Add random edges
while [[ $CURRENT_EDGES -lt $TARGET_EDGES ]]; do
  a=$((RANDOM % N + 1))
  b=$((RANDOM % N + 1))
  add_edge "$a" "$b"
  CURRENT_EDGES=$(count_edges)
done


# ---------- Run containers ----------
for i in $(seq 1 $N); do
  NAME="${HOSTS[$i]}"

  LOG="log/${NAME}"
  rm -rf $LOG
  mkdir -p $LOG

  HOST_PORT=$((9200 + i))

  # peers for node i (already comma-separated)
  raw="${PEERS[$i]}"
  ids=(${raw//,/ })
  ids=($(printf "%s\n" "${ids[@]}" | sort -n | uniq))

  PEER_IDS=$(IFS=,; echo "${ids[*]}")
  PEER_HOSTS=$(IFS=,; for id in "${ids[@]}"; do printf "%s," "${HOSTS[$id]}"; done | sed 's/,$//')

  echo "Node $i ($NAME): peers -> ${PEER_IDS:-none}"

  docker run -d \
    --name "$NAME" \
    --hostname "$NAME" \
    --network "$NETWORK" \
    -e LISTEN_HOST="$NAME" \
    -e ID="$i" \
    -e LISTEN_PORT="$PORT" \
    -e PEER_IDS="$PEER_IDS" \
    -e PEER_HOSTS="$PEER_HOSTS" \
    -v "$(pwd)/${LOG}:/var/log/extrema_propagation" \
    -p "${HOST_PORT}:9200" \
    node:latest >/dev/null

done

echo "Nodes started"
