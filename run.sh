#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 5 ]]; then
  echo "Usage: $0 <start_index> <number_of_nodes> <region> <gn_region> <port_offset>"
  exit 1
fi

INDEX="$1"
COUNT="$2"
REGION="$3"
GN_REGION="$4"
PORT_OFFSET="$5"
NETWORK="funet"
IMAGE="ep"

cd ../
docker build -t "$IMAGE" -f extrema_propagation/Dockerfile .
cd extrema_propagation

# Create Docker network if it doesn't already exist
if ! docker network ls --format '{{.Name}}' | grep -q "^$NETWORK$"; then
  echo "🔧 Creating Docker network '$NETWORK'..."
  docker network create "$NETWORK"
fi

CONTACT_NODE_ID="${REGION}_node_1"
CONTACT_NODE_ADDR="${REGION}_node_1:6001"

for i in $(seq "$(($INDEX))" "$(($INDEX + $COUNT -1))"); do
    NAME="${REGION}_node_$i"
    LISTEN_ADDR="${NAME}:6001"

    LOG="log/${NAME}"
    rm -rf $LOG
    mkdir -p $LOG/results

    echo "Starting container $NAME from image $IMAGE..."
    docker run -dit \
        --name "$NAME" \
        --hostname "$NAME" \
        --network "$NETWORK" \
        --env-file .env \
        -e NODE_REGION="$REGION" \
        -e NODE_ID="$NAME" \
        -e CONTACT_NODE_ID="$CONTACT_NODE_ID" \
        -e CONTACT_NODE_ADDR="$CONTACT_NODE_ADDR" \
        -e LISTEN_ADDR="$LISTEN_ADDR" \
        -v "$(pwd)/${LOG}:/var/log/fu" \
        "$IMAGE"

    sleep 0.5

    containers=$(docker ps --format "{{.Names}}")
    if [ -z "$containers" ]; then
        echo "No running containers found."
        exit 1
    fi
    random_container=$(echo "$containers" | shuf -n 1)

    CONTACT_NODE_ID="$random_container"
    CONTACT_NODE_ADDR="${random_container}:6001"
done

echo "Started $COUNT containers with REGION=$REGION."
