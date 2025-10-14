#!/usr/bin/env bash

docker ps -a --format '{{.Names}}' | grep '_node_' | xargs -r docker rm -f