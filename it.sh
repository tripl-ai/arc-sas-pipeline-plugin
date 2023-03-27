#!/bin/bash
function finish {
  # stop docker services
  docker-compose -f src/it/resources/docker-compose.yml down
}
trap finish EXIT
# start docker services
docker-compose -f src/it/resources/docker-compose.yml up --build -d && \
# run the test
docker run \
--rm \
--net arc-integration \
-v $(pwd):/app \
-w /app \
amazoncorretto:8 \
./sbt "+it:test"
