#!/usr/bin/env bash
# One-time setup: create our 2 Kafka topics.
# Needed because docker-compose sets AUTO_CREATE_TOPICS_ENABLE=false (typos fail loudly).
# Run from Git Bash:  bash scripts/create-topics.sh
# --if-not-exists makes it safe to run again.

export MSYS_NO_PATHCONV=1   # stop Git Bash from rewriting /opt/kafka/... into a Windows path

for topic in emoji-events aggregated-emoji; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 3 \
    --replication-factor 1
done

docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
