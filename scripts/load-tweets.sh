#!/bin/sh

# Load tweets into the raw-tweets channel from a file

tweet_file=${1:-~/tmp/geotweets.json}
KAFKA_DIR=${KAFKA_DIR:-~/src/kafka/confluent-3.0.0}

${KAFKA_DIR}/bin/kafka-topics --list --zookeeper localhost:2181 | grep -q '^raw-tweets$'

if [ $? -eq 0 ]
then
    ${KAFKA_DIR}/bin/kafka-topics --delete --zookeeper localhost:2181 --topic raw-tweets
fi

${KAFKA_DIR}/bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic raw-tweets

head -1 ${tweet_file} | ${KAFKA_DIR}/bin/kafka-console-producer --broker-list localhost:9092 --topic raw-tweets
sleep 5
tail -n +2 ${tweet_file} | ${KAFKA_DIR}/bin/kafka-console-producer --broker-list localhost:9092 --topic raw-tweets
