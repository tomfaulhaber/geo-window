#!/bin/bash

# Do a hard reset of Kafka, Zookeeper, and Kafka Streams so that we can
# start from scratch.
#
# USE THIS WITH CARE!!!


STATE_DIR=/tmp

KAFKA_RUNNING=no
ZK_RUNNING=no


if [ $(ps aux | grep SupportedKafka | wc -l) -gt 1 ]
then
    echo "Kafka is still running, please stop it and try again"
    KAFKA_RUNNING=yes
fi

if [ $(ps aux | grep QuorumPeerMain | wc -l) -gt 1 ]
then
    echo "Zookeeper is still running, please stop it and try again"
    ZK_RUNNING=yes
fi

if [ "${KAFKA_RUNNING}" == "yes" -o "${ZK_RUNNING}" == "yes" ]
then
    exit 1
fi

rm -rf "${STATE_DIR}/kafka-streams"
rm -rf "${STATE_DIR}/kafka-logs"
rm -rf "${STATE_DIR}/zookeeper"
