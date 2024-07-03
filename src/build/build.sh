#!/bin/bash

docker compose up -d

sleep 60

docker exec -it broker kafka-topics --create --topic=src-java-wordcount --bootstrap-server=localhost:9092 --partitions=3