#!/bin/bash


sudo apt update -y
sudo apt install -y openjdk-11-jdk

sudo apt install -y maven

#mvn archetype:generate -DgroupId=com.kafka.streams -DartifactId=kafka-streams -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false