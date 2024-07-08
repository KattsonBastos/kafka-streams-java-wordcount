#!/bin/bash


build() {

  ## 
  echo "### Maven build"
  # mvn clean package
  mvn clean install
  mvn package assembly:single

}

streamp_proc() { 
  echo "### Streaming.."
  # java -cp target/rides-1.0-SNAPSHOT.jar com.kafka.rides.RideStreamProcessor
  java -jar target/rides-1.0-SNAPSHOT-jar-with-dependencies.jar
}


produce() { 
  echo "### Producing.."
  java -cp target/rides-1.0-SNAPSHOT-jar-with-dependencies.jar com.kafka.rides.RideDataProducer

}

case $1 in
  build)
    build
    ;;
  stp)
    streamp_proc
    ;;
  
  pd)
    produce
    ;;
  *)
    echo "Usage: $0 {build, stp}"
    ;;
esac