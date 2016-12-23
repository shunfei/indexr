#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/../../


if [[ "x$1" = "x-i" ]]; then
    mvn clean install -DskipTests=true -pl indexr-segment -am
elif [[ "x$1" = "x-t" ]]; then
    mvn clean install -pl indexr-segment -am
    mvn assembly:single -DdescriptorId=jar-with-dependencies -pl indexr-segment -am
else
    mvn clean install -DskipTests=true -pl indexr-segment -am
    mvn assembly:single -DdescriptorId=jar-with-dependencies -DskipTests=true -pl indexr-segment -am
fi
