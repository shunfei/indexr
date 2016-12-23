#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/../../

if [[ "x$1" = "x-i" ]]; then
    mvn install -DskipTests=true -pl indexr-server -am
elif [[ "x$1" = "x-clean" ]]; then
    mvn clean -DskipTests=true -pl indexr-server -am
else
    mvn install -DskipTests=true -pl indexr-server -am
    mvn assembly:single -DdescriptorId=jar-with-dependencies -DskipTests=true -pl indexr-server -am
fi