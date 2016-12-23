#!/usr/bin/env bash
SCRIPT_DIR=$(cd $(dirname $0);echo $PWD)
cd ${SCRIPT_DIR}/../../

mvn clean install -DskipTests=true -pl indexr-hadoop -am