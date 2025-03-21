#!/bin/bash

set -e

docker run --rm -v ./configuration:/app/configuration --network kafka-demo_default -t kafka-demo $@
