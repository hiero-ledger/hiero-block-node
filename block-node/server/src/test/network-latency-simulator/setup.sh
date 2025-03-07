#!/bin/bash

echo "Setting up test-context"

mkdir -p test-context
mkdir -p test-context/protos

cp -R ../../../../protos/src/main/protobuf/*.proto test-context/protos/
cp ../resources/consumer.sh test-context/

# Make sure to make scripts executable
chmod +x test-context/consumer.sh start.sh configure_latency.sh

echo "Successfully set up test-context"
