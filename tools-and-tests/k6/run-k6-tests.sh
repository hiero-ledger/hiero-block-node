#!/usr/bin/env bash

GRADLEW="./gradlew -q"

setup_proto_defs() {
    (
    cd ../../
    VERSION=$(cat version.txt)
    $GRADLEW clean &> /dev/null # todo we should be able to skip this clean, but locally, too many caching errors...
    $GRADLEW :protobuf-sources:generateBlockNodeProtoArtifact &> /dev/null
    src_tar=./protobuf-sources/block-node-protobuf-${VERSION}.tgz
    dst_dir=./tools-and-tests/k6/k6-proto
    rm -rf "$dst_dir"
    mkdir -p "$dst_dir"
    tar -xzf "$src_tar" -C "$dst_dir"
    )
}

run_bn() {
    (
    cd ../../
    $GRADLEW clean &> /dev/null # todo we should be able to skip this clean, but locally, too many caching errors...
    $GRADLEW :app:stopDockerContainer &> /dev/null
    $GRADLEW :app:startDockerContainerCI &> /dev/null
    )
}

run_simulator() {
    local a="" b=""
    if [[ $# -eq 2 ]]; then
      a=$1
      b=$2
    else
      echo "No arguments supplied for simulator start and end block number to stream!" >&2
    fi
    if (( a > b )); then
        echo "Start block number must be less than or equal to end block number: start=$a, end=$b" >&2
        exit 1
    elif (( a < 0 )); then
        echo "Start and end block numbers must be whole numbers: start=$a, end=$b" >&2
        exit 1
    fi
    (
    cd ../../
    export GENERATOR_START_BLOCK_NUMBER=$a
    export GENERATOR_END_BLOCK_NUMBER=$b
    export BLOCK_STREAM_MILLISECONDS_PER_BLOCK=100 # stream the blocks fast, no need to stream them 1 per sec, adjust as needed
    echo "Starting simulator to stream from block number ${GENERATOR_START_BLOCK_NUMBER} to ${GENERATOR_END_BLOCK_NUMBER}..."
    $GRADLEW clean &> /dev/null # todo we should be able to skip this clean, but locally, too many caching errors...
    $GRADLEW :simulator:stopDockerContainer &> /dev/null
    $GRADLEW :simulator:startDockerContainerPublisher &> /dev/null
    # sleep for the difference in blocks * milliseconds per block + buffer time of 10 seconds for simulator startup
    local sleep_time=$(( (GENERATOR_END_BLOCK_NUMBER - GENERATOR_START_BLOCK_NUMBER) * BLOCK_STREAM_MILLISECONDS_PER_BLOCK / 1000 + 10 ))
    echo "Sleeping for ${sleep_time} seconds to allow block streaming to complete..."
    sleep $sleep_time
    echo "Stopping simulator after block streaming completed."
    )
}

cleanup() {
    (
    cd ../../
    $GRADLEW stopDockerContainer &> /dev/null # stop all running containers
    )
}

# todo add more tests here and also make sure to reset BN

run_server_status_test() {
    echo "Running server status test..."
    local out_file=${k6_out_dir}/server_status_test.log
    k6 run ./average-load/bn-server-status.js >> "${out_file}" 2>&1
    echo "Server status test completed."
}

run_query_validation_test() {
    echo "Running query validation test..."
    local out_file=${k6_out_dir}/query_validation_test.log
    k6 run ./smoke/bn-query-validation.js >> "${out_file}" 2>&1
    echo "Query validation test completed."
}

run_stream_validation_test() {
    echo "Running stream validation test..."
    local out_file=${k6_out_dir}/stream_validation_test.log
    k6 run ./smoke/bn-stream-validation.js >> "${out_file}" 2>&1
    echo "Stream validation test completed."
}

run_publisher_validation_test() {
    echo "Running publisher validation test..."
    local out_file=${k6_out_dir}/publisher_validation_test.log
    k6 run ./smoke/bn-publisher-validation.js >> "${out_file}" 2>&1
    echo "Publisher validation test completed."
}

run_shared_node_tests() {
    # These tests can run on a shared node setup as they only read data
    run_bn
    run_simulator 0 100
    run_server_status_test
    run_query_validation_test
    run_stream_validation_test
}

run_publisher_tests() {
    echo "Running publisher tests..."
    run_bn
    run_publisher_validation_test
    echo "Publisher tests completed."
}

run_tests() {
    trap cleanup EXIT
    echo "Starting K6 tests..."
    local k6_out_dir="k6-out"
    rm -rf "${k6_out_dir}" # remove old output dir if exists before running tests
    mkdir -p "${k6_out_dir}"
    echo "Setting up tests & environment..."
    setup_proto_defs
    echo "Running shared node tests..."
#    run_shared_node_tests
    run_publisher_tests
    echo "Shared node tests completed."
    echo "K6 tests completed. Output available at: ${k6_out_dir}"
}

run_tests
