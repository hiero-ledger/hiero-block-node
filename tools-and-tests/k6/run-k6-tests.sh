#!/usr/bin/env bash

setup_proto_defs() {
    (
    cd ../../
    VERSION=$(cat version.txt)
    ./gradlew clean # todo we should be able to skip this clean, but locally, too many caching errors...
    ./gradlew :protobuf-sources:generateBlockNodeProtoArtifact
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
    ./gradlew clean # todo we should be able to skip this clean, but locally, too many caching errors...
    ./gradlew :app:stopDockerContainer
    ./gradlew :app:startDockerContainerCI
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
    echo "Starting simulator to stream from block number ${GENERATOR_START_BLOCK_NUMBER} to ${GENERATOR_END_BLOCK_NUMBER}"
    ./gradlew clean # todo we should be able to skip this clean, but locally, too many caching errors...
    ./gradlew :simulator:stopDockerContainer
    ./gradlew :simulator:startDockerContainerPublisher
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
    ./gradlew stopDockerContainer # stop all running containers
    )
}

# todo add more tests here and also make sure to reset BN

run_server_status_test() {
    run_bn
    run_simulator 0 10
    echo "Running server status test..."
    k6 run ./average-load/bn-server-status.js >> "${k6_out_file}" 2>&1
    echo "Server status test completed."
}

run_query_validation_test() {
    run_bn
    run_simulator 0 10
    echo "Running query validation test..."
    k6 run ./smoke/bn-query-validation.js >> "${k6_out_file}" 2>&1
    echo "Query validation test completed."
}

run_stream_validation_test() {
    run_bn
    run_simulator 0 10
    echo "Running stream validation test..."
    k6 run ./smoke/bn-stream-validation.js >> "${k6_out_file}" 2>&1
    echo "Stream validation test completed."
}

run_tests() {
    trap cleanup EXIT
    local k6_out_file="k6-out.txt"
    rm -f "${k6_out_file}" # remove old output file if exists before running tests
    setup_proto_defs
    run_server_status_test
    run_query_validation_test
    run_stream_validation_test
    echo "K6 tests completed. Output written to ${k6_out_file}"
    cat "${k6_out_file}"
}

run_tests
