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

# todo add more tests here and also make sure to reset BN

run_server_status_test() {
    run_test "Server Status Test" "server_status_test" "./src/average-load/bn-server-status.js"
}

run_server_status_detail_test() {
    run_test "Server Status Detail Test" "server_status_detail_test" "./src/average-load/bn-server-status-detail.js"
}

run_query_validation_test() {
    run_test "Query Validation Test" "query_validation_test" "./src/smoke/bn-query-validation.js"
}

run_stream_validation_test() {
    run_test "Stream Validation Test" "stream_validation_test" "./src/smoke/bn-stream-validation.js"
}

run_shared_node_tests() {
    declare -i rc=0
    run_server_status_test
    rc+=$?
    run_server_status_detail_test
    rc+=$?
    run_query_validation_test
    rc+=$?
    run_stream_validation_test
    rc+=$?
    return $rc
}

#
# A generic test runner that accepts parameters for the test to run
#
# $1 - test name
# $2 - log filename without path or extension
# $3 - test script fully qualified filename
#
run_test() {
    local test_name=$1
    local log_file=$2
    local test_script=$3
    echo "Running ${test_name} ..."
    local out_file=${k6_out_dir}/"${log_file}".log
    k6 run "${test_script}" >> "${out_file}" 2>&1
    if [[ $? -ne 0 ]]; then
        echo "${test_name} FAILED."
        return 1
        # Optional: exit the script with a specific error code
    else
        echo "${test_name} PASSED."
        return 0
    fi
}

run_tests() {
    trap cleanup EXIT
    echo "Starting K6 tests..."
    local k6_out_dir="k6-out"
    rm -rf "${k6_out_dir}" # remove old output dir if exists before running tests
    mkdir -p "${k6_out_dir}"
    echo "Setting up tests & environment..."
    start_solo || exit $?
    setup_proto_defs
    echo "Running shared node tests..."
    run_shared_node_tests
    rc=$?
    echo "Shared node tests completed. ret=$rc"
    echo "K6 tests completed. Output available at: ${k6_out_dir}"
    return $((rc))
}

cleanup() {
    (
    pushd "../scripts/solo-e2e-test" || exit
    task down
    popd || exit
    )
}

start_solo() {
    (
    pushd "../scripts/solo-e2e-test" || return
    task up
    if [[ $? -ne 0 ]]; then
        echo "FAILED TO START SOLO"
        return 1
    fi
    popd || return
    )
}

run_tests
