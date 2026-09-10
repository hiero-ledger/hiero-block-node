#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Mirror Node assertion primitives. Sourced by solo-test-runner.sh and by
# scripts/test/test-mirror-assertions.sh (which provides mocks).
#
# Expects the following to be defined by the caller:
#   - fetch_bn_server_status()  - returns raw grpcurl serverStatus JSON for a BN
#   - TOPOLOGIES_DIR            - directory containing topology YAML files
#   - TOPOLOGY                  - active topology name (without .yaml suffix)
#
# Functions exported:
#   get_all_mirror_nodes              - mirror node names from topology file
#   get_mn_rest_port                  - local REST port for a mirror node
#   fetch_mn_latest_block             - latest block number from MN REST API (overridable)
#   assert_mirror_blocks_increasing   - verify MN is importing new blocks
#   assert_mirror_lag                 - verify MN is not falling behind the BN

# get_all_mirror_nodes: reads mirror node names from the active topology file.
# Empty output is not a failure — callers check and skip gracefully.
function get_all_mirror_nodes {
    local topology_file="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"
    if [[ ! -f "$topology_file" ]]; then
        return 0
    fi
    yq -r '.mirror_nodes | keys | .[]' "$topology_file" 2>/dev/null | grep -v '^null$' || true
}

# get_mn_rest_port: returns the local REST port for mirror-N (5551, 5552, …).
# Matches the port-forward allocation in solo-port-forward.sh (MN REST starts at 5551).
function get_mn_rest_port {
    local target="$1"
    local node_num
    node_num=$(echo "$target" | grep -oE '[0-9]+$' || echo "1")
    echo $((5550 + node_num))
}

# fetch_mn_latest_block: query the MN REST API for the latest block number.
# Overridable in fixture tests. Returns empty string on failure.
function fetch_mn_latest_block {
    local target="$1"
    local port
    port=$(get_mn_rest_port "$target")
    curl -s --max-time 10 \
        "http://localhost:${port}/api/v1/blocks?limit=1&order=desc" 2>/dev/null \
        | jq -r '.blocks[0].number // empty' 2>/dev/null
}

# assert_mirror_blocks_increasing_single: verify that a single MN is importing
# new blocks. Same retry semantics as assert_blocks_increasing_single.
function assert_mirror_blocks_increasing_single {
    local target="$1"
    local wait_seconds="${2:-60}"
    local max_attempts="${3:-3}"

    local attempt=1
    local baseline="" current=""

    for _ in 1 2 3; do
        baseline=$(fetch_mn_latest_block "$target")
        if [[ -n "$baseline" && "$baseline" != "null" ]]; then
            break
        fi
        sleep 2
    done

    if [[ -z "$baseline" || "$baseline" == "null" ]]; then
        echo "${target}: Could not get baseline block number from Mirror Node"
        return 1
    fi

    while [[ $attempt -le $max_attempts ]]; do
        sleep "$wait_seconds"

        current=""
        for _ in 1 2 3; do
            current=$(fetch_mn_latest_block "$target")
            if [[ -n "$current" && "$current" != "null" ]]; then
                break
            fi
            sleep 2
        done

        if [[ -z "$current" || "$current" == "null" ]]; then
            attempt=$((attempt + 1))
            continue
        fi

        if [[ "$current" -gt "$baseline" ]]; then
            local total_wait=$((wait_seconds * attempt))
            echo "${target}: $baseline -> $current (+$((current - baseline)) in ${total_wait}s, attempt $attempt/$max_attempts)"
            return 0
        fi

        attempt=$((attempt + 1))
    done

    local total_wait=$((wait_seconds * max_attempts))
    echo "${target}: Mirror Node blocks not increasing after ${max_attempts} attempts (${total_wait}s total): $baseline -> ${current:-unknown}"
    return 1
}

function assert_mirror_blocks_increasing {
    local target="${1:-all}"
    local wait_seconds="${2:-60}"
    local max_attempts="${3:-3}"

    local all_mns
    all_mns=$(get_all_mirror_nodes)

    if [[ -z "$all_mns" ]]; then
        echo "No mirror nodes in topology, skipping mirror-blocks-increasing"
        return 0
    fi

    if [[ "$target" == "all" ]]; then
        local failed=0 results=""
        for mn in $all_mns; do
            local result
            result=$(assert_mirror_blocks_increasing_single "$mn" "$wait_seconds" "$max_attempts") || failed=1
            results="${results}${result}\n"
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_mirror_blocks_increasing_single "$target" "$wait_seconds" "$max_attempts"
    fi
}

# assert_mirror_lag_single: verify that a single MN has not fallen behind its
# paired BN. The BN to compare against is the first entry in
# mirror_nodes.<target>.block_nodes from the topology (falls back to block-node-1).
function assert_mirror_lag_single {
    local target="$1"
    local max_blocks_behind="${2:-30}"

    local topology_file="${TOPOLOGIES_DIR}/${TOPOLOGY}.yaml"
    local bn_target
    bn_target=$(yq -r ".mirror_nodes.\"${target}\".block_nodes[0] // \"block-node-1\"" \
        "$topology_file" 2>/dev/null || echo "block-node-1")
    [[ -z "$bn_target" || "$bn_target" == "null" ]] && bn_target="block-node-1"

    local mn_last
    mn_last=$(fetch_mn_latest_block "$target")
    if [[ -z "$mn_last" || "$mn_last" == "null" ]]; then
        echo "${target}: Could not get Mirror Node latest block"
        return 1
    fi

    local bn_status bn_last
    bn_status=$(fetch_bn_server_status "$bn_target")
    bn_last=$(echo "$bn_status" | jq -r '.lastAvailableBlock // empty' 2>/dev/null)
    if [[ -z "$bn_last" || "$bn_last" == "null" ]]; then
        echo "${target}: Could not get Block Node (${bn_target}) lastAvailableBlock"
        return 1
    fi

    local lag=$((bn_last - mn_last))
    if [[ $lag -gt $max_blocks_behind ]]; then
        echo "${target}: Mirror lag ${lag} blocks (mn=${mn_last}, bn=${bn_last}) exceeds max ${max_blocks_behind}"
        return 1
    fi

    echo "${target}: mn=${mn_last} bn=${bn_last} lag=${lag} (max=${max_blocks_behind})"
    return 0
}

function assert_mirror_lag {
    local target="${1:-all}"
    local max_blocks_behind="${2:-30}"

    local all_mns
    all_mns=$(get_all_mirror_nodes)

    if [[ -z "$all_mns" ]]; then
        echo "No mirror nodes in topology, skipping mirror-lag"
        return 0
    fi

    if [[ "$target" == "all" ]]; then
        local failed=0 results=""
        for mn in $all_mns; do
            local result
            result=$(assert_mirror_lag_single "$mn" "$max_blocks_behind") || failed=1
            results="${results}${result}\n"
        done
        echo -e "${results%\\n}"
        return $failed
    else
        assert_mirror_lag_single "$target" "$max_blocks_behind"
    fi
}
