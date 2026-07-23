# SPDX-License-Identifier: Apache-2.0
#
# grpcurl-based assertion helpers for the E2E node-operator workflow. Source this file, then call:
#   assert_range <expectedFirst> <expectedLast>   — assert serverStatus first/lastAvailableBlock
#   assert_get_block <blockNumber>                — assert getBlock(n) returns that block
#   assert_subscribe <first> <last>               — subscribe the bounded range, assert it streams then ends OK
#
# Reads env: SERVER_PORT (default 40840), PROTO_PATH (default protobuf-sources/proto).
# Service names are org.hiero.block.api.* (NOT com.hedera.hapi.block.*).

: "${SERVER_PORT:=40840}"
: "${PROTO_PATH:=protobuf-sources/proto}"

assert_range() {
  local expected_first="$1" expected_last="$2"
  local status first last
  status=$(grpcurl -plaintext -emit-defaults \
    -import-path "${PROTO_PATH}" -proto block-node/api/node_service.proto \
    -d '{}' "localhost:${SERVER_PORT}" \
    org.hiero.block.api.BlockNodeService/serverStatus)
  echo "${status}"
  first=$(echo "${status}" | jq -r '.firstAvailableBlock')
  last=$(echo "${status}" | jq -r '.lastAvailableBlock')
  if [[ "${first}" != "${expected_first}" || "${last}" != "${expected_last}" ]]; then
    echo "::error::serverStatus range ${first}..${last} != expected ${expected_first}..${expected_last}"
    return 1
  fi
  echo "OK: serverStatus range == ${expected_first}..${expected_last}"
}

assert_get_block() {
  local block_number="$1" got
  got=$(grpcurl -plaintext -emit-defaults -max-msg-sz 268435456 \
    -import-path "${PROTO_PATH}" -proto block-node/api/block_access_service.proto \
    -d "{\"block_number\": ${block_number}}" "localhost:${SERVER_PORT}" \
    org.hiero.block.api.BlockAccessService/getBlock \
    | jq -r '.block.items[0].blockHeader.number')
  if [[ "${got}" != "${block_number}" ]]; then
    echo "::error::getBlock(${block_number}) returned header number '${got}'"
    return 1
  fi
  echo "OK: getBlock(${block_number})"
}

# assert_subscribe <first> <last>: subscribe the bounded range over grpcurl and assert a block header for
# every block in [first,last] plus a terminal SUCCESS. -emit-defaults renders block 0's header.number
# (proto3 default 0) and the status. Stream goes to a temp file (never echoed) so it doesn't bloat the log.
assert_subscribe() {
  local first="$1" last="$2" tmp count min max status expected_count
  expected_count=$((last - first + 1))
  tmp=$(mktemp)
  grpcurl -plaintext -emit-defaults -max-msg-sz 268435456 \
    -import-path "${PROTO_PATH}" -proto block-node/api/block_stream_subscribe_service.proto \
    -d "{\"start_block_number\": ${first}, \"end_block_number\": ${last}}" \
    "localhost:${SERVER_PORT}" \
    org.hiero.block.api.BlockStreamSubscribeService/subscribeBlockStream > "${tmp}"
  IFS=$'\t' read -r count min max < <(jq -rs \
    '[.. | .blockHeader? | objects | .number | tonumber] | unique | [length, (min // -1), (max // -1)] | @tsv' "${tmp}")
  status=$(jq -rs '[.. | .status? | strings] | last // "MISSING"' "${tmp}")
  rm -f "${tmp}"
  echo "subscribe ${first}..${last}: headers=${count} range=${min}..${max} status=${status}"
  if [[ "${status}" != "SUCCESS" ]]; then
    echo "::error::subscribe(${first}..${last}) terminal status '${status}' != SUCCESS"
    return 1
  fi
  if [[ "${count}" != "${expected_count}" || "${min}" != "${first}" || "${max}" != "${last}" ]]; then
    echo "::error::subscribe(${first}..${last}) got ${count} headers (${min}..${max}), expected ${expected_count} (${first}..${last})"
    return 1
  fi
  echo "OK: subscribe(${first}..${last})"
}
