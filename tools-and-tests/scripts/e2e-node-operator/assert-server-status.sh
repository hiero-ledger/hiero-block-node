# SPDX-License-Identifier: Apache-2.0
#
# grpcurl-based assertion helpers for the E2E node-operator workflow. Source this file, then call:
#   assert_server_status [first] [last] [nextExpected]  — assert serverStatus fields; "" skips that field
#   assert_get_block <blockNumber>                       — assert getBlock(n) returns that block
#   assert_subscribe <first> <last>                     — subscribe the bounded range, assert it streams then ends OK
#
# assert_server_status checks firstAvailableBlock, lastAvailableBlock, and nextExpectedBlock.
# Pass "" for any argument to skip that field's check. Examples:
#   assert_server_status 0 2 3                          — blocks 0-2 stored, next expected = 3
#   assert_server_status "" "" 18446744073709551615     — empty state (no publisher, uint64-max)
#
# Reads env: SERVER_PORT (default 40840), PROTO_PATH (default protobuf-sources/proto).
# Service names are org.hiero.block.api.* (NOT com.hedera.hapi.block.*).

: "${SERVER_PORT:=40840}"
: "${PROTO_PATH:=protobuf-sources/proto}"

assert_server_status() {
  local expected_first="$1" expected_last="$2" expected_next="$3"
  local status first last next failed=0
  status=$(grpcurl -plaintext -emit-defaults \
    -import-path "${PROTO_PATH}" -proto block-node/api/node_service.proto \
    -d '{}' "localhost:${SERVER_PORT}" \
    org.hiero.block.api.BlockNodeService/serverStatus)
  echo "${status}"
  if [[ -n "${expected_first}" ]]; then
    first=$(echo "${status}" | jq -r '.firstAvailableBlock')
    if [[ "${first}" != "${expected_first}" ]]; then
      echo "::error::serverStatus firstAvailableBlock '${first}' != expected '${expected_first}'"
      failed=1
    fi
  fi
  if [[ -n "${expected_last}" ]]; then
    last=$(echo "${status}" | jq -r '.lastAvailableBlock')
    if [[ "${last}" != "${expected_last}" ]]; then
      echo "::error::serverStatus lastAvailableBlock '${last}' != expected '${expected_last}'"
      failed=1
    fi
  fi
  if [[ -n "${expected_next}" ]]; then
    next=$(echo "${status}" | jq -r '.nextExpectedBlock')
    if [[ "${next}" != "${expected_next}" ]]; then
      echo "::error::serverStatus nextExpectedBlock '${next}' != expected '${expected_next}'"
      failed=1
    fi
  fi
  [[ "${failed}" == 0 ]] || return 1
  echo "OK: serverStatus first=${expected_first:-<skip>} last=${expected_last:-<skip>} next=${expected_next:-<skip>}"
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
