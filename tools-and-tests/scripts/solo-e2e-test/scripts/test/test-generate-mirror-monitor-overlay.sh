#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Regression check for generate_mirror_monitor_overlay's TPS/enabled/scenario-type logic
# in solo-deploy-network.sh. Solo's own `--pinger` flag silently no-ops the
# monitor.enabled/pinger.tps wiring for mirror-node-version >= 0.152.0 (its
# hasMirrorNodeMemoryImprovements gate skips those --set flags entirely past
# that version) -- this function bypasses that via a values-file override
# instead. Confirmed via a real CI run that MIRROR_NODE_PINGER_TPS had zero
# effect (mirror-tx-presence: absent) at TPS=100/1000 before this fix. Also
# overrides pinger's `type` (Solo's own bundled defaults hardcode CRYPTO_TRANSFER,
# not the monitor app's real CONSENSUS_SUBMIT_MESSAGE default) -- without this,
# even a healthy, correctly-enabled monitor would submit traffic
# indistinguishable from NLG's own CryptoTransfer.
#
# Runs without a cluster: extracts the function via sed (same approach as
# test-execute-load-start.sh) and writes to a real temp file, then parses it
# back with yq to check the actual values rather than string-matching YAML text.
#
# Exit 0 on all-pass, 1 on any failure.

set -u -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_SCRIPT="${SCRIPT_DIR}/../solo-deploy-network.sh"

passed=0
failed=0
function pass { echo "  PASS  $1"; passed=$((passed+1)); }
function fail { echo "  FAIL  $1"; failed=$((failed+1)); }

eval "$(sed -n '/^function generate_mirror_monitor_overlay/,/^}/p' "${DEPLOY_SCRIPT}")"

if ! declare -f generate_mirror_monitor_overlay >/dev/null; then
    echo "FATAL: could not extract generate_mirror_monitor_overlay from ${DEPLOY_SCRIPT}"
    exit 1
fi

tmpfile="$(mktemp)"
trap 'rm -f "${tmpfile}"' EXIT

# ----------------------------------------------------------------------------
echo "[1] generate_mirror_monitor_overlay defaults to tps=5, enabled=true when MIRROR_NODE_PINGER_TPS is unset"
unset MIRROR_NODE_PINGER_TPS
generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
tps="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.pinger.tps' "${tmpfile}")"
enabled="$(yq '.monitor.enabled' "${tmpfile}")"
if [[ "${tps}" == "5" && "${enabled}" == "true" ]]; then
    pass "unset -> tps=5, enabled=true"
else
    fail "expected tps=5 enabled=true, got tps=${tps} enabled=${enabled}"
fi

# ----------------------------------------------------------------------------
echo "[2] generate_mirror_monitor_overlay passes through an arbitrary TPS value"
MIRROR_NODE_PINGER_TPS=1000 generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
tps="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.pinger.tps' "${tmpfile}")"
enabled="$(yq '.monitor.enabled' "${tmpfile}")"
if [[ "${tps}" == "1000" && "${enabled}" == "true" ]]; then
    pass "MIRROR_NODE_PINGER_TPS=1000 -> tps=1000, enabled=true"
else
    fail "expected tps=1000 enabled=true, got tps=${tps} enabled=${enabled}"
fi

# ----------------------------------------------------------------------------
echo "[3] generate_mirror_monitor_overlay disables monitor when TPS is 0"
MIRROR_NODE_PINGER_TPS=0 generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
tps="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.pinger.tps' "${tmpfile}")"
enabled="$(yq '.monitor.enabled' "${tmpfile}")"
if [[ "${tps}" == "0" && "${enabled}" == "false" ]]; then
    pass "MIRROR_NODE_PINGER_TPS=0 -> tps=0, enabled=false"
else
    fail "expected tps=0 enabled=false, got tps=${tps} enabled=${enabled}"
fi

# ----------------------------------------------------------------------------
echo "[4] generate_mirror_monitor_overlay produces valid, parseable YAML"
MIRROR_NODE_PINGER_TPS=42 generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
if yq '.' "${tmpfile}" >/dev/null 2>&1; then
    pass "output is valid YAML"
else
    fail "output is not valid YAML"
fi

# ----------------------------------------------------------------------------
echo "[5] generate_mirror_monitor_overlay overrides pinger's type to CONSENSUS_SUBMIT_MESSAGE with a real topicId/messageSize"
generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
scenario_type="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.pinger.type' "${tmpfile}")"
topic_id="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.pinger.properties.topicId' "${tmpfile}")"
message_size="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.pinger.properties.messageSize' "${tmpfile}")"
if [[ "${scenario_type}" == "CONSENSUS_SUBMIT_MESSAGE" && "${topic_id}" == '${topic.ping}' && "${message_size}" == "1024" ]]; then
    pass "type=CONSENSUS_SUBMIT_MESSAGE, topicId=\${topic.ping} (auto-bootstrap expression), messageSize=1024"
else
    fail "expected type=CONSENSUS_SUBMIT_MESSAGE topicId=\${topic.ping} messageSize=1024, got type=${scenario_type} topicId=${topic_id} messageSize=${message_size}"
fi

# ----------------------------------------------------------------------------
echo "[6] generate_mirror_monitor_overlay raises monitor's resource limits above Solo's default (500m/1000Mi), which OOMKilled the pod twice this session"
generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
mem_limit="$(yq '.monitor.resources.limits.memory' "${tmpfile}")"
mem_request="$(yq '.monitor.resources.requests.memory' "${tmpfile}")"
if [[ "${mem_limit}" == "2Gi" && "${mem_request}" == "512Mi" ]]; then
    pass "memory limit=2Gi, request=512Mi (above Solo's 1000Mi/0 default)"
else
    fail "expected memory limit=2Gi request=512Mi, got limit=${mem_limit} request=${mem_request}"
fi

# ----------------------------------------------------------------------------
echo "[7] generate_mirror_monitor_overlay adds the xfer (CryptoTransfer) scenario when MIRROR_NODE_XFER_TPS is set"
MIRROR_NODE_PINGER_TPS=5000 MIRROR_NODE_XFER_TPS=5000 generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
xfer_tps="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.xfer.tps' "${tmpfile}")"
xfer_type="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.xfer.type' "${tmpfile}")"
xfer_sender="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.xfer.properties.senderAccountId' "${tmpfile}")"
xfer_recipient="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios.xfer.properties.recipientAccountId' "${tmpfile}")"
if [[ "${xfer_tps}" == "5000" && "${xfer_type}" == "CRYPTO_TRANSFER" && "${xfer_sender}" == "0.0.2" && "${xfer_recipient}" == "0.0.55" ]]; then
    pass "xfer.tps=5000, type=CRYPTO_TRANSFER, sender=0.0.2, recipient=0.0.55"
else
    fail "expected xfer tps=5000 type=CRYPTO_TRANSFER sender=0.0.2 recipient=0.0.55, got tps=${xfer_tps} type=${xfer_type} sender=${xfer_sender} recipient=${xfer_recipient}"
fi

# ----------------------------------------------------------------------------
echo "[8] generate_mirror_monitor_overlay omits the xfer scenario entirely when MIRROR_NODE_XFER_TPS is unset/0"
unset MIRROR_NODE_XFER_TPS
MIRROR_NODE_PINGER_TPS=100 generate_mirror_monitor_overlay "${tmpfile}" >/dev/null
xfer_present="$(yq '.monitor.config.hiero.mirror.monitor.publish.scenarios | has("xfer")' "${tmpfile}")"
if [[ "${xfer_present}" == "false" ]]; then
    pass "xfer scenario key absent when MIRROR_NODE_XFER_TPS is unset"
else
    fail "expected xfer scenario key absent, got has(xfer)=${xfer_present}"
fi

echo
echo "RESULT: ${passed} passed, ${failed} failed"
[[ $failed -eq 0 ]]
