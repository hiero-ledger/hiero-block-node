#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Render tests for the plugins.profile selector (issue #4045).
#
# Verifies, with `helm template` only (no cluster required):
#   - each named profile (lfh/rfh/all/minimal) resolves to the expected plugin list;
#   - the profile selector and the values-overrides/plugin-profile-*.yaml files agree
#     (single source of truth);
#   - precedence: an explicit non-empty plugins.names overrides a profile;
#   - an explicit empty plugins.names keeps its meaning and a profile does not override it;
#   - an unrecognized plugins.profile fails templating with a clear message;
#   - the plugins.names chart default stays byte-identical to the "lfh" profile.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
CHART=charts/block-node-server
OVERRIDES="${CHART}/values-overrides"
RELEASE=bn-plugins-profile-test

pass=0
fail() { echo "FAIL: $*"; exit 1; }
ok() { echo "OK: $*"; pass=$((pass + 1)); }

# Expected plugin lists, read straight from the profile files (the single source of truth) so
# this test never hardcodes a second copy. The assertions below compare these file-parsed lists
# against what the chart resolves through Helm -- two independent parse paths, so a helper bug is
# still caught while the lists cannot drift from values-overrides/plugin-profile-*.yaml.
profile_names() { sed -n 's/^[[:space:]]*names:[[:space:]]*"\(.*\)"$/\1/p' "${OVERRIDES}/plugin-profile-$1.yaml" | head -n1; }
LFH=$(profile_names lfh)
RFH=$(profile_names rfh)
ALL=$(profile_names all)
MINIMAL=$(profile_names minimal)

# Resolve the effective plugin-name list the chart renders for the given --set/-f args, by
# reading the DESIRED_HASH line of the resolve-plugins init container (maven source), which
# embeds the exact resolved names string as "<appVersion>:<names>:<thirdParty>:<repos>".
resolved_names() {
  # The hash line is: DESIRED_HASH=$(echo "<appVersion>:<names>:<thirdParty>:<repos>" | ...).
  # thirdParty is empty in these tests, so <names> is bounded by the first ':' and the '::[' that
  # opens the (JSON) repos field. Capturing this way avoids splitting on colons inside the JSON.
  helm template "${RELEASE}" "${CHART}" --show-only templates/statefulset.yaml "$@" 2>/dev/null \
    | sed -n 's/.*DESIRED_HASH=$(echo "[^:]*:\(.*\)::\[.*/\1/p' \
    | head -n1
}

assert_names() {
  local label=$1 expected=$2
  shift 2
  local got
  got=$(resolved_names "$@")
  [[ "${got}" == "${expected}" ]] || fail "${label}: expected names [${expected}], got [${got}]"
  ok "${label} resolves to the expected plugin list"
}

echo "--- each profile resolves to its expected list (via --set plugins.profile)"
assert_names "profile=lfh" "${LFH}" --set plugins.profile=lfh
assert_names "profile=rfh" "${RFH}" --set plugins.profile=rfh
assert_names "profile=all" "${ALL}" --set plugins.profile=all
assert_names "profile=minimal" "${MINIMAL}" --set plugins.profile=minimal

echo "--- the override files resolve to the same lists (single source of truth)"
assert_names "override lfh" "${LFH}" -f "${OVERRIDES}/plugin-profile-lfh.yaml"
assert_names "override rfh" "${RFH}" -f "${OVERRIDES}/plugin-profile-rfh.yaml"
assert_names "override all" "${ALL}" -f "${OVERRIDES}/plugin-profile-all.yaml"
assert_names "override minimal" "${MINIMAL}" -f "${OVERRIDES}/plugin-profile-minimal.yaml"

echo "--- chart default (no selector) is byte-identical to the lfh profile"
assert_names "default" "${LFH}"

echo "--- precedence: an explicit non-empty plugins.names overrides a profile"
EXPLICIT="facility-app-state,health,server-status"
assert_names "explicit names beats profile" "${EXPLICIT}" \
  --set plugins.profile=rfh --set-string plugins.names="facility-app-state\,health\,server-status"

echo "--- precedence: a profile does not override an intentional empty plugins.names"
empty_out=$(helm template "${RELEASE}" "${CHART}" --show-only templates/statefulset.yaml \
  --set plugins.profile=rfh --set-string plugins.names="" 2>/dev/null || true)
grep -q "resolve-plugins" <<<"${empty_out}" \
  && fail "intentional empty: resolve-plugins init container rendered (profile overrode empty)"
grep -q "cloud-storage-archive" <<<"${empty_out}" \
  && fail "intentional empty: rfh plugins leaked into the render"
ok "intentional empty plugins.names is respected over a profile"

echo "--- an unrecognized profile fails templating with a clear message"
if out=$(helm template "${RELEASE}" "${CHART}" --set plugins.profile=bogus 2>&1); then
  fail "unknown profile: render succeeded, expected failure"
fi
grep -q 'plugins.profile "bogus" is not recognized' <<<"${out}" \
  || fail "unknown profile: message did not name the bad value; got: ${out}"
grep -q "all, lfh, minimal, rfh" <<<"${out}" \
  || fail "unknown profile: message did not list valid profiles; got: ${out}"
ok "unknown profile fails loudly and lists the valid profiles"

echo "--- image source routes the resolved names into BN_PLUGINS"
bn_plugins=$(helm template "${RELEASE}" "${CHART}" --show-only templates/statefulset.yaml \
  -f "${OVERRIDES}/full.yaml" --set plugins.profile=minimal 2>/dev/null \
  | sed -n 's/.*value: "\(.*\)"/\1/p' | grep -F "facility-app-state" | head -n1)
[[ "${bn_plugins}" == "${MINIMAL}" ]] \
  || fail "image source: BN_PLUGINS expected [${MINIMAL}], got [${bn_plugins}]"
ok "image source resolves BN_PLUGINS through the profile selector"

echo "PASS (${pass} assertions)"
