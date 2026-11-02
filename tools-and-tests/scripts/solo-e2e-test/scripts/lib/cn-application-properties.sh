#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Shared generator for the CN application.properties override file.
# Sourced by solo-deploy-network.sh (deploy-time) and
# wrb-distribution/cn-upgrade-tss.sh (upgrade-time) so both paths
# produce identical properties from a single source of truth, and by
# resolve-versions.sh for the Nova WRAPS decision (cn_is_nova_wraps).
#
# Reads:
#   TSS_ENABLED                "true" enables tss.hintsEnabled/historyEnabled (and, for a CN
#                              older than the Nova WRAPS library, tss.wrapsEnabled and
#                              tss.wrapsProvingKeyDownloadEnabled)
#   WRB_RSA                    "true" appends blockStream.streamWrappedRecordBlocks=true
#   CN_NOVA_WRAPS              see cn_is_nova_wraps
#   CN_NOVA_WRAPS_MIN_VERSION  see cn_is_nova_wraps
#
# Usage: generate_cn_application_properties <output-file> [<cn-version>]
#   The optional CN version decides whether the tss.wraps* properties are written
#   (see cn_needs_wraps_proving_key); without it they are written, as before.

# shellcheck source=semver.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/semver.sh"

# The first Consensus Node release on the Nova WRAPS TSS library (hiero-consensus-node#27506,
# cherry-picked into CN 0.79.0). Such a node signs every block with the same block proof
# layout, needs no WRAPS proving key staged, and no longer defines the tss.wrapsEnabled and
# tss.wrapsProvingKey* properties.
# TODO(#3689): confirm once CN 0.79.0 is released.
CN_NOVA_WRAPS_MIN_VERSION="${CN_NOVA_WRAPS_MIN_VERSION:-0.79.0}"

# cn_is_nova_wraps <cn-version>
# Returns 0 (true) when the Consensus Node is on the Nova WRAPS TSS library.
#   CN_NOVA_WRAPS=true   it is
#   CN_NOVA_WRAPS=false  it is not
#   CN_NOVA_WRAPS unset  decided from <cn-version>: a version at or above
#                        CN_NOVA_WRAPS_MIN_VERSION is, except that a pre-release counts as
#                        earlier while the minimum is a release. The CN 0.79.0 and 0.80.0
#                        pre-releases published so far (alpha) are on the earlier library,
#                        although 0.80.0-alpha.N sorts above 0.79.0. Lowering
#                        CN_NOVA_WRAPS_MIN_VERSION to a pre-release (for example 0.79.0-rc.3)
#                        counts the versions from it, pre-releases included.
# A CN built from a branch reports that branch's version.txt (for example 0.79.0-SNAPSHOT
# on CN main), which says nothing about the library it carries, so such a build needs
# CN_NOVA_WRAPS set explicitly. A value that is not a version (empty, or a keyword such
# as "rc") counts as earlier.
# Used by cn_needs_wraps_proving_key and by resolve-versions.sh (compatibility tier, CN floor).
cn_is_nova_wraps() {
  local cn_version="${1:-}"
  local nova
  case "${CN_NOVA_WRAPS:-}" in
    true)
      nova="true"
      ;;
    false)
      nova="false"
      ;;
    "")
      if [[ ! "${CN_NOVA_WRAPS_MIN_VERSION}" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+ ]]; then
        echo "ERROR: CN_NOVA_WRAPS_MIN_VERSION must be a version such as 0.79.0, got '${CN_NOVA_WRAPS_MIN_VERSION}'" >&2
        exit 1
      fi
      if [[ ! "${cn_version}" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+ ]]; then
        nova="false"
      elif [[ "$(compare_semver "${cn_version}" "${CN_NOVA_WRAPS_MIN_VERSION}")" == "-1" ]]; then
        nova="false"
      elif [[ "${cn_version}" == *-* && "${CN_NOVA_WRAPS_MIN_VERSION}" != *-* ]]; then
        nova="false"
      else
        nova="true"
      fi
      ;;
    *)
      echo "ERROR: CN_NOVA_WRAPS must be 'true', 'false' or unset, got '${CN_NOVA_WRAPS}'" >&2
      exit 1
      ;;
  esac
  [[ "${nova}" == "true" ]]
}

# cn_needs_wraps_proving_key <cn-version>
# Returns 0 (true) when the Consensus Node predates the Nova WRAPS TSS library (see
# cn_is_nova_wraps), so Solo must stage the WRAPS proving key for it and its
# application.properties must carry the tss.wrapsEnabled and tss.wrapsProvingKeyDownloadEnabled
# properties.
cn_needs_wraps_proving_key() {
  if cn_is_nova_wraps "${1:-}"; then
    return 1
  else
    return 0
  fi
}

generate_cn_application_properties() {
  local output_file="${1}"
  local cn_version="${2:-}"
  cat > "${output_file}" << 'EOF'
hedera.config.version=0
ledger.id=0x01
netty.mode=TEST
contracts.chainId=298
hedera.recordStream.logPeriod=1
balances.exportPeriodSecs=400
files.maxSizeKb=2048
hedera.recordStream.compressFilesOnCreation=true
balances.compressOnCreation=true
contracts.maxNumWithHapiSigsAccess=0
autoRenew.targetTypes=
nodes.gossipFqdnRestricted=false
hedera.profiles.active=TEST
nodes.updateAccountIdAllowed=true
blockStream.streamMode=BOTH
# TODO: we can remove this after we no longer need less than v0.59.x
networkAdmin.exportCandidateRoster=true
# for v0.59+, write the network.json file when you freeze the network
networkAdmin.diskNetworkExport=ONLY_FREEZE_BLOCK
hedera.realm=0
hedera.shard=0
nodes.webProxyEndpointsEnabled=true
nodes.nodeRewardsEnabled=false

blockStream.writerMode=FILE_AND_GRPC

blockNode.connectionStallThresholdMillis=5000
EOF

  # TSS on/off is independent of WRB streaming: rsa-wrb (WRB_RSA) always
  # implies TSS_ENABLED=false (see load_topology) and additionally wants WRB
  # streaming instead of the normal record stream, whereas a plain
  # --tss-enabled false deploy (e.g. to test a mid-life TSS cutover via
  # cn-upgrade-tss.sh, which is what actually turns TSS on for that scenario, see
  # issue #3125 step 11) wants TSS off at genesis but the normal record stream
  # still on, since steps 1-10 consume record files, not WRB.
  # A CN on the Nova WRAPS library no longer defines the tss.wraps* properties, so they
  # are only written for an older CN (cn_needs_wraps_proving_key).
  if [[ "${TSS_ENABLED}" != "true" ]]; then
    cat >> "${output_file}" << 'EOF'

tss.hintsEnabled=false
tss.historyEnabled=false
tss.forceMockSignatures=false
EOF
    if cn_needs_wraps_proving_key "${cn_version}"; then
      cat >> "${output_file}" << 'EOF'
tss.wrapsEnabled=false
EOF
    fi
  else
    cat >> "${output_file}" << 'EOF'

tss.hintsEnabled=true
tss.historyEnabled=true
tss.forceMockSignatures=false
EOF
    if cn_needs_wraps_proving_key "${cn_version}"; then
      cat >> "${output_file}" << 'EOF'
tss.wrapsEnabled=true
# CN <= v0.77.x defaults this to false, which also skips verifying and
# extracting the tarball Solo pre-stages at data/keys/wraps.tar.gz, so WRAPS
# never starts. Its hash matches tss.wrapsProvingKeyHash, so nothing is downloaded.
tss.wrapsProvingKeyDownloadEnabled=true
EOF
    fi
  fi

  if [[ "${WRB_RSA}" == "true" ]]; then
    cat >> "${output_file}" << 'EOF'

blockStream.streamWrappedRecordBlocks=true
EOF
  else
    cat >> "${output_file}" << 'EOF'

blockStream.streamWrappedRecordBlocks=false
EOF
  fi
}
