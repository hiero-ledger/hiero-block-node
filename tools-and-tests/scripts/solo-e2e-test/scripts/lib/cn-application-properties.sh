#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Shared generator for the CN application.properties override file.
# Sourced by solo-deploy-network.sh (deploy-time) and
# wrb-distribution/cn-upgrade-tss.sh (upgrade-time) so both paths
# produce identical properties from a single source of truth.
#
# Reads:
#   TSS_ENABLED  — "true" enables tss.hintsEnabled/historyEnabled/wrapsEnabled
#   WRB_RSA      — "true" appends blockStream.streamWrappedRecordBlocks=true

generate_cn_application_properties() {
  local output_file="${1}"
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
  # cn-upgrade-tss.sh, which is what actually turns TSS on for that scenario —
  # issue #3125 step 11) wants TSS off at genesis but the normal record stream
  # still on, since steps 1-10 consume record files, not WRB.
  if [[ "${TSS_ENABLED}" != "true" ]]; then
    cat >> "${output_file}" << 'EOF'

tss.hintsEnabled=false
tss.historyEnabled=false
tss.forceMockSignatures=false
tss.wrapsEnabled=false
EOF
  else
    cat >> "${output_file}" << 'EOF'

tss.hintsEnabled=true
tss.historyEnabled=true
tss.forceMockSignatures=false
tss.wrapsEnabled=true
EOF
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
