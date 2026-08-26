#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# WRB Distribution E2E (#3125 slice 4 — step 7) — install a second Mirror Node
# (mirror-2) into the Solo namespace, pointing at BN2 and BN3 as block-node
# sources with startBlockNumber=1 (the first WRB block; see the startBlockNumber
# section below for why 0 fails and why BN2.lastAvailableBlock is not used).
#
# We do NOT use `solo mirror node add` because Solo 0.79 doesn't support a
# second Mirror Node in an existing deployment — the CLI re-installs the same
# "mirror" release rather than creating a new one. Instead we render a minimal
# raw-K8s manifest (postgres + importer + service) modelled on the proven
# recipe in wrb-sequential-comparison.sh's deploy_mn2_to_bn2 function.
#
# Scope for slice 4: importer + REST (no gRPC/Web3 sidecars). MN v0.157.1 has
# no `hiero.mirror.importer.block.verification.enabled` toggle, and
# BlockStreamVerifier.verifyWrappedRecordBlockSignature runs unconditionally
# for every block regardless of whether the network is on RSA or TSS signing --
# it is NOT a no-op (see the address-book bootstrap step below, added once this
# was discovered crashing the pod on block 0). The step 7 assertion only needs
# to observe "MN2 is consuming blocks from a BN source" via the importer pod's
# block-source env vars + Ready state.
#
# REST was added in slice 6 (step 12) so assert-cutover-sync.sh can query MN2's
# actual last-available block over its standard REST API, the same way it
# already queries mirror-1 -- without it, MN2 can never be observed converging
# at all, since nothing else exposes an equivalent "last block" endpoint.
#
# Reads:
#   NAMESPACE         (default "solo-network")
#   CLUSTER_REFERENCE (default "kind-solo-cluster")
#   MN_VERSION        (used for the importer image tag; default "latest")
#   BN_HOST_2         (default block-node-2.${NAMESPACE}.svc.cluster.local)
#   BN_HOST_3         (default block-node-3.${NAMESPACE}.svc.cluster.local)
#   BN2_GRPC_PORT     (default 40841 — matches add-bn.sh's convention:
#                     grpc_port = 40839 + bn_index; BN2 has bn_index=2)
#   MN2_READY_TIMEOUT (default 600)
#   CLI_LIB           (wrb-cli's built lib dir; default resolved from
#                     ${ENV_FILE}, falling back to the repo's standard
#                     :tools:installDist output path)
#   ENV_FILE          (shared env written by install-and-run-wrb-cli.sh;
#                     default "/tmp/wrb-distribution-step12.env")

set -euo pipefail

: "${NAMESPACE:=solo-network}"
: "${CLUSTER_REFERENCE:=kind-solo-cluster}"
: "${BN_HOST_2:=block-node-2.${NAMESPACE}.svc.cluster.local}"
: "${BN_HOST_3:=block-node-3.${NAMESPACE}.svc.cluster.local}"
# BN2's grpc port on the runner is set up by add-bn.sh at 40839+bn_index
# (see the port-forward layout in add-bn.sh L104). Keep the same formula
# here rather than hard-coding the literal, so the derivation stays obvious
# and both scripts can be updated together if the base port ever changes.
: "${BN2_GRPC_PORT:=$((40839 + 2))}"
MN2_READY_TIMEOUT="${MN2_READY_TIMEOUT:-600}"

log() { echo "[wrb-dist-add-mn2] $*"; }
fail() { echo "[wrb-dist-add-mn2] ERROR: $*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
ENV_FILE="${ENV_FILE:-/tmp/wrb-distribution-step12.env}"
[[ -f "${ENV_FILE}" ]] && source "${ENV_FILE}"
: "${CLI_LIB:=${REPO_ROOT}/tools-and-tests/tools/build/install/tools/lib}"

# 1) Resolve the importer image tag.
#    Force the JVM image (gcr.io/mirrornode/hedera-mirror-importer). The default
#    ghcr.io/hiero-ledger/hiero-mirror-node/importer image is GraalVM-native
#    and can't reflect into List<BlockNodeProperties> for BN-source binding —
#    see the same override in network-topology-tool/generate-chart-values-config-overlays.sh.
mn_tag="${MN_VERSION:-latest}"
mn_tag="${mn_tag#v}"
importer_image="gcr.io/mirrornode/hedera-mirror-importer:${mn_tag}"
rest_image="gcr.io/mirrornode/hedera-mirror-rest:${mn_tag}"
log "  Using JVM importer image: ${importer_image}"
log "  Using REST image: ${rest_image}"

# 2) Resolve MN2's startBlockNumber.
# Always start MN2 from block 1 — the first block in the WRB block stream.
# Using startBlockNumber=0 causes MN2's importer to fail with "No block node
# can provide block 0" and never recover (block 0 is treated as a genesis
# sentinel that must always exist; its absence is a fatal error, not a
# wait-and-retry situation).
#
# Using startBlockNumber=BN2.lastAvailableBlock (the approach tried earlier)
# is risky here: BN2 already has CN-pushed live blocks at delay=340 (before
# the CN is reconfigured to push to BN3). Those blocks are cleared by BN2's
# subsequent rollout restarts at delay=460/472/485, leaving BN2 far behind
# MN2's startBlockNumber — MN2 then waits forever for a block it can never
# reach. Starting from 1 is safe: the importer will wait/retry for block 1
# once BN2/BN3 get it via backfill (delay=460+), then import everything
# from 1 onward.
start_block=1
if command -v grpcurl >/dev/null 2>&1; then
    status_json=$(grpcurl -plaintext -d '{}' "localhost:${BN2_GRPC_PORT}" \
        org.hiero.block.api.BlockNodeService/serverStatus 2>/dev/null || echo '{}')
    bn_last=$(echo "${status_json}" | jq -r '.lastAvailableBlock // empty' 2>/dev/null || echo "")
    log "  BN2 lastAvailableBlock=${bn_last:-<empty>} at deploy time (MN2 will always start at block ${start_block})"
else
    log "  grpcurl not on PATH (MN2 will start at block ${start_block})"
fi

# 2b) Generate MN2's initial address book from the Solo CN's real RSA keys.
#
# Without this, MN2's importer starts with a completely empty DB and, on its
# very first block, tries to bootstrap an address book from a classpath
# resource keyed by network name ("addressbook/${HIERO_MIRROR_IMPORTER_NETWORK}")
# -- which doesn't exist for a custom/test network name like "OTHER", so
# BlockStreamVerifier.verifyWrappedRecordBlockSignature crashes the pod with
# "Unable to load bootstrap address book" the moment it tries to verify block
# 0's signature. mirror-1 never hits this because it was bootstrapped natively
# by Solo and already has an address book row from genesis; MN2 is added
# post-hoc with nothing to fall back on.
#
# The fix (same recipe already proven for BN2/BN3's RSA roster in
# reconfigure-bn-roster-bootstrap-rsa.sh, and for wrb-cli's own wrap command in
# install-and-run-wrb-cli.sh): pull the CN's actual RSA keys via
# extract-solo-ab-and-generate.sh, convert to the legacy binary NodeAddressBook
# format Mirror Node expects for `hiero.mirror.importer.initialAddressBook`,
# and mount it into the importer pod. If generation fails for any reason, we
# warn and fall back to the (broken, pre-existing) default rather than aborting
# the whole test run over it.
address_book_b64=""
mn2_ab_json="${TMPDIR:-/tmp}/wrb-dist-mn2-addressbook-history.json"
mn2_ab_bin="${TMPDIR:-/tmp}/wrb-dist-mn2-addressbook.bin"
log "Generating MN2's initial address book from the Solo CN's real RSA keys..."
if bash "${SCRIPT_DIR}/../extract-solo-ab-and-generate.sh" \
    "${NAMESPACE}" "$(date -u +%s).0" "${mn2_ab_json}"; then
    if [[ -d "${CLI_LIB}" ]]; then
        if java -cp "${CLI_LIB}/*" org.hiero.block.tools.BlockStreamTool mirror generateBinFromAddressBookJson \
            "${mn2_ab_json}" -o "${mn2_ab_bin}"; then
            address_book_b64=$(base64 < "${mn2_ab_bin}" | tr -d '\n')
            log "  Generated initial address book binary ($(wc -c < "${mn2_ab_bin}" | tr -d ' ') bytes)"
        else
            log "  WARNING: generateBinFromAddressBookJson failed; MN2 will use the default (broken) bootstrap"
        fi
    else
        log "  WARNING: CLI lib not found at ${CLI_LIB}; MN2 will use the default (broken) bootstrap"
    fi
else
    log "  WARNING: Could not extract address book from CN; MN2 will use the default (broken) bootstrap"
fi

# Only wire up the addressbook Secret + mount + Spring config override when
# generation actually succeeded above; an empty/missing bootstrap file would
# be a strictly worse failure mode (empty-protobuf parse error) than the
# original classpath-resource crash, so an empty address_book_b64 here means
# "emit nothing" and MN2 falls back to its pre-existing (broken) behavior.
mn2_addressbook_secret_yaml=""
mn2_importer_extra_env=""
mn2_importer_volumemounts=""
mn2_importer_volumes=""
if [[ -n "${address_book_b64}" ]]; then
    initial_address_book_app_yaml_b64=$(printf 'hiero:\n  mirror:\n    importer:\n      initialAddressBook: /usr/etc/hiero/addressbook.bin\n' \
        | base64 | tr -d '\n')
    mn2_addressbook_secret_yaml=$(cat <<SECEOF
---
apiVersion: v1
kind: Secret
metadata:
  name: mn2-importer-addressbook
  namespace: ${NAMESPACE}
type: Opaque
data:
  addressbook.bin: ${address_book_b64}
  application.yaml: ${initial_address_book_app_yaml_b64}
SECEOF
)
    mn2_importer_extra_env=$(cat <<ENVEOF
        - name: SPRING_CONFIG_ADDITIONAL_LOCATION
          value: "file:/usr/etc/hiero/"
ENVEOF
)
    mn2_importer_volumemounts=$(cat <<VMEOF
        volumeMounts:
        - name: addressbook
          mountPath: /usr/etc/hiero
          readOnly: true
VMEOF
)
    mn2_importer_volumes=$(cat <<VOLEOF
      volumes:
      - name: addressbook
        secret:
          secretName: mn2-importer-addressbook
VOLEOF
)
fi

# 3) Emit the manifest.
mn2_manifest="${TMPDIR:-/tmp}/wrb-dist-mn2-manifest.yaml"
cat > "${mn2_manifest}" <<EOF
# Generated by add-mn2.sh for WRB distribution E2E (#3125 slice 4 step 7).
# Provides a minimal, self-contained MN2 that pulls blocks from BN2 + BN3,
# bootstrapped with the Solo CN's real address book (see the generation step
# above) so block-signature verification has real keys to check against.
${mn2_addressbook_secret_yaml}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: mn2-postgres-init
  namespace: ${NAMESPACE}
data:
  init.sql: |
    CREATE EXTENSION IF NOT EXISTS btree_gist;
    CREATE EXTENSION IF NOT EXISTS pg_trgm;
    CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
    -- Roles the MN importer's Flyway migrations expect to exist (v1 baseline
    -- references mirror_importer / mirror_grpc / mirror_rest / mirror_web3).
    CREATE ROLE readonly;
    CREATE ROLE readwrite IN ROLE readonly;
    CREATE ROLE temporary_admin IN ROLE readwrite;
    CREATE ROLE mirror_importer;
    CREATE ROLE mirror_grpc;
    CREATE ROLE mirror_rest;
    CREATE ROLE mirror_web3;
    GRANT temporary_admin TO mirror_node;
    GRANT temporary_admin TO mirror_importer;
    CREATE SCHEMA IF NOT EXISTS temporary;
    GRANT ALL PRIVILEGES ON SCHEMA temporary TO mirror_node;
    ALTER DEFAULT PRIVILEGES IN SCHEMA temporary GRANT ALL ON TABLES TO mirror_node;
    ALTER DEFAULT PRIVILEGES IN SCHEMA temporary GRANT ALL ON SEQUENCES TO mirror_node;
    ALTER DATABASE mirror_node SET search_path = public, public, temporary;
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mn2-postgres
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/instance: mirror-2
    app.kubernetes.io/component: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mn2-postgres
  template:
    metadata:
      labels:
        app: mn2-postgres
        app.kubernetes.io/instance: mirror-2
        app.kubernetes.io/component: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:14-alpine
        env:
        - name: POSTGRES_DB
          value: mirror_node
        - name: POSTGRES_USER
          value: mirror_node
        - name: POSTGRES_PASSWORD
          value: mirror_node_pass
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: init-scripts
          mountPath: /docker-entrypoint-initdb.d
      volumes:
      - name: init-scripts
        configMap:
          name: mn2-postgres-init
---
apiVersion: v1
kind: Service
metadata:
  name: mn2-postgres
  namespace: ${NAMESPACE}
spec:
  type: ClusterIP
  selector:
    app: mn2-postgres
  ports:
  - port: 5432
    targetPort: 5432
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mirror-2-importer
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/instance: mirror-2
    app.kubernetes.io/component: importer
    app.kubernetes.io/name: importer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mirror-2-importer
  template:
    metadata:
      labels:
        app: mirror-2-importer
        app.kubernetes.io/instance: mirror-2
        app.kubernetes.io/component: importer
        app.kubernetes.io/name: importer
    spec:
      containers:
      - name: importer
        image: ${importer_image}
        env:
        - name: SPRING_DATASOURCE_URL
          value: "jdbc:postgresql://mn2-postgres:5432/mirror_node?sslmode=disable"
        - name: SPRING_DATASOURCE_USERNAME
          value: "mirror_node"
        - name: SPRING_DATASOURCE_PASSWORD
          value: "mirror_node_pass"
        - name: SPRING_JPA_PROPERTIES_HIBERNATE_HBM2DDL_AUTO
          value: "create"
        - name: HIERO_MIRROR_IMPORTER_NETWORK
          value: "OTHER"
        - name: HIERO_MIRROR_IMPORTER_STARTBLOCKNUMBER
          value: "${start_block}"
        # BlockNodeProperties in mirror-node v0.157.1 is:
        #   endpoints: SortedSet<ServiceEndpoint{host, port, apis, requiresTls}>
        #   priority:  int
        # so the block-node coordinates live under nodes[N].endpoints[0].host/port,
        # not the flat nodes[N].host/port shape earlier versions used.
        - name: HIERO_MIRROR_IMPORTER_BLOCK_ENABLED
          value: "true"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_SOURCETYPE
          value: "BLOCK_NODE"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_PRIORITY
          value: "0"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_ENDPOINTS_0_HOST
          value: "${BN_HOST_2}"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_0_ENDPOINTS_0_PORT
          value: "40840"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_PRIORITY
          value: "0"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_ENDPOINTS_0_HOST
          value: "${BN_HOST_3}"
        - name: HIERO_MIRROR_IMPORTER_BLOCK_NODES_1_ENDPOINTS_0_PORT
          value: "40840"
        - name: HIERO_MIRROR_IMPORTER_DOWNLOADER_BUCKETNAME
          value: "dummy-not-used"
        - name: HIERO_MIRROR_IMPORTER_DOWNLOADER_RECORD_ENABLED
          value: "false"
        - name: HIERO_MIRROR_IMPORTER_DOWNLOADER_BALANCE_ENABLED
          value: "false"
        - name: HIERO_MIRROR_IMPORTER_DB_SCHEMA
          value: "public"
        - name: HIERO_MIRROR_IMPORTER_DB_TEMPSCHEMA
          value: "temporary"
${mn2_importer_extra_env}
${mn2_importer_volumemounts}
${mn2_importer_volumes}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mirror-2-rest
  namespace: ${NAMESPACE}
  labels:
    app.kubernetes.io/instance: mirror-2
    app.kubernetes.io/component: rest
    app.kubernetes.io/name: rest
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mirror-2-rest
  template:
    metadata:
      labels:
        app: mirror-2-rest
        app.kubernetes.io/instance: mirror-2
        app.kubernetes.io/component: rest
        app.kubernetes.io/name: rest
    spec:
      containers:
      - name: rest
        image: ${rest_image}
        ports:
        - containerPort: 5551
          name: http
        env:
        # Same DB credentials as the importer above -- mn2-postgres only has the
        # mirror_node superuser role (its init.sql grants no login/password to the
        # per-service roles like mirror_rest that a real Solo chart deployment
        # would use), so REST connects the same way the importer does.
        - name: HIERO_MIRROR_REST_DB_HOST
          value: "mn2-postgres"
        - name: HIERO_MIRROR_REST_DB_NAME
          value: "mirror_node"
        - name: HIERO_MIRROR_REST_DB_USERNAME
          value: "mirror_node"
        - name: HIERO_MIRROR_REST_DB_PASSWORD
          value: "mirror_node_pass"
        # REST defaults to caching via Redis at 127.0.0.1:6379; without a Redis sidecar
        # (deliberately not deployed here -- this is a test-only mirror node), that
        # connection is refused, and the readiness/liveness health checks fail on it.
        - name: HIERO_MIRROR_REST_REDIS_ENABLED
          value: "false"
        readinessProbe:
          httpGet:
            path: /health/readiness
            port: http
          periodSeconds: 2
          failureThreshold: 60
        livenessProbe:
          httpGet:
            path: /health/liveness
            port: http
          periodSeconds: 2
          failureThreshold: 60
---
apiVersion: v1
kind: Service
metadata:
  name: mirror-2-rest
  namespace: ${NAMESPACE}
spec:
  type: ClusterIP
  selector:
    app: mirror-2-rest
  ports:
  - name: http
    port: 80
    targetPort: 5551
EOF

log "Applying MN2 manifest to namespace ${NAMESPACE}..."
kubectl --context "${CLUSTER_REFERENCE}" apply -f "${mn2_manifest}" \
    || fail "kubectl apply failed for MN2 manifest"

log "Waiting for mn2-postgres pod Ready (timeout ${MN2_READY_TIMEOUT}s)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    wait --for=condition=Ready pod -l app=mn2-postgres \
    --timeout="${MN2_READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe pod -l app=mn2-postgres | tail -40 || true
        fail "mn2-postgres did not become Ready within ${MN2_READY_TIMEOUT}s"
    }

log "Waiting for mirror-2-importer pod Ready (timeout ${MN2_READY_TIMEOUT}s)..."
kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    wait --for=condition=Ready pod -l app=mirror-2-importer \
    --timeout="${MN2_READY_TIMEOUT}s" \
    || {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe pod -l app=mirror-2-importer | tail -60 || true
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            logs -l app=mirror-2-importer --tail=100 || true
        fail "mirror-2-importer did not become Ready within ${MN2_READY_TIMEOUT}s"
    }

# Don't wait for the k8s Ready condition here -- REST's own /health/readiness requires at
# least one imported record_file row, but BN2/BN3 (this MN's block-node sources) don't get
# backfill configured until step 10, well after this script runs. Waiting on Ready would
# always time out at this point in the test regardless of whether the deployment is actually
# healthy. Just confirm the container started; the readinessProbe (and the Service's
# endpoint) will flip healthy on its own once step 10's backfill actually delivers blocks,
# which happens well before step 12's assert-cutover-sync needs to query it.
log "Waiting for mirror-2-rest container to start (timeout ${MN2_READY_TIMEOUT}s)..."
elapsed=0
until [[ "$(kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
    get pod -l app=mirror-2-rest -o jsonpath='{.items[0].status.phase}' 2>/dev/null)" == "Running" ]]; do
    (( elapsed >= MN2_READY_TIMEOUT )) && {
        kubectl --context "${CLUSTER_REFERENCE}" --namespace "${NAMESPACE}" \
            describe pod -l app=mirror-2-rest | tail -60 || true
        fail "mirror-2-rest container did not start within ${MN2_READY_TIMEOUT}s"
    }
    sleep 2
    elapsed=$(( elapsed + 2 ))
done

log "MN2 is Ready (postgres + importer + rest)."
