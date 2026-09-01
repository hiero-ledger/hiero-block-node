#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# backfill-wrb-to-bn.sh -- seed a Block Node deployment with a local
# wrappedBlocks archive. Three modes:
#
#   --install-and-seed
#                   (recommended one-shot for a fresh box)
#                   Runs `sudo solo-provisioner block node install` to
#                   create the PVCs + everything, immediately scales the
#                   StatefulSet to 0 so nothing writes to the archive
#                   PVC yet, seeds via the pre-install flow, then scales
#                   the StatefulSet back to 1. Operator ends up with a
#                   BN serving pre-seeded historical blocks after one
#                   script invocation. Requires --config and --values
#                   files (passed through to solo-provisioner).
#
#   --pre-install   (recommended when Solo Provisioner has already been
#                    invoked but the BN pod is scaled down / absent)
#                   Copy WRB zips directly into the PV's host-path
#                   backing BEFORE the BN pod starts. The BN then picks
#                   them up on first startup via BlockFileHistoricPlugin's
#                   scan, with no pod restart, no wipe risk, and hard-link
#                   speed when the source and PV share a filesystem.
#                   Requires the PVCs pre-provisioned + bound but the BN
#                   pod not yet running.
#
#   --post-install  Legacy path -- tar-stream into a running pod, roll the
#                   StatefulSet, plugin re-scans on restart. Only safe when
#                   the archive PVC is real (mounted as a proper volume);
#                   silently loses data when the mount degrades to the
#                   pod's ephemeral rootfs.
#
#   (default: auto-detect -- pre-install if pod isn't Running, else
#   post-install. --install-and-seed must be explicit.)
#
# Pre-install flow (the fast, safe one):
#
#   1. Preflight: PVCs exist + Bound; StatefulSet is scaled 0 (or missing).
#   2. Locate the archive PV's host path via `kubectl get pv <name> -o
#      jsonpath='{.spec.hostPath.path}{.spec.local.path}'`.
#   3. Stage via `blocks bulk-load` (uses the CLI's own file selection +
#      resumability) into a staging dir on the same filesystem as the PV
#      host path (so step 4 can hard-link).
#   4. Copy the staged files into the PV host path -- `cp -al` when the
#      staging and PV paths are on the same filesystem (near-instant, no
#      double space), `cp -r` otherwise. Chown to hedera:hedera (UID/GID
#      2000, the BN's runtime user).
#   5. Print the "next: install the BN" instruction. The script deliberately
#      does NOT start the BN itself -- deploy mechanics (Solo Provisioner
#      vs plain Helm) are the operator's call.
#
# Post-install flow (the tar-stream path):
#
#   1. Preflight: pod Ready.
#   2. Stage via `blocks bulk-load` into a local staging dir.
#   3. `tar -C <staging> -cf - . | kubectl exec -i ... -- tar xf - -C
#      <historic-mount>`.
#   4. `kubectl rollout restart statefulset/...`, wait for pod Ready.
#   5. Cleanup + verification instructions.
#
# Prerequisites:
#   * `kubectl` on PATH; current context pointed at the target cluster
#     (or override via BN_KUBE_CONTEXT)
#   * `java` on PATH
#   * A shaded wrb-cli jar (`tools-*-all.jar`) findable, or set CLI_JAR
#   * For --pre-install: root or hedera-owned write access to the PV host
#     path (script uses `sudo` for the copy + chown if not already root)
#
# Usage:
#   ./backfill-wrb-to-bn.sh [--pre-install|--post-install] <wrappedBlocks-dir>
#
# Environment overrides (all modes):
#   BN_KUBE_CONTEXT       kubectl context           (default: current context)
#   BN_NAMESPACE          Kubernetes namespace      (default: block-node)
#   BN_STATEFULSET        StatefulSet name          (default: block-node-block-node-server)
#   BN_POD                Pod name                  (default: <BN_STATEFULSET>-0)
#   BN_CONTAINER          Container name in pod     (default: block-node-server)
#   STAGING_DIR           Local staging dir         (default: /tmp/bn-backfill-<pid>)
#   CLI_JAR               Path to wrb-cli jar       (default: hunt for tools-*-all.jar)
#
# --pre-install specific:
#   ARCHIVE_PVC_NAME      PVC name for the archive  (default: archive-storage-pvc)
#   FORCE_COPY            "true" -> always `cp -r`, (default: false, i.e.
#                         never `cp -al`             auto-detect same-fs)
#
# --post-install specific:
#   HISTORIC_MOUNT_PATH   On-pod historic dir       (default: /opt/hiero/block-node/data/historic)
#   READY_TIMEOUT         Pod-ready wait (seconds)  (default: 300)
#   SKIP_ROLLOUT          "true" -> skip pod        (default: false)
#                         restart
#   KEEP_STAGING          "true" -> keep staging    (default: false)
#                         dir on success
#
# Examples:
#
#   # Fresh install, previewnet Tier 1 -- seed archive BEFORE bringing up the BN
#   ./backfill-wrb-to-bn.sh --pre-install ~/wrappedBlocks
#   sudo solo-provisioner block node install -p previewnet --config ... --values ...
#
#   # Already-running BN, tolerate the tar-stream+restart cycle
#   ./backfill-wrb-to-bn.sh --post-install ~/wrappedBlocks

set -euo pipefail

# --- Config ---
: "${BN_KUBE_CONTEXT:=}"
: "${BN_NAMESPACE:=block-node}"
: "${BN_STATEFULSET:=block-node-block-node-server}"
: "${BN_POD:=${BN_STATEFULSET}-0}"
: "${BN_CONTAINER:=block-node-server}"
: "${STAGING_DIR:=/tmp/bn-backfill-$$}"
: "${ARCHIVE_PVC_NAME:=archive-storage-pvc}"
: "${HISTORIC_MOUNT_PATH:=/opt/hiero/block-node/data/historic}"
: "${READY_TIMEOUT:=300}"
: "${SKIP_ROLLOUT:=false}"
: "${KEEP_STAGING:=false}"
: "${FORCE_COPY:=false}"
BN_HEDERA_UID="${BN_HEDERA_UID:-2000}"
BN_HEDERA_GID="${BN_HEDERA_GID:-2000}"

# --- Logging ---
log()  { echo "[backfill-wrb-to-bn] $*"; }
fail() { echo "[backfill-wrb-to-bn] ERROR: $*" >&2; exit 1; }

# --- Argument parsing ---
MODE="auto"
WRB_SOURCE_DIR=""
# --install-and-seed passthrough flags to solo-provisioner:
SOLO_PROFILE=""
SOLO_CONFIG=""
SOLO_VALUES=""
SOLO_EXTRA_FLAGS=(--skip-hardware-checks --non-interactive)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --install-and-seed) MODE="install-and-seed"; shift ;;
        --pre-install)      MODE="pre-install"; shift ;;
        --post-install)     MODE="post-install"; shift ;;
        --auto)             MODE="auto"; shift ;;
        --profile)          SOLO_PROFILE="$2"; shift 2 ;;
        --config)           SOLO_CONFIG="$2"; shift 2 ;;
        --values)           SOLO_VALUES="$2"; shift 2 ;;
        --solo-flag)        SOLO_EXTRA_FLAGS+=("$2"); shift 2 ;;
        -h|--help)          sed -n '2,90p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        --*)                fail "unknown flag: $1" ;;
        *)                  [[ -z "$WRB_SOURCE_DIR" ]] || fail "unexpected arg: $1"
                            WRB_SOURCE_DIR="$1"; shift ;;
    esac
done
[[ -n "$WRB_SOURCE_DIR" ]] \
    || fail "usage: $0 [--install-and-seed --profile <p> --config <c> --values <v>|--pre-install|--post-install] <wrappedBlocks-dir>"
[[ -d "$WRB_SOURCE_DIR" ]] || fail "wrappedBlocks dir does not exist: $WRB_SOURCE_DIR"

if [[ "$MODE" == "install-and-seed" ]]; then
    [[ -n "$SOLO_PROFILE" ]] || fail "--install-and-seed requires --profile <p> (e.g. previewnet)"
    [[ -n "$SOLO_CONFIG"  ]] || fail "--install-and-seed requires --config <path-to-provisioner-config.yaml>"
    [[ -n "$SOLO_VALUES"  ]] || fail "--install-and-seed requires --values <path-to-values.yaml>"
    [[ -f "$SOLO_CONFIG"  ]] || fail "--config file does not exist: $SOLO_CONFIG"
    [[ -f "$SOLO_VALUES"  ]] || fail "--values file does not exist: $SOLO_VALUES"
fi

# --- Locate the wrb-cli jar ---
if [[ -z "${CLI_JAR:-}" ]]; then
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    candidate=$(ls "${script_dir}/../build/libs/"tools-*-all.jar 2>/dev/null | head -1 || true)
    [[ -n "$candidate" ]] || candidate=$(ls tools-*-all.jar 2>/dev/null | head -1 || true)
    [[ -n "$candidate" ]] || fail "wrb-cli jar not found; set CLI_JAR=<path> to override"
    CLI_JAR="$candidate"
fi
[[ -f "$CLI_JAR" ]] || fail "CLI_JAR does not exist: $CLI_JAR"

# --- kubectl invocation prefix ---
kctl=(kubectl --namespace "${BN_NAMESPACE}")
[[ -n "${BN_KUBE_CONTEXT}" ]] && kctl+=(--context "${BN_KUBE_CONTEXT}")

# --- Common preflight ---
command -v kubectl >/dev/null || fail "kubectl not on PATH"
command -v java    >/dev/null || fail "java not on PATH"

# --- Auto-detect mode ---
if [[ "$MODE" == "auto" ]]; then
    if "${kctl[@]}" get pod "$BN_POD" >/dev/null 2>&1 \
       && "${kctl[@]}" get pod "$BN_POD" -o jsonpath='{.status.phase}' 2>/dev/null | grep -q Running; then
        MODE="post-install"
        log "Auto-detected mode: post-install (pod $BN_POD is Running)"
    else
        MODE="pre-install"
        log "Auto-detected mode: pre-install (pod $BN_POD not Running or missing)"
    fi
fi

log "Config:"
log "  mode:                  $MODE"
log "  source WRB dir:        $WRB_SOURCE_DIR"
log "  staging dir:           $STAGING_DIR"
log "  CLI jar:               $CLI_JAR"
log "  kubectl context:       ${BN_KUBE_CONTEXT:-<current>}"
log "  namespace:             $BN_NAMESPACE"

# ---------- PRE-INSTALL MODE ----------
run_pre_install() {
    log "  archive PVC name:      $ARCHIVE_PVC_NAME"
    log "  force copy:            $FORCE_COPY"

    # 1. Verify PVC exists + Bound
    log "Preflight: verifying $ARCHIVE_PVC_NAME is Bound..."
    local pvc_status pv_name
    pvc_status=$("${kctl[@]}" get pvc "$ARCHIVE_PVC_NAME" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    [[ "$pvc_status" == "Bound" ]] || fail "PVC $ARCHIVE_PVC_NAME is '${pvc_status:-missing}', expected Bound"
    pv_name=$("${kctl[@]}" get pvc "$ARCHIVE_PVC_NAME" -o jsonpath='{.spec.volumeName}')
    [[ -n "$pv_name" ]] || fail "could not resolve PV name for $ARCHIVE_PVC_NAME"
    log "  Bound to PV: $pv_name"

    # 2. Verify StatefulSet scaled 0 (or absent) so no BN process is holding the PVC
    local replicas
    replicas=$("${kctl[@]}" get statefulset "$BN_STATEFULSET" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo "")
    if [[ -n "$replicas" && "$replicas" != "0" ]]; then
        fail "StatefulSet $BN_STATEFULSET has replicas=$replicas; scale to 0 first: kubectl scale statefulset/$BN_STATEFULSET --replicas=0 -n $BN_NAMESPACE"
    fi

    # 3. Locate PV host path (hostPath OR local)
    local pv_host_path
    pv_host_path=$(kubectl get pv "$pv_name" -o jsonpath='{.spec.hostPath.path}')
    [[ -n "$pv_host_path" ]] || pv_host_path=$(kubectl get pv "$pv_name" -o jsonpath='{.spec.local.path}')
    [[ -n "$pv_host_path" ]] || fail "PV $pv_name is not a hostPath / local volume (nfs/gce-pd/... requires --post-install mode)"
    log "  PV host path:          $pv_host_path"

    # 4. Ensure the host path exists locally (we're running on the same box)
    [[ -d "$pv_host_path" ]] || fail "PV host path $pv_host_path is not present on this host -- are you running the script on the correct node?"

    # 5. Stage via bulk-load. Same-fs check for hard-link path.
    local source_fs staging_fs
    source_fs=$(stat -c %d "$WRB_SOURCE_DIR")
    # Try to put staging on the same fs as the PV host path so we can hard-link
    if [[ "$FORCE_COPY" != "true" ]] && [[ "$(dirname "$pv_host_path")" != "/" ]]; then
        alt_staging="$(dirname "$pv_host_path")/.backfill-staging-$$"
        mkdir -p "$alt_staging" 2>/dev/null && STAGING_DIR="$alt_staging" \
            || log "  (fell back to $STAGING_DIR; couldn't create staging under $(dirname "$pv_host_path"))"
    fi
    mkdir -p "$STAGING_DIR"
    staging_fs=$(stat -c %d "$STAGING_DIR")
    log "  staging fs device:     $staging_fs"
    log "  pv-host-path fs dev:   $(stat -c %d "$pv_host_path")"

    log "Staging via 'blocks bulk-load' ($WRB_SOURCE_DIR -> $STAGING_DIR)..."
    java -jar "$CLI_JAR" blocks bulk-load --source "$WRB_SOURCE_DIR" --dest "$STAGING_DIR" \
        || fail "'blocks bulk-load' staging failed"

    local staged_count
    staged_count=$(find "$STAGING_DIR" -name '*.zip' | wc -l)
    [[ "$staged_count" -gt 0 ]] || fail "no .zip files staged; nothing to seed"
    log "  Staged $staged_count zip file(s)."

    # 6. Copy or hard-link into the PV host path
    local pv_fs
    pv_fs=$(stat -c %d "$pv_host_path")
    local cp_flags="-r"
    if [[ "$FORCE_COPY" != "true" ]] && [[ "$staging_fs" == "$pv_fs" ]]; then
        cp_flags="-al"
        log "  same filesystem detected -> hard-linking (cp -al)"
    else
        log "  different filesystems (or --copy forced) -> full copy (cp -r)"
    fi

    local sudo_prefix=""
    [[ $EUID -eq 0 ]] || sudo_prefix="sudo"
    log "Copying into $pv_host_path (this may take a while for large archives)..."
    $sudo_prefix cp $cp_flags "$STAGING_DIR"/. "$pv_host_path/" \
        || fail "copy into $pv_host_path failed"

    log "Setting ownership to ${BN_HEDERA_UID}:${BN_HEDERA_GID} (hedera:hedera on the BN container)..."
    $sudo_prefix chown -R "${BN_HEDERA_UID}:${BN_HEDERA_GID}" "$pv_host_path/" \
        || fail "chown of $pv_host_path failed"

    # 7. Cleanup (only if we hard-linked -- otherwise staging is a real duplicate,
    #    but we've been given no signal about whether the operator wants to keep it)
    if [[ "$KEEP_STAGING" == "true" ]]; then
        log "KEEP_STAGING=true; leaving staging dir at $STAGING_DIR"
    else
        rm -rf "$STAGING_DIR"
        log "Removed staging dir $STAGING_DIR."
    fi

    log ""
    log "Pre-seed complete. Next: start the BN (do NOT restart if already running)."
    log "  Solo Provisioner (fresh install):"
    log "    sudo solo-provisioner block node install -p <profile> --config ... --values ..."
    log "  Or scale a scaled-down StatefulSet back up:"
    log "    kubectl scale statefulset/$BN_STATEFULSET --replicas=1 -n $BN_NAMESPACE"
    log ""
    log "Verify blocks visible after BN startup:"
    log "  kubectl port-forward -n $BN_NAMESPACE svc/$BN_STATEFULSET 18082:40982 &"
    log "  grpcurl -plaintext -emit-defaults -import-path <proto-dir> \\"
    log "    -proto block-node/api/node_service.proto -d '{}' \\"
    log "    localhost:18082 org.hiero.block.api.BlockNodeService/serverStatus"
    log "  Expect firstAvailableBlock: '0', lastAvailableBlock: <real number>"
}

# ---------- POST-INSTALL MODE ----------
run_post_install() {
    log "  target pod:            $BN_POD (container $BN_CONTAINER)"
    log "  target statefulset:    $BN_STATEFULSET"
    log "  historic mount path:   $HISTORIC_MOUNT_PATH"

    log "Preflight: verifying target pod $BN_POD is Ready..."
    "${kctl[@]}" get pod "$BN_POD" >/dev/null || fail "pod $BN_POD not found in namespace $BN_NAMESPACE"
    "${kctl[@]}" wait --for=condition=Ready pod/"$BN_POD" --timeout=60s \
        || fail "pod $BN_POD not Ready within 60s"

    log "Staging wrapped blocks via 'blocks bulk-load' ($WRB_SOURCE_DIR -> $STAGING_DIR)..."
    mkdir -p "$STAGING_DIR"
    java -jar "$CLI_JAR" blocks bulk-load --source "$WRB_SOURCE_DIR" --dest "$STAGING_DIR" \
        || fail "'blocks bulk-load' staging failed"

    if [[ -z "$(find "$STAGING_DIR" -name '*.zip' -print -quit 2>/dev/null)" ]]; then
        fail "no .zip files staged; nothing to backfill"
    fi
    local staged_count
    staged_count=$(find "$STAGING_DIR" -name '*.zip' | wc -l)
    log "Staged $staged_count zip file(s)."

    log "WARNING: post-install mode is UNSAFE when the archive PVC isn't a proper"
    log "persistent volume. If mounts have degraded to ephemeral rootfs, the pod"
    log "restart below will WIPE the copied files. --pre-install is safer for"
    log "fresh installs. See PVC status: kubectl get pvc -n $BN_NAMESPACE"

    log "Streaming staged blocks into $BN_POD:$HISTORIC_MOUNT_PATH..."
    "${kctl[@]}" exec "$BN_POD" -c "$BN_CONTAINER" -- mkdir -p "$HISTORIC_MOUNT_PATH" \
        || fail "failed to ensure $HISTORIC_MOUNT_PATH exists on $BN_POD"
    tar -C "$STAGING_DIR" -cf - . | \
        "${kctl[@]}" exec -i "$BN_POD" -c "$BN_CONTAINER" -- tar xf - -C "$HISTORIC_MOUNT_PATH" \
        || fail "failed to stream staged blocks into $BN_POD"
    log "Blocks copied onto $BN_POD's historic volume."

    if [[ "$SKIP_ROLLOUT" == "true" ]]; then
        log "SKIP_ROLLOUT=true; not rolling. Restart manually to trigger re-scan."
    else
        log "Rolling statefulset/$BN_STATEFULSET so BlockFileHistoricPlugin re-scans..."
        "${kctl[@]}" rollout restart statefulset/"$BN_STATEFULSET" \
            || fail "rollout restart failed for $BN_STATEFULSET"
        "${kctl[@]}" rollout status statefulset/"$BN_STATEFULSET" --timeout="${READY_TIMEOUT}s" \
            || fail "rollout did not complete within ${READY_TIMEOUT}s"
        "${kctl[@]}" wait --for=condition=Ready pod/"$BN_POD" --timeout="${READY_TIMEOUT}s" \
            || fail "$BN_POD did not become Ready after restart"
        log "$BN_POD is Ready after rollout."
    fi

    if [[ "$KEEP_STAGING" == "true" ]]; then
        log "KEEP_STAGING=true; leaving staging dir at $STAGING_DIR"
    else
        rm -rf "$STAGING_DIR"
        log "Removed staging dir $STAGING_DIR."
    fi

    log ""
    log "Backfill complete. Verify with:"
    log "  kubectl port-forward -n $BN_NAMESPACE svc/$BN_STATEFULSET 18082:40982 &"
    log "  grpcurl -plaintext -emit-defaults -import-path <proto-dir> \\"
    log "    -proto block-node/api/node_service.proto -d '{}' \\"
    log "    localhost:18082 org.hiero.block.api.BlockNodeService/serverStatus"
    log "  Expect firstAvailableBlock: '0', lastAvailableBlock: <real number>"
}

# ---------- INSTALL-AND-SEED MODE ----------
# One-shot: solo-provisioner install -> scale down -> pre-install seed -> scale up.
run_install_and_seed() {
    log "  profile:               $SOLO_PROFILE"
    log "  provisioner config:    $SOLO_CONFIG"
    log "  values file:           $SOLO_VALUES"

    # Fail fast + graceful if Solo Provisioner isn't installed.
    if ! command -v solo-provisioner >/dev/null 2>&1; then
        fail "solo-provisioner is not on PATH. Install it first with:
    curl -sSL https://raw.githubusercontent.com/hashgraph/solo-weaver/main/install.sh | bash
Then verify with: sudo solo-provisioner -v
See https://docs.hiero.org/block-node-overview/block-node-hardware-specifications/solo-weaver-single-node-k8s-deployment for the full install recipe."
    fi

    # Also confirm the tool actually runs (permissions / version sanity).
    sudo solo-provisioner -v >/dev/null 2>&1 \
        || fail "solo-provisioner is present but failed 'sudo solo-provisioner -v'; check permissions or reinstall"

    # 1. Install (creates PVCs + everything). If already installed, this will
    #    fail with the "already installed" guard, which is fine -- we assume
    #    the operator meant to seed an existing install.
    log "Running: sudo solo-provisioner block node install -p $SOLO_PROFILE ..."
    if sudo solo-provisioner block node install \
            -p "$SOLO_PROFILE" \
            --config "$SOLO_CONFIG" \
            --values "$SOLO_VALUES" \
            "${SOLO_EXTRA_FLAGS[@]}"; then
        log "Solo Provisioner install completed."
    else
        rc=$?
        log "Solo Provisioner install returned exit code $rc. Continuing on the assumption the release already exists (existing PVCs will be reused). If this was a fatal failure, abort now."
    fi

    # 2. Scale StatefulSet to 0 so no BN process holds the archive PVC while we seed.
    log "Scaling statefulset/$BN_STATEFULSET to 0 for seeding..."
    "${kctl[@]}" scale statefulset/"$BN_STATEFULSET" --replicas=0 \
        || fail "failed to scale $BN_STATEFULSET to 0"
    # Wait for the pod to actually be gone (scale returns before termination).
    for _ in $(seq 1 30); do
        if ! "${kctl[@]}" get pod "$BN_POD" >/dev/null 2>&1; then break; fi
        sleep 2
    done
    "${kctl[@]}" get pod "$BN_POD" >/dev/null 2>&1 \
        && fail "pod $BN_POD is still present after 60s; refusing to seed while pod exists"
    log "Pod $BN_POD is gone; safe to seed."

    # 3. Delegate to the pre-install flow (same steps as --pre-install).
    log "Seeding archive PVC via pre-install flow..."
    run_pre_install

    # 4. Scale StatefulSet back to 1 so the BN comes up on the seeded PVC.
    log "Scaling statefulset/$BN_STATEFULSET back to 1..."
    "${kctl[@]}" scale statefulset/"$BN_STATEFULSET" --replicas=1 \
        || fail "failed to scale $BN_STATEFULSET to 1"

    # 5. Wait for pod Ready.
    log "Waiting for pod $BN_POD to become Ready (timeout ${READY_TIMEOUT}s)..."
    "${kctl[@]}" wait --for=condition=Ready pod/"$BN_POD" --timeout="${READY_TIMEOUT}s" \
        || fail "pod $BN_POD did not become Ready within ${READY_TIMEOUT}s"
    log "Pod $BN_POD is Ready."

    log ""
    log "install-and-seed complete. Verify blocks visible with:"
    log "  kubectl port-forward -n $BN_NAMESPACE svc/$BN_STATEFULSET 18082:40982 &"
    log "  grpcurl -plaintext -emit-defaults -import-path <proto-dir> \\"
    log "    -proto block-node/api/node_service.proto -d '{}' \\"
    log "    localhost:18082 org.hiero.block.api.BlockNodeService/serverStatus"
    log "  Expect firstAvailableBlock: '0', lastAvailableBlock: <real number>"
}

# ---------- Dispatch ----------
case "$MODE" in
    install-and-seed) run_install_and_seed ;;
    pre-install)      run_pre_install ;;
    post-install)     run_post_install ;;
    *)                fail "unknown mode: $MODE" ;;
esac
