# WRB Differential Test Guide

Epic #2767: E2E differential test to validate Mirror Node databases are identical whether ingesting live blocks or WRB CLI-wrapped blocks.

## Overview

This test compares Mirror Node databases when ingesting blocks from two sources:
- **MN1**: Ingests live blocks from BN1 (connected to CN1)
- **MN2**: Ingests wrapped blocks from BN2 (serving pre-wrapped historic blocks)

If MN1 and MN2 databases are identical, it proves WRB CLI wrapping preserves all block data correctly.

## Architecture

```
CN1 → BN1 → MN1 (live blocks)

WRB CLI Wrapped Blocks → BN2 → MN2 (wrapped blocks)

Compare: MN1 database == MN2 database
```

## Components

### 1. Topology: `wrb-differential-test.yaml`

- 1 Consensus Node (CN1)
- 2 Block Nodes (BN1 - live, BN2 - historic)
- 2 Mirror Nodes (MN1, MN2)

### 2. Scripts

- `wrb-cli-wrap-and-compare.sh` - WRB CLI wrapping (extract, wrap, validate)
- `wrb-deploy-bn2-historic.sh` - BN2 historic deployment (copy blocks, restart, verify)

### 3. Configuration

- `values-overrides/wrb-historic-only.yaml` - BN2 values override (historic-only mode)

### 4. Test Case

- `tests/wrb-differential-test.yaml` - Full E2E test orchestration

## Prerequisites

- Docker and kind installed
- Solo CLI installed and configured
- zstd installed (`brew install zstd` on macOS)
- Python 3.x with required packages

## Running the Test

### Option 1: Full Orchestration Script (Recommended)

The complete end-to-end test in a single command:

```bash
cd tools-and-tests/scripts/solo-e2e-test

# Run complete differential test
./scripts/wrb-full-differential-test.sh --block-range 0:100

# With options
./scripts/wrb-full-differential-test.sh \
  --block-range 0:100 \
  --output-dir /tmp/my-test-results \
  --verbose

# Skip already-completed steps
./scripts/wrb-full-differential-test.sh \
  --skip-build \
  --skip-extract \
  --block-range 0:100
```

**What it does**:
1. ✅ Build tools JAR
2. ✅ Extract record files from MinIO
3. ✅ Wrap blocks with WRB CLI
4. ✅ Deploy wrapped blocks to BN2
5. ✅ Compare Mirror Node APIs (MN1 vs MN2)
6. ✅ Validate jumpstart.bin Merkle hashes
7. ✅ Generate comprehensive test report

**Output**: `/tmp/wrb-test-results/`
- `test-run.log` - Full test execution log
- `wrb-differential-test-report.json` - Structured results
- `mirror-node-api-comparison.json` - API comparison details
- `jumpstart-comparison.log` - Merkle hash validation

### Option 2: Framework Test Runner

```bash
cd tools-and-tests/scripts/solo-e2e-test

# Run full test with wrb-differential-test topology
task test:run TEST_FILE=tests/wrb-differential-test.yaml TOPOLOGY=wrb-differential-test
```

### Option 2: Manual Step-by-Step

#### Step 1: Deploy Network

```bash
cd tools-and-tests/scripts/solo-e2e-test

# Deploy with wrb-differential-test topology
task deploy TOPOLOGY=wrb-differential-test

# Wait for network to be ready
task status
```

#### Step 2: Produce Blocks

```bash
# Wait for CN to produce blocks (2-3 minutes)
sleep 120

# Verify blocks are being produced
task port-forward  # In separate terminal
curl http://localhost:40840/api/v1/status  # BN1 should show blocks
```

#### Step 3: Wrap Blocks with WRB CLI

```bash
# Build tools jar
./scripts/wrb-cli-wrap-and-compare.sh build

# Extract record files from MinIO
./scripts/wrb-cli-wrap-and-compare.sh extract

# Wrap blocks
./scripts/wrb-cli-wrap-and-compare.sh wrap

# Verify wrapped blocks
ls -lh /tmp/wrb-cli-phase2-validation/cli-wrapped-blocks/
```

#### Step 4: Deploy Wrapped Blocks to BN2

```bash
# Copy wrapped blocks to BN2's archive PVC
./scripts/wrb-deploy-bn2-historic.sh copy-blocks

# Restart BN2 to discover blocks
./scripts/wrb-deploy-bn2-historic.sh restart

# Verify BN2 can serve blocks
./scripts/wrb-deploy-bn2-historic.sh verify

# Or run all steps at once
./scripts/wrb-deploy-bn2-historic.sh all
```

#### Step 5: Verify and Compare Mirror Nodes

```bash
# Check MN1 (live blocks from BN1)
kubectl logs -n solo-network -l app=hedera-mirror-node --tail=50

# Check MN2 (wrapped blocks from BN2)
kubectl logs -n solo-network -l app=hedera-mirror-node-2 --tail=50

# Compare MN1 and MN2 databases via REST API
./scripts/compare-mirror-node-apis.sh --block-range 0:100

# View detailed comparison report
cat /tmp/mirror-node-comparison-report.json | jq
```

## Expected Results

### Successful Test

- ✅ CN1 produces blocks
- ✅ BN1 receives and streams live blocks
- ✅ WRB CLI wraps blocks successfully
- ✅ BN2 discovers and serves wrapped blocks
- ✅ MN1 ingests live blocks from BN1
- ✅ MN2 ingests wrapped blocks from BN2
- ✅ MN1 and MN2 databases are identical (Task #7 - API comparison)

### Key Metrics

- Block production rate: ~1 block/second (CN)
- Block wrapping time: ~10-30 seconds for 200 blocks
- Block copy time: ~5-10 seconds (depends on block count)
- BN2 restart time: ~30-60 seconds
- Mirror Node ingestion: ~1-2 minutes for 200 blocks

## Troubleshooting

### BN2 Not Finding Blocks

```bash
# Check BN2 logs for block discovery
kubectl logs -n solo-network <bn2-pod-name> | grep -i "zipblockarchive\|historic"

# Verify blocks were copied
kubectl exec -n solo-network <bn2-pod-name> -- \
  find /opt/hiero/block-node/data/historic -name "*.zip"

# Check directory structure (must match BlockWriter format)
kubectl exec -n solo-network <bn2-pod-name> -- \
  ls -R /opt/hiero/block-node/data/historic
```

### Wrapped Blocks Format Mismatch

The WRB CLI `BlockWriter` creates blocks in the same format as `BlockFileHistoricPlugin`:
- Nested directories: 3 digits per level (e.g., `000/000/000/...`)
- Zip archives: 10,000 blocks per zip (e.g., `00000s.zip`)
- Block files: ZSTD compressed (e.g., `0000000000000000123.blk.zstd`)

If blocks aren't discovered, verify the directory structure matches.

### Mirror Node Not Ingesting

```bash
# Check Mirror Node configuration
kubectl get configmap -n solo-network -l app=hedera-mirror-node

# Verify Mirror Node is subscribed to correct BN
kubectl logs -n solo-network <mn-pod-name> | grep -i "subscribe\|block"
```

## Next Steps (Outstanding Tasks)

### Task #8: Full E2E Differential Test Orchestration

Complete integration and polish:
- Test full E2E workflow end-to-end
- Handle edge cases and failure scenarios
- Add comprehensive error reporting
- Integrate jumpstart.bin Merkle hash verification
- Add to CI/CD pipeline
- Document production deployment guidelines

## Reference

- Epic #2767: WRB Mirror Node Validation
- Task #4: ✅ Block Node loading from disk (completed)
- Task #5: ⏸️ Import command (deferred - not needed for test)
- Task #6: ✅ Configure BN2 to serve wrapped blocks (completed)
- Task #7: ✅ Mirror Node API comparison tool (completed)
- Task #8: 🔄 E2E test orchestration (pending)

## Files

**Topology & Configuration:**
- `topologies/wrb-differential-test.yaml` - Test topology definition
- `charts/block-node-server/values-overrides/wrb-historic-only.yaml` - BN2 historic-only config

**Scripts:**
- `scripts/wrb-full-differential-test.sh` - **Master orchestration script (NEW!)**
- `scripts/wrb-cli-wrap-and-compare.sh` - WRB CLI wrapping utilities
- `scripts/wrb-deploy-bn2-historic.sh` - BN2 historic deployment
- `scripts/compare-mirror-node-apis.sh` - Mirror Node API comparison wrapper

**Python Tools:**
- `scripts/python/compare_mirror_nodes.py` - Mirror Node REST API comparison
- `scripts/python/compare_jumpstart_hashes.py` - Jumpstart Merkle hash comparison
- `scripts/python/validate_jumpstart_format.py` - Jumpstart file format validator

**Tests & Documentation:**
- `tests/wrb-differential-test.yaml` - E2E test case definition
- `WRB_DIFFERENTIAL_TEST.md` - This guide (testing & development)
- `WRB_PRODUCTION_DEPLOYMENT.md` - Production deployment guidelines
