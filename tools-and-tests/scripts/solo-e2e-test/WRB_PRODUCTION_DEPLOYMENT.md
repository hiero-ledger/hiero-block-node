# WRB Production Deployment Guidelines

Guidelines for deploying WRB CLI-wrapped blocks in production environments (mainnet, testnet, previewnet).

## Overview

The WRB (Wrapped Record Block) CLI allows conversion of historical record files into Block Node-compatible wrapped blocks. This enables:
- **Fast catch-up**: New Block Nodes can start from wrapped blocks instead of streaming from genesis
- **Archive serving**: Dedicated Block Nodes can serve historical blocks from wrapped archives
- **Disaster recovery**: Pre-wrapped blocks provide recovery points
- **Mirror Node validation**: Wrapped blocks can be validated against live blocks

## Architecture Options

### Option 1: Dedicated Historic Block Node

Deploy a Block Node that serves ONLY from historic storage (no live streaming).

**Use Case**: Archive node for historical API queries

```
Historic Blocks (S3/GCS) → BN-Historic → Mirror Node / API Clients
```

**Configuration**:
- Use `wrb-historic-only.yaml` values override
- Disable live streaming plugins
- Mount wrapped blocks to `FILES_HISTORIC_ROOT_PATH`
- Scale independently from live BNs

**Pros**:
- Simple architecture
- Independent scaling
- No impact on live streaming

**Cons**:
- Doesn't serve latest blocks
- Requires separate deployment

### Option 2: Hybrid Block Node

Block Node that serves both live and historic blocks.

**Use Case**: Full-service Block Node with fast catch-up

```
CN → BN-Hybrid (live + historic) → Mirror Node
     ↓
  Historic Blocks
```

**Configuration**:
- Enable both `blocks-file-historic` and `stream-subscriber` plugins
- Pre-load wrapped blocks for fast catch-up
- Continue streaming live blocks

**Pros**:
- Single deployment
- Fast catch-up from wrapped blocks
- Seamless transition to live streaming

**Cons**:
- More complex configuration
- Higher resource requirements

## Wrapping Process for Production

### 1. Extract Record Files

```bash
# Download from GCS/S3 bucket
gsutil -m rsync -r gs://hedera-mainnet-streams/recordstreams/ ./record-files/

# Or use mc for S3
mc mirror s3/hedera-mainnet-streams/recordstreams ./record-files/
```

### 2. Generate Metadata

Required metadata files:
- `block_times.bin` - Block timestamp index
- `day_blocks.json` - Daily block ranges
- `addressBookHistory.json` - Address book changes

Generate using network-specific configuration:

```bash
# Mainnet
HIERO_NETWORK_CONFIG=mainnet-config.json \
  java -jar tools.jar blocks wrap \
    --input-dir ./record-files \
    --output-dir ./wrapped-blocks \
    --blocktimes-file ./metadata/block_times.bin \
    --day-blocks ./metadata/day_blocks.json
```

### 3. Wrap Blocks

```bash
# Production wrapping with tuned performance
java -Xmx32G -XX:+UseG1GC \
  -jar tools.jar blocks wrap \
  --input-dir ./record-files \
  --output-dir ./wrapped-blocks \
  --parse-threads 16 \
  --serialize-threads 16 \
  --network mainnet
```

**Performance Tuning**:
- `--parse-threads`: CPU count - 2 (leave headroom)
- `--serialize-threads`: CPU count - 2
- `-Xmx`: 4GB per thread as starting point
- Use NVMe storage for temporary work

**Expected Throughput**:
- ~1,000-2,000 blocks/minute on 16-core machine
- ~5-10GB temporary disk space per 100K blocks
- ~200MB RAM per thread

### 4. Validate Wrapped Blocks

Always validate before deployment:

```bash
# Validate block integrity
java -jar tools.jar blocks validate \
  --skip-signatures \
  --validate-balances=true \
  ./wrapped-blocks

# Verify jumpstart.bin
python3 scripts/python/validate_jumpstart_format.py \
  ./wrapped-blocks/jumpstart.bin
```

### 5. Upload to Cloud Storage

```bash
# Upload to GCS with versioning
gsutil -m rsync -r \
  ./wrapped-blocks/ \
  gs://my-org-wrapped-blocks/mainnet/$(date +%Y-%m-%d)/

# Upload to S3 with versioning
aws s3 sync \
  ./wrapped-blocks/ \
  s3://my-org-wrapped-blocks/mainnet/$(date +%Y-%m-%d)/ \
  --storage-class STANDARD_IA
```

## Block Node Deployment

### Kubernetes Deployment

**helm values override** (`production-historic-bn.yaml`):

```yaml
# Historic Block Node for production
resources:
  requests:
    cpu: "8"
    memory: "16Gi"
  limits:
    cpu: "16"
    memory: "32Gi"

plugins:
  names: "facility-messaging,block-access-service,health,server-status,blocks-file-historic"

blockNode:
  config:
    FILES_HISTORIC_ROOT_PATH: "/opt/hiero/block-node/data/historic"
    FILES_HISTORIC_COMPRESSION: "ZSTD"
    FILES_HISTORIC_POWERS_OF_TEN_PER_ZIP_FILE_CONTENTS: "4"
    FILES_HISTORIC_MAX_FILES_PER_DIR: "3"
    PRODUCER_TYPE: "NO_OP"  # Historic-only mode

  persistence:
    archive:
      volumeName: "archive-storage"
      mountPath: "/opt/hiero/block-node/data/historic"
      size: 5Ti  # Adjust based on block range
      storageClass: "fast-ssd"  # Use fast storage

  # Init container to download wrapped blocks from cloud storage
  initContainers:
    - name: download-wrapped-blocks
      image: google/cloud-sdk:alpine
      command:
        - sh
        - -c
        - |
          # Download from GCS
          gsutil -m rsync -r \
            gs://my-org-wrapped-blocks/mainnet/latest/ \
            /archive-pvc/archive-data/
      volumeMounts:
        - name: archive-storage
          mountPath: /archive-pvc
```

**Deploy**:

```bash
helm upgrade --install block-node-historic \
  charts/block-node-server \
  -f production-historic-bn.yaml \
  --namespace hiero-prod
```

### Direct VM Deployment

1. **Install Block Node**:

```bash
# Download release
wget https://github.com/hiero-ledger/hiero-block-node/releases/download/v0.X.Y/block-node-v0.X.Y.tar.gz
tar -xzf block-node-v0.X.Y.tar.gz
cd block-node
```

2. **Configure**:

```properties
# config/application.properties
files.historic.rootPath=/data/historic
files.historic.compression=ZSTD
producer.type=NO_OP
plugins.names=facility-messaging,block-access-service,health,server-status,blocks-file-historic
```

3. **Download wrapped blocks**:

```bash
# From GCS
gsutil -m rsync -r \
  gs://my-org-wrapped-blocks/mainnet/latest/ \
  /data/historic/
```

4. **Start**:

```bash
./bin/block-node start
```

## Monitoring and Validation

### Health Checks

```bash
# Block Node health
curl http://localhost:40840/healthz/readyz

# Verify blocks are accessible
curl http://localhost:40840/api/v1/blocks/0
```

### Metrics to Monitor

- **Block serving rate**: Blocks served per second
- **Cache hit ratio**: Historic block cache efficiency
- **Disk I/O**: Read IOPS and throughput
- **Memory usage**: Block cache and JVM heap
- **gRPC connections**: Active subscribers

### Validation Against Live

Compare historic Block Node against live Block Node:

```bash
# Download comparison tool
git clone https://github.com/hiero-ledger/hiero-block-node
cd hiero-block-node/tools-and-tests/scripts/solo-e2e-test

# Compare block ranges
python3 scripts/python/compare_mirror_nodes.py \
  http://live-mn.example.com/api/v1 \
  http://historic-mn.example.com/api/v1 \
  --block-range 0:1000000 \
  --output validation-report.json
```

## Disaster Recovery

### Backup Strategy

1. **Wrapped blocks** (primary): Cloud storage (GCS/S3) with versioning
2. **Metadata files**: `block_times.bin`, `day_blocks.json`, `addressBookHistory.json`
3. **Jumpstart state**: `jumpstart.bin` for fast recovery
4. **Configuration**: Helm values, application.properties

### Recovery Procedures

**Scenario 1: Block Node failure**

```bash
# Redeploy with same wrapped blocks
helm upgrade --install block-node-historic \
  charts/block-node-server \
  -f production-historic-bn.yaml

# Wait for pod ready
kubectl wait --for=condition=ready pod -l app=block-node-historic

# Verify serving
curl http://block-node-historic/api/v1/blocks/0
```

**Scenario 2: Wrapped blocks corruption**

```bash
# Download fresh copy from cloud backup
gsutil -m rsync -r \
  gs://my-org-wrapped-blocks/mainnet/backup-YYYY-MM-DD/ \
  /data/historic/

# Restart Block Node
kubectl rollout restart statefulset/block-node-historic
```

**Scenario 3: Incremental update**

```bash
# Wrap new blocks (blocks X to Y)
java -jar tools.jar blocks wrap \
  --input-dir ./new-record-files \
  --output-dir ./new-wrapped-blocks \
  --blocktimes-file ./metadata/block_times.bin \
  --day-blocks ./metadata/day_blocks.json

# Upload incremental update
gsutil -m rsync -r \
  ./new-wrapped-blocks/ \
  gs://my-org-wrapped-blocks/mainnet/incremental/

# Rolling update: download and restart
kubectl exec -it block-node-historic-0 -- \
  gsutil -m rsync -r \
    gs://my-org-wrapped-blocks/mainnet/incremental/ \
    /data/historic/

kubectl rollout restart statefulset/block-node-historic
```

## Security Considerations

### Access Control

- **Cloud storage**: Use IAM roles with least privilege
- **Block Node API**: Enable authentication and rate limiting
- **Network**: Restrict gRPC access via firewall rules

### Data Integrity

- **Hash verification**: Validate block hashes against known good state
- **Jumpstart validation**: Verify Merkle tree state matches expected
- **Checksums**: Use cloud storage checksums (CRC32C, MD5)

### Compliance

- **Data retention**: Follow organizational data retention policies
- **Audit logging**: Enable access logs for wrapped blocks
- **Encryption**: Encrypt at rest (cloud storage) and in transit (TLS)

## Cost Optimization

### Storage Costs

- **Compression**: ZSTD reduces storage by ~60-70%
- **Storage class**: Use infrequent access for old blocks
- **Lifecycle policies**: Archive very old blocks to Glacier/Coldline

**Example storage tiers**:
- Blocks 0-1M: Standard (frequent access)
- Blocks 1M-10M: Infrequent Access
- Blocks 10M+: Archive/Glacier

### Compute Costs

- **Spot instances**: Use for wrapping workloads (interruptible)
- **Reserved capacity**: For production historic Block Nodes
- **Auto-scaling**: Scale historic BNs based on demand

## Troubleshooting

### Blocks Not Discovered

```bash
# Check directory structure
kubectl exec -it block-node-historic-0 -- \
  find /data/historic -type f -name "*.zip" | head -20

# Check plugin logs
kubectl logs block-node-historic-0 | grep -i "ZipBlockArchive\|BlockFileHistoricPlugin"

# Verify format matches expected
# Should be: .../000/000/000/.../00000s.zip
```

### Performance Issues

```bash
# Check disk I/O
kubectl exec -it block-node-historic-0 -- iostat -x 1

# Check memory usage
kubectl top pod block-node-historic-0

# Increase cache size
# Edit config: FILES_HISTORIC_BLOCK_CACHE_SIZE
```

### Comparison Failures

```bash
# Run detailed comparison
python3 scripts/python/compare_mirror_nodes.py \
  http://live-mn/api/v1 \
  http://historic-mn/api/v1 \
  --block-range 0:100 \
  --verbose \
  --output detailed-diff.json

# Check differences
jq '.differences' detailed-diff.json
```

## References

- Epic #2767: WRB Mirror Node Validation
- WRB CLI Documentation: `tools/README.md`
- Block Node Configuration: `charts/block-node-server/README.md`
- BlockFileHistoricPlugin: `block-node/blocks-file-historic/`

## Support

For issues or questions:
- GitHub Issues: https://github.com/hiero-ledger/hiero-block-node/issues
- Documentation: https://docs.hiero.org/block-node
- Community: https://discord.gg/hiero
