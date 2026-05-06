# Block Tools CLI Runbook

This runbook provides comprehensive instructions for setting up and operating the Hiero Block Tools CLI. These tools are used for high-performance record-to-block conversion, mainnet validation, and block data management.

## 1. System Setup and Prerequisites

### Server Requirements
*   **CPU:** 16+ cores recommended (Stage 1 and 3 of the conversion pipeline are highly parallel).
*   **Memory:** 64GB+ RAM. JVM heap should be tuned using `-Xms` and `-Xmx` (e.g., `-Xmx48g`).
*   **Disk:**
    *   Conversion of the full Hedera Mainnet history requires ~30 TB of storage.
    *   **ZFS** is highly recommended for handling trillions of small files and providing efficient compression/snapshots.
*   **OS:** Linux (Ubuntu 22.04+ recommended) or macOS.

### Installation
1.  **Java:** JDK 25 or later is required for advanced virtual thread support and high-concurrency performance.
2.  **Zstd:** The `zstd` command-line tool must be installed for archive compression.
    ```bash
    # Ubuntu
    sudo apt install zstd
    # macOS
    brew install zstd
    ```
3.  **GCP CLI:** Required for "requester-pays" bucket access.
    ```bash
    gcloud auth application-default login
    gcloud config set project <YOUR_PROJECT_ID>
    ```
4.  **Build the Tool:**
    ```bash
    ./gradlew :tools:shadowJar
    ```

### Network Selection
Most commands support a `--network` flag to configure bucket names, API endpoints, and genesis address books.
*   `--network mainnet` (Default)
*   `--network testnet`
*   `--network previewnet`

---

## 2. Metadata and Workflows

### Required Metadata Files
Before running validation or conversion, you must fetch network metadata:
1.  **Block Times & Day Blocks:** `java -jar tools.jar mirror update --network <network>`
2.  **Day Listings:** `java -jar tools.jar days updateDayListings --network <network>`
3.  **Address Book:** `java -jar tools.jar mirror generateAddressBook -o addressBookHistory.json`

### Workflow: Wrapping Record Files
Converts daily `.tar.zstd` record file archives into the Block Node historic format.
```bash
java -jar tools.jar blocks wrap -i ./compressedDays -o ./wrappedBlocks --network <network>
```

### Workflow: Full Validation
Validates the entire block stream hash chain, signatures, and HBAR supply from genesis.
```bash
java -Xmx48g -jar tools.jar blocks validate /path/to/wrappedBlocks --network <network>
```

---

## 3. Performance Tuning & Optimization

The conversion pipeline uses a four-stage concurrent model. Tune these flags based on your hardware:

| Flag | Purpose | Recommended |
|------|---------|-------------|
| `--parse-threads` | Stage 1 (RSA Verification) | CPU Cores - 1 |
| `--serialize-threads` | Stage 3 (Zstd Compression) | CPU Cores - 1 |
| `--prefetch` | Sliding window of future blocks | Same as parse-threads |
| `-Xmx` | Maximum JVM Heap size | 75% of System RAM |

**Sample Directory Structure:**
Successful conversion produces a hierarchy like this:
```text
wrappedBlocks/
├── block-0000/
│   ├── 0.zip          (Blocks 0-9,999)
│   ├── 10000.zip      (Blocks 10,000-19,999)
├── addressBookHistory.json
├── nodeStakeHistory.json
├── signature_statistics_wrap.csv
└── wrap-commit.bin    (Durable watermark)
```

---

## 4. Operations and Maintenance

### Graceful Shutdown
*   **SIGINT (Ctrl+C)** triggers an orderly drain of the pipeline.
*   The tool will flush the current `.zip` file, save the `streamingMerkleTree.bin` checkpoint, and update `wrap-commit.bin`.
*   **DO NOT** use `kill -9` as it may leave a corrupted `.zip` file that requires manual deletion.

### Verifying Results
Monitor the `signature_statistics_wrap.csv` file generated during conversion. It contains:
*   `blockNumber`, `blockTime`
*   `totalNodes`, `totalSignatures`
*   `validatedStake`, `threshold`
A healthy run should show `validatedStake >= threshold` for all blocks.

---

## 5. Troubleshooting and Gotchas

### Common Issues Table

| Problem | Solution |
|---------|----------|
| `zstd: command not found` | Install zstd (see Installation section). |
| GCP `PERMISSION_DENIED` | Run `gcloud auth login` and check project billing. |
| `IndexOutOfBoundsException` | `block_times.bin` is stale. Run `mirror update`. |
| Validation fails on block 0 | Ensure the correct genesis address book is loaded. |
| Edge-day sync problems | Use `days clean` to remove bad record sets, then re-download. |

### HBAR Supply Validation
*   **Requirement:** HBAR supply validation **must** start from block 0 (genesis).
*   **Checkpointing:** The tool saves `hbarSupplyState.bin` periodically. If this file is missing or corrupt, supply validation will be disabled on resume unless you restart from block 0.
