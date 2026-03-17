# Testnet Pipeline Guide

End-to-end guide for running the record-to-block conversion pipeline against the Hedera testnet.

## Prerequisites

- **Java JDK 25** or later
- **`zstd`** command-line tool installed (used for day archive compression)

  ```bash
  # macOS
  brew install zstd
  # Ubuntu/Debian
  sudo apt install zstd
  ```
- **GCP credentials** configured for requester-pays bucket access:

  ```bash
  gcloud auth application-default login
  gcloud config set project YOUR_PROJECT_ID
  ```

## Testnet Details

|       Property       |                        Value                         |
|----------------------|------------------------------------------------------|
| Network name         | `testnet`                                            |
| Genesis date         | 2024-02-01                                           |
| Consensus nodes      | 7 (account IDs 0.0.3 through 0.0.9)                  |
| GCS bucket           | `hedera-testnet-streams`                             |
| Mirror Node API      | `https://testnet.mirrornode.hedera.com/api/v1/`      |
| Total HBAR supply    | 5 billion (5,000,000,000 HBAR)                       |
| Genesis address book | Bundled as classpath resource (no generation needed) |

## Step-by-Step Pipeline

All commands use the `--network testnet` flag, which must appear **before** the subcommand. For brevity, the examples below use `TOOLS_JAR` as a shorthand:

```bash
export TOOLS_JAR="java -jar tools-and-tests/tools/build/libs/tools-<VERSION>-SNAPSHOT-all.jar"
```

### 1. Build the JAR

```bash
./gradlew :tools:shadowJar
```

### 2. Fetch Metadata from Mirror Node

Generate `block_times.bin` and `day_blocks.json` from the testnet Mirror Node REST API. This does **not** require GCP credentials.

```bash
$TOOLS_JAR --network testnet mirror update
```

To limit the fetch to a specific date range:

```bash
$TOOLS_JAR --network testnet mirror update --end-date 2024-03-01
```

### 3. Generate Day Listings

Download file listing metadata from the testnet GCS bucket. This tells the download commands which files exist for each day.

```bash
$TOOLS_JAR --network testnet days updateDayListings
```

### 4. Download Days

Download compressed daily record file archives from the testnet GCS bucket.

```bash
# Download all available testnet days
$TOOLS_JAR --network testnet days download-days-v3 2024 2 1 2026 3 16

# Or download a smaller range to test
$TOOLS_JAR --network testnet days download-days-v3 2024 2 1 2024 2 28
```

### 5. Wrap Record Files into Blocks

Convert the downloaded record file day archives into wrapped Block Stream blocks.

```bash
$TOOLS_JAR --network testnet blocks wrap \
  -i compressedDays \
  -o testnetWrappedBlocks
```

The testnet genesis address book is bundled and will be loaded automatically.

### 6. Validate Wrapped Blocks

Validate the hash chain, signatures, and structure of the wrapped blocks.

```bash
$TOOLS_JAR --network testnet blocks validate testnetWrappedBlocks
```

To skip balance validation (balance checkpoints are not yet available for testnet):

```bash
$TOOLS_JAR --network testnet blocks validate \
  --no-validate-balances \
  testnetWrappedBlocks
```

> **Note:** Balance checkpoint files are not yet available for testnet. Use `--no-validate-balances` to skip balance validation.

## Common Issues

|                         Problem                          |                                                                 Solution                                                                  |
|----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `zstd: command not found`                                | Install zstd: `brew install zstd` (macOS) or `apt install zstd` (Linux).                                                                  |
| GCP authentication errors                                | Run `gcloud auth application-default login` and ensure a billing-enabled project is set with `gcloud config set project YOUR_PROJECT_ID`. |
| "Requester pays" access denied                           | Ensure your GCP project has billing enabled and is set as the active project.                                                             |
| Edge-day sync problems (missing files at day boundaries) | Some days near the genesis boundary may have incomplete data. Use `days clean` to remove bad record sets, then re-download.               |
| `IndexOutOfBoundsException` during download              | The `block_times.bin` may be out of date. Re-run `--network testnet mirror update` to refresh it.                                         |
| Validation fails on first block                          | Ensure you are starting from block 0 for full validation, or use `--skip-signatures` for partial runs.                                    |
