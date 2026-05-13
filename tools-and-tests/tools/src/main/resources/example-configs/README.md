# Network Configuration Examples

This directory contains example network configuration files for the Hiero block tools CLI.

## Supported Networks

### Standard Networks (Hardcoded)

These networks have built-in configurations and don't require config files:

- **mainnet** - Hedera mainnet
- **testnet** - Hedera testnet

### Config-Loaded Networks

These networks require configuration files:

- **previewnet** - Hedera previewnet (config file required)
- **other** - Custom/private networks (config file required)

## Configuration File Format

Network configurations are JSON files with the following structure:

```json
{
  "networkName": "previewnet",
  "gcsBucketName": "hedera-preview-streams",
  "bucketPathPrefix": "recordstreams/",
  "mirrorNodeApiUrl": "https://previewnet.mirrornode.hedera.com/api/v1/",
  "genesisDate": "2023-04-06",
  "genesisTimestamp": "2023-04-06T12_00_00.000000000Z",
  "minNodeAccountId": 3,
  "maxNodeAccountId": 9,
  "totalHbarSupplyTinybar": 5000000000000000000,
  "genesisAddressBookResource": "previewnet-genesis-address-book.proto.bin"
}
```

### Field Descriptions

|            Field             |  Type  |                             Description                              |
|------------------------------|--------|----------------------------------------------------------------------|
| `networkName`                | string | Human-readable network name                                          |
| `gcsBucketName`              | string | GCS bucket containing record streams                                 |
| `bucketPathPrefix`           | string | Path prefix within the GCS bucket                                    |
| `mirrorNodeApiUrl`           | string | Base URL for the mirror node REST API (must end with '/')            |
| `genesisDate`                | string | First calendar day of the network (ISO-8601: YYYY-MM-DD)             |
| `genesisTimestamp`           | string | First block timestamp in record-file format (underscores for colons) |
| `minNodeAccountId`           | number | Minimum consensus node account ID number                             |
| `maxNodeAccountId`           | number | Maximum consensus node account ID number                             |
| `totalHbarSupplyTinybar`     | number | Total HBAR supply in tinybar                                         |
| `genesisAddressBookResource` | string | Classpath resource name for the genesis address book                 |

## Usage

### Using Previewnet

1. Copy the example config to the expected location:

   ```bash
   mkdir -p ~/.hiero/networks
   cp previewnet-config.json ~/.hiero/networks/previewnet-config.json
   ```
2. Update the config with actual previewnet values if needed
3. Use with CLI commands:

   ```bash
   java -jar tools-*.jar blocks wrap --network previewnet ...
   java -jar tools-*.jar blocks validate --network previewnet ...
   ```

### Using Custom Networks

1. Create or copy a config file:

   ```bash
   cp custom-network-config.json /path/to/my-network-config.json
   ```
2. Update the config with your network's actual values
3. Set the environment variable:

   ```bash
   export HIERO_NETWORK_CONFIG=/path/to/my-network-config.json
   ```
4. Use with CLI commands:

   ```bash
   java -jar tools-*.jar blocks wrap --network other ...
   java -jar tools-*.jar blocks validate --network other ...
   ```

## Examples

### Download blocks from previewnet

```bash
mkdir -p ~/.hiero/networks
cp previewnet-config.json ~/.hiero/networks/previewnet-config.json

java -jar tools-*.jar download-day \
  --network previewnet \
  --date 2024-05-10 \
  --output compressedDays/
```

### Wrap blocks from custom network

```bash
export HIERO_NETWORK_CONFIG=./custom-network-config.json

java -jar tools-*.jar blocks wrap \
  --network other \
  --input-dir compressedDays \
  --output-dir wrappedBlocks
```

### Validate blocks from custom network

```bash
export HIERO_NETWORK_CONFIG=./my-network-config.json

java -jar tools-*.jar blocks validate \
  --network other \
  wrappedBlocks/
```

## Notes

- Config files must be valid JSON
- The `genesisDate` must be in ISO-8601 format (YYYY-MM-DD)
- The `mirrorNodeApiUrl` must end with a trailing slash
- Genesis address book files must be placed in `src/main/resources/` and referenced by filename
