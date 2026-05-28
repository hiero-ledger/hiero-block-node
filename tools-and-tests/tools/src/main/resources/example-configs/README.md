# Network Configuration Examples

This directory contains example network configuration files for the Hiero block tools CLI.

## Supported Networks

### Standard Networks (Hardcoded)

These networks have built-in configurations and don't require config files:

- **mainnet** - Hedera mainnet (GCS)
- **testnet** - Hedera testnet (GCS)

### Config-Loaded Networks

These networks require configuration files:

- **previewnet** - Hedera previewnet (config file required)
- **other** - Custom/private networks (config file required)

## Storage Backends

The CLI supports two cloud storage backends:

### Google Cloud Storage (GCS)

- Used by Hedera public networks (mainnet, testnet, previewnet)
- Requires GCS API access
- Set `bucketType: "GCS"`

### S3-Compatible Storage

- Used for private networks (Solo, custom deployments)
- Works with MinIO, AWS S3, or any S3-compatible storage
- Set `bucketType: "S3"`
- Requires endpoint URL and credentials

## Configuration File Format

### GCS Configuration

Example for Hedera previewnet:

```json
{
  "networkName": "previewnet",
  "bucketType": "GCS",
  "bucketName": "hedera-preview-streams",
  "pathPrefix": "recordstreams/",
  "endpoint": null,
  "region": null,
  "accessKey": null,
  "secretKey": null,
  "mirrorNodeApiUrl": "https://previewnet.mirrornode.hedera.com/api/v1/",
  "genesisDate": "2023-04-06",
  "genesisTimestamp": "2023-04-06T12_00_00.000000000Z",
  "minNodeAccountId": 3,
  "maxNodeAccountId": 9,
  "totalHbarSupplyTinybar": 5000000000000000000,
  "genesisAddressBookResource": "previewnet-genesis-address-book.proto.bin"
}
```

### S3 Configuration

Example for Solo network with MinIO:

```json
{
  "networkName": "solo",
  "bucketType": "S3",
  "bucketName": "solo-streams",
  "pathPrefix": "recordstreams/",
  "endpoint": "http://minio.solo-network.svc.cluster.local:9000",
  "region": "us-east-1",
  "accessKey": "minioadmin",
  "secretKey": "minioadmin",
  "mirrorNodeApiUrl": "http://block-node-1.solo-network.svc.cluster.local:8080/api/v1/",
  "genesisDate": "2024-01-01",
  "genesisTimestamp": "2024-01-01T00_00_00.000000000Z",
  "minNodeAccountId": 0,
  "maxNodeAccountId": 0,
  "totalHbarSupplyTinybar": 5000000000000000000,
  "genesisAddressBookResource": ""
}
```

### Field Descriptions

|            Field             |  Type  |                             Description                              |              Required For              |
|------------------------------|--------|----------------------------------------------------------------------|----------------------------------------|
| `networkName`                | string | Human-readable network name                                          | All                                    |
| `bucketType`                 | string | Storage backend: "GCS" or "S3"                                       | All (defaults to "GCS" if omitted)     |
| `bucketName`                 | string | Bucket name containing record streams                                | All                                    |
| `pathPrefix`                 | string | Path prefix within the bucket (e.g., "recordstreams/")               | All                                    |
| `endpoint`                   | string | S3 endpoint URL (e.g., "http://minio:9000")                          | S3 only (null for GCS)                 |
| `region`                     | string | S3 region (e.g., "us-east-1")                                        | S3 only (null for GCS)                 |
| `accessKey`                  | string | S3 access key ID                                                     | S3 only (null for GCS)                 |
| `secretKey`                  | string | S3 secret access key                                                 | S3 only (null for GCS)                 |
| `mirrorNodeApiUrl`           | string | Base URL for the mirror node REST API (must end with '/')            | All                                    |
| `genesisDate`                | string | First calendar day of the network (ISO-8601: YYYY-MM-DD)             | All                                    |
| `genesisTimestamp`           | string | First block timestamp in record-file format (underscores for colons) | All                                    |
| `minNodeAccountId`           | number | Minimum consensus node account ID number                             | All                                    |
| `maxNodeAccountId`           | number | Maximum consensus node account ID number                             | All                                    |
| `totalHbarSupplyTinybar`     | number | Total HBAR supply in tinybar                                         | All                                    |
| `genesisAddressBookResource` | string | Classpath resource name for the genesis address book                 | All (can be empty for custom networks) |

## Usage

### Using Previewnet (GCS)

1. Copy the example config to the expected location:

   ```bash
   mkdir -p ~/.hiero/networks
   cp previewnet-config.json ~/.hiero/networks/previewnet-config.json
   ```
2. Update the config with actual previewnet values (bucket name, genesis date, etc.)
3. Use with CLI commands:

   ```bash
   java -jar tools-*.jar blocks wrap --network previewnet ...
   java -jar tools-*.jar blocks validate --network previewnet ...
   java -jar tools-*.jar days live-sequential --network previewnet ...
   ```

### Using Solo Network (S3/MinIO)

1. Create a config file for your Solo network:

   ```bash
   cp solo-s3-config.json ~/.hiero/networks/solo-config.json
   ```
2. Update the config with your MinIO endpoint and credentials:
   - Set `endpoint` to your MinIO service URL
   - Update `accessKey` and `secretKey` (default: minioadmin/minioadmin)
   - Set `mirrorNodeApiUrl` to your Block Node's mirror API endpoint
3. Set the environment variable:

   ```bash
   export HIERO_NETWORK_CONFIG=~/.hiero/networks/solo-config.json
   ```
4. Use with CLI commands:

   ```bash
   java -jar tools-*.jar days live-sequential --network other ...
   ```

### Using Custom Networks

1. Create or copy a config file:

   ```bash
   cp custom-network-config.json /path/to/my-network-config.json
   ```
2. Update the config with your network's actual values:
   - Set `bucketType` to "GCS" or "S3"
   - For S3: provide endpoint, region, accessKey, secretKey
   - For GCS: leave S3 fields as null
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

### Stream and wrap blocks from Solo network (MinIO)

```bash
export HIERO_NETWORK_CONFIG=./solo-s3-config.json

java -jar tools-*.jar days live-sequential \
  --network other \
  --output-dir compressedDays \
  --wrap-output-dir wrappedBlocks
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
- For GCS configurations, set S3 fields to `null`
- For S3 configurations, all S3 fields (endpoint, region, accessKey, secretKey) are required
- Genesis address book files must be placed in `src/main/resources/` and referenced by filename
- MinIO default credentials are `minioadmin`/`minioadmin` (change for production)
