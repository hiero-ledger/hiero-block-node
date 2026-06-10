# Runbook: Generating RSA Roster Bootstrap File

## Overview
As per Issue #2660, Block Nodes (BN) need an address book roster ready at deployment time so they don't have to query the Mirror Node (MN) API dynamically to verify Wrapped Record Blocks (WRBs). 

This runbook explains how to use the `generate-rsa-roster-bootstrap.sh` script to pre-fetch this data.

## Prerequisites
- `curl` installed
- `jq` installed (Command-line JSON processor)

## Execution Steps

1. Navigate to the root of the repository.
2. Run the script. You can pass the Mirror Node URL and Output File path as optional arguments.

**Default execution (Mainnet):**
```bash
./scripts/generate-rsa-roster-bootstrap.sh
```

**Custom execution (e.g., Testnet):**
```bash
./scripts/generate-rsa-roster-bootstrap.sh "https://testnet.mirrornode.hedera.com" "testnet-roster.json"
```

3. Take the generated JSON file and mount/place it into your Block Node's deployment configuration directory as required by the `roster-bootstrap-rsa` plugin.
