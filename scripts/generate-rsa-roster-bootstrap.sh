#!/bin/bash
# Script to generate an RSA roster bootstrap file by querying a Mirror Node API.
# Resolves Issue #2660 for hiero-block-node.

set -e

# Default configurations
MIRROR_NODE_URL="${1:-https://mainnet-public.mirrornode.hedera.com}"
OUTPUT_FILE="${2:-bootstrap-roster.json}"
NEXT_LINK="/api/v1/network/nodes"

echo "🔍 Fetching network nodes from Mirror Node: ${MIRROR_NODE_URL}"

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "❌ Error: 'jq' is not installed. Please install jq to run this script."
    exit 1
fi

# Initialize an empty JSON array to hold all nodes
ALL_NODES="[]"

# Loop through paginated API responses
while [ "${NEXT_LINK}" != "null" ] && [ -n "${NEXT_LINK}" ]; do
    echo "➡️ Fetching page: ${NEXT_LINK}..."
    
    RESPONSE=$(curl -s --fail "${MIRROR_NODE_URL}${NEXT_LINK}") || {
        echo "❌ Error: Failed to reach Mirror Node API."
        exit 1
    }
    
    # Extract nodes from the current page and append to the master array
    PAGE_NODES=$(echo "${RESPONSE}" | jq '.nodes')
    ALL_NODES=$(jq -s '.[0] + .[1]' <(echo "${ALL_NODES}") <(echo "${PAGE_NODES}"))
    
    # Get the next page link, if it exists
    NEXT_LINK=$(echo "${RESPONSE}" | jq -r '.links.next // empty')
done

echo "✅ Successfully retrieved all nodes. Parsing RSA keys..."

# Parse the accumulated JSON array to extract the required fields
echo "${ALL_NODES}" | jq '{
    "roster": [
        .[] | {
            "node_id": .node_id,
            "account_id": .node_account_id,
            "rsa_public_key": .rsa_public_key
        }
    ],
    "generated_at": (now | todateiso8601)
}' > "${OUTPUT_FILE}"

echo "🎉 Roster bootstrap file successfully generated at: ${OUTPUT_FILE}"
