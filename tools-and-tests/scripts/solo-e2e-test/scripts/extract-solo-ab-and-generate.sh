#!/bin/bash
# Generate addressBookHistory.json from Solo network
# Usage: ./extract-solo-ab-and-generate.sh <namespace> <genesis-timestamp> <output-file>

set -euo pipefail

NAMESPACE="${1:-solo-network}"
GENESIS_TS="${2}"  # Format: seconds.nanos (e.g., 1781663143.447997286)
OUTPUT_FILE="${3:-addressBookHistory.json}"

# Split genesis timestamp
GENESIS_SECONDS="${GENESIS_TS%.*}"
GENESIS_NANOS="${GENESIS_TS#*.}"

echo "[solo-ab] Extracting address book from Solo network..."

# Strategy 1: Extract RSA key directly from consensus node pod
# The key stored in mirror Helm values doesn't match the actual signing key
echo "[solo-ab] Extracting RSA key from consensus node pod..."

# Strategy 2: Extract RSA public keys from Solo network node pods
echo "[solo-ab] Listing all network node pods in namespace ${NAMESPACE}..."
ALL_PODS=$(kubectl get pods -n "${NAMESPACE}" --no-headers 2>/dev/null | grep "network-node" | awk '{print $1}' || echo "")

if [ -z "${ALL_PODS}" ]; then
    echo "[solo-ab] ERROR: No network node pods found"
    exit 1
fi

echo "[solo-ab] Found network node pods:"
echo "${ALL_PODS}" | while read -r pod; do echo "  - ${pod}"; done

# Build JSON array of nodes using a temp file
TEMP_NODES_FILE="/tmp/solo-nodes-$$.json"
echo "[]" > "${TEMP_NODES_FILE}"

# Extract keys from each pod
for POD_NAME in ${ALL_PODS}; do
    echo "[solo-ab] Processing pod: ${POD_NAME}"

    # Try to determine node number from pod name
    NODE_NUM=$(echo "${POD_NAME}" | grep -oE 'node[0-9]+' | grep -oE '[0-9]+' | head -1 || echo "")

    if [ -z "${NODE_NUM}" ]; then
        echo "[solo-ab]   WARNING: Could not determine node number, skipping"
        continue
    fi

    echo "[solo-ab]   Detected node number: ${NODE_NUM}"
    NODE_ACCOUNT_ID=$((NODE_NUM + 2))

    # Try to list keys directory
    echo "[solo-ab]   Listing keys directory..."
    kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- \
        ls -la /opt/hgcapp/services-hedera/HapiApp2.0/data/keys/ 2>/dev/null | head -20 || echo "  (could not list)"

    # Try multiple possible key file names (try agreement key first, then signing key)
    RSA_KEY=""
    for key_file in \
        "a-public-node${NODE_NUM}.pem" \
        "a-public-node${NODE_ACCOUNT_ID}.pem" \
        "a-public.pem" \
        "s-public-node${NODE_NUM}.pem" \
        "s-public-node${NODE_ACCOUNT_ID}.pem" \
        "s-public.pem" \
        "public.pem"
    do
        # Extract PEM cert, then extract just the public key (SubjectPublicKeyInfo)
        # as hex-encoded DER for the address book
        PEM_CONTENT=$(kubectl exec -n "${NAMESPACE}" "${POD_NAME}" -- \
            cat "/opt/hgcapp/services-hedera/HapiApp2.0/data/keys/${key_file}" 2>/dev/null || echo "")

        if [ -z "${PEM_CONTENT}" ]; then
            continue
        fi

        # Detect if it's a certificate or a raw public key
        if echo "${PEM_CONTENT}" | grep -q "BEGIN CERTIFICATE"; then
            # Extract public key from certificate via openssl
            RSA_KEY=$(echo "${PEM_CONTENT}" | openssl x509 -pubkey -noout 2>/dev/null | \
                grep -v "BEGIN\|END" | tr -d '\n' | base64 -d 2>/dev/null | od -An -tx1 | tr -d ' \n' || echo "")
        else
            # Already a public key, decode PEM directly
            RSA_KEY=$(echo "${PEM_CONTENT}" | grep -v "BEGIN\|END" | tr -d '\n' | \
                base64 -d 2>/dev/null | od -An -tx1 | tr -d ' \n' || echo "")
        fi

        if [ -n "${RSA_KEY}" ]; then
            echo "[solo-ab]   ✓ Found RSA key in ${key_file}"
            break
        fi
    done

    if [ -z "${RSA_KEY}" ]; then
        echo "[solo-ab]   WARNING: Could not extract RSA key"
        continue
    fi

    # Add node to JSON array
    NODE_JSON=$(jq -n \
        --arg nodeId "${NODE_NUM}" \
        --arg accountNum "${NODE_ACCOUNT_ID}" \
        --arg rsaKey "${RSA_KEY}" \
        '{
            nodeId: $nodeId,
            nodeAccountId: {
                shardNum: "0",
                realmNum: "0",
                accountNum: $accountNum
            },
            RSAPubKey: $rsaKey
        }')

    jq --argjson node "${NODE_JSON}" '. + [$node]' "${TEMP_NODES_FILE}" > "${TEMP_NODES_FILE}.tmp"
    mv "${TEMP_NODES_FILE}.tmp" "${TEMP_NODES_FILE}"
    echo "[solo-ab]   ✓ Added node ${NODE_NUM}"
done

NODES_JSON=$(cat "${TEMP_NODES_FILE}")
rm -f "${TEMP_NODES_FILE}"

NODE_COUNT=$(echo "${NODES_JSON}" | jq 'length')
echo "[solo-ab] Extracted ${NODE_COUNT} node keys total"

if [ "${NODE_COUNT}" -eq 0 ]; then
    echo "[solo-ab] ERROR: Could not extract any node keys"
    exit 1
fi

# Build the final address book JSON
jq -n \
  --arg seconds "${GENESIS_SECONDS}" \
  --argjson nanos "${GENESIS_NANOS}" \
  --argjson nodes "${NODES_JSON}" \
  '{
    addressBooks: [
      {
        blockTimestamp: {
          seconds: $seconds,
          nanos: $nanos
        },
        addressBook: {
          nodeAddress: $nodes
        }
      }
    ]
  }' > "${OUTPUT_FILE}"

if ! jq '.' "${OUTPUT_FILE}" > /dev/null 2>&1; then
    echo "[solo-ab] ERROR: Generated invalid JSON"
    exit 1
fi

echo "[solo-ab] Successfully generated ${OUTPUT_FILE}"
echo "[solo-ab] Address book contains:"
jq -r '.addressBooks[0].addressBook.nodeAddress[] | "  Node \(.nodeId) (0.0.\(.nodeAccountId.accountNum))"' "${OUTPUT_FILE}"

exit 0
