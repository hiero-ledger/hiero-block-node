#!/usr/bin/env bash
set -euo pipefail

TOPOLOGY_FILE=${1:-network-topology.yaml}
OUT_DIR="out"
mkdir -p "$OUT_DIR"

echo "📦 Generating configs from $TOPOLOGY_FILE ..."
command -v yq >/dev/null 2>&1 || { echo "❌ yq is required (https://github.com/mikefarah/yq)"; exit 1; }

# 1️⃣ Generate BN configs
for bn in $(yq -r '.block_nodes | keys[]' "$TOPOLOGY_FILE"); do
  bn_dir="$OUT_DIR/$bn"
  mkdir -p "$bn_dir"

  peers=$(yq -r ".block_nodes[\"$bn\"].peers[]" "$TOPOLOGY_FILE" 2>/dev/null || true)
  json="["
  prio=1
  for peer in $peers; do
    addr=$(yq -r ".block_nodes[\"$peer\"].address" "$TOPOLOGY_FILE")
    port=$(yq -r ".block_nodes[\"$peer\"].port" "$TOPOLOGY_FILE")
    json+="$(jq -nc --arg a "$addr" --argjson p "$port" --argjson pr "$prio" '{address:$a,port:$p,priority:$pr}'),"
    prio=$((prio+1))
  done
  json="${json%,}]"
  [[ "$json" == "[" ]] && json="[]"

  echo "{\"nodes\": $json}" | jq . > "$bn_dir/config.json"
  echo "✅ Generated $bn_dir/config.json"
done

# 2️⃣ Generate CN configs
for cn in $(yq -r '.consensus_nodes | keys[]' "$TOPOLOGY_FILE"); do
  cn_dir="$OUT_DIR/$cn"
  mkdir -p "$cn_dir"

  bns=$(yq -r ".consensus_nodes[\"$cn\"].block_nodes[]" "$TOPOLOGY_FILE")
  json="["
  prio=1
  for bn in $bns; do
    addr=$(yq -r ".block_nodes[\"$bn\"].address" "$TOPOLOGY_FILE")
    port=$(yq -r ".block_nodes[\"$bn\"].port" "$TOPOLOGY_FILE")
    json+="$(jq -nc --arg a "$addr" --argjson p "$port" --argjson pr "$prio" '{address:$a,port:$p,priority:$pr}'),"
    prio=$((prio+1))
  done
  json="${json%,}]"
  [[ "$json" == "[" ]] && json="[]"

  echo "{\"nodes\": $json}" | jq . > "$cn_dir/config.json"
  echo "✅ Generated $cn_dir/config.json"
done

echo "🎉 All configs generated under $OUT_DIR/"
