#!/usr/bin/env bash
set -euo pipefail

TOPOLOGY_FILE=${1:-network-topology.yaml}
OUT_DIR="out"
mkdir -p "$OUT_DIR"

echo "📦 Generating configs from $TOPOLOGY_FILE ..."
command -v yq >/dev/null 2>&1 || { echo "❌ yq is required (https://github.com/mikefarah/yq)"; exit 1; }

REF_TRANSFORM='reduce .[]? as $ref ([]; . + (
    if ($ref | type) == "string" then [{id:$ref, priority:null}]
    elif ($ref | type) == "object" and (($ref | has("node")) or ($ref | has("id"))) then [{id: ($ref.node // $ref.id), priority: ($ref.priority // null)}]
    elif ($ref | type) == "object" then [ $ref | to_entries[] | {id: .key, priority: (.value // null)} ]
    else [] end
  ))
  | .[]
  | "\(.id)|\(.priority // "")"'

# 1️⃣ Generate BN configs
for bn in $(yq -r '.block_nodes | keys[]' "$TOPOLOGY_FILE"); do
  bn_dir="$OUT_DIR/$bn"
  mkdir -p "$bn_dir"

  json="["
  prio=1
  while IFS='|' read -r peer_id peer_priority; do
    [[ -z "$peer_id" ]] && continue
    addr=$(yq -r ".block_nodes[\"$peer_id\"].address" "$TOPOLOGY_FILE")
    port=$(yq -r ".block_nodes[\"$peer_id\"].port" "$TOPOLOGY_FILE")
    if [[ -z "$peer_priority" ]]; then
      peer_priority=$(yq -r ".block_nodes[\"$peer_id\"].priority // \"\"" "$TOPOLOGY_FILE")
    fi
    if [[ -z "$peer_priority" ]]; then
      peer_priority=$prio
    fi
    json+="$(jq -nc --arg a "$addr" --argjson p "$port" --argjson pr "$peer_priority" '{address:$a,port:$p,priority:$pr}'),"
    prio=$((prio+1))
  done < <(yq -o=json '.block_nodes["'"$bn"'"].peers // []' "$TOPOLOGY_FILE" | jq -r "$REF_TRANSFORM")
  json="${json%,}]"
  [[ "$json" == "[" ]] && json="[]"

  echo "{\"nodes\": $json}" | jq . > "$bn_dir/config.json"
  echo "✅ Generated $bn_dir/config.json"
done

# 2️⃣ Generate CN configs
for cn in $(yq -r '.consensus_nodes | keys[]' "$TOPOLOGY_FILE"); do
  cn_dir="$OUT_DIR/$cn"
  mkdir -p "$cn_dir"

  json="["
  prio=1
  while IFS='|' read -r bn_id bn_priority; do
    [[ -z "$bn_id" ]] && continue
    addr=$(yq -r ".block_nodes[\"$bn_id\"].address" "$TOPOLOGY_FILE")
    port=$(yq -r ".block_nodes[\"$bn_id\"].port" "$TOPOLOGY_FILE")
    if [[ -z "$bn_priority" ]]; then
      bn_priority=$(yq -r ".block_nodes[\"$bn_id\"].priority // \"\"" "$TOPOLOGY_FILE")
    fi
    if [[ -z "$bn_priority" ]]; then
      bn_priority=$prio
    fi
    json+="$(jq -nc --arg a "$addr" --argjson p "$port" --argjson pr "$bn_priority" '{address:$a,port:$p,priority:$pr}'),"
    prio=$((prio+1))
  done < <(yq -o=json '.consensus_nodes["'"$cn"'"].block_nodes // []' "$TOPOLOGY_FILE" | jq -r "$REF_TRANSFORM")
  json="${json%,}]"
  [[ "$json" == "[" ]] && json="[]"

  echo "{\"nodes\": $json}" | jq . > "$cn_dir/config.json"
  echo "✅ Generated $cn_dir/config.json"
done

echo "🎉 All configs generated under $OUT_DIR/"
