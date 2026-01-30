#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Queries Block Node metrics directly from the Prometheus endpoint and outputs a summary.
#
# Usage:
#   ./solo-metrics-summary.sh [port] [format]
#
# Arguments:
#   port:   Metrics port (default: 16007)
#   format: Output format - 'text' or 'github-summary' (default: text)
#
# Examples:
#   ./solo-metrics-summary.sh                        # Query port 16007, text output
#   ./solo-metrics-summary.sh 16008                  # Query port 16008, text output
#   ./solo-metrics-summary.sh 16007 github-summary   # GitHub Actions summary format

set -o pipefail

METRICS_PORT="${1:-16007}"
OUTPUT_FORMAT="${2:-text}"

# Fetch raw prometheus metrics from Block Node endpoint
METRICS=$(curl -s "http://localhost:${METRICS_PORT}/metrics" 2>/dev/null)

if [[ -z "$METRICS" ]]; then
  if [[ "$OUTPUT_FORMAT" == "github-summary" ]]; then
    echo "### Block Node Metrics Summary"
    echo ""
    echo "> Metrics unavailable (could not connect to port ${METRICS_PORT})"
  else
    echo "ERROR: Could not fetch metrics from port ${METRICS_PORT}"
  fi
  exit 1
fi

# Extract key metrics (handle counter format with labels)
# These metric names match the Block Node's actual Prometheus metric names (hiero_block_node_ prefix)

# Throughput metrics
BLOCK_ITEMS=$(echo "$METRICS" | grep "^hiero_block_node_publisher_block_items_received_total " | awk '{print $2}' | head -1)
BLOCKS_VERIFIED=$(echo "$METRICS" | grep "^hiero_block_node_verification_blocks_received_total " | awk '{print $2}' | head -1)
BLOCKS_STORED=$(echo "$METRICS" | grep "^hiero_block_node_files_recent_blocks_stored " | awk '{print $2}' | head -1)
LATEST_BLOCK=$(echo "$METRICS" | grep "^hiero_block_node_publisher_highest_block_number_inbound " | awk '{print $2}' | head -1)

# Connection metrics
PUBLISHER_CONNS=$(echo "$METRICS" | grep "^hiero_block_node_publisher_open_connections " | awk '{print $2}' | head -1)
SUBSCRIBER_CONNS=$(echo "$METRICS" | grep "^hiero_block_node_subscriber_open_connections " | awk '{print $2}' | head -1)

# Health metrics (should all be 0 in a healthy system)
VERIFY_FAILED=$(echo "$METRICS" | grep "^hiero_block_node_verification_blocks_failed_total " | awk '{print $2}' | head -1)
VERIFY_ERRORS=$(echo "$METRICS" | grep "^hiero_block_node_verification_blocks_error_total " | awk '{print $2}' | head -1)
STREAM_ERRORS=$(echo "$METRICS" | grep "^hiero_block_node_publisher_stream_errors_total " | awk '{print $2}' | head -1)

# Queue utilization metrics (percentage)
ITEM_QUEUE_PCT=$(echo "$METRICS" | grep "^hiero_block_node_messaging_item_queue_percent_used " | awk '{print $2}' | head -1)
NOTIF_QUEUE_PCT=$(echo "$METRICS" | grep "^hiero_block_node_messaging_notification_queue_percent_used " | awk '{print $2}' | head -1)

# Defaults for missing values (metrics may not exist if no activity)
BLOCK_ITEMS="${BLOCK_ITEMS:-0}"
BLOCKS_VERIFIED="${BLOCKS_VERIFIED:-0}"
BLOCKS_STORED="${BLOCKS_STORED:-0}"
LATEST_BLOCK="${LATEST_BLOCK:-0}"
PUBLISHER_CONNS="${PUBLISHER_CONNS:-0}"
SUBSCRIBER_CONNS="${SUBSCRIBER_CONNS:-0}"
VERIFY_FAILED="${VERIFY_FAILED:-0}"
VERIFY_ERRORS="${VERIFY_ERRORS:-0}"
STREAM_ERRORS="${STREAM_ERRORS:-0}"
ITEM_QUEUE_PCT="${ITEM_QUEUE_PCT:-0}"
NOTIF_QUEUE_PCT="${NOTIF_QUEUE_PCT:-0}"

# Calculate avg block items per block (use awk for numeric zero check)
AVG_ITEMS=$(awk -v items="$BLOCK_ITEMS" -v verified="$BLOCKS_VERIFIED" 'BEGIN {
  if (verified + 0 > 0) printf "%.2f", items / verified
  else print "N/A"
}')

# Format queue percentages with % suffix
ITEM_QUEUE_DISPLAY=$(awk "BEGIN {printf \"%.1f%%\", $ITEM_QUEUE_PCT}")
NOTIF_QUEUE_DISPLAY=$(awk "BEGIN {printf \"%.1f%%\", $NOTIF_QUEUE_PCT}")

# Output based on format
if [[ "$OUTPUT_FORMAT" == "github-summary" ]]; then
  cat << EOF
### Block Node Metrics Summary

| Metric | Value |
|--------|-------|
| **Throughput** | |
| Block Items Received | ${BLOCK_ITEMS} |
| Blocks Verified | ${BLOCKS_VERIFIED} |
| Blocks Stored | ${BLOCKS_STORED} |
| Avg Items/Block | ${AVG_ITEMS} |
| Latest Block | ${LATEST_BLOCK} |
| **Connections** | |
| Publisher Connections | ${PUBLISHER_CONNS} |
| Subscriber Connections | ${SUBSCRIBER_CONNS} |
| **Health** | |
| Verification Failures | ${VERIFY_FAILED} |
| Verification Errors | ${VERIFY_ERRORS} |
| Stream Errors | ${STREAM_ERRORS} |
| **Queue Utilization** | |
| Item Queue | ${ITEM_QUEUE_DISPLAY} |
| Notification Queue | ${NOTIF_QUEUE_DISPLAY} |

EOF
else
  cat << EOF
Block Node Metrics (port ${METRICS_PORT})
=================================
Throughput:
  Block Items Received: ${BLOCK_ITEMS}
  Blocks Verified:      ${BLOCKS_VERIFIED}
  Blocks Stored:        ${BLOCKS_STORED}
  Avg Items/Block:      ${AVG_ITEMS}
  Latest Block:         ${LATEST_BLOCK}

Connections:
  Publisher:   ${PUBLISHER_CONNS}
  Subscriber:  ${SUBSCRIBER_CONNS}

Health:
  Verification Failures: ${VERIFY_FAILED}
  Verification Errors:   ${VERIFY_ERRORS}
  Stream Errors:         ${STREAM_ERRORS}

Queue Utilization:
  Item Queue:         ${ITEM_QUEUE_DISPLAY}
  Notification Queue: ${NOTIF_QUEUE_DISPLAY}
EOF
fi
