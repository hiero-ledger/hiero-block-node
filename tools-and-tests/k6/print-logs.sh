#!/usr/bin/env bash

print_logs() {
    local k6_out_dir="k6-out"
    # for each log file in the k6_out_dir, cat to console
    for log_file in "${k6_out_dir}"/*.log; do
        echo "===== Contents of ${log_file} ====="
        cat "${log_file}"
        echo "===================================="
    done
}

print_logs
