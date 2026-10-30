#!/bin/bash

# Function to display help message
show_help() {
    echo "Usage: $0 <directory_path> [start_number]"
    echo ""
    echo "Description:"
    echo "  Scans a directory for files named <number>.blk.zstd and identifies missing ranges of numbers."
    echo ""
    echo "Parameters:"
    echo "  directory_path    : Relative or absolute path to the directory to scan (required)."
    echo "  start_number      : Optional number to start checking for gaps (non-negative integer)."
    echo "                      If not provided, starts from the smallest number found in the directory."
    echo "  -h, --help        : Display this help message and exit."
    echo ""
    echo "Examples:"
    echo "  $0 ./data          : Scan ./data starting from the smallest number found."
    echo "  $0 ./data 0        : Scan ./data starting from 0."
    echo "  $0 /path/to/data 100 : Scan /path/to/data starting from 100."
    exit 0
}

# Check for help option
if [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_help
fi

# Check if directory path is provided
if [ $# -lt 1 ]; then
    echo "Error: Directory path is required."
    echo "Use -h or --help for usage information."
    exit 1
fi

# Assign directory path
dir_path="$1"

# Validate directory exists
if [ ! -d "$dir_path" ]; then
    echo "Error: Directory '$dir_path' does not exist."
    exit 1
fi

# Check for optional start number parameter
if [ $# -eq 2 ]; then
    start_number="$2"
    # Validate that the parameter is a non-negative integer
    if ! [[ "$start_number" =~ ^[0-9]+$ ]]; then
        echo "Error: Start number must be a non-negative integer."
        exit 1
    fi
else
    start_number=""
fi

# Find all files matching *.blk.zstd in the specified directory and subdirectories
# Extract the numeric part, sort numerically and unique
sorted_numbers=($(find "$dir_path" -type f -name '*.blk.zstd' -exec basename {} \; | sed 's/\.blk\.zstd$//' | sort -n -u))

# If no files found, handle the case
if [ ${#sorted_numbers[@]} -eq 0 ]; then
    if [ -n "$start_number" ]; then
        echo "No files found in '$dir_path', but start number provided: $start_number"
        echo "Missing range: $start_number to infinity"
        echo "Total number of gaps: 1"
    else
        echo "No files found in '$dir_path'."
    fi
    exit 0
fi

# If start_number is not provided, use the smallest number found
if [ -z "$start_number" ]; then
    start_number=${sorted_numbers[0]}
fi

# Track number of gaps found
gap_count=0

# Check for gap between provided start_number and first file
if [ $start_number -lt ${sorted_numbers[0]} ]; then
    if (( 10#$start_number < 10#${sorted_numbers[0]} )); then
        from=$((10#$start_number))
        to=$((10#${sorted_numbers[0]} - 1))
        echo "A: Missing range: $from to $to"
        ((gap_count++))
    fi
fi

# Check for gap in sorted numbers to find gaps
prev=${sorted_numbers[0]}
for i in "${sorted_numbers[@]}"; do
    if (( 10#$i > 10#$prev + 1 )); then
        from=$((10#$prev + 1))
        to=$((10#$i - 1))
        echo "B: Missing range: $from to $to"
        ((gap_count++))
    fi
    prev=$i
done

# Print the total number of gaps
echo "Total number of gaps: $gap_count"