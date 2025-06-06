#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

set -eo pipefail

script_name=$0
function usage {
    echo ""
    echo "Retrieves CN protobuf and combines it along with local BN protobuf into a single artifact."
    echo "Script should be run from 'hiero-block-node/protobuf' directory"
    echo ""
    echo "Usage: $script_name -t string -v string -o string "
    echo ""
    echo "  -t string   CN tag commit hash or branch value"
    echo "              (example: main or <commit_hash>)"
    echo "  -v string   BN artifact release version"
    echo "              (example: 0.10.0-snapshot)"
    echo "  -o string   Output artifact location and intermediate directory"
    echo "              (example: bn-protobuf)"
    echo "  -c bool     Flag if intermediate output location should be removed after execution"
    echo "              (example: true, default is false)"
    echo "  -i bool     Flag if BN API proto should be included, if false no artifact is created"
    echo "              (example: true, default is true)"
    echo "  -b string   BN API path"
    echo "              (assumes wordkingDir is protobuf module, default is src/main/proto/)"
    echo ""
}

while getopts ":t:v:o:c:i:b:" opt; do
  case ${opt} in
    t)
      repository_tag=$OPTARG;;
    v)
      release_version=$OPTARG;;
    o)
      output_dir=$OPTARG;;
    c)
      cleanup=$OPTARG;;
    i)
      include_bn_api=$OPTARG;;
    b)
      bn_api_path=$OPTARG;;
    \?)
      echo "Missing parameters, Usage: $0 <repository_tag> <release_version> <output_dir>"
      usage
  esac
done

# verify presence of required params and set defaults for optionals
if [[ -z $repository_tag ]]; then
    echo "Missing '-t' (repository_tag) parameter"
    usage
    exit 1
fi
if [[ -z $release_version ]]; then
    echo "Missing '-v' (version) parameter"
    usage
    exit 1
fi
if [[ -z $output_dir ]]; then
    echo "Missing '-o' (output_dir) parameter"
    usage
    exit 1
fi

# handle defaults for optional fields
cleanup="${cleanup:-false}"
include_bn_api="${include_bn_api:-true}"
bn_api_path="${bn_api_path:-src/main/proto/}"

echo "Running $0, working directory: $PWD"
echo "repository_tag: $repository_tag, release_version: $release_version, output_dir: $output_dir, cleanup: $cleanup, include_bn_api: $include_bn_api, bn_api_path: $bn_api_path ..."

# Clone repo if doesn't exist locally
if [ ! -d "./hiero-consensus-node" ]; then
  echo "Cloning hiero-ledger/hiero-consensus-node repo to '$PWD/hiero-consensus-node'"
  git clone --depth 1 --sparse --no-checkout --filter=blob:none https://github.com/hiero-ledger/hiero-consensus-node.git
  cd hiero-consensus-node || { echo "Failed to change directory"; exit 1; }

  git sparse-checkout init
  git sparse-checkout set "hapi/hedera-protobuf-java-api/src/main/proto"
  git checkout "$repository_tag"
  cd ../
else
  echo "hiero-consensus-node repo already exists, skipping clone and checkout"
fi

# prepare output dir
rm -rf $output_dir
mkdir -p $output_dir

# Copy CN 'block' protobuf files to the output_dir directory, remove block_service.proto to avoid conflicts
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/block "$output_dir"
rm -f ./block-node-protobuf/block/block_service.proto

# Copy CN 'platform' protobuf files to the output_dir directory
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/platform "$output_dir"

# Copy CN 'services' protobuf files to the output_dir directory
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/services "$output_dir"

# Copy CN 'streams' protobuf files to the output_dir directory
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/streams "$output_dir"

if $include_bn_api; then
  # create artifact file if BN APIs are included
  tar -czf "block-node-protobuf-$release_version.tgz" -C "$output_dir" . -C ${bn_api_path} ./block-node
  echo "CN + BN proto artifact 'block-node-protobuf-$release_version.tgz' successfully created."
fi

if $cleanup; then
  echo "Clean up hiero-consensus-node clone directory"
  rm -rf ./hiero-consensus-node

  echo "Cleaning up intermediate "$output_dir" directory"
  rm -rf "$output_dir"
fi

if [ $? -eq 0 ]; then
  echo "CN proto retrieval and $output_dir contents successfully created."
else
  echo "Error building joint protobuf files."
fi

exit 0
