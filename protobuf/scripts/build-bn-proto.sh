#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

set -eo pipefail

programname=$0
function usage {
    echo ""
    echo "Retrieves CN protobuf and combines it along with local BN protobuf into a single artifact."
    echo "Script should be run from 'hiero-block-node/protobuf' directory"
    echo ""
    echo "usage: $programname -t string -v string -o string "
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
    echo "              (assumes wordkingDir is protobuf module, default is src/main/proto/org/hiero/block/api)"
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
if [[ -z $cleanup ]]; then
    cleanup=false
fi
if [[ -z $include_bn_api ]]; then
    include_bn_api=true
fi
if [[ -z $bn_api_path ]]; then
    echo "Set 'bn_api_path' default to 'src/main/proto/org/hiero/block/api'"
    bn_api_path="src/main/proto/org/hiero/block/api"
fi

# Clone the repository
echo "Running $0, working directory: $PWD"
echo "repository_tag: $repository_tag, release_version: $release_version, output_dir: $output_dir, cleanup: $cleanup ..."

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

rm -rf $output_dir
mkdir -p $output_dir

# Copy CN 'block' protobuf files to the /block-node-protobuf directory, remove block_service.proto to avoid duplication
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/block "$output_dir"
rm -f ./block-node-protobuf/block/block_service.proto

# Move CN 'platform' protobuf files to the /block-node-protobuf directory
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/platform "$output_dir"

# Move CN 'services' protobuf files to the /block-node-protobuf directory
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/services "$output_dir"

# Move CN 'streams' protobuf files to the /block-node-protobuf directory
cp -r ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/streams "$output_dir"

if $include_bn_api; then
  # Copy BN repo protobuf files to the new directory
  echo "Copy BN API proto src:'$bn_api_path' to target:'$output_dir' directory"
  cp -r $bn_api_path/* "$output_dir/"
fi

if $include_bn_api; then
  tar -czf "block-node-protobuf-$release_version.tgz" -C "./$output_dir" .
fi

if $cleanup; then
  echo "Clean up hiero-consensus-node clone directory"
  rm -rf ./hiero-consensus-node

  echo "Cleaning up intermediate "$output_dir" directory"
  rm -rf "./$output_dir"
fi


if [ $? -eq 0 ]; then
  echo "$output_dir archive successfully created."
else
  echo "Error building archive."
fi

exit 0
