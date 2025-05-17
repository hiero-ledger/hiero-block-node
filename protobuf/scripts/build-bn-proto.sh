#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

set -eo pipefail

programname=$0
function usage {
    echo ""
    echo "Retrieves CN protobuf and combines it along with local BN protobuf into a single artifact."
    echo ""
    echo "usage: $programname -t string -v string -o string "
    echo ""
    echo "  -t string   CN tag or branch value"
    echo "              (example: main or <commit_hash>)"
    echo "  -v string   BN artifact release version"
    echo "              (example: 0.10.0-snapshot)"
    echo "  -o string   Output location of artifact and intermediate directory"
    echo "              (example: bn-protobuf)"
    echo ""
}

while getopts ":t:v:o:c:" opt; do
  case ${opt} in
    t)
      repository_tag=$OPTARG;;
    v)
      release_version=$OPTARG;;
    o)
      output_dir=$OPTARG;;
    c)
      cleanup=$OPTARG;;
    \?)
      echo "Missing parameters, Usage: $0 <repository_tag> <release_version> <output_dir>"
      usage
  esac
done

if [[ -z $repository_tag ]]; then
    echo "Missing '-t' (repository_tag) parameter"
    usage
    exit 1
elif [[ -z $release_version ]]; then
    echo "Missing '-v' (version) parameter"
    usage
    exit 1
elif [[ -z $output_dir ]]; then
    echo "Missing '-o' (output_dir) parameter"
    usage
    exit 1
elif [[ -z $cleanup ]]; then
    cleanup=false
fi

# Clone the repository
echo "Cloning hiero-consensus-node repo proto into './hiero-consensus-node', repository_tag: $repository_tag, release_version: $release_version, output_dir: $output_dir, cleanup: $cleanup ..."
exit 0

git clone --depth 1 --sparse --no-checkout --filter=blob:none https://github.com/hiero-ledger/hiero-consensus-node.git
cd hiero-consensus-node || { echo "Failed to change directory"; exit 1; }

git sparse-checkout init
git sparse-checkout set "hapi/hedera-protobuf-java-api/src/main/proto"
git checkout "$repository_tag"

cd ../
mkdir $output_dir

# Move CN 'block' protobuf files to the /block-node-protobuf directory, remove block_service.proto to avoid duplication
mv ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/block "$output_dir/block"
rm -f ./block-node-protobuf/block/block_service.proto

# Move CN 'platform' protobuf files to the /block-node-protobuf directory
mv ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/platform "$output_dir/platform"

# Move CN 'services' protobuf files to the /block-node-protobuf directory
mv ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/services "$output_dir/services"

# Move CN 'streams' protobuf files to the /block-node-protobuf directory
mv ./hiero-consensus-node/hapi/hedera-protobuf-java-api/src/main/proto/streams "$output_dir/streams"

# Copy BN repo protobuf files to the new directory
cp -r protobuf/src/main/proto/org/hiero/block/api/* "$output_dir/"

tar -czf "block-node-protobuf-$release_version.tgz" -C "./$output_dir" .

if [[ $cleanup ]]; then
    echo "Cleaning up intermediate directory"
    rm -rf "./$output_dir"

# clean up hiero-consensus-node clone directory
rm -rf ./hiero-consensus-node

if [ $? -eq 0 ]; then
  echo "$output_dir archive successfully created."
else
  echo "Error building archive."
fi

exit 0