#!/usr/bin/env bash

if [[ $# -lt 1 ]]; then
  echo "Usage: ${0} [version] [project_dir]"
  exit 1
fi

VERSION=$1

echo "Building image [block-node-server:${VERSION}]"
echo

SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct)

# run docker build for default stage
docker buildx build --target base-image --load -t "block-node-server:${VERSION}" \
 --build-context distributions=../distributions \
 --build-arg VERSION="${VERSION}" \
 --build-arg SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" . || exit "${?}"

echo
echo "Image [block-node-server:${VERSION}] built successfully!"

# run docker build for dev stage
docker buildx build --target dev --load -t "block-node-server-dev:${VERSION}" \
 --build-context distributions=../distributions \
 --build-arg VERSION="${VERSION}" \
 --build-arg SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" . || exit "${?}"

echo
echo "Image [block-node-server-dev:${VERSION}] built successfully!"
