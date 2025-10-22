#!/usr/bin/env bash

if [[ $# -lt 1 ]]; then
  echo "Usage: ${0} [version] [arch (amd64|arm64)]"
  exit 1
fi

VERSION=$1
ARCH=$2
[[ -n "$ARCH" ]] && PLATFORM_ARG="--platform linux/${ARCH}"

echo "Building image [block-tools:${VERSION}]"
echo

SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct)
# run docker build
docker buildx build --load -t "block-tools:${VERSION}" \
 ${PLATFORM_ARG} \
 --build-context distributions=../distributions \
 --build-arg VERSION="${VERSION}" \
 --build-arg SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" . || exit "${?}"

echo
echo "Image [block-tools:${VERSION}] built successfully!"
