#!/usr/bin/env bash

if [[ $# -lt 1 ]]; then
  echo "Usage: ${0} [version] [project_dir]"
  exit 1
fi

VERSION=$1

SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct)
PLUGINS=$(ls ../plugin-catalog | paste -sd, -)

build_image() {
  local target=$1 image="$2:${VERSION}"
  echo "Building image [${image}]"
  echo
  docker buildx build --target "${target}" --load -t "${image}" \
   --build-context distributions=../distributions \
   --build-context catalog=../plugin-catalog \
   --build-arg VERSION="${VERSION}" \
   --build-arg PLUGINS="${PLUGINS}" \
   --build-arg SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" . || exit "${?}"
  echo
  echo "Image [${image}] built successfully!"
}

build_image base-image block-node-server
build_image solo-dev block-node-server-solo-dev
build_image full block-node-server-full
build_image plugins block-node-server-plugins
