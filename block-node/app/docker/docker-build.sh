#!/usr/bin/env bash

BASE_URL= "https://mvnrepository.com/artifact/org.hiero.block"

func download_jar() {
  local artifact=$1
  local version=$2
  local jar_name="${artifact}-${version}.jar"
  local url="${BASE_URL}/${artifact}/${version}/${jar_name}"

  echo "Downloading ${jar_name} from ${url}..."
  curl -L -o "../distributions/${jar_name}" "${url}" || {
    echo "Failed to download ${jar_name}"
    exit 1
  }
}

if [[ $# -lt 1 ]]; then
  echo "Usage: ${0} [version] [project_dir]"
  exit 1
fi

VERSION=$1

echo "Building image [block-node-server:${VERSION}]"
echo

SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct)
# run docker build
docker buildx build --load -t "block-node-server:${VERSION}" \
 --build-context distributions=../distributions \
 --build-arg VERSION="${VERSION}" \
 --build-arg SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH}" . || exit "${?}"

echo
echo "Image [block-node-server:${VERSION}] built successfully!"
