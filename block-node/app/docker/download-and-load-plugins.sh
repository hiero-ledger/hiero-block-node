#!/bin/bash

# Change 0.25.0-rc1 to ${VERSION} when versions align
if [ -n "${PLUGINS:-}" ]; then
  echo "$PLUGINS" | tr ',' '\n' | while read -r plugin; do
    curl -L -o "${BN_WORKDIR}/app-${VERSION}/lib/${plugin}.jar" \
      "https://repo1.maven.org/maven2/org/hiero/block-node/${plugin}/0.25.0-rc1/${plugin}-0.25.0-rc1.jar"
  done
fi

exec "${BN_WORKDIR}/app-${VERSION}/bin/app"
