#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Helpers to tell which block-node plugins are published to Maven at a given version. Source this file.
#
# A plugin that is new in a PR (or new since a release) cannot be resolved by the chart's resolve-plugins init
# container, which fails the whole install when any listed plugin is missing. CI therefore drops such plugins
# from the lists it hands to Maven and supplies the PR's own build of them instead.
#
# Functions:
#   is_published <plugin> <version>   exit 0 if the plugin is on Maven at <version>. <plugin> is an artifact id
#                                     or artifact:version, in which case its own version wins over <version>.
#                                     SNAPSHOT versions are checked on Sonatype, others on Maven Central.
#   names_for <names> <version>       print the comma-separated subset of <names> that is published, logging a
#                                     ::notice:: on stderr for each one that is not.
#
# Note: a failed lookup (including a network error) counts as "not published".

is_published() { # $1 = plugin (name or name:version), $2 = version
  local artifact="${1%%:*}" version="${1#*:}" url
  [[ "${version}" == "$1" ]] && version="$2"
  if [[ "${version}" == *-SNAPSHOT ]]; then
    url="https://central.sonatype.com/repository/maven-snapshots/org/hiero/block-node/${artifact}/${version}/maven-metadata.xml"
  else
    url="https://repo1.maven.org/maven2/org/hiero/block-node/${artifact}/${version}/${artifact}-${version}.pom"
  fi
  curl -fsI "${url}" >/dev/null 2>&1
}

names_for() { # $1 = names, $2 = version; prints the names published at that version
  local kept=() name entries
  IFS=',' read -ra entries <<< "$1"
  for name in ${entries[@]+"${entries[@]}"}; do
    [[ -z "${name}" ]] && continue
    if is_published "${name}" "$2"; then
      kept+=("${name}")
    else
      echo "::notice::${name} is not on Maven for $2; not resolving it" >&2
    fi
  done
  (IFS=','; echo "${kept[*]-}")
}
