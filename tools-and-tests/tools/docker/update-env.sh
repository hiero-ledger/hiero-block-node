#!/usr/bin/env bash

# This scripts create a '.env' file that is used for docker & docker-compose as an input of environment variables.
# This script is called by gradle and get the current project version as an input param

if [ $# -lt 1 ]; then
  # <VERSION> is required!
  echo "USAGE: $0 <VERSION>"
  exit 1
fi

project_version=$1

echo "VERSION=$project_version" > .env
