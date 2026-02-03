#!/bin/bash
# SPDX-License-Identifier: Apache-2.0
# run gradle jar build and send output to /dev/null
echo "building..."
./gradlew -q tool:shadowJar > /dev/null
# check if last command failed and exit if so
if [ $? -ne 0 ]; then
  echo "Build failed"
  exit 1
fi
# find the jar name in the build/libs directory
JAR=$(find tools-and-tests/tools/build/libs -name 'tools-*-all.jar')
# run the command line tool built jar file forwarding all arguments
java -jar $JAR "$@"
