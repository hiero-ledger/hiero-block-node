# Build & Run Tools CLI App
Instructions for building and running the command-line tools subproject.

Prerequisites
- Use the Gradle wrapper that's committed to the repo. From the project root use `./gradlew` (Unix) or `gradlew.bat` (Windows).

Common tasks

CLI Tools build
- Build fat JAR with dependencies (correct project path):
  - Command line: `./gradlew tools:shadowJar`
  - IntelliJ Gradle task: `tools [shadowJar]`
- Output: `tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar`
- Run CLI App:
  - Example: `java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar days --help`

Automation note (for bots and CI)
- Whenever you change any code under `tools-and-tests/tools/**`, build using the exact Gradle task above: `tools:shadowJar`.
- Do not use `tools-and-tests:tools:shadowJar` or a root-level `shadowJar` — they will not package the CLI correctly.
- If in doubt, rerun with: `./gradlew tools:build --rerun-tasks --no-build-cache`.
