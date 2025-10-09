# Build & Run Tools CLI APP
This is the instructions for building and running the command line tools subproject.

Prerequisites
- Use the Gradle wrapper that's committed to the repo. From project root use `./gradlew` (Unix) or `gradlew.bat` (Windows).

Common tasks

CLI Tools build
- Build: `./gradlew :tools:shadowJar`
- Run CLI App: `java -jar tools-and-tests/tools/build/libs/tools-0.21.0-SNAPSHOT-all.jar <ARGUMENTS>`
