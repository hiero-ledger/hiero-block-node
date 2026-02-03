# Build & run (development)

Prerequisites
- Use the Gradle wrapper that's committed to the repo. From project root use `./gradlew` (Unix) or `gradlew.bat` (Windows).

Common tasks

Root build (all modules)
- Build: `./gradlew build`
- Run tests: `./gradlew test`

The project is made up of several apps/modules. They each have their own README and BUILDING instructions:
- `/block-node`: The Block Node Server implementation.
- `/common`: Common library used by both the Block Node and the Simulator.
- `/tools-and-tests/protobuf-protoc`: The HAPI API protobuf compiled with the Google protoc compiler.
- `/tools-and-tests/simulator`: A simulator for the Block Node, which can be used to test the Block Node in a local environment.
- [/tools-and-tests/tools](tools-and-tests/tools/BUILDING.md): A set of command line tools for working with Block Stream files.
- `/tools-and-tests/suites`: A set of e2e tests that can be used to verify the correctness of the Block Node.
