# Quickstart of the Tools

## Table of Contents

1. [Running locally](#running-locally)
   1. [Build the Tools](#build-the-tools)
   1. [Run the Tools](#run-the-tools)

## Running locally:

- Tools subproject qualifier: `:tools`
- Assuming your working directory is the repo root

> **NOTE:** one may use the `-p` flag for `./gradlew` in order to avoid
> specifying the target subproject repeatedly on each task when running
> multiple tasks. When running only a single task, however, it is
> recommended to use the project qualifier (i.e. `:tools:`) for
> both simplicity and clarity.

### Easy way for Unix based OSs
There is a command line script for building and running tool, which is located in the root of the repository. It has the
nice extra feature of giving you colored console output.
```
./tool.sh info --help
```

### Build the Tools

> **NOTE:** if you have not done so already, it is
> generally recommended to build the entire repo first:
> ```bash
> ./gradlew clean build -x test
> ```

1. To quickly build the Tools sources (without running tests), do the following:
   ```bash
   ./gradlew -p tools clean build -x test
   ```

### Run the Tools

1. To run the Tools, do the following:
   ```bash
   # Here is an example of running the info command with the help option, simply
   # replace `info --help` with the desired command and options to run the tools
   # quickly using the `./gradlew run` task.
   ./gradlew -q :tools:run --args="info --help"
   ```
