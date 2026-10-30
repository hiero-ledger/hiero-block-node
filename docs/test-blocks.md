# Test blocks

Tests that need real blocks do not read them from the repository. The blocks are published as OCI images
(test block archives), pinned by digest in `gradle.properties`, and downloaded by Gradle before tests run.

## Archives

An archive is one OCI image holding one set of test blocks for one purpose. Each archive is versioned, pinned and
downloaded on its own, so a new version of one archive never touches the others.

One name is used everywhere. For an archive called `<archive>`:

- the image is `<registry>/<archive>`, where `<registry>` is `hiero.testBlocks.registry` in `gradle.properties`,
- the root directory inside the image is `/<archive>`,
- the local directory is `.test-blocks/<archive>/` (gitignored),
- the pin is `hiero.testBlocks.image.<archive>` in `gradle.properties`.

Archive names use only `a-z`, `0-9` and `-`.

|     Archive      |                    Purpose                     |                                                       Used by                                                       |
|------------------|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| `ci-test-blocks` | Small block sets for unit and E2E suite tests. | Every Gradle `Test` task; the `PR Checks` and `E2E Test Suites` workflows; the legacy chain of `e2e-node-operator`. |

A PR that adds a new archive adds its row here.

## Layout

Inside an archive, each set of blocks has its own folder:

```
<archive>/
  MANIFEST.sha256
  <family>/<subtype>/<tps>/<env>[-<variant>]/
    manifest.json
    <block-number>.blk[.<compression>]
```

- `<family>/<subtype>`: the kind of block, for example `tss/direct` (each block carries its own TSS signature),
  `tss/indirect` (a state proof leads to a later signed block) or `wrb/v6` (wrapped record blocks, record file
  version 6). Archives hold signed blocks only; blocks without a real proof stay in the repository with the tests
  that use them.
- `<tps>`: the transaction rate the blocks were captured at, `varied` when it is not known.
- `<env>`: where the blocks were produced: `mainnet`, `testnet` or `local` (a local consensus node, a Solo cluster or
  the in-process test harness). The optional `-<variant>` suffix keeps sets of the same kind apart, for example
  `local-cn-11-12` and `local-cn-11-12-sha2-384`.
- One file per block, named by block number. The extension gives the compression (`.blk.gz`, `.blk.zstd`, `.blk`).
  A set folder may also hold other files the tests need, such as `address-book.json`.

`MANIFEST.sha256` lists the sha256 of every other file in the archive, sorted, in `sha256sum` format with paths
relative to the archive root. It pins the content: the download is checked against it.

### `manifest.json`

Each set folder has a `manifest.json` with only what the path and the file names do not already say:

|      Field      |                                                Meaning                                                |
|-----------------|-------------------------------------------------------------------------------------------------------|
| `environment`   | `mainnet`, `testnet` or `local`.                                                                      |
| `variant`       | The folder name suffix after `<env>-`. Absent when the folder has none.                               |
| `hashAlgorithm` | `SHA2-256` or `SHA2-384`, read from the block footer hash length (32 or 48 bytes).                    |
| `hapiVersion`   | `BlockHeader.hapi_proto_version` of the blocks.                                                       |
| `source`        | Where the blocks came from.                                                                           |
| `notes`         | Optional. Anything a user of the set needs to know (proofs not verifiable, temporary set, and so on). |

Example (`ci-test-blocks/wrb/v6/varied/local-solo-4n/manifest.json`):

```json
{
  "environment": "local",
  "variant": "solo-4n",
  "hashAlgorithm": "SHA2-256",
  "hapiVersion": "0.79.0",
  "source": "Wrapped record blocks from a local 4-node Solo network, re-captured with SHA-256 hashing in #3780.",
  "notes": "Block 0 is the only one carrying tss-init metadata. Block proofs are verifiable by the current verifier. address-book.json and genesis-network.json belong to this network."
}
```

To read these facts from a new block, use the repository CLI: `blocks json` (see
[tools-and-tests/tools](../tools-and-tests/tools/README.md)) prints the block with its header and footer.

## Using test blocks

### Unit tests

Nothing to do. Every `Test` task depends on `./gradlew fetchTestBlocks`, which downloads `ci-test-blocks` into
`.test-blocks/ci-test-blocks/` and passes that directory to the tests as the system property `hiero.testBlocks.dir`.
The first run needs Docker and network access; after that the archive is downloaded again only when its pin changes.

In Java, `ResourceTestBlockBuilder.testBlocksRoot()` (block node test fixtures) resolves paths inside the archive.

### Other archives

```bash
./gradlew fetchTestBlocks -PtestBlocks=<archive>[,<archive>...]
```

downloads the given archives (and `ci-test-blocks`) into `.test-blocks/<archive>/`.

### Using a local copy

```bash
./gradlew test -Phiero.testBlocks.dir=/path/to/dir
```

uses `/path/to/dir/<archive>/` instead of `.test-blocks/<archive>/` and downloads nothing. The directory must
already contain every archive the build needs. Use it to try new or changed blocks before publishing them.

### Without Gradle

The images are `FROM scratch` and are never run. Extract one with:

```bash
ref=<registry>/<archive>@sha256:<digest>   # from gradle.properties
docker pull "$ref"
id=$(docker create "$ref" /none)
docker cp "$id":/<archive> .
docker rm "$id"
(cd <archive> && sha256sum -c --quiet MANIFEST.sha256)
```

### Local changes are protected

`fetchTestBlocks` writes the image reference it extracted to `.test-blocks/<archive>/.image-ref`. When the pin changes,
it first compares the existing directory with its `MANIFEST.sha256`. If files were added, changed or removed, it fails
and lists them instead of overwriting them. Publish the changes as a new archive version, or delete the directory.

## Changing test blocks

Published versions are immutable. Any change, including adding a set to an existing archive, is a new version of
that archive and a PR that changes its pin.

1. Get the current tree: `./gradlew fetchTestBlocks -PtestBlocks=<archive>` and copy `.test-blocks/<archive>/` to a
   work directory outside the repository (without `.image-ref`). For a new archive, start from an empty
   `<archive>/` directory.
2. Add or change the set folders, each with its `manifest.json`.
3. Normalize file modes and regenerate the content list:

   ```bash
   cd <work-dir>
   chmod -R u=rwX,go=rX <archive>
   cd <archive>
   find . -type f ! -name MANIFEST.sha256 ! -name .image-ref | LC_ALL=C sort | xargs sha256sum > MANIFEST.sha256
   ```
4. Next to the `<archive>/` directory, add a `Dockerfile`. For `ci-test-blocks` (use the archive name in `COPY`):

   ```dockerfile
   FROM scratch
   LABEL org.opencontainers.image.source="https://github.com/hiero-ledger/hiero-block-node"
   LABEL org.opencontainers.image.description="Hiero Block Node CI test blocks (regular set)"
   LABEL org.opencontainers.image.licenses="Apache-2.0"
   COPY ci-test-blocks /ci-test-blocks
   ```
5. Build. The flags make the build reproducible: the same tree always gives the same digest.

   ```bash
   SOURCE_DATE_EPOCH=0 docker buildx build --no-cache --provenance=false --sbom=false \
     --output type=docker,name=<archive>:<version>,rewrite-timestamp=true .
   ```

   `<version>` is `YYYY.MM.DD-N` (N counts versions cut on the same day). Consumers pin by digest; the tag is for
   people.

6. Test with the local copy before pushing: put the new `<archive>/` under a directory and run the tests with
   `-Phiero.testBlocks.dir=<that directory>`.

7. Push it to the registry (needs write access, see below):

   ```bash
   docker tag <archive>:<version> <registry>/<archive>:<version>
   docker push <registry>/<archive>:<version>
   ```

   The push prints the digest.

8. In `gradle.properties`, set `hiero.testBlocks.image.<archive>=<archive>:<version>@sha256:<digest>`. For a new
   archive, add a row to the table above. Open a PR with these changes.

A later change will automate steps 3 to 8 with a Gradle task.

## Registry

All archives live under one registry location, `hiero.testBlocks.registry`. Pulls are anonymous. Tags are immutable,
and a version pinned by `main` or a release branch is never deleted. Devops owns the registry: write access,
retention and cost.
