# Test blocks

Tests that need real blocks do not read them from the repository. The blocks are published as OCI images in one
package, pinned by digest in `gradle.properties`, and downloaded by Gradle before tests run.

## Flavors

A flavor is one set of test blocks for one purpose, published as tags of one OCI package. Each flavor is versioned,
pinned and downloaded on its own, so a new version of one flavor never touches the others.

- The package is `hiero.testBlocks.package` in `gradle.properties`
  (`ghcr.io/hiero-ledger/hiero-block-node/test-blocks`).
- A version is `<flavor>-<YYYY.MM.DD>-<N>`, where `N` counts versions of that flavor cut on the same day, for example
  `ci-2026.11.03-1`. Tags are immutable.
- The root directory inside every image of the flavor is `/<flavor>`, and `/<flavor>/VERSION` holds the version.
- The local directory is `.test-blocks/<flavor>/` (gitignored).

Flavor names use only `a-z`, `0-9` and `-`.

| Flavor |                    Purpose                     |                                                       Used by                                                       |  Size  | Parts |
|--------|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|--------|-------|
| `ci`   | Small block sets for unit and E2E suite tests. | Every Gradle `Test` task; the `PR Checks` and `E2E Test Suites` workflows; the legacy chain of `e2e-node-operator`. | 5.6 MB | 1     |

A PR that adds a new flavor adds its row here.

### Parts

An image (each layer of it) must stay under 10 GB, so a larger flavor version is split by block range into parts of
at most 9.5 GB, one image each. All parts of a version are published, pinned and downloaded together, and never
bumped alone. Every part has the same root directory, so the parts merge into one tree.

|               |                        One part                         |                           Several parts (p1..pK)                           |
|---------------|---------------------------------------------------------|----------------------------------------------------------------------------|
| Tag           | `<flavor>-<YYYY.MM.DD>-<N>`                             | `<flavor>-<YYYY.MM.DD>-<N>-p<k>`                                           |
| Pin           | `hiero.testBlocks.image.<flavor>=<tag>@sha256:<digest>` | `hiero.testBlocks.image.<flavor>.p<k>=<tag>@sha256:<digest>`, one per part |
| Checksum file | `parts/p1.sha256`                                       | `parts/p<k>.sha256` in part `k`                                            |

For example:

```properties
hiero.testBlocks.image.bnce.p1=bnce-2026.11.20-1-p1@sha256:<digest>
hiero.testBlocks.image.bnce.p2=bnce-2026.11.20-1-p2@sha256:<digest>
hiero.testBlocks.image.bnce.p3=bnce-2026.11.20-1-p3@sha256:<digest>
```

The build fails if a flavor has both kinds of pin, if the parts are not `p1..pK` without gaps, if the `-p<k>` of a
tag differs from its key, or if the parts name different versions.

## Layout

Inside a flavor, each set of blocks has its own folder:

```
<flavor>/
  VERSION
  parts/p<k>.sha256
  <family>/<subtype>/<tps>/<env>[-<variant>]/
    manifest.json
    <block-number>.blk[.<compression>]
```

- `<family>/<subtype>`: the kind of block, for example `tss/direct` (each block carries its own TSS signature),
  `tss/indirect` (a state proof leads to a later signed block) or `wrb/v6` (wrapped record blocks, record file
  version 6). Flavors hold signed blocks only; blocks without a real proof stay in the repository with the tests
  that use them.
- `<tps>`: the transaction rate the blocks were captured at, `varied` when it is not known.
- `<env>`: where the blocks were produced: `mainnet`, `testnet` or `local` (a local consensus node, a Solo cluster or
  the in-process test harness). The optional `-<variant>` suffix keeps sets of the same kind apart, for example
  `local-cn-11-12` and `local-cn-11-12-sha2-384`.
- One file per block, named by block number. The extension gives the compression (`.blk.gz`, `.blk.zstd`, `.blk`).
  A set folder may also hold other files the tests need, such as `address-book.json`.

`parts/p<k>.sha256` lists the sha256 of every file in part `k` except `parts/`, sorted, in `sha256sum` format with
paths relative to the flavor root. It pins the content: the download is checked against every `parts/*.sha256`, and
`VERSION` must equal the pinned version. The files that are not blocks (`VERSION`, `manifest.json`, address books and
so on) are copied into every part, so they are listed in several checksum files, with the same hash.

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

Example (`ci/wrb/v6/varied/local-solo-4n/manifest.json`):

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

Nothing to do. Every `Test` task depends on `./gradlew fetchTestBlocks`, which downloads `ci` into `.test-blocks/ci/`
and passes that directory to the tests as the system property `hiero.testBlocks.dir`. The first run needs Docker and
network access; after that the flavor is downloaded again only when its pin changes.

In Java, `ResourceTestBlockBuilder.testBlocksRoot()` (block node test fixtures) resolves paths inside the flavor.

### Other flavors

```bash
./gradlew fetchTestBlocks -PtestBlocks=<flavor>[,<flavor>...]
```

downloads the given flavors (and `ci`) into `.test-blocks/<flavor>/`, all parts of a flavor merged into one
directory. The parts are pulled one at a time and each image is removed after extraction, so Docker holds at most
one part.

### On CI runners

The Block Node CI runners mount published flavors read-only, one directory per part:

```
/opt/block-node/test-blocks/<flavor>/p<k>/   <- the image's /<flavor> directory (image volume with subPath <flavor>)
```

The mount root is `hiero.testBlocks.mountDir` in `gradle.properties`. When every pinned part of a flavor is mounted
and its `VERSION` equals the pinned version, `fetchTestBlocks` fills `.test-blocks/<flavor>/` with symlinks to the
mounted files instead of pulling, and marks the directory with `.mounted`. The mounted files are not hashed again.

When a part is missing or holds another version, the build logs a warning and pulls the pinned version. So a PR that
changes a pin works before the runners mount the new version, only slower. Without a mount (a laptop, a fork), the
build pulls as usual.

Release engineering mounts a new version on request, see step 9 of [Changing test blocks](#changing-test-blocks).

### Using a local copy

```bash
./gradlew test -Phiero.testBlocks.dir=/path/to/dir
```

uses `/path/to/dir/<flavor>/` instead of `.test-blocks/<flavor>/` and downloads nothing. The directory must
already contain every flavor the build needs. Use it to try new or changed blocks before publishing them.

### Without Gradle

The images are `FROM scratch` and are never run. Extract a flavor (repeat the `docker` lines for every part, into
the same directory) with:

```bash
ref=<package>:<pin>   # from gradle.properties
docker pull "$ref"
id=$(docker create "$ref" /none)
mkdir -p <flavor> && docker cp "$id":/<flavor>/. <flavor>/
docker rm "$id"
(cd <flavor> && cat VERSION && for f in parts/*.sha256; do sha256sum -c --quiet "$f"; done)
```

### Local changes are protected

`fetchTestBlocks` writes the image references it extracted (one line per part) to `.test-blocks/<flavor>/.image-ref`.
When the pin changes, it first compares the existing directory with its `parts/*.sha256`. If files were added,
changed or removed, it fails and lists them instead of overwriting them. Publish the changes as a new flavor
version, or delete the directory.

## Changing test blocks

Published versions are immutable. Any change, including adding a set to an existing flavor, is a new version of
that flavor and a PR that changes its pins.

1. Get the current tree: `./gradlew fetchTestBlocks -PtestBlocks=<flavor>` and copy `.test-blocks/<flavor>/` to a
   work directory outside the repository, without `.image-ref`, `.mounted` and `parts/`. On a runner the directory
   may hold symlinks into the mount (see [On CI runners](#on-ci-runners)); copy it with `cp -rL` to get the files.
   For a new flavor, start from an empty `<flavor>/` directory.
2. Add or change the set folders, each with its `manifest.json`, and write the new version:
   `echo <flavor>-<YYYY.MM.DD>-<N> > <flavor>/VERSION`.
3. Split the tree into parts of at most 9.5 GB, `p1/<flavor>/` .. `pK/<flavor>/`, normalize the file modes and write
   each part's checksum file. Blocks are cut by set folder and block number; the files that are not blocks go into
   every part. A flavor under the limit gives only `p1/`.

   ```bash
   cd <work-dir>
   flavor=<flavor>
   limit=$((9500 * 1000 * 1000))
   k=1; size=0
   find "$flavor" -type f -name '*.blk*' -printf '%s %P\n' | sort -k2,2V |
     while read -r bytes path; do
       if [ "$size" -gt 0 ] && [ $((size + bytes)) -gt "$limit" ]; then k=$((k + 1)); size=0; fi
       mkdir -p "p$k/$flavor/$(dirname "$path")"
       cp "$flavor/$path" "p$k/$flavor/$path"
       size=$((size + bytes))
     done
   for part in p*; do
     (cd "$flavor" && find . -type f ! -name '*.blk*' -exec cp --parents {} "../$part/$flavor/" \;)
     chmod -R u=rwX,go=rX "$part"
     (cd "$part/$flavor" && mkdir parts &&
       find . -type f ! -path './parts/*' | LC_ALL=C sort | xargs sha256sum > "parts/$part.sha256")
   done
   ```
4. In each `p<k>/` directory, add a `Dockerfile` (use the flavor name in `COPY`):

   ```dockerfile
   FROM scratch
   LABEL org.opencontainers.image.source="https://github.com/hiero-ledger/hiero-block-node"
   LABEL org.opencontainers.image.description="Hiero Block Node test blocks: <flavor>"
   LABEL org.opencontainers.image.licenses="Apache-2.0"
   COPY <flavor> /<flavor>
   ```
5. Build each part. The flags make the build reproducible: the same tree always gives the same digest. `<tag>` is
   the version for a flavor with one part, and `<version>-p<k>` for each part otherwise.

   ```bash
   cd p<k>
   SOURCE_DATE_EPOCH=0 docker buildx build --no-cache --provenance=false --sbom=false \
     --output type=docker,name=test-blocks:<tag>,rewrite-timestamp=true .
   ```
6. Test with the local copy before pushing: merge the parts into one `<flavor>/` directory
   (`cp -r p*/<flavor> <dir>/`) and run the tests with `-Phiero.testBlocks.dir=<dir>`.
7. Push every part to the package (needs write access, see below):

   ```bash
   docker tag test-blocks:<tag> <package>:<tag>
   docker push <package>:<tag>
   ```

   The push prints the digest.

8. In `gradle.properties`, pin every part: `hiero.testBlocks.image.<flavor>=<tag>@sha256:<digest>` for one part, or
   `hiero.testBlocks.image.<flavor>.p<k>=<tag>@sha256:<digest>` for each part, and remove the pins of parts the
   new version no longer has. For a new flavor, add a row to the table above. Open a PR with these changes.

9. After the PR is merged, if the flavor is mounted on the CI runners (the load flavors; `ci` is optional), ask
   release engineering to mount the new version:

   ```
   Title: Mount test-blocks <flavor>-<YYYY.MM.DD>-<N> (<K> parts) on BN runners
   Package: ghcr.io/hiero-ledger/hiero-block-node/test-blocks
   Parts (tag / digest / size, one layer each):
     <flavor>-<YYYY.MM.DD>-<N>-p1  sha256:<...>  <n> GB  -> /opt/block-node/test-blocks/<flavor>/p1/
     <flavor>-<YYYY.MM.DD>-<N>-p2  sha256:<...>  <n> GB  -> /opt/block-node/test-blocks/<flavor>/p2/
   Mount: read-only, subPath <flavor>; replaces all parts of the previous version (remove parts that no longer exist)
   Pinned by: <PR link> (merged)
   Previous version to remove: <flavor>-<old version>
   ```

   Until the mount is updated, CI pulls the pinned version (see [On CI runners](#on-ci-runners)).

A later change will automate steps 3 to 8 with a Gradle task.

## Package

All flavors are tags of one package, `hiero.testBlocks.package`. Pulls are anonymous. Tags are immutable, and a
version pinned by `main` or a release branch is never deleted. Release engineering owns the package (retention,
cost, access); the team's maintainers push new versions.

To push, log in once with a GitHub personal access token that has the `write:packages` scope (your account needs
push rights on the package):

```bash
echo <token> | docker login ghcr.io -u <github-user> --password-stdin
```
