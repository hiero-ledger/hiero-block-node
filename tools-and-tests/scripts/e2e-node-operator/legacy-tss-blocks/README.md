# Legacy TSS harness blocks

Blocks 0 to 2 of the test harness chain, signed with the TSS library the Block Node used before 0.46.0
(hedera-cryptography 3.13.0). The node operator lifecycle workflow (`.github/workflows/e2e-node-operator.yaml`)
installs the latest released Block Node and pushes these blocks to it before upgrading to the snapshot. A
release from before 0.46.0 can only verify TSS block proofs of that library, and the snapshot's harness
(`:block-verification:generateHarnessBlocks`) can no longer sign them, so they are kept here.

|   folder    | hash algorithm | used for a base release |
|-------------|----------------|-------------------------|
| `sha2-384/` | SHA-384        | before 0.45.0           |
| `sha2-256/` | SHA-256        | 0.45.x                  |

Block 0 carries the `LedgerIdPublication` the base release provisions its TSS data from. After the base phase,
the workflow resets the TSS bootstrap file on the state volume to the snapshot's before the upgrade, as an
operator does when upgrading across the TSS library change.

Generated on 2026-10-15 at commit `c8d1bafbb` (hedera-cryptography 3.13.0) with:

```
./gradlew :block-verification:generateHarnessBlocks -PoutputDir=<folder> -Pcount=3 -Palgorithm=SHA2_256
./gradlew :block-verification:generateHarnessBlocks -PoutputDir=<folder> -Pcount=3 -Palgorithm=SHA2_384
```

|        file         | size (bytes) |                               sha256                               |
|---------------------|--------------|--------------------------------------------------------------------|
| `sha2-256/0.blk.gz` | 4163         | `2db09e629a9e3a28b1026aa8847e91cf94478f95fa0b66243139f489a2f65821` |
| `sha2-256/1.blk.gz` | 2178         | `1661c75fd351749a3a6cf9479b8578bb3d28cc183082c81833b47baa2d24f0ce` |
| `sha2-256/2.blk.gz` | 2212         | `e9da8dd3f94c6f6bb990547a761dcb02c6d0410d8087be166e1d954b3b20f949` |
| `sha2-384/0.blk.gz` | 4180         | `08b64870bccb391abed9c5988cd1ecf70adf3c0c24b7b36d790ca58d1519a2d5` |
| `sha2-384/1.blk.gz` | 2217         | `44522c5523d35338c41396e64a68905eca0ecef327811c26796783fa653fcce9` |
| `sha2-384/2.blk.gz` | 2263         | `028597fff4d2f601b5b0f7fa6ebd331df775de9a991c45016309cf4ae543715e` |

Remove this folder, and the `BASE_TSS_ERA` gate in the workflow, once the latest released Block Node is 0.46.0
or newer.
