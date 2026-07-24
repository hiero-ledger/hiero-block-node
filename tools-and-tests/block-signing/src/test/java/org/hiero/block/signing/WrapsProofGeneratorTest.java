// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/// Offline (re)generator for the committed genesis WRAPS proof resource.
///
/// This is NOT a normal unit test: it is skipped unless the `GENERATE_WRAPS_PROOF=true` environment
/// variable is set, because it constructs a WRAPS SNARK proof — which takes ~13 minutes and requires
/// the ~2 GB proving artifacts (`TSS_LIB_WRAPS_ARTIFACTS_PATH` pointing at
/// `decider_pp.bin`/`decider_vp.bin`/`nova_pp.bin`/`nova_vp.bin`).
///
/// Run it to (re)generate the resource — e.g. after a `hedera-cryptography` upgrade — with:
/// ```
/// TSS_LIB_WRAPS_ARTIFACTS_PATH=~/.solo/cache/wraps-v1.0.0-keys GENERATE_WRAPS_PROOF=true \
///   ./gradlew :block-signing:test --tests '*WrapsProofGeneratorTest*'
/// ```
/// then commit the updated `src/main/resources/org/hiero/block/signing/genesis-wraps-proof.bin`.
class WrapsProofGeneratorTest {

    private static final Path RESOURCE_PATH =
            Path.of("src", "main", "resources", "org", "hiero", "block", "signing", "genesis-wraps-proof.bin");

    @Test
    void regeneratesCommittedGenesisWrapsProof() throws IOException {
        assumeTrue(
                "true".equals(System.getenv("GENERATE_WRAPS_PROOF")),
                "set GENERATE_WRAPS_PROOF=true (with TSS_LIB_WRAPS_ARTIFACTS_PATH) to regenerate the WRAPS proof");

        final byte[] compressedProof = TssBlockSigner.generateGenesisWrapsProof();
        assertThat(compressedProof).hasSize(704);

        Files.createDirectories(RESOURCE_PATH.getParent());
        Files.write(RESOURCE_PATH, compressedProof);
    }
}
