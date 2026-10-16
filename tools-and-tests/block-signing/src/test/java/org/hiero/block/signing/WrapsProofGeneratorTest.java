// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/// Offline (re)generator for the committed genesis WRAPS resource.
///
/// This is NOT a normal unit test: it is skipped unless the `GENERATE_WRAPS_PROOF=true` environment
/// variable is set. Constructing a WRAPS needs the full TSS library jar (the build resolves the smaller
/// verifier variant by default); with that variable set, `build.gradle.kts` swaps the full jar into this
/// module's test run. Construction takes about ten seconds on a laptop.
///
/// Regenerate the resource, for example after a TSS library upgrade that changes the WRAPS format, with:
/// ```
/// GENERATE_WRAPS_PROOF=true ./gradlew :block-signing:test --tests '*WrapsProofGeneratorTest*'
/// ```
/// then commit the updated `src/main/resources/org/hiero/block/signing/genesis-wraps-proof.bin`.
/// The prover is randomized, so every run writes different bytes for the same roster; each result is
/// verified before it is written. `TssBlockSigner.create()` and `TssBlockSignerTest` check that the
/// committed resource still verifies.
class WrapsProofGeneratorTest {

    private static final Path RESOURCE_PATH =
            Path.of("src", "main", "resources", "org", "hiero", "block", "signing", "genesis-wraps-proof.bin");

    /// This test aims to assert that a genesis WRAPS for the signer's deterministic roster can be
    /// constructed and verifies (the generator checks that before returning), and then writes it as the
    /// committed resource. It only runs when explicitly requested, see the class comment.
    @Test
    void regeneratesCommittedGenesisWrapsProof() throws IOException {
        assumeTrue(
                "true".equals(System.getenv("GENERATE_WRAPS_PROOF")),
                "set GENERATE_WRAPS_PROOF=true to regenerate the genesis WRAPS");

        final byte[] compressedProof = TssBlockSigner.generateGenesisWrapsProof();
        assertThat(compressedProof).isNotEmpty();

        Files.createDirectories(RESOURCE_PATH.getParent());
        Files.write(RESOURCE_PATH, compressedProof);
    }
}
