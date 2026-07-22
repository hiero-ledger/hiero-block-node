// SPDX-License-Identifier: Apache-2.0
/// Test-only library that produces real, verifiable block-proof signatures — RSA record-file (WRB)
/// proofs and TSS/hinTS threshold block signatures — together with the verification material
/// (`TssData` / `RangedAddressBookHistory`) needed to provision a matching verifier.
///
/// This module provides no `BlockNodePlugin` and is never on the production
/// app module graph; it exists only to drive verification from unit, integration, and E2E tests.
module org.hiero.block.signing {
    exports org.hiero.block.signing;

    requires transitive com.hedera.pbj.runtime;
    requires transitive org.hiero.block.protobuf.pbj;
    requires com.hedera.cryptography.hints;
    requires com.hedera.cryptography.wraps;
    requires org.hiero.block.common;
    requires static com.github.spotbugs.annotations;
}
