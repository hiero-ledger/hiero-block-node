// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import static java.util.Objects.requireNonNull;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.TssData;

/// Carries the material a verifier must be provisioned with to accept proofs from a [BlockSigner].
///
/// Exactly one arm is populated: [#tssData] for the TSS scheme (fed to the verifier's context /
/// `PluginTestBase.updateTssData`), or [#addressBookHistory] for the RSA scheme (fed via
/// `PluginTestBase.updateAddressBookHistory`). Both can also be written to the E2E bootstrap files
/// read from `app.state.tssBootstrapFilePath` / `app.state.rsaBootstrapFilePath`.
public record VerificationMaterial(
        @Nullable TssData tssData, @Nullable RangedAddressBookHistory addressBookHistory) {

    /// Creates TSS verification material.
    @NonNull
    public static VerificationMaterial ofTss(@NonNull final TssData tssData) {
        return new VerificationMaterial(requireNonNull(tssData), null);
    }

    /// Creates RSA verification material.
    @NonNull
    public static VerificationMaterial ofRsa(@NonNull final RangedAddressBookHistory addressBookHistory) {
        return new VerificationMaterial(null, requireNonNull(addressBookHistory));
    }

    /// Writes [#tssData] as JSON to the `app.state.tssBootstrapFilePath` bootstrap file.
    public void writeTssBootstrapFile(@NonNull final Path path) throws IOException {
        final Bytes json = TssData.JSON.toBytes(requireNonNull(tssData, "no tssData present"));
        Files.write(path, json.toByteArray());
    }

    /// Writes [#addressBookHistory] as JSON to the `app.state.rsaBootstrapFilePath` bootstrap file.
    public void writeRsaBootstrapFile(@NonNull final Path path) throws IOException {
        final Bytes json = RangedAddressBookHistory.JSON.toBytes(
                requireNonNull(addressBookHistory, "no addressBookHistory present"));
        Files.write(path, json.toByteArray());
    }
}
