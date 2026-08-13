// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.signing.TssBlockSigner;

/**
 * Command-line entry point that materializes a length-N chain of harness-signed blocks as
 * {@code <n>.blk.gz} files on disk — used by the E2E lifecycle workflow to obtain valid
 * TSS-signed blocks without a committed fixture set.
 *
 * <p>Usage: {@code java ... GenerateHarnessBlocksMain <outputDir> <count>}
 *
 * <p>Emits {@code <outputDir>/0.blk.gz}..{@code <outputDir>/<count-1>.blk.gz}. Block 0 carries
 * a {@code LedgerIdPublicationTransactionBody} so the receiving Block Node self-provisions its
 * TSS state on the first push, matching the behaviour of the previously-committed
 * {@code CN_11_12_TSS_WRAPS/*.blk.gz} fixtures.
 */
public final class GenerateHarnessBlocksMain {

    private GenerateHarnessBlocksMain() {}

    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: GenerateHarnessBlocksMain <outputDir> <count>");
            System.exit(2);
        }
        final Path outputDir = Path.of(args[0]);
        final int count = Integer.parseInt(args[1]);
        if (count < 1) {
            System.err.println("count must be >= 1");
            System.exit(2);
        }
        Files.createDirectories(outputDir);

        final TssBlockSigner signer = TssBlockSigner.createDeterministic();
        final HarnessChainBuilder builder = HarnessChainBuilder.create(signer);
        for (long n = 0; n < count; n++) {
            final HarnessChainBuilder.Signed signed = n == 0 ? builder.genesisWithPublication() : builder.next(n);
            final Bytes bytes = BlockUnparsed.PROTOBUF.toBytes(signed.block().blockUnparsed());
            final Path file = outputDir.resolve(n + ".blk.gz");
            try (final GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(file))) {
                out.write(bytes.toByteArray());
            }
            System.out.println("Wrote " + file + " (" + bytes.length() + " bytes)");
        }
    }
}
