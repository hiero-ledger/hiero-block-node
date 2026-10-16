// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.harness;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.zip.GZIPOutputStream;
import org.hiero.block.common.hasher.HashAlgorithm;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.blocks.TestBlockBuilder;
import org.hiero.block.signing.TssBlockSigner;

/**
 * Command-line entry point that materializes a length-N chain of harness-signed blocks as
 * {@code <n>.blk.gz} files on disk, used by the E2E lifecycle workflow to obtain valid
 * TSS-signed blocks without a committed fixture set.
 *
 * <p>Usage: {@code java ... GenerateHarnessBlocksMain <outputDir> <count> [algorithm]}, where the
 * optional algorithm is a {@link HashAlgorithm} constant name and defaults to
 * {@link TestBlockBuilder#BLOCK_HASH_ALGORITHM}. Every emitted header declares the algorithm the
 * chain is hashed with. The blocks carry the TSS block proofs of the TSS library this build uses,
 * so a release on an earlier TSS library cannot verify them whatever the algorithm; the E2E
 * lifecycle workflow keeps committed chains for such releases.
 *
 * <p>Emits {@code <outputDir>/0.blk.gz}..{@code <outputDir>/<count-1>.blk.gz}. Block 0 carries
 * a {@code LedgerIdPublicationTransactionBody} so the receiving Block Node self-provisions its
 * TSS state on the first push, matching the behaviour of a consensus node's genesis block. The
 * signer's TSS data is also written as {@code <outputDir>/tss-bootstrap-roster.json}, the file a
 * Block Node reads from {@code app.state.tssBootstrapFilePath}, for workflows that provision a
 * node holding TSS data of another network (the reset step of the E2E lifecycle workflow).
 */
public final class GenerateHarnessBlocksMain {

    /** File name of the emitted TSS bootstrap file. */
    public static final String BOOTSTRAP_FILE_NAME = "tss-bootstrap-roster.json";

    private GenerateHarnessBlocksMain() {}

    public static void main(final String[] args) throws IOException, NoSuchAlgorithmException {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: GenerateHarnessBlocksMain <outputDir> <count> [algorithm]");
            System.exit(2);
        }
        final Path outputDir = Path.of(args[0]);
        final int count = Integer.parseInt(args[1]);
        final HashAlgorithm algorithm =
                args.length == 3 ? HashAlgorithm.valueOf(args[2]) : TestBlockBuilder.BLOCK_HASH_ALGORITHM;
        if (count < 1) {
            System.err.println("count must be >= 1");
            System.exit(2);
        }
        Files.createDirectories(outputDir);

        final TssBlockSigner signer = TssBlockSigner.create();
        final HarnessChainBuilder builder = HarnessChainBuilder.create(signer, algorithm);
        for (long n = 0; n < count; n++) {
            final HarnessChainBuilder.Signed signed = n == 0 ? builder.genesisWithPublication() : builder.next(n);
            final Bytes bytes = BlockUnparsed.PROTOBUF.toBytes(signed.block().blockUnparsed());
            final Path file = outputDir.resolve(n + ".blk.gz");
            try (final GZIPOutputStream out = new GZIPOutputStream(Files.newOutputStream(file))) {
                out.write(bytes.toByteArray());
            }
            System.out.println("Wrote " + file + " (" + bytes.length() + " bytes)");
        }
        final Path bootstrapFile = outputDir.resolve(BOOTSTRAP_FILE_NAME);
        signer.verificationMaterial().writeTssBootstrapFile(bootstrapFile);
        System.out.println("Wrote " + bootstrapFile);
    }
}
