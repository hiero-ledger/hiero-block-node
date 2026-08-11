// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app.fixtures.blocks;

import static org.hiero.block.node.app.fixtures.blocks.TestBlock.MAX_BLOCK_MESSAGE_DEPTH;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import org.hiero.block.internal.BlockUnparsed;
import org.hiero.block.node.app.fixtures.TestUtils;

public class ResourceTestBlockBuilder {
    /// A simple interface to define a resource block identifier enum
    public interface ResourceBlock {
        String resourceName();

        Bytes blockRootHash();

        long blockNumber();

        default BlockUnparsed loadBlock() throws IOException, ParseException {
            try (final InputStream stream =
                            TestUtils.class.getModule().getResourceAsStream("test-blocks/" + resourceName());
                    final GZIPInputStream gzipInputStream = new GZIPInputStream(stream)) {
                final byte[] bytes = gzipInputStream.readAllBytes();
                return BlockUnparsed.PROTOBUF.parse(
                        Bytes.wrap(bytes).toReadableSequentialData(),
                        false,
                        true,
                        MAX_BLOCK_MESSAGE_DEPTH,
                        Integer.MAX_VALUE);
            }
        }
    }

    /// TSS WRAPS test blocks.
    public enum WRAPS implements ResourceBlock {
        /// Genesis block — bootstraps TSS parameters and ledger ID.
        BLOCK_0(
                "CN_11_12_TSS_WRAPS/0.blk.gz",
                "3c421aac698b04fccdd46acad435125ea53af91fe404e787c8ccf4c23a11aa69190468ea1dd797af11d75335f4840749",
                0),
        /// Sequential block 1 (pre-settled Schnorr signature).
        BLOCK_1(
                "CN_11_12_TSS_WRAPS/1.blk.gz",
                "832ec52cfbb467c8d1373afefe682b27132c31ebaf4846fbb6e437c582ce09ea2a59c1d7aadc26827e969efc79a45c89",
                1),
        /// Sequential block 2 (pre-settled Schnorr signature).
        BLOCK_2(
                "CN_11_12_TSS_WRAPS/2.blk.gz",
                "cb1c9b37ed6ae0f34b771f9f7692f266a1f77f7caf14669f359373137cc7986f0aef9ad17e8a63303c8b5e08423dc407",
                2),
        /// Sequential block 3 (pre-settled Schnorr signature).
        BLOCK_3(
                "CN_11_12_TSS_WRAPS/3.blk.gz",
                "61d26789479764f622932348931833d2dda482bbf3f798e727e31bb50fecd76db894c256b1ebb11516413e3d08777cfd",
                3),
        /// Sequential block 4 (pre-settled Schnorr signature).
        BLOCK_4(
                "CN_11_12_TSS_WRAPS/4.blk.gz",
                "a6d563cf11b04e5884958223169cbdd080f27425628584db76b0447982e869ebac08e9f8ecaa03d6b8aa225e8e188eb4",
                4),
        /// Transition block — first block with WRAPS signature (Schnorr to WRAPS transition).
        BLOCK_322(
                "CN_11_12_TSS_WRAPS/322.blk.gz",
                "15c9a3e7d027c112e3378a1a66277e5b1e3d9b1aa4ec67749ad3ef0e4d99859e0784672c8c5f838717a5da8e6fcbf67f",
                322),
        /// Post-settled block — has WRAPS signature (settled TSS).
        BLOCK_391(
                "CN_11_12_TSS_WRAPS/391.blk.gz",
                "b850faadc920c095b77523520803a99c1387ebaeeb2a150aa3381b68ccfb58bf35ff8d5ade56b7313ad9f40baca1103e",
                391);
        private final String resourceName;
        private final Bytes blockRootHash;
        private final long blockNumber;

        WRAPS(final String resourceName, final String blockRootHash, final long blockNumber) {
            this.resourceName = resourceName;
            this.blockRootHash = Bytes.fromHex(blockRootHash);
            this.blockNumber = blockNumber;
        }

        @Override
        public String resourceName() {
            return resourceName;
        }

        @Override
        public Bytes blockRootHash() {
            return blockRootHash;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }
    }

    /// Sample wrapped record blocks (WRB) for the V6 `SignedRecordFileProof` verification path.
    /// Each constant maps to a `test-blocks/WRB/<network>/<blockNumber>.blk.gz` resource and
    /// carries the name of the network folder so callers can fetch the matching
    /// [NodeAddressBook] via [#loadAddressBook(String)].
    public enum WRB implements ResourceBlock {
        /// Solo-network genesis WRB block — the only one in this batch containing tss-init metadata.
        SOLO_4N_BLOCK_0(
                "WRB/SOLO_4N/0.blk.gz",
                "cc2c7c849e9863458d2deb779dcca9563bec6ef4943b5709d8623803af78f9372d5e38ce8f9ae594fedc74909aece65d",
                0,
                "SOLO_4N"),
        SOLO_4N_BLOCK_1(
                "WRB/SOLO_4N/1.blk.gz",
                "8b70cecb8deabc57acb12fa8696bcc27e696b7b1ede3febc2e3f0aaeef422bd55a043b319f326cbecbd886852f392b7a",
                1,
                "SOLO_4N"),
        SOLO_4N_BLOCK_2(
                "WRB/SOLO_4N/2.blk.gz",
                "6cfb9066c335aebe27563d8b7ffc2fae0baad4898880e82753827a9147b3b63455bc5fc7b794adeda0810198d717ee30",
                2,
                "SOLO_4N"),
        SOLO_4N_BLOCK_3(
                "WRB/SOLO_4N/3.blk.gz",
                "6204e2c36e154b152d43e9e5322346ef4691080d70bb589c8609bef12f63e4ccc1cfa386a27ab66a16c80679f368ac9f",
                3,
                "SOLO_4N"),
        SOLO_4N_BLOCK_4(
                "WRB/SOLO_4N/4.blk.gz",
                "32806f4d9977cf41ba2750e9b4254545c19870ff01a1783654b50429b201f2a310bae9cc549821f263935454de88bc43",
                4,
                "SOLO_4N");
        private final String resourceName;
        private final Bytes blockRootHash;
        private final long blockNumber;
        private final String network;

        WRB(final String resourceName, final String blockRootHash, final long blockNumber, final String network) {
            this.resourceName = resourceName;
            this.blockRootHash = Bytes.fromHex(blockRootHash);
            this.blockNumber = blockNumber;
            this.network = network;
        }

        @Override
        public String resourceName() {
            return resourceName;
        }

        @Override
        public Bytes blockRootHash() {
            return blockRootHash;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }

        /// Network folder for this fixture (also the `<network>` argument to [#loadAddressBook(String)]).
        public String network() {
            return network;
        }

        /// Load the address book based on network.
        public NodeAddressBook loadAddressBook() throws IOException, ParseException {
            final String resourcePath = "test-blocks/WRB/" + network + "/address-book.json";
            try (final InputStream stream = TestUtils.class.getModule().getResourceAsStream(resourcePath)) {
                if (stream == null) {
                    throw new IOException("Address book fixture not found on classpath: " + resourcePath);
                }
                return NodeAddressBook.JSON.parse(Bytes.wrap(stream.readAllBytes()));
            }
        }
    }

    /// Sample blocks containing state proofs from a hapiTestWraps capture with Schnorr TSS signatures.
    /// Every 5th block (0, 5, ...) is directly signed with Schnorr; blocks in between carry
    /// state proofs referencing the next signed block. Block 0 contains
    /// LedgerIdPublicationTransactionBody for TSS initialization.
    public enum StateProof implements ResourceBlock {
        /// Genesis block — bootstraps TSS parameters and ledger ID. Direct Schnorr proof.
        BLOCK_0(
                "CN_11_12_TSS_SCHNORR/0.blk.gz",
                "85d7117a94f091156a97e1f94fb11b640c12431527106e50aa7ce8e4e02af287f3f28591dc43d4c3942a67e7064e4148",
                0),
        /// Indirect proof — 4-gap state proof, references signed block 5.
        BLOCK_1(
                "CN_11_12_TSS_SCHNORR/1.blk.gz",
                "9799848eff3420617d5c2eb8e092f903bde6de8662887105f3bd17363d7c1cd3629814f9fc58a9d8f5e1e6ed3fdd53c1",
                1),
        /// Indirect proof — 3-gap state proof, references signed block 5.
        BLOCK_2(
                "CN_11_12_TSS_SCHNORR/2.blk.gz",
                "0facbf0e5d94d4576635796c6be83f2d16203e6be633e475b70942f2b22c880944c156e1badcd27be96ffc7ee7b7e194",
                2),
        /// Indirect proof — 2-gap state proof, references signed block 5.
        BLOCK_3(
                "CN_11_12_TSS_SCHNORR/3.blk.gz",
                "9d1ac9ba0c57558ab99691f97414d19698e068ff6d600502c3896fccfe3f58c261515f00dc3c116107961385be9f8eec",
                3),
        /// Indirect proof — 1-gap state proof, references signed block 5.
        BLOCK_4(
                "CN_11_12_TSS_SCHNORR/4.blk.gz",
                "e313796875ef100613684c0ee4ef1a80a13b73aa9c97f5f9592f9b9a25c798b33e36e397fa0e1456b17cee2155b8d7ac",
                4),
        /// Direct Schnorr TSS proof — the signed block referenced by blocks 1-4.
        BLOCK_5(
                "CN_11_12_TSS_SCHNORR/5.blk.gz",
                "1c417b370965dd85e274b568694d0a5b3325b39f619a840df0161339864a30e389ba3aec4c7763236d2f4c505d031921",
                5);
        private final String resourceName;
        private final Bytes blockRootHash;
        private final long blockNumber;

        StateProof(final String resourceName, final String blockRootHash, final long blockNumber) {
            this.resourceName = resourceName;
            this.blockRootHash = Bytes.fromHex(blockRootHash);
            this.blockNumber = blockNumber;
        }

        @Override
        public String resourceName() {
            return resourceName;
        }

        @Override
        public Bytes blockRootHash() {
            return blockRootHash;
        }

        @Override
        public long blockNumber() {
            return blockNumber;
        }
    }

    public static ResourceTestBlock load(final WRAPS wrapsBlock) throws IOException, ParseException {
        return new ResourceTestBlock(wrapsBlock.blockNumber(), wrapsBlock.loadBlock(), wrapsBlock.blockRootHash());
    }

    public static List<ResourceTestBlock> loadMultiple(final WRAPS... wrapsBlocks) throws IOException, ParseException {
        final List<ResourceTestBlock> result = new ArrayList<>();
        for (final WRAPS wrapsBlock : wrapsBlocks) {
            result.add(load(wrapsBlock));
        }
        return result;
    }

    public static ResourceTestWRBBlock load(final WRB wrbBlock) throws IOException, ParseException {
        return new ResourceTestWRBBlock(
                wrbBlock.blockNumber(), wrbBlock.loadBlock(), wrbBlock.blockRootHash(), wrbBlock.loadAddressBook());
    }

    public static List<ResourceTestWRBBlock> loadMultiple(final WRB... wrbBlocks) throws IOException, ParseException {
        final List<ResourceTestWRBBlock> result = new ArrayList<>();
        for (final WRB wrbBlock : wrbBlocks) {
            result.add(load(wrbBlock));
        }
        return result;
    }

    public static ResourceTestBlock load(final StateProof stateProofBlock) throws IOException, ParseException {
        return new ResourceTestBlock(
                stateProofBlock.blockNumber(), stateProofBlock.loadBlock(), stateProofBlock.blockRootHash());
    }

    public static List<ResourceTestBlock> loadMultiple(final StateProof... stateProofBlocks)
            throws IOException, ParseException {
        final List<ResourceTestBlock> result = new ArrayList<>();
        for (final StateProof stateProofBlock : stateProofBlocks) {
            result.add(load(stateProofBlock));
        }
        return result;
    }
}
