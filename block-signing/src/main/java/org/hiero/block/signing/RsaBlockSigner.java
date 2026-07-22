// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.signing;

import com.hedera.hapi.block.stream.BlockProof;
import com.hedera.hapi.block.stream.RecordFileSignature;
import com.hedera.hapi.block.stream.SignedRecordFileProof;
import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.common.hasher.HashingUtilities;

/// Produces version 6 record-file (WRB) [BlockProof]s signed with `SHA384withRSA`.
///
/// Each node signs the version 6 payload `SHA-384(int32be(6) || record_file_contents)` (see
/// [HashingUtilities#computeV6SignedPayload]). The [#verificationMaterial] is a
/// [RangedAddressBookHistory] with one open-ended era carrying every node's RSA public key as a
/// hex-encoded DER `SubjectPublicKeyInfo` — exactly what the verifier decodes from
/// `NodeAddress.rsaPubKey`.
public final class RsaBlockSigner implements BlockSigner {

    private static final int RECORD_FILE_PROOF_VERSION = 6;
    private static final int RSA_KEY_SIZE = 3072;
    private static final String KEY_ALGORITHM = "RSA";
    private static final String SIGNATURE_ALGORITHM = "SHA384withRSA";

    private final List<RsaNode> nodes;
    private final VerificationMaterial verificationMaterial;

    private RsaBlockSigner(@NonNull final List<RsaNode> nodes) {
        this.nodes = List.copyOf(nodes);
        this.verificationMaterial = VerificationMaterial.ofRsa(buildHistory(this.nodes));
    }

    /// Creates a single-node RSA signer.
    @NonNull
    public static RsaBlockSigner create() {
        return create(1);
    }

    /// Creates an RSA signer with `nodeCount` nodes (node ids `0..nodeCount-1`, all fresh keypairs).
    @NonNull
    public static RsaBlockSigner create(final int nodeCount) {
        if (nodeCount < 1) {
            throw new IllegalArgumentException("nodeCount must be >= 1, got " + nodeCount);
        }
        try {
            final KeyPairGenerator generator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
            generator.initialize(RSA_KEY_SIZE, new SecureRandom());
            final List<RsaNode> nodes = new ArrayList<>(nodeCount);
            for (int nodeId = 0; nodeId < nodeCount; nodeId++) {
                final KeyPair keyPair = generator.generateKeyPair();
                nodes.add(new RsaNode(nodeId, keyPair.getPrivate(), keyPair.getPublic()));
            }
            return new RsaBlockSigner(nodes);
        } catch (final GeneralSecurityException fatal) {
            throw new IllegalStateException("Failed to generate RSA key material", fatal);
        }
    }

    @Override
    @NonNull
    public BlockProof signBlockProof(final long blockNumber, @NonNull final Bytes recordFileContents) {
        final byte[] payload = HashingUtilities.computeV6SignedPayload(recordFileContents);
        final List<RecordFileSignature> signatures = new ArrayList<>(nodes.size());
        for (final RsaNode node : nodes) {
            signatures.add(RecordFileSignature.newBuilder()
                    .signaturesBytes(Bytes.wrap(node.sign(payload)))
                    .nodeId(node.nodeId())
                    .build());
        }
        return BlockProof.newBuilder()
                .block(blockNumber)
                .signedRecordFileProof(SignedRecordFileProof.newBuilder()
                        .version(RECORD_FILE_PROOF_VERSION)
                        .recordFileSignatures(signatures)
                        .build())
                .build();
    }

    @Override
    @NonNull
    public VerificationMaterial verificationMaterial() {
        return verificationMaterial;
    }

    @NonNull
    private static RangedAddressBookHistory buildHistory(@NonNull final List<RsaNode> nodes) {
        final List<NodeAddress> addresses = new ArrayList<>(nodes.size());
        for (final RsaNode node : nodes) {
            addresses.add(NodeAddress.newBuilder()
                    .nodeId(node.nodeId())
                    .rsaPubKey(HexFormat.of().formatHex(node.publicKey().getEncoded()))
                    .build());
        }
        final NodeAddressBook book =
                NodeAddressBook.newBuilder().nodeAddress(addresses).build();
        return RangedAddressBookHistory.newBuilder()
                .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                        .addressBook(book)
                        .startBlock(0L)
                        .endBlock(-1L)
                        .build()))
                .build();
    }

    /// One RSA node: its id, signing key, and public key (published in the address book).
    private record RsaNode(long nodeId, PrivateKey privateKey, PublicKey publicKey) {
        byte[] sign(final byte[] payload) {
            try {
                final Signature engine = Signature.getInstance(SIGNATURE_ALGORITHM);
                engine.initSign(privateKey);
                engine.update(payload);
                return engine.sign();
            } catch (final GeneralSecurityException fatal) {
                throw new IllegalStateException("Failed to RSA-sign the record-file payload", fatal);
            }
        }
    }
}
