// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;

import com.hedera.cryptography.tss.TSS;
import com.hedera.cryptography.wraps.WRAPSVerificationKey;
import com.hedera.hapi.node.tss.LedgerIdNodeContribution;
import com.hedera.hapi.node.tss.LedgerIdPublicationTransactionBody;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import org.hiero.block.node.app.config.node.NodeConfig;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

public class TssBootstrapPlugin implements BlockNodePlugin {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /** The block node context, for access to core facilities. */
    private BlockNodeContext context;
    /** The configuration for verification */
    private NodeConfig nodeConfig;
    /** Trusted ledger ID for TSS verification, initialized from file or block 0. */
    public static Bytes activeLedgerId;
    /** Full TSS publication body used for persistence and state restoration. */
    public static LedgerIdPublicationTransactionBody activeTssPublication;
    /** True once TSS parameters have been persisted (file bootstrap or successful query peer BN). */
    public static boolean tssParametersPersisted;

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(BlockNodeContext context, ServiceBuilder serviceBuilder) {
        // setting context and config
        this.context = context;
        nodeConfig = context.configuration().getConfigData(NodeConfig.class);
        // Bootstrap TSS parameters from persisted file if available. The file contains a
        // serialized LedgerIdPublicationTransactionBody with ledger ID, address book, and WRAPS VK.
        final var tssParametersFile = nodeConfig.tssParametersFilePath();
        if (Files.exists(tssParametersFile)) {
            try {
                Bytes fileBytes = Bytes.wrap(Files.readAllBytes(tssParametersFile));
                LedgerIdPublicationTransactionBody publication =
                        LedgerIdPublicationTransactionBody.PROTOBUF.parse(fileBytes);
                initializeTssParameters(publication);
                tssParametersPersisted = true;
                LOGGER.log(INFO, "Loaded TSS parameters from file: {0}", tssParametersFile);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read TSS parameters file: " + tssParametersFile, e);
            } catch (ParseException e) {
                throw new IllegalStateException("Failed to parse TSS parameters file: " + tssParametersFile, e);
            }
        } else {
            // todo: query a peer bn for the TSS info
            // then persist the results
            persistTssParameters();
        }
    }

    private void persistTssParameters() {
        final LedgerIdPublicationTransactionBody publication = activeTssPublication;
        if (publication == null) {
            return;
        }
        final var tssParametersFile = nodeConfig.tssParametersFilePath();
        try {
            Files.createDirectories(tssParametersFile.getParent());
            Bytes serialized = LedgerIdPublicationTransactionBody.PROTOBUF.toBytes(publication);
            Files.write(tssParametersFile, serialized.toByteArray());
            tssParametersPersisted = true;
            LOGGER.log(INFO, "Persisted TSS parameters to file: {0}", tssParametersFile);
        } catch (IOException e) {
            LOGGER.log(
                    WARNING,
                    "Failed to persist TSS parameters to {0}: {1}".formatted(tssParametersFile, e.getMessage()),
                    e);
        }
    }

    /**
     * Initializes native TSS state (address book, WRAPS VK) and sets the active ledger ID
     * and TSS publication. Called from file bootstrap, block 0 processing, and tests.
     */
    public static void initializeTssParameters(@NonNull LedgerIdPublicationTransactionBody publication) {
        if (tssParametersPersisted) {
            return;
        }
        List<LedgerIdNodeContribution> contributions = publication.nodeContributions();
        int nodeCount = contributions.size();
        byte[][] publicKeys = new byte[nodeCount][];
        long[] nodeIds = new long[nodeCount];
        long[] weights = new long[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            LedgerIdNodeContribution contribution = contributions.get(i);
            publicKeys[i] = contribution.historyProofKey().toByteArray();
            nodeIds[i] = contribution.nodeId();
            weights[i] = contribution.weight();
        }
        TSS.setAddressBook(publicKeys, weights, nodeIds);
        Bytes historyProofVk = publication.historyProofVerificationKey();
        if (historyProofVk.length() > 0) {
            WRAPSVerificationKey.setCurrentKey(historyProofVk.toByteArray());
        }
        activeLedgerId = publication.ledgerId();
        activeTssPublication = publication;
    }
}
