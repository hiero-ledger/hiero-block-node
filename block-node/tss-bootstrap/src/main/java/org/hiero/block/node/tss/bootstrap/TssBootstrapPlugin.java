// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.tss.bootstrap;

import static java.lang.System.Logger.Level.INFO;

import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import org.hiero.block.api.TssData;
import org.hiero.block.node.spi.ApplicationStateFacility;
import org.hiero.block.node.spi.BlockNodeContext;
import org.hiero.block.node.spi.BlockNodePlugin;
import org.hiero.block.node.spi.ServiceBuilder;

public class TssBootstrapPlugin implements BlockNodePlugin {
    private final System.Logger LOGGER = System.getLogger(getClass().getName());
    /// The block node context, for access to core facilities.
    private BlockNodeContext context;
    /// The application state facility, for updating application state.
    private ApplicationStateFacility applicationStateFacility;
    /// The configuration for verification
    @SuppressWarnings("FieldCanBeLocal")
    private TssBootstrapConfig tssBootstrapConfig;

    /// {@inheritDoc}
    @NonNull
    @Override
    public List<Class<? extends Record>> configDataTypes() {
        return List.of(TssBootstrapConfig.class);
    }

    /// {@inheritDoc}
    @Override
    public void init(
            BlockNodeContext context,
            ServiceBuilder serviceBuilder,
            @NonNull final ApplicationStateFacility applicationStateFacility) {
        this.context = context;
        this.applicationStateFacility = Objects.requireNonNull(applicationStateFacility);
        tssBootstrapConfig = context.configuration().getConfigData(TssBootstrapConfig.class);
        TssData tssData = TssData.DEFAULT;

        final var tssParametersFile = tssBootstrapConfig.tssParametersFilePath();
        if (Files.exists(tssParametersFile)) {
            try {
                Bytes fileBytes = Bytes.wrap(Files.readAllBytes(tssParametersFile));
                tssData = TssData.PROTOBUF.parse(fileBytes);
                LOGGER.log(INFO, "Loaded TSS parameters from file: {0}", tssParametersFile);
                // todo: notify app
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read TSS parameters file: " + tssParametersFile, e);
            } catch (ParseException e) {
                throw new IllegalStateException("Failed to parse TSS parameters file: " + tssParametersFile, e);
            }
        } else {
            // get the TssData from the config
            String ledgerId64 = tssBootstrapConfig.ledgerId();
            String wrapsVerificationKey64 = tssBootstrapConfig.wrapsVerificationKey();
            if (ledgerId64 != null && wrapsVerificationKey64 != null) {
                tssData = TssData.newBuilder()
                        .ledgerId(Bytes.fromBase64(ledgerId64))
                        .wrapsVerificationKey(Bytes.fromBase64(wrapsVerificationKey64))
                        .build();
                applicationStateFacility.updateTssData(tssData);
            }
        }
    }
}
