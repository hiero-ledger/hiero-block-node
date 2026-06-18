// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.backfill;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.hiero.block.internal.BlockNodeSource;
import org.hiero.block.internal.BlockNodeSourceConfig;

/**
 * Utility for converting configured backfill sources into the {@link NetworkData} representation that is
 * published to the Application State facility (and reported by the {@code /statusz} endpoints).
 *
 * <p>Each backfill source becomes a {@link NetworkConnection} whose {@code remote} is the source endpoint,
 * whose {@code local} is the wildcard self endpoint {@code $:*}, whose {@code category} is {@code partner},
 * and which carries no certificate.
 */
final class BackfillNetworkData {
    private static final String BACKFILL_DEFAULT_CATEGORY = "partner";
    private static final String DEFAULT_TLS_SCHEME = "grpcs";
    private static final String DEFAULT_NON_TLS_SCHEME = "grpc";

    private BackfillNetworkData() {
        // utility class; not instantiable
    }

    /**
     * Converts the configured backfill sources into a {@link NetworkData}.
     *
     * @param sources the parsed backfill sources
     * @param tlsEnabled whether TLS is required (from backfill configuration)
     * @return the equivalent NetworkData
     */
    static NetworkData toNetworkData(@NonNull final BlockNodeSource sources, final boolean tlsEnabled) {
        final List<NetworkConnection> connections =
                new ArrayList<>(sources.nodes().size());
        for (final BlockNodeSourceConfig node : sources.nodes()) {
            connections.add(toNetworkConnection(node, tlsEnabled));
        }
        return NetworkData.newBuilder().activeEndpoints(connections).build();
    }

    /**
     * Converts a single backfill source into a {@link NetworkConnection}. The {@code scheme} and
     * {@code protocol} come from the source configuration, falling back to {@code http(s)} (per the backfill
     * TLS setting) and {@code TCP} respectively when unset.
     *
     * @param node the backfill source configuration
     * @param tls whether TLS is required (from backfill configuration)
     * @return the equivalent NetworkConnection
     */
    static NetworkConnection toNetworkConnection(@NonNull final BlockNodeSourceConfig node, final boolean tls) {
        final String scheme =
                node.scheme().isBlank() ? (tls ? DEFAULT_TLS_SCHEME : DEFAULT_NON_TLS_SCHEME) : node.scheme();
        final IpProtocol protocol = node.protocol() == IpProtocol.UNIDENTIFIED ? IpProtocol.TCP : node.protocol();
        return NetworkConnection.newBuilder()
                .remote(new ConnectionReference(node.address(), Integer.toString(node.port())))
                .local(new ConnectionReference("$", "*"))
                .category(BACKFILL_DEFAULT_CATEGORY)
                .scheme(scheme)
                .protocol(protocol)
                .tlsRequired(tls)
                .certificate(Bytes.EMPTY)
                .build();
    }
}
