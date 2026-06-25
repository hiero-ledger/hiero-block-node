// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.nio.charset.StandardCharsets;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.hiero.block.node.app.fixtures.plugintest.TestApplicationStateFacility;
import org.hiero.block.node.app.fixtures.plugintest.TestServerRequest;
import org.hiero.block.node.app.fixtures.plugintest.TestServerResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class InboundStatusHandlerTest {

    private static NetworkData singleEndpoint(final String category, final String remoteAddress) {
        return NetworkData.newBuilder()
                .activeEndpoints(NetworkConnection.newBuilder()
                        .local(new ConnectionReference("$", "*"))
                        .remote(new ConnectionReference(remoteAddress, "40840"))
                        .category(category)
                        .scheme("grpcs")
                        .protocol(IpProtocol.TCP)
                        .tlsRequired(true)
                        .certificate(Bytes.EMPTY)
                        .build())
                .build();
    }

    private static NetworkData parse(final TestServerResponse response) throws Exception {
        return NetworkData.JSON.parse(Bytes.wrap(((String) response.sentEntity()).getBytes(StandardCharsets.UTF_8)));
    }

    private static boolean containsRemote(final NetworkData data, final String address) {
        return data.activeEndpoints().stream()
                .anyMatch(c -> c.remote().address().equals(address));
    }

    @Test
    @DisplayName("Inbound merges publishers, inbound partners and backfill sources (but not outbound partners)")
    void mergesInboundSets() throws Exception {
        final TestApplicationStateFacility appState = new TestApplicationStateFacility();
        appState.setKnownPublishers(singleEndpoint("publisher", "pub.example.com"));
        appState.setInboundPartners(singleEndpoint("partner", "inpartner.example.com"));
        appState.setBackfillSources(singleEndpoint("partner", "backfill.example.com"));
        appState.setOutboundPartners(singleEndpoint("partner", "outpartner.example.com"));

        final TestServerResponse response = new TestServerResponse();
        new InboundStatusHandler(new TestServerRequest("/statusz/inbound"), response, appState).createAndSendResponse();

        assertEquals(200, response.sentStatus());
        assertEquals("application/json", response.contentType());

        final NetworkData sent = parse(response);
        assertEquals(3, sent.activeEndpoints().size());
        assertTrue(containsRemote(sent, "pub.example.com"));
        assertTrue(containsRemote(sent, "inpartner.example.com"));
        assertTrue(containsRemote(sent, "backfill.example.com"));
        assertFalse(containsRemote(sent, "outpartner.example.com"));
    }

    @Test
    @DisplayName("Inbound with all-empty sets returns an empty NetworkData")
    void emptyWhenNoConnections() throws Exception {
        final TestApplicationStateFacility appState = new TestApplicationStateFacility();
        appState.setKnownPublishers(NetworkData.DEFAULT);
        appState.setInboundPartners(NetworkData.DEFAULT);
        appState.setBackfillSources(NetworkData.DEFAULT);

        final TestServerResponse response = new TestServerResponse();
        new InboundStatusHandler(new TestServerRequest("/statusz/inbound"), response, appState).createAndSendResponse();

        assertEquals(200, response.sentStatus());
        assertTrue(parse(response).activeEndpoints().isEmpty());
    }
}
