// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.health;

import static org.hiero.block.node.health.HttpConnectionSupport.closeAfterHttp1;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.helidon.http.HeaderNames;
import io.helidon.webserver.http.ServerRequest;
import io.helidon.webserver.http.ServerResponse;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RosterEntry;
import org.hiero.block.api.TssData;
import org.hiero.block.api.TssRoster;
import org.hiero.block.node.spi.ApplicationStateFacility;

/// Builds and sends the `/statusz/inbound` response: the merge of the known
/// publishers, the inbound designated partners, and the backfill sources,
/// serialized as [NetworkData] JSON.
///
/// This is an independent, `final` class with no shared base or interface
/// (to avoid virtual-call overhead). Its single [#createAndSendResponse()]
/// entry point is invoked synchronously on the (virtual) request thread
/// by [HealthServicePlugin].
final class InboundStatusHandler {
    private final Logger LOGGER = System.getLogger(getClass().getName());
    private final ServerRequest request;
    private final ServerResponse response;
    private final ApplicationStateFacility appState;
    /// A string indicating "any" for a connection reference address or port.
    static final String WILDCARD_VALUE = "*";
    /// A connection reference that means "any host/any port".
    static final ConnectionReference WILDCARD_CONNECTION = ConnectionReference.newBuilder()
            .address(WILDCARD_VALUE)
            .port(WILDCARD_VALUE)
            .build();

    InboundStatusHandler(
            @NonNull final ServerRequest request,
            @NonNull final ServerResponse response,
            @NonNull final ApplicationStateFacility appState) {
        this.request = request;
        this.response = response;
        this.appState = appState;
    }

    /// Builds the inbound [NetworkData] and sends it as
    /// an `application/json` response.
    void createAndSendResponse() {
        final List<NetworkConnection> endpoints = new ArrayList<>();
        NetworkData publishers = appState.knownPublishers();
        TssData tssRoster = appState.getTssData();
        List<NetworkConnection> publisherConnections = publishers.activeEndpoints();
        publisherConnections.addAll(connectionsFromRoster(tssRoster.currentRoster()));
        endpoints.addAll(filterToUniqueConnections(publisherConnections));
        endpoints.addAll(appState.inboundPartners().activeEndpoints());
        endpoints.addAll(appState.backfillSources().activeEndpoints());
        final NetworkData data =
                NetworkData.newBuilder().activeEndpoints(endpoints).build();
        closeAfterHttp1(request, response.status(200).header(HeaderNames.CONTENT_TYPE, "application/json"))
                .send(NetworkData.JSON.toJSON(data));
    }

    /// Given a TSS Roster, return a list of network connections.
    /// This method depends on access to state information for the connection
    /// details of each Node in the Roster.
    /// @param roster a TSS Roster to process
    /// @return a List of network connections containing one entry for each
    ///     node in the roster, or empty if node information is not available.
    private List<NetworkConnection> connectionsFromRoster(final TssRoster roster) {
        if (roster != null && roster.rosterEntries() != null && !roster.rosterEntries().isEmpty()) {
            int entryCount = roster.rosterEntries().size();
            List<NetworkConnection> connections = new ArrayList<>(entryCount);
            for (RosterEntry entry : roster.rosterEntries()) {
                NetworkConnection newValue = connectionFromRosterEntry(entry);
                if (newValue != null) {
                    connections.add(newValue);
                }
            }
            return connections;
        } else {
            return List.of();
        }
    }

    /// This is a stub method because currently RosterEntry values contain
    /// a node ID, but no node data, so there is no source of connection data
    /// until we have a mechanism to query the network's Node Store from state
    /// in order to fill in node data.
    /// @param entry a [RosterEntry] to process into a network connection by
    ///     reference to the node information in network state.
    /// @return a [NetworkConnection] containing connection information needed
    ///     to connect to the node referred to by the Roster Entry, or null if
    ///     the required node information is not available.
    private NetworkConnection connectionFromRosterEntry(final RosterEntry entry) {
        return null;
    }

    /// Returns a new list containing the entries of `connections` with duplicates removed,
    /// preserving first-occurrence order. The supplied list is not modified. Two connections are
    /// considered duplicates when their `remote` reference has the same address and port; all other
    /// fields are ignored for this comparison.
    ///
    /// @param connections the connections to de-duplicate; must not be null
    /// @return a new list of the unique connections, in first-seen order
    static List<NetworkConnection> filterToUniqueConnections(final List<NetworkConnection> connections) {
        final List<NetworkConnection> unique = new ArrayList<>();
        for (final NetworkConnection connection : connections.stream()
                .sorted(InboundStatusHandler::compareNetworkConnection)
                .toList()) {
            // This is a sorted list, if there are duplicates, they will be adjacent.
            if (unique.isEmpty() || compareNetworkConnection(connection, unique.getLast()) != 0) {
                unique.add(connection);
            }
        }
        return unique;
    }

    /// A method to compare two network connections for ordering purposes.
    /// This method complies with the compare method for a [Comparable].
    /// @param left A network connection to compare for ordering purposes.
    /// @param right A network connection to compare for ordering purposes.
    /// @return -1, 0, or 1 if "left" is less than, equal to, or greater than "right".
    private static int compareNetworkConnection(NetworkConnection left, NetworkConnection right) {
        int result = 0;
        if (left != right) {
            if (left == null || right == null) {
                result = left == null ? -1 : 1;
            }
            if (result == 0) {
                result = compareConnectionReference(left.remote(), right.remote());
            }
            if (result == 0) {
                result = compareConnectionReference(left.local(), right.local());
            }
            if (result == 0) {
                result = left.tlsRequired() == false ? right.tlsRequired() == false ? 0 : -1 : 1;
            }
            if (result == 0) {
                result = compareComparable(left.protocol(), right.protocol());
            }
            if (result == 0) {
                result = compareComparable(left.category(), right.category());
            }
            if (result == 0) {
                result = compareComparable(left.scheme(), right.scheme());
            }
            if (result == 0) {
                result = compareComparable(left.certificate(), right.certificate());
            }
        }
        return result;
    }

    /// Compare two comparable values via `compareTo`.
    /// This method handles null inputs consistently.
    /// @param left a [Comparable] value to compare for ordering.
    /// @param right a [Comparable] value to compare for ordering.
    /// @return -1, 0, or 1 if "left" is less than, equal to, or greater than "right".
    /// @param <T> the underlying type that implements Comparable.
    private static <T extends Comparable<T>> int compareComparable(final T left, final T right) {
        int result = 0;
        if (left != right) {
            if (left == null || right == null) {
                result = left == null ? -1 : 1;
            }
            if (result == 0) {
                result = left.compareTo(right);
            }
        }
        return result;
    }

    /// Compare two [ConnectionReference] values for ordering.
    /// @param left a connection reference to compare for ordering.
    /// @param right a connection reference to compare for ordering.
    /// @return -1, 0, or 1 if "left" is less than, equal to, or greater than "right".
    private static int compareConnectionReference(final ConnectionReference left, final ConnectionReference right) {
        int result = 0;
        if (left != right) {
            final String leftAddress = left.address();
            final String rightAddress = right.address();
            final String leftPort = left.port();
            final String rightPort = right.port();
            if (leftAddress == null || rightAddress == null) {
                result = leftAddress == null ? rightAddress == null ? 0 : -1 : 1;
            }
            if (result == 0 && (leftPort == null || rightPort == null)) {
                result = leftPort == null ? rightPort == null ? 0 : -1 : 1;
            }
            if (result == 0) {
                final boolean leftWildAddress = WILDCARD_VALUE.equals(leftAddress);
                final boolean rightWildAddress = WILDCARD_VALUE.equals(leftAddress);
                final boolean leftWildPort = WILDCARD_VALUE.equals(leftAddress);
                final boolean rightWildPort = WILDCARD_VALUE.equals(leftAddress);
                // wildcards sort before non-wildcards
                if (leftWildAddress || rightWildAddress) {
                    result = leftWildAddress ? rightWildAddress ? 0 : -1 : 1;
                }
                if (result == 0 && (leftWildPort || rightWildPort)) {
                    result = leftWildPort ? rightWildPort ? 0 : -1 : 1;
                }
                if (result == 0) {
                    result = leftAddress.compareTo(rightAddress);
                }
                if (result == 0) {
                    result = leftPort.compareTo(rightPort);
                }
            }
        }
        return result;
    }
}
