// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.INFO;
import static java.lang.System.Logger.Level.WARNING;
import static org.hiero.block.node.base.ParseHelper.standardParse;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.ServiceEndpoint;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import java.io.IOException;
import java.lang.System.Logger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedList;
import java.util.List;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.node.app.config.state.ApplicationStateConfig;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.historicalblocks.BlockRangeSet;

final class ApplicationStateUtility {
    /// The logger for this class.
    private static final Logger LOGGER = System.getLogger(ApplicationStateUtility.class.getCanonicalName());
    /// Max protobuf/JSON message size for application-state files loaded from disk (small).
    private static final int MAX_APP_STATE_MESSAGE_SIZE_BYTES = 1 * 1024 * 1024;
    /// A string indicating "any" for a connection reference address or port.
    static final String WILDCARD_VALUE = "*";
    /// A connection reference that means "any host/any port".
    static final ConnectionReference WILDCARD_CONNECTION = ConnectionReference.newBuilder()
            .address(WILDCARD_VALUE)
            .port(WILDCARD_VALUE)
            .build();
    /// The "scheme" part of a grpc URI _without_ TLS.
    static final String GRPC_SCHEME = "grpc";
    /// The "scheme" part of a grpc URI _with_ TLS.
    static final String GRPC_TLS_SCHEME = "grpcs";

    private ApplicationStateUtility() {}

    /// Returns {@code true} when {@code incoming} represents a strictly newer address-book history
    /// than {@code current}, meaning it should replace the existing context value.
    ///
    /// "Newer" is defined as:
    /// <ul>
    ///   <li>The current history is null or empty (always accept the first history).</li>
    ///   <li>The incoming history's last era starts at a higher block number.</li>
    ///   <li>Equal last-era start blocks but more total eras (defensive; should not occur in
    ///       normal operation).</li>
    /// </ul>
    static boolean isNewerHistory(RangedAddressBookHistory incoming, RangedAddressBookHistory current) {
        if (current == null
                || current.addressBooks() == null
                || current.addressBooks().isEmpty()) return true;
        if (incoming == null
                || incoming.addressBooks() == null
                || incoming.addressBooks().isEmpty()) return false;
        final long currentLast = current.addressBooks().getLast().startBlock();
        final long incomingLast = incoming.addressBooks().getLast().startBlock();
        return incomingLast > currentLast
                || (incomingLast == currentLast
                        && incoming.addressBooks().size()
                                > current.addressBooks().size());
    }

    /// Merge two sets of block ranges into a single list of block ranges.
    /// This is a very expensive method, O(n<sup>2</sup>), so it should be used
    /// carefully. Perhaps a future update to BlockRangeSet will add a more
    /// efficient merge process. Called unconditionally on every scan (not only when
    /// `availableBlocks` changes) — `storedBlocks` can change independently (e.g. a plugin calling
    /// `addStoredBlockRange`), and skipping the merge in that case previously let `storedBlocks()`
    /// regress to the unmerged, durable-only set once `availableBlocks` stabilized.
    ///
    /// @param storedBlocks a set of stored blocks to merge into the result.
    /// @param availableBlocks a set of available blocks to merge into the result.
    static List<BlockRange> mergeRanges(final BlockRangeSet storedBlocks, final BlockRangeSet availableBlocks) {
        ConcurrentLongRangeSet combined = new ConcurrentLongRangeSet();
        combined.addAll(storedBlocks);
        combined.addAll(availableBlocks);
        return combined.streamRanges()
                .map(longRange -> new BlockRange(longRange.start(), longRange.end()))
                .toList();
    }

    /// Convert BlockRangeSet to BlockRange
    static List<BlockRange> toBlockRange(BlockRangeSet blockRangeSet) {
        return blockRangeSet
                .streamRanges()
                .map(longRange -> new BlockRange(longRange.start(), longRange.end()))
                .toList();
    }

    /// Loads a [NetworkData] document from the given JSON file.
    /// Missing files and parse failures are logged and yield
    /// [NetworkData#DEFAULT] (an empty set) rather than throwing, mirroring
    /// the lenient handling used for other optional application-state files.
    ///
    /// @param path the JSON file to read
    /// @return the parsed NetworkData, or [NetworkData#DEFAULT] if
    ///     absent or unreadable
    static NetworkData loadNetworkData(final Path path) {
        try {
            if (path == null || !Files.isRegularFile(path)) {
                LOGGER.log(DEBUG, "Network data file not present or unreadable, using empty set: {0}", path);
                return NetworkData.DEFAULT;
            }
            final NetworkData data = standardParse(
                    NetworkData.JSON, Bytes.wrap(Files.readAllBytes(path)), MAX_APP_STATE_MESSAGE_SIZE_BYTES);
            return data;
        } catch (ParseException | IOException e) {
            LOGGER.log(WARNING, "Failed to read network data file %s.".formatted(path), e);
            return NetworkData.DEFAULT;
        }
    }

    /// Validates that the NodeAddressBook has at least one entry with a
    /// non-blank `rsa_pubkey` that can be encoded and procesed.
    ///
    /// @param book the address book to validate
    /// @param source human-readable source name for error messages
    /// @return true iff the address book is validated, false otherwise.
    /// @throws IllegalStateException if the JVM does not have the RSA algorithm available.
    static boolean validateAddressBook(final NodeAddressBook book, final String source) {
        try {
            if (book == null || book.nodeAddress() == null || book.nodeAddress().isEmpty()) {
                LOGGER.log(
                        DEBUG,
                        "RSA address book from %s contains no entries — cannot verify WRB proofs".formatted(source));
                return false;
            }
            final long declared = book.nodeAddress().stream()
                    .filter(a -> !(a == null
                            || a.rsaPubKey() == null
                            || a.rsaPubKey().isBlank()))
                    .count();
            final String noValidKeyMessage = "RSA address book from %s has %s entries but none have a valid RSA_PubKey"
                    .formatted(source, declared);
            if (declared == 0) {
                LOGGER.log(DEBUG, noValidKeyMessage);
                return false;
            }
            long usable = 0;
            final HexFormat hex = HexFormat.of();
            // Obtain KeyFactory once — provider lookup is not cheap and RSA must always be available.
            final KeyFactory kf;
            kf = KeyFactory.getInstance("RSA");
            for (final NodeAddress addr : book.nodeAddress()) {
                if (addr == null || addr.rsaPubKey() == null || addr.rsaPubKey().isBlank()) {
                    continue;
                }
                usable += checkKeyIsUseable(addr, hex, kf);
            }
            if (usable == 0) {
                LOGGER.log(INFO, noValidKeyMessage);
            }
            return usable > 0;
        } catch (NoSuchAlgorithmException e) {
            // RSA must be available in every JVM — this is a JVM misconfiguration
            throw new IllegalStateException("RSA KeyFactory not available", e);
        }
    }

    private static long checkKeyIsUseable(final NodeAddress address, final HexFormat hexForm, final KeyFactory kf) {
        try {
            final byte[] keyBytes = hexForm.parseHex(address.rsaPubKey());
            kf.generatePublic(new X509EncodedKeySpec(keyBytes));
            return 1;
        } catch (InvalidKeySpecException | IllegalArgumentException e) {
            // This is just a non-useable key, so just log DEBUG and don't count the key.
            LOGGER.log(DEBUG, "Malformed RSA_PubKey for node {0} — skipped: {1}", address.nodeId(), e);
        }
        return 0;
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
                .sorted(ApplicationStateUtility::compareNetworkConnection)
                .toList()) {
            // This is a sorted list, if there are duplicates, they will be adjacent.
            if (unique.isEmpty() || compareNetworkConnection(connection, unique.getLast()) != 0) {
                unique.add(connection);
            }
        }
        return unique;
    }

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

    /// Derives the list of publisher [NetworkConnection]s described by an address book.
    ///
    /// Each [ServiceEndpoint] on each node becomes one connection whose `remote` reference carries
    /// the endpoint's address and port, and whose `local` reference is the wildcard `*:*`. The
    /// address is the dotted-decimal form of `ipAddressV4` (see [#toIpV4String]) or, if the node
    /// publishes a `domainName` instead, that domain name. An endpoint that sets both
    /// `ipAddressV4` and `domainName`, sets neither, or carries a malformed `ipAddressV4` is
    /// invalid and skipped.
    ///
    /// @param book the address book to derive connections from; must not be null
    /// @param stateConfig the configuration for the application state. We use
    ///     the publisher inbound category from this configuration.
    ///
    /// @return a new, mutable list of derived connections (possibly empty)
    static List<NetworkConnection> publisherConnectionsFrom(
            final NodeAddressBook book, final ApplicationStateConfig stateConfig) {
        final List<NetworkConnection> connections = new LinkedList<>();
        for (final NodeAddress node : book.nodeAddress()) {
            for (final ServiceEndpoint endpoint : node.serviceEndpoint()) {
                final boolean hasIp =
                        endpoint.ipAddressV4() != null && endpoint.ipAddressV4().length() > 0;
                final boolean hasDomain =
                        endpoint.domainName() != null && !endpoint.domainName().isBlank();
                // Exactly one of ipAddressV4/domainName must be set; both-set or neither-set is invalid.
                if (hasIp == hasDomain) {
                    continue;
                }
                final String address = hasIp ? toIpV4String(endpoint.ipAddressV4()) : endpoint.domainName();
                if (address == null) {
                    // ipAddressV4 was set but not a well-formed 4-byte address.
                    continue;
                }
                final ConnectionReference remote = ConnectionReference.newBuilder()
                        .address(address)
                        .port(Integer.toString(endpoint.port()))
                        .build();
                connections.add(NetworkConnection.newBuilder()
                        .remote(remote)
                        .local(WILDCARD_CONNECTION)
                        .category(stateConfig.publisherInboundCategory())
                        .scheme(GRPC_SCHEME)
                        .protocol(IpProtocol.TCP)
                        .tlsRequired(false)
                        .build());
            }
        }
        return connections;
    }

    /// Translates a 4-byte, big-endian IPv4 address into dotted-decimal string form.
    /// For example the bytes `7F 00 00 01` become `127.0.0.1`.
    ///
    /// @param ipAddressV4 the raw 4-byte, big-endian address
    /// @return the dotted-decimal string, or null if the input is not exactly four bytes
    static String toIpV4String(final Bytes ipAddressV4) {
        if (ipAddressV4.length() != 4) {
            return null;
        }
        final byte[] octets = ipAddressV4.toByteArray();
        return (octets[0] & 0xFF) + "." + (octets[1] & 0xFF) + "." + (octets[2] & 0xFF) + "." + (octets[3] & 0xFF);
    }

    /// UncaughtExceptionHandler for logging uncaught exceptions
    static void uncaughtExceptionHandler(Thread thread, Throwable throwable) {
        LOGGER.log(WARNING, "Uncaught exception in ApplicationStateFacility thread: " + thread.getName(), throwable);
    }
}
