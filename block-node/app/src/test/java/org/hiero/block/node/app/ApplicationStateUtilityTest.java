// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.app;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.hedera.hapi.node.base.NodeAddress;
import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.hapi.node.base.ServiceEndpoint;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.swirlds.config.api.Configuration;
import com.swirlds.config.api.ConfigurationBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.hiero.block.api.BlockRange;
import org.hiero.block.api.NetworkConnection;
import org.hiero.block.api.NetworkConnection.ConnectionReference;
import org.hiero.block.api.NetworkConnection.IpProtocol;
import org.hiero.block.api.NetworkData;
import org.hiero.block.api.RangedAddressBookHistory;
import org.hiero.block.api.RangedNodeAddressBook;
import org.hiero.block.node.app.config.state.ApplicationStateConfig;
import org.hiero.block.node.base.ranges.ConcurrentLongRangeSet;
import org.hiero.block.node.spi.historicalblocks.LongRange;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/// Tests for [ApplicationStateUtility] static methods.
class ApplicationStateUtilityTest {
    private static ServiceEndpoint ipEndpoint(final String hexIp, final int port) {
        return ServiceEndpoint.newBuilder()
                .ipAddressV4(Bytes.fromHex(hexIp))
                .port(port)
                .build();
    }

    private static ServiceEndpoint domainEndpoint(final String domain, final int port) {
        return ServiceEndpoint.newBuilder().domainName(domain).port(port).build();
    }

    private static NodeAddressBook bookWithEndpoints(final ServiceEndpoint... endpoints) {
        return NodeAddressBook.newBuilder()
                .nodeAddress(NodeAddress.newBuilder()
                        .nodeId(1L)
                        .serviceEndpoint(endpoints)
                        .build())
                .build();
    }

    @Nested
    @DisplayName("loadNetworkData")
    class LoadNetworkData {
        @Test
        @DisplayName("A valid NetworkData JSON file is parsed")
        void loadsNetworkDataFromFile(@TempDir final Path dir) throws IOException {
            final NetworkData data = NetworkData.newBuilder()
                    .activeEndpoints(NetworkConnection.newBuilder()
                            .local(new ConnectionReference("$", "*"))
                            .remote(new ConnectionReference("pub.example.com", "40840"))
                            .category("publisher")
                            .scheme("https")
                            .protocol(IpProtocol.TCP)
                            .tlsRequired(true)
                            .certificate(Bytes.EMPTY)
                            .build())
                    .build();
            final Path file = dir.resolve("known-publishers.json");
            Files.writeString(file, NetworkData.JSON.toJSON(data));

            final NetworkData loaded = ApplicationStateUtility.loadNetworkData(file);

            assertEquals(1, loaded.activeEndpoints().size());
            assertEquals(
                    "pub.example.com", loaded.activeEndpoints().get(0).remote().address());
        }

        @Test
        @DisplayName("A missing file yields an empty NetworkData")
        void missingFileReturnsEmpty(@TempDir final Path dir) {
            assertTrue(ApplicationStateUtility.loadNetworkData(dir.resolve("does-not-exist.json"))
                    .activeEndpoints()
                    .isEmpty());
        }

        @Test
        @DisplayName("A null path yields an empty NetworkData")
        void nullPathReturnsEmpty() {
            assertTrue(ApplicationStateUtility.loadNetworkData(null)
                    .activeEndpoints()
                    .isEmpty());
        }

        @Test
        @DisplayName("A corrupt (unparseable) file yields an empty NetworkData")
        void corruptFileReturnsEmpty(@TempDir final Path dir) throws IOException {
            final Path file = dir.resolve("corrupt.json");
            Files.writeString(file, "{ this is not valid json !!!}");
            assertTrue(ApplicationStateUtility.loadNetworkData(file)
                    .activeEndpoints()
                    .isEmpty());
        }
    }

    @Nested
    @DisplayName("validateAddressBook")
    class ValidateAddressBook {
        @Test
        @DisplayName("rejects empty book and all-blank RSA keys")
        void rejectsInvalidBooks() {
            assertThrows(
                    IllegalStateException.class,
                    () -> ApplicationStateUtility.validateAddressBook(
                            NodeAddressBook.newBuilder().build(), "test-empty"),
                    "Empty address book must throw");

            final NodeAddressBook allBlank = NodeAddressBook.newBuilder()
                    .nodeAddress(
                            NodeAddress.newBuilder().nodeId(0).rsaPubKey("").build())
                    .build();
            assertThrows(
                    IllegalStateException.class,
                    () -> ApplicationStateUtility.validateAddressBook(allBlank, "test-all-blank"),
                    "Book with only blank RSA keys must throw");
        }

        @Test
        @DisplayName("accepts book with at least one non-blank RSA key")
        void acceptsValidBook() throws NoSuchAlgorithmException {
            final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            final KeyPair kp = kpg.generateKeyPair();
            final String hexKey = HexFormat.of().formatHex(kp.getPublic().getEncoded());
            final NodeAddressBook valid = NodeAddressBook.newBuilder()
                    .nodeAddress(
                            NodeAddress.newBuilder().nodeId(0).rsaPubKey(hexKey).build())
                    .build();
            assertDoesNotThrow(() -> ApplicationStateUtility.validateAddressBook(valid, "test-valid"));
        }

        @Test
        @DisplayName("rejects book where every non-blank RSA key is syntactically malformed")
        void rejectsAllMalformedRsaKeys() {
            final NodeAddressBook malformed = NodeAddressBook.newBuilder()
                    .nodeAddress(NodeAddress.newBuilder()
                            .nodeId(0)
                            .rsaPubKey("not-valid-hex!")
                            .build())
                    .build();
            assertThrows(
                    IllegalStateException.class,
                    () -> ApplicationStateUtility.validateAddressBook(malformed, "test-malformed"),
                    "Book with only malformed RSA keys must throw");
        }

        @Test
        @DisplayName("accepts book where at least one RSA key is valid even if others are malformed")
        void acceptsBookWithMixedValidAndMalformedKeys() throws NoSuchAlgorithmException {
            final KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            final String goodKey =
                    HexFormat.of().formatHex(kpg.generateKeyPair().getPublic().getEncoded());
            final NodeAddressBook mixed = NodeAddressBook.newBuilder()
                    .nodeAddress(
                            NodeAddress.newBuilder()
                                    .nodeId(0)
                                    .rsaPubKey("not-valid-hex!")
                                    .build(),
                            NodeAddress.newBuilder()
                                    .nodeId(1)
                                    .rsaPubKey(goodKey)
                                    .build())
                    .build();
            assertDoesNotThrow(() -> ApplicationStateUtility.validateAddressBook(mixed, "test-mixed"));
        }
    }

    @Nested
    @DisplayName("toIpV4String")
    class ToIpV4String {

        @ParameterizedTest(name = "{0} -> {1}")
        @DisplayName("decodes 4-byte big-endian addresses to dotted-decimal with unsigned octets")
        @MethodSource("ipV4DecodeCases")
        void decodesVariousValues(final String hex, final String expected) {
            assertEquals(expected, ApplicationStateUtility.toIpV4String(Bytes.fromHex(hex)));
        }

        /// Hex-encoded 4-byte address and its expected dotted-decimal form, spread widely across
        /// the 32-bit space: the all-zero and all-ones boundaries plus values exercising high-bit-set
        /// octets in every position.
        static Stream<Arguments> ipV4DecodeCases() {
            return Stream.of(
                    arguments("00000000", "0.0.0.0"),
                    arguments("7F000001", "127.0.0.1"),
                    arguments("03060912", "3.6.9.18"),
                    arguments("11223344", "17.34.51.68"),
                    arguments("18273645", "24.39.54.69"),
                    arguments("1F2E3D4C", "31.46.61.76"),
                    arguments("25354555", "37.53.69.85"),
                    arguments("30609000", "48.96.144.0"),
                    arguments("3B5A7910", "59.90.121.16"),
                    arguments("47586970", "71.88.105.112"),
                    arguments("5C6D7E8F", "92.109.126.143"),
                    arguments("66778899", "102.119.136.153"),
                    arguments("73727170", "115.114.113.112"),
                    arguments("81828384", "129.130.131.132"),
                    arguments("8A0B1C2D", "138.11.28.45"),
                    arguments("95A5B5C5", "149.165.181.197"),
                    arguments("A9B8C7D6", "169.184.199.214"),
                    arguments("B4C3D2E1", "180.195.210.225"),
                    arguments("C1D2E3F4", "193.210.227.244"),
                    arguments("D5C4B3A2", "213.196.179.162"),
                    arguments("E7F0091A", "231.240.9.26"),
                    arguments("FEDCBA98", "254.220.186.152"),
                    arguments("01020304", "1.2.3.4"),
                    arguments("08080808", "8.8.8.8"),
                    arguments("0A141E28", "10.20.30.40"),
                    arguments("20304050", "32.48.64.80"),
                    arguments("2A2A2A2A", "42.42.42.42"),
                    arguments("33445566", "51.68.85.102"),
                    arguments("40506070", "64.80.96.112"),
                    arguments("4D3C2B1A", "77.60.43.26"),
                    arguments("55555555", "85.85.85.85"),
                    arguments("64645A50", "100.100.90.80"),
                    arguments("7F000101", "127.0.1.1"),
                    arguments("80808080", "128.128.128.128"),
                    arguments("8899AABB", "136.153.170.187"),
                    arguments("9A8B7C6D", "154.139.124.109"),
                    arguments("A1B2C3D4", "161.178.195.212"),
                    arguments("ABCDEF10", "171.205.239.16"),
                    arguments("C0A80101", "192.168.1.1"),
                    arguments("CAFEBABE", "202.254.186.190"),
                    arguments("DEADBEEF", "222.173.190.239"),
                    arguments("F1E2D3C4", "241.226.211.196"),
                    arguments("FFFFFFFF", "255.255.255.255"));
        }

        @Test
        @DisplayName("returns null for input that is not exactly four bytes")
        void nullForWrongLength() {
            assertNull(ApplicationStateUtility.toIpV4String(Bytes.fromHex("7F0000")), "3 bytes must be rejected");
            assertNull(ApplicationStateUtility.toIpV4String(Bytes.fromHex("7F00000101")), "5 bytes must be rejected");
            assertNull(ApplicationStateUtility.toIpV4String(Bytes.EMPTY), "empty must be rejected");
        }
    }

    @Nested
    @DisplayName("publisherConnectionsFrom")
    class PublisherConnectionsFrom {
        private final ApplicationStateConfig appStateConfig;

        PublisherConnectionsFrom() {
            final ConfigurationBuilder configBuilder = ConfigurationBuilder.create();
            final Class<ApplicationStateConfig> asfType = ApplicationStateConfig.class;
            final Configuration config =
                    configBuilder.withConfigDataType(asfType).build();
            appStateConfig = config.getConfigData(asfType);
        }

        @Test
        @DisplayName("an IPv4 endpoint becomes a fully-populated publisher connection")
        void ipv4EndpointBecomesConnection() {
            final List<NetworkConnection> connections = ApplicationStateUtility.publisherConnectionsFrom(
                    bookWithEndpoints(ipEndpoint("7F000001", 40840)), appStateConfig);

            assertEquals(1, connections.size());
            final NetworkConnection connection = connections.getFirst();
            assertEquals("127.0.0.1", connection.remote().address());
            assertEquals("40840", connection.remote().port());
            assertEquals("*", connection.local().address());
            assertEquals("*", connection.local().port());
            assertEquals("publisher", connection.category());
            assertEquals("grpc", connection.scheme());
            assertEquals(IpProtocol.TCP, connection.protocol());
            assertFalse(connection.tlsRequired());
            assertEquals(Bytes.EMPTY, connection.certificate());
        }

        @Test
        @DisplayName("a domain-name endpoint uses the domain as the remote address")
        void domainEndpointUsesDomain() {
            final List<NetworkConnection> connections = ApplicationStateUtility.publisherConnectionsFrom(
                    bookWithEndpoints(domainEndpoint("node1.example.com", 50211)), appStateConfig);

            assertEquals(1, connections.size());
            assertEquals("node1.example.com", connections.getFirst().remote().address());
            assertEquals("50211", connections.getFirst().remote().port());
        }

        @Test
        @DisplayName("an endpoint that sets both ipAddressV4 and domainName is skipped")
        void bothSetIsSkipped() {
            final ServiceEndpoint both = ServiceEndpoint.newBuilder()
                    .ipAddressV4(Bytes.fromHex("7F000001"))
                    .domainName("node1.example.com")
                    .port(40840)
                    .build();
            assertTrue(ApplicationStateUtility.publisherConnectionsFrom(bookWithEndpoints(both), appStateConfig)
                    .isEmpty());
        }

        @Test
        @DisplayName("an endpoint that sets neither ipAddressV4 nor domainName is skipped")
        void neitherSetIsSkipped() {
            final ServiceEndpoint neither =
                    ServiceEndpoint.newBuilder().port(40840).build();
            assertTrue(ApplicationStateUtility.publisherConnectionsFrom(bookWithEndpoints(neither), appStateConfig)
                    .isEmpty());
        }

        @Test
        @DisplayName("an endpoint with a malformed (non-4-byte) ipAddressV4 is skipped")
        void malformedIpIsSkipped() {
            assertTrue(ApplicationStateUtility.publisherConnectionsFrom(
                            bookWithEndpoints(ipEndpoint("7F0000", 40840)), appStateConfig)
                    .isEmpty());
        }

        @Test
        @DisplayName("connections are flattened across every node and every endpoint")
        void flattensAcrossNodesAndEndpoints() {
            final ServiceEndpoint valid1 = ipEndpoint("7F000001", 1);
            final ServiceEndpoint invalidBoth = ServiceEndpoint.newBuilder()
                    .ipAddressV4(Bytes.fromHex("7F000002"))
                    .domainName("x.example.com")
                    .port(2)
                    .build();
            final ServiceEndpoint valid2 = domainEndpoint("node2.example.com", 3);
            final NodeAddressBook book = NodeAddressBook.newBuilder()
                    .nodeAddress(
                            NodeAddress.newBuilder()
                                    .nodeId(1L)
                                    .serviceEndpoint(valid1, invalidBoth)
                                    .build(),
                            NodeAddress.newBuilder()
                                    .nodeId(2L)
                                    .serviceEndpoint(valid2)
                                    .build())
                    .build();

            final List<NetworkConnection> connections =
                    ApplicationStateUtility.publisherConnectionsFrom(book, appStateConfig);
            assertEquals(2, connections.size());
            assertEquals("127.0.0.1", connections.get(0).remote().address());
            assertEquals("node2.example.com", connections.get(1).remote().address());
        }

        @Test
        @DisplayName("an address book with no nodes yields no connections")
        void emptyBookYieldsNoConnections() {
            assertTrue(ApplicationStateUtility.publisherConnectionsFrom(
                            NodeAddressBook.newBuilder().build(), appStateConfig)
                    .isEmpty());
        }
    }

    // ---- isNewerHistory -------------------------------------------------------------------------

    @Nested
    @DisplayName("isNewerHistory")
    class IsNewerHistory {

        private RangedAddressBookHistory historyWithLastBlock(final long startBlock) {
            return RangedAddressBookHistory.newBuilder()
                    .addressBooks(List.of(RangedNodeAddressBook.newBuilder()
                            .startBlock(startBlock)
                            .endBlock(-1L)
                            .build()))
                    .build();
        }

        private RangedAddressBookHistory historyWithTwoEras(final long era1Start, final long era2Start) {
            return RangedAddressBookHistory.newBuilder()
                    .addressBooks(List.of(
                            RangedNodeAddressBook.newBuilder()
                                    .startBlock(era1Start)
                                    .endBlock(era2Start - 1)
                                    .build(),
                            RangedNodeAddressBook.newBuilder()
                                    .startBlock(era2Start)
                                    .endBlock(-1L)
                                    .build()))
                    .build();
        }

        @Test
        @DisplayName("null current always accepts incoming")
        void nullCurrentAcceptsIncoming() {
            assertTrue(ApplicationStateUtility.isNewerHistory(historyWithLastBlock(100L), null));
        }

        @Test
        @DisplayName("empty current always accepts incoming")
        void emptyCurrentAcceptsIncoming() {
            final RangedAddressBookHistory empty =
                    RangedAddressBookHistory.newBuilder().build();
            assertTrue(ApplicationStateUtility.isNewerHistory(historyWithLastBlock(100L), empty));
        }

        @Test
        @DisplayName("empty incoming is never newer than a non-empty current")
        void emptyIncomingIsNeverNewer() {
            final RangedAddressBookHistory empty =
                    RangedAddressBookHistory.newBuilder().build();
            assertFalse(ApplicationStateUtility.isNewerHistory(empty, historyWithLastBlock(100L)));
        }

        @Test
        @DisplayName("incoming with higher last startBlock is newer")
        void higherLastStartBlockIsNewer() {
            assertTrue(ApplicationStateUtility.isNewerHistory(historyWithLastBlock(200L), historyWithLastBlock(100L)));
        }

        @Test
        @DisplayName("incoming with lower last startBlock is not newer")
        void lowerLastStartBlockIsNotNewer() {
            assertFalse(ApplicationStateUtility.isNewerHistory(historyWithLastBlock(100L), historyWithLastBlock(200L)));
        }

        @Test
        @DisplayName("equal last startBlock and equal era count is not newer")
        void sameLastStartBlockSameCountIsNotNewer() {
            assertFalse(ApplicationStateUtility.isNewerHistory(historyWithLastBlock(100L), historyWithLastBlock(100L)));
        }

        @Test
        @DisplayName("equal last startBlock but more total eras is newer")
        void sameLastStartBlockMoreErasIsNewer() {
            assertTrue(
                    ApplicationStateUtility.isNewerHistory(historyWithTwoEras(0L, 100L), historyWithLastBlock(100L)));
        }

        @Test
        @DisplayName("equal last startBlock but fewer total eras is not newer")
        void sameLastStartBlockFewerErasIsNotNewer() {
            assertFalse(
                    ApplicationStateUtility.isNewerHistory(historyWithLastBlock(100L), historyWithTwoEras(0L, 100L)));
        }
    }

    // ---- mergeRanges ----------------------------------------------------------------------------

    @Nested
    @DisplayName("mergeRanges")
    class MergeRanges {

        @Test
        @DisplayName("both empty yields empty list")
        void bothEmptyYieldsEmpty() {
            assertTrue(ApplicationStateUtility.mergeRanges(new ConcurrentLongRangeSet(), new ConcurrentLongRangeSet())
                    .isEmpty());
        }

        @Test
        @DisplayName("only storedBlocks has a range")
        void onlyStoredBlocksHasRange() {
            final List<BlockRange> result = ApplicationStateUtility.mergeRanges(
                    new ConcurrentLongRangeSet(0, 99), new ConcurrentLongRangeSet());
            assertEquals(1, result.size());
            assertEquals(0L, result.getFirst().rangeStart());
            assertEquals(99L, result.getFirst().rangeEnd());
        }

        @Test
        @DisplayName("only availableBlocks has a range")
        void onlyAvailableBlocksHasRange() {
            final List<BlockRange> result = ApplicationStateUtility.mergeRanges(
                    new ConcurrentLongRangeSet(), new ConcurrentLongRangeSet(100, 199));
            assertEquals(1, result.size());
            assertEquals(100L, result.getFirst().rangeStart());
            assertEquals(199L, result.getFirst().rangeEnd());
        }

        @Test
        @DisplayName("disjoint ranges from both sets appear as separate entries")
        void disjointRangesAppearSeparately() {
            final ConcurrentLongRangeSet stored = new ConcurrentLongRangeSet(0, 49);
            final ConcurrentLongRangeSet available = new ConcurrentLongRangeSet(100, 149);
            final List<BlockRange> result = ApplicationStateUtility.mergeRanges(stored, available);
            assertEquals(2, result.size());
            assertEquals(0L, result.get(0).rangeStart());
            assertEquals(49L, result.get(0).rangeEnd());
            assertEquals(100L, result.get(1).rangeStart());
            assertEquals(149L, result.get(1).rangeEnd());
        }

        @Test
        @DisplayName("overlapping ranges from both sets merge into one")
        void overlappingRangesMerge() {
            final List<BlockRange> result = ApplicationStateUtility.mergeRanges(
                    new ConcurrentLongRangeSet(0, 100), new ConcurrentLongRangeSet(50, 200));
            assertEquals(1, result.size());
            assertEquals(0L, result.getFirst().rangeStart());
            assertEquals(200L, result.getFirst().rangeEnd());
        }

        @Test
        @DisplayName("identical ranges in both sets appear exactly once")
        void identicalRangesAppearOnce() {
            final List<BlockRange> result = ApplicationStateUtility.mergeRanges(
                    new ConcurrentLongRangeSet(0, 99), new ConcurrentLongRangeSet(0, 99));
            assertEquals(1, result.size());
            assertEquals(0L, result.getFirst().rangeStart());
            assertEquals(99L, result.getFirst().rangeEnd());
        }
    }

    // ---- toBlockRange ---------------------------------------------------------------------------

    @Nested
    @DisplayName("toBlockRange")
    class ToBlockRange {

        @Test
        @DisplayName("empty set yields empty list")
        void emptySetYieldsEmpty() {
            assertTrue(ApplicationStateUtility.toBlockRange(new ConcurrentLongRangeSet())
                    .isEmpty());
        }

        @Test
        @DisplayName("single range converts to one BlockRange with correct bounds")
        void singleRangeConverts() {
            final List<BlockRange> result = ApplicationStateUtility.toBlockRange(new ConcurrentLongRangeSet(10, 20));
            assertEquals(1, result.size());
            assertEquals(10L, result.getFirst().rangeStart());
            assertEquals(20L, result.getFirst().rangeEnd());
        }

        @Test
        @DisplayName("multiple disjoint ranges each become a BlockRange in ascending order")
        void multipleRangesConvertInOrder() {
            final ConcurrentLongRangeSet rangeSet = new ConcurrentLongRangeSet();
            rangeSet.add(new LongRange(0, 9));
            rangeSet.add(new LongRange(20, 29));
            rangeSet.add(new LongRange(50, 59));
            final List<BlockRange> result = ApplicationStateUtility.toBlockRange(rangeSet);
            assertEquals(3, result.size());
            assertEquals(0L, result.get(0).rangeStart());
            assertEquals(9L, result.get(0).rangeEnd());
            assertEquals(20L, result.get(1).rangeStart());
            assertEquals(29L, result.get(1).rangeEnd());
            assertEquals(50L, result.get(2).rangeStart());
            assertEquals(59L, result.get(2).rangeEnd());
        }
    }

    // ---- filterToUniqueConnections --------------------------------------------------------------

    @Nested
    @DisplayName("filterToUniqueConnections")
    class FilterToUniqueConnections {

        private NetworkConnection publisherConnection(final String address, final int port) {
            return NetworkConnection.newBuilder()
                    .remote(new ConnectionReference(address, Integer.toString(port)))
                    .local(new ConnectionReference("*", "*"))
                    .category("publisher")
                    .scheme("grpc")
                    .protocol(IpProtocol.TCP)
                    .tlsRequired(false)
                    .build();
        }

        @Test
        @DisplayName("empty input yields empty output")
        void emptyInputYieldsEmpty() {
            assertTrue(
                    ApplicationStateUtility.filterToUniqueConnections(List.of()).isEmpty());
        }

        @Test
        @DisplayName("single connection is retained")
        void singleConnectionRetained() {
            final NetworkConnection conn = publisherConnection("1.2.3.4", 40840);
            final List<NetworkConnection> result = ApplicationStateUtility.filterToUniqueConnections(List.of(conn));
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("two identical connections collapse to one")
        void identicalConnectionsCollapse() {
            final NetworkConnection conn = publisherConnection("1.2.3.4", 40840);
            final List<NetworkConnection> result =
                    ApplicationStateUtility.filterToUniqueConnections(new ArrayList<>(List.of(conn, conn)));
            assertEquals(1, result.size());
        }

        @Test
        @DisplayName("two distinct connections are both retained")
        void distinctConnectionsBothRetained() {
            final NetworkConnection conn1 = publisherConnection("1.2.3.4", 40840);
            final NetworkConnection conn2 = publisherConnection("5.6.7.8", 40840);
            final List<NetworkConnection> result =
                    ApplicationStateUtility.filterToUniqueConnections(new ArrayList<>(List.of(conn1, conn2)));
            assertEquals(2, result.size());
        }

        @Test
        @DisplayName("a duplicate buried in a larger list is collapsed to one entry")
        void duplicateBuriedInLargerListIsCollapsed() {
            final NetworkConnection conn1 = publisherConnection("1.2.3.4", 1);
            final NetworkConnection conn2 = publisherConnection("5.6.7.8", 2);
            final NetworkConnection conn3 = publisherConnection("9.10.11.12", 3);
            final List<NetworkConnection> result = ApplicationStateUtility.filterToUniqueConnections(
                    new ArrayList<>(List.of(conn1, conn2, conn3, conn1)));
            assertEquals(3, result.size());
        }
    }
}
