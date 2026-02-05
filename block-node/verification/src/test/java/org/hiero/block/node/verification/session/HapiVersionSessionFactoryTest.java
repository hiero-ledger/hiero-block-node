// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.verification.session;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.hedera.hapi.node.base.SemanticVersion;
import java.util.stream.Stream;
import org.hiero.block.node.spi.blockmessaging.BlockSource;
import org.hiero.block.node.verification.session.impl.DummyVerificationSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("HapiVersionSessionFactory")
class HapiVersionSessionFactoryTest {

    private BlockSource blockSource;

    @BeforeEach
    void setUp() {
        blockSource = mock(BlockSource.class);
    }

    // ---------- Helpers ----------

    private static SemanticVersion sv(int maj, int min, int pat) {
        return SemanticVersion.newBuilder().major(maj).minor(min).patch(pat).build();
    }

    private static <T> void assertCreates(SemanticVersion ver, Class<T> expectedType, BlockSource src) {
        var session = HapiVersionSessionFactory.createSession(123L, src, ver, null, null);
        assertNotNull(session, "session should not be null");
        assertTrue(
                expectedType.isInstance(session),
                () -> "expected instance of " + expectedType.getSimpleName()
                        + " for version " + ver.major() + "." + ver.minor() + "." + ver.patch()
                        + " but got " + session.getClass().getSimpleName());
    }

    // ---------- Version selection tests ----------

    static Stream<Arguments> latestImplVersions() {
        return Stream.of(Arguments.of(sv(0, 69, 0)), Arguments.of(sv(0, 69, 1)));
    }

    // @todo(2002): Update to expect ExtendedMerkleTreeSession once proper v0.71.0 verification is implemented
    @ParameterizedTest(name = ">= 0.69.0 resolves to DummyVerificationSession for {0}")
    @MethodSource("latestImplVersions")
    void selectsLatestImplFor0690AndAbove(SemanticVersion v) {
        assertCreates(v, DummyVerificationSession.class, blockSource);
    }

    static Stream<Arguments> midRangeImplVersions() {
        return Stream.of(
                Arguments.of(sv(0, 64, 0)),
                Arguments.of(sv(0, 64, 1)),
                Arguments.of(sv(0, 65, 0)),
                Arguments.of(sv(0, 65, 1)),
                Arguments.of(sv(0, 66, 0)),
                Arguments.of(sv(0, 67, 999), Arguments.of(sv(0, 68, 999))));
    }

    // @todo(1661): Fix this test when upgrading to CN 0.70+
    @Test
    @DisplayName("Upcoming changes on 0.70.0 should resolve to DummyVerificationSession")
    void selectsDummyImplFor0700() {
        assertCreates(sv(0, 70, 0), DummyVerificationSession.class, blockSource);
    }

    @ParameterizedTest(name = ">= 0.64.0 and < 0.68.0 resolves to DummyVerificationSession for {0}")
    @MethodSource("midRangeImplVersions")
    void selects0640ImplForRange(SemanticVersion v) {
        assertCreates(v, DummyVerificationSession.class, blockSource);
    }

    // @todo(2002): Update to expect ExtendedMerkleTreeSession at v0.69.0 once proper verification is implemented
    @Test
    @DisplayName("Boundary: 0.67.x and 0.69.0 both resolve to DummyVerificationSession")
    void boundaryFlipAt0680() {
        assertCreates(sv(0, 68, 999), DummyVerificationSession.class, blockSource);
        assertCreates(sv(0, 69, 0), DummyVerificationSession.class, blockSource);
    }

    @Test
    @DisplayName("Below lowest supported (0.64.0) throws")
    void belowLowestThrows() {
        var ex = assertThrows(
                IllegalArgumentException.class,
                () -> HapiVersionSessionFactory.createSession(0L, blockSource, sv(0, 63, 1), null, null));
        assertTrue(
                ex.getMessage().toLowerCase().contains("unsupported hapi version"),
                "message should mention unsupported");
    }

    // ---------- Argument validation tests ----------

    @Test
    @DisplayName("Null blockSource throws NPE")
    void nullBlockSourceThrows() {
        assertThrows(
                NullPointerException.class,
                () -> HapiVersionSessionFactory.createSession(0L, null, sv(0, 68, 0), null, null));
    }

    @Test
    @DisplayName("Null hapiVersion throws NPE")
    void nullVersionThrows() {
        assertThrows(
                NullPointerException.class,
                () -> HapiVersionSessionFactory.createSession(0L, blockSource, null, null, null));
    }

    @Test
    @DisplayName("Negative blockNumber throws IAE")
    void negativeBlockNumberThrows() {
        assertThrows(
                IllegalArgumentException.class,
                () -> HapiVersionSessionFactory.createSession(-1L, blockSource, sv(0, 68, 0), null, null));
    }

    // ---------- Smoke tests with different block numbers ----------

    @Nested
    class BlockNumberAgnostic {
        // @todo(2002): Update to expect ExtendedMerkleTreeSession once proper v0.71.0 verification is implemented
        @Test
        @DisplayName("Uses the same impl regardless of blockNumber (0 and large)")
        void blockNumberDoesNotAffectImpl() {
            SemanticVersion v = sv(0, 69, 3);
            VerificationSession s1 = HapiVersionSessionFactory.createSession(0L, blockSource, v, null, null);
            VerificationSession s2 = HapiVersionSessionFactory.createSession(9_999_999L, blockSource, v, null, null);

            assertAll(
                    () -> assertTrue(s1 instanceof DummyVerificationSession),
                    () -> assertTrue(s2 instanceof DummyVerificationSession));
        }
    }
}
