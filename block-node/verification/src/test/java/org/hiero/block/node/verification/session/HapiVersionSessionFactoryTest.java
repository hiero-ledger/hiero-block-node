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
import org.hiero.block.node.verification.session.impl.ExtendedMerkleTreeSession;
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
        var session = HapiVersionSessionFactory.createSession(123L, src, ver);
        assertNotNull(session, "session should not be null");
        assertTrue(
                expectedType.isInstance(session),
                () -> "expected instance of " + expectedType.getSimpleName()
                        + " for version " + ver.major() + "." + ver.minor() + "." + ver.patch()
                        + " but got " + session.getClass().getSimpleName());
    }

    // ---------- Version selection tests ----------

    static Stream<Arguments> latestImplVersions() {
        return Stream.of(Arguments.of(sv(0, 68, 0)), Arguments.of(sv(0, 68, 1)));
    }

    @ParameterizedTest(name = ">= 0.68.0 resolves to ExtendedMerkleTreeSession for {0}")
    @MethodSource("latestImplVersions")
    void selectsLatestImplFor0680AndAbove(SemanticVersion v) {
        assertCreates(v, ExtendedMerkleTreeSession.class, blockSource);
    }

    static Stream<Arguments> midRangeImplVersions() {
        return Stream.of(
                Arguments.of(sv(0, 64, 0)),
                Arguments.of(sv(0, 64, 1)),
                Arguments.of(sv(0, 65, 0)),
                Arguments.of(sv(0, 65, 1)),
                Arguments.of(sv(0, 66, 0)),
                Arguments.of(sv(0, 67, 999)));
    }

    @ParameterizedTest(name = ">= 0.64.0 and < 0.68.0 resolves to DummyVerificationSession for {0}")
    @MethodSource("midRangeImplVersions")
    void selects0640ImplForRange(SemanticVersion v) {
        assertCreates(v, DummyVerificationSession.class, blockSource);
    }

    @Test
    @DisplayName("Boundary: 0.67.x resolves to 0640; 0.68.0 flips to 0680")
    void boundaryFlipAt0680() {
        assertCreates(sv(0, 67, 999), DummyVerificationSession.class, blockSource);
        assertCreates(sv(0, 68, 0), ExtendedMerkleTreeSession.class, blockSource);
    }

    @Test
    @DisplayName("Below lowest supported (0.64.0) throws")
    void belowLowestThrows() {
        var ex = assertThrows(
                IllegalArgumentException.class,
                () -> HapiVersionSessionFactory.createSession(0L, blockSource, sv(0, 63, 1)));
        assertTrue(
                ex.getMessage().toLowerCase().contains("unsupported hapi version"),
                "message should mention unsupported");
    }

    // ---------- Argument validation tests ----------

    @Test
    @DisplayName("Null blockSource throws NPE")
    void nullBlockSourceThrows() {
        assertThrows(NullPointerException.class, () -> HapiVersionSessionFactory.createSession(0L, null, sv(0, 68, 0)));
    }

    @Test
    @DisplayName("Null hapiVersion throws NPE")
    void nullVersionThrows() {
        assertThrows(NullPointerException.class, () -> HapiVersionSessionFactory.createSession(0L, blockSource, null));
    }

    @Test
    @DisplayName("Negative blockNumber throws IAE")
    void negativeBlockNumberThrows() {
        assertThrows(
                IllegalArgumentException.class,
                () -> HapiVersionSessionFactory.createSession(-1L, blockSource, sv(0, 68, 0)));
    }

    // ---------- Smoke tests with different block numbers ----------

    @Nested
    class BlockNumberAgnostic {
        @Test
        @DisplayName("Uses the same impl regardless of blockNumber (0 and large)")
        void blockNumberDoesNotAffectImpl() {
            var v = sv(0, 68, 3);
            var s1 = HapiVersionSessionFactory.createSession(0L, blockSource, v);
            var s2 = HapiVersionSessionFactory.createSession(9_999_999L, blockSource, v);

            assertAll(
                    () -> assertTrue(s1 instanceof ExtendedMerkleTreeSession),
                    () -> assertTrue(s2 instanceof ExtendedMerkleTreeSession));
        }
    }
}
