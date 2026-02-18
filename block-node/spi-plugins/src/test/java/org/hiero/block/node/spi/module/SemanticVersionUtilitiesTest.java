// SPDX-License-Identifier: Apache-2.0
//  SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.spi.module;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.node.base.SemanticVersion;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link SemanticVersionUtilities} functionality.
 */
public class SemanticVersionUtilitiesTest {
    /**
     * This test aims to verify that the {@link SemanticVersionUtilities#from(String)}
     * returns {@code true} for valid inputs and {@code false} for invalid inputs
     *
     * @param input parameterized, the String to test
     * @param shouldPass parameterized, the expected result
     */
    @ParameterizedTest
    @MethodSource("goodAndBadSemanticVersions")
    void testRequireNotBlankPass(final String input, final boolean shouldPass) {
        final SemanticVersion semVer = SemanticVersionUtilities.from(input);
        if (semVer != null && !shouldPass) fail("Input <" + input + "> should have failed");
        if (semVer == null && shouldPass) fail("Input <" + input + "> should have passed");
    }

    /**
     * This test aims to verify that the {@link SemanticVersionUtilities#from(Class)}
     * returns a valid version.
     */
    @Test
    void testClassVersions() {
        final SemanticVersion semVer1 = SemanticVersionUtilities.from(SemanticVersionUtilitiesTest.class);
        final SemanticVersion semVer2 = SemanticVersionUtilities.from(SemanticVersionUtilities.class);
        assertEquals(semVer1, semVer2);
    }

    /**
     * Provides valid test data for cases where the value to test is greater than or equal to the base value.
     *
     * @return a stream of arguments where each argument is a pair of {@code (toTest, base)} values,
     *         such that {@code toTest >= base}.
     */
    private static Stream<Arguments> goodAndBadSemanticVersions() {
        return Stream.of(
                Arguments.of("", false),
                Arguments.of("..", false),
                Arguments.of(null, false),
                Arguments.of("..-+", false),
                Arguments.of("0.0.0-+", false),
                Arguments.of("0.0.0-", false),
                Arguments.of("0.0.0", true),
                Arguments.of("v0.0.0", true),
                Arguments.of("V1.2.3", true),
                Arguments.of("0.1.0-alpha+", false),
                Arguments.of("0.1.0-alpha", true),
                Arguments.of("0.1.0-alpha+123A72", true),
                Arguments.of("0.28.0-SNAPSHOT", true));
    }
}
