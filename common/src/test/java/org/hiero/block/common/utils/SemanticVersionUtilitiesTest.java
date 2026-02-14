// SPDX-License-Identifier: Apache-2.0
//  SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.hedera.hapi.node.base.SemanticVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
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
    @MethodSource("org.hiero.block.common.CommonsTestUtility#goodAndBadSemanticVersions")
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
}
