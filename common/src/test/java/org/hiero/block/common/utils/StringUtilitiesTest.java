// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link StringUtilities} functionality.
 */
class StringUtilitiesTest {
    /**
     * This test aims to verify that the {@link StringUtilities#isBlank(String)}
     * returns {@code true} if the input string is blank.
     *
     * @param toTest parameterized, the String to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#blankStrings")
    void testRequireNotBlankPass(final String toTest) {
        assertThat(StringUtilities.isBlank(toTest)).isTrue();
    }

    /**
     * This test aims to verify that the {@link StringUtilities#isBlank(String)}
     * returns {@code false} if the input string is not blank.
     *
     * @param toTest parameterized, the string to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#nonBlankStrings")
    void testRequireNotBlankFail(final String toTest) {
        assertThat(StringUtilities.isBlank(toTest)).isFalse();
    }
}
