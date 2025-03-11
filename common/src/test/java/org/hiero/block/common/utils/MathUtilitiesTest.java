// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common.utils;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link MathUtilities} functionality.
 */
class MathUtilitiesTest {
    /**
     * This test aims to verify that the {@link MathUtilities#isPowerOfTwo(int)}
     * returns {@code true} if the input number is a power of two.
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#powerOfTwoIntegers")
    void testIsPowerOfTwoPass(final int toTest) {
        final boolean actual = MathUtilities.isPowerOfTwo(toTest);
        assertThat(actual).isTrue();
    }

    /**
     * This test aims to verify that the {@link MathUtilities#isPowerOfTwo(int)}
     * returns {@code false} if the input number is not a power of two.
     */
    @ParameterizedTest
    @MethodSource({
        "com.hedera.block.common.CommonsTestUtility#nonPowerOfTwoIntegers",
        "com.hedera.block.common.CommonsTestUtility#negativePowerOfTwoIntegers"
    })
    void testIsPowerOfTwoFail(final int toTest) {
        final boolean actual = MathUtilities.isPowerOfTwo(toTest);
        assertThat(actual).isFalse();
    }

    /**
     * This test aims to verify that the {@link MathUtilities#isEven(int)}
     * returns {@code true} if the input number is even.
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#evenIntegers")
    void testIsEvenPass(final int toTest) {
        final boolean actual = MathUtilities.isEven(toTest);
        assertThat(actual).isTrue();
    }

    /**
     * This test aims to verify that the {@link MathUtilities#isEven(int)}
     * returns {@code false} if the input number is odd.
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#oddIntegers")
    void testIsEvenFail(final int toTest) {
        final boolean actual = MathUtilities.isEven(toTest);
        assertThat(actual).isFalse();
    }

    /**
     * This test aims to verify that the
     * {@link MathUtilities#isPositivePowerOf10(long)} returns {@code true} if
     * the input to check is a positive power of 10.
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#positivePowersOf10")
    void testIsPositivePowerOf10Pass(final long toTest) {
        final boolean actual = MathUtilities.isPositivePowerOf10(toTest);
        assertThat(actual).isTrue();
    }

    /**
     * This test aims to verify that the
     * {@link MathUtilities#isPositivePowerOf10(long)} returns {@code true} if
     * the input to check is a positive power of 10.
     */
    @ParameterizedTest
    @MethodSource({
        "com.hedera.block.common.CommonsTestUtility#negativePowersOf10",
        "com.hedera.block.common.CommonsTestUtility#nonPowersOf10"
    })
    void testIsPositivePowerOf10Fail(final long toTest) {
        final boolean actual = MathUtilities.isPositivePowerOf10(toTest);
        assertThat(actual).isFalse();
    }
}
