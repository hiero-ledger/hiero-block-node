/*
 * Copyright (C) 2024 Hedera Hashgraph, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hedera.block.common.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.function.Consumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link Preconditions} functionality.
 */
class PreconditionsTest {
    /**
     * This test aims to verify that the
     * {@link Preconditions#requireNotBlank(String)} will return the input
     * 'toTest' parameter if the non-blank check passes. Test includes
     * overloads.
     *
     * @param toTest parameterized, the string to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#nonBlankStrings")
    void testRequireNotBlankPass(final String toTest) {
        final Consumer<String> asserts =
                actual -> assertThat(actual).isNotNull().isNotBlank().isEqualTo(toTest);

        final String actual = Preconditions.requireNotBlank(toTest);
        assertThat(actual).satisfies(asserts);

        final String actualOverload = Preconditions.requireNotBlank(toTest, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireNotBlank(String)} will throw an
     * {@link IllegalArgumentException} if the non-blank check fails. Test
     * includes overloads.
     *
     * @param toTest parameterized, the string to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#blankStrings")
    void testRequireNotBlankFail(final String toTest) {
        assertThatIllegalArgumentException().isThrownBy(() -> Preconditions.requireNotBlank(toTest));

        final String testErrorMessage = "test error message";
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireNotBlank(toTest, testErrorMessage))
                .withMessage(testErrorMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireWhole(long)} will return the input 'toTest'
     * parameter if the positive check passes. Test includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#wholeNumbers")
    void testRequireWholePass(final int toTest) {
        final Consumer<Integer> asserts =
                actual -> assertThat(actual).isGreaterThanOrEqualTo(0).isEqualTo(toTest);

        final int actual = (int) Preconditions.requireWhole(toTest);
        assertThat(actual).satisfies(asserts);

        final int actualOverload = (int) Preconditions.requireWhole(toTest, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireWhole(long)} will throw an
     * {@link IllegalArgumentException} if the positive check fails. Test
     * includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#negativeIntegers")
    void testRequireWholeFail(final int toTest) {
        assertThatIllegalArgumentException().isThrownBy(() -> Preconditions.requireWhole(toTest));

        final String testErrorMessage = "test error message";
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireWhole(toTest, testErrorMessage))
                .withMessage(testErrorMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePositive(int)} will return the input 'toTest'
     * parameter if the positive check passes. Test includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#positiveIntegers")
    void testRequirePositivePass(final int toTest) {
        final Consumer<Integer> asserts =
                actual -> assertThat(actual).isPositive().isEqualTo(toTest);

        final int actual = Preconditions.requirePositive(toTest);
        assertThat(actual).satisfies(asserts);

        final int actualOverload = Preconditions.requirePositive(toTest, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePositive(int)} will throw an
     * {@link IllegalArgumentException} if the positive check fails. Test
     * includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#zeroAndNegativeIntegers")
    void testRequirePositiveFail(final int toTest) {
        assertThatIllegalArgumentException().isThrownBy(() -> Preconditions.requirePositive(toTest));

        final String testErrorMessage = "test error message";
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePositive(toTest, testErrorMessage))
                .withMessage(testErrorMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePowerOfTwo(int)} will return the input
     * 'toTest' parameter if the power of two check passes. Test includes
     * overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#powerOfTwoIntegers")
    void testRequirePowerOfTwoPass(final int toTest) {
        final Consumer<Integer> asserts =
                actual -> assertThat(actual).isPositive().isEqualTo(toTest);

        final int actual = Preconditions.requirePowerOfTwo(toTest);
        assertThat(actual).satisfies(asserts);

        final int actualOverload = Preconditions.requirePowerOfTwo(toTest, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePowerOfTwo(int)} will throw an
     * {@link IllegalArgumentException} if the power of two check fails. Test
     * includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource({
        "com.hedera.block.common.CommonsTestUtility#nonPowerOfTwoIntegers",
        "com.hedera.block.common.CommonsTestUtility#negativePowerOfTwoIntegers"
    })
    void testRequirePowerOfTwoFail(final int toTest) {
        assertThatIllegalArgumentException().isThrownBy(() -> Preconditions.requirePowerOfTwo(toTest));

        final String testErrorMessage = "test error message";
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePowerOfTwo(toTest, testErrorMessage))
                .withMessage(testErrorMessage);
    }
}
