// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.common.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link Preconditions} functionality.
 */
class PreconditionsTest {
    private static final String DEFAULT_NOT_BLANK_MESSAGE = "The input String is required to be non-blank.";
    private static final String DEFAULT_REQUIRE_POSITIVE_MESSAGE = "The input number [%d] is required to be positive.";
    private static final String DEFAULT_GT_OR_EQ_MESSAGE =
            "The input number [%d] is required to be greater or equal than [%d].";
    private static final String DEFAULT_EXACTLY_DIVISIBLE_MESSAGE =
            "The input number [%d] is required to be exactly divisible by [%d].";
    private static final String DEFAULT_REQUIRE_IN_RANGE_MESSAGE =
            "The input number [%d] is required to be in the range [%d, %d] boundaries included.";
    private static final String DEFAULT_REQUIRE_WHOLE_MESSAGE =
            "The input number [%d] is required to be a whole number.";
    private static final String DEFAULT_REQUIRE_POWER_OF_TWO_MESSAGE =
            "The input number [%d] is required to be a power of two.";
    private static final String DEFAULT_REQUIRE_IS_EVEN = "The input number [%d] is required to be even.";
    private static final String DEFAULT_REQUIRE_POSITIVE_POWER_OF_10_MESSAGE =
            "The input number [%d] is required to be a positive power of 10.";

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
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireNotBlank(toTest))
                .withMessage(DEFAULT_NOT_BLANK_MESSAGE);

        final String testErrorMessage = DEFAULT_NOT_BLANK_MESSAGE.concat(" custom test error message");
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
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireWhole(toTest))
                .withMessage(DEFAULT_REQUIRE_WHOLE_MESSAGE.formatted(toTest));

        final String testMessage = DEFAULT_REQUIRE_WHOLE_MESSAGE.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireWhole(toTest, testMessage))
                .withMessage(expectedTestMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireGreaterOrEqual(long, long)} will return the input
     * 'toTest' parameter if the check passes.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#validGreaterOrEqualValues")
    void testRequireGreaterOrEqualPass(final long toTest, final long base) {
        final Consumer<Long> asserts =
                actual -> assertThat(actual).isGreaterThanOrEqualTo(base).isEqualTo(toTest);

        final long actual = Preconditions.requireGreaterOrEqual(toTest, base);
        assertThat(actual).satisfies(asserts);

        final long actualOverload = Preconditions.requireGreaterOrEqual(toTest, base, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireGreaterOrEqual(long, long)} will throw an
     * {@link IllegalArgumentException} if the check fails.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#invalidGreaterOrEqualValues")
    void testRequireGreaterOrEqualFail(final long toTest, final long base) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireGreaterOrEqual(toTest, base))
                .withMessage(DEFAULT_GT_OR_EQ_MESSAGE.formatted(toTest, base));

        final String testMessage = DEFAULT_GT_OR_EQ_MESSAGE.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest, base);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireGreaterOrEqual(toTest, base, testMessage))
                .withMessage(expectedTestMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireExactlyDivisibleBy(long, long)} will return
     * the input 'toTest' parameter if the modulo division check passes. Test
     * includes overloads.
     *
     * @param toTest parameterized, the dividend
     * @param modulus parameterized, the modulus
     */
    @ParameterizedTest
    @MethodSource("moduloDivisionPairsZeroRemainder")
    void testRequireModuloDivisionZeroRemainderPass(final int toTest, final int modulus) {
        final Consumer<Long> asserts = actual -> assertThat(actual).isEqualTo(toTest);

        final long actual = Preconditions.requireExactlyDivisibleBy(toTest, modulus);
        assertThat(actual).satisfies(asserts);

        final long actualOverload = Preconditions.requireExactlyDivisibleBy(toTest, modulus, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireExactlyDivisibleBy(long, long)} will throw an
     * {@link IllegalArgumentException} if the modulo division check fails. Test
     * includes overloads.
     *
     * @param toTest parameterized, the dividend
     * @param modulus parameterized, the modulus
     */
    @ParameterizedTest
    @MethodSource("moduloDivisionPairsNonZeroRemainder")
    void testRequireModuloDivisionNonZeroRemainderFail(final int toTest, final int modulus) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireExactlyDivisibleBy(toTest, modulus))
                .withMessage(DEFAULT_EXACTLY_DIVISIBLE_MESSAGE.formatted(toTest, modulus));

        final String testMessage = DEFAULT_EXACTLY_DIVISIBLE_MESSAGE.concat("custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest, modulus);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireExactlyDivisibleBy(toTest, modulus, testMessage))
                .withMessage(expectedTestMessage);
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
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePositive(toTest))
                .withMessage(DEFAULT_REQUIRE_POSITIVE_MESSAGE.formatted(toTest));

        final String testMessage = DEFAULT_REQUIRE_POSITIVE_MESSAGE.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePositive(toTest, testMessage))
                .withMessage(expectedTestMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePositive(long)} will return the input 'toTest'
     * parameter if the positive check passes. Test includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#positiveIntegers")
    void testRequirePositiveLongPass(final long toTest) {
        final Consumer<Long> asserts = actual -> assertThat(actual).isPositive().isEqualTo(toTest);

        final long actual = Preconditions.requirePositive(toTest);
        assertThat(actual).satisfies(asserts);

        final long actualOverload = Preconditions.requirePositive(toTest, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePositive(long)} will throw an
     * {@link IllegalArgumentException} if the positive check fails. Test
     * includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#zeroAndNegativeIntegers")
    void testRequirePositiveLongFail(final long toTest) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePositive(toTest))
                .withMessage(DEFAULT_REQUIRE_POSITIVE_MESSAGE.formatted(toTest));

        final String testMessage = DEFAULT_REQUIRE_POSITIVE_MESSAGE.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePositive(toTest, testMessage))
                .withMessage(expectedTestMessage);
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
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePowerOfTwo(toTest))
                .withMessage(DEFAULT_REQUIRE_POWER_OF_TWO_MESSAGE.formatted(toTest));

        final String testMessage = DEFAULT_REQUIRE_POWER_OF_TWO_MESSAGE.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePowerOfTwo(toTest, testMessage))
                .withMessage(expectedTestMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireInRange(int, int, int)} will return the
     * input 'toTest' parameter if the range check passes. Test includes
     * overloads.
     *
     * @param toTest parameterized, the number to test
     * @param lowerBoundary parameterized, the lower boundary
     * @param upperBoundary parameterized, the upper boundary
     */
    @ParameterizedTest
    @MethodSource("validRequireInRangeValues")
    void testRequireInRangePass(final int toTest, final int lowerBoundary, final int upperBoundary) {
        final Consumer<Integer> asserts = actual ->
                assertThat(actual).isBetween(lowerBoundary, upperBoundary).isEqualTo(toTest);

        final int actual = Preconditions.requireInRange(toTest, lowerBoundary, upperBoundary);
        assertThat(actual).satisfies(asserts);

        final int actualOverload =
                Preconditions.requireInRange(toTest, lowerBoundary, upperBoundary, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireInRange(int, int, int)} will throw an
     * {@link IllegalArgumentException} if the range check fails. Test includes
     * overloads.
     *
     * @param toTest parameterized, the number to test
     * @param lowerBoundary parameterized, the lower boundary
     * @param upperBoundary parameterized, the upper boundary
     */
    @ParameterizedTest
    @MethodSource("invalidRequireInRangeValues")
    void testRequireInRangeFail(final int toTest, final int lowerBoundary, final int upperBoundary) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireInRange(toTest, lowerBoundary, upperBoundary))
                .withMessage(DEFAULT_REQUIRE_IN_RANGE_MESSAGE.formatted(toTest, lowerBoundary, upperBoundary));

        final String testMessage = DEFAULT_REQUIRE_IN_RANGE_MESSAGE.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest, lowerBoundary, upperBoundary);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireInRange(toTest, lowerBoundary, upperBoundary, testMessage))
                .withMessage(expectedTestMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requireEven(int)} will return the input 'toTest'
     * parameter if the even check passes. Test includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#evenIntegers")
    void testRequireEvenPass(final int toTest) {
        final Consumer<Integer> asserts = actual -> assertThat(actual).isEven().isEqualTo(toTest);

        final int actual = Preconditions.requireEven(toTest);
        assertThat(actual).satisfies(asserts);

        final int actualOverload = Preconditions.requireEven(toTest, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /** This test aims to verify that the {@link Preconditions#requireEven(int)} will throw an {@link IllegalArgumentException} if the even check fails. Test includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#oddIntegers")
    void testRequireEvenFail(final int toTest) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireEven(toTest))
                .withMessage(DEFAULT_REQUIRE_IS_EVEN.formatted(toTest));

        final String testMessage = DEFAULT_REQUIRE_IS_EVEN.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requireEven(toTest, testMessage))
                .withMessage(expectedTestMessage);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePositivePowerOf10(int)} will return the input
     * 'toTest' parameter if the positive power of 10 check passes. Test
     * includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource("com.hedera.block.common.CommonsTestUtility#positiveIntPowersOf10")
    void testRequirePositivePowerOf10Pass(final int toTest) {
        final Consumer<Integer> asserts =
                actual -> assertThat(actual).isPositive().isEqualTo(toTest);

        final int actual = Preconditions.requirePositivePowerOf10(toTest);
        assertThat(actual).satisfies(asserts);

        final int actualOverload = Preconditions.requirePositivePowerOf10(toTest, "test error message");
        assertThat(actualOverload).satisfies(asserts);
    }

    /**
     * This test aims to verify that the
     * {@link Preconditions#requirePositivePowerOf10(int)} will throw an
     * {@link IllegalArgumentException} if the positive power of 10 check fails.
     * Test includes overloads.
     *
     * @param toTest parameterized, the number to test
     */
    @ParameterizedTest
    @MethodSource({
        "com.hedera.block.common.CommonsTestUtility#negativeIntPowersOf10",
        "com.hedera.block.common.CommonsTestUtility#nonPowersOf10"
    })
    void testRequirePositivePowerOf10Fail(final int toTest) {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePositivePowerOf10(toTest))
                .withMessage(DEFAULT_REQUIRE_POSITIVE_POWER_OF_10_MESSAGE.formatted(toTest));

        final String testMessage = DEFAULT_REQUIRE_POSITIVE_POWER_OF_10_MESSAGE.concat(" custom test error message");
        final String expectedTestMessage = testMessage.formatted(toTest);
        assertThatIllegalArgumentException()
                .isThrownBy(() -> Preconditions.requirePositivePowerOf10(toTest, testMessage))
                .withMessage(expectedTestMessage);
    }

    private static Stream<Arguments> validRequireInRangeValues() {
        return Stream.of(
                Arguments.of(0, 0, 0),
                Arguments.of(0, 0, 1),
                Arguments.of(1, 0, 1),
                Arguments.of(1, 0, 2),
                Arguments.of(-1, -1, -1),
                Arguments.of(-2, -2, -1),
                Arguments.of(-1, -2, -1),
                Arguments.of(-1, -2, 0));
    }

    private static Stream<Arguments> invalidRequireInRangeValues() {
        return Stream.of(
                Arguments.of(0, 1, 1),
                Arguments.of(0, 1, 2),
                Arguments.of(1, 2, 3),
                Arguments.of(-1, 0, 1),
                Arguments.of(-1, 0, 0),
                Arguments.of(1, 0, 0));
    }

    private static Stream<Arguments> moduloDivisionPairsZeroRemainder() {
        return Stream.of(
                Arguments.of(0, 10),
                Arguments.of(6, 3),
                Arguments.of(8, 2),
                Arguments.of(8, 4),
                Arguments.of(10, 10),
                Arguments.of(20, 10),
                Arguments.of(100, 10),
                Arguments.of(1_000, 10),
                Arguments.of(10_000, 10),
                Arguments.of(100_000, 10),
                Arguments.of(1_000_000, 10),
                Arguments.of(10_000_000, 10),
                Arguments.of(100_000_000, 10),
                Arguments.of(1_000_000_000, 10),
                Arguments.of(-10, 10),
                Arguments.of(-20, 10),
                Arguments.of(-100, 10),
                Arguments.of(-1_000, 10),
                Arguments.of(-10_000, 10),
                Arguments.of(-100_000, 10),
                Arguments.of(-1_000_000, 10),
                Arguments.of(-10_000_000, 10),
                Arguments.of(-100_000_000, 10),
                Arguments.of(-1_000_000_000, 10));
    }

    private static Stream<Arguments> moduloDivisionPairsNonZeroRemainder() {
        return Stream.of(
                Arguments.of(1, 10),
                Arguments.of(6, 4),
                Arguments.of(7, 2),
                Arguments.of(7, 4),
                Arguments.of(11, 10),
                Arguments.of(21, 10),
                Arguments.of(101, 10),
                Arguments.of(1_001, 10),
                Arguments.of(10_001, 10),
                Arguments.of(100_001, 10),
                Arguments.of(1_000_001, 10),
                Arguments.of(10_000_001, 10),
                Arguments.of(100_000_001, 10),
                Arguments.of(1_000_000_001, 10),
                Arguments.of(-1, 10),
                Arguments.of(-11, 10),
                Arguments.of(-21, 10),
                Arguments.of(-101, 10),
                Arguments.of(-1_001, 10),
                Arguments.of(-10_001, 10),
                Arguments.of(-100_001, 10),
                Arguments.of(-1_000_001, 10),
                Arguments.of(-10_000_001, 10),
                Arguments.of(-100_000_001, 10),
                Arguments.of(-1_000_000_001, 10));
    }
}
