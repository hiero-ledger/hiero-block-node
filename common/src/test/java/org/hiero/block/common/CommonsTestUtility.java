// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.common;

import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Utilities for testing the common module.
 */
public final class CommonsTestUtility {
    /**
     * Some simple non-blank strings only with spaces and new lines and tabs (there are more whitespace chars).
     */
    public static Stream<Arguments> nonBlankStrings() {
        return Stream.of(
                Arguments.of("a"),
                Arguments.of(" a"),
                Arguments.of("a "),
                Arguments.of(" a "),
                Arguments.of("a b"),
                Arguments.of("\na"), // new line
                Arguments.of("\ta"), // tab
                Arguments.of("\fa"), // form feed
                Arguments.of("\ra"), // carriage return
                Arguments.of("\u000Ba"), // vertical tab
                Arguments.of("\u001Ca"), // file separator
                Arguments.of("\u001Da"), // group separator
                Arguments.of("\u001Ea"), // record separator
                Arguments.of("\u001Fa") // unit separator
                );
    }

    /**
     * Some simple blank strings.
     */
    public static Stream<Arguments> blankStrings() {
        return Stream.of(
                Arguments.of((String) null),
                Arguments.of(""),
                Arguments.of(" "),
                Arguments.of("\n"), // new line
                Arguments.of("\t"), // tab
                Arguments.of("\f"), // form feed
                Arguments.of("\r"), // carriage return
                Arguments.of("\u000B"), // vertical tab
                Arguments.of("\u001C"), // file separator
                Arguments.of("\u001D"), // group separator
                Arguments.of("\u001E"), // record separator
                Arguments.of("\u001F") // unit separator
                );
    }

    /**
     * Some valid power of two integers.
     */
    public static Stream<Arguments> powerOfTwoIntegers() {
        return Stream.of(
                Arguments.of(1),
                Arguments.of(2),
                Arguments.of(4),
                Arguments.of(8),
                Arguments.of(16),
                Arguments.of(32),
                Arguments.of(64),
                Arguments.of(128),
                Arguments.of(256),
                Arguments.of(512),
                Arguments.of(1_024),
                Arguments.of(2_048),
                Arguments.of(4_096),
                Arguments.of(8_192),
                Arguments.of(16_384),
                Arguments.of(32_768),
                Arguments.of(65_536),
                Arguments.of(131_072),
                Arguments.of(262_144),
                Arguments.of(524_288),
                Arguments.of(1_048_576),
                Arguments.of(2_097_152),
                Arguments.of(4_194_304),
                Arguments.of(8_388_608),
                Arguments.of(16_777_216),
                Arguments.of(33_554_432),
                Arguments.of(67_108_864),
                Arguments.of(134_217_728),
                Arguments.of(268_435_456),
                Arguments.of(536_870_912),
                Arguments.of(1_073_741_824));
    }

    /**
     * Some power of two integers, but with negative sign.
     */
    public static Stream<Arguments> negativePowerOfTwoIntegers() {
        return Stream.of(
                Arguments.of(-1),
                Arguments.of(-2),
                Arguments.of(-4),
                Arguments.of(-8),
                Arguments.of(-16),
                Arguments.of(-32),
                Arguments.of(-64),
                Arguments.of(-128),
                Arguments.of(-256),
                Arguments.of(-512),
                Arguments.of(-1_024),
                Arguments.of(-2_048),
                Arguments.of(-4_096),
                Arguments.of(-8_192),
                Arguments.of(-16_384),
                Arguments.of(-32_768),
                Arguments.of(-65_536),
                Arguments.of(-131_072),
                Arguments.of(-262_144),
                Arguments.of(-524_288),
                Arguments.of(-1_048_576),
                Arguments.of(-2_097_152),
                Arguments.of(-4_194_304),
                Arguments.of(-8_388_608),
                Arguments.of(-16_777_216),
                Arguments.of(-33_554_432),
                Arguments.of(-67_108_864),
                Arguments.of(-134_217_728),
                Arguments.of(-268_435_456),
                Arguments.of(-536_870_912),
                Arguments.of(-1_073_741_824));
    }

    /**
     * Some non power of two integers.
     */
    public static Stream<Arguments> nonPowerOfTwoIntegers() {
        return Stream.of(
                Arguments.of(0),
                Arguments.of(3),
                Arguments.of(5),
                Arguments.of(6),
                Arguments.of(7),
                Arguments.of(9),
                Arguments.of(10),
                Arguments.of(11),
                Arguments.of(511),
                Arguments.of(1_023),
                Arguments.of(4_097),
                Arguments.of(16_381),
                Arguments.of(65_535),
                Arguments.of(524_287),
                Arguments.of(33_554_431),
                Arguments.of(1_073_741_825));
    }

    /**
     * Some positive integers.
     */
    public static Stream<Arguments> positiveIntegers() {
        return Stream.of(
                Arguments.of(1),
                Arguments.of(2),
                Arguments.of(3),
                Arguments.of(4),
                Arguments.of(5),
                Arguments.of(100),
                Arguments.of(1_000),
                Arguments.of(10_000),
                Arguments.of(100_000),
                Arguments.of(1_000_000),
                Arguments.of(10_000_000));
    }

    /**
     * Some whole numbers.
     */
    public static Stream<Arguments> wholeNumbers() {
        return Stream.concat(Stream.of(Arguments.of(0)), positiveIntegers());
    }

    /**
     * Some negative integers.
     */
    public static Stream<Arguments> negativeIntegers() {
        return Stream.of(
                Arguments.of(-1),
                Arguments.of(-2),
                Arguments.of(-3),
                Arguments.of(-4),
                Arguments.of(-5),
                Arguments.of(-100),
                Arguments.of(-1_000),
                Arguments.of(-10_000),
                Arguments.of(-100_000),
                Arguments.of(-1_000_000),
                Arguments.of(-10_000_000));
    }

    /**
     * Some even numbers.
     */
    public static Stream<Arguments> evenIntegers() {
        return Stream.of(
                Arguments.of(-4),
                Arguments.of(-2),
                Arguments.of(0),
                Arguments.of(2),
                Arguments.of(4),
                Arguments.of(6),
                Arguments.of(8),
                Arguments.of(10),
                Arguments.of(100),
                Arguments.of(1_000));
    }

    /**
     * Some odd numbers.
     */
    public static Stream<Arguments> oddIntegers() {
        return Stream.of(
                Arguments.of(-3),
                Arguments.of(-1),
                Arguments.of(1),
                Arguments.of(3),
                Arguments.of(5),
                Arguments.of(7),
                Arguments.of(9),
                Arguments.of(11),
                Arguments.of(101),
                Arguments.of(1_001));
    }

    /**
     * Provides valid test data for cases where the value to test is greater than or equal to the base value.
     *
     * @return a stream of arguments where each argument is a pair of {@code (toTest, base)} values,
     *         such that {@code toTest >= base}.
     */
    public static Stream<Arguments> validGreaterOrEqualValues() {
        return Stream.of(Arguments.of(10L, 5L), Arguments.of(5L, 5L), Arguments.of(0L, -5L), Arguments.of(100L, 50L));
    }

    /**
     * Provides invalid test data for cases where the value to test is less than the base value.
     *
     * @return a stream of arguments where each argument is a pair of {@code (toTest, base)} values,
     *         such that {@code toTest < base}.
     */
    public static Stream<Arguments> invalidGreaterOrEqualValues() {
        return Stream.of(Arguments.of(3L, 5L), Arguments.of(-10L, -5L), Arguments.of(0L, 1L), Arguments.of(-1L, 0L));
    }

    /**
     * Zero and some negative integers.
     */
    public static Stream<Arguments> zeroAndNegativeIntegers() {
        return Stream.concat(Stream.of(Arguments.of(0)), negativeIntegers());
    }

    public static Stream<Arguments> positivePowersOf10() {
        return Stream.of(
                Arguments.of(1L),
                Arguments.of(10L),
                Arguments.of(100L),
                Arguments.of(1_000L),
                Arguments.of(10_000L),
                Arguments.of(100_000L),
                Arguments.of(1_000_000L),
                Arguments.of(10_000_000L),
                Arguments.of(100_000_000L),
                Arguments.of(1_000_000_000L),
                Arguments.of(10_000_000_000L),
                Arguments.of(100_000_000_000L),
                Arguments.of(1_000_000_000_000L),
                Arguments.of(10_000_000_000_000L),
                Arguments.of(100_000_000_000_000L),
                Arguments.of(1_000_000_000_000_000L),
                Arguments.of(10_000_000_000_000_000L),
                Arguments.of(100_000_000_000_000_000L),
                Arguments.of(1_000_000_000_000_000_000L));
    }

    public static Stream<Arguments> negativePowersOf10() {
        return Stream.of(
                Arguments.of(-1L),
                Arguments.of(-10L),
                Arguments.of(-100L),
                Arguments.of(-1_000L),
                Arguments.of(-10_000L),
                Arguments.of(-100_000L),
                Arguments.of(-1_000_000L),
                Arguments.of(-10_000_000L),
                Arguments.of(-100_000_000L),
                Arguments.of(-1_000_000_000L),
                Arguments.of(-10_000_000_000L),
                Arguments.of(-100_000_000_000L),
                Arguments.of(-1_000_000_000_000L),
                Arguments.of(-10_000_000_000_000L),
                Arguments.of(-100_000_000_000_000L),
                Arguments.of(-1_000_000_000_000_000L),
                Arguments.of(-10_000_000_000_000_000L),
                Arguments.of(-100_000_000_000_000_000L),
                Arguments.of(-1_000_000_000_000_000_000L));
    }

    public static Stream<Arguments> positiveIntPowersOf10() {
        return Stream.of(
                Arguments.of(1),
                Arguments.of(10),
                Arguments.of(100),
                Arguments.of(1_000),
                Arguments.of(10_000),
                Arguments.of(100_000),
                Arguments.of(1_000_000),
                Arguments.of(10_000_000),
                Arguments.of(100_000_000),
                Arguments.of(1_000_000_000));
    }

    public static Stream<Arguments> negativeIntPowersOf10() {
        return Stream.of(
                Arguments.of(-1),
                Arguments.of(-10),
                Arguments.of(-100),
                Arguments.of(-1_000),
                Arguments.of(-10_000),
                Arguments.of(-100_000),
                Arguments.of(-1_000_000),
                Arguments.of(-10_000_000),
                Arguments.of(-100_000_000),
                Arguments.of(-1_000_000_000));
    }

    // @todo(517) add 0 and MIN MAX values here as well, also, make the same logic to test for longs as well
    public static Stream<Arguments> nonPowersOf10() {
        return Stream.of(
                Arguments.of(2),
                Arguments.of(11),
                Arguments.of(20),
                Arguments.of(50),
                Arguments.of(101),
                Arguments.of(10_100));
    }

    private CommonsTestUtility() {}
}
