// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.swirlds.config.api.validation.ConfigViolationException;
import org.hiero.block.node.app.fixtures.TestConfigurationBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/// Tests for the validation of the [VerificationConfig].
@DisplayName("Verification Config Tests")
class VerificationConfigTest {
    /// Tests for the size of the active sessions buffer.
    @Nested
    @DisplayName("Active Sessions Buffer Size")
    class ActiveSessionsBufferSize {
        /// This test aims to assert that the active sessions buffer size defaults to
        /// 100 when it is not configured.
        @Test
        @DisplayName("activeSessionsBufferSize defaults to 100")
        void testActiveSessionsBufferSizeDefaultsToOneHundred() {
            final VerificationConfig config = new TestConfigurationBuilder()
                    .withConfigDataType(VerificationConfig.class)
                    .getOrCreateConfig()
                    .getConfigData(VerificationConfig.class);
            assertThat(config.activeSessionsBufferSize()).isEqualTo(100);
        }

        /// This test aims to assert that the smallest valid active sessions buffer
        /// size, 1, is accepted.
        @Test
        @DisplayName("activeSessionsBufferSize accepts 1")
        void testAcceptsAnActiveSessionsBufferSizeOfOne() {
            final VerificationConfig config =
                    builderWithBufferSize("1").getOrCreateConfig().getConfigData(VerificationConfig.class);
            assertThat(config.activeSessionsBufferSize()).isEqualTo(1);
        }

        /// This test aims to assert that an active sessions buffer size below 1 is
        /// rejected when the configuration is built, since a buffer whose limit is
        /// below one session would consult the eviction policy on every admission.
        @ParameterizedTest
        @ValueSource(strings = {"0", "-1"})
        @DisplayName("activeSessionsBufferSize below 1 is rejected")
        void testRejectsAnActiveSessionsBufferSizeBelowOne(final String bufferSize) {
            final TestConfigurationBuilder builder = builderWithBufferSize(bufferSize);
            assertThatExceptionOfType(ConfigViolationException.class).isThrownBy(builder::getOrCreateConfig);
        }
    }

    /// A configuration builder for the verification configuration with the given
    /// active sessions buffer size.
    private static TestConfigurationBuilder builderWithBufferSize(final String bufferSize) {
        return new TestConfigurationBuilder()
                .withConfigDataType(VerificationConfig.class)
                .withValue("verification.activeSessionsBufferSize", bufferSize);
    }
}
