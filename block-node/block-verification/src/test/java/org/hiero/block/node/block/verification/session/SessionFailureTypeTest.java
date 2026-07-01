// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.block.verification.session;

import static org.assertj.core.api.Assertions.assertThat;

import org.hiero.block.node.spi.blockmessaging.VerificationNotification.FailureType;
import org.junit.jupiter.api.Test;

class SessionFailureTypeTest {

    @Test
    void badBlockProofMapsToCorrectFailureType() {
        assertThat(SessionFailureType.BAD_BLOCK_PROOF.asFailureType()).isEqualTo(FailureType.BAD_BLOCK_PROOF);
    }

    @Test
    void unableToParseMapsToCorrectFailureType() {
        assertThat(SessionFailureType.UNABLE_TO_PARSE.asFailureType()).isEqualTo(FailureType.UNABLE_TO_PARSE);
    }

    @Test
    void missingMandatoryItemMapsToCorrectFailureType() {
        assertThat(SessionFailureType.MISSING_MANDATORY_ITEM.asFailureType())
                .isEqualTo(FailureType.MISSING_MANDATORY_ITEM);
    }

    @Test
    void missingMandatoryFieldMapsToCorrectFailureType() {
        assertThat(SessionFailureType.MISSING_MANDATORY_FIELD.asFailureType())
                .isEqualTo(FailureType.MISSING_MANDATORY_FIELD);
    }

    @Test
    void missingVerificationDataMapsToCorrectFailureType() {
        assertThat(SessionFailureType.MISSING_VERIFICATION_DATA.asFailureType())
                .isEqualTo(FailureType.MISSING_VERIFICATION_DATA);
    }

    @Test
    void unrecognizedProofTypeMapsToCorrectFailureType() {
        assertThat(SessionFailureType.UNRECOGNIZED_PROOF_TYPE.asFailureType())
                .isEqualTo(FailureType.UNRECOGNIZED_PROOF_TYPE);
    }

    @Test
    void unsupportedHapiVersionMapsToCorrectFailureType() {
        assertThat(SessionFailureType.UNSUPPORTED_HAPI_VERSION.asFailureType())
                .isEqualTo(FailureType.UNSUPPORTED_HAPI_VERSION);
    }

    @Test
    void cancelledMapsToCorrectFailureType() {
        assertThat(SessionFailureType.CANCELLED.asFailureType()).isEqualTo(FailureType.CANCELLED);
    }

    @Test
    void unknownErrorMapsToCorrectFailureType() {
        assertThat(SessionFailureType.UNKNOWN_ERROR.asFailureType()).isEqualTo(FailureType.UNKNOWN_ERROR);
    }

    @Test
    void allValuesMappedToNonNull() {
        for (final SessionFailureType type : SessionFailureType.values()) {
            assertThat(type.asFailureType()).as("mapping for %s", type).isNotNull();
        }
    }

    @Test
    void enumValueCount_matchesExpected() {
        assertThat(SessionFailureType.values()).hasSize(9);
    }
}
