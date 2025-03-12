// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.verification.service;

import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NoOpBlockVerificationServiceTest {

    @Test
    void onBlockItemsReceived() {
        NoOpBlockVerificationService noOpBlockVerificationService = new NoOpBlockVerificationService();
        noOpBlockVerificationService.onBlockItemsReceived(new ArrayList<>());
    }
}
