// SPDX-License-Identifier: Apache-2.0
/*
 * (c) 2016-2018 Swirlds, Inc.
 *
 * This software is the confidential and proprietary information of
 * Swirlds, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Swirlds.
 *
 * SWIRLDS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SWIRLDS SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */
package org.hiero.block.tools.states.utils;

import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * A drop-in replacement for {@link DataOutputStream}, which handles FastCopyable classes specially. It
 * doesn't add any new methods or variables that are public. It is designed for use with the FastCopyable
 * interface, and its use is described there.
 */
public class FCDataOutputStream extends DataOutputStream {

    /**
     * {@inheritDoc}
     */
    public FCDataOutputStream(OutputStream out) {
        super(out);
    }
}
