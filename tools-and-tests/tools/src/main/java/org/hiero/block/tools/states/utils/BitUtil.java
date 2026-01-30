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

public final class BitUtil {

    /**
     * Finds b = leftmost 1 bit in size (assuming size > 1)
     *
     * @param value
     * 		> 1
     * @return leftmost 1 bit
     */
    public static long findLeftMostBit(final long value) {
        if (value == 0) {
            return 0;
        }

        long leftMostBit = 1L << 62;
        while ((value & leftMostBit) == 0) {
            leftMostBit >>= 1;
        }

        return leftMostBit;
    }
}
