// SPDX-License-Identifier: Apache-2.0
/*
 * (c) 2016-2019 Swirlds, Inc.
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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public abstract class DataStreamUtils {

    /**
     * write a byte array to the given stream
     *
     * @param stream
     * 		the stream to write to
     * @param data
     * 		the array to write
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static void writeByteArray(DataOutputStream stream, byte[] data) throws IOException {
        int len = (data == null ? 0 : data.length);
        stream.writeInt(len);
        for (int i = 0; i < len; i++) {
            stream.writeByte(data[i]);
        }
    }

    /**
     * read a byte array from the given stream
     *
     * @param stream
     * 		the stream to read from
     * @return the array that was read
     * @throws IOException
     * 		thrown if there are any problems during the operation
     */
    public static byte[] readByteArray(DataInputStream stream) throws IOException {
        int len = stream.readInt();
        byte[] data = new byte[len];
        for (int i = 0; i < len; i++) {
            data[i] = stream.readByte();
        }
        return data;
    }
}
