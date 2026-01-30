// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;

public interface DeserializeFunction<T extends Record> {
    T deserialize(DataInputStream inStream) throws java.io.IOException;
}
