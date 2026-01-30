// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;

public interface ParseFunction<T> {
    T copyFrom(DataInputStream inStream) throws java.io.IOException;
}
