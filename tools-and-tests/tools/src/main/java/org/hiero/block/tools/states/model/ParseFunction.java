// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import org.hiero.block.tools.states.utils.FCDataInputStream;

public interface ParseFunction<T> {
    T copyFrom(FCDataInputStream inStream) throws java.io.IOException;
}
