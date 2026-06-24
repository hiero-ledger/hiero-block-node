// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.base;

/**
 * Constants for PBJ codec parsing.
 *
 * <p>PBJ 0.15.9 reduced the library default ({@code Codec.DEFAULT_MAX_DEPTH}) from 512 to 128.
 * All parse call sites must pass this constant explicitly so they are not silently affected
 * by future changes to the library default (see issue #3056).
 */
public final class ParseConstants {

    /** Maximum nesting depth for all PBJ codec parse calls. */
    public static final int MAX_PARSE_DEPTH = 256;

    private ParseConstants() {}
}
