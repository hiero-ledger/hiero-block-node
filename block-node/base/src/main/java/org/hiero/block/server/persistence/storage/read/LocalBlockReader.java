// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.persistence.storage.read;

/**
 * A marker interface that groups all writers that operate on a local file
 * system.
 *
 * @param <T> the type that will be returned after reading the block
 */
public interface LocalBlockReader<T> extends BlockReader<T> {}
