// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.history.model;

import java.nio.file.Path;

/**
 * In-memory representation of a file with its path and data.
 *
 * @param path relative path of the file
 * @param data the file data in bytes
 */
public record InMemoryFile(Path path, byte[] data) {}
