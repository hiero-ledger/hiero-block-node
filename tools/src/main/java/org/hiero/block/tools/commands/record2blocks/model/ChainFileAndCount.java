// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.record2blocks.model;

/**
 * Simple Record for a ChainFile and the count of how many times there are similar chain files for a record file set.
 *
 * @param chainFile A chain file that is one of the common identical ones in a record file set
 * @param count The number of files that are identical in the record file set
 */
public record ChainFileAndCount(ChainFile chainFile, int count) {}
