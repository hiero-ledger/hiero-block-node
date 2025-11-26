// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.mirrornode;

public class BlockInfo {
    public int count;
    public String hapiVersion;
    public String hash;
    public String name;
    public long number;
    public String previousHash;
    public long size;
    public long gasUsed;
    public String timestampFrom;
    public String timestampTo;

    @Override
    public String toString() {
        return "BlockInfo{" + "number="
                + number + ", name='"
                + name + '\'' + ", hash='"
                + hash + '\'' + ", previousHash='"
                + previousHash + '\'' + ", count="
                + count + ", hapiVersion='"
                + hapiVersion + '\'' + ", size="
                + size + ", gasUsed="
                + gasUsed + ", timestampFrom='"
                + timestampFrom + '\'' + ", timestampTo='"
                + timestampTo + '\'' + '}';
    }
}
