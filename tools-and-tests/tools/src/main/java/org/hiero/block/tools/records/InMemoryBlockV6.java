package org.hiero.block.tools.records;

import com.hedera.hapi.node.base.NodeAddressBook;
import java.time.Instant;
import java.util.List;

public class InMemoryBlockV6 extends InMemoryBlockV5 {

    public InMemoryBlockV6(Instant recordFileTime, InMemoryFile primaryRecordFile,
        List<InMemoryFile> otherRecordFiles, List<InMemoryFile> signatureFiles,
        List<InMemoryFile> primarySidecarFiles, List<InMemoryFile> otherSidecarFiles) {
        super(recordFileTime, primaryRecordFile, otherRecordFiles, signatureFiles, primarySidecarFiles,
            otherSidecarFiles);
    }

    @Override
    public ValidationResult validate(byte[] startRunningHash, NodeAddressBook addressBook) {
        return null;
    }
}
