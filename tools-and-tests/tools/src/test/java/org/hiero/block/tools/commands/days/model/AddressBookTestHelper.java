// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.commands.days.model;

import com.hedera.hapi.node.transaction.TransactionBody;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.WritableStreamingData;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.hiero.block.tools.days.model.TarZstdDayReader;
import org.hiero.block.tools.records.model.unparsed.UnparsedRecordBlock;

public class AddressBookTestHelper {
    // Set of blocks that contain all the file changes for the 2021-06-08 address book update
    public static final Set<String> REC_FILE_NAMES = Set.of(
            "2021-06-08T17_35_26.000831000Z.rcd",
            "2021-06-08T17_35_34.007644000Z.rcd",
            "2021-06-08T17_35_42.084364000Z.rcd",
            "2021-06-08T17_35_50.003420000Z.rcd",
            "2021-06-08T17_35_56.045590000Z.rcd");

    public static void main(String[] args) {
        List<UnparsedRecordBlock> day = TarZstdDayReader.readTarZstd(Path.of("REAL_DATA/2021-06-08.tar.zstd"));

        List<TransactionBody> addressBookTransactions = day.stream()
                .filter(b -> REC_FILE_NAMES.contains(
                        b.primaryRecordFile().path().getFileName().toString()))
                .flatMap(block -> {
                    var vr = block.validate(null, null);
                    return vr.addressBookTransactions().stream();
                })
                .toList();
        // parse the primary record file to find all transactions
        System.out.println("addressBookTransactions.size() = " + addressBookTransactions.size());

        Path resourcesDir = Path.of("tools-and-tests/tools/src/main/resources");
        try (WritableStreamingData out = new WritableStreamingData(Files.newOutputStream(
                resourcesDir.resolve("2021-06-08T17_35_26.000831000Z-file-102-update-transaction-body.bin")))) {
            out.writeInt(addressBookTransactions.size());
            for (TransactionBody tb : addressBookTransactions) {
                Bytes tbBytes = TransactionBody.PROTOBUF.toBytes(tb);
                out.writeInt((int) tbBytes.length());
                tbBytes.writeTo(out);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
