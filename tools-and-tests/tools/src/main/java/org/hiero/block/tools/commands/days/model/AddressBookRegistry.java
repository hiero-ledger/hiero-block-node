package org.hiero.block.tools.commands.days.model;

import com.hedera.hapi.node.base.NodeAddressBook;
import com.hedera.pbj.runtime.ParseException;
import com.hedera.pbj.runtime.io.buffer.Bytes;
import com.hedera.pbj.runtime.io.stream.ReadableStreamingData;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AddressBookRegistry {
    private final List<NodeAddressBook> addressBooks = new ArrayList<>();

    public AddressBookRegistry() {
        try {
            addressBooks.add(loadGenesisAddressBook());
        } catch (ParseException e) {
            throw new RuntimeException("Error loading Genesis Address Book", e);
        }
    }

    public static NodeAddressBook loadGenesisAddressBook() throws ParseException {
        try (var in = new ReadableStreamingData(Objects.requireNonNull(AddressBookRegistry.class.getClassLoader()
            .getResourceAsStream("mainnet-genesis-address-book.proto.bin")))) {
            return NodeAddressBook.PROTOBUF.parse(in);
        }
    }

    public static NodeAddressBook readAddressBook(byte[] bytes) throws ParseException {
        return NodeAddressBook.PROTOBUF.parse(Bytes.wrap(bytes));
    }
}
