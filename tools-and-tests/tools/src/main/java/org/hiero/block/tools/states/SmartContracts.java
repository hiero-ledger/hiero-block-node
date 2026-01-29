package org.hiero.block.tools.states;

import java.util.ArrayList;
import java.util.List;

public class SmartContracts {
    private static int DWORD_BYTES = 32;

    public static List<DataWordPair> deserializeKeyValuePairs(byte[] serializedMap) {
        List<DataWordPair> cacheToPut = new ArrayList<>();
        int offset = 0;
        while (offset < serializedMap.length) {
            byte[] keyBytes = new byte[DWORD_BYTES];
            byte[] valBytes = new byte[DWORD_BYTES];
            System.arraycopy(serializedMap, offset, keyBytes, 0, DWORD_BYTES);
            offset += DWORD_BYTES;
            System.arraycopy(serializedMap, offset, valBytes, 0, DWORD_BYTES);
            offset += DWORD_BYTES;
            cacheToPut.add(new DataWordPair(new DataWord(keyBytes), new DataWord(valBytes)));
        }
        return cacheToPut;
    }

    public record DataWordPair(DataWord key, DataWord value) {
        public DataWordPair(DataWord key, DataWord value) {
            this.key = key;
            this.value = value;
        }
    }

    public record DataWord(byte[] data) {
        public static final int DATA_SIZE = 32;

        public DataWord(byte[] data) {
            this.data = data;
            if (data.length != DATA_SIZE) {
                throw new IllegalArgumentException("DataWord must be exactly " + DATA_SIZE + " bytes long.");
            }
        }
    }
}
