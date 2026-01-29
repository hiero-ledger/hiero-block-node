package org.hiero.block.tools.states;


import static org.hiero.block.tools.states.blockstream.BlockStreamWriter.writeBlockStream;

import org.hiero.block.tools.states.io.FCDataInputStream;
import org.hiero.block.tools.states.model.SignedState;
import org.hiero.block.tools.states.postgres.BinaryObjectCsvRow;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

public class Main {

    public static SignedState loadSignedState(Path stateFile) throws IOException {
        SignedState signedState = new SignedState();
        try (FCDataInputStream fin = new FCDataInputStream(new BufferedInputStream(Files.newInputStream(stateFile), 1024 * 1024))) {
            signedState.copyFrom(fin);
            signedState.copyFromExtra(fin);
        }
        return signedState;
    }

    public static void main(String[] args) throws Exception {
        final long round = 33485415;//33312259;
        // Load the first signed state at round 33485415 from the mainnet data directory
        final Path stateFile = Path.of("mainnet-data/"+round+"/SignedState.swh");
        final SignedState signedState = loadSignedState(stateFile);
        // load binary object exported from Postgres database as CSV file
        final Path csvPath = Path.of("mainnet-data/"+round+"/binary_objects.csv");
        final List<BinaryObjectCsvRow> binaryObjectRows = BinaryObjectCsvRow.loadBinaryObjects(csvPath);
        // binary objects by hash map
        Map<String, BinaryObjectCsvRow> binaryObjectByHexHashMap = new HashMap<>();
        for (BinaryObjectCsvRow row : binaryObjectRows) {
            binaryObjectByHexHashMap.put(row.hexHash(), row);
        }
        // convert to block stream
        writeBlockStream("build/blockstream-round-"+round, signedState, binaryObjectByHexHashMap);

        // compute signed state hash
//        byte[] signedStateHash = signedState.generateSignedStateHash();
//        System.out.println("Signed state hash: " + HexFormat.of().formatHex(signedStateHash));
//        byte[] readHash = signedState.readHash();
//        System.out.println("Read hash: " + HexFormat.of().formatHex(readHash));
//        if (!java.util.Arrays.equals(signedStateHash, readHash)) {
//            System.err.println("Signed state hash does not match read hash!");
//        } else {
//            System.out.println("Signed state hash matches read hash.");
//        }
    }

    public static void printBytes(FCDataInputStream fin, int numOf128s) throws IOException {
        byte[] bytes = new byte[128];
        for (int i = 0; i < numOf128s; i++) {
            fin.readFully(bytes);
            System.out.println("bytes    : "+ HexFormat.of().formatHex(bytes));
//            System.out.println("    ascii: "+ new String(bytes, StandardCharsets.UTF_8));
        }
    }

}
