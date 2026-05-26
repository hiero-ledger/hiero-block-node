// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.pbj.runtime.io.buffer.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Read / write the contents of {@link LiveState} to a single file. The encoding
 * is a simple length-prefixed binary stream — see the static schema header
 * below — chosen to keep the plugin free of any tar / VirtualMap dependencies
 * while still being deterministic, compact, and easy to verify.
 *
 * <p>Writes go through a {@code .tmp} sibling and atomic move so a crash mid-write
 * leaves the previous snapshot intact.
 */
final class LiveStateSnapshotIO {

    /** Magic 4 bytes so we can detect a corrupted or wrong-format file fast. */
    static final int MAGIC = 0x4C535453; // "LSTS"
    /** Bumped when the encoding changes incompatibly. */
    static final int VERSION = 1;

    private LiveStateSnapshotIO() {}

    static void write(@NonNull final Path target, @NonNull final LiveState state) throws IOException {
        Files.createDirectories(target.getParent());
        final Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(tmp))) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);

            final Map<Integer, Bytes> singletons = state.singletonsSnapshot();
            out.writeInt(singletons.size());
            for (final Map.Entry<Integer, Bytes> e : singletons.entrySet()) {
                out.writeInt(e.getKey());
                writeBytes(out, e.getValue());
            }

            final Map<Integer, Map<Bytes, Bytes>> kvs = state.kvsSnapshot();
            out.writeInt(kvs.size());
            for (final Map.Entry<Integer, Map<Bytes, Bytes>> e : kvs.entrySet()) {
                out.writeInt(e.getKey());
                out.writeInt(e.getValue().size());
                for (final Map.Entry<Bytes, Bytes> kv : e.getValue().entrySet()) {
                    writeBytes(out, kv.getKey());
                    writeBytes(out, kv.getValue());
                }
            }

            final Map<Integer, List<Bytes>> queues = state.queuesSnapshot();
            out.writeInt(queues.size());
            for (final Map.Entry<Integer, List<Bytes>> e : queues.entrySet()) {
                out.writeInt(e.getKey());
                out.writeInt(e.getValue().size());
                for (final Bytes b : e.getValue()) {
                    writeBytes(out, b);
                }
            }
        }
        try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final IOException atomicFailed) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    static void read(@NonNull final Path source, @NonNull final LiveState state) throws IOException {
        try (DataInputStream in = new DataInputStream(Files.newInputStream(source))) {
            final int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IOException("Unexpected magic in " + source + ": 0x" + Integer.toHexString(magic));
            }
            final int version = in.readInt();
            if (version != VERSION) {
                throw new IOException("Unsupported snapshot version " + version + " in " + source);
            }

            final Map<Integer, Bytes> singletons = new TreeMap<>();
            final int singletonCount = in.readInt();
            for (int i = 0; i < singletonCount; i++) {
                final int stateId = in.readInt();
                singletons.put(stateId, readBytes(in));
            }

            final Map<Integer, Map<Bytes, Bytes>> kvs = new TreeMap<>();
            final int kvStateCount = in.readInt();
            for (int i = 0; i < kvStateCount; i++) {
                final int stateId = in.readInt();
                final int entryCount = in.readInt();
                final Map<Bytes, Bytes> map = new HashMap<>(entryCount * 2);
                for (int j = 0; j < entryCount; j++) {
                    final Bytes key = readBytes(in);
                    final Bytes value = readBytes(in);
                    map.put(key, value);
                }
                kvs.put(stateId, map);
            }

            final Map<Integer, List<Bytes>> queues = new TreeMap<>();
            final int queueStateCount = in.readInt();
            for (int i = 0; i < queueStateCount; i++) {
                final int stateId = in.readInt();
                final int len = in.readInt();
                final List<Bytes> elements = new ArrayList<>(len);
                for (int j = 0; j < len; j++) {
                    elements.add(readBytes(in));
                }
                queues.put(stateId, elements);
            }

            state.restoreFrom(singletons, kvs, queues);
        }
    }

    private static void writeBytes(@NonNull final DataOutputStream out, @NonNull final Bytes b) throws IOException {
        final byte[] arr = b.toByteArray();
        out.writeInt(arr.length);
        out.write(arr);
    }

    @NonNull
    private static Bytes readBytes(@NonNull final DataInputStream in) throws IOException {
        final int len = in.readInt();
        if (len < 0) {
            throw new IOException("Negative byte length: " + len);
        }
        final byte[] arr = new byte[len];
        in.readFully(arr);
        return Bytes.wrap(arr);
    }
}
