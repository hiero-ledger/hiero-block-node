// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.hiero.block.tools.states.utils.Utils;

public class FCMap<K, V> extends HashMap<K, V> {
    /** Expected length of root hash byte array */
    public static final int HASH_LENGTH = 48;
    /** Delimiter to identify to the beginning of a serialized tree */
    private static final int BEGIN_MARKER_VALUE = 1_835_364_971; // 'm', 'e', 'r', 'k'
    /** Delimiter to identify the end of a serialized tree */
    private static final int END_MARKER_VALUE = 1_818_588_274; // 'l', 'e', 't', 'r'
    /** Begin Marker as array of bytes so it can be written directly to the stream */
    private static final byte[] BEGIN_MARKER =
            ByteBuffer.allocate(Integer.BYTES).putInt(BEGIN_MARKER_VALUE).array();
    /** End Marker as array of bytes, so it can be written directly to the stream */
    private static final byte[] END_MARKER =
            ByteBuffer.allocate(Integer.BYTES).putInt(END_MARKER_VALUE).array();
    /** Start delimiter for leaf keys */
    private static final int KEY_S = 1_801_812_339; // 'k', 'e', 'y', 's'
    /** End delimiter for leaf keys */
    private static final int KEY_E = 1_801_812_325; // 'k', 'e', 'y', 's'
    /** Start delimiter for leaf values */
    private static final int VALUE_S = 1_986_096_243; // 'v', 'a', 'l', 's'
    /** End delimiter for leaf values */
    private static final int VALUE_E = 1_986_096_229; // 'v', 'a', 'l', 's'

    private byte[] rootHash;
    private FCMInternalNode<K, V> root;
    private final ParseFunction<K> keyDeserializer;
    private final ParseFunction<V> valueDeserializer;

    public FCMap(ParseFunction<K> keyDeserializer, ParseFunction<V> valueDeserializer) {
        super();
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    public void copyFrom(DataInputStream inStream) throws IOException {
        final int beginMarker = inStream.readInt();
        if (BEGIN_MARKER_VALUE != beginMarker) {
            throw new IOException("The stream is not at the beginning of a serialized FCMap");
        }

        rootHash = deserializeRootHash(inStream); // discards root hash
        final int endMarker = inStream.readInt();
        if (END_MARKER_VALUE != endMarker) {
            throw new IOException("The serialized FCMap stream ends unexpectedly");
        }
    }

    public void copyFromExtra(DataInputStream inStream) throws IOException {

        final int beginMarker = inStream.readInt();
        if (BEGIN_MARKER_VALUE != beginMarker) {
            throw new IOException("The stream is not at the beginning of a serialized FCMap");
        }

        // Discard the version number
        long version = inStream.readLong();
        if (version != 1) {
            throw new IOException("Unsupported FCMap version: " + version);
        }

        final byte[] rootHash = deserializeRootHash(inStream);
        final List<FCMLeaf<K, V>> leaves = copyTreeFrom(inStream);
        //        final FCMNode<K, V> rootNode;
        //        { // inlined copyTreeFrom() as multiple returns
        ////            copyTreeFrom(inStream, keyDeserializer, valueDeserializer);
        //            // Discard the version number
        //            long treeVersion = inStream.readLong();
        //            leaves = deserializeLeaves(inStream, keyDeserializer, valueDeserializer);
        //            int size = leaves.size();
        //            if (size == 0) {
        //                rootNode = new FCMInternalNode<>(null, null);
        //            } else {
        //                List<FCMNode<K,V>> internalNodes = generateInitialInternalNodes(leaves);
        //                while (internalNodes.size() > 1) {
        //                    internalNodes = generateInternalLevel(internalNodes);
        //                }
        //
        //                rootNode = internalNodes.get(0);
        ////                this.setRightMostLeaf(); // TODO not sure if this is needed
        //            }
        //        }
        final int endMarker = inStream.readInt();
        if (END_MARKER_VALUE != endMarker) {
            throw new IOException("The serialized FCMap stream ends unexpectedly");
        }

        // old code to recompute the root hash
        //        final MerkleHash<K, V> merkleHashWorker = new MerkleHash<>();
        //        merkleHashWorker.computeByLevels(this.tree);
        //        final byte[] treeRootHash = this.tree.getHash();
        //        if (this.copyCheckEnabled && !Arrays.equals(rootHash, treeRootHash)) {
        //            throw new IllegalStateException("The root hash serialized doesn't match the tree root hash");
        //        }

        // old code to create the internal map
        //        this.internalMap = new HashMap<>(leaves.size());
        //        for (FCMLeaf<K, V> leaf : leaves) {
        //            this.internalMap.put(leaf.getKey(), leaf);
        //        }
        for (FCMLeaf<K, V> leaf : leaves) {
            put(leaf.key(), leaf.value());
        }
    }

    public List<FCMLeaf<K, V>> copyTreeFrom(DataInputStream inputStream) throws IOException {
        try {
            // Discard the version number
            inputStream.readLong();
            final List<FCMLeaf<K, V>> leaves = deserializeLeaves(inputStream);
            int size = leaves.size();
            if (size == 0) {
                root = new FCMInternalNode<>(null, null);
                return leaves;
            }

            List<? extends FCMNode<K, V>> internalNodes = generateInitialInternalNodes(leaves);
            while (internalNodes.size() > 1) {
                internalNodes = generateInternalLevel(internalNodes);
            }

            this.root = (FCMInternalNode<K, V>) internalNodes.get(0);
            //            this.setRightMostLeaf(); TODO not sure if this is needed
            return leaves;
        } catch (IOException ioException) {
            throw ioException;
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private static <K, V> List<FCMNode<K, V>> generateInitialInternalNodes(final List<FCMLeaf<K, V>> leaves) {
        final long sizeofLastCompleteLevel = Utils.findLeftMostBit(leaves.size());
        final long nextSizeOfLastCompleteLevel = sizeofLastCompleteLevel << 1;
        final long numberOfLeavesInUpperLevel = nextSizeOfLastCompleteLevel - leaves.size();
        final long numberOfLeavesAtTheBottom = leaves.size() - numberOfLeavesInUpperLevel;
        if (numberOfLeavesAtTheBottom < 1) {
            return generateInternalLevel(leaves);
        }
        final List<? extends FCMNode<K, V>> bottomLeaves = leaves.subList(0, (int) numberOfLeavesAtTheBottom);
        final List<? extends FCMNode<K, V>> upperLeaves =
                leaves.subList((int) numberOfLeavesAtTheBottom, leaves.size());
        final List<FCMNode<K, V>> internalNodes = new ArrayList<>(generateInternalLevel(bottomLeaves));
        internalNodes.addAll(upperLeaves);
        return internalNodes;
    }

    /**
     * Generates the complete tree based on the provided nodes.
     *
     * @param nodes
     * 		Parent nodes
     * @return Parent nodes of the provided parent nodes
     */
    private static <K, V> List<FCMNode<K, V>> generateInternalLevel(List<? extends FCMNode<K, V>> nodes) {
        List<FCMNode<K, V>> internalNodes = new ArrayList<>(nodes.size() / 2);
        if (nodes.size() == 1) {
            final FCMNode<K, V> rightChild = nodes.get(0);
            FCMInternalNode<K, V> internalNode = new FCMInternalNode<>(rightChild, null);
            //            internalNode.setRightChild(rightChild);
            //            rightChild.setParent(internalNode);
            internalNodes.add(internalNode);
            return internalNodes;
        }

        final int limit = nodes.size();
        int index = 0;
        while (index < limit) {
            FCMNode<K, V> leftChild = nodes.get(index++);
            FCMNode<K, V> rightChild = nodes.get(index++);
            FCMInternalNode<K, V> internalNode = new FCMInternalNode<>(leftChild, rightChild);

            //            internalNode.setLeftChild(leftChild);
            //            internalNode.setRightChild(rightChild);
            //
            //            leftChild.setParent(internalNode);
            //            rightChild.setParent(internalNode);

            internalNodes.add(internalNode);
        }

        return internalNodes;
    }

    private static byte[] deserializeRootHash(final DataInputStream inStream) throws IOException {
        final byte[] rootHash = new byte[HASH_LENGTH];
        inStream.readFully(rootHash, 0, HASH_LENGTH);
        return rootHash;
    }

    // com.swirlds.map.internal.FCSerializer#deserializeBody
    private List<FCMLeaf<K, V>> deserializeLeaves(final DataInputStream inputStream) throws IOException {
        final long numberOfLeaves = inputStream.readLong();
        final List<FCMLeaf<K, V>> leaves = new ArrayList<>((int) numberOfLeaves);
        System.out.println("Deserializing " + numberOfLeaves + " leaves");
        for (int leafIndex = 0; leafIndex < numberOfLeaves; leafIndex++) {
            final int keyS = inputStream.readInt();
            if (keyS != KEY_S) {
                throw new IOException(
                        String.format("Key %d/%d does not start at the correct position", leafIndex, numberOfLeaves));
            }
            final K key = keyDeserializer.copyFrom(inputStream);
            final int keyE = inputStream.readInt();
            if (keyE != KEY_E) {
                throw new IOException(String.format(
                        "Key %d/%d didn't consume the input stream correctly", leafIndex, numberOfLeaves));
            }

            final int valueS = inputStream.readInt();
            if (valueS != VALUE_S) {
                throw new IOException(
                        String.format("Value %d/%d does not start at the correct position", leafIndex, numberOfLeaves));
            }

            final V value = valueDeserializer.copyFrom(inputStream);
            final int valueE = inputStream.readInt();
            if (valueE != VALUE_E) {
                throw new IOException(String.format(
                        "Value %d/%d didn't consume the input stream correctly", leafIndex, numberOfLeaves));
            }

            leaves.add(new FCMLeaf<>(key, value));
        }

        return leaves;
    }

    public void copyTo(DataOutputStream outStream) throws IOException {
        outStream.write(BEGIN_MARKER);
        outStream.write(rootHash);
        outStream.write(END_MARKER);
    }

    private byte[] computeByLevels() throws IOException {
        byte[] computedHash;
        // TODO assume that the tree is not empty
        //        if (root == null) {
        //            computedHash = EMPTY_HASH;
        //        } else if (tree.size() == 1) {
        //            this.computeTreeWithOneLeaf(tree);
        //            return;
        //        }

        //        final Stack<Collection<FCMNode<K, V>>> levels = tree.retrieveLevels(forceHash);
        //
        //        while (!levels.isEmpty()) {
        //            this.computeHashForLevel(levels.pop().iterator(), forceHash);
        //        }
        //        byte[] readRootHash = root.getHash();
        return root.getHash();
    }
}
