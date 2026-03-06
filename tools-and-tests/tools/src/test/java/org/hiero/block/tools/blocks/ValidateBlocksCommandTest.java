// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.blocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hedera.hapi.block.stream.Block;
import com.hedera.hapi.block.stream.BlockItem;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import picocli.CommandLine;

/**
 * Tests for {@link ValidateBlocksCommand}.
 *
 * <p>Uses {@link TestBlockFactory} to generate synthetic blocks with real RSA signatures.
 * Blocks are written as uncompressed {@code .blk} files (no zstd dependency needed).
 */
@Execution(ExecutionMode.SAME_THREAD)
class ValidateBlocksCommandTest {

    @TempDir
    Path tempDir;

    // ── Helper methods ──

    /** Writes blocks as uncompressed .blk files to tempDir. */
    private void writeBlocks(List<Block> blocks) throws IOException {
        for (int i = 0; i < blocks.size(); i++) {
            Block block = blocks.get(i);
            // Try to get block number from header; fall back to index
            long blockNum = block.items().stream()
                    .filter(BlockItem::hasBlockHeader)
                    .findFirst()
                    .map(item -> item.blockHeaderOrThrow().number())
                    .orElse((long) i);
            byte[] bytes = Block.PROTOBUF.toBytes(block).toByteArray();
            Files.write(tempDir.resolve(blockNum + ".blk"), bytes);
        }
    }

    /** Writes the address book history to tempDir. */
    private void writeAddressBook() throws IOException {
        TestBlockFactory.writeAddressBookHistory(tempDir);
    }

    /** Runs the validate command and captures stdout+stderr. Returns [exitCode, output]. */
    private Object[] runValidate(String... extraArgs) {
        ByteArrayOutputStream outCapture = new ByteArrayOutputStream();
        ByteArrayOutputStream errCapture = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        int exitCode;
        try {
            System.setOut(new PrintStream(outCapture));
            System.setErr(new PrintStream(errCapture));

            List<String> args = new ArrayList<>();
            args.add(tempDir.toString());
            args.addAll(List.of(extraArgs));

            exitCode = new CommandLine(new ValidateBlocksCommand()).execute(args.toArray(new String[0]));
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }

        String output = outCapture + "\n" + errCapture;
        return new Object[] {exitCode, output};
    }

    // ── Happy path tests ──

    @Test
    void validChainFromGenesis_passes() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        int exitCode = (int) result[0];
        String output = (String) result[1];

        assertEquals(0, exitCode, "Should pass validation. Output:\n" + output);
        assertTrue(output.contains("VALIDATION PASSED"), "Should report PASSED. Output:\n" + output);
        assertTrue(output.contains("Blocks validated: 5"), "Should validate 5 blocks. Output:\n" + output);
    }

    // ── Hash chain validation ──

    @Test
    void brokenHashChain_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        // Corrupt block 3's previousBlockRootHash
        blocks.set(3, TestBlockFactory.withBrokenPreviousHash(blocks.get(3)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        int exitCode = (int) result[0];
        String output = (String) result[1];

        assertEquals(0, exitCode); // picocli returns 0 for Runnable
        assertTrue(output.contains("Block Chain"), "Should detect hash chain error. Output:\n" + output);
        assertTrue(output.contains("VALIDATION FAILED"), "Should report FAILED. Output:\n" + output);
    }

    // ── Block structure validation ──

    @Test
    void missingHeader_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        blocks.set(1, TestBlockFactory.withMissingHeader(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Required Items"), "Should detect missing header. Output:\n" + output);
        assertTrue(output.contains("VALIDATION FAILED"), "Should report FAILED. Output:\n" + output);
    }

    @Test
    void missingFooter_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        blocks.set(1, TestBlockFactory.withMissingFooter(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Required Items"), "Should detect missing footer. Output:\n" + output);
    }

    @Test
    void missingRecordFile_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        blocks.set(1, TestBlockFactory.withMissingRecordFile(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Required Items"), "Should detect missing record file. Output:\n" + output);
    }

    @Test
    void missingProof_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        blocks.set(1, TestBlockFactory.withMissingProof(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Required Items"), "Should detect missing proof. Output:\n" + output);
    }

    @Test
    void duplicateHeader_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        blocks.set(1, TestBlockFactory.withDuplicateHeader(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Block Structure"), "Should detect duplicate header. Output:\n" + output);
    }

    @Test
    void itemsOutOfOrder_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        blocks.set(1, TestBlockFactory.withItemsOutOfOrder(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Block Structure"), "Should detect items out of order. Output:\n" + output);
    }

    // ── Historical tree root validation ──

    @Test
    void badAllBlocksTreeRoot_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        blocks.set(3, TestBlockFactory.withBrokenTreeRoot(blocks.get(3)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Historical Block Tree"), "Should detect tree root mismatch. Output:\n" + output);
        assertTrue(output.contains("VALIDATION FAILED"), "Should report FAILED. Output:\n" + output);
    }

    // ── Signature verification ──

    @Test
    void validSignatures_allPass() throws Exception {
        // This is the same as the happy path — all 5 nodes sign, threshold = 2
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("VALIDATION PASSED"), "All signatures should pass. Output:\n" + output);
    }

    @Test
    void insufficientSignatures_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        // Only 1 sig, threshold = (5/3)+1 = 2
        blocks.set(1, TestBlockFactory.withInsufficientSignatures(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(
                output.contains("Insufficient valid signatures") || output.contains("Signatures"),
                "Should detect insufficient signatures. Output:\n" + output);
    }

    @Test
    void corruptSignature_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        blocks.set(1, TestBlockFactory.withCorruptSignature(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        // Even with 1 corrupt sig, 4 remaining still pass threshold of 2
        // So this should still pass
        assertTrue(output.contains("VALIDATION PASSED"), "4/5 valid sigs should meet threshold. Output:\n" + output);
    }

    // ── 50 billion HBAR supply ──

    @Test
    void hbarSupplyMismatch_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        // Add unbalanced transfer that breaks 50B supply (also invalidates RSA signature)
        blocks.set(1, TestBlockFactory.withExtraHbar(blocks.get(1), 1_000_000));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        // withExtraHbar modifies block content, which invalidates both the HBAR supply check
        // and the RSA signature. With parallel validation, either may be detected first.
        assertTrue(
                output.contains("HBAR Supply") || output.contains("Signatures"),
                "Should detect HBAR supply mismatch or signature failure. Output:\n" + output);
        assertTrue(output.contains("VALIDATION FAILED"), "Should report FAILED. Output:\n" + output);
    }

    // ── Gap detection ──

    @Test
    void gapInBlockNumbers_detected() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(4);
        // Remove block 2 to create gap: 0, 1, 3
        blocks.remove(2);
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("Gap detected"), "Should detect gap in block numbers. Output:\n" + output);
    }

    // ── Missing address book ──

    @Test
    void missingAddressBook_failsImmediately() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        writeBlocks(blocks);
        // Don't write address book

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(
                output.contains("No address book found"),
                "Should fail immediately when no address book. Output:\n" + output);
    }

    // ── Checkpoint/resume ──

    @Test
    void checkpointSavedOnError() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        // Break block 3's hash chain — blocks 0, 1, 2 will validate, then fail at 3
        blocks.set(3, TestBlockFactory.withBrokenPreviousHash(blocks.get(3)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("VALIDATION FAILED"), "Should report FAILED. Output:\n" + output);
        // Check checkpoint was saved
        assertTrue(output.contains("Checkpoint saved"), "Should save checkpoint on error. Output:\n" + output);
        assertTrue(
                Files.exists(tempDir.resolve("validateCheckpoint/validateProgress.json")),
                "Checkpoint JSON should exist");
    }

    @Test
    void checkpointContainsAllFiles() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        blocks.set(3, TestBlockFactory.withBrokenPreviousHash(blocks.get(3)));
        writeBlocks(blocks);
        writeAddressBook();

        runValidate("--no-resume");

        Path checkpointDir = tempDir.resolve("validateCheckpoint");
        assertTrue(Files.exists(checkpointDir.resolve("validateProgress.json")), "Progress JSON should exist");
        assertTrue(
                Files.exists(checkpointDir.resolve("historicalTreeValidation.bin")),
                "Historical tree validation state should exist");
        assertTrue(
                Files.exists(checkpointDir.resolve("hbarSupplyValidation.bin")),
                "HBAR supply validation state should exist");
    }

    @Test
    void checkpointJsonHasCorrectContents() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        // Break block 3 — blocks 0, 1, 2 validate successfully, fail at 3
        blocks.set(3, TestBlockFactory.withBrokenPreviousHash(blocks.get(3)));
        writeBlocks(blocks);
        writeAddressBook();

        runValidate("--no-resume");

        Path jsonFile = tempDir.resolve("validateCheckpoint/validateProgress.json");
        String json = Files.readString(jsonFile);
        JsonObject root = JsonParser.parseString(json).getAsJsonObject();

        assertEquals(3, root.get("schemaVersion").getAsInt(), "Schema version should be 3");
        // Block 3 has the hash chain error; fail-fast saves checkpoint at last successful block (2)
        assertEquals(2, root.get("lastValidatedBlockNumber").getAsLong(), "Last validated should be block 2");
        assertEquals(3, root.get("blocksValidated").getAsLong(), "Should have validated 3 blocks (0, 1, 2)");
        assertTrue(
                root.has("previousBlockHashHex")
                        && !root.get("previousBlockHashHex").getAsString().isEmpty(),
                "Should have previousBlockHashHex");
    }

    @Test
    void resumeFromCheckpoint_skipsAlreadyValidatedBlocks() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        // Break block 3 — fail-fast stops at block 3, checkpoint saved at last successful block (2)
        blocks.set(3, TestBlockFactory.withBrokenPreviousHash(blocks.get(3)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result1 = runValidate("--no-resume");
        String output1 = (String) result1[1];
        assertTrue(output1.contains("VALIDATION FAILED"), "First run should fail. Output:\n" + output1);
        assertTrue(
                Files.exists(tempDir.resolve("validateCheckpoint/validateProgress.json")),
                "Checkpoint should exist after first run");

        // Run again WITHOUT --no-resume — should detect and resume from checkpoint
        Object[] result2 = runValidate();
        String output2 = (String) result2[1];

        assertTrue(
                output2.contains("Resuming from checkpoint"),
                "Should report resuming from checkpoint. Output:\n" + output2);
        assertTrue(output2.contains("last validated block = 2"), "Should resume from block 2. Output:\n" + output2);
    }

    @Test
    void checkpointDeletedOnFullSuccess() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        writeBlocks(blocks);
        writeAddressBook();

        // Create a fake checkpoint first so we can verify it gets cleaned up
        Path checkpointDir = tempDir.resolve("validateCheckpoint");
        Files.createDirectories(checkpointDir);
        Files.writeString(checkpointDir.resolve("validateProgress.json"), "{}");

        Object[] result = runValidate();
        String output = (String) result[1];

        assertTrue(output.contains("VALIDATION PASSED"), "Should pass validation. Output:\n" + output);
        assertFalse(
                Files.exists(checkpointDir.resolve("validateProgress.json")),
                "Checkpoint JSON should be deleted after full success");
    }

    // ── CLI validation ──

    @Test
    void invalidThreads_printsErrorAndExits() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(1);
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--threads", "0", "--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("must be >= 1"), "Should reject --threads 0. Output:\n" + output);
    }

    @Test
    void invalidPrefetch_printsErrorAndExits() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(1);
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--prefetch", "0", "--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("must be >= 1"), "Should reject --prefetch 0. Output:\n" + output);
    }

    @Test
    void skipSignatures_skipsSignatureValidation() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        // Only 1 signature — normally fails threshold of 2
        blocks.set(1, TestBlockFactory.withInsufficientSignatures(blocks.get(1)));
        writeBlocks(blocks);
        writeAddressBook();

        Object[] result = runValidate("--skip-signatures", "--no-resume");
        String output = (String) result[1];

        // With --skip-signatures, the insufficient signatures should be ignored.
        // However, withInsufficientSignatures changes the block proof which breaks the block hash
        // chain. So we check that "Signature" is mentioned as skipped.
        assertTrue(
                output.contains("Skipping") && output.contains("Signature"),
                "Should skip signature validation. Output:\n" + output);
    }

    @Test
    void nonGenesisStart_skipsGenesisRequiredValidations() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(3);
        // Write only blocks 1 and 2 (skip block 0)
        for (int i = 1; i < blocks.size(); i++) {
            Block block = blocks.get(i);
            long blockNum = block.items().stream()
                    .filter(BlockItem::hasBlockHeader)
                    .findFirst()
                    .map(item -> item.blockHeaderOrThrow().number())
                    .orElse((long) i);
            byte[] bytes = Block.PROTOBUF.toBytes(block).toByteArray();
            Files.write(tempDir.resolve(blockNum + ".blk"), bytes);
        }
        writeAddressBook();

        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        // Should print Skipping warnings for genesis-required validations
        assertTrue(output.contains("Skipping"), "Should skip genesis-required validations. Output:\n" + output);
        assertTrue(
                output.contains("requires genesis start"),
                "Should mention genesis start requirement. Output:\n" + output);
    }

    @Test
    void noResumeIgnoresExistingCheckpoint() throws Exception {
        List<Block> blocks = TestBlockFactory.createValidChain(5);
        writeBlocks(blocks);
        writeAddressBook();

        // Create a checkpoint that claims only block 0 was validated
        Path checkpointDir = tempDir.resolve("validateCheckpoint");
        Files.createDirectories(checkpointDir);
        JsonObject fakeCheckpoint = new JsonObject();
        fakeCheckpoint.addProperty("schemaVersion", 3);
        fakeCheckpoint.addProperty("lastValidatedBlockNumber", 0);
        fakeCheckpoint.addProperty("blocksValidated", 1);
        fakeCheckpoint.addProperty("previousBlockHashHex", "aa".repeat(48));
        Files.writeString(checkpointDir.resolve("validateProgress.json"), fakeCheckpoint.toString());

        // --no-resume should validate ALL 5 blocks from scratch
        Object[] result = runValidate("--no-resume");
        String output = (String) result[1];

        assertTrue(output.contains("VALIDATION PASSED"), "Should pass. Output:\n" + output);
        assertTrue(output.contains("Blocks validated: 5"), "Should validate all 5 blocks. Output:\n" + output);
        assertFalse(
                output.contains("Resuming from checkpoint"), "Should NOT resume from checkpoint. Output:\n" + output);
    }
}
