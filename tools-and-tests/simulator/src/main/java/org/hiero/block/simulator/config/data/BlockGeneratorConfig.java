// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.simulator.config.data;

import com.swirlds.config.api.ConfigData;
import com.swirlds.config.api.ConfigProperty;
import com.swirlds.config.api.validation.annotation.Min;
import java.nio.file.Files;
import java.nio.file.Path;
import org.hiero.block.common.utils.StringUtilities;
import org.hiero.block.simulator.config.logging.Loggable;
import org.hiero.block.simulator.config.types.GenerationMode;

/**
 * Defines the configuration for the BlockStreamManager (Generator) of blocks in the Hedera Block Simulator.
 *
 * @param generationMode the mode of block generation (e.g., directory-based)
 * @param minEventsPerBlock the minimum number of events per block
 * @param maxEventsPerBlock the maximum number of events per block
 * @param minTransactionsPerEvent the minimum number of transactions per event
 * @param maxTransactionsPerEvent the maximum number of transactions per event
 * @param folderRootPath the root path of the folder containing block files
 * @param managerImplementation the implementation class name of the block stream manager
 * @param paddedLength the length to which block identifiers are padded
 * @param fileExtension the file extension used for block files
 * @param startBlockNumber the optional start block number for the BlockAsFileLargeDataSets manager
 * @param endBlockNumber the optional end block number for the BlockAsFileLargeDataSets manager
 * @param invalidBlockHash if set to true, will send invalid block root hash
 */
@ConfigData("generator")
public record BlockGeneratorConfig(
        @Loggable @ConfigProperty(defaultValue = "CRAFT") GenerationMode generationMode,
        @Loggable @ConfigProperty(defaultValue = "1") @Min(1) int minEventsPerBlock,
        @Loggable @ConfigProperty(defaultValue = "10") int maxEventsPerBlock,
        @Loggable @ConfigProperty(defaultValue = "1") @Min(1) int minTransactionsPerEvent,
        @Loggable @ConfigProperty(defaultValue = "10") int maxTransactionsPerEvent,
        @Loggable @ConfigProperty(defaultValue = "") String folderRootPath,
        @Loggable @ConfigProperty(defaultValue = "BlockAsFileBlockStreamManager") String managerImplementation,
        @Loggable @ConfigProperty(defaultValue = "36") int paddedLength,
        @Loggable @ConfigProperty(defaultValue = ".blk.gz") String fileExtension,
        // Optional block number range for the BlockAsFileLargeDataSets manager
        @Loggable @ConfigProperty(defaultValue = "0") @Min(0) int startBlockNumber,
        @Loggable @ConfigProperty(defaultValue = "-1") int endBlockNumber,
        @Loggable @ConfigProperty(defaultValue = "false") boolean invalidBlockHash,
        @Loggable @ConfigProperty(defaultValue = "0") int shardNum,
        @Loggable @ConfigProperty(defaultValue = "0") int realmNum) {

    /**
     * Constructs a new {@code BlockGeneratorConfig} instance with validation.
     *
     * @throws IllegalArgumentException if the folder root path is not absolute or the folder does not exist
     */
    public BlockGeneratorConfig {
        // Default generation mode is CRAFT.
        // If set to DIR, block files must be located in a directory on the file system.
        // Set location at app.properties
        if (generationMode == GenerationMode.DIR) {
            // Verify folderRootPath property
            // Check if not empty
            if (StringUtilities.isBlank(folderRootPath)) {
                throw new IllegalArgumentException("Root path is not provided");
            }
            Path path = Path.of(folderRootPath);
            // Check if absolute
            if (!path.isAbsolute()) {
                throw new IllegalArgumentException(folderRootPath + " Root path must be absolute");
            }
            // Check if the folder exists
            if (Files.notExists(path)) {
                throw new IllegalArgumentException("Folder does not exist: " + path);
            }

            folderRootPath = path.toString();
        }

        if (endBlockNumber > -1 && endBlockNumber < startBlockNumber) {
            throw new IllegalArgumentException("endBlockNumber must be greater than or equal to startBlockNumber");
        }
    }

    /**
     * Creates a new {@link Builder} for constructing a {@code BlockGeneratorConfig}.
     *
     * @return a new {@code Builder} instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for creating instances of {@link BlockGeneratorConfig}.
     */
    public static class Builder {
        private GenerationMode generationMode = GenerationMode.DIR;
        private int minEventsPerBlock = 1;
        private int maxEventsPerBlock = 10;
        private int minTransactionsPerEvent = 1;
        private int maxTransactionsPerEvent = 10;
        private String folderRootPath = "";
        private String managerImplementation = "BlockAsFileBlockStreamManager";
        private int paddedLength = 36;
        private String fileExtension = ".blk.gz";
        private int startBlockNumber;
        private int endBlockNumber;
        private boolean invalidBlockHash = false;
        private int shardNum = 0;
        private int realmNum = 0;

        /**
         * Creates a new instance of the {@code Builder} class with default configuration values.
         */
        public Builder() {
            // Default constructor
        }

        /**
         * Sets the generation mode for block generation.
         *
         * @param generationMode the {@link GenerationMode} to use
         * @return this {@code Builder} instance
         */
        public Builder generationMode(GenerationMode generationMode) {
            this.generationMode = generationMode;
            return this;
        }

        /**
         * Sets the minimum number of events per block.
         *
         * @param minEventsPerBlock the minimum number of events per block
         * @return this {@code Builder} instance
         */
        public Builder minEventsPerBlock(int minEventsPerBlock) {
            this.minEventsPerBlock = minEventsPerBlock;
            return this;
        }

        /**
         * Sets the maximum number of events per block.
         *
         * @param maxEventsPerBlock the maximum number of events per block
         * @return this {@code Builder} instance
         */
        public Builder maxEventsPerBlock(int maxEventsPerBlock) {
            this.maxEventsPerBlock = maxEventsPerBlock;
            return this;
        }

        /**
         * Sets the minimum number of transactions per event.
         *
         * @param minTransactionsPerEvent the minimum number of transactions per event
         * @return this {@code Builder} instance
         */
        public Builder minTransactionsPerEvent(int minTransactionsPerEvent) {
            this.minTransactionsPerEvent = minTransactionsPerEvent;
            return this;
        }

        /**
         * Sets the maximum number of transactions per event.
         *
         * @param maxTransactionsPerEvent the maximum number of transactions per event
         * @return this {@code Builder} instance
         */
        public Builder maxTransactionsPerEvent(int maxTransactionsPerEvent) {
            this.maxTransactionsPerEvent = maxTransactionsPerEvent;
            return this;
        }

        /**
         * Sets the root path of the folder containing block files.
         *
         * @param folderRootPath the absolute path to the folder
         * @return this {@code Builder} instance
         */
        public Builder folderRootPath(String folderRootPath) {
            this.folderRootPath = folderRootPath;
            return this;
        }

        /**
         * Sets the implementation class name of the block stream manager.
         *
         * @param managerImplementation the class name of the manager implementation
         * @return this {@code Builder} instance
         */
        public Builder managerImplementation(String managerImplementation) {
            this.managerImplementation = managerImplementation;
            return this;
        }

        /**
         * Sets the length to which block identifiers are padded.
         *
         * @param paddedLength the padded length
         * @return this {@code Builder} instance
         */
        public Builder paddedLength(int paddedLength) {
            this.paddedLength = paddedLength;
            return this;
        }

        /**
         * Sets the file extension used for block files.
         *
         * @param fileExtension the file extension (e.g., ".blk.gz")
         * @return this {@code Builder} instance
         */
        public Builder fileExtension(String fileExtension) {
            this.fileExtension = fileExtension;
            return this;
        }

        /**
         * Sets the start block number for the BlockAsFileLargeDataSets manager to
         * begin reading blocks.
         *
         * @param startBlockNumber the start block number
         * @return this {@code Builder} instance
         */
        public Builder startBlockNumber(int startBlockNumber) {
            this.startBlockNumber = startBlockNumber;
            return this;
        }

        /**
         * Sets the end block number for the BlockAsFileLargeDataSets manager to
         * stop reading blocks.
         *
         * @param endBlockNumber the end block number
         * @return this {@code Builder} instance
         */
        public Builder endBlockNumber(int endBlockNumber) {
            this.endBlockNumber = endBlockNumber;
            return this;
        }

        /**
         * Sets whether to send an invalid block root hash.
         *
         * @param invalidBlockHash true to send an invalid block root hash, false otherwise
         * @return this {@code Builder} instance
         */
        public Builder invalidBlockHash(boolean invalidBlockHash) {
            this.invalidBlockHash = invalidBlockHash;
            return this;
        }

        public Builder shardNum(int shardNum) {
            this.shardNum = shardNum;
            return this;
        }

        public Builder realmNum(int realmNum) {
            this.realmNum = realmNum;
            return this;
        }

        /**
         * Builds a new {@link BlockGeneratorConfig} instance with the configured values.
         *
         * @return a new {@code BlockGeneratorConfig}
         */
        public BlockGeneratorConfig build() {
            return new BlockGeneratorConfig(
                    generationMode,
                    minEventsPerBlock,
                    maxEventsPerBlock,
                    minTransactionsPerEvent,
                    maxTransactionsPerEvent,
                    folderRootPath,
                    managerImplementation,
                    paddedLength,
                    fileExtension,
                    startBlockNumber,
                    endBlockNumber,
                    invalidBlockHash,
                    shardNum,
                    realmNum);
        }
    }
}
