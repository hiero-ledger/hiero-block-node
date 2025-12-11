// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.node.state.live;

import com.hedera.hapi.block.stream.output.MapChangeKey;
import com.hedera.hapi.block.stream.output.MapChangeValue;
import com.hedera.hapi.block.stream.output.NewStateChange;
import com.hedera.hapi.block.stream.output.NewStateType;
import com.hedera.hapi.block.stream.output.QueuePushChange;
import com.hedera.hapi.block.stream.output.SingletonUpdateChange;
import com.hedera.hapi.block.stream.output.StateChange;
import com.hedera.hapi.block.stream.output.StateIdentifier;
import com.hedera.hapi.node.base.TokenAssociation;
import com.hedera.hapi.node.state.common.EntityIDPair;
import com.hedera.hapi.node.state.common.EntityNumber;
import com.hedera.hapi.node.state.primitives.ProtoBytes;
import com.hedera.hapi.node.state.primitives.ProtoLong;
import com.hedera.hapi.node.state.primitives.ProtoString;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Utility class for parsing state changes from block stream items.
 * This class handles the conversion from block stream state change format
 * to the internal state representation.
 */
public final class StateChangeParser {

    private StateChangeParser() {
        // Utility class
    }

    /**
     * Get the state name for a given state ID.
     *
     * @param stateId the state ID
     * @return the state name in "ServiceName.STATE_KEY" format
     */
    @NonNull
    public static String stateNameOf(final int stateId) {
        final StateIdentifier identifier = StateIdentifier.fromProtobufOrdinal(stateId);
        return switch (identifier) {
            case UNKNOWN -> "Unknown." + stateId;
            case STATE_ID_NODES -> "AddressBookService.NODES";
            case STATE_ID_ACCOUNT_NODE_REL -> "AddressBookService.ACCOUNT_NODE_REL";
            case STATE_ID_BLOCKS -> "BlockRecordService.BLOCKS";
            case STATE_ID_RUNNING_HASHES -> "BlockRecordService.RUNNING_HASHES";
            case STATE_ID_BLOCK_STREAM_INFO -> "BlockStreamService.BLOCK_STREAM_INFO";
            case STATE_ID_CONGESTION_LEVEL_STARTS -> "CongestionThrottleService.CONGESTION_LEVEL_STARTS";
            case STATE_ID_THROTTLE_USAGE_SNAPSHOTS -> "CongestionThrottleService.THROTTLE_USAGE_SNAPSHOTS";
            case STATE_ID_TOPICS -> "ConsensusService.TOPICS";
            case STATE_ID_BYTECODE -> "ContractService.BYTECODE";
            case STATE_ID_STORAGE -> "ContractService.STORAGE";
            case STATE_ID_EVM_HOOK_STATES -> "ContractService.EVM_HOOK_STATES";
            case STATE_ID_LAMBDA_STORAGE -> "ContractService.LAMBDA_STORAGE";
            case STATE_ID_ENTITY_ID -> "EntityIdService.ENTITY_ID";
            case STATE_ID_ENTITY_COUNTS -> "EntityIdService.ENTITY_COUNTS";
            case STATE_ID_MIDNIGHT_RATES -> "FeeService.MIDNIGHT_RATES";
            case STATE_ID_FILES -> "FileService.FILES";
            case STATE_ID_UPGRADE_DATA_150 -> "FileService.UPGRADE_DATA_150";
            case STATE_ID_UPGRADE_DATA_151 -> "FileService.UPGRADE_DATA_151";
            case STATE_ID_UPGRADE_DATA_152 -> "FileService.UPGRADE_DATA_152";
            case STATE_ID_UPGRADE_DATA_153 -> "FileService.UPGRADE_DATA_153";
            case STATE_ID_UPGRADE_DATA_154 -> "FileService.UPGRADE_DATA_154";
            case STATE_ID_UPGRADE_DATA_155 -> "FileService.UPGRADE_DATA_155";
            case STATE_ID_UPGRADE_DATA_156 -> "FileService.UPGRADE_DATA_156";
            case STATE_ID_UPGRADE_DATA_157 -> "FileService.UPGRADE_DATA_157";
            case STATE_ID_UPGRADE_DATA_158 -> "FileService.UPGRADE_DATA_158";
            case STATE_ID_UPGRADE_DATA_159 -> "FileService.UPGRADE_DATA_159";
            case STATE_ID_FREEZE_TIME -> "FreezeService.FREEZE_TIME";
            case STATE_ID_UPGRADE_FILE_HASH -> "FreezeService.UPGRADE_FILE_HASH";
            case STATE_ID_PLATFORM_STATE -> "PlatformStateService.PLATFORM_STATE";
            case STATE_ID_ROSTER_STATE -> "RosterService.ROSTER_STATE";
            case STATE_ID_ROSTERS -> "RosterService.ROSTERS";
            case STATE_ID_TRANSACTION_RECEIPTS -> "RecordCache.TRANSACTION_RECEIPTS";
            case STATE_ID_SCHEDULES_BY_EQUALITY -> "ScheduleService.SCHEDULES_BY_EQUALITY";
            case STATE_ID_SCHEDULES_BY_EXPIRY_SEC -> "ScheduleService.SCHEDULES_BY_EXPIRY_SEC";
            case STATE_ID_SCHEDULES_BY_ID -> "ScheduleService.SCHEDULES_BY_ID";
            case STATE_ID_SCHEDULE_ID_BY_EQUALITY -> "ScheduleService.SCHEDULE_ID_BY_EQUALITY";
            case STATE_ID_SCHEDULED_COUNTS -> "ScheduleService.SCHEDULED_COUNTS";
            case STATE_ID_SCHEDULED_ORDERS -> "ScheduleService.SCHEDULED_ORDERS";
            case STATE_ID_SCHEDULED_USAGES -> "ScheduleService.SCHEDULED_USAGES";
            case STATE_ID_ACCOUNTS -> "TokenService.ACCOUNTS";
            case STATE_ID_ALIASES -> "TokenService.ALIASES";
            case STATE_ID_NFTS -> "TokenService.NFTS";
            case STATE_ID_PENDING_AIRDROPS -> "TokenService.PENDING_AIRDROPS";
            case STATE_ID_STAKING_INFOS -> "TokenService.STAKING_INFOS";
            case STATE_ID_STAKING_NETWORK_REWARDS -> "TokenService.STAKING_NETWORK_REWARDS";
            case STATE_ID_TOKEN_RELS -> "TokenService.TOKEN_RELS";
            case STATE_ID_TOKENS -> "TokenService.TOKENS";
            case STATE_ID_NODE_REWARDS -> "TokenService.NODE_REWARDS";
            case STATE_ID_TSS_MESSAGES -> "TssBaseService.TSS_MESSAGES";
            case STATE_ID_TSS_VOTES -> "TssBaseService.TSS_VOTES";
            case STATE_ID_TSS_ENCRYPTION_KEYS -> "TssBaseService.TSS_ENCRYPTION_KEYS";
            case STATE_ID_TSS_STATUS -> "TssBaseService.TSS_STATUS";
            case STATE_ID_HINTS_KEY_SETS -> "HintsService.HINTS_KEY_SETS";
            case STATE_ID_ACTIVE_HINTS_CONSTRUCTION -> "HintsService.ACTIVE_HINTS_CONSTRUCTION";
            case STATE_ID_NEXT_HINTS_CONSTRUCTION -> "HintsService.NEXT_HINTS_CONSTRUCTION";
            case STATE_ID_PREPROCESSING_VOTES -> "HintsService.PREPROCESSING_VOTES";
            case STATE_ID_CRS_STATE -> "HintsService.CRS_STATE";
            case STATE_ID_CRS_PUBLICATIONS -> "HintsService.CRS_PUBLICATIONS";
            case STATE_ID_LEDGER_ID -> "HistoryService.LEDGER_ID";
            case STATE_ID_PROOF_KEY_SETS -> "HistoryService.PROOF_KEY_SETS";
            case STATE_ID_ACTIVE_PROOF_CONSTRUCTION -> "HistoryService.ACTIVE_PROOF_CONSTRUCTION";
            case STATE_ID_NEXT_PROOF_CONSTRUCTION -> "HistoryService.NEXT_PROOF_CONSTRUCTION";
            case STATE_ID_HISTORY_SIGNATURES -> "HistoryService.HISTORY_SIGNATURES";
            case STATE_ID_PROOF_VOTES -> "HistoryService.PROOF_VOTES";
        };
    }

    /**
     * Parse the service name from a full state name.
     *
     * @param stateName the full state name in "ServiceName.STATE_KEY" format
     * @return the service name
     */
    @NonNull
    public static String parseServiceName(@NonNull final String stateName) {
        final int dotIndex = stateName.indexOf('.');
        if (dotIndex < 0) {
            throw new IllegalArgumentException("Invalid state name format: " + stateName);
        }
        return stateName.substring(0, dotIndex);
    }

    /**
     * Parse the state key from a full state name.
     *
     * @param stateName the full state name in "ServiceName.STATE_KEY" format
     * @return the state key
     */
    @NonNull
    public static String parseStateKey(@NonNull final String stateName) {
        final int dotIndex = stateName.indexOf('.');
        if (dotIndex < 0) {
            throw new IllegalArgumentException("Invalid state name format: " + stateName);
        }
        return stateName.substring(dotIndex + 1);
    }

    /**
     * Determine if a state change represents a singleton update.
     *
     * @param change the state change
     * @return true if singleton update
     */
    public static boolean isSingletonUpdate(@NonNull final StateChange change) {
        return change.changeOperation().kind() == StateChange.ChangeOperationOneOfType.SINGLETON_UPDATE;
    }

    /**
     * Determine if a state change represents a map update.
     *
     * @param change the state change
     * @return true if map update
     */
    public static boolean isMapUpdate(@NonNull final StateChange change) {
        return change.changeOperation().kind() == StateChange.ChangeOperationOneOfType.MAP_UPDATE;
    }

    /**
     * Determine if a state change represents a map delete.
     *
     * @param change the state change
     * @return true if map delete
     */
    public static boolean isMapDelete(@NonNull final StateChange change) {
        return change.changeOperation().kind() == StateChange.ChangeOperationOneOfType.MAP_DELETE;
    }

    /**
     * Determine if a state change represents a queue push.
     *
     * @param change the state change
     * @return true if queue push
     */
    public static boolean isQueuePush(@NonNull final StateChange change) {
        return change.changeOperation().kind() == StateChange.ChangeOperationOneOfType.QUEUE_PUSH;
    }

    /**
     * Determine if a state change represents a queue pop.
     *
     * @param change the state change
     * @return true if queue pop
     */
    public static boolean isQueuePop(@NonNull final StateChange change) {
        return change.changeOperation().kind() == StateChange.ChangeOperationOneOfType.QUEUE_POP;
    }

    /**
     * Determine if a state change represents a new state being added.
     *
     * @param change the state change
     * @return true if new state
     */
    public static boolean isNewState(@NonNull final StateChange change) {
        return change.changeOperation().kind() == StateChange.ChangeOperationOneOfType.STATE_ADD;
    }

    /**
     * Determine if a state change represents a state being removed.
     *
     * @param change the state change
     * @return true if state removed
     */
    public static boolean isStateRemoved(@NonNull final StateChange change) {
        return change.changeOperation().kind() == StateChange.ChangeOperationOneOfType.STATE_REMOVE;
    }

    /**
     * Convert a singleton update change to the appropriate Java object.
     *
     * @param singletonUpdateChange the singleton update change
     * @return the Java object representing the singleton value
     */
    @NonNull
    public static Object singletonValueFor(@NonNull final SingletonUpdateChange singletonUpdateChange) {
        return switch (singletonUpdateChange.newValue().kind()) {
            case UNSET -> throw new IllegalStateException("Singleton update value is not set");
            case BLOCK_INFO_VALUE -> singletonUpdateChange.blockInfoValueOrThrow();
            case CONGESTION_LEVEL_STARTS_VALUE -> singletonUpdateChange.congestionLevelStartsValueOrThrow();
            case ENTITY_NUMBER_VALUE -> new EntityNumber(singletonUpdateChange.entityNumberValueOrThrow());
            case EXCHANGE_RATE_SET_VALUE -> singletonUpdateChange.exchangeRateSetValueOrThrow();
            case NETWORK_STAKING_REWARDS_VALUE -> singletonUpdateChange.networkStakingRewardsValueOrThrow();
            case NODE_REWARDS_VALUE -> singletonUpdateChange.nodeRewardsValueOrThrow();
            case BYTES_VALUE -> new ProtoBytes(singletonUpdateChange.bytesValueOrThrow());
            case STRING_VALUE -> new ProtoString(singletonUpdateChange.stringValueOrThrow());
            case RUNNING_HASHES_VALUE -> singletonUpdateChange.runningHashesValueOrThrow();
            case THROTTLE_USAGE_SNAPSHOTS_VALUE -> singletonUpdateChange.throttleUsageSnapshotsValueOrThrow();
            case TIMESTAMP_VALUE -> singletonUpdateChange.timestampValueOrThrow();
            case BLOCK_STREAM_INFO_VALUE -> singletonUpdateChange.blockStreamInfoValueOrThrow();
            case PLATFORM_STATE_VALUE -> singletonUpdateChange.platformStateValueOrThrow();
            case ROSTER_STATE_VALUE -> singletonUpdateChange.rosterStateValueOrThrow();
            case HINTS_CONSTRUCTION_VALUE -> singletonUpdateChange.hintsConstructionValueOrThrow();
            case ENTITY_COUNTS_VALUE -> singletonUpdateChange.entityCountsValueOrThrow();
            case HISTORY_PROOF_CONSTRUCTION_VALUE -> singletonUpdateChange.historyProofConstructionValueOrThrow();
            case CRS_STATE_VALUE -> singletonUpdateChange.crsStateValueOrThrow();
        };
    }

    /**
     * Convert a queue push change to the appropriate Java object.
     *
     * @param queuePushChange the queue push change
     * @return the Java object representing the queue element value
     */
    @NonNull
    public static Object queuePushValueFor(@NonNull final QueuePushChange queuePushChange) {
        return switch (queuePushChange.value().kind()) {
            case UNSET, PROTO_STRING_ELEMENT -> throw new IllegalStateException("Queue push value is not supported");
            case PROTO_BYTES_ELEMENT -> new ProtoBytes(queuePushChange.protoBytesElementOrThrow());
            case TRANSACTION_RECEIPT_ENTRIES_ELEMENT -> queuePushChange.transactionReceiptEntriesElementOrThrow();
        };
    }

    /**
     * Convert a map change key to the appropriate Java object.
     *
     * @param mapChangeKey the map change key
     * @return the Java object representing the key
     */
    @NonNull
    public static Object mapKeyFor(@NonNull final MapChangeKey mapChangeKey) {
        return switch (mapChangeKey.keyChoice().kind()) {
            case UNSET -> throw new IllegalStateException("Key choice is not set for " + mapChangeKey);
            case ACCOUNT_ID_KEY -> mapChangeKey.accountIdKeyOrThrow();
            case TOKEN_RELATIONSHIP_KEY -> pairFrom(mapChangeKey.tokenRelationshipKeyOrThrow());
            case ENTITY_NUMBER_KEY -> new EntityNumber(mapChangeKey.entityNumberKeyOrThrow());
            case FILE_ID_KEY -> mapChangeKey.fileIdKeyOrThrow();
            case NFT_ID_KEY -> mapChangeKey.nftIdKeyOrThrow();
            case PROTO_BYTES_KEY -> new ProtoBytes(mapChangeKey.protoBytesKeyOrThrow());
            case PROTO_LONG_KEY -> new ProtoLong(mapChangeKey.protoLongKeyOrThrow());
            case PROTO_STRING_KEY -> new ProtoString(mapChangeKey.protoStringKeyOrThrow());
            case SCHEDULE_ID_KEY -> mapChangeKey.scheduleIdKeyOrThrow();
            case SLOT_KEY_KEY -> mapChangeKey.slotKeyKeyOrThrow();
            case TOKEN_ID_KEY -> mapChangeKey.tokenIdKeyOrThrow();
            case TOPIC_ID_KEY -> mapChangeKey.topicIdKeyOrThrow();
            case CONTRACT_ID_KEY -> mapChangeKey.contractIdKeyOrThrow();
            case PENDING_AIRDROP_ID_KEY -> mapChangeKey.pendingAirdropIdKeyOrThrow();
            case TIMESTAMP_SECONDS_KEY -> mapChangeKey.timestampSecondsKeyOrThrow();
            case SCHEDULED_ORDER_KEY -> mapChangeKey.scheduledOrderKeyOrThrow();
            case TSS_MESSAGE_MAP_KEY -> mapChangeKey.tssMessageMapKeyOrThrow();
            case TSS_VOTE_MAP_KEY -> mapChangeKey.tssVoteMapKeyOrThrow();
            case HINTS_PARTY_ID_KEY -> mapChangeKey.hintsPartyIdKeyOrThrow();
            case PREPROCESSING_VOTE_ID_KEY -> mapChangeKey.preprocessingVoteIdKeyOrThrow();
            case NODE_ID_KEY -> mapChangeKey.nodeIdKeyOrThrow();
            case CONSTRUCTION_NODE_ID_KEY -> mapChangeKey.constructionNodeIdKeyOrThrow();
            case HOOK_ID_KEY -> mapChangeKey.hookIdKeyOrThrow();
            case LAMBDA_SLOT_KEY -> mapChangeKey.lambdaSlotKeyOrThrow();
        };
    }

    /**
     * Convert a map change value to the appropriate Java object.
     *
     * @param mapChangeValue the map change value
     * @return the Java object representing the value
     */
    @NonNull
    public static Object mapValueFor(@NonNull final MapChangeValue mapChangeValue) {
        return switch (mapChangeValue.valueChoice().kind()) {
            case UNSET -> throw new IllegalStateException("Value choice is not set for " + mapChangeValue);
            case ACCOUNT_VALUE -> mapChangeValue.accountValueOrThrow();
            case ACCOUNT_ID_VALUE -> mapChangeValue.accountIdValueOrThrow();
            case BYTECODE_VALUE -> mapChangeValue.bytecodeValueOrThrow();
            case FILE_VALUE -> mapChangeValue.fileValueOrThrow();
            case NFT_VALUE -> mapChangeValue.nftValueOrThrow();
            case PROTO_STRING_VALUE -> new ProtoString(mapChangeValue.protoStringValueOrThrow());
            case SCHEDULE_VALUE -> mapChangeValue.scheduleValueOrThrow();
            case SCHEDULE_ID_VALUE -> mapChangeValue.scheduleIdValueOrThrow();
            case SCHEDULE_LIST_VALUE -> mapChangeValue.scheduleListValueOrThrow();
            case SLOT_VALUE_VALUE -> mapChangeValue.slotValueValueOrThrow();
            case STAKING_NODE_INFO_VALUE -> mapChangeValue.stakingNodeInfoValueOrThrow();
            case TOKEN_VALUE -> mapChangeValue.tokenValueOrThrow();
            case TOKEN_RELATION_VALUE -> mapChangeValue.tokenRelationValueOrThrow();
            case TOPIC_VALUE -> mapChangeValue.topicValueOrThrow();
            case NODE_VALUE -> mapChangeValue.nodeValueOrThrow();
            case ACCOUNT_PENDING_AIRDROP_VALUE -> mapChangeValue.accountPendingAirdropValueOrThrow();
            case ROSTER_VALUE -> mapChangeValue.rosterValueOrThrow();
            case SCHEDULED_COUNTS_VALUE -> mapChangeValue.scheduledCountsValueOrThrow();
            case THROTTLE_USAGE_SNAPSHOTS_VALUE -> mapChangeValue.throttleUsageSnapshotsValueOrThrow();
            case TSS_ENCRYPTION_KEYS_VALUE -> mapChangeValue.tssEncryptionKeysValueOrThrow();
            case TSS_MESSAGE_VALUE -> mapChangeValue.tssMessageValueOrThrow();
            case TSS_VOTE_VALUE -> mapChangeValue.tssVoteValueOrThrow();
            case HINTS_KEY_SET_VALUE -> mapChangeValue.hintsKeySetValueOrThrow();
            case PREPROCESSING_VOTE_VALUE -> mapChangeValue.preprocessingVoteValueOrThrow();
            case CRS_PUBLICATION_VALUE -> mapChangeValue.crsPublicationValueOrThrow();
            case HISTORY_PROOF_VOTE_VALUE -> mapChangeValue.historyProofVoteValueOrThrow();
            case HISTORY_SIGNATURE_VALUE -> mapChangeValue.historySignatureValueOrThrow();
            case PROOF_KEY_SET_VALUE -> mapChangeValue.proofKeySetValueOrThrow();
            case EVM_HOOK_STATE_VALUE -> mapChangeValue.evmHookStateValueOrThrow();
            case NODE_ID_VALUE -> mapChangeValue.nodeIdValueOrThrow();
        };
    }

    /**
     * Create an EntityIDPair from a TokenAssociation.
     *
     * @param tokenAssociation the token association
     * @return the entity ID pair
     */
    @NonNull
    public static EntityIDPair pairFrom(@NonNull final TokenAssociation tokenAssociation) {
        return new EntityIDPair(tokenAssociation.accountId(), tokenAssociation.tokenId());
    }

    /**
     * Determine the state type from a state change.
     *
     * @param change the state change
     * @return the new state type
     */
    @NonNull
    public static NewStateType getStateType(@NonNull final StateChange change) {
        final var op = change.changeOperation();
        return switch (op.kind()) {
            case STATE_ADD -> {
                final NewStateChange newState = op.as();
                yield newState.stateType();
            }
            case SINGLETON_UPDATE -> NewStateType.SINGLETON;
            case MAP_UPDATE, MAP_DELETE -> NewStateType.VIRTUAL_MAP;
            case QUEUE_PUSH, QUEUE_POP -> NewStateType.QUEUE;
            default -> throw new IllegalArgumentException("Cannot determine state type from: " + op.kind());
        };
    }
}
