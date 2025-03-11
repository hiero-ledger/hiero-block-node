// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.server.service;

import com.hedera.hapi.block.SubscribeStreamResponseUnparsed;
import com.hedera.hapi.block.SubscribeStreamResponseCode;

/** Constants used in the BlockNode service. */
public final class Constants {
    /** Constant mapped to PbjProtocolProvider.CONFIG_NAME in the PBJ Helidon Plugin */
    public static final String PBJ_PROTOCOL_PROVIDER_CONFIG_NAME = "pbj";

    /** Constant mapped to the name of the BlockStream service in the .proto file */
    public static final String SERVICE_NAME_BLOCK_STREAM = "BlockStreamService";

    /** Constant mapped to the name of the BlockAccess service in the .proto file */
    public static final String SERVICE_NAME_BLOCK_ACCESS = "BlockAccessService";

    /** Constant representing the service domain */
    public static final String SERVICE_DOMAIN = "com.hedera.hapi.block.";

    /** Constant mapped to the full name of the BlockStream service */
    public static final String FULL_SERVICE_NAME_BLOCK_STREAM = SERVICE_DOMAIN + SERVICE_NAME_BLOCK_STREAM;

    /** Constant mapped to the full name of the BlockAccess service */
    public static final String FULL_SERVICE_NAME_BLOCK_ACCESS = SERVICE_DOMAIN + SERVICE_NAME_BLOCK_ACCESS;

    /** Constant defining the block file extension */
    public static final String BLOCK_FILE_EXTENSION = ".blk";

    /** Constant defining zip file extension */
    public static final String ZIP_FILE_EXTENSION = ".zip";

    public static final SubscribeStreamResponseUnparsed READ_STREAM_INVALID_START_BLOCK_NUMBER_RESPONSE =
            SubscribeStreamResponseUnparsed.newBuilder()
            .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_START_BLOCK_NUMBER)
            .build();
    public static final SubscribeStreamResponseUnparsed READ_STREAM_INVALID_END_BLOCK_NUMBER_RESPONSE =
            SubscribeStreamResponseUnparsed.newBuilder()
            .status(SubscribeStreamResponseCode.READ_STREAM_INVALID_END_BLOCK_NUMBER)
            .build();
    public static final SubscribeStreamResponseUnparsed READ_STREAM_SUCCESS_RESPONSE =
            SubscribeStreamResponseUnparsed.newBuilder()
            .status(SubscribeStreamResponseCode.READ_STREAM_SUCCESS)
            .build();
    public static final SubscribeStreamResponseUnparsed READ_STREAM_NOT_AVAILABLE =
            SubscribeStreamResponseUnparsed.newBuilder()
            .status(SubscribeStreamResponseCode.READ_STREAM_NOT_AVAILABLE)
            .build();

    private Constants() {}
}
