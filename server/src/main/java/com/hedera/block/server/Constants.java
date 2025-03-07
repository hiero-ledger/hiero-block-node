// SPDX-License-Identifier: Apache-2.0
package com.hedera.block.server;

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

    private Constants() {}
}
