package org.hiero.block.tools.states.model;

import java.util.HashMap;

/**
 *  Mapping of Class name and Object Id
 *
 * @author Akshay
 * @Date : 1/17/2019
 */
public enum JObjectType {

  JKey, JKeyList, JThresholdKey, JEd25519Key, JECDSA_384Key, JRSA_3072Key, JContractIDKey, JAccountID,
  JTransactionID, JTransactionReceipt, JContractFunctionResult, JTransferList, JTransactionRecord,
  JContractLogInfo, JTimestamp, JAccountAmount, JFileInfo, JExchangeRate, JExchangeRateSet, JMemoAdminKey;

  private static final HashMap<JObjectType, Long> LOOKUP_TABLE = new HashMap<>();
  private static final HashMap<Long, JObjectType> REV_LOOKUP_TABLE = new HashMap<>();

  static {
    addLookup(JKey, 15503731);
    addLookup(JKeyList, 15512048);
    addLookup(JThresholdKey, 15520365);
    addLookup(JEd25519Key, 15528682);
    addLookup(JECDSA_384Key, 15536999);
    addLookup(JRSA_3072Key, 15620169);
    addLookup(JContractIDKey, 15545316);
    addLookup(JAccountID, 15553633);
    addLookup(JTransactionID, 15561950);
    addLookup(JTransactionReceipt, 15570267);
    addLookup(JContractFunctionResult, 15578584);
    addLookup(JTransferList, 15586901);
    addLookup(JTransactionRecord, 15595218);
    addLookup(JContractLogInfo, 15603535);
    addLookup(JTimestamp, 15611852);
    addLookup(JAccountAmount, 15628486);
    addLookup(JFileInfo, 15636803);
    addLookup(JExchangeRate, 15645120);
    addLookup(JExchangeRateSet, 15653437);
    addLookup(JMemoAdminKey, 15661754);
  }

  JObjectType() {

  }

  private static void addLookup(final JObjectType type, final long value) {
    LOOKUP_TABLE.put(type, value);
    REV_LOOKUP_TABLE.put(value, type);
  }

  public static JObjectType valueOf(final long value) {
    return REV_LOOKUP_TABLE.get(value);
  }

  public long longValue() {
    return LOOKUP_TABLE.get(this);
  }
}
