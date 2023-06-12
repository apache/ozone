package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;

import java.util.HashMap;
import java.util.List;

/**
 * Return class for OMMetadataManager#getPendingDeletionKeys.
 */
public class PendingKeysDeletion {

  private HashMap<String, RepeatedOmKeyInfo> keysToModify;
  private List<BlockGroup> keyBlocksList;

  public PendingKeysDeletion(List<BlockGroup> keyBlocksList,
       HashMap<String, RepeatedOmKeyInfo> keysToModify) {
    this.keysToModify = keysToModify;
    this.keyBlocksList = keyBlocksList;
  }

  public HashMap<String, RepeatedOmKeyInfo> getKeysToModify() {
    return keysToModify;
  }

  public List<BlockGroup> getKeyBlocksList() {
    return keyBlocksList;
  }
}
