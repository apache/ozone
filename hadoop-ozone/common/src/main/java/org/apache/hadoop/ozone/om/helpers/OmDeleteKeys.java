package org.apache.hadoop.ozone.om.helpers;

import java.util.List;

/**
 * Represent class which has info of Keys to be deleted from Client.
 */
public class OmDeleteKeys {

  private String volume;
  private String bucket;

  private List<String> keyNames;


  public OmDeleteKeys(String volume, String bucket, List<String> keyNames) {
    this.volume = volume;
    this.bucket = bucket;
    this.keyNames = keyNames;
  }

  public String getVolume() {
    return volume;
  }

  public String getBucket() {
    return bucket;
  }

  public List< String > getKeyNames() {
    return keyNames;
  }
}
