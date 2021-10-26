package org.apache.hadoop.ozone.om.protocol;

public class S3Authentication {
  private String stringToSign;
  private String signature;
  private String accessID;

  public S3Authentication(final String stringToSign, final String signature, final String accessID) {
    this.accessID = accessID;
    this.stringToSign = stringToSign;
    this.signature = signature;
  }
  public String getStringTosSign() {
    return stringToSign;
  }

  public String getSignature() {
    return signature;
  }

  public String getAccessID() {
    return accessID;
  }
}
