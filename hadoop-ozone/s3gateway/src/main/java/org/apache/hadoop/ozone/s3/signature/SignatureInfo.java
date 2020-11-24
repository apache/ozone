package org.apache.hadoop.ozone.s3.signature;

public class SignatureInfo {

  private String date;

  private String awsAccessId;

  private String signature;

  private String signedHeaders;

  private String credentialScope;

  private String algorithm;

  public SignatureInfo(
      String date,
      String awsAccessId,
      String signature,
      String signedHeaders,
      String credentialScope,
      String algorithm
  ) {
    this.date = date;
    this.awsAccessId = awsAccessId;
    this.signature = signature;
    this.signedHeaders = signedHeaders;
    this.credentialScope = credentialScope;
    this.algorithm = algorithm;
  }

  public String getAwsAccessId() {
    return awsAccessId;
  }

  public String getSignature() {
    return signature;
  }

  public String getDate() {
    return date;
  }

  public String getSignedHeaders() {
    return signedHeaders;
  }

  public String getCredentialScope() {
    return credentialScope;
  }

  public String getAlgorithm() {
    return algorithm;
  }
}
