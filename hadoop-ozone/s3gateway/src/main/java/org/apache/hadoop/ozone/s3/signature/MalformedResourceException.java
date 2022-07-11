package org.apache.hadoop.ozone.s3.signature;

public class MalformedResourceException extends Exception {
  private final String resource;

  public MalformedResourceException(String resource) {
    this.resource = resource;
  }

  public MalformedResourceException(String message, String resource) {
    super(message);
    this.resource = resource;
  }

  public String getResource() {
    return resource;
  }
}
