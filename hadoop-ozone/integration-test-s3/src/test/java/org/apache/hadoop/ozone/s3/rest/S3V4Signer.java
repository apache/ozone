package org.apache.hadoop.ozone.s3.rest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Utility for generating AWS S3 Signature V4 headers for HTTP requests.
 * Only supports unsigned payloads (useful for GET/HEAD).
 */
public class S3V4Signer {
  /**
   * Signs the given HttpURLConnection with AWS S3 Signature V4 headers.
   *
   * @param conn        The HttpURLConnection to sign
   * @param accessKey   AWS access key
   * @param secretKey   AWS secret key
   * @param region      AWS region (e.g., "us-east-1")
   * @param service     AWS service (e.g., "s3")
   * @param bucket      S3 bucket name
   * @param queryString The query string (e.g., "max-keys=-1")
   */
  public static void signRequest(HttpURLConnection conn, String accessKey, String secretKey, String region,
                                 String service, String bucket, String queryString) {
    try {
      String method = conn.getRequestMethod();
      String host = conn.getURL().getHost();
      int port = conn.getURL().getPort();
      if (port != -1 && port != 80 && port != 443) {
        host = host + ":" + port;
      }
      String amzDate = getAmzDate();
      String dateStamp = getDateStamp();

      String payloadHash = hash(""); // SHA256("") for empty payload
      String canonicalUri = "/" + bucket + "/";
      String canonicalHeaders = "host:" + host + "\n" +
          "x-amz-content-sha256:" + payloadHash + "\n" +
          "x-amz-date:" + amzDate + "\n";
      String signedHeaders = "host;x-amz-content-sha256;x-amz-date";

      String canonicalRequest = method + "\n" +
          canonicalUri + "\n" +
          queryString + "\n" +
          canonicalHeaders + "\n" +
          signedHeaders + "\n" +
          payloadHash;

      String algorithm = "AWS4-HMAC-SHA256";
      String credentialScope = dateStamp + "/" + region + "/" + service + "/aws4_request";
      String stringToSign = algorithm + "\n" +
          amzDate + "\n" +
          credentialScope + "\n" +
          hash(canonicalRequest);

      byte[] signingKey = getSignatureKey(secretKey, dateStamp, region, service);
      String signature = bytesToHex(hmacSHA256(stringToSign, signingKey));

      String authorizationHeader = algorithm + " " +
          "Credential=" + accessKey + "/" + credentialScope + ", " +
          "SignedHeaders=" + signedHeaders + ", " +
          "Signature=" + signature;

      conn.setRequestProperty("x-amz-date", amzDate);
      conn.setRequestProperty("x-amz-content-sha256", payloadHash);
      conn.setRequestProperty("Authorization", authorizationHeader);
    } catch (Exception e) {
      throw new RuntimeException("S3V4Signer failed to sign request: " + e.getMessage(), e);
    }
  }

  /**
   * Returns the current date in the format "yyyyMMdd'T'HHmmss'Z'".
   *
   * @return The current date in the format "yyyyMMdd'T'HHmmss'Z'"
   */
  private static String getAmzDate() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    return fmt.format(new Date());
  }

  /**
   * Returns the current date in the format "yyyyMMdd".
   *
   * @return The current date in the format "yyyyMMdd"
   */
  private static String getDateStamp() {
    SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMdd");
    fmt.setTimeZone(TimeZone.getTimeZone("UTC"));
    return fmt.format(new Date());
  }

  /**
   * Returns the SHA-256 hash of the given string.
   *
   * @param text The string to hash
   * @return The SHA-256 hash of the given string
   * @throws Exception If hashing fails
   */
  private static String hash(String text) throws Exception {
    MessageDigest md = MessageDigest.getInstance("SHA-256");
    md.update(text.getBytes(StandardCharsets.UTF_8));
    byte[] digest = md.digest();
    return bytesToHex(digest);
  }

  /**
   * Returns the HMAC-SHA256 of the given data using the given key.
   *
   * @param data The data to sign
   * @param key  The key to use for signing
   * @return The HMAC-SHA256 of the given data using the given key
   * @throws Exception If signing fails
   */
  private static byte[] hmacSHA256(String data, byte[] key) throws Exception {
    String algorithm = "HmacSHA256";
    Mac mac = Mac.getInstance(algorithm);
    mac.init(new SecretKeySpec(key, algorithm));
    return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns the signature key for the given secret key, date stamp, region, and service.
   *
   * @param key        The secret key
   * @param dateStamp  The date stamp
   * @param regionName The region name
   * @param serviceName The service name
   * @return The signature key for the given secret key, date stamp, region, and service
   * @throws Exception If key derivation fails
   */
  private static byte[] getSignatureKey(String key, String dateStamp, String regionName, String serviceName)
      throws Exception {
    byte[] kSecret = ("AWS4" + key).getBytes(StandardCharsets.UTF_8);
    byte[] kDate = hmacSHA256(dateStamp, kSecret);
    byte[] kRegion = hmacSHA256(regionName, kDate);
    byte[] kService = hmacSHA256(serviceName, kRegion);
    return hmacSHA256("aws4_request", kService);
  }

  /**
   * Returns the hexadecimal representation of the given bytes.
   *
   * @param bytes The bytes to convert
   * @return The hexadecimal representation of the given bytes
   */
  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
