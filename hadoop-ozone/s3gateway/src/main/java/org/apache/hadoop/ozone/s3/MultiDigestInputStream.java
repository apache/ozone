/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

/**
 * An InputStream that computes multiple message digests simultaneously
 * as data is read from the underlying stream.
 * <p>
 * This class extends FilterInputStream and allows multiple digest algorithms
 * (e.g., MD5, SHA-256) to be computed in a single pass over the data,
 * which is more efficient than reading the stream multiple times.
 * </p>
 * <p>
 * Example usage:
 * <pre>
 * MessageDigest md5 = MessageDigest.getInstance("MD5");
 * MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
 * MultiMessageDigestInputStream mdis = new MultiMessageDigestInputStream(
 *     inputStream, md5, sha256);
 * // Read from mdis...
 * byte[] md5Hash = mdis.getDigest("MD5").digest();
 * byte[] sha256Hash = mdis.getDigest("SHA-256").digest();
 * </pre>
 * </p>
 */
public class MultiDigestInputStream extends FilterInputStream {

  private final Map<String, MessageDigest> digests;
  private boolean on = true;

  /**
   * Creates a MultiMessageDigestInputStream with the specified digests.
   *
   * @param in the underlying input stream
   * @param inputDigests the message digest instances to compute
   */
  public MultiDigestInputStream(InputStream in, MessageDigest... inputDigests) {
    super(in);
    this.digests = new HashMap<>();
    for (MessageDigest digest : inputDigests) {
      digests.put(digest.getAlgorithm(), digest);
    }
  }

  @Override
  public int read() throws IOException {
    int ch = in.read();
    if (ch != -1) {
      updateDigests((byte) ch);
    }
    return ch;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesRead = in.read(b, off, len);
    if (bytesRead > 0) {
      updateDigests(b, off, bytesRead);
    }
    return bytesRead;
  }

  private void updateDigests(byte b) {
    if (!on) {
      return;
    }
    for (MessageDigest digest : digests.values()) {
      digest.update(b);
    }
  }

  private void updateDigests(byte[] b, int off, int len) {
    if (!on) {
      return;
    }
    for (MessageDigest digest : digests.values()) {
      digest.update(b, off, len);
    }
  }

  /**
   * Gets the MessageDigest for the specified algorithm.
   *
   * @param algorithm the digest algorithm name (e.g., "MD5", "SHA-256")
   * @return the MessageDigest instance for the specified algorithm,
   *         or null if the algorithm was not registered
   */
  public MessageDigest getMessageDigest(String algorithm) {
    return digests.get(algorithm);
  }

  /**
   * Returns a map of all digests being computed.
   *
   * @return an immutable view of the digests map
   */
  public Map<String, MessageDigest> getAllDigests() {
    return new HashMap<>(digests);
  }

  /**
   * Resets all message digests.
   */
  public void resetDigests() {
    for (MessageDigest digest : digests.values()) {
      digest.reset();
    }
  }

  /**
   * Turns the digest function on or off. The default is on. When it is on,
   * a call to one of the read methods results in an update on all message
   * digests. When it is off, the message digests are not updated.
   *
   * @param enabled true to turn the digest function on, false to turn it off
   */
  public void on(boolean enabled) {
    this.on = enabled;
  }

  /**
   * Sets the message digest for a specific algorithm, replacing any existing
   * digest for that algorithm.
   *
   * @param algorithm the digest algorithm name
   * @param digest the message digest to associate with the algorithm
   */
  public void setMessageDigest(String algorithm, MessageDigest digest) {
    digests.put(algorithm, digest);
  }

  /**
   * Adds a new message digest algorithm to be computed.
   * If the algorithm already exists, it will be replaced.
   *
   * @param algorithm the digest algorithm name
   * @throws NoSuchAlgorithmException if the algorithm is not available
   */
  public void addMessageDigest(String algorithm)
      throws NoSuchAlgorithmException {
    digests.put(algorithm, MessageDigest.getInstance(algorithm));
  }

  /**
   * Removes the message digest for a specific algorithm.
   *
   * @param algorithm the digest algorithm name to remove
   * @return the removed MessageDigest, or null if not found
   */
  public MessageDigest removeMessageDigest(String algorithm) {
    return digests.remove(algorithm);
  }

  /**
   * Returns a string representation of this stream and its message digests.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    return getClass().getName() + " [on=" + on + ", algorithms="
        + digests.keySet() + "]";
  }
}
