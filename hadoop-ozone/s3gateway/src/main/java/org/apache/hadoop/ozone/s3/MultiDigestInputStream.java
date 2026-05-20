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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An InputStream that computes multiple message digests simultaneously
 * as data is read from the underlying stream.
 *
 * <p>
 * This class extends {@link FilterInputStream} and allows multiple digest
 * algorithms (for example, MD5 or SHA-256) to be computed in a single pass
 * over the data. This is more efficient than reading the stream multiple
 * times when multiple digests are required.
 * </p>
 *
 * <p>Important note about relationship to {@code DigestInputStream}:</p>
 * <ul>
 *   <li>This class is conceptually similar to {@link java.security.DigestInputStream}.
 *       Several methods (notably {@link #read()} , {@link #read(byte[], int, int)} and
 *       {@link #on(boolean)}) follow the same behavior and semantics as in
 *       {@code DigestInputStream} and are documented here with that intent.
 *   </li>
 *   <li>Where method signatures differ from {@code DigestInputStream} (for
 *       example {@link #getMessageDigest(String)} which takes an algorithm name
 *       and returns the corresponding digest), the difference is explicitly
 *       documented on the method itself.</li>
 * </ul>
 *
 * <p>Example usage:</p>
 * <pre>
 * MessageDigest md5 = MessageDigest.getInstance("MD5");
 * MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
 * MultiDigestInputStream mdis = new MultiDigestInputStream(inputStream, md5, sha256);
 * // Read from mdis (reads will update all registered digests while 'on' is true)
 * byte[] md5Hash = mdis.getMessageDigest("MD5").digest();
 * byte[] sha256Hash = mdis.getMessageDigest("SHA-256").digest();
 * </pre>
 *
 * <p>Notes:</p>
 * <ul>
 *   <li>The constructor accepts one or more already-created {@link MessageDigest}
 *       instances; the digests are kept and updated as data is read.</li>
 *   <li>Call {@link #on(boolean)} with {@code false} to temporarily disable
 *       digest updates (for example, to skip computing during certain reads),
 *       and {@code true} to re-enable. This behavior mirrors
 *       {@link java.security.DigestInputStream#on(boolean)}.</li>
 *   <li>{@link #getAllDigests()} returns a copy of the internal digest map.</li>
 * </ul>
 *
 * @see java.security.DigestInputStream
 */
public class MultiDigestInputStream extends FilterInputStream {

  private final Map<String, MessageDigest> digests;
  private boolean on = true;

  /**
   * Creates a MultiDigestInputStream with the specified digests.
   *
   * @param in the underlying input stream
   * @param inputDigests the message digest instances to compute (may be zero-length)
   */
  public MultiDigestInputStream(InputStream in, Collection<MessageDigest> inputDigests) {
    super(in);
    this.digests = new HashMap<>();
    for (MessageDigest digest : inputDigests) {
      digests.put(digest.getAlgorithm(), digest);
    }
  }

  /**
   * Reads the next byte of data from the input stream. If a byte is read and
   * digest updates are enabled (see {@link #on(boolean)}), the byte is
   * supplied to all registered digests.
   *
   * @return the next byte of data, or -1 if the end of the stream is reached
   * @throws IOException if an I/O error occurs
   */
  @Override
  public int read() throws IOException {
    int ch = in.read();
    if (ch != -1) {
      updateDigests((byte) ch);
    }
    return ch;
  }

  /**
   * Reads up to {@code len} bytes of data into an array of bytes from the
   * input stream. If bytes are read and digest updates are enabled, the
   * read bytes are supplied to all registered digests.
   *
   * @param b the buffer into which the data is read
   * @param off the start offset in array {@code b} at which the data is written
   * @param len the maximum number of bytes to read
   * @return the total number of bytes read into the buffer, or -1 if there is
   *         no more data because the end of the stream has been reached
   * @throws IOException if an I/O error occurs
   */
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
   * Gets the {@link MessageDigest} instance for the specified algorithm.
   *
   * <p>Note: {@code DigestInputStream#getMessageDigest()} returns
   * the single digest instance associated with that stream. This class may
   * manage multiple digests; therefore this method accepts an algorithm name
   * and returns the corresponding {@link MessageDigest} or {@code null} if not
   * registered.
   *
   * @param algorithm the digest algorithm name (for example, "MD5" or "SHA-256")
   * @return the MessageDigest instance for the specified algorithm,
   *         or {@code null} if the algorithm was not registered
   * @see java.security.DigestInputStream#getMessageDigest()
   */
  public MessageDigest getMessageDigest(String algorithm) {
    return digests.get(algorithm);
  }

  /**
   * Returns a copy of the map of all digests being computed.
   * Modifications to the returned map do not affect the stream's internal state.
   *
   * @return a shallow copy of the digests map (algorithm name to MessageDigest)
   */
  public Map<String, MessageDigest> getAllDigests() {
    return new HashMap<>(digests);
  }

  /**
   * Resets all message digests by calling {@link MessageDigest#reset()} on each
   * registered digest.
   */
  public void resetDigests() {
    for (MessageDigest digest : digests.values()) {
      digest.reset();
    }
  }

  /**
   * Enable or disable updating of the registered digests while reading.
   *
   * @param enabled true to turn the digest function on, false to turn it off
   */
  public void on(boolean enabled) {
    this.on = enabled;
  }

  /**
   * Associates the given MessageDigest with the specified algorithm name,
   * replacing any existing digest for that algorithm.
   *
   * @param algorithm the digest algorithm name
   * @param digest the MessageDigest instance to set
   */
  public void setMessageDigest(String algorithm, MessageDigest digest) {
    digests.put(algorithm, digest);
  }

  /**
   * Adds a new message digest algorithm to be computed. If the algorithm name
   * already exists in the map, it will be replaced by the newly created
   * MessageDigest instance.
   *
   * @param algorithm the digest algorithm name
   * @throws NoSuchAlgorithmException if the algorithm is not available
   */
  public void addMessageDigest(String algorithm)
      throws NoSuchAlgorithmException {
    digests.put(algorithm, MessageDigest.getInstance(algorithm));
  }

  /**
   * Removes and returns the message digest instance for the specified
   * algorithm name.
   *
   * @param algorithm the digest algorithm name to remove
   * @return the removed MessageDigest, or {@code null} if not found
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
