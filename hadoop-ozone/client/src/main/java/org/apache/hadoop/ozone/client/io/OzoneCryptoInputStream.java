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

package org.apache.hadoop.ozone.client.io;

import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.hadoop.crypto.CryptoCodec;
import org.apache.hadoop.crypto.CryptoInputStream;
import org.apache.hadoop.crypto.CryptoStreamUtils;
import org.apache.hadoop.hdds.scm.storage.PartInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A CryptoInputStream for Ozone with length. This stream is used to read
 * Keys in Encrypted Buckets.
 */
public class OzoneCryptoInputStream extends CryptoInputStream
    implements PartInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneCryptoInputStream.class);

  private final long length;
  private final int bufferSize;
  private final String keyName;
  private final int partIndex;

  // If a read's start/ length position doesn't coincide with a Crypto buffer
  // boundary, it will be adjusted as reads should happen only at the buffer
  // boundaries for decryption to happen correctly. In this case, after the
  // data has been read and decrypted, only the requested data should be
  // returned to the client. readPositionAdjustedBy and readLengthAdjustedBy
  // store these adjustment information. Before returning to client, the first
  // readPositionAdjustedBy number of bytes and the last readLengthAdjustedBy
  // number of bytes must be discarded.
  private int readPositionAdjustedBy = 0;
  private int readLengthAdjustedBy = 0;

  public OzoneCryptoInputStream(LengthInputStream in,
      CryptoCodec codec, byte[] key, byte[] iv,
      String keyName, int partIndex) throws IOException {
    super(in.getWrappedStream(), codec, key, iv);
    this.length = in.getLength();
    // This is the buffer size used while creating the CryptoInputStream
    // internally
    this.bufferSize = CryptoStreamUtils.getBufferSize(codec.getConf());
    this.keyName = keyName;
    this.partIndex = partIndex;
  }

  @Override
  public long getLength() {
    return length;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    // CryptoInputStream reads hadoop.security.crypto.buffer.size number of
    // bytes (default 8KB) at a time. This needs to be taken into account
    // in calculating the numBytesToRead.
    int numBytesToRead = getNumBytesToRead(len, (int)getRemaining(),
        getBufferSize());
    int numBytesRead;

    if (readPositionAdjustedBy != 0 || readLengthAdjustedBy != 0) {
      // There was some adjustment made in position and/ or length of data
      // to be read to account for Crypto buffer boundary. Hence, read the
      // data into a temp buffer and then copy only the requested data into
      // clients buffer.
      byte[] tempBuffer = new byte[numBytesToRead];
      int actualNumBytesRead = super.read(tempBuffer, 0,
          numBytesToRead);
      numBytesRead = actualNumBytesRead - readPositionAdjustedBy -
          readLengthAdjustedBy;

      if (actualNumBytesRead != numBytesToRead) {
        throw new IOException(String.format("Inconsistent read for key=%s " +
                "part=%d length=%d numBytesToRead(accounting for Crypto " +
                "boundaries)=%d numBytesRead(actual)=%d " +
                "numBytesToBeRead(into client buffer discarding crypto " +
                "boundary adjustments)=%d",
            keyName, partIndex, getLength(), numBytesToRead,
            actualNumBytesRead, numBytesRead));
      }

      // TODO: Byte array copies are not optimal. If there is a better and
      //  more optimal solution to copy only a part of read data into
      //  client buffer, this should be updated.
      System.arraycopy(tempBuffer, readPositionAdjustedBy, b, off,
          numBytesRead);

      LOG.debug("OzoneCryptoInputStream for key: {} part: {} read {} bytes " +
              "instead of {} bytes to account for Crypto buffer boundary. " +
              "Client buffer will be copied with read data from position {}" +
              "up to position {}, discarding the extra bytes read to " +
              "maintain Crypto buffer boundary limits", keyName, partIndex,
          actualNumBytesRead, numBytesRead, readPositionAdjustedBy,
          actualNumBytesRead - readPositionAdjustedBy);

      if (readLengthAdjustedBy > 0) {
        seek(getPos() - readLengthAdjustedBy);
      }

      // Reset readPositionAdjustedBy and readLengthAdjustedBy
      readPositionAdjustedBy = 0;
      readLengthAdjustedBy = 0;
    } else {
      numBytesRead = super.read(b, off, numBytesToRead);
      if (numBytesRead != numBytesToRead) {
        throw new IOException(String.format("Inconsistent read for key=%s " +
                "part=%d length=%d numBytesToRead=%d numBytesRead=%d",
            keyName, partIndex, getLength(), numBytesToRead,
            numBytesRead));
      }
    }
    return numBytesRead;
  }

  /**
   * Get number of bytes to read from the current stream based on the length
   * to be read, number of bytes remaining in the stream and the Crypto buffer
   * size.
   * Reads should be performed at the CryptoInputStream Buffer boundaries only.
   * Otherwise, the decryption will be incorrect.
   */
  private int getNumBytesToRead(int lenToRead, int remaining,
                                int cryptoBufferSize) throws IOException {

    Preconditions.checkArgument(readPositionAdjustedBy == 0);
    Preconditions.checkArgument(readLengthAdjustedBy == 0);

    // Check and adjust position if required
    adjustReadPosition(cryptoBufferSize);
    remaining += readPositionAdjustedBy;
    lenToRead += readPositionAdjustedBy;

    return adjustNumBytesToRead(lenToRead, remaining, cryptoBufferSize);
  }

  /**
   * Reads should be performed at the CryptoInputStream Buffer boundary size.
   * Otherwise, the decryption will be incorrect. Hence, if the position is
   * not at the boundary limit, we have to adjust the position and might need
   * to read more data than requested. The extra data will be filtered out
   * before returning to the client.
   */
  private void adjustReadPosition(long cryptoBufferSize) throws IOException {
    // Position of the buffer in current stream
    long currentPosOfStream = getPos();
    int modulus = (int) (currentPosOfStream % cryptoBufferSize);
    if (modulus != 0) {
      // Adjustment required.
      // Update readPositionAdjustedBy and seek to the adjusted position
      readPositionAdjustedBy = modulus;
      // Seek current partStream to adjusted position. We do not need to
      // reset the seeked positions of other streams.
      seek(currentPosOfStream - readPositionAdjustedBy);
      LOG.debug("OzoneCryptoInputStream for key: {} part: {} adjusted " +
              "position {} by -{} to account for Crypto buffer boundary",
          keyName, partIndex, currentPosOfStream, readPositionAdjustedBy);
    }
  }

  /**
   * If the length of data requested does not end at a Crypto Buffer
   * boundary, the number of bytes to be read must be adjusted accordingly.
   * The extra data will be filtered out before returning to the client.
   */
  private int adjustNumBytesToRead(int lenToRead, int remaining,
                                   int cryptoBufferSize) {
    int numBytesToRead = Math.min(cryptoBufferSize, remaining);
    if (lenToRead < numBytesToRead) {
      // Adjustment required; Update readLengthAdjustedBy.
      readLengthAdjustedBy = numBytesToRead - lenToRead;
      LOG.debug("OzoneCryptoInputStream for key: {} part: {} adjusted length " +
              "by +{} to account for Crypto buffer boundary",
          keyName, partIndex, readLengthAdjustedBy);
    }
    return numBytesToRead;
  }

}
