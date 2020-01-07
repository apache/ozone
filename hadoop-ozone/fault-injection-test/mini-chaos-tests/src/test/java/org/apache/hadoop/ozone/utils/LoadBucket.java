/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.utils;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

/**
 * Bucket to perform read/write & delete ops.
 */
public class LoadBucket {
  private static final Logger LOG =
            LoggerFactory.getLogger(LoadBucket.class);

  private final OzoneBucket bucket;

  public LoadBucket(OzoneBucket bucket) {
    this.bucket = bucket;
  }

  public void writeData(ByteBuffer buffer, String keyName) throws Exception {
    int bufferCapacity = buffer.capacity();

    LOG.debug("LOADGEN: Writing key {}", keyName);
    try (OzoneOutputStream stream = bucket.createKey(keyName,
        bufferCapacity, ReplicationType.RATIS, ReplicationFactor.THREE,
            new HashMap<>())) {
      stream.write(buffer.array());
      LOG.trace("LOADGEN: Written key {}", keyName);
    } catch (Throwable t) {
      LOG.error("LOADGEN: Create key:{} failed with exception, skipping",
              keyName, t);
      throw t;
    }
  }

  public void readData(ByteBuffer buffer, String keyName) throws Exception {
    LOG.debug("LOADGEN: Reading key {}", keyName);

    int bufferCapacity = buffer.capacity();

    try (OzoneInputStream stream = bucket.readKey(keyName)) {
      byte[] readBuffer = new byte[bufferCapacity];
      int readLen = stream.read(readBuffer);

      if (readLen < bufferCapacity) {
        throw new IOException("Read mismatch, key:" + keyName +
                " read data length:" + readLen + " is smaller than excepted:"
                + bufferCapacity);
      }

      if (!Arrays.equals(readBuffer, buffer.array())) {
        throw new IOException("Read mismatch, key:" + keyName +
                " read data does not match the written data");
      }
      LOG.trace("LOADGEN: Read key {}", keyName);
    } catch (Throwable t) {
      LOG.error("LOADGEN: Read key:{} failed with exception", keyName, t);
      throw t;
    }
  }

  public void deleteKey(String keyName) throws Exception {
    LOG.debug("LOADGEN: Deleting key {}", keyName);
    try {
      bucket.deleteKey(keyName);
      LOG.trace("LOADGEN: Deleted key {}", keyName);
    } catch (Throwable t) {
      LOG.error("LOADGEN: Unable to delete key:{}", keyName, t);
      throw t;
    }
  }
}
