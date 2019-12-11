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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
  private final OzoneFileSystem fs;

  public LoadBucket(OzoneBucket bucket, OzoneConfiguration conf)
    throws Exception {
    this.bucket = bucket;
    this.fs = (OzoneFileSystem)FileSystem.get(getFSUri(bucket), conf);
  }

  private boolean isFsOp() {
    return RandomUtils.nextBoolean();
  }

  // Write ops.

  private OutputStream getOutputStream(boolean fsOp,
                                       String fileName) throws Exception {
    if (fsOp) {
      return bucket.createKey(fileName, 0, ReplicationType.RATIS,
        ReplicationFactor.THREE, new HashMap<>());
    } else {
      return fs.create(new Path("/", fileName));
    }
  }

  public void writeKey(ByteBuffer buffer,
                       String keyName) throws Exception {
    writeKey(isFsOp(), buffer, keyName);
  }

  public void writeKey(boolean fsOp, ByteBuffer buffer,
                       String keyName) throws Exception {
    LOG.info("LOADGEN: {} Writing key {}", fsOp, keyName);
    try (OutputStream stream = getOutputStream(fsOp, keyName)) {
      stream.write(buffer.array());
      LOG.trace("LOADGEN: Written key {}", keyName);
    } catch (Throwable t) {
      LOG.error("LOADGEN: Create key:{} failed with exception, skipping",
              keyName, t);
      throw t;
    }
  }

  // Read ops.

  private InputStream getInputStream(boolean fsOp,
                                     String fileName) throws Exception {
    if (fsOp) {
      return bucket.readKey(fileName);
    } else {
      return fs.open(new Path("/", fileName));
    }
  }

  public void readKey(ByteBuffer buffer, String keyName) throws Exception {
    readKey(isFsOp(), buffer, keyName);
  }

  public void readKey(boolean fsOp, ByteBuffer buffer,
                      String keyName) throws Exception {
    LOG.info("LOADGEN: {} Reading key {}", fsOp, keyName);

    int bufferCapacity = buffer.capacity();

    try (InputStream stream = getInputStream(fsOp, keyName)) {
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

  // Delete ops.

  private void delete(boolean fsOp, String fileName) throws IOException {
    if (fsOp) {
      bucket.deleteKey(fileName);
    } else {
      fs.delete(new Path("/", fileName), true);
    }
  }

  public void deleteKey(String keyName) throws Exception {
    deleteKey(isFsOp(), keyName);
  }

  public void deleteKey(boolean fsOp, String keyName) throws Exception {
    LOG.info("LOADGEN: {} Deleting key {}", fsOp, keyName);
    try {
      delete(fsOp, keyName);
      LOG.trace("LOADGEN: Deleted key {}", keyName);
    } catch (Throwable t) {
      LOG.error("LOADGEN: Unable to delete key:{}", keyName, t);
      throw t;
    }
  }

  private static URI getFSUri(OzoneBucket bucket) throws URISyntaxException {
    return new URI(String.format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
      bucket.getName(), bucket.getVolumeName()));
  }
}
