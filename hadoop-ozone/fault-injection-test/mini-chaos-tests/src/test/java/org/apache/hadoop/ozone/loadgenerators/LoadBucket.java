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

package org.apache.hadoop.ozone.loadgenerators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ozone.OzoneFileSystem;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket to perform read/write & delete ops.
 */
public class LoadBucket {
  private static final Logger LOG =
            LoggerFactory.getLogger(LoadBucket.class);

  private final OzoneBucket bucket;
  private final OzoneFileSystem fs;

  public LoadBucket(OzoneBucket bucket, OzoneConfiguration conf,
      String omServiceID) throws Exception {
    this.bucket = bucket;
    if (omServiceID == null) {
      this.fs = (OzoneFileSystem) FileSystem.get(getFSUri(bucket), conf);
    } else {
      this.fs = (OzoneFileSystem) FileSystem.get(getFSUri(bucket, omServiceID),
          conf);
    }
  }

  private boolean isFsOp() {
    return RandomUtils.secure().randomBoolean();
  }

  // Write ops.
  public void writeKey(ByteBuffer buffer,
                       String keyName) throws Exception {
    writeKey(isFsOp(), buffer, keyName);
  }

  public void writeKey(boolean fsOp, ByteBuffer buffer,
                       String keyName) throws Exception {
    Op writeOp = new WriteOp(fsOp, keyName, buffer);
    writeOp.execute();
  }

  public void createDirectory(String keyName) throws Exception {
    Op dirOp = new DirectoryOp(keyName, false);
    dirOp.execute();
  }

  public void readDirectory(String keyName) throws Exception {
    Op dirOp = new DirectoryOp(keyName, true);
    dirOp.execute();
  }

  // Read ops.
  public void readKey(ByteBuffer buffer, String keyName) throws Exception {
    readKey(isFsOp(), buffer, keyName);
  }

  public void readKey(boolean fsOp, ByteBuffer buffer,
                      String keyName) throws Exception {
    Op readOp = new ReadOp(fsOp, keyName, buffer);
    readOp.execute();
  }

  // Delete ops.
  public void deleteKey(String keyName) throws Exception {
    deleteKey(isFsOp(), keyName);
  }

  public void deleteKey(boolean fsOp, String keyName) throws Exception {
    Op deleteOp = new DeleteOp(fsOp, keyName);
    deleteOp.execute();
  }

  private static URI getFSUri(OzoneBucket bucket) throws URISyntaxException {
    return new URI(String.format("%s://%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
      bucket.getName(), bucket.getVolumeName()));
  }

  private static URI getFSUri(OzoneBucket bucket, String omServiceID)
      throws URISyntaxException {
    return new URI(String.format("%s://%s.%s.%s/", OzoneConsts.OZONE_URI_SCHEME,
        bucket.getName(), bucket.getVolumeName(), omServiceID));
  }

  abstract class Op {
    private final boolean fsOp;
    private final String opName;
    private final String keyName;

    Op(boolean fsOp, String keyName) {
      this.fsOp = fsOp;
      this.keyName = keyName;
      this.opName = (fsOp ? "Filesystem" : "Bucket") + ":"
          + getClass().getSimpleName();
    }

    public void execute() throws Exception {
      LOG.info("Going to {}", this);
      try {
        if (fsOp) {
          Path p = new Path("/", keyName);
          doFsOp(p);
        } else {
          doBucketOp(keyName);
        }
        doPostOp();
        LOG.trace("Done: {}", this);
      } catch (Throwable t) {
        LOG.error("Unable to {}", this, t);
        throw t;
      }
    }

    abstract void doFsOp(Path p) throws IOException;

    abstract void doBucketOp(String key) throws IOException;

    abstract void doPostOp() throws IOException;

    @Override
    public String toString() {
      return "opType=" + opName + " keyName=" + keyName;
    }
  }

  /**
   * Create and Read Directories.
   */
  public class DirectoryOp extends Op {
    private final boolean readDir;

    DirectoryOp(String keyName, boolean readDir) {
      super(true, keyName);
      this.readDir = readDir;
    }

    @Override
    void doFsOp(Path p) throws IOException {
      if (readDir) {
        FileStatus status = fs.getFileStatus(p);
        assertTrue(status.isDirectory());
        assertEquals(p, Path.getPathWithoutSchemeAndAuthority(status.getPath()));
      } else {
        assertTrue(fs.mkdirs(p));
      }
    }

    @Override
    void doBucketOp(String key) throws IOException {
      // nothing to do here
    }

    @Override
    void doPostOp() throws IOException {
      // Nothing to do here
    }

    @Override
    public String toString() {
      return super.toString() + " "
          + (readDir ? "readDirectory" : "writeDirectory");
    }
  }

  /**
   * Write file/key to bucket.
   */
  public class WriteOp extends Op {
    private OutputStream os;
    private final ByteBuffer buffer;

    WriteOp(boolean fsOp, String keyName, ByteBuffer buffer) {
      super(fsOp, keyName);
      this.buffer = buffer;
    }

    @Override
    void doFsOp(Path p) throws IOException {
      os = fs.create(p);
    }

    @Override
    void doBucketOp(String key) throws IOException {
      os = bucket.createKey(key, 0, ReplicationType.RATIS,
          ReplicationFactor.THREE, new HashMap<>());
    }

    @Override
    void doPostOp() throws IOException {
      try {
        os.write(buffer.array());
      } finally {
        os.close();
      }
    }

    @Override
    public String toString() {
      return super.toString() + " buffer:" + buffer.limit();
    }
  }

  /**
   * Read file/key from bucket.
   */
  public class ReadOp extends Op {
    private InputStream is;
    private final ByteBuffer buffer;

    ReadOp(boolean fsOp, String keyName, ByteBuffer buffer) {
      super(fsOp, keyName);
      this.buffer = buffer;
      this.is = null;
    }

    @Override
    void doFsOp(Path p) throws IOException {
      is = fs.open(p);
    }

    @Override
    void doBucketOp(String key) throws IOException {
      is = bucket.readKey(key);
    }

    @Override
    void doPostOp() throws IOException {
      int bufferCapacity = buffer.capacity();
      try {
        byte[] readBuffer = new byte[bufferCapacity];
        int readLen = is.read(readBuffer);

        if (readLen < bufferCapacity) {
          throw new IOException("Read mismatch, " +
              " read data length:" + readLen + " is smaller than excepted:"
              + bufferCapacity);
        }

        if (!Arrays.equals(readBuffer, buffer.array())) {
          throw new IOException("Read mismatch," +
              " read data does not match the written data");
        }
      } finally {
        is.close();
      }
    }

    @Override
    public String toString() {
      return super.toString() + " buffer:" + buffer.limit();
    }
  }

  /**
   * Delete file/key from bucket.
   */
  public class DeleteOp extends Op {
    DeleteOp(boolean fsOp, String keyName) {
      super(fsOp, keyName);
    }

    @Override
    void doFsOp(Path p) throws IOException {
      fs.delete(p, true);
    }

    @Override
    void doBucketOp(String key) throws IOException {
      bucket.deleteKey(key);
    }

    @Override
    void doPostOp() {
      // Nothing to do here
    }

    @Override
    public String toString() {
      return super.toString();
    }
  }
}
