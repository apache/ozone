/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.codec;

import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.ShortCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;
import org.apache.hadoop.hdds.utils.db.Codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * Codec to serialize/deserialize {@link NSSummary}.
 */
public final class NSSummaryCodec implements Codec<NSSummary> {

  private static final Codec<NSSummary> INSTANCE = new NSSummaryCodec();

  public static Codec<NSSummary> get() {
    return INSTANCE;
  }

  private final Codec<Integer> integerCodec = IntegerCodec.get();
  private final Codec<Short> shortCodec = ShortCodec.get();
  private final Codec<Long> longCodec = LongCodec.get();
  private final Codec<String> stringCodec = StringCodec.get();
  // 1 int fields + 41-length int array
  // + 2 dummy field to track list size/dirName length
  private static final int NUM_OF_INTS =
      3 + ReconConstants.NUM_OF_FILE_SIZE_BINS;

  private NSSummaryCodec() {
    // singleton
  }

  @Override
  public byte[] toPersistedFormat(NSSummary object) throws IOException {
    Set<Long> childDirs = object.getChildDir();
    String dirName = object.getDirName();
    int stringLen = dirName.getBytes(StandardCharsets.UTF_8).length;
    int numOfChildDirs = childDirs.size();
    final int resSize = NUM_OF_INTS * Integer.BYTES
        + (numOfChildDirs + 1) * Long.BYTES // 1 long field for parentId + list size
        + Short.BYTES // 2 dummy shorts to track length
        + stringLen // directory name length
        + Long.BYTES; // Added space for parentId serialization

    ByteArrayOutputStream out = new ByteArrayOutputStream(resSize);
    out.write(integerCodec.toPersistedFormat(object.getNumOfFiles()));
    out.write(longCodec.toPersistedFormat(object.getSizeOfFiles()));
    out.write(shortCodec.toPersistedFormat(
        (short) ReconConstants.NUM_OF_FILE_SIZE_BINS));
    int[] fileSizeBucket = object.getFileSizeBucket();
    for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
      out.write(integerCodec.toPersistedFormat(fileSizeBucket[i]));
    }
    out.write(integerCodec.toPersistedFormat(numOfChildDirs));
    for (long childDirId : childDirs) {
      out.write(longCodec.toPersistedFormat(childDirId));
    }
    out.write(integerCodec.toPersistedFormat(stringLen));
    out.write(stringCodec.toPersistedFormat(dirName));
    out.write(longCodec.toPersistedFormat(object.getParentId()));

    return out.toByteArray();
  }

  @Override
  public NSSummary fromPersistedFormat(byte[] rawData) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(rawData));
    NSSummary res = new NSSummary();
    res.setNumOfFiles(in.readInt());
    res.setSizeOfFiles(in.readLong());
    short len = in.readShort();
    assert (len == (short) ReconConstants.NUM_OF_FILE_SIZE_BINS);
    int[] fileSizeBucket = new int[len];
    for (int i = 0; i < len; ++i) {
      fileSizeBucket[i] = in.readInt();
    }
    res.setFileSizeBucket(fileSizeBucket);

    int listSize = in.readInt();
    Set<Long> childDir = new HashSet<>();
    for (int i = 0; i < listSize; ++i) {
      childDir.add(in.readLong());
    }
    res.setChildDir(childDir);

    int strLen = in.readInt();
    if (strLen == 0) {
      return res;
    }
    byte[] buffer = new byte[strLen];
    int bytesRead = in.read(buffer);
    assert (bytesRead == strLen);
    String dirName = stringCodec.fromPersistedFormat(buffer);
    res.setDirName(dirName);

    // Check if there is enough data available to read the parentId
    if (in.available() >= Long.BYTES) {
      long parentId = in.readLong();
      res.setParentId(parentId);
    } else {
      // Set default parentId to -1 indicating it's from old format
      res.setParentId(-1);
    }
    return res;
  }

  @Override
  public NSSummary copyObject(NSSummary object) {
    NSSummary copy = new NSSummary();
    copy.setNumOfFiles(object.getNumOfFiles());
    copy.setSizeOfFiles(object.getSizeOfFiles());
    copy.setFileSizeBucket(object.getFileSizeBucket());
    copy.setChildDir(object.getChildDir());
    copy.setDirName(object.getDirName());
    copy.setParentId(object.getParentId());
    return copy;
  }
}
