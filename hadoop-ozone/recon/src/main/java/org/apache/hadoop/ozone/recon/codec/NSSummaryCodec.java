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

package org.apache.hadoop.ozone.recon.codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.IntegerCodec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.hdds.utils.db.ShortCodec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;

/**
 * Codec to serialize/deserialize {@link NSSummary}.
 */
public final class NSSummaryCodec implements Codec<NSSummary> {

  private static final Codec<NSSummary> INSTANCE = new NSSummaryCodec();

  private final Codec<Integer> integerCodec = IntegerCodec.get();
  private final Codec<Short> shortCodec = ShortCodec.get();
  private final Codec<Long> longCodec = LongCodec.get();
  private final Codec<String> stringCodec = StringCodec.get();

  private NSSummaryCodec() {
    // singleton
  }

  public static Codec<NSSummary> get() {
    return INSTANCE;
  }

  @Override
  public Class<NSSummary> getTypeClass() {
    return NSSummary.class;
  }

  @Override
  public byte[] toPersistedFormatImpl(NSSummary object) throws IOException {
    final byte[] dirName = stringCodec.toPersistedFormat(object.getDirName());
    Set<Long> childDirs = object.getChildDir();
    Set<Long> deletedChildDirs = object.getDeletedChildDir();
    int numOfChildDirs = childDirs.size();
    int numOfDeletedChildDirs = deletedChildDirs.size();

    // Calculate size: existing fields + new deleted fields
    final int resSize = (6 + ReconConstants.NUM_OF_FILE_SIZE_BINS) * Integer.BYTES
        + (numOfChildDirs + numOfDeletedChildDirs + 4) * Long.BYTES // sizes, parentId, and size fields
        + Short.BYTES
        + dirName.length;

    ByteArrayOutputStream out = new ByteArrayOutputStream(resSize);
    out.write(integerCodec.toPersistedFormat(object.getNumOfFiles()));
    out.write(longCodec.toPersistedFormat(object.getSizeOfFiles()));
    out.write(integerCodec.toPersistedFormat(object.getNumOfDeletedFiles()));
    out.write(longCodec.toPersistedFormat(object.getSizeOfDeletedFiles()));
    out.write(integerCodec.toPersistedFormat(object.getNumOfDeletedDirs()));
    out.write(longCodec.toPersistedFormat(object.getSizeOfDeletedDirs()));
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
    out.write(integerCodec.toPersistedFormat(numOfDeletedChildDirs));
    for (long deletedChildDirId : deletedChildDirs) {
      out.write(longCodec.toPersistedFormat(deletedChildDirId));
    }
    out.write(integerCodec.toPersistedFormat(dirName.length));
    out.write(dirName);
    out.write(longCodec.toPersistedFormat(object.getParentId()));

    return out.toByteArray();
  }

  @Override
  public NSSummary fromPersistedFormatImpl(byte[] rawData) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(rawData));
    NSSummary res = new NSSummary();
    res.setNumOfFiles(in.readInt());
    res.setSizeOfFiles(in.readLong());
    
    // Try to read new fields, if they exist
    try {
      res.setNumOfDeletedFiles(in.readInt());
      res.setSizeOfDeletedFiles(in.readLong());
      res.setNumOfDeletedDirs(in.readInt());
      res.setSizeOfDeletedDirs(in.readLong());
    } catch (IOException e) {
      // If reading fails, these are old format records
      res.setNumOfDeletedFiles(0);
      res.setSizeOfDeletedFiles(0);
      res.setNumOfDeletedDirs(0);
      res.setSizeOfDeletedDirs(0);
      // Reset stream position
      in = new DataInputStream(new ByteArrayInputStream(rawData));
      in.readInt(); // numOfFiles
      in.readLong(); // sizeOfFiles
    }
    
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

    // Try to read deleted child directories
    try {
      int deletedListSize = in.readInt();
      Set<Long> deletedChildDir = new HashSet<>();
      for (int i = 0; i < deletedListSize; ++i) {
        deletedChildDir.add(in.readLong());
      }
      res.setDeletedChildDir(deletedChildDir);
    } catch (IOException e) {
      // If reading fails, this is old format
      res.setDeletedChildDir(new HashSet<>());
    }

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
    copy.setNumOfDeletedFiles(object.getNumOfDeletedFiles());
    copy.setSizeOfDeletedFiles(object.getSizeOfDeletedFiles());
    copy.setNumOfDeletedDirs(object.getNumOfDeletedDirs());
    copy.setSizeOfDeletedDirs(object.getSizeOfDeletedDirs());
    copy.setFileSizeBucket(object.getFileSizeBucket());
    copy.setChildDir(new HashSet<>(object.getChildDir()));
    copy.setDeletedChildDir(new HashSet<>(object.getDeletedChildDir()));
    copy.setDirName(object.getDirName());
    copy.setParentId(object.getParentId());
    return copy;
  }
}
