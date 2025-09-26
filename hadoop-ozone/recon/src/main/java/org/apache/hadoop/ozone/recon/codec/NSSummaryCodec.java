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

import jakarta.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.CodecException;
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
  public boolean supportCodecBuffer() {
    return true;
  }

  @Override
  public CodecBuffer toCodecBuffer(@Nonnull NSSummary object, CodecBuffer.Allocator allocator) throws CodecException {
    try {
      Set<Long> childDirs = object.getChildDir();
      int numOfChildDirs = childDirs.size();
      String dirName = object.getDirName();

      CodecBuffer dirNameBuffer = stringCodec.toCodecBuffer(dirName, allocator);
      int dirNameSize = dirNameBuffer.readableBytes();

      // total size: primitives + childDirs + dirName length + dirName data
      final int totalSize = Integer.BYTES * (3 + ReconConstants.NUM_OF_FILE_SIZE_BINS) // numFiles + numOfChildDirs + dirNameSize + fileSizeBucket
          + Long.BYTES * (numOfChildDirs + 2) // childDirs + sizeOfFiles + parentId
          + Short.BYTES // fileSizeBucket length
          + dirNameSize; // actual dirName bytes

      CodecBuffer buffer = allocator.apply(totalSize);

      buffer.putInt(object.getNumOfFiles());
      buffer.putLong(object.getSizeOfFiles());
      buffer.putShort((short) ReconConstants.NUM_OF_FILE_SIZE_BINS);

      int[] fileSizeBucket = object.getFileSizeBucket();
      for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; ++i) {
        buffer.putInt(fileSizeBucket[i]);
      }

      buffer.putInt(numOfChildDirs);
      for (long childDirId : childDirs) {
        buffer.putLong(childDirId);
      }

      buffer.putInt(dirNameSize);
      if (dirNameSize > 0) {
        buffer.put(dirNameBuffer.asReadOnlyByteBuffer());
      }
      buffer.putLong(object.getParentId());

      return buffer;
    } catch (Exception e) {
      throw new CodecException("Failed to encode NSSummary to CodecBuffer", e);
    }
  }

  @Override
  public NSSummary fromCodecBuffer(@Nonnull CodecBuffer buffer) throws CodecException {
    try {
      ByteBuffer byteBuffer = buffer.asReadOnlyByteBuffer();
      NSSummary result = new NSSummary();

      result.setNumOfFiles(byteBuffer.getInt());
      result.setSizeOfFiles(byteBuffer.getLong());

      short bucketLength = byteBuffer.getShort();
      assert (bucketLength == (short) ReconConstants.NUM_OF_FILE_SIZE_BINS);

      int[] fileSizeBucket = new int[bucketLength];
      for (int i = 0; i < bucketLength; ++i) {
        fileSizeBucket[i] = byteBuffer.getInt();
      }
      result.setFileSizeBucket(fileSizeBucket);

      int numChildDirs = byteBuffer.getInt();
      Set<Long> childDirs = new LinkedHashSet<>(numChildDirs);
      for (int i = 0; i < numChildDirs; ++i) {
        childDirs.add(byteBuffer.getLong());
      }
      result.setChildDir(childDirs);

      int dirNameSize = byteBuffer.getInt();
      if (dirNameSize == 0) {
        return result;
      }

      byte[] dirNameBytes = new byte[dirNameSize];
      byteBuffer.get(dirNameBytes);
      CodecBuffer dirNameBuffer = CodecBuffer.wrap(dirNameBytes);
      String dirName = stringCodec.fromCodecBuffer(dirNameBuffer);
      result.setDirName(dirName);
      result.setParentId(byteBuffer.getLong());

      return result;
    } catch (Exception e) {
      throw new CodecException("Failed to decode NSSummary from CodecBuffer", e);
    }
  }

  @Override
  public byte[] toPersistedFormatImpl(NSSummary object) throws IOException {
    final byte[] dirName = stringCodec.toPersistedFormat(object.getDirName());
    Set<Long> childDirs = object.getChildDir();
    int numOfChildDirs = childDirs.size();

    // int: 1 field (numOfFiles) + 2 sizes (childDirs, dirName) + NUM_OF_FILE_SIZE_BINS (fileSizeBucket)
    final int resSize = (3 + ReconConstants.NUM_OF_FILE_SIZE_BINS) * Integer.BYTES
        + (numOfChildDirs + 1) * Long.BYTES // 1 long field for parentId + list size
        + Short.BYTES // 2 dummy shorts to track length
        + dirName.length // directory name length
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
    short len = in.readShort();
    assert (len == (short) ReconConstants.NUM_OF_FILE_SIZE_BINS);
    int[] fileSizeBucket = new int[len];
    for (int i = 0; i < len; ++i) {
      fileSizeBucket[i] = in.readInt();
    }
    res.setFileSizeBucket(fileSizeBucket);

    int listSize = in.readInt();
    Set<Long> childDir = new LinkedHashSet<>();
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
