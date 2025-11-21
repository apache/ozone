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
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.StringCodec;
import org.apache.hadoop.ozone.recon.ReconConstants;
import org.apache.hadoop.ozone.recon.api.types.NSSummary;

/**
 * Codec to serialize/deserialize {@link NSSummary}.
 */
public final class NSSummaryCodec implements Codec<NSSummary> {

  private static final Codec<NSSummary> INSTANCE = new NSSummaryCodec();
  private final Codec<String> stringCodec = StringCodec.get();

  private NSSummaryCodec() {
  }

  public static Codec<NSSummary> get() {
    return INSTANCE;
  }

  @Override
  public Class<NSSummary> getTypeClass() {
    return NSSummary.class;
  }

  @Override
  public byte[] toPersistedFormatImpl(NSSummary o) throws IOException {
    // Pre-size buffer conservatively to reduce BAOS growth
    final byte[] dirNameBytes = stringCodec.toPersistedFormat(o.getDirName());
    final int[] buckets = o.getFileSizeBucket(); // returns a COPY (safe)
    final Set<Long> childDirs = o.getChildDir();
    final int numChild = childDirs.size();

    // Rough capacity: headers + buckets + child list + dir bytes + parentId + replSize
    final int cap =
        Integer.BYTES            // numOfFiles
            + Long.BYTES               // sizeOfFiles
            + Short.BYTES              // bucket len
            + (ReconConstants.NUM_OF_FILE_SIZE_BINS * Integer.BYTES)
            + Integer.BYTES            // child list size
            + (numChild * Long.BYTES)  // child ids
            + Integer.BYTES            // dir name len
            + dirNameBytes.length      // dir name
            + Long.BYTES               // parentId
            + Long.BYTES;              // replicatedSizeOfFiles

    ByteArrayOutputStream baos = new ByteArrayOutputStream(cap);
    DataOutputStream out = new DataOutputStream(baos);

    // totals
    out.writeInt(o.getNumOfFiles());
    out.writeLong(o.getSizeOfFiles());

    // buckets
    out.writeShort((short) ReconConstants.NUM_OF_FILE_SIZE_BINS);
    for (int i = 0; i < ReconConstants.NUM_OF_FILE_SIZE_BINS; i++) {
      out.writeInt(buckets[i]);
    }

    // children
    out.writeInt(numChild);
    if (numChild > 0) {
      for (long id : childDirs) {
        out.writeLong(id);
      }
    }

    // dir name
    out.writeInt(dirNameBytes.length);
    if (dirNameBytes.length > 0) {
      out.write(dirNameBytes);
    }

    // parent + replicated totals
    out.writeLong(o.getParentId());
    out.writeLong(o.getReplicatedSizeOfFiles());

    out.flush();
    return baos.toByteArray();
  }

  @Override
  public NSSummary fromPersistedFormatImpl(byte[] raw) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(raw));
    NSSummary res = new NSSummary();

    // totals
    res.setNumOfFiles(in.readInt());
    res.setSizeOfFiles(in.readLong());

    // buckets
    short len = in.readShort();
    if (len != (short) ReconConstants.NUM_OF_FILE_SIZE_BINS) {
      // tolerate older/newer sizes by reading min(len, expected) and discarding/zero-filling rest
      int[] buf = new int[len];
      for (int i = 0; i < len; i++) {
        buf[i] = in.readInt();
      }
      int[] normalized = new int[ReconConstants.NUM_OF_FILE_SIZE_BINS];
      System.arraycopy(buf, 0, normalized, 0, Math.min(buf.length, normalized.length));
      res.setFileSizeBucket(normalized);
    } else {
      int[] bucket = new int[len];
      for (int i = 0; i < len; i++) {
        bucket[i] = in.readInt();
      }
      res.setFileSizeBucket(bucket);
    }

    // children
    int listSize = in.readInt();
    if (listSize > 0) {
      Set<Long> childDir = new HashSet<>(Math.max(16, listSize * 2));
      for (int i = 0; i < listSize; i++) {
        childDir.add(in.readLong());
      }
      res.setChildDir(childDir);
    } else {
      res.setChildDir(null); // keep lazy/empty
    }

    // dir name
    int strLen = in.readInt();
    if (strLen > 0) {
      byte[] nameBytes = new byte[strLen];
      int read = in.read(nameBytes);
      if (read != strLen) {
        throw new IOException("Corrupt NSSummary: expected " + strLen + " bytes, got " + read);
      }
      res.setDirName(stringCodec.fromPersistedFormat(nameBytes));
    } else {
      res.setDirName("");
    }

    // parentId + replicatedSizeOfFiles (handle legacy rows)
    readParentIdAndReplicatedSize(in, res);
    return res;
  }

  @Override
  public NSSummary copyObject(NSSummary o) {
    // Deep copy the mutable parts; public getters are safe (perform copies where needed)
    NSSummary c = new NSSummary(
        o.getNumOfFiles(),
        o.getSizeOfFiles(),
        o.getReplicatedSizeOfFiles(),
        o.getFileSizeBucket(),
        o.getChildDir(),
        o.getDirName(),
        o.getParentId()
    );
    return c;
  }

  private static void readParentIdAndReplicatedSize(DataInputStream in, NSSummary out)
      throws IOException {
    // Legacy tolerance: these may be absent in older rows.
    int avail = in.available();
    if (avail >= Long.BYTES * 2) {
      out.setParentId(in.readLong());
      out.setReplicatedSizeOfFiles(in.readLong());
    } else if (avail >= Long.BYTES) {
      out.setParentId(in.readLong());
      out.setReplicatedSizeOfFiles(-1L);
    } else {
      out.setParentId(-1L);
      out.setReplicatedSizeOfFiles(-1L);
    }
  }
}
