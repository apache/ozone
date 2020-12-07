/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.recon.codec;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.LongCodec;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaWithTimestamp;
import org.apache.hadoop.ozone.recon.scm.ContainerReplicaWithTimestampList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Codec for ContainerReplicaWithTimestampList.
 */
public class ContainerReplicaWithTimestampListCodec
    implements Codec<ContainerReplicaWithTimestampList> {

  // UUID takes 2 long to store. Each timestamp takes 1 long.
  static final int SIZE_PER_ENTRY = 4 * Long.BYTES;
  private final Codec<Long> lc = new LongCodec();

  @Override
  public byte[] toPersistedFormat(ContainerReplicaWithTimestampList obj)
      throws IOException {

    List<ContainerReplicaWithTimestamp> lst = obj.getList();
    final int sizeOfRes = SIZE_PER_ENTRY * lst.size();
    // ByteArrayOutputStream constructor has a sanity check on size.
    ByteArrayOutputStream out = new ByteArrayOutputStream(sizeOfRes);
    for (ContainerReplicaWithTimestamp ts : lst) {
      out.write(lc.toPersistedFormat(ts.getId().getMostSignificantBits()));
      out.write(lc.toPersistedFormat(ts.getId().getLeastSignificantBits()));
      out.write(lc.toPersistedFormat(ts.getFirstSeenTime()));
      out.write(lc.toPersistedFormat(ts.getLastSeenTime()));
    }
    return out.toByteArray();
  }

  @Override
  public ContainerReplicaWithTimestampList fromPersistedFormat(byte[] rawData)
      throws IOException {

    assert(rawData.length % SIZE_PER_ENTRY == 0);
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(rawData));
    List<ContainerReplicaWithTimestamp> lst = new ArrayList<>();
    while (in.available() > 0) {
      final long uuid_msb = in.readLong();
      final long uuid_lsb = in.readLong();
      final long firstSeenTime = in.readLong();
      final long lastSeenTime = in.readLong();
      final UUID id = new UUID(uuid_msb, uuid_lsb);
      lst.add(new ContainerReplicaWithTimestamp(
          id, firstSeenTime, lastSeenTime, null));
    }
    in.close();
    return new ContainerReplicaWithTimestampList(lst);
  }

  @Override
  public ContainerReplicaWithTimestampList copyObject(
      ContainerReplicaWithTimestampList obj) {
    return obj;
  }
}
