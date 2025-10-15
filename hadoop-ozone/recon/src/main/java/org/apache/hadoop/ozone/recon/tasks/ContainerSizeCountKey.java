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

package org.apache.hadoop.ozone.recon.tasks;

import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.storage.proto.OzoneManagerStorageProtos.ContainerSizeCountKeyProto;

/**
 * Key class used for grouping container size counts in RocksDB storage.
 * Represents a key of containerSizeUpperBound for CONTAINER_COUNT_BY_SIZE column family.
 */
public class ContainerSizeCountKey {
  private static final Codec<ContainerSizeCountKey> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(ContainerSizeCountKeyProto.getDefaultInstance()),
      ContainerSizeCountKey::fromProto,
      ContainerSizeCountKey::toProto,
      ContainerSizeCountKey.class);

  private final Long containerSizeUpperBound;

  public ContainerSizeCountKey(Long containerSizeUpperBound) {
    this.containerSizeUpperBound = containerSizeUpperBound;
  }

  public static Codec<ContainerSizeCountKey> getCodec() {
    return CODEC;
  }

  public Long getContainerSizeUpperBound() {
    return containerSizeUpperBound;
  }

  public ContainerSizeCountKeyProto toProto() {
    return ContainerSizeCountKeyProto.newBuilder()
        .setContainerSizeUpperBound(containerSizeUpperBound)
        .build();
  }

  public static ContainerSizeCountKey fromProto(ContainerSizeCountKeyProto proto) {
    return new ContainerSizeCountKey(proto.getContainerSizeUpperBound());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ContainerSizeCountKey that = (ContainerSizeCountKey) o;
    return containerSizeUpperBound.equals(that.containerSizeUpperBound);
  }

  @Override
  public int hashCode() {
    return containerSizeUpperBound.hashCode();
  }

  @Override
  public String toString() {
    return "ContainerSizeCountKey{" +
        "containerSizeUpperBound=" + containerSizeUpperBound +
        '}';
  }
}
