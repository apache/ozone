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

package org.apache.hadoop.hdds.scm.container;

import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * Wrapper for container checksums (data, metadata, etc.).
 * Provides equality, hash, and hex string rendering.
 */
@Immutable
public final class ContainerChecksums {
  // Checksum of the data within the wrapper.
  private final long dataChecksum;
  // UNSET_DATA_CHECKSUM is an internal placeholder, it should not be used outside this class.
  private static final long UNSET_DATA_CHECKSUM = -1;

  // Checksum of the metadata within the wrapper.
  private final long metadataChecksum;
  // UNSET_DATA_CHECKSUM is an internal placeholder, it should not be used outside this class.
  private static final long UNSET_METADATA_CHECKSUM = -1;

  private static final ContainerChecksums UNKNOWN =
      new ContainerChecksums(UNSET_DATA_CHECKSUM, UNSET_METADATA_CHECKSUM);

  private ContainerChecksums(long dataChecksum, long metadataChecksum) {
    this.dataChecksum = dataChecksum;
    this.metadataChecksum = metadataChecksum;
  }

  public static ContainerChecksums unknown() {
    return UNKNOWN;
  }
  
  public static ContainerChecksums of(long dataChecksum, long metadataChecksum) {
    return new ContainerChecksums(dataChecksum, metadataChecksum);
  }

  public long getDataChecksum() {
    if (needsDataChecksum()) {
      return 0;
    }
    return dataChecksum;
  }

  public boolean needsDataChecksum() {
    return dataChecksum == UNSET_DATA_CHECKSUM;
  }

  public long getMetadataChecksum() {
    if (needsMetadataChecksum()) {
      return 0;
    }
    return metadataChecksum;
  }

  public boolean needsMetadataChecksum() {
    return metadataChecksum == UNSET_METADATA_CHECKSUM;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ContainerChecksums)) {
      return false;
    }
    ContainerChecksums that = (ContainerChecksums) obj;
    return getDataChecksum() == that.getDataChecksum() &&
        getMetadataChecksum() == that.getMetadataChecksum();
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataChecksum, metadataChecksum);
  }

  @Override
  public String toString() {
    return "data=" + Long.toHexString(getDataChecksum()) +
        ", metadata=" + Long.toHexString(getMetadataChecksum());
  }
}
