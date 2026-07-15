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
 * A value of 0 indicates an unknown or unset checksum.
 */
@Immutable
public final class ContainerChecksums {
  // Checksum of the data within the wrapper.
  private final long dataChecksum;

  // Checksum of the metadata within the wrapper.
  private final long metadataChecksum;

  private static final ContainerChecksums UNKNOWN =
      new ContainerChecksums(0L, 0L);

  private ContainerChecksums(long dataChecksum, long metadataChecksum) {
    this.dataChecksum = dataChecksum;
    this.metadataChecksum = metadataChecksum;
  }

  public static ContainerChecksums unknown() {
    return UNKNOWN;
  }

  public static ContainerChecksums of(long dataChecksum) {
    return new ContainerChecksums(dataChecksum, 0L);
  }

  public static ContainerChecksums of(long dataChecksum, long metadataChecksum) {
    return new ContainerChecksums(dataChecksum, metadataChecksum);
  }

  public long getDataChecksum() {
    return dataChecksum;
  }

  public long getMetadataChecksum() {
    return metadataChecksum;
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
    return dataChecksum == that.dataChecksum &&
        metadataChecksum == that.metadataChecksum;
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
