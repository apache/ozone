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

/**
 * Wrapper for container checksums (data, metadata, etc.).
 * Provides equality, hash, and hex string rendering.
 */
public class ContainerChecksums {
  private final long dataChecksum;
  private final Long metadataChecksum; // nullable for future use

  public ContainerChecksums(long dataChecksum) {
    this(dataChecksum, null);
  }

  public ContainerChecksums(long dataChecksum, Long metadataChecksum) {
    this.dataChecksum = dataChecksum;
    this.metadataChecksum = metadataChecksum;
  }

  public long getDataChecksum() {
    return dataChecksum;
  }

  public Long getMetadataChecksum() {
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
           Objects.equals(metadataChecksum, that.metadataChecksum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataChecksum, metadataChecksum);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("data=").append(Long.toHexString(dataChecksum));
    if (metadataChecksum != null) {
      sb.append(", metadata=").append(Long.toHexString(metadataChecksum));
    }
    return sb.toString();
  }
} 
