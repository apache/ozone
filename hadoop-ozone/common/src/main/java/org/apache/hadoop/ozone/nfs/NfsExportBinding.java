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

package org.apache.hadoop.ozone.nfs;

import java.util.Objects;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.om.exceptions.OMException;

/** Persistent export configuration. A binding validates identity, not caller authorization. */
public final class NfsExportBinding {
  private final UUID exportId;
  private final String clusterId;
  private final String volumeName;
  private final String bucketName;
  private final long volumeId;
  private final long bucketId;

  public NfsExportBinding(UUID exportId, String clusterId, String volumeName, String bucketName,
      long volumeId, long bucketId) {
    this.exportId = Objects.requireNonNull(exportId, "exportId");
    this.clusterId = requireName(clusterId, "clusterId");
    this.volumeName = requireName(volumeName, "volumeName");
    this.bucketName = requireName(bucketName, "bucketName");
    this.volumeId = volumeId;
    this.bucketId = bucketId;
  }

  /**
   * Load an operator-persisted binding. Every property is mandatory; startup must never generate a replacement UUID.
   * Object IDs use unsigned decimal notation to preserve all 64 bits.
   */
  public static NfsExportBinding fromConfiguration(ConfigurationSource configuration, String exportName) {
    Objects.requireNonNull(configuration, "configuration");
    if (exportName == null || !exportName.matches("[A-Za-z0-9_-]+")) {
      throw new IllegalArgumentException("Invalid NFS export configuration name");
    }
    String prefix = "ozone.nfs.export." + exportName + ".";
    String uuid = requiredProperty(configuration, prefix + "uuid");
    UUID exportId = UUID.fromString(uuid);
    if (!exportId.toString().equalsIgnoreCase(uuid)) {
      throw new IllegalArgumentException(prefix + "uuid must use the canonical UUID format");
    }
    return new NfsExportBinding(exportId, requiredProperty(configuration, prefix + "cluster.id"),
        requiredProperty(configuration, prefix + "volume"), requiredProperty(configuration, prefix + "bucket"),
        Long.parseUnsignedLong(requiredProperty(configuration, prefix + "volume.id")),
        Long.parseUnsignedLong(requiredProperty(configuration, prefix + "bucket.id")));
  }

  private static String requiredProperty(ConfigurationSource configuration, String key) {
    String value = configuration.getTrimmed(key);
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException("Missing NFS export property: " + key);
    }
    return value;
  }

  private static String requireName(String value, String name) {
    Objects.requireNonNull(value, name);
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(name + " must not be empty");
    }
    return value;
  }

  public UUID getExportId() {
    return exportId;
  }

  public String getClusterId() {
    return clusterId;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public long getVolumeId() {
    return volumeId;
  }

  public long getBucketId() {
    return bucketId;
  }

  public NfsFileHandle getRootHandle() {
    return new NfsFileHandle(exportId, volumeId, bucketId, bucketId, NfsFileHandle.Kind.BUCKET);
  }

  /**
   * Validate before calling OM. The connected cluster ID must come from trusted backend discovery,
   * never from a request. OM must still validate the live incarnation and authorize the caller.
   */
  public void validateHandle(String connectedClusterId, NfsFileHandle handle) throws OMException {
    Objects.requireNonNull(handle, "handle");
    if (!clusterId.equals(connectedClusterId) || !exportId.equals(handle.getExportId())
        || volumeId != handle.getVolumeId() || bucketId != handle.getBucketId()
        || (handle.getKind() == NfsFileHandle.Kind.BUCKET) != (handle.getObjectId() == bucketId)) {
      throw new OMException("NFS handle does not match the export binding", OMException.ResultCodes.FILE_NOT_FOUND);
    }
  }
}
