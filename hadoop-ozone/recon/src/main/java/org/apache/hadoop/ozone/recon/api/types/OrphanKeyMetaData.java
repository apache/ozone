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

package org.apache.hadoop.ozone.recon.api.types;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.OrphanKeyMetaDataProto;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Holds orphan key metadata containing object id of such key/file.
 *
 */
public class OrphanKeyMetaData {
  private Set<Long> objectIds;
  private Long status;
  private String volumeName;
  private String bucketName;

  public OrphanKeyMetaData(Set<Long> objectIds, Long status, String volumeName,
                           String bucketName) {
    this.objectIds = objectIds;
    this.status = status;
    this.volumeName = volumeName;
    this.bucketName = bucketName;
  }

  public Set<Long> getObjectIds() {
    return objectIds;
  }

  public void setObjectIds(Set<Long> objectIds) {
    this.objectIds = objectIds;
  }

  public Long getStatus() {
    return status;
  }

  public void setStatus(Long status) {
    this.status = status;
  }

  public String getVolumeName() {
    return volumeName;
  }

  public void setVolumeName(String volumeName) {
    this.volumeName = volumeName;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public static OrphanKeyMetaData fromProto(
      OrphanKeyMetaDataProto proto) {
    return new OrphanKeyMetaData(proto.getObjectIdList().stream().collect(
        Collectors.toSet()), proto.getStatus(),
        proto.getVolumeName(), proto.getBucketName());
  }

  public OrphanKeyMetaDataProto toProto() {
    return OrphanKeyMetaDataProto.newBuilder().addAllObjectId(objectIds)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStatus(status).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OrphanKeyMetaData that = (OrphanKeyMetaData) o;
    return Objects.equals(objectIds, that.objectIds) &&
        Objects.equals(status, that.status) &&
        Objects.equals(volumeName, that.volumeName) &&
        Objects.equals(bucketName, that.bucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(objectIds, status, volumeName, bucketName);
  }
}
