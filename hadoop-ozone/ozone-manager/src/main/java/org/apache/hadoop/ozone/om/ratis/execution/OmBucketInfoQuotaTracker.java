/**
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
package org.apache.hadoop.ozone.om.ratis.execution;


import com.google.common.base.Preconditions;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;

/**
 * A class that encapsulates Bucket Info.
 */
public final class OmBucketInfoQuotaTracker extends OmBucketInfo {
  private BucketQuotaResource.BucketQuota resource;
  private long incUsedBytes;
  private long incUsedNamespace;

  public static OmBucketInfoQuotaTracker convert(OmBucketInfo info) {
    Builder builder = info.toBuilder();
    return createObject(info, builder);
  }

  private OmBucketInfoQuotaTracker(Builder b) {
    super(b);
    resource = BucketQuotaResource.instance().get(b.getObjectID());
  }

  @Override
  public void incrUsedBytes(long bytes) {
    incUsedBytes += bytes;
    resource.addUsedBytes(bytes);
  }

  @Override
  public void incrUsedNamespace(long namespaceToUse) {
    incUsedNamespace += namespaceToUse;
    resource.addUsedNamespace(namespaceToUse);
  }

  @Override
  public long getUsedBytes() {
    return resource.getUsedBytes() + super.getUsedBytes();
  }

  @Override
  public long getUsedNamespace() {
    return resource.getUsedNamespace() + super.getUsedNamespace();
  }

  public long getIncUsedBytes() {
    return incUsedBytes;
  }
  
  public long getIncUsedNamespace() {
    return incUsedNamespace;
  }

  /**
   * reset resource used bytes as bucket info in db and cache is updated on flush.
   */
  public void reset() {
    resource.addUsedBytes(-incUsedBytes);
    resource.addUsedNamespace(-incUsedNamespace);
  }

  @Override
  public OmBucketInfoQuotaTracker copyObject() {
    Builder builder = toBuilder();
    return createObject(this, builder);
  }

  private static OmBucketInfoQuotaTracker createObject(OmBucketInfo info, Builder builder) {
    if (info.getEncryptionKeyInfo() != null) {
      builder.setBucketEncryptionKey(info.getEncryptionKeyInfo().copy());
    }

    if (info.getDefaultReplicationConfig() != null) {
      builder.setDefaultReplicationConfig(info.getDefaultReplicationConfig().copy());
    }

    Preconditions.checkNotNull(info.getVolumeName());
    Preconditions.checkNotNull(info.getBucketName());
    Preconditions.checkNotNull(info.getAcls());
    Preconditions.checkNotNull(info.getStorageType());
    return new OmBucketInfoQuotaTracker(builder);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
