/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.om.helpers;

import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * BucketType enum
 * We have 3 types of buckets - FSO, OBJECT_STORE, and LEGACY
 * LEGACY is used to represent the old buckets which are already
 * present in DB while user can create new buckets as FSO or OBJECT_STORE.
 */
public enum BucketType {
  FILE_SYSTEM_OPTIMIZED, OBJECT_STORE, LEGACY;
  public static final BucketType DEFAULT = OBJECT_STORE;
  public static BucketType fromProto(
      OzoneManagerProtocolProtos.BucketTypeProto bucketType) {
    if (bucketType == null) {
      return BucketType.LEGACY;
    }
    switch (bucketType) {
    case FILE_SYSTEM_OPTIMIZED:
      return BucketType.FILE_SYSTEM_OPTIMIZED;
    case LEGACY:
      return BucketType.LEGACY;
    case OBJECT_STORE:
      return BucketType.OBJECT_STORE;
    default:
      return DEFAULT;
    }
  }

  public OzoneManagerProtocolProtos.BucketTypeProto toProto() {
    switch (this) {
    case FILE_SYSTEM_OPTIMIZED:
      return OzoneManagerProtocolProtos.BucketTypeProto.FILE_SYSTEM_OPTIMIZED;
    case OBJECT_STORE:
      return OzoneManagerProtocolProtos.BucketTypeProto.OBJECT_STORE;
    case LEGACY:
      return OzoneManagerProtocolProtos.BucketTypeProto.LEGACY;
    default:
      throw new IllegalArgumentException(
          "Error: BucketType not found, type=" + this);
    }
  }
}
