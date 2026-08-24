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

package org.apache.hadoop.hdds.client;

import jakarta.annotation.Nonnull;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;

/**
 * Utility class for converting between Hadoop's {@link StorageType} and
 * Ozone's protobuf representation {@link HddsProtos.StorageTypeProto}.
 */
public final class StorageTypeUtils {
  private StorageTypeUtils() {
  }

  public static HddsProtos.StorageTypeProto getStorageTypeProto(@Nonnull StorageType type)
      throws IllegalArgumentException {
    switch (type) {
    case SSD:
      return HddsProtos.StorageTypeProto.SSD;
    case DISK:
      return HddsProtos.StorageTypeProto.DISK;
    case ARCHIVE:
      return HddsProtos.StorageTypeProto.ARCHIVE;
    case PROVIDED:
      return HddsProtos.StorageTypeProto.PROVIDED;
    case RAM_DISK:
      return HddsProtos.StorageTypeProto.RAM_DISK;
    case NVDIMM:
      return HddsProtos.StorageTypeProto.NVDIMM;
    default:
      throw new IllegalArgumentException("Illegal Storage Type specified");
    }
  }

  public static StorageType getFromProtobuf(@Nonnull HddsProtos.StorageTypeProto proto) throws
      IllegalArgumentException {
    switch (proto) {
    case SSD:
      return StorageType.SSD;
    case DISK:
      return StorageType.DISK;
    case ARCHIVE:
      return StorageType.ARCHIVE;
    case PROVIDED:
      return StorageType.PROVIDED;
    case RAM_DISK:
      return StorageType.RAM_DISK;
    case NVDIMM:
      return StorageType.NVDIMM;
    default:
      throw new IllegalArgumentException("Illegal Storage Type specified");
    }
  }

  /**
   * Returns Filesystem StorageType enum value corresponding to the int ID value.
   *
   * @param storageTypeID StorageType int ID value
   * @return StorageType
   */
  public static StorageType getStorageTypeFromID(int storageTypeID) throws
      IllegalArgumentException {
    if (StorageTypeProto.forNumber(storageTypeID) == null) {
      throw new IllegalArgumentException("Illegal storageTypeID " + storageTypeID);
    }
    return getFromProtobuf(StorageTypeProto.forNumber(storageTypeID));
  }

  /**
   * Returns integer representation of protobuf StorageType.
   *
   * @return storageType int ID value
   */
  public static int getIDFromProtobuf(@Nonnull HddsProtos.StorageTypeProto proto) throws
      IllegalArgumentException {
    switch (proto) {
    case SSD:
    case DISK:
    case ARCHIVE:
    case PROVIDED:
    case RAM_DISK:
    case NVDIMM:
      return proto.getNumber();
    default:
      throw new IllegalArgumentException("Illegal Storage Type specified");
    }
  }
}

