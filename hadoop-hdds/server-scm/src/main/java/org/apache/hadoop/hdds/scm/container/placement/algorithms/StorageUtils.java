/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.placement.algorithms;

import com.google.common.base.Preconditions;

import java.util.function.Predicate;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.MetadataStorageReportProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;
import org.apache.hadoop.hdds.scm.node.DatanodeInfo;

/**
 * Storage/Datanode/Volume util methods.
 */
public final class StorageUtils {
  private StorageUtils() {
  }

  /**
   * Returns true if this node has enough space to meet our requirement.
   *
   * @param datanodeDetails DatanodeDetails
   * @return true if we have enough space.
   */
  public static boolean hasEnoughSpace(DatanodeDetails datanodeDetails,
                                       long metadataSizeRequired,
                                       long dataSizeRequired) {
    return hasEnoughSpace(datanodeDetails, null, null,
        metadataSizeRequired, dataSizeRequired);
  }

  public static boolean hasEnoughSpace(
      DatanodeDetails datanodeDetails,
      Predicate<StorageReportProto> dataFilter,
      Predicate<MetadataStorageReportProto> metadataFilter,
      long metadataSizeRequired, long dataSizeRequired) {
    Preconditions.checkArgument(datanodeDetails instanceof DatanodeInfo);

    boolean enoughForData = false;
    boolean enoughForMeta = false;

    DatanodeInfo datanodeInfo = (DatanodeInfo) datanodeDetails;

    if (dataSizeRequired > 0) {
      for (StorageReportProto reportProto : datanodeInfo.getStorageReports()) {
        if (reportProto.getRemaining() > dataSizeRequired
            && (dataFilter == null || dataFilter.test(reportProto))) {
          enoughForData = true;
          break;
        }
      }
    } else {
      enoughForData = true;
    }

    if (!enoughForData) {
      return false;
    }

    if (metadataSizeRequired > 0) {
      for (MetadataStorageReportProto reportProto
          : datanodeInfo.getMetadataStorageReports()) {
        if (reportProto.getRemaining() > metadataSizeRequired
            && (metadataFilter == null || metadataFilter.test(reportProto))) {
          enoughForMeta = true;
          break;
        }
      }
    } else {
      enoughForMeta = true;
    }

    return enoughForData && enoughForMeta;
  }
}
