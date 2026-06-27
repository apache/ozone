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

package org.apache.hadoop.hdds.scm.node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdds.client.StorageTier;
import org.apache.hadoop.hdds.client.StorageTierUtil;
import org.apache.hadoop.hdds.client.StorageTypeUtils;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.StorageTypeProto;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageReportProto;

/**
 * Util class for Node operations.
 */
public final class NodeUtils {

  private NodeUtils() {
  }

  public static List<StorageTier> getDatanodesStorageTypes(
      List<DatanodeDetails> dns, NodeManager nodeManager) {
    List<Set<StorageType>> dnStorageTypes = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      DatanodeInfo datanodeInfo = nodeManager.getDatanodeInfo(dn);
      if (datanodeInfo == null) {
        throw new IllegalStateException("Cannot get Datanode : " + dn.getUuidString() + " Info");
      }
      dnStorageTypes.add(getDatanodeStorageTypes(datanodeInfo));
    }
    return StorageTierUtil.findSupportedStorageTiers(dnStorageTypes);
  }

  public static Set<StorageType> getDatanodeStorageTypes(
      DatanodeInfo datanodeInfo) {
    List<StorageReportProto> storageReportProtos =
        datanodeInfo.getStorageReports();
    if (storageReportProtos == null || storageReportProtos.isEmpty()) {
      return Collections.emptySet();
    }

    Set<StorageType> uniqueStorageTypes = new HashSet<>();
    for (StorageReportProto storageReportProto : storageReportProtos) {
      uniqueStorageTypes.add(StorageTypeUtils.getFromProtobuf((
          storageReportProto.getStorageType())));
    }
    return uniqueStorageTypes;
  }

  public static StorageTypeProto getStorageTypeFromStorageReportProto(
      StorageReportProto storageReportProto) {
    return storageReportProto.hasStorageType()
        ? storageReportProto.getStorageType() : StorageTypeProto.DISK;
  }
}
