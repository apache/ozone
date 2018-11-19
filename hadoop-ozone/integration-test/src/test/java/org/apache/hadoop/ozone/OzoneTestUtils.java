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
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ratis.util.CheckedConsumer;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;

/**
 * Helper class for Tests.
 */
public final class OzoneTestUtils {
  /**
   * Never Constructed.
   */
  private OzoneTestUtils() {
  }

  /**
   * Close containers which contain the blocks listed in
   * omKeyLocationInfoGroups.
   *
   * @param omKeyLocationInfoGroups locationInfos for a key.
   * @param scm StorageContainerManager instance.
   * @return true if close containers is successful.
   * @throws IOException
   */
  public static void closeContainers(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups,
      StorageContainerManager scm) throws Exception {
    performOperationOnKeyContainers((blockID) -> {
      if (scm.getContainerManager()
          .getContainer(ContainerID.valueof(blockID.getContainerID()))
          .getState() == HddsProtos.LifeCycleState.OPEN) {
        scm.getContainerManager()
            .updateContainerState(ContainerID.valueof(blockID.getContainerID()),
                HddsProtos.LifeCycleEvent.FINALIZE);
      }
      if (scm.getContainerManager()
          .getContainer(ContainerID.valueof(blockID.getContainerID()))
          .getState() == HddsProtos.LifeCycleState.CLOSING) {
        scm.getContainerManager()
            .updateContainerState(ContainerID.valueof(blockID.getContainerID()),
                HddsProtos.LifeCycleEvent.CLOSE);
      }
      Assert.assertFalse(scm.getContainerManager()
          .getContainer(ContainerID.valueof(blockID.getContainerID()))
          .isOpen());
    }, omKeyLocationInfoGroups);
  }

  /**
   * Performs the provided consumer on containers which contain the blocks
   * listed in omKeyLocationInfoGroups.
   *
   * @param consumer Consumer which accepts BlockID as argument.
   * @param omKeyLocationInfoGroups locationInfos for a key.
   * @throws IOException
   */
  public static void performOperationOnKeyContainers(
      CheckedConsumer<BlockID, Exception> consumer,
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups) throws Exception {

    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup : omKeyLocationInfoGroups) {
      List<OmKeyLocationInfo> omKeyLocationInfos =
          omKeyLocationInfoGroup.getLocationList();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfos) {
        BlockID blockID = omKeyLocationInfo.getBlockID();
        consumer.accept(blockID);
      }
    }
  }

}
