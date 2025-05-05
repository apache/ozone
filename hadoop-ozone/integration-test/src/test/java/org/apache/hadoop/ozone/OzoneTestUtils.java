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

package org.apache.hadoop.ozone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.LambdaTestUtils.VoidCallable;
import org.apache.ratis.util.function.CheckedConsumer;

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
   * Triggers Close container event for containers which contain the blocks
   * listed in omKeyLocationInfoGroups.
   *
   * @param omKeyLocationInfoGroups locationInfos for a key.
   * @param scm StorageContainerManager instance.
   * @throws Exception
   */
  public static void triggerCloseContainerEvent(
      List<OmKeyLocationInfoGroup> omKeyLocationInfoGroups,
      StorageContainerManager scm) throws Exception {
    performOperationOnKeyContainers((blockID) -> scm.getEventQueue()
            .fireEvent(SCMEvents.CLOSE_CONTAINER,
                ContainerID.valueOf(blockID.getContainerID())),
        omKeyLocationInfoGroups);
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
          .getContainer(ContainerID.valueOf(blockID.getContainerID()))
          .getState() == HddsProtos.LifeCycleState.OPEN) {
        scm.getContainerManager()
            .updateContainerState(ContainerID.valueOf(blockID.getContainerID()),
                HddsProtos.LifeCycleEvent.FINALIZE);
      }
      if (scm.getContainerManager()
          .getContainer(ContainerID.valueOf(blockID.getContainerID()))
          .getState() == HddsProtos.LifeCycleState.CLOSING) {
        scm.getContainerManager()
            .updateContainerState(ContainerID.valueOf(blockID.getContainerID()),
                HddsProtos.LifeCycleEvent.CLOSE);
      }
      assertFalse(scm.getContainerManager()
          .getContainer(ContainerID.valueOf(blockID.getContainerID()))
          .isOpen());
    }, omKeyLocationInfoGroups);
  }

  /**
   * Close all containers.
   *
   * @param eventPublisher event publisher.
   * @param scm StorageContainerManager instance.
   * @return true if close containers is successful.
   * @throws IOException
   */
  public static void closeAllContainers(EventPublisher eventPublisher,
      StorageContainerManager scm) {
    for (ContainerInfo container :
        scm.getContainerManager().getContainers()) {
      eventPublisher.fireEvent(SCMEvents.CLOSE_CONTAINER,
          container.containerID());
    }
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

    for (OmKeyLocationInfoGroup omKeyLocationInfoGroup :
        omKeyLocationInfoGroups) {
      List<OmKeyLocationInfo> omKeyLocationInfos =
          omKeyLocationInfoGroup.getLocationList();
      for (OmKeyLocationInfo omKeyLocationInfo : omKeyLocationInfos) {
        BlockID blockID = omKeyLocationInfo.getBlockID();
        consumer.accept(blockID);
      }
    }
  }

  public static <E extends Throwable> void expectOmException(
      OMException.ResultCodes code,
      VoidCallable eval) {

    OMException ex = assertThrows(OMException.class, () -> eval.call(), "OMException is expected");
    assertEquals(code, ex.getResult());
  }

  /**
    * Close container & Wait till container state becomes CLOSED.
   */
  public static void closeContainer(StorageContainerManager scm,
      ContainerInfo container)
      throws IOException, TimeoutException, InterruptedException {
    Pipeline pipeline = scm.getPipelineManager()
        .getPipeline(container.getPipelineID());
    scm.getPipelineManager().closePipeline(pipeline.getId());
    GenericTestUtils.waitFor(() ->
            container.getState() == HddsProtos.LifeCycleState.CLOSED,
        200, 30000);
  }

  /**
   * Flush deleted block log & wait till something was flushed.
   */
  public static void flushAndWaitForDeletedBlockLog(StorageContainerManager scm)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        scm.getScmHAManager().asSCMHADBTransactionBuffer().flush();
        if (scm.getScmBlockManager().getDeletedBlockLog().getNumOfValidTransactions() > 0) {
          return true;
        }
      } catch (IOException e) {
      }
      return false;
    }, 100, 3000);
  }

  /**
   * Wait till all blocks are removed.
   */
  public static void waitBlockDeleted(StorageContainerManager scm)
      throws InterruptedException, TimeoutException {
    GenericTestUtils.waitFor(() -> {
      try {
        if (scm.getScmBlockManager().getDeletedBlockLog().getNumOfValidTransactions() == 0) {
          return true;
        }
      } catch (IOException e) {
      }
      return false;
    }, 1000, 60000);
  }
}
