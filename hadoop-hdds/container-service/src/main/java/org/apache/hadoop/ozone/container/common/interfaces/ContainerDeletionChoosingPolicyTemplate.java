/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common.interfaces;


import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingService.ContainerBlockInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Abstract class that serves as the template for deletion choosing policy.
 */
public abstract class ContainerDeletionChoosingPolicyTemplate
    implements ContainerDeletionChoosingPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerDeletionChoosingPolicyTemplate.class);

  @Override
  public final List<ContainerBlockInfo> chooseContainerForBlockDeletion(
      int blockCount, Map<Long, ContainerData> candidateContainers)
      throws StorageContainerException {
    Preconditions.checkNotNull(candidateContainers,
        "Internal assertion: candidate containers cannot be null");

    int originalBlockCount = blockCount;
    List<ContainerBlockInfo> result = new ArrayList<>();
    List<KeyValueContainerData> orderedList = new LinkedList<>();

    for (ContainerData entry: candidateContainers.values()) {
      orderedList.add((KeyValueContainerData) entry);
    }

    orderByDescendingPriority(orderedList);

    // Here we are returning containers based on blockCount which is basically
    // number of blocks to be deleted in an interval. We are also considering
    // the boundary case where the blocks of the last container exceeds the
    // number of blocks to be deleted in an interval, there we return that
    // container but with container we also return an integer so that total
    // blocks don't exceed the number of blocks to be deleted in an interval.

    for (KeyValueContainerData entry : orderedList) {
      if (entry.getNumPendingDeletionBlocks() > 0) {
        long numBlocksToDelete = Math.min(blockCount,
            entry.getNumPendingDeletionBlocks());
        blockCount -= numBlocksToDelete;
        result.add(new ContainerBlockInfo(entry, numBlocksToDelete));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Select container {} for block deletion, "
              + "pending deletion blocks num: {}.", entry.getContainerID(),
              entry.getNumPendingDeletionBlocks());
        }
        if (blockCount == 0) {
          break;
        }
      }
    }
    LOG.info("Chosen {}/{} blocks from {} candidate containers.",
        (originalBlockCount - blockCount), blockCount, orderedList.size());
    return result;
  }

  /**
   * Abstract step for ordering the container data to be deleted.
   * Subclass need to implement the concrete ordering implementation
   * in descending order (more prioritized -> less prioritized)
   * @param candidateContainers candidate containers to be ordered
   */
  protected abstract void orderByDescendingPriority(
      List<KeyValueContainerData> candidateContainers);
}
