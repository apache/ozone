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
package org.apache.hadoop.ozone.container.common.impl;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hdds.scm.container.common.helpers
    .StorageContainerException;
import org.apache.hadoop.ozone.container.common.interfaces
    .ContainerDeletionChoosingPolicy;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.ozone.container.keyvalue.statemachine.background.BlockDeletingService.ContainerBlockInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Randomly choosing containers for block deletion.
 */
public class RandomContainerDeletionChoosingPolicy
    implements ContainerDeletionChoosingPolicy {
  private static final Logger LOG =
      LoggerFactory.getLogger(RandomContainerDeletionChoosingPolicy.class);

  @Override
  public List<ContainerBlockInfo> chooseContainerForBlockDeletion(
      int blockCount, Map<Long, ContainerData> candidateContainers)
      throws StorageContainerException {
    Preconditions.checkNotNull(candidateContainers,
        "Internal assertion: candidate containers cannot be null");

    List<ContainerBlockInfo> result = new ArrayList<>();
    ContainerData[] values = new ContainerData[candidateContainers.size()];
    // to get a shuffle list
    ContainerData[] shuffled = candidateContainers.values().toArray(values);
    ArrayUtils.shuffle(shuffled);

    // Here we are returning containers based on totalBlocks which is basically
    // number of blocks to be deleted in an interval. We are also considering
    // the boundary case where the blocks of the last container exceeds the
    // number of blocks to be deleted in an interval, there we return that
    // container but with container we also return an integer so that total
    // blocks don't exceed the number of blocks to be deleted in an interval.

    for (ContainerData entry : shuffled) {
      if (((KeyValueContainerData) entry).getNumPendingDeletionBlocks() > 0) {
        long numBlocksToDelete = Math.min(blockCount,
            ((KeyValueContainerData) entry).getNumPendingDeletionBlocks());
        blockCount -= numBlocksToDelete;
        result.add(new ContainerBlockInfo(entry, numBlocksToDelete));
        if (LOG.isDebugEnabled()) {
          LOG.debug("Select container {} for block deletion, "
                  + "pending deletion blocks num: {}.", entry.getContainerID(),
              ((KeyValueContainerData) entry).getNumPendingDeletionBlocks());
        }
        if (blockCount == 0) {
          break;
        }
      }
    }
    return result;
  }
}
