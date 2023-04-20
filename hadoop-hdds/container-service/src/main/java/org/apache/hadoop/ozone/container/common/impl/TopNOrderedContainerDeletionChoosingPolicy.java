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

import org.apache.hadoop.ozone.container.common.interfaces.ContainerDeletionChoosingPolicyTemplate;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;

import java.util.Comparator;
import java.util.List;

/**
 * TopN Ordered choosing policy that choosing containers based on pending
 * deletion blocks' number.
 */
public class TopNOrderedContainerDeletionChoosingPolicy
    extends ContainerDeletionChoosingPolicyTemplate {
  /** customized comparator used to compare differentiate container data. **/
  private static final Comparator<KeyValueContainerData>
        KEY_VALUE_CONTAINER_DATA_COMPARATOR = (KeyValueContainerData c1,
                                               KeyValueContainerData c2) ->
              Long.compare(c2.getNumPendingDeletionBlocks(),
                  c1.getNumPendingDeletionBlocks());

  @Override
  protected void orderByDescendingPriority(
      List<KeyValueContainerData> candidateContainers) {
    // get top N list ordered by pending deletion blocks' number
    candidateContainers.sort(KEY_VALUE_CONTAINER_DATA_COMPARATOR);
  }
}
