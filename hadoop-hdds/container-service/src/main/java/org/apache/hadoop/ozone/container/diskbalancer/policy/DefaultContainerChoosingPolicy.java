/*
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

package org.apache.hadoop.ozone.container.diskbalancer.policy;

import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Choose a container from specified volume, make sure it's not being balancing.
 */
public class DefaultContainerChoosingPolicy implements ContainerChoosingPolicy {
  public static final Logger LOG = LoggerFactory.getLogger(
      DefaultContainerChoosingPolicy.class);

  @Override
  public List<ContainerData> chooseContainer(OzoneContainer ozoneContainer,
      HddsVolume hddsVolume, Set<Long> inProgressContainerIDs,
      Long targetSize) {
    List<ContainerData> results = new ArrayList<>();
    long sizeTotal = 0L;

    Iterator<Container<?>> itr = ozoneContainer.getController()
        .getContainers(hddsVolume);
    while (itr.hasNext() && sizeTotal < targetSize) {
      ContainerData containerData = itr.next().getContainerData();
      if (!inProgressContainerIDs.contains(
          containerData.getContainerID()) && containerData.isClosed()) {
        results.add(containerData);
        sizeTotal += containerData.getBytesUsed();
      }
    }
    return results;
  }
}
