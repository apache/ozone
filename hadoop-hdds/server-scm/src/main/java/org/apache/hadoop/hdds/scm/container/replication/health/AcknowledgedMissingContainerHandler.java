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

package org.apache.hadoop.hdds.scm.container.replication.health;

import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.replication.ContainerCheckRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used in Replication Manager to skip containers that have been
 * acknowledged as missing. These containers will still be marked as
 * MISSING in the health state but will not trigger replication.
 */
public class AcknowledgedMissingContainerHandler extends AbstractCheck {

  private static final Logger LOG = LoggerFactory.getLogger(AcknowledgedMissingContainerHandler.class);

  @Override
  public boolean handle(ContainerCheckRequest request) {
    ContainerInfo containerInfo = request.getContainerInfo();
    ContainerID containerID = containerInfo.containerID();
    LOG.debug("Checking container {}, ackMissing={} in AcknowledgedMissingContainerHandler",
        containerID, containerInfo.isAckMissing());

    if (!containerInfo.isAckMissing()) {
      LOG.debug("Container {} is not acknowledged ", containerID);
      return false;
    }
    LOG.debug("Container {} has been acknowledged as missing.", containerID);

    if (request.getContainerReplicas().isEmpty()) {
      LOG.debug("Acknowledged missing container {} confirmed to have no replicas.", containerID);
    } else {
      LOG.warn("Container {} was acknowledged as missing but now has {} replicas. " +
          "The container may have been recovered. Consider un-acknowledging it.",
          containerID, request.getContainerReplicas().size());
    }
    return true;
  }
}
