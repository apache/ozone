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

package org.apache.hadoop.hdds.scm.container.balancer;

import java.util.Map;
import org.apache.hadoop.hdds.protocol.DatanodeID;

/**
 * Information about the process of moving data.
 */
public class DataMoveInfo {
  private final long sizeScheduledForMove;
  private final long dataSizeMoved;
  private final Map<DatanodeID, Long> sizeEnteringNodes;
  private final Map<DatanodeID, Long> sizeLeavingNodes;

  public DataMoveInfo(
      long sizeScheduledForMove,
      long dataSizeMoved,
      Map<DatanodeID, Long> sizeEnteringNodes,
      Map<DatanodeID, Long> sizeLeavingNodes) {
    this.sizeScheduledForMove = sizeScheduledForMove;
    this.dataSizeMoved = dataSizeMoved;
    this.sizeEnteringNodes = sizeEnteringNodes;
    this.sizeLeavingNodes = sizeLeavingNodes;
  }

  public long getSizeScheduledForMove() {
    return sizeScheduledForMove;
  }

  public long getDataSizeMoved() {
    return dataSizeMoved;
  }

  public Map<DatanodeID, Long> getSizeEnteringNodes() {
    return sizeEnteringNodes;
  }

  public Map<DatanodeID, Long> getSizeLeavingNodes() {
    return sizeLeavingNodes;
  }
}
