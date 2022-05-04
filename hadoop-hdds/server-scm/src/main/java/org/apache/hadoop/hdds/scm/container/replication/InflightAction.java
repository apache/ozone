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
package org.apache.hadoop.hdds.scm.container.replication;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;

/**
* InflightAction is a Wrapper class to hold the InflightAction
* with its start time and the target datanode.
*/
public class InflightAction {
  private final DatanodeDetails datanode;
  private final long time;

  public InflightAction(final DatanodeDetails datanode,
                         final long time) {
    this.datanode = datanode;
    this.time = time;
  }

  @VisibleForTesting
  public DatanodeDetails getDatanode() {
    return datanode;
  }

  public long getTime() {
    return time;
  }
}
