/*
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

package org.apache.hadoop.ozone.recon.scm;

/**
 * Container last seen on a particular DataNode info class.
 */
public class ContainerReplicaHistory {
  private long firstSeenTime;
  private long lastSeenTime;

  ContainerReplicaHistory(long firstSeenTime, long lastSeenTime) {
    this.firstSeenTime = firstSeenTime;
    this.lastSeenTime = lastSeenTime;
  }

  public long getFirstSeenTime() {
    return firstSeenTime;
  }

  public long getLastSeenTime() {
    return lastSeenTime;
  }

  public void setLastSeenTime(long lastSeenTime) {
    this.lastSeenTime = lastSeenTime;
  }
}
