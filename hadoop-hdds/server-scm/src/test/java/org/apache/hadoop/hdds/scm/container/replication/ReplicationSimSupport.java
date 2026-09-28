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

package org.apache.hadoop.hdds.scm.container.replication;

/**
 * Runs one pass of the ReplicationManager's under- and over-replication processor threads on the caller's thread. Used
 * by the SCM simulation, which runs these periodic tasks itself.
 */
public final class ReplicationSimSupport {

  private ReplicationSimSupport() {
  }

  public static void processUnderReplicated(ReplicationManager rm) {
    if (rm.shouldRun()) {
      new UnderReplicatedProcessor(rm, rm.getConfig()::getUnderReplicatedInterval).processAll(rm.getQueue());
    }
  }

  public static void processOverReplicated(ReplicationManager rm) {
    if (rm.shouldRun()) {
      new OverReplicatedProcessor(rm, rm.getConfig()::getOverReplicatedInterval).processAll(rm.getQueue());
    }
  }

  /** ReplicationManager wakes up its monitor on a node state change only when no work is queued. */
  public static boolean hasNoQueuedWork(ReplicationManager rm) {
    return rm.getQueue().isEmpty();
  }
}
