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

package org.apache.hadoop.ozone.om.jobworker.node;

import org.apache.hadoop.hdds.protocol.JobworkerDetails;
import org.apache.hadoop.hdds.server.events.EventHandler;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles New JobWorker event.
 * When a new JobWorker is registered, this handler processes initial setup
 * and makes the node available for task assignment.
 */
public class NewJobworkerHandler implements EventHandler<JobworkerDetails> {
  private static final Logger LOG =
      LoggerFactory.getLogger(NewJobworkerHandler.class);

  private final JobworkerNodeManager nodeManager;

  public NewJobworkerHandler(JobworkerNodeManager nodeManager) {
    this.nodeManager = nodeManager;
  }

  @Override
  public void onMessage(JobworkerDetails jobworkerDetails,
                        EventPublisher publisher) {
    LOG.info("New JobWorker registered: {}. Preparing for task assignment.",
        jobworkerDetails);

    // TODO Jobworker: Add process logic for node op state
  }
}
