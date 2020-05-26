/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.ha;

import org.apache.hadoop.hdds.conf.ConfigurationSource;

import java.io.IOException;

/**
 * SCMHAManager provides HA service for SCM.
 *
 * It uses Apache Ratis for HA implementation. We will have a 2N+1
 * node Ratis ring. The Ratis ring will have one Leader node and 2N follower
 * nodes.
 *
 * TODO
 *
 */
public class SCMHAManager {

  private static boolean isLeader = true;

  private final SCMRatisServer ratisServer;

  /**
   * Creates SCMHAManager instance.
   */
  public SCMHAManager(final ConfigurationSource conf) throws IOException {
    this.ratisServer = new SCMRatisServer(
        conf.getObject(SCMHAConfiguration.class), conf);
  }

  /**
   * Starts HA service.
   */
  public void start() throws IOException {
    ratisServer.start();
  }

  /**
   * Returns true if the current SCM is the leader.
   */
  public static boolean isLeader() {
    return isLeader;
  }

  /**
   * Returns RatisServer instance associated with the SCM instance.
   */
  public SCMRatisServer getRatisServer() {
    return ratisServer;
  }

  /**
   * Stops the HA service.
   */
  public void shutdown() throws IOException {
    ratisServer.stop();
  }

}
