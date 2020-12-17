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

import java.io.IOException;
import java.util.Optional;

/**
 * SCMHAManager provides HA service for SCM.
 */
public interface SCMHAManager {

  /**
   * Starts HA service.
   */
  void start() throws IOException;

  /**
   * For HA mode, return an Optional that holds term of the
   * underlying RaftServer iff current SCM is in leader role.
   * Otherwise, return an empty optional.
   *
   * For non-HA mode, return an Optional that holds term 0.
   */
  Optional<Long> isLeader();

  /**
   * Returns RatisServer instance associated with the SCM instance.
   */
  SCMRatisServer getRatisServer();

  /**
   * Stops the HA service.
   */
  void shutdown() throws IOException;
}
