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

package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.server.ServiceRuntimeInfo;

/**
 * This is the JMX management interface for DN information.
 */
@InterfaceAudience.Private
public interface DNMXBean extends ServiceRuntimeInfo {

  /**
   * Gets the datanode hostname.
   *
   * @return the datanode hostname for the datanode.
   */
  String getHostname();

  /**
   * Gets the client rpc port.
   *
   * @return the client rpc port
   */
  String getClientRpcPort();

  /**
   * Gets the http port.
   *
   * @return the http port
   */
  String getHttpPort();

  /**
   * Gets the https port.
   *
   * @return the http port
   */
  String getHttpsPort();
}
