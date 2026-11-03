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

package org.apache.hadoop.hdds.scm;

/**
 * Span attribute keys used by the datanode clients.
 */
public final class ClientTracingAttributes {

  /** Hostname of the datanode that served a container command; its UUID is in the datanode's service name. */
  public static final String HDDS_DATANODE = "hdds.datanode";

  public static final String HDDS_DATANODE_UUID = "hdds.datanode.uuid";

  /** Event added when a container command failed on one datanode and the client moves on to the next one. */
  public static final String HDDS_DATANODE_FAILED = "hdds.datanode.failed";

  private ClientTracingAttributes() {
  }
}
