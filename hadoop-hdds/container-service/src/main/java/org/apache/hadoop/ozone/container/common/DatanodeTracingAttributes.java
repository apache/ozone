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

package org.apache.hadoop.ozone.container.common;

/**
 * Span attribute keys used by the datanode.
 */
public final class DatanodeTracingAttributes {

  /** Audit status of a container command: SUCCESS or FAILURE. */
  public static final String STATUS = "ozone.status";

  /** {@code Type} of a container command, for example {@code WriteChunk}. */
  public static final String DN_CMD_TYPE = "dn.cmd_type";

  private DatanodeTracingAttributes() {
  }
}
