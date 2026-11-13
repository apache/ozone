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
 * Span attribute keys used by SCM.
 */
public final class ScmTracingAttributes {

  /** Status or result code of a failed request, for example SAFE_MODE_EXCEPTION. */
  public static final String STATUS = "ozone.status";

  /** {@code Type} of an SCM request, for example {@code AllocateScmBlock}. */
  public static final String SCM_CMD_TYPE = "scm.cmd_type";

  /** ID of the container a block was allocated in. */
  public static final String HDDS_CONTAINER_ID = "hdds.container.id";

  public static final String HDDS_BLOCK_LOCAL_ID = "hdds.block.local_id";

  /** ID of the pipeline of the container a block was allocated in. */
  public static final String PIPELINE_ID = "pipeline.id";

  /** {@code true} when SCM created a new pipeline to allocate a container. */
  public static final String SCM_PIPELINE_CREATED = "scm.pipeline.created";

  /** Event added when SCM could not find a container for a block and returns no block. */
  public static final String SCM_ALLOCATE_FAILED = "scm.allocate.failed";

  private ScmTracingAttributes() {
  }
}
