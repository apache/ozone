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

  private ScmTracingAttributes() {
  }
}
