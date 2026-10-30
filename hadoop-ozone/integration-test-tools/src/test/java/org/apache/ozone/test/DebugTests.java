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

package org.apache.ozone.test;

import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.shell.TestOzoneDebugReplicasVerify;
import org.apache.hadoop.ozone.shell.TestOzoneDebugShell;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;

/**
 * Debug tests to be run with a single non-HA cluster.
 * <p/>
 * Specific tests are implemented in separate classes, and they are subclasses
 * here as {@link Nested} inner classes.  This allows running all tests in the
 * same cluster.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DebugTests extends ClusterForTests<MiniOzoneCluster> {

  @Nested
  class OzoneDebugShell extends TestOzoneDebugShell {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class OzoneDebugReplicasVerify extends TestOzoneDebugReplicasVerify {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }
}
