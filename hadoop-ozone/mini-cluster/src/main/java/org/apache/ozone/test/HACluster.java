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

import java.util.UUID;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;

/** Cluster for HA tests. */
public abstract class HACluster extends ClusterForTests<MiniOzoneHAClusterImpl> {
  @Override
  protected MiniOzoneHAClusterImpl.Builder newClusterBuilder() {
    return MiniOzoneCluster.newHABuilder(createOzoneConfig())
        .setOMServiceId("om-" + UUID.randomUUID())
        .setNumOfOzoneManagers(3)
        .setSCMServiceId("scm-" + UUID.randomUUID())
        .setNumOfStorageContainerManagers(3);
  }

  /** Test cases which need HA cluster should implement this. */
  public interface TestCase extends ClusterForTests.TestCase {
    @Override
    MiniOzoneHAClusterImpl cluster();
  }
}
