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

import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.failure.Failures;
import org.apache.hadoop.ozone.loadgenerators.AgedDirLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.NestedDirLoadGenerator;
import org.apache.hadoop.ozone.loadgenerators.RandomDirLoadGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import picocli.CommandLine;

/**
 * Chaos cluster for Storage Container Manager.
 */
@CommandLine.Command(
    name = "scm",
    description = "run chaos cluster across Storage Container Managers",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestStorageContainerManagerMiniChaosOzoneCluster extends
    TestMiniChaosOzoneCluster implements Callable<Void> {

  @BeforeAll
  void setup() {
    setNumManagers(3, 3, true);
    setNumDatanodes(3);

    addLoadClasses(AgedDirLoadGenerator.class);
    addLoadClasses(RandomDirLoadGenerator.class);
    addLoadClasses(NestedDirLoadGenerator.class);

    addFailureClasses(Failures.StorageContainerManagerRestartFailure.class);
    addFailureClasses(Failures.StorageContainerManagerStartStopFailure.class);
  }

  @Override
  public Void call() throws Exception {
    setup();
    startChaosCluster();
    return null;
  }

}
