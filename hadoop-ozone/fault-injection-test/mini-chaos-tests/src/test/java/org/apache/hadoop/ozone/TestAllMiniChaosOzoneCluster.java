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
import org.apache.hadoop.ozone.loadgenerators.LoadGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import picocli.CommandLine;

/**
 * Test all kinds of chaos.
 */
@CommandLine.Command(
    name = "all",
    description = "run chaos cluster across all daemons",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestAllMiniChaosOzoneCluster extends TestMiniChaosOzoneCluster
    implements Callable<Void> {

  @BeforeAll
  void setup() {
    setNumManagers(3, 3, true);

    LoadGenerator.getClassList().forEach(this::addLoadClasses);
    Failures.getClassList().forEach(this::addFailureClasses);
  }

  @Override
  public Void call() throws Exception {
    setup();
    startChaosCluster();
    return null;
  }

}
